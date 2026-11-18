from __future__ import annotations

import gzip
import json
import shutil
import sqlite3
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _configure_connection(conn: sqlite3.Connection) -> sqlite3.Connection:
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    conn.execute("PRAGMA busy_timeout=30000;")
    return conn


class PluginDatabase:
    """Core-managed WAL SQLite database helper with versioned migrations and online backups."""

    def __init__(self, db_path: Path, *, plugin_id: str, state_root: Path) -> None:
        self.db_path = db_path
        self.plugin_id = plugin_id
        self.state_root = state_root
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        # Initialize WAL mode immediately so file exists on disk for backup discovery
        with self.connect() as _:
            pass

    @contextmanager
    def connect(self) -> Iterator[sqlite3.Connection]:
        """Yield a WAL-configured SQLite connection inside a transaction context."""
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        try:
            _configure_connection(conn)
            with conn:
                yield conn
        finally:
            conn.close()

    def migrate(self, migrations: dict[int, str] | list[str]) -> list[int]:
        """Apply ordered SQL schema migrations idempotently and return newly applied versions."""
        normalized: dict[int, str] = (
            {idx + 1: sql for idx, sql in enumerate(migrations)}
            if isinstance(migrations, list)
            else dict(migrations)
        )
        applied_now: list[int] = []
        with self.connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS _agy_schema_migrations (
                    version INTEGER PRIMARY KEY,
                    applied_at TEXT NOT NULL
                );
                """
            )
            existing = {
                int(row["version"])
                for row in conn.execute("SELECT version FROM _agy_schema_migrations").fetchall()
            }
            for version in sorted(normalized):
                if version in existing:
                    continue
                sql_script = normalized[version]
                conn.executescript(sql_script)
                conn.execute(
                    "INSERT INTO _agy_schema_migrations (version, applied_at) VALUES (?, ?)",
                    (version, _now_iso()),
                )
                applied_now.append(version)
        return applied_now

    def applied_migrations(self) -> list[int]:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='_agy_schema_migrations'"
            ).fetchone()
            if row is None:
                return []
            rows = conn.execute(
                "SELECT version FROM _agy_schema_migrations ORDER BY version ASC"
            ).fetchall()
            return [int(r["version"]) for r in rows]

    def execute(self, sql: str, params: tuple[Any, ...] = ()) -> int:
        """Execute a DML query and return `cursor.rowcount`."""
        with self.connect() as conn:
            cur = conn.execute(sql, params)
            return int(cur.rowcount)

    def fetchone(self, sql: str, params: tuple[Any, ...] = ()) -> dict[str, Any] | None:
        with self.connect() as conn:
            row = conn.execute(sql, params).fetchone()
            return dict(row) if row is not None else None

    def fetchall(self, sql: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute(sql, params).fetchall()
            return [dict(r) for r in rows]

    def backup_compressed(
        self,
        *,
        backups_dir: Path | None = None,
        retain: int = 14,
        timestamp_slug: str | None = None,
    ) -> Path:
        """Create a WAL-safe online SQLite backup compressed to `.sqlite3.gz` and prune old files."""
        target_dir = backups_dir or (self.state_root / "backups" / "plugins" / self.plugin_id)
        target_dir.mkdir(parents=True, exist_ok=True)
        ts = timestamp_slug or datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        stem = self.db_path.stem
        dest_gz = target_dir / f"{stem}-{ts}.sqlite3.gz"

        with tempfile.NamedTemporaryFile(
            prefix=f"agy-bk-{self.plugin_id}-{stem}-", suffix=".sqlite3", delete=False
        ) as tmp:
            tmp_path = Path(tmp.name)

        try:
            with (
                sqlite3.connect(self.db_path, timeout=30.0) as src_conn,
                sqlite3.connect(tmp_path, timeout=30.0) as dst_conn,
            ):
                src_conn.backup(dst_conn)
            with tmp_path.open("rb") as f_in, gzip.open(dest_gz, "wb", compresslevel=6) as f_out:
                shutil.copyfileobj(f_in, f_out)
        finally:
            tmp_path.unlink(missing_ok=True)

        if retain > 0:
            snapshots = sorted(target_dir.glob(f"{stem}-*.sqlite3.gz"))
            for stale in snapshots[:-retain]:
                stale.unlink(missing_ok=True)

        return dest_gz


class PluginKeyValueStore:
    """Zero-SQL JSON key-value store managed by the Core for simple plugin state."""

    def __init__(self, database: PluginDatabase) -> None:
        self._db = database
        self._init_table()

    def _init_table(self) -> None:
        with self._db.connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS _agy_plugin_kv (
                    key TEXT PRIMARY KEY,
                    value_json TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                """
            )

    def get(self, key: str, default: Any = None) -> Any:
        row = self._db.fetchone(
            "SELECT value_json FROM _agy_plugin_kv WHERE key = ?",
            (key,),
        )
        if row is None:
            return default
        return json.loads(str(row["value_json"]))

    def set(self, key: str, value: Any) -> None:
        payload = json.dumps(value)
        now = _now_iso()
        self._db.execute(
            """
            INSERT INTO _agy_plugin_kv (key, value_json, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET
                value_json = excluded.value_json,
                updated_at = excluded.updated_at
            """,
            (key, payload, now),
        )

    def delete(self, key: str) -> bool:
        count = self._db.execute("DELETE FROM _agy_plugin_kv WHERE key = ?", (key,))
        return count > 0

    def list_keys(self, prefix: str = "") -> list[str]:
        if prefix:
            rows = self._db.fetchall(
                "SELECT key FROM _agy_plugin_kv WHERE key LIKE ? ORDER BY key ASC",
                (f"{prefix}%",),
            )
        else:
            rows = self._db.fetchall("SELECT key FROM _agy_plugin_kv ORDER BY key ASC")
        return [str(r["key"]) for r in rows]

    def get_all(self, prefix: str = "") -> dict[str, Any]:
        if prefix:
            rows = self._db.fetchall(
                "SELECT key, value_json FROM _agy_plugin_kv WHERE key LIKE ? ORDER BY key ASC",
                (f"{prefix}%",),
            )
        else:
            rows = self._db.fetchall("SELECT key, value_json FROM _agy_plugin_kv ORDER BY key ASC")
        return {str(r["key"]): json.loads(str(r["value_json"])) for r in rows}
