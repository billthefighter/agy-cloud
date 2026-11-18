from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from agent_tasks.common.models import IssueStatus


@dataclass(slots=True)
class IssueRecord:
    issue_id: str
    short_id: str
    project_slug: str
    github_repo: str
    default_branch: str
    title: str
    permalink: str
    status: IssueStatus
    turn: int
    conversation_id: str | None
    thread_ts: str | None
    channel_id: str | None
    pr_url: str | None
    last_error: str | None
    updated_at: str
    task_kind: str = "sentry"
    base_ref: str | None = None
    target_branch: str | None = None
    model_override: str | None = None
    banner_note: str | None = None
    dedup_key: str | None = None
    occurrence_count: int = 1
    last_seen_at: str | None = None
    last_event_meta: str | None = None
    resolved_ref: str | None = None
    affected_files: list[str] = field(default_factory=list)
    pending_feedback: list[str] = field(default_factory=list)
    auto_followup_count: int = 0


@dataclass(slots=True)
class SignalObservationRecord:
    dedup_key: str
    source_kind: str
    first_seen_at: str
    last_seen_at: str
    consecutive_hits: int
    consecutive_misses: int
    total_hits: int
    episode_open: bool
    recovered_at: str | None
    promoted_issue_id: str | None
    latest_payload: dict[str, Any]


@dataclass(slots=True)
class TimerRecord:
    id: int
    dedup_key: str
    issue_id: str | None
    kind: str
    fire_at: str
    prompt: str
    created_by: str
    status: str
    created_at: str
    fired_at: str | None = None


@dataclass(slots=True)
class PwaThreadRecord:
    thread_id: str
    thread_ts: str | None
    kind: str
    title: str
    subtitle: str
    status: str
    folder: str
    pinned: bool
    unread_count: int
    tags: list[str]
    model: str
    conversation_id: str | None
    issue_id: str | None
    food_entry_id: int | None
    last_message_preview: str
    created_at: str
    updated_at: str


@dataclass(slots=True)
class PwaMessageRecord:
    id: int
    thread_id: str
    external_ts: str | None
    sender: str
    body_md: str
    payload: dict[str, Any]
    created_at: str
    updated_at: str


@dataclass(slots=True)
class PushSubscriptionRecord:
    endpoint: str
    p256dh: str
    auth: str
    user_agent: str
    prefs: dict[str, bool] = field(default_factory=dict)
    created_at: str = ""
    last_sent_at: str | None = None


class StateStore:
    """SQLite state store persisting task <-> Slack thread <-> AGY session mappings."""

    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA foreign_keys=ON;")
        return conn

    def _init_schema(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS issues (
                    issue_id TEXT PRIMARY KEY,
                    short_id TEXT NOT NULL,
                    project_slug TEXT NOT NULL,
                    github_repo TEXT NOT NULL,
                    default_branch TEXT NOT NULL DEFAULT 'main',
                    title TEXT NOT NULL,
                    permalink TEXT NOT NULL DEFAULT '',
                    status TEXT NOT NULL,
                    turn INTEGER NOT NULL DEFAULT 0,
                    conversation_id TEXT,
                    thread_ts TEXT,
                    channel_id TEXT,
                    pr_url TEXT,
                    last_error TEXT,
                    updated_at TEXT NOT NULL,
                    task_kind TEXT NOT NULL DEFAULT 'sentry',
                    base_ref TEXT,
                    target_branch TEXT,
                    model_override TEXT,
                    banner_note TEXT
                );
                """
            )
            existing_cols = {
                str(r["name"]) for r in conn.execute("PRAGMA table_info(issues)").fetchall()
            }
            migrations = {
                "task_kind": "ALTER TABLE issues ADD COLUMN task_kind TEXT NOT NULL DEFAULT 'sentry'",
                "base_ref": "ALTER TABLE issues ADD COLUMN base_ref TEXT",
                "target_branch": "ALTER TABLE issues ADD COLUMN target_branch TEXT",
                "model_override": "ALTER TABLE issues ADD COLUMN model_override TEXT",
                "banner_note": "ALTER TABLE issues ADD COLUMN banner_note TEXT",
                "dedup_key": "ALTER TABLE issues ADD COLUMN dedup_key TEXT",
                "occurrence_count": "ALTER TABLE issues ADD COLUMN occurrence_count INTEGER NOT NULL DEFAULT 1",
                "last_seen_at": "ALTER TABLE issues ADD COLUMN last_seen_at TEXT",
                "last_event_meta": "ALTER TABLE issues ADD COLUMN last_event_meta TEXT",
                "resolved_ref": "ALTER TABLE issues ADD COLUMN resolved_ref TEXT",
                "affected_files_json": "ALTER TABLE issues ADD COLUMN affected_files_json TEXT NOT NULL DEFAULT '[]'",
                "pending_feedback_json": "ALTER TABLE issues ADD COLUMN pending_feedback_json TEXT NOT NULL DEFAULT '[]'",
                "auto_followup_count": "ALTER TABLE issues ADD COLUMN auto_followup_count INTEGER NOT NULL DEFAULT 0",
            }
            for col, ddl in migrations.items():
                if col not in existing_cols:
                    conn.execute(ddl)

            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_issues_thread_ts
                ON issues (thread_ts)
                WHERE thread_ts IS NOT NULL;
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_issues_dedup_key
                ON issues (dedup_key)
                WHERE dedup_key IS NOT NULL;
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS signal_observations (
                    dedup_key TEXT PRIMARY KEY,
                    source_kind TEXT NOT NULL,
                    first_seen_at TEXT NOT NULL,
                    last_seen_at TEXT NOT NULL,
                    consecutive_hits INTEGER NOT NULL DEFAULT 1,
                    consecutive_misses INTEGER NOT NULL DEFAULT 0,
                    total_hits INTEGER NOT NULL DEFAULT 1,
                    episode_open INTEGER NOT NULL DEFAULT 1,
                    recovered_at TEXT,
                    promoted_issue_id TEXT,
                    latest_payload_json TEXT NOT NULL DEFAULT '{}'
                );
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_signal_obs_source_open
                ON signal_observations (source_kind, episode_open);
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS timers (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    dedup_key TEXT NOT NULL,
                    issue_id TEXT,
                    kind TEXT NOT NULL DEFAULT 'followup',
                    fire_at TEXT NOT NULL,
                    prompt TEXT NOT NULL DEFAULT '',
                    created_by TEXT NOT NULL DEFAULT 'agent',
                    status TEXT NOT NULL DEFAULT 'pending',
                    created_at TEXT NOT NULL,
                    fired_at TEXT
                );
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_timers_pending_fire
                ON timers (status, fire_at)
                WHERE status = 'pending';
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS channel_bindings (
                    role TEXT PRIMARY KEY,
                    channel_id TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS usage_ledger (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    recorded_at TEXT NOT NULL,
                    cycle_key TEXT NOT NULL,
                    issue_id TEXT NOT NULL,
                    turn INTEGER NOT NULL,
                    conversation_id TEXT,
                    task_kind TEXT NOT NULL,
                    model TEXT NOT NULL,
                    prompt_tokens INTEGER NOT NULL DEFAULT 0,
                    cached_tokens INTEGER NOT NULL DEFAULT 0,
                    output_tokens INTEGER NOT NULL DEFAULT 0,
                    thoughts_tokens INTEGER NOT NULL DEFAULT 0,
                    cost_usd REAL NOT NULL DEFAULT 0.0,
                    note TEXT
                );
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_usage_ledger_cycle
                ON usage_ledger (cycle_key);
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_usage_ledger_conv
                ON usage_ledger (conversation_id)
                WHERE conversation_id IS NOT NULL AND conversation_id != '';
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS billing_alerts (
                    cycle_key TEXT NOT NULL,
                    alert_key TEXT NOT NULL,
                    sent_at TEXT NOT NULL,
                    PRIMARY KEY (cycle_key, alert_key)
                );
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS kv_settings (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS pwa_threads (
                    thread_id TEXT PRIMARY KEY,
                    thread_ts TEXT UNIQUE,
                    kind TEXT NOT NULL,
                    title TEXT NOT NULL,
                    subtitle TEXT NOT NULL DEFAULT '',
                    status TEXT NOT NULL DEFAULT 'active',
                    folder TEXT NOT NULL DEFAULT 'inbox',
                    pinned INTEGER NOT NULL DEFAULT 0,
                    unread_count INTEGER NOT NULL DEFAULT 0,
                    tags_json TEXT NOT NULL DEFAULT '[]',
                    model TEXT NOT NULL DEFAULT 'gemini-4-hydrogen',
                    conversation_id TEXT,
                    issue_id TEXT,
                    food_entry_id INTEGER,
                    last_message_preview TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_pwa_threads_folder_order
                ON pwa_threads (folder, pinned DESC, updated_at DESC);
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_pwa_threads_issue_id
                ON pwa_threads (issue_id)
                WHERE issue_id IS NOT NULL;
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS pwa_messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    thread_id TEXT NOT NULL REFERENCES pwa_threads(thread_id) ON DELETE CASCADE,
                    external_ts TEXT,
                    sender TEXT NOT NULL,
                    body_md TEXT NOT NULL,
                    payload_json TEXT NOT NULL DEFAULT '{}',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_pwa_messages_thread_id
                ON pwa_messages (thread_id, id ASC);
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_pwa_messages_external_ts
                ON pwa_messages (external_ts)
                WHERE external_ts IS NOT NULL;
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS pwa_push_subscriptions (
                    endpoint TEXT PRIMARY KEY,
                    p256dh TEXT NOT NULL,
                    auth TEXT NOT NULL,
                    user_agent TEXT NOT NULL DEFAULT '',
                    prefs_json TEXT NOT NULL DEFAULT '{}',
                    created_at TEXT NOT NULL,
                    last_sent_at TEXT
                );
                """
            )

    @staticmethod
    def _now() -> str:
        return datetime.now(UTC).isoformat()

    @staticmethod
    def _parse_str_list(raw: str | None) -> list[str]:
        if not raw:
            return []
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, list):
                return [str(x) for x in parsed if str(x).strip()]
        except (ValueError, TypeError):
            pass
        return []

    @staticmethod
    def _row_to_record(row: sqlite3.Row) -> IssueRecord:
        return IssueRecord(
            issue_id=row["issue_id"],
            short_id=row["short_id"],
            project_slug=row["project_slug"],
            github_repo=row["github_repo"],
            default_branch=row["default_branch"],
            title=row["title"],
            permalink=row["permalink"],
            status=IssueStatus(row["status"]),
            turn=int(row["turn"]),
            conversation_id=row["conversation_id"],
            thread_ts=row["thread_ts"],
            channel_id=row["channel_id"],
            pr_url=row["pr_url"],
            last_error=row["last_error"],
            updated_at=row["updated_at"],
            task_kind=row["task_kind"] or "sentry",
            base_ref=row["base_ref"],
            target_branch=row["target_branch"],
            model_override=row["model_override"],
            banner_note=row["banner_note"],
            dedup_key=row["dedup_key"],
            occurrence_count=int(row["occurrence_count"] or 1),
            last_seen_at=row["last_seen_at"],
            last_event_meta=row["last_event_meta"],
            resolved_ref=row["resolved_ref"],
            affected_files=StateStore._parse_str_list(row["affected_files_json"]),
            pending_feedback=StateStore._parse_str_list(row["pending_feedback_json"]),
            auto_followup_count=int(row["auto_followup_count"] or 0),
        )

    @staticmethod
    def _row_to_signal_obs(row: sqlite3.Row) -> SignalObservationRecord:
        try:
            payload = json.loads(row["latest_payload_json"] or "{}")
            if not isinstance(payload, dict):
                payload = {}
        except (ValueError, TypeError):
            payload = {}
        return SignalObservationRecord(
            dedup_key=str(row["dedup_key"]),
            source_kind=str(row["source_kind"]),
            first_seen_at=str(row["first_seen_at"]),
            last_seen_at=str(row["last_seen_at"]),
            consecutive_hits=int(row["consecutive_hits"] or 0),
            consecutive_misses=int(row["consecutive_misses"] or 0),
            total_hits=int(row["total_hits"] or 0),
            episode_open=bool(row["episode_open"]),
            recovered_at=row["recovered_at"],
            promoted_issue_id=row["promoted_issue_id"],
            latest_payload=payload,
        )

    @staticmethod
    def _row_to_timer(row: sqlite3.Row) -> TimerRecord:
        return TimerRecord(
            id=int(row["id"]),
            dedup_key=str(row["dedup_key"]),
            issue_id=row["issue_id"],
            kind=str(row["kind"]),
            fire_at=str(row["fire_at"]),
            prompt=str(row["prompt"] or ""),
            created_by=str(row["created_by"] or "agent"),
            status=str(row["status"]),
            created_at=str(row["created_at"]),
            fired_at=row["fired_at"],
        )

    def register_if_new(
        self,
        *,
        issue_id: str,
        short_id: str,
        project_slug: str,
        github_repo: str,
        default_branch: str,
        title: str,
        permalink: str,
        task_kind: str = "sentry",
        base_ref: str | None = None,
        target_branch: str | None = None,
        model_override: str | None = None,
        banner_note: str | None = None,
        thread_ts: str | None = None,
        channel_id: str | None = None,
        dedup_key: str | None = None,
        last_event_meta: str | None = None,
    ) -> IssueRecord | None:
        """Insert a newly discovered issue or task. Returns None if already tracked."""
        now = self._now()
        with self._connect() as conn:
            cur = conn.execute(
                """
                INSERT OR IGNORE INTO issues (
                    issue_id, short_id, project_slug, github_repo, default_branch,
                    title, permalink, status, turn, thread_ts, channel_id, updated_at,
                    task_kind, base_ref, target_branch, model_override, banner_note,
                    dedup_key, occurrence_count, last_seen_at, last_event_meta
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)
                """,
                (
                    issue_id,
                    short_id,
                    project_slug,
                    github_repo,
                    default_branch,
                    title,
                    permalink,
                    IssueStatus.QUEUED.value,
                    thread_ts,
                    channel_id,
                    now,
                    task_kind,
                    base_ref,
                    target_branch,
                    model_override,
                    banner_note,
                    dedup_key,
                    now,
                    last_event_meta,
                ),
            )
            if cur.rowcount == 0:
                return None
            row = conn.execute("SELECT * FROM issues WHERE issue_id = ?", (issue_id,)).fetchone()
            return self._row_to_record(row)

    def get_by_issue_id(self, issue_id: str) -> IssueRecord | None:
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM issues WHERE issue_id = ?", (issue_id,)).fetchone()
            return self._row_to_record(row) if row else None

    def get_by_thread_ts(self, thread_ts: str) -> IssueRecord | None:
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM issues WHERE thread_ts = ?", (thread_ts,)).fetchone()
            return self._row_to_record(row) if row else None

    def get_by_short_id(self, short_id: str) -> IssueRecord | None:
        clean = short_id.strip()
        if not clean:
            return None
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM issues WHERE UPPER(short_id) = UPPER(?) ORDER BY updated_at DESC LIMIT 1",
                (clean,),
            ).fetchone()
            return self._row_to_record(row) if row else None

    def list_recent_issues(
        self,
        *,
        task_kind: str = "",
        github_repo: str = "",
        short_id_prefix: str = "",
        limit: int = 20,
    ) -> list[IssueRecord]:
        clauses: list[str] = []
        params: list[Any] = []
        if task_kind.strip():
            clauses.append("task_kind = ?")
            params.append(task_kind.strip())
        if github_repo.strip():
            clauses.append("github_repo = ?")
            params.append(github_repo.strip())
        if short_id_prefix.strip():
            clauses.append("UPPER(short_id) LIKE UPPER(?)")
            params.append(f"{short_id_prefix.strip()}%")
        where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        clamped_limit = max(1, min(100, int(limit)))
        with self._connect() as conn:
            rows = conn.execute(
                f"SELECT * FROM issues {where_sql} ORDER BY updated_at DESC, rowid DESC LIMIT ?",
                (*params, clamped_limit),
            ).fetchall()
            return [self._row_to_record(r) for r in rows]

    def count_running(self) -> int:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT COUNT(*) AS n FROM issues WHERE status = ?",
                (IssueStatus.RUNNING.value,),
            ).fetchone()
            return int(row["n"])

    def status_counts(self) -> dict[str, int]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT status, COUNT(*) AS n FROM issues GROUP BY status"
            ).fetchall()
            return {str(row["status"]): int(row["n"]) for row in rows}

    def list_by_status(self, status: IssueStatus) -> list[IssueRecord]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM issues WHERE status = ? ORDER BY updated_at ASC",
                (status.value,),
            ).fetchall()
            return [self._row_to_record(r) for r in rows]

    def bump_turn_for_feedback(
        self,
        issue_id: str,
        *,
        model_override: str | None = None,
        reset_auto_followups: bool = True,
    ) -> IssueRecord:
        now = self._now()
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE issues
                SET turn = turn + 1,
                    status = ?,
                    model_override = COALESCE(?, model_override),
                    auto_followup_count = CASE WHEN ? THEN 0 ELSE auto_followup_count END,
                    last_error = NULL,
                    updated_at = ?
                WHERE issue_id = ?
                """,
                (
                    IssueStatus.QUEUED.value,
                    model_override,
                    1 if reset_auto_followups else 0,
                    now,
                    issue_id,
                ),
            )
            row = conn.execute("SELECT * FROM issues WHERE issue_id = ?", (issue_id,)).fetchone()
            if row is None:
                raise KeyError(f"Unknown issue_id: {issue_id}")
            return self._row_to_record(row)

    def mark_status(
        self,
        issue_id: str,
        status: IssueStatus,
        *,
        conversation_id: str | None = None,
        thread_ts: str | None = None,
        channel_id: str | None = None,
        pr_url: str | None = None,
        last_error: str | None = None,
        banner_note: str | None = None,
        model_override: str | None = None,
        target_branch: str | None = None,
        resolved_ref: str | None = None,
        affected_files: list[str] | None = None,
        increment_auto_followup: bool = False,
    ) -> IssueRecord:
        now = self._now()
        files_json = json.dumps(affected_files) if affected_files is not None else None
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE issues
                SET status = ?,
                    conversation_id = COALESCE(?, conversation_id),
                    thread_ts = COALESCE(?, thread_ts),
                    channel_id = COALESCE(?, channel_id),
                    pr_url = COALESCE(?, pr_url),
                    last_error = ?,
                    banner_note = COALESCE(?, banner_note),
                    model_override = COALESCE(?, model_override),
                    target_branch = COALESCE(?, target_branch),
                    resolved_ref = COALESCE(?, resolved_ref),
                    affected_files_json = COALESCE(?, affected_files_json),
                    auto_followup_count = auto_followup_count + ?,
                    updated_at = ?
                WHERE issue_id = ?
                """,
                (
                    status.value,
                    conversation_id,
                    thread_ts,
                    channel_id,
                    pr_url,
                    last_error,
                    banner_note,
                    model_override,
                    target_branch,
                    resolved_ref,
                    files_json,
                    1 if increment_auto_followup else 0,
                    now,
                    issue_id,
                ),
            )
            row = conn.execute("SELECT * FROM issues WHERE issue_id = ?", (issue_id,)).fetchone()
            if row is None:
                raise KeyError(f"Unknown issue_id: {issue_id}")
            return self._row_to_record(row)

    def get_latest_by_dedup_key(self, dedup_key: str) -> IssueRecord | None:
        clean = (dedup_key or "").strip()
        if not clean:
            return None
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT * FROM issues
                WHERE dedup_key = ?
                ORDER BY updated_at DESC, rowid DESC
                LIMIT 1
                """,
                (clean,),
            ).fetchone()
            return self._row_to_record(row) if row else None

    def record_duplicate_hit(
        self,
        issue_id: str,
        *,
        last_event_meta: str | None = None,
        permalink: str | None = None,
        base_ref: str | None = None,
        now_iso: str | None = None,
    ) -> IssueRecord:
        ts = now_iso or self._now()
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE issues
                SET occurrence_count = occurrence_count + 1,
                    last_seen_at = ?,
                    last_event_meta = COALESCE(?, last_event_meta),
                    permalink = CASE
                        WHEN ? IS NOT NULL AND ? != '' THEN ?
                        ELSE permalink
                    END,
                    base_ref = COALESCE(?, base_ref)
                WHERE issue_id = ?
                """,
                (ts, last_event_meta, permalink, permalink, permalink, base_ref, issue_id),
            )
            row = conn.execute("SELECT * FROM issues WHERE issue_id = ?", (issue_id,)).fetchone()
            if row is None:
                raise KeyError(f"Unknown issue_id: {issue_id}")
            return self._row_to_record(row)

    def reopen_for_regression(
        self,
        issue_id: str,
        *,
        banner_note: str | None = None,
        last_event_meta: str | None = None,
        permalink: str | None = None,
        base_ref: str | None = None,
    ) -> IssueRecord:
        now = self._now()
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE issues
                SET turn = turn + 1,
                    status = ?,
                    occurrence_count = occurrence_count + 1,
                    last_seen_at = ?,
                    last_event_meta = COALESCE(?, last_event_meta),
                    banner_note = COALESCE(?, banner_note),
                    permalink = CASE
                        WHEN ? IS NOT NULL AND ? != '' THEN ?
                        ELSE permalink
                    END,
                    base_ref = COALESCE(?, base_ref),
                    auto_followup_count = 0,
                    last_error = NULL,
                    updated_at = ?
                WHERE issue_id = ?
                """,
                (
                    IssueStatus.QUEUED.value,
                    now,
                    last_event_meta,
                    banner_note,
                    permalink,
                    permalink,
                    permalink,
                    base_ref,
                    now,
                    issue_id,
                ),
            )
            row = conn.execute("SELECT * FROM issues WHERE issue_id = ?", (issue_id,)).fetchone()
            if row is None:
                raise KeyError(f"Unknown issue_id: {issue_id}")
            return self._row_to_record(row)

    def find_overlapping_open_issues(
        self,
        github_repo: str,
        affected_files: list[str],
        *,
        exclude_issue_id: str,
    ) -> list[tuple[IssueRecord, list[str]]]:
        norm_targets = {f.strip().lstrip("./") for f in affected_files if f.strip()}
        if not norm_targets:
            return []
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT * FROM issues
                WHERE github_repo = ?
                  AND issue_id != ?
                  AND status IN (?, ?)
                ORDER BY updated_at DESC
                """,
                (
                    github_repo,
                    exclude_issue_id,
                    IssueStatus.PR_OPENED.value,
                    IssueStatus.AWAITING_REVIEW.value,
                ),
            ).fetchall()
        matches: list[tuple[IssueRecord, list[str]]] = []
        for row in rows:
            rec = self._row_to_record(row)
            rec_files = {f.strip().lstrip("./") for f in rec.affected_files if f.strip()}
            overlap = sorted(norm_targets & rec_files)
            if overlap:
                matches.append((rec, overlap))
        return matches

    def append_pending_feedback(
        self,
        issue_id: str,
        text: str,
        *,
        model_override: str | None = None,
    ) -> int:
        clean = text.strip()
        with self._connect() as conn:
            row = conn.execute(
                "SELECT pending_feedback_json FROM issues WHERE issue_id = ?",
                (issue_id,),
            ).fetchone()
            if row is None:
                raise KeyError(f"Unknown issue_id: {issue_id}")
            items = self._parse_str_list(row["pending_feedback_json"])
            if clean:
                items.append(clean)
            conn.execute(
                """
                UPDATE issues
                SET pending_feedback_json = ?,
                    model_override = COALESCE(?, model_override),
                    updated_at = ?
                WHERE issue_id = ?
                """,
                (json.dumps(items), model_override, self._now(), issue_id),
            )
            return len(items)

    def pop_pending_feedback(self, issue_id: str) -> list[str]:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT pending_feedback_json FROM issues WHERE issue_id = ?",
                (issue_id,),
            ).fetchone()
            if row is None:
                return []
            items = self._parse_str_list(row["pending_feedback_json"])
            if items:
                conn.execute(
                    "UPDATE issues SET pending_feedback_json = '[]' WHERE issue_id = ?",
                    (issue_id,),
                )
            return items

    def observe_signals_batch(
        self,
        source_kind: str,
        active_payloads: dict[str, dict[str, Any]],
        *,
        recovery_polls: int = 3,
        now_iso: str | None = None,
    ) -> dict[str, SignalObservationRecord]:
        """Record one poll snapshot for `source_kind`, advancing hit/miss streaks & recovery."""
        ts = now_iso or self._now()
        result: dict[str, SignalObservationRecord] = {}
        with self._connect() as conn:
            open_rows = conn.execute(
                "SELECT * FROM signal_observations WHERE source_kind = ? AND episode_open = 1",
                (source_kind,),
            ).fetchall()
            open_keys = {str(r["dedup_key"]): r for r in open_rows}

            # Mark missing previously-open signals toward recovery
            for key, row in open_keys.items():
                if key in active_payloads:
                    continue
                misses = int(row["consecutive_misses"] or 0) + 1
                if misses >= max(1, recovery_polls):
                    conn.execute(
                        """
                        UPDATE signal_observations
                        SET consecutive_hits = 0,
                            consecutive_misses = ?,
                            episode_open = 0,
                            recovered_at = ?
                        WHERE dedup_key = ?
                        """,
                        (misses, ts, key),
                    )
                else:
                    conn.execute(
                        """
                        UPDATE signal_observations
                        SET consecutive_hits = 0,
                            consecutive_misses = ?
                        WHERE dedup_key = ?
                        """,
                        (misses, key),
                    )

            # Upsert active signals
            for key, payload in active_payloads.items():
                payload_json = json.dumps(payload)
                existing = conn.execute(
                    "SELECT * FROM signal_observations WHERE dedup_key = ?",
                    (key,),
                ).fetchone()
                if existing is None:
                    conn.execute(
                        """
                        INSERT INTO signal_observations (
                            dedup_key, source_kind, first_seen_at, last_seen_at,
                            consecutive_hits, consecutive_misses, total_hits,
                            episode_open, latest_payload_json
                        ) VALUES (?, ?, ?, ?, 1, 0, 1, 1, ?)
                        """,
                        (key, source_kind, ts, ts, payload_json),
                    )
                else:
                    was_open = bool(existing["episode_open"])
                    hits = (int(existing["consecutive_hits"] or 0) + 1) if was_open else 1
                    first_seen = str(existing["first_seen_at"]) if was_open else ts
                    conn.execute(
                        """
                        UPDATE signal_observations
                        SET first_seen_at = ?,
                            last_seen_at = ?,
                            consecutive_hits = ?,
                            consecutive_misses = 0,
                            total_hits = total_hits + 1,
                            episode_open = 1,
                            latest_payload_json = ?
                        WHERE dedup_key = ?
                        """,
                        (first_seen, ts, hits, payload_json, key),
                    )
                fresh = conn.execute(
                    "SELECT * FROM signal_observations WHERE dedup_key = ?",
                    (key,),
                ).fetchone()
                result[key] = self._row_to_signal_obs(fresh)
        return result

    def mark_signal_promoted(self, dedup_key: str, issue_id: str) -> None:
        with self._connect() as conn:
            conn.execute(
                "UPDATE signal_observations SET promoted_issue_id = ? WHERE dedup_key = ?",
                (issue_id, dedup_key),
            )

    def get_signal_observation(self, dedup_key: str) -> SignalObservationRecord | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM signal_observations WHERE dedup_key = ?",
                (dedup_key,),
            ).fetchone()
            return self._row_to_signal_obs(row) if row else None

    def schedule_timer(
        self,
        *,
        dedup_key: str,
        fire_at: str,
        prompt: str,
        issue_id: str | None = None,
        kind: str = "followup",
        created_by: str = "agent",
    ) -> TimerRecord:
        """Create or replace a pending timer keyed by `dedup_key` so timers never stack."""
        now = self._now()
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE timers
                SET status = 'superseded'
                WHERE dedup_key = ? AND status = 'pending'
                """,
                (dedup_key,),
            )
            cur = conn.execute(
                """
                INSERT INTO timers (
                    dedup_key, issue_id, kind, fire_at, prompt, created_by, status, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, 'pending', ?)
                """,
                (dedup_key, issue_id, kind, fire_at, prompt.strip(), created_by, now),
            )
            row = conn.execute("SELECT * FROM timers WHERE id = ?", (cur.lastrowid,)).fetchone()
            return self._row_to_timer(row)

    def pop_due_timers(self, now_iso: str | None = None) -> list[TimerRecord]:
        cutoff = now_iso or self._now()
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT * FROM timers
                WHERE status = 'pending' AND fire_at <= ?
                ORDER BY fire_at ASC, id ASC
                """,
                (cutoff,),
            ).fetchall()
            if not rows:
                return []
            ids = [int(r["id"]) for r in rows]
            placeholders = ",".join("?" for _ in ids)
            conn.execute(
                f"UPDATE timers SET status = 'fired', fired_at = ? WHERE id IN ({placeholders})",
                [cutoff, *ids],
            )
            updated = conn.execute(
                f"SELECT * FROM timers WHERE id IN ({placeholders}) ORDER BY fire_at ASC, id ASC",
                ids,
            ).fetchall()
            return [self._row_to_timer(r) for r in updated]

    def cancel_timers_for_issue(self, issue_id: str) -> int:
        with self._connect() as conn:
            cur = conn.execute(
                """
                UPDATE timers
                SET status = 'cancelled'
                WHERE issue_id = ? AND status = 'pending'
                """,
                (issue_id,),
            )
            return int(cur.rowcount or 0)

    def cancel_timer_by_id(self, timer_id: int) -> bool:
        with self._connect() as conn:
            cur = conn.execute(
                "UPDATE timers SET status = 'cancelled' WHERE id = ? AND status = 'pending'",
                (timer_id,),
            )
            return bool(cur.rowcount)

    def list_pending_timers(self) -> list[TimerRecord]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM timers WHERE status = 'pending' ORDER BY fire_at ASC, id ASC"
            ).fetchall()
            return [self._row_to_timer(r) for r in rows]

    def set_channel_binding(self, role: str, channel_id: str) -> None:
        now = self._now()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO channel_bindings (role, channel_id, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(role) DO UPDATE SET
                    channel_id = excluded.channel_id,
                    updated_at = excluded.updated_at
                """,
                (role.strip().lower(), channel_id.strip(), now),
            )

    def get_channel_bindings(self) -> dict[str, str]:
        with self._connect() as conn:
            rows = conn.execute("SELECT role, channel_id FROM channel_bindings").fetchall()
            return {str(r["role"]): str(r["channel_id"]) for r in rows}

    def prior_conversation_tokens(self, conversation_id: str) -> tuple[int, int, int, int]:
        if not conversation_id.strip():
            return (0, 0, 0, 0)
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT
                    COALESCE(SUM(prompt_tokens), 0) AS p,
                    COALESCE(SUM(cached_tokens), 0) AS c,
                    COALESCE(SUM(output_tokens), 0) AS o,
                    COALESCE(SUM(thoughts_tokens), 0) AS t
                FROM usage_ledger
                WHERE conversation_id = ?
                """,
                (conversation_id,),
            ).fetchone()
            return (int(row["p"]), int(row["c"]), int(row["o"]), int(row["t"]))

    def record_usage_entry(
        self,
        *,
        cycle_key: str,
        issue_id: str,
        turn: int,
        conversation_id: str | None,
        task_kind: str,
        model: str,
        prompt_tokens: int,
        cached_tokens: int,
        output_tokens: int,
        thoughts_tokens: int,
        cost_usd: float,
        note: str | None = None,
        recorded_at: str | None = None,
    ) -> None:
        ts = recorded_at or self._now()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO usage_ledger (
                    recorded_at, cycle_key, issue_id, turn, conversation_id,
                    task_kind, model, prompt_tokens, cached_tokens,
                    output_tokens, thoughts_tokens, cost_usd, note
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    ts,
                    cycle_key,
                    issue_id,
                    turn,
                    conversation_id,
                    task_kind,
                    model,
                    prompt_tokens,
                    cached_tokens,
                    output_tokens,
                    thoughts_tokens,
                    float(cost_usd),
                    note,
                ),
            )

    def get_cycle_usage_summary(self, cycle_key: str) -> dict[str, Any]:
        with self._connect() as conn:
            totals = conn.execute(
                """
                SELECT
                    COALESCE(SUM(cost_usd), 0.0) AS spent_usd,
                    COALESCE(SUM(prompt_tokens), 0) AS prompt_tokens,
                    COALESCE(SUM(cached_tokens), 0) AS cached_tokens,
                    COALESCE(SUM(output_tokens), 0) AS output_tokens,
                    COALESCE(SUM(thoughts_tokens), 0) AS thoughts_tokens,
                    COUNT(CASE WHEN task_kind != 'manual_sync' THEN 1 END) AS job_count
                FROM usage_ledger
                WHERE cycle_key = ?
                """,
                (cycle_key,),
            ).fetchone()
            task_rows = conn.execute(
                """
                SELECT task_kind, COALESCE(SUM(cost_usd), 0.0) AS usd
                FROM usage_ledger
                WHERE cycle_key = ?
                GROUP BY task_kind
                ORDER BY usd DESC
                """,
                (cycle_key,),
            ).fetchall()
            model_rows = conn.execute(
                """
                SELECT model, COALESCE(SUM(cost_usd), 0.0) AS usd
                FROM usage_ledger
                WHERE cycle_key = ?
                GROUP BY model
                ORDER BY usd DESC
                """,
                (cycle_key,),
            ).fetchall()
            return {
                "cycle_key": cycle_key,
                "spent_usd": float(totals["spent_usd"]),
                "prompt_tokens": int(totals["prompt_tokens"]),
                "cached_tokens": int(totals["cached_tokens"]),
                "output_tokens": int(totals["output_tokens"]),
                "thoughts_tokens": int(totals["thoughts_tokens"]),
                "job_count": int(totals["job_count"]),
                "by_task_kind": {str(r["task_kind"]): float(r["usd"]) for r in task_rows},
                "by_model": {str(r["model"]): float(r["usd"]) for r in model_rows},
            }

    def has_billing_alert(self, cycle_key: str, alert_key: str) -> bool:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT 1 FROM billing_alerts WHERE cycle_key = ? AND alert_key = ?",
                (cycle_key, alert_key),
            ).fetchone()
            return row is not None

    def mark_billing_alert(self, cycle_key: str, alert_key: str) -> None:
        now = self._now()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT OR IGNORE INTO billing_alerts (cycle_key, alert_key, sent_at)
                VALUES (?, ?, ?)
                """,
                (cycle_key, alert_key, now),
            )

    def set_kv(self, key: str, value: str) -> None:
        now = self._now()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO kv_settings (key, value, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(key) DO UPDATE SET
                    value = excluded.value,
                    updated_at = excluded.updated_at
                """,
                (key, value, now),
            )

    def get_kv(self, key: str) -> str | None:
        with self._connect() as conn:
            row = conn.execute("SELECT value FROM kv_settings WHERE key = ?", (key,)).fetchone()
            return str(row["value"]) if row else None

    @staticmethod
    def _row_to_pwa_thread(row: sqlite3.Row) -> PwaThreadRecord:
        try:
            tags = [str(t) for t in json.loads(row["tags_json"] or "[]")]
        except (ValueError, TypeError):
            tags = []
        return PwaThreadRecord(
            thread_id=str(row["thread_id"]),
            thread_ts=row["thread_ts"],
            kind=str(row["kind"]),
            title=str(row["title"]),
            subtitle=str(row["subtitle"] or ""),
            status=str(row["status"]),
            folder=str(row["folder"] or "inbox"),
            pinned=bool(row["pinned"]),
            unread_count=int(row["unread_count"] or 0),
            tags=tags,
            model=str(row["model"] or "gemini-4-hydrogen"),
            conversation_id=row["conversation_id"],
            issue_id=row["issue_id"],
            food_entry_id=int(row["food_entry_id"]) if row["food_entry_id"] is not None else None,
            last_message_preview=str(row["last_message_preview"] or ""),
            created_at=str(row["created_at"]),
            updated_at=str(row["updated_at"]),
        )

    @staticmethod
    def _row_to_pwa_message(row: sqlite3.Row) -> PwaMessageRecord:
        try:
            raw_payload = json.loads(row["payload_json"] or "{}")
            payload = raw_payload if isinstance(raw_payload, dict) else {}
        except (ValueError, TypeError):
            payload = {}
        return PwaMessageRecord(
            id=int(row["id"]),
            thread_id=str(row["thread_id"]),
            external_ts=row["external_ts"],
            sender=str(row["sender"]),
            body_md=str(row["body_md"]),
            payload=payload,
            created_at=str(row["created_at"]),
            updated_at=str(row["updated_at"]),
        )

    @staticmethod
    def _row_to_push_sub(row: sqlite3.Row) -> PushSubscriptionRecord:
        try:
            raw_prefs = json.loads(row["prefs_json"] or "{}")
            prefs = (
                {str(k): bool(v) for k, v in raw_prefs.items()}
                if isinstance(raw_prefs, dict)
                else {}
            )
        except (ValueError, TypeError):
            prefs = {}
        return PushSubscriptionRecord(
            endpoint=str(row["endpoint"]),
            p256dh=str(row["p256dh"]),
            auth=str(row["auth"]),
            user_agent=str(row["user_agent"] or ""),
            prefs=prefs,
            created_at=str(row["created_at"]),
            last_sent_at=row["last_sent_at"],
        )

    @staticmethod
    def _preview_text(text: str) -> str:
        collapsed = " ".join((text or "").split())
        if len(collapsed) <= 160:
            return collapsed
        return collapsed[:157].rstrip() + "..."

    def upsert_pwa_thread(
        self,
        *,
        thread_id: str,
        kind: str,
        title: str,
        subtitle: str | None = None,
        status: str | None = None,
        thread_ts: str | None = None,
        folder: str | None = None,
        pinned: bool | None = None,
        tags: list[str] | None = None,
        model: str | None = None,
        conversation_id: str | None = None,
        issue_id: str | None = None,
        food_entry_id: int | None = None,
        last_message_preview: str | None = None,
        increment_unread: bool = False,
        unarchive_on_activity: bool = True,
    ) -> PwaThreadRecord:
        now = self._now()
        clean_ts = thread_ts.strip() if thread_ts and thread_ts.strip() else None
        clean_conv = (
            conversation_id.strip() if conversation_id and conversation_id.strip() else None
        )
        clean_issue = issue_id.strip() if issue_id and issue_id.strip() else None
        clean_preview = (
            self._preview_text(last_message_preview) if last_message_preview is not None else None
        )

        with self._connect() as conn:
            existing = conn.execute(
                "SELECT * FROM pwa_threads WHERE thread_id = ?", (thread_id,)
            ).fetchone()
            if existing is None:
                conn.execute(
                    """
                    INSERT INTO pwa_threads (
                        thread_id, thread_ts, kind, title, subtitle, status, folder, pinned,
                        unread_count, tags_json, model, conversation_id, issue_id, food_entry_id,
                        last_message_preview, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        thread_id,
                        clean_ts,
                        kind,
                        title.strip() or thread_id,
                        (subtitle or "").strip(),
                        status or "active",
                        folder or "inbox",
                        1 if pinned else 0,
                        1 if increment_unread else 0,
                        json.dumps(tags or []),
                        model or "gemini-4-hydrogen",
                        clean_conv,
                        clean_issue,
                        food_entry_id,
                        clean_preview or "",
                        now,
                        now,
                    ),
                )
            else:
                if clean_ts and clean_ts != existing["thread_ts"]:
                    conflict = conn.execute(
                        "SELECT thread_id FROM pwa_threads WHERE thread_ts = ? AND thread_id != ?",
                        (clean_ts, thread_id),
                    ).fetchone()
                    if conflict is not None:
                        clean_ts = existing["thread_ts"]

                next_folder = (
                    folder
                    if folder is not None
                    else ("inbox" if unarchive_on_activity else str(existing["folder"]))
                )
                next_unread = int(existing["unread_count"] or 0) + (1 if increment_unread else 0)
                conn.execute(
                    """
                    UPDATE pwa_threads
                    SET thread_ts = COALESCE(?, thread_ts),
                        kind = ?,
                        title = COALESCE(?, title),
                        subtitle = COALESCE(?, subtitle),
                        status = COALESCE(?, status),
                        folder = ?,
                        pinned = COALESCE(?, pinned),
                        unread_count = ?,
                        tags_json = COALESCE(?, tags_json),
                        model = COALESCE(?, model),
                        conversation_id = COALESCE(?, conversation_id),
                        issue_id = COALESCE(?, issue_id),
                        food_entry_id = COALESCE(?, food_entry_id),
                        last_message_preview = COALESCE(?, last_message_preview),
                        updated_at = ?
                    WHERE thread_id = ?
                    """,
                    (
                        clean_ts,
                        kind or str(existing["kind"]),
                        title.strip() if title and title.strip() else None,
                        subtitle.strip() if subtitle is not None else None,
                        status,
                        next_folder,
                        (1 if pinned else 0) if pinned is not None else None,
                        next_unread,
                        json.dumps(tags) if tags is not None else None,
                        model,
                        clean_conv,
                        clean_issue,
                        food_entry_id,
                        clean_preview,
                        now,
                        thread_id,
                    ),
                )
            row = conn.execute(
                "SELECT * FROM pwa_threads WHERE thread_id = ?", (thread_id,)
            ).fetchone()
            return self._row_to_pwa_thread(row)

    def get_pwa_thread(self, thread_id: str) -> PwaThreadRecord | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM pwa_threads WHERE thread_id = ?", (thread_id,)
            ).fetchone()
            return self._row_to_pwa_thread(row) if row else None

    def get_pwa_thread_by_ts(self, thread_ts: str) -> PwaThreadRecord | None:
        clean = (thread_ts or "").strip()
        if not clean:
            return None
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM pwa_threads WHERE thread_ts = ?", (clean,)).fetchone()
            return self._row_to_pwa_thread(row) if row else None

    def get_pwa_thread_by_issue(self, issue_id: str) -> PwaThreadRecord | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM pwa_threads WHERE issue_id = ? OR thread_id = ? LIMIT 1",
                (issue_id, issue_id),
            ).fetchone()
            return self._row_to_pwa_thread(row) if row else None

    def list_pwa_threads(
        self,
        *,
        folder: str = "inbox",
        filter_mode: str = "all",
        kind: str = "",
        exclude_kinds: tuple[str, ...] = (),
        query: str = "",
        limit: int = 100,
    ) -> list[PwaThreadRecord]:
        clauses: list[str] = []
        params: list[Any] = []

        effective_folder = "archived" if filter_mode == "archived" else folder
        if effective_folder in ("inbox", "archived"):
            clauses.append("folder = ?")
            params.append(effective_folder)

        if filter_mode == "needs_review":
            clauses.append("status = 'awaiting_review'")
        elif filter_mode == "running":
            clauses.append("status IN ('queued', 'running')")
        elif filter_mode == "chats":
            clauses.append("kind = 'chat'")

        if kind and kind != "all":
            clauses.append("kind = ?")
            params.append(kind)
        elif exclude_kinds:
            placeholders = ", ".join("?" for _ in exclude_kinds)
            clauses.append(f"kind NOT IN ({placeholders})")
            params.extend(exclude_kinds)

        if query.strip():
            like = f"%{query.strip()}%"
            clauses.append(
                "(title LIKE ? OR subtitle LIKE ? OR last_message_preview LIKE ? OR tags_json LIKE ?)"
            )
            params.extend([like, like, like, like])

        where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        sql = f"""
            SELECT * FROM pwa_threads
            {where_sql}
            ORDER BY pinned DESC, updated_at DESC
            LIMIT ?
        """
        params.append(max(1, min(limit, 500)))
        with self._connect() as conn:
            rows = conn.execute(sql, params).fetchall()
            return [self._row_to_pwa_thread(r) for r in rows]

    def get_pwa_counts(self, *, exclude_kinds: tuple[str, ...] = ()) -> dict[str, int]:
        where_sql = ""
        params: list[Any] = []
        if exclude_kinds:
            placeholders = ", ".join("?" for _ in exclude_kinds)
            where_sql = f"WHERE kind NOT IN ({placeholders})"
            params.extend(exclude_kinds)
        with self._connect() as conn:
            row = conn.execute(
                f"""
                SELECT
                    COALESCE(SUM(CASE WHEN folder = 'inbox' THEN 1 ELSE 0 END), 0) AS inbox,
                    COALESCE(SUM(CASE WHEN folder = 'archived' THEN 1 ELSE 0 END), 0) AS archived,
                    COALESCE(SUM(CASE WHEN folder = 'inbox' AND unread_count > 0 THEN 1 ELSE 0 END), 0) AS unread_threads,
                    COALESCE(SUM(CASE WHEN folder = 'inbox' THEN unread_count ELSE 0 END), 0) AS unread_total,
                    COALESCE(SUM(CASE WHEN folder = 'inbox' AND status = 'awaiting_review' THEN 1 ELSE 0 END), 0) AS needs_review,
                    COALESCE(SUM(CASE WHEN status IN ('queued', 'running') THEN 1 ELSE 0 END), 0) AS running,
                    COALESCE(SUM(CASE WHEN folder = 'inbox' AND kind = 'chat' THEN 1 ELSE 0 END), 0) AS chats
                FROM pwa_threads
                {where_sql}
                """,
                params,
            ).fetchone()
            return {
                "inbox": int(row["inbox"] or 0),
                "archived": int(row["archived"] or 0),
                "unread_threads": int(row["unread_threads"] or 0),
                "unread_total": int(row["unread_total"] or 0),
                "needs_review": int(row["needs_review"] or 0),
                "running": int(row["running"] or 0),
                "chats": int(row["chats"] or 0),
            }

    def update_pwa_thread_org(
        self,
        thread_id: str,
        *,
        folder: str | None = None,
        pinned: bool | None = None,
        mark_read: bool = False,
        mark_unread: bool = False,
        tags: list[str] | None = None,
        title: str | None = None,
        status: str | None = None,
        model: str | None = None,
        conversation_id: str | None = None,
    ) -> PwaThreadRecord | None:
        with self._connect() as conn:
            existing = conn.execute(
                "SELECT * FROM pwa_threads WHERE thread_id = ?", (thread_id,)
            ).fetchone()
            if existing is None:
                return None

            next_unread = int(existing["unread_count"] or 0)
            if mark_read:
                next_unread = 0
            elif mark_unread:
                next_unread = max(1, next_unread)

            next_folder = (
                folder if folder in ("inbox", "archived") else str(existing["folder"] or "inbox")
            )
            next_pinned = (
                (1 if pinned else 0) if pinned is not None else int(existing["pinned"] or 0)
            )
            next_tags = json.dumps(tags) if tags is not None else str(existing["tags_json"] or "[]")
            next_title = title.strip() if title and title.strip() else str(existing["title"])
            next_status = status if status is not None else str(existing["status"])
            next_model = model if model is not None else str(existing["model"])
            next_conv = (
                conversation_id if conversation_id is not None else existing["conversation_id"]
            )
            now = self._now()

            conn.execute(
                """
                UPDATE pwa_threads
                SET folder = ?, pinned = ?, unread_count = ?, tags_json = ?, title = ?,
                    status = ?, model = ?, conversation_id = ?, updated_at = ?
                WHERE thread_id = ?
                """,
                (
                    next_folder,
                    next_pinned,
                    next_unread,
                    next_tags,
                    next_title,
                    next_status,
                    next_model,
                    next_conv,
                    now,
                    thread_id,
                ),
            )
            row = conn.execute(
                "SELECT * FROM pwa_threads WHERE thread_id = ?", (thread_id,)
            ).fetchone()
            return self._row_to_pwa_thread(row) if row else None

    def append_pwa_message(
        self,
        *,
        thread_id: str,
        sender: str,
        body_md: str,
        external_ts: str | None = None,
        payload: dict[str, Any] | None = None,
        increment_unread: bool = False,
        unarchive_on_activity: bool = True,
    ) -> PwaMessageRecord:
        now = self._now()
        clean_ts = external_ts.strip() if external_ts and external_ts.strip() else None
        payload_json = json.dumps(payload or {})
        preview = self._preview_text(body_md)

        with self._connect() as conn:
            cur = conn.execute(
                """
                INSERT INTO pwa_messages (
                    thread_id, external_ts, sender, body_md, payload_json, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (thread_id, clean_ts, sender, body_md, payload_json, now, now),
            )
            conn.execute(
                """
                UPDATE pwa_threads
                SET last_message_preview = ?,
                    unread_count = unread_count + ?,
                    folder = CASE WHEN ? THEN 'inbox' ELSE folder END,
                    updated_at = ?
                WHERE thread_id = ?
                """,
                (
                    preview,
                    1 if increment_unread else 0,
                    1 if unarchive_on_activity else 0,
                    now,
                    thread_id,
                ),
            )
            row = conn.execute(
                "SELECT * FROM pwa_messages WHERE id = ?", (cur.lastrowid,)
            ).fetchone()
            return self._row_to_pwa_message(row)

    def update_pwa_message_by_ts(
        self,
        *,
        external_ts: str,
        body_md: str,
        payload: dict[str, Any] | None = None,
    ) -> PwaMessageRecord | None:
        clean_ts = (external_ts or "").strip()
        if not clean_ts:
            return None
        now = self._now()
        preview = self._preview_text(body_md)
        with self._connect() as conn:
            existing = conn.execute(
                "SELECT * FROM pwa_messages WHERE external_ts = ? ORDER BY id DESC LIMIT 1",
                (clean_ts,),
            ).fetchone()
            if existing is None:
                return None

            merged_payload: dict[str, Any]
            if payload is None:
                try:
                    merged_payload = json.loads(existing["payload_json"] or "{}")
                except (ValueError, TypeError):
                    merged_payload = {}
            else:
                merged_payload = payload

            conn.execute(
                """
                UPDATE pwa_messages
                SET body_md = ?, payload_json = ?, updated_at = ?
                WHERE id = ?
                """,
                (body_md, json.dumps(merged_payload), now, int(existing["id"])),
            )
            conn.execute(
                """
                UPDATE pwa_threads
                SET last_message_preview = ?, updated_at = ?
                WHERE thread_id = ?
                """,
                (preview, now, str(existing["thread_id"])),
            )
            row = conn.execute(
                "SELECT * FROM pwa_messages WHERE id = ?", (int(existing["id"]),)
            ).fetchone()
            return self._row_to_pwa_message(row) if row else None

    def add_or_remove_pwa_reaction(
        self,
        *,
        external_ts: str,
        name: str,
        remove: bool = False,
    ) -> PwaMessageRecord | None:
        clean_ts = (external_ts or "").strip()
        if not clean_ts:
            return None
        with self._connect() as conn:
            existing = conn.execute(
                "SELECT * FROM pwa_messages WHERE external_ts = ? ORDER BY id DESC LIMIT 1",
                (clean_ts,),
            ).fetchone()
            if existing is None:
                return None
            try:
                payload = json.loads(existing["payload_json"] or "{}")
                if not isinstance(payload, dict):
                    payload = {}
            except (ValueError, TypeError):
                payload = {}
            reactions = [str(r) for r in payload.get("reactions", [])]
            if remove:
                reactions = [r for r in reactions if r != name]
            elif name not in reactions:
                reactions.append(name)
            payload["reactions"] = reactions
            now = self._now()
            conn.execute(
                "UPDATE pwa_messages SET payload_json = ?, updated_at = ? WHERE id = ?",
                (json.dumps(payload), now, int(existing["id"])),
            )
            row = conn.execute(
                "SELECT * FROM pwa_messages WHERE id = ?", (int(existing["id"]),)
            ).fetchone()
            return self._row_to_pwa_message(row) if row else None

    def list_pwa_messages(self, thread_id: str, limit: int = 200) -> list[PwaMessageRecord]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM pwa_messages WHERE thread_id = ? ORDER BY id ASC LIMIT ?",
                (thread_id, max(1, min(limit, 1000))),
            ).fetchall()
            return [self._row_to_pwa_message(r) for r in rows]

    def upsert_push_subscription(
        self,
        *,
        endpoint: str,
        p256dh: str,
        auth: str,
        user_agent: str = "",
        prefs: dict[str, bool] | None = None,
    ) -> PushSubscriptionRecord:
        now = self._now()
        prefs_json = json.dumps(prefs or {})
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO pwa_push_subscriptions (
                    endpoint, p256dh, auth, user_agent, prefs_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(endpoint) DO UPDATE SET
                    p256dh = excluded.p256dh,
                    auth = excluded.auth,
                    user_agent = CASE
                        WHEN excluded.user_agent != '' THEN excluded.user_agent
                        ELSE pwa_push_subscriptions.user_agent
                    END,
                    prefs_json = CASE
                        WHEN excluded.prefs_json != '{}' THEN excluded.prefs_json
                        ELSE pwa_push_subscriptions.prefs_json
                    END
                """,
                (endpoint, p256dh, auth, user_agent[:250], prefs_json, now),
            )
            row = conn.execute(
                "SELECT * FROM pwa_push_subscriptions WHERE endpoint = ?", (endpoint,)
            ).fetchone()
            return self._row_to_push_sub(row)

    def delete_push_subscription(self, endpoint: str) -> bool:
        with self._connect() as conn:
            cur = conn.execute("DELETE FROM pwa_push_subscriptions WHERE endpoint = ?", (endpoint,))
            return cur.rowcount > 0

    def update_push_prefs(
        self, endpoint: str, prefs: dict[str, bool]
    ) -> PushSubscriptionRecord | None:
        clean_prefs = {str(k): bool(v) for k, v in prefs.items()}
        with self._connect() as conn:
            cur = conn.execute(
                "UPDATE pwa_push_subscriptions SET prefs_json = ? WHERE endpoint = ?",
                (json.dumps(clean_prefs), endpoint),
            )
            if cur.rowcount == 0:
                return None
            row = conn.execute(
                "SELECT * FROM pwa_push_subscriptions WHERE endpoint = ?", (endpoint,)
            ).fetchone()
            return self._row_to_push_sub(row) if row else None

    def list_push_subscriptions(self) -> list[PushSubscriptionRecord]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM pwa_push_subscriptions ORDER BY created_at DESC"
            ).fetchall()
            return [self._row_to_push_sub(r) for r in rows]

    def touch_push_sent(self, endpoint: str) -> None:
        now = self._now()
        with self._connect() as conn:
            conn.execute(
                "UPDATE pwa_push_subscriptions SET last_sent_at = ? WHERE endpoint = ?",
                (now, endpoint),
            )
