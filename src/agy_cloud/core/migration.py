from __future__ import annotations

import json
import os
import sqlite3
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path


@dataclass(slots=True)
class PvcMigrationReport:
    state_dir: str
    backup_dir: str
    state_db_backed_up: bool = False
    nutrition_db_migrated: bool = False
    meals_count: int = 0
    garmin_days_migrated: int = 0
    handoffs_imported: int = 0
    photos_symlinked: bool = False
    notes: list[str] = field(default_factory=list)


def sqlite_online_backup(src_db: Path, dst_db: Path) -> None:
    """Perform an atomic SQLite online backup that safely flushes WAL frames into `dst_db`."""
    dst_db.parent.mkdir(parents=True, exist_ok=True)
    with (
        sqlite3.connect(src_db, timeout=30.0) as src_conn,
        sqlite3.connect(dst_db, timeout=30.0) as dst_conn,
    ):
        src_conn.backup(dst_conn)


def migrate_agent_tasks_pvc(
    state_dir: Path,
    *,
    timestamp_slug: str | None = None,
) -> PvcMigrationReport:
    """Non-destructively migrate an `agent-tasks-state` PVC in-place for `agy-cloud`.

    Guarantees:
    1. Never deletes or truncates `/state/state.sqlite3`, `/state/nutrition.sqlite3`, or
       `/state/food_photos/`, preserving instant rollback to `agent-tasks-controller`.
    2. Uses SQLite online `backup()` (not raw file copy) so active `-wal` frames are
       checkpointed cleanly into `/state/backups/pre-agy-cloud-<ts>/` and plugin DBs.
    3. Populates `/state/plugins/health/nutrition.sqlite3` (full schema + data) and
       seeds `/state/plugins/garmin/garmin.sqlite3` from `garmin_daily_metrics`.
    4. Links `/state/plugins/health/media/food_photos -> ../../../food_photos` for
       zero-copy photo continuity.
    5. Imports coding/handoff tasks from `state.sqlite3` (`issues` table) into
       `/state/handoffs.sqlite3`.
    """
    ts = timestamp_slug or datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    backup_dir = state_dir / "backups" / f"pre-agy-cloud-{ts}"
    backup_dir.mkdir(parents=True, exist_ok=True)

    report = PvcMigrationReport(
        state_dir=str(state_dir),
        backup_dir=str(backup_dir),
    )

    legacy_state_db = state_dir / "state.sqlite3"
    legacy_nutrition_db = state_dir / "nutrition.sqlite3"

    # 1. Atomic pre-migration backup of legacy SQLite databases
    if legacy_state_db.is_file():
        sqlite_online_backup(legacy_state_db, backup_dir / "state.sqlite3")
        report.state_db_backed_up = True
        report.notes.append("Backed up /state/state.sqlite3 via SQLite online backup.")

    if legacy_nutrition_db.is_file():
        sqlite_online_backup(legacy_nutrition_db, backup_dir / "nutrition.sqlite3")
        report.notes.append("Backed up /state/nutrition.sqlite3 via SQLite online backup.")

        # 2. Migrate nutrition.sqlite3 -> /state/plugins/health/nutrition.sqlite3
        health_db = state_dir / "plugins" / "health" / "nutrition.sqlite3"
        should_copy_nutrition = not health_db.is_file()
        if not should_copy_nutrition:
            try:
                with (
                    sqlite3.connect(legacy_nutrition_db, timeout=15.0) as lconn,
                    sqlite3.connect(health_db, timeout=15.0) as hconn,
                ):
                    l_cnt = lconn.execute("SELECT COUNT(*) FROM meal_entries").fetchone()[0]
                    h_cnt = hconn.execute("SELECT COUNT(*) FROM meal_entries").fetchone()[0]
                    if int(l_cnt) > int(h_cnt):
                        should_copy_nutrition = True
            except sqlite3.Error:
                pass
        if should_copy_nutrition:
            sqlite_online_backup(legacy_nutrition_db, health_db)
            report.nutrition_db_migrated = True

        # Ensure `meals` view/table compatibility and count existing meal_entries
        with sqlite3.connect(health_db, timeout=30.0) as hconn:
            hconn.row_factory = sqlite3.Row
            hconn.execute("PRAGMA busy_timeout=30000;")
            tables = {
                str(r["name"])
                for r in hconn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            }
            if "meal_entries" in tables:
                row = hconn.execute("SELECT COUNT(*) AS cnt FROM meal_entries").fetchone()
                report.meals_count = int(row["cnt"]) if row else 0
                if "meals" not in tables:
                    hconn.execute(
                        """
                        CREATE TABLE IF NOT EXISTS meals (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            local_date TEXT NOT NULL,
                            title TEXT NOT NULL,
                            base_calories_kcal INTEGER NOT NULL,
                            base_protein_g REAL NOT NULL,
                            base_carbs_g REAL NOT NULL,
                            base_fat_g REAL NOT NULL,
                            scale_multiplier REAL NOT NULL DEFAULT 1.0,
                            thread_ts TEXT NOT NULL DEFAULT ''
                        );
                        """
                    )
                    hconn.execute(
                        """
                        INSERT OR IGNORE INTO meals (
                            id, local_date, title, base_calories_kcal,
                            base_protein_g, base_carbs_g, base_fat_g,
                            scale_multiplier, thread_ts
                        )
                        SELECT
                            id,
                            local_date,
                            title,
                            CAST(ROUND(total_calories_kcal / MAX(scale_factor, 0.01)) AS INTEGER),
                            ROUND(total_protein_g / MAX(scale_factor, 0.01), 1),
                            ROUND(total_carbs_g / MAX(scale_factor, 0.01), 1),
                            ROUND(total_fat_g / MAX(scale_factor, 0.01), 1),
                            scale_factor,
                            COALESCE(slack_thread_ts, '')
                        FROM meal_entries
                        WHERE status != 'deleted';
                        """
                    )

            # 3. Seed `/state/plugins/garmin/garmin.sqlite3` from `garmin_daily_metrics`
            if "garmin_daily_metrics" in tables:
                garmin_db = state_dir / "plugins" / "garmin" / "garmin.sqlite3"
                garmin_db.parent.mkdir(parents=True, exist_ok=True)
                g_rows = hconn.execute("SELECT * FROM garmin_daily_metrics").fetchall()
                weights_by_date: dict[str, float] = {}
                if "body_metrics" in tables:
                    for wrow in hconn.execute(
                        "SELECT local_date, weight_lbs FROM body_metrics"
                    ).fetchall():
                        weights_by_date[str(wrow["local_date"])] = float(wrow["weight_lbs"])

                with sqlite3.connect(garmin_db, timeout=30.0) as gconn:
                    gconn.execute("PRAGMA busy_timeout=30000;")
                    gconn.execute("PRAGMA journal_mode=WAL;")
                    gconn.execute(
                        """
                        CREATE TABLE IF NOT EXISTS garmin_daily (
                            local_date TEXT PRIMARY KEY,
                            active_kcal INTEGER NOT NULL DEFAULT 0,
                            resting_kcal INTEGER NOT NULL DEFAULT 1950,
                            steps INTEGER NOT NULL DEFAULT 0,
                            readiness_score INTEGER,
                            weight_lbs REAL
                        );
                        """
                    )
                    for grow in g_rows:
                        ldate = str(grow["local_date"])
                        active_kcal = round(float(grow["active_kcal"]))
                        resting_kcal = round(float(grow["bmr_full_day_kcal"]))
                        steps = int(grow["steps"])
                        gconn.execute(
                            """
                            INSERT OR REPLACE INTO garmin_daily (
                                local_date, active_kcal, resting_kcal, steps, readiness_score, weight_lbs
                            ) VALUES (?, ?, ?, ?, NULL, ?)
                            """,
                            (
                                ldate,
                                active_kcal,
                                resting_kcal,
                                steps,
                                weights_by_date.get(ldate),
                            ),
                        )
                        report.garmin_days_migrated += 1

    # 4. Zero-copy symlink `/state/plugins/health/media/food_photos -> ../../../food_photos`
    legacy_photos = state_dir / "food_photos"
    if legacy_photos.is_dir():
        health_media_dir = state_dir / "plugins" / "health" / "media"
        health_media_dir.mkdir(parents=True, exist_ok=True)
        link_path = health_media_dir / "food_photos"
        if not link_path.exists() and not link_path.is_symlink():
            rel_target = os.path.relpath(legacy_photos, health_media_dir)
            link_path.symlink_to(rel_target, target_is_directory=True)
        report.photos_symlinked = True

    # 5. Bridge coding/handoff records from `state.sqlite3` (`issues`) into `handoffs.sqlite3`
    if legacy_state_db.is_file():
        from agy_cloud.core.handoff import HandoffRecord, HandoffStore

        handoff_store = HandoffStore(state_dir / "handoffs.sqlite3")
        with sqlite3.connect(legacy_state_db, timeout=30.0) as sconn:
            sconn.row_factory = sqlite3.Row
            tables = {
                str(r["name"])
                for r in sconn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            }
            if "issues" in tables:
                rows = sconn.execute(
                    "SELECT * FROM issues WHERE task_kind IN ('handoff', 'slack_coding')"
                ).fetchall()
                for row in rows:
                    issue_id = str(row["issue_id"])
                    art_dir = state_dir / "artifacts" / issue_id
                    has_patch = _artifact_has_result_patch(art_dir)
                    new_status = str(row["status"]).lower()
                    new_conv_id = row["conversation_id"]
                    new_last_error = row["last_error"]
                    existing = handoff_store.get(issue_id)
                    if existing is not None:
                        if (
                            existing.status != new_status
                            or existing.last_error != new_last_error
                            or existing.has_result_patch != has_patch
                            or (new_conv_id and existing.remote_conversation_id != new_conv_id)
                        ):
                            handoff_store.update_status(
                                issue_id,
                                status=new_status,
                                remote_conversation_id=new_conv_id,
                                has_result_patch=has_patch,
                                last_error=new_last_error,
                            )
                        continue
                    handoff_store.create(
                        HandoffRecord(
                            handoff_id=issue_id,
                            short_id=str(row["short_id"]),
                            github_repo=str(row["github_repo"]),
                            base_branch=str(row["default_branch"] or "main"),
                            base_sha=str(row["base_ref"] or ""),
                            prompt=str(row["title"]),
                            semantic_state="",
                            verify_hint="",
                            focus_subdir="",
                            model=str(row["model_override"] or "gemini-4-hydrogen"),
                            status=new_status,
                            turn=max(1, int(row["turn"] or 1)),
                            remote_conversation_id=new_conv_id,
                            has_result_patch=has_patch,
                            last_error=new_last_error,
                            created_at=str(row["updated_at"]),
                            updated_at=str(row["updated_at"]),
                        )
                    )
                    report.handoffs_imported += 1

    return report


def _artifact_has_result_patch(art_dir: Path) -> bool:
    """Determine whether an artifact directory contains an actual worker-produced result patch."""
    result_file = art_dir / "result.json"
    if result_file.is_file():
        try:
            payload = json.loads(result_file.read_text(encoding="utf-8"))
            if isinstance(payload, dict) and isinstance(payload.get("has_patch"), bool):
                return payload["has_patch"]
        except (OSError, json.JSONDecodeError):
            pass

    fix_patch = art_dir / "fix.patch"
    if not fix_patch.is_file():
        return False
    fix_bytes = fix_patch.read_bytes()
    if not fix_bytes.strip():
        return False
    initial_patch = art_dir / "initial.patch"
    if initial_patch.is_file() and initial_patch.read_bytes() == fix_bytes:
        return False
    return True
