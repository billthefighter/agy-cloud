from __future__ import annotations

import gzip
import json
import re
import shutil
import sqlite3
from dataclasses import asdict
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from agent_tasks.common.config import FoodTrackerConfig
from agent_tasks.tasks.food_tracker.models import (
    VALID_MEAL_REACTIONS,
    AdaptiveTdeeEstimate,
    BodyMetricRecord,
    ConfidenceLevel,
    DailySummary,
    DynamicTargetInfo,
    EstimatedMealItem,
    GarminActivityRecord,
    GarminDailyMetric,
    MealEntryRecord,
    MealEstimateSchema,
    MealItemRecord,
    MealSlot,
    NutritionTarget,
    SavedFood,
    WeeklySummary,
)

_SLUG_CLEAN_RE = re.compile(r"[^a-z0-9]+")
_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def slugify_food_name(name: str) -> str:
    """Normalize a human food name into a compact URL/command-safe slug."""
    cleaned = _SLUG_CLEAN_RE.sub("-", name.strip().lower()).strip("-")
    return cleaned[:48] or "meal-staple"


def effective_local_date(
    now_utc: datetime | None = None,
    tz_name: str = "America/Los_Angeles",
    rollover_hour: int = 3,
) -> str:
    """Convert UTC timestamp to local food-log date with an early-morning rollover."""
    dt = now_utc or datetime.now(UTC)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    local_dt = dt.astimezone(ZoneInfo(tz_name))
    shifted = local_dt - timedelta(hours=max(0, rollover_hour))
    return shifted.date().isoformat()


def default_meal_slot_for_time(
    now_utc: datetime | None = None,
    tz_name: str = "America/Los_Angeles",
) -> MealSlot:
    """Infer a sensible default meal slot from the user's local clock hour."""
    dt = now_utc or datetime.now(UTC)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    hour = dt.astimezone(ZoneInfo(tz_name)).hour
    if hour < 5:
        return "snack"
    if hour <= 10:
        return "breakfast"
    if hour <= 15:
        return "lunch"
    if 17 <= hour <= 21:
        return "dinner"
    return "snack"


def normalize_item_macros(item: EstimatedMealItem) -> tuple[EstimatedMealItem, bool]:
    """Clamp negatives and reconcile vision/user items whose kcal diverge >25% from Atwater."""
    protein = max(0.0, float(item.protein_g))
    carbs = max(0.0, float(item.carbs_g))
    fat = max(0.0, float(item.fat_g))
    fiber = max(0.0, float(item.fiber_g))
    kcal = max(0.0, float(item.calories_kcal))
    grams = max(0.0, float(item.estimated_grams)) if item.estimated_grams is not None else None

    atwater_kcal = 4.0 * protein + 4.0 * carbs + 9.0 * fat
    reconciled = False

    if (kcal <= 0.0 and atwater_kcal > 0.0) or (
        item.source in ("vision_estimate", "user_edit")
        and atwater_kcal > 20.0
        and abs(kcal - atwater_kcal) / max(kcal, atwater_kcal) > 0.25
    ):
        kcal = round(atwater_kcal, 1)
        reconciled = True

    normalized = item.model_copy(
        update={
            "estimated_grams": round(grams, 1) if grams is not None else None,
            "calories_kcal": round(kcal, 1),
            "protein_g": round(protein, 1),
            "carbs_g": round(carbs, 1),
            "fat_g": round(fat, 1),
            "fiber_g": round(fiber, 1),
        }
    )
    return normalized, reconciled


def _sum_scaled_items(
    items: list[EstimatedMealItem] | list[MealItemRecord],
    scale_factor: float,
) -> tuple[float, float, float, float, float]:
    factor = max(0.01, float(scale_factor))
    kcal = round(sum(float(i.calories_kcal) for i in items) * factor, 1)
    protein = round(sum(float(i.protein_g) for i in items) * factor, 1)
    carbs = round(sum(float(i.carbs_g) for i in items) * factor, 1)
    fat = round(sum(float(i.fat_g) for i in items) * factor, 1)
    fiber = round(sum(float(i.fiber_g) for i in items) * factor, 1)
    return kcal, protein, carbs, fat, fiber


def resolve_valid_local_date(candidate: str | None, fallback_local_date: str) -> str:
    """Validate an optional YYYY-MM-DD override string; return fallback if invalid."""
    if not candidate:
        return fallback_local_date
    clean = candidate.strip()
    if not _DATE_RE.fullmatch(clean):
        return fallback_local_date
    try:
        date.fromisoformat(clean)
        return clean
    except ValueError:
        return fallback_local_date


class NutritionStore:
    """WAL-mode SQLite persistence for meals, targets, saved staples, body weight, and backups."""

    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=30000;")
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA foreign_keys=ON;")
        return conn

    def _init_schema(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS nutrition_targets (
                    effective_date TEXT PRIMARY KEY,
                    calories_kcal INTEGER NOT NULL,
                    protein_g REAL NOT NULL,
                    carbs_g REAL NOT NULL,
                    fat_g REAL NOT NULL,
                    fiber_g REAL NOT NULL DEFAULT 30,
                    notes TEXT,
                    updated_at TEXT NOT NULL
                );
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS saved_foods (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    slug TEXT UNIQUE NOT NULL,
                    name TEXT NOT NULL,
                    serving_desc TEXT NOT NULL,
                    serving_grams REAL,
                    calories_kcal REAL NOT NULL,
                    protein_g REAL NOT NULL,
                    carbs_g REAL NOT NULL,
                    fat_g REAL NOT NULL,
                    fiber_g REAL NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL
                );
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS meal_entries (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    logged_at TEXT NOT NULL,
                    local_date TEXT NOT NULL,
                    meal_slot TEXT NOT NULL,
                    title TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'confirmed',
                    raw_description TEXT NOT NULL DEFAULT '',
                    photo_relpath TEXT,
                    slack_channel_id TEXT,
                    slack_thread_ts TEXT UNIQUE,
                    slack_card_ts TEXT,
                    model_used TEXT NOT NULL,
                    confidence TEXT NOT NULL,
                    assumptions_note TEXT,
                    clarification_question TEXT,
                    scale_factor REAL NOT NULL DEFAULT 1.0,
                    total_calories_kcal REAL NOT NULL,
                    total_protein_g REAL NOT NULL,
                    total_carbs_g REAL NOT NULL,
                    total_fat_g REAL NOT NULL,
                    total_fiber_g REAL NOT NULL DEFAULT 0,
                    reaction_emoji TEXT,
                    reasoning_trace TEXT,
                    updated_at TEXT NOT NULL
                );
                """
            )
            meal_cols = {
                str(r["name"]) for r in conn.execute("PRAGMA table_info(meal_entries)").fetchall()
            }
            if "reaction_emoji" not in meal_cols:
                conn.execute("ALTER TABLE meal_entries ADD COLUMN reaction_emoji TEXT")
            if "reasoning_trace" not in meal_cols:
                conn.execute("ALTER TABLE meal_entries ADD COLUMN reasoning_trace TEXT")
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_meal_entries_local_date
                ON meal_entries (local_date)
                WHERE status != 'deleted';
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS meal_items (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    meal_entry_id INTEGER NOT NULL REFERENCES meal_entries(id) ON DELETE CASCADE,
                    saved_food_id INTEGER REFERENCES saved_foods(id) ON DELETE SET NULL,
                    name TEXT NOT NULL,
                    quantity_desc TEXT NOT NULL,
                    estimated_grams REAL,
                    calories_kcal REAL NOT NULL,
                    protein_g REAL NOT NULL,
                    carbs_g REAL NOT NULL,
                    fat_g REAL NOT NULL,
                    fiber_g REAL NOT NULL DEFAULT 0,
                    source TEXT NOT NULL
                );
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_meal_items_entry
                ON meal_items (meal_entry_id);
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS body_metrics (
                    local_date TEXT PRIMARY KEY,
                    weight_lbs REAL NOT NULL,
                    ema_weight_lbs REAL NOT NULL,
                    notes TEXT,
                    logged_at TEXT NOT NULL
                );
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS food_digest_log (
                    local_date TEXT PRIMARY KEY,
                    sent_at TEXT NOT NULL
                );
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS garmin_daily_metrics (
                    local_date TEXT PRIMARY KEY,
                    bmr_raw_kcal REAL NOT NULL,
                    bmr_full_day_kcal REAL NOT NULL,
                    active_kcal REAL NOT NULL,
                    total_burn_kcal REAL NOT NULL,
                    steps INTEGER NOT NULL DEFAULT 0,
                    resting_hr INTEGER,
                    moderate_min INTEGER NOT NULL DEFAULT 0,
                    vigorous_min INTEGER NOT NULL DEFAULT 0,
                    duration_ms INTEGER NOT NULL DEFAULT 0,
                    activities_json TEXT NOT NULL DEFAULT '[]',
                    recovery_json TEXT NOT NULL DEFAULT '{}',
                    synced_at TEXT NOT NULL
                );
                """
            )
            cols = {
                str(r["name"])
                for r in conn.execute("PRAGMA table_info(garmin_daily_metrics)").fetchall()
            }
            if "recovery_json" not in cols:
                conn.execute(
                    "ALTER TABLE garmin_daily_metrics ADD COLUMN recovery_json TEXT NOT NULL DEFAULT '{}'"
                )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS food_settings (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS garmin_settings_history (
                    effective_date TEXT PRIMARY KEY,
                    dynamic_enabled INTEGER NOT NULL,
                    goal_delta_kcal INTEGER NOT NULL,
                    active_factor REAL NOT NULL,
                    burn_offset_kcal INTEGER NOT NULL DEFAULT 0,
                    dynamic_mode TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                """
            )

    @staticmethod
    def _now() -> str:
        return datetime.now(UTC).isoformat()

    @staticmethod
    def _row_to_item(row: sqlite3.Row) -> MealItemRecord:
        return MealItemRecord(
            id=int(row["id"]),
            meal_entry_id=int(row["meal_entry_id"]),
            saved_food_id=int(row["saved_food_id"]) if row["saved_food_id"] is not None else None,
            name=str(row["name"]),
            quantity_desc=str(row["quantity_desc"]),
            estimated_grams=(
                float(row["estimated_grams"]) if row["estimated_grams"] is not None else None
            ),
            calories_kcal=float(row["calories_kcal"]),
            protein_g=float(row["protein_g"]),
            carbs_g=float(row["carbs_g"]),
            fat_g=float(row["fat_g"]),
            fiber_g=float(row["fiber_g"]),
            source=row["source"],
        )

    def _fetch_entry_with_items(
        self, conn: sqlite3.Connection, entry_id: int
    ) -> MealEntryRecord | None:
        row = conn.execute("SELECT * FROM meal_entries WHERE id = ?", (entry_id,)).fetchone()
        if row is None:
            return None
        item_rows = conn.execute(
            "SELECT * FROM meal_items WHERE meal_entry_id = ? ORDER BY id ASC",
            (entry_id,),
        ).fetchall()
        return self._row_to_entry(row, [self._row_to_item(r) for r in item_rows])

    @staticmethod
    def _row_to_entry(row: sqlite3.Row, items: list[MealItemRecord]) -> MealEntryRecord:
        slot: MealSlot = row["meal_slot"]
        conf: ConfidenceLevel = row["confidence"]
        return MealEntryRecord(
            id=int(row["id"]),
            logged_at=str(row["logged_at"]),
            local_date=str(row["local_date"]),
            meal_slot=slot,
            title=str(row["title"]),
            status=str(row["status"]),
            raw_description=str(row["raw_description"]),
            photo_relpath=row["photo_relpath"],
            slack_channel_id=row["slack_channel_id"],
            slack_thread_ts=row["slack_thread_ts"],
            slack_card_ts=row["slack_card_ts"],
            model_used=str(row["model_used"]),
            confidence=conf,
            assumptions_note=row["assumptions_note"],
            clarification_question=row["clarification_question"],
            scale_factor=float(row["scale_factor"]),
            total_calories_kcal=float(row["total_calories_kcal"]),
            total_protein_g=float(row["total_protein_g"]),
            total_carbs_g=float(row["total_carbs_g"]),
            total_fat_g=float(row["total_fat_g"]),
            total_fiber_g=float(row["total_fiber_g"]),
            updated_at=str(row["updated_at"]),
            reaction_emoji=row["reaction_emoji"],
            reasoning_trace=row["reasoning_trace"] if "reasoning_trace" in row.keys() else None,
            items=items,
        )

    @staticmethod
    def _row_to_saved_food(row: sqlite3.Row) -> SavedFood:
        return SavedFood(
            id=int(row["id"]),
            slug=str(row["slug"]),
            name=str(row["name"]),
            serving_desc=str(row["serving_desc"]),
            serving_grams=(
                float(row["serving_grams"]) if row["serving_grams"] is not None else None
            ),
            calories_kcal=float(row["calories_kcal"]),
            protein_g=float(row["protein_g"]),
            carbs_g=float(row["carbs_g"]),
            fat_g=float(row["fat_g"]),
            fiber_g=float(row["fiber_g"]),
            created_at=str(row["created_at"]),
        )

    def set_target(
        self,
        *,
        effective_date: str,
        calories_kcal: int,
        protein_g: float,
        carbs_g: float,
        fat_g: float,
        fiber_g: float = 30.0,
        notes: str | None = None,
    ) -> NutritionTarget:
        now = self._now()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO nutrition_targets (
                    effective_date, calories_kcal, protein_g, carbs_g, fat_g, fiber_g, notes,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(effective_date) DO UPDATE SET
                    calories_kcal = excluded.calories_kcal,
                    protein_g = excluded.protein_g,
                    carbs_g = excluded.carbs_g,
                    fat_g = excluded.fat_g,
                    fiber_g = excluded.fiber_g,
                    notes = excluded.notes,
                    updated_at = excluded.updated_at
                """,
                (
                    effective_date,
                    int(calories_kcal),
                    float(protein_g),
                    float(carbs_g),
                    float(fat_g),
                    float(fiber_g),
                    notes,
                    now,
                ),
            )
        return NutritionTarget(
            effective_date=effective_date,
            calories_kcal=int(calories_kcal),
            protein_g=float(protein_g),
            carbs_g=float(carbs_g),
            fat_g=float(fat_g),
            fiber_g=float(fiber_g),
            notes=notes,
            updated_at=now,
        )

    def get_target_for_date(
        self,
        local_date: str,
        fallback_cfg: FoodTrackerConfig | None = None,
    ) -> NutritionTarget:
        cfg = fallback_cfg or FoodTrackerConfig()
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT * FROM nutrition_targets
                WHERE effective_date <= ?
                ORDER BY effective_date DESC
                LIMIT 1
                """,
                (local_date,),
            ).fetchone()
        if row is not None:
            return NutritionTarget(
                effective_date=str(row["effective_date"]),
                calories_kcal=int(row["calories_kcal"]),
                protein_g=float(row["protein_g"]),
                carbs_g=float(row["carbs_g"]),
                fat_g=float(row["fat_g"]),
                fiber_g=float(row["fiber_g"]),
                notes=row["notes"],
                updated_at=str(row["updated_at"]),
            )
        return NutritionTarget(
            effective_date=local_date,
            calories_kcal=int(cfg.default_calories_kcal),
            protein_g=float(cfg.default_protein_g),
            carbs_g=float(cfg.default_carbs_g),
            fat_g=float(cfg.default_fat_g),
            fiber_g=float(cfg.default_fiber_g),
            notes="config default",
        )

    def upsert_saved_food(
        self,
        *,
        slug: str,
        name: str,
        serving_desc: str,
        calories_kcal: float,
        protein_g: float,
        carbs_g: float,
        fat_g: float,
        fiber_g: float = 0.0,
        serving_grams: float | None = None,
    ) -> SavedFood:
        clean_slug = slugify_food_name(slug)
        now = self._now()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO saved_foods (
                    slug, name, serving_desc, serving_grams,
                    calories_kcal, protein_g, carbs_g, fat_g, fiber_g, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(slug) DO UPDATE SET
                    name = excluded.name,
                    serving_desc = excluded.serving_desc,
                    serving_grams = excluded.serving_grams,
                    calories_kcal = excluded.calories_kcal,
                    protein_g = excluded.protein_g,
                    carbs_g = excluded.carbs_g,
                    fat_g = excluded.fat_g,
                    fiber_g = excluded.fiber_g
                """,
                (
                    clean_slug,
                    name.strip(),
                    serving_desc.strip(),
                    round(serving_grams, 1) if serving_grams is not None else None,
                    round(max(0.0, float(calories_kcal)), 1),
                    round(max(0.0, float(protein_g)), 1),
                    round(max(0.0, float(carbs_g)), 1),
                    round(max(0.0, float(fat_g)), 1),
                    round(max(0.0, float(fiber_g)), 1),
                    now,
                ),
            )
            row = conn.execute("SELECT * FROM saved_foods WHERE slug = ?", (clean_slug,)).fetchone()
            assert row is not None
            return self._row_to_saved_food(row)

    def delete_saved_food(self, slug: str) -> bool:
        clean_slug = slugify_food_name(slug)
        with self._connect() as conn:
            cur = conn.execute("DELETE FROM saved_foods WHERE slug = ?", (clean_slug,))
            return cur.rowcount > 0

    def get_saved_food(self, slug: str) -> SavedFood | None:
        clean_slug = slugify_food_name(slug)
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM saved_foods WHERE slug = ?", (clean_slug,)).fetchone()
            return self._row_to_saved_food(row) if row is not None else None

    def list_saved_foods(self) -> list[SavedFood]:
        with self._connect() as conn:
            rows = conn.execute("SELECT * FROM saved_foods ORDER BY slug ASC").fetchall()
            return [self._row_to_saved_food(r) for r in rows]

    def create_meal_entry(
        self,
        *,
        local_date: str,
        estimate: MealEstimateSchema,
        raw_description: str,
        model_used: str,
        photo_relpath: str | None = None,
        slack_channel_id: str | None = None,
        slack_thread_ts: str | None = None,
        slack_card_ts: str | None = None,
        scale_factor: float = 1.0,
        saved_food_ids: dict[int, int] | None = None,
        logged_at: str | None = None,
        reasoning_trace: str | None = None,
    ) -> MealEntryRecord:
        now = logged_at or self._now()
        effective_date = resolve_valid_local_date(estimate.local_date_override, local_date)
        normalized_items = [normalize_item_macros(item)[0] for item in estimate.items]
        factor = max(0.01, float(scale_factor))
        total_kcal, total_p, total_c, total_f, total_fiber = _sum_scaled_items(
            normalized_items, factor
        )
        status = (
            "needs_clarification"
            if estimate.confidence == "low" and estimate.clarification_question.strip()
            else "confirmed"
        )
        with self._connect() as conn:
            cur = conn.execute(
                """
                INSERT INTO meal_entries (
                    logged_at, local_date, meal_slot, title, status, raw_description,
                    photo_relpath, slack_channel_id, slack_thread_ts, slack_card_ts,
                    model_used, confidence, assumptions_note, clarification_question,
                    scale_factor, total_calories_kcal, total_protein_g, total_carbs_g,
                    total_fat_g, total_fiber_g, reasoning_trace, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    now,
                    effective_date,
                    estimate.meal_slot,
                    estimate.title.strip() or "Logged meal",
                    status,
                    raw_description,
                    photo_relpath,
                    slack_channel_id,
                    slack_thread_ts,
                    slack_card_ts,
                    model_used,
                    estimate.confidence,
                    estimate.assumptions_note.strip() or None,
                    estimate.clarification_question.strip() or None,
                    factor,
                    total_kcal,
                    total_p,
                    total_c,
                    total_f,
                    total_fiber,
                    (reasoning_trace or getattr(estimate, "_reasoning_trace", None) or "").strip()
                    or None,
                    now,
                ),
            )
            entry_id = int(cur.lastrowid)
            for idx, item in enumerate(normalized_items):
                sf_id = saved_food_ids.get(idx) if saved_food_ids else None
                conn.execute(
                    """
                    INSERT INTO meal_items (
                        meal_entry_id, saved_food_id, name, quantity_desc, estimated_grams,
                        calories_kcal, protein_g, carbs_g, fat_g, fiber_g, source
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        entry_id,
                        sf_id,
                        item.name,
                        item.quantity_desc,
                        item.estimated_grams,
                        item.calories_kcal,
                        item.protein_g,
                        item.carbs_g,
                        item.fat_g,
                        item.fiber_g,
                        item.source,
                    ),
                )
            entry = self._fetch_entry_with_items(conn, entry_id)
            assert entry is not None
            return entry

    def get_meal_entry(self, entry_id: int) -> MealEntryRecord | None:
        with self._connect() as conn:
            return self._fetch_entry_with_items(conn, entry_id)

    def get_meal_by_thread_ts(self, thread_ts: str) -> MealEntryRecord | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT id FROM meal_entries WHERE slack_thread_ts = ?",
                (thread_ts,),
            ).fetchone()
            if row is None:
                return None
            return self._fetch_entry_with_items(conn, int(row["id"]))

    def set_slack_card_ts(
        self,
        entry_id: int,
        card_ts: str,
        *,
        thread_ts: str | None = None,
        channel_id: str | None = None,
    ) -> MealEntryRecord:
        now = self._now()
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE meal_entries
                SET slack_card_ts = ?,
                    slack_thread_ts = COALESCE(?, slack_thread_ts),
                    slack_channel_id = COALESCE(?, slack_channel_id),
                    updated_at = ?
                WHERE id = ?
                """,
                (card_ts, thread_ts, channel_id, now, entry_id),
            )
            entry = self._fetch_entry_with_items(conn, entry_id)
            if entry is None:
                raise KeyError(f"Unknown meal_entry id: {entry_id}")
            return entry

    def scale_meal_entry(self, entry_id: int, scale_factor: float) -> MealEntryRecord:
        factor = max(0.01, float(scale_factor))
        now = self._now()
        with self._connect() as conn:
            entry = self._fetch_entry_with_items(conn, entry_id)
            if entry is None:
                raise KeyError(f"Unknown meal_entry id: {entry_id}")
            total_kcal, total_p, total_c, total_f, total_fiber = _sum_scaled_items(
                entry.items, factor
            )
            conn.execute(
                """
                UPDATE meal_entries
                SET scale_factor = ?,
                    total_calories_kcal = ?,
                    total_protein_g = ?,
                    total_carbs_g = ?,
                    total_fat_g = ?,
                    total_fiber_g = ?,
                    updated_at = ?
                WHERE id = ?
                """,
                (factor, total_kcal, total_p, total_c, total_f, total_fiber, now, entry_id),
            )
            updated = self._fetch_entry_with_items(conn, entry_id)
            assert updated is not None
            return updated

    def set_meal_reaction(self, entry_id: int, reaction_emoji: str | None) -> MealEntryRecord:
        clean = reaction_emoji.strip() if isinstance(reaction_emoji, str) else None
        if clean == "":
            clean = None
        if clean is not None and clean not in VALID_MEAL_REACTIONS:
            raise ValueError(f"Unsupported meal reaction emoji: {clean!r}")
        now = self._now()
        with self._connect() as conn:
            cur = conn.execute(
                "UPDATE meal_entries SET reaction_emoji = ?, updated_at = ? WHERE id = ?",
                (clean, now, entry_id),
            )
            if cur.rowcount == 0:
                raise KeyError(f"Unknown meal_entry id: {entry_id}")
            updated = self._fetch_entry_with_items(conn, entry_id)
            assert updated is not None
            return updated

    def replace_meal_items(
        self,
        entry_id: int,
        estimate: MealEstimateSchema,
        model_used: str,
        *,
        photo_relpath: str | None = None,
        reasoning_trace: str | None = None,
    ) -> MealEntryRecord:
        now = self._now()
        normalized_items = [normalize_item_macros(item)[0] for item in estimate.items]
        total_kcal, total_p, total_c, total_f, total_fiber = _sum_scaled_items(
            normalized_items, 1.0
        )
        status = (
            "needs_clarification"
            if estimate.confidence == "low" and estimate.clarification_question.strip()
            else "confirmed"
        )
        with self._connect() as conn:
            existing = self._fetch_entry_with_items(conn, entry_id)
            if existing is None:
                raise KeyError(f"Unknown meal_entry id: {entry_id}")
            effective_date = resolve_valid_local_date(
                estimate.local_date_override, existing.local_date
            )
            conn.execute("DELETE FROM meal_items WHERE meal_entry_id = ?", (entry_id,))
            for item in normalized_items:
                conn.execute(
                    """
                    INSERT INTO meal_items (
                        meal_entry_id, saved_food_id, name, quantity_desc, estimated_grams,
                        calories_kcal, protein_g, carbs_g, fat_g, fiber_g, source
                    ) VALUES (?, NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        entry_id,
                        item.name,
                        item.quantity_desc,
                        item.estimated_grams,
                        item.calories_kcal,
                        item.protein_g,
                        item.carbs_g,
                        item.fat_g,
                        item.fiber_g,
                        item.source,
                    ),
                )
            conn.execute(
                """
                UPDATE meal_entries
                SET local_date = ?,
                    meal_slot = ?,
                    title = ?,
                    status = ?,
                    photo_relpath = COALESCE(?, photo_relpath),
                    model_used = ?,
                    confidence = ?,
                    assumptions_note = ?,
                    clarification_question = ?,
                    scale_factor = 1.0,
                    total_calories_kcal = ?,
                    total_protein_g = ?,
                    total_carbs_g = ?,
                    total_fat_g = ?,
                    total_fiber_g = ?,
                    reasoning_trace = COALESCE(?, reasoning_trace),
                    updated_at = ?
                WHERE id = ?
                """,
                (
                    effective_date,
                    estimate.meal_slot,
                    estimate.title.strip() or existing.title,
                    status,
                    photo_relpath,
                    model_used,
                    estimate.confidence,
                    estimate.assumptions_note.strip() or None,
                    estimate.clarification_question.strip() or None,
                    total_kcal,
                    total_p,
                    total_c,
                    total_f,
                    total_fiber,
                    (reasoning_trace or getattr(estimate, "_reasoning_trace", None) or "").strip()
                    or None,
                    now,
                    entry_id,
                ),
            )
            updated = self._fetch_entry_with_items(conn, entry_id)
            assert updated is not None
            return updated

    def update_meal_entry(
        self,
        entry_id: int,
        *,
        local_date: str | None = None,
        meal_slot: MealSlot | None = None,
        title: str | None = None,
        scale_factor: float | None = None,
        items: list[EstimatedMealItem] | None = None,
    ) -> MealEntryRecord:
        """Update editable metadata and/or itemized macros for an existing meal entry."""
        now = self._now()
        with self._connect() as conn:
            existing = self._fetch_entry_with_items(conn, entry_id)
            if existing is None or existing.status == "deleted":
                raise KeyError(f"Unknown meal_entry id: {entry_id}")

            if local_date is not None:
                resolved_date = resolve_valid_local_date(local_date.strip(), "")
                if not resolved_date:
                    raise ValueError(f"Invalid local_date: {local_date!r}")
            else:
                resolved_date = existing.local_date

            resolved_slot = meal_slot if meal_slot is not None else existing.meal_slot
            if resolved_slot not in ("breakfast", "lunch", "dinner", "snack"):
                raise ValueError(f"Invalid meal_slot: {meal_slot!r}")

            resolved_title = (
                title.strip() if title is not None and title.strip() else existing.title
            )
            resolved_scale = (
                max(0.1, min(10.0, float(scale_factor)))
                if scale_factor is not None
                else float(existing.scale_factor)
            )

            if items is not None:
                normalized_items = [normalize_item_macros(item)[0] for item in items]
                conn.execute("DELETE FROM meal_items WHERE meal_entry_id = ?", (entry_id,))
                for item in normalized_items:
                    conn.execute(
                        """
                        INSERT INTO meal_items (
                            meal_entry_id, saved_food_id, name, quantity_desc, estimated_grams,
                            calories_kcal, protein_g, carbs_g, fat_g, fiber_g, source
                        ) VALUES (?, NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            entry_id,
                            item.name,
                            item.quantity_desc,
                            item.estimated_grams,
                            item.calories_kcal,
                            item.protein_g,
                            item.carbs_g,
                            item.fat_g,
                            item.fiber_g,
                            item.source,
                        ),
                    )
                total_kcal, total_p, total_c, total_f, total_fiber = _sum_scaled_items(
                    normalized_items, resolved_scale
                )
            else:
                total_kcal, total_p, total_c, total_f, total_fiber = _sum_scaled_items(
                    existing.items, resolved_scale
                )

            conn.execute(
                """
                UPDATE meal_entries
                SET local_date = ?,
                    meal_slot = ?,
                    title = ?,
                    scale_factor = ?,
                    total_calories_kcal = ?,
                    total_protein_g = ?,
                    total_carbs_g = ?,
                    total_fat_g = ?,
                    total_fiber_g = ?,
                    updated_at = ?
                WHERE id = ?
                """,
                (
                    resolved_date,
                    resolved_slot,
                    resolved_title,
                    resolved_scale,
                    total_kcal,
                    total_p,
                    total_c,
                    total_f,
                    total_fiber,
                    now,
                    entry_id,
                ),
            )
            updated = self._fetch_entry_with_items(conn, entry_id)
            assert updated is not None
            return updated

    def soft_delete_meal(self, entry_id: int) -> MealEntryRecord:
        now = self._now()
        with self._connect() as conn:
            conn.execute(
                "UPDATE meal_entries SET status = 'deleted', updated_at = ? WHERE id = ?",
                (now, entry_id),
            )
            updated = self._fetch_entry_with_items(conn, entry_id)
            if updated is None:
                raise KeyError(f"Unknown meal_entry id: {entry_id}")
            return updated

    def save_meal_as_staple(self, entry_id: int, slug: str | None = None) -> SavedFood:
        entry = self.get_meal_entry(entry_id)
        if entry is None:
            raise KeyError(f"Unknown meal_entry id: {entry_id}")
        target_slug = slugify_food_name(slug) if slug else slugify_food_name(entry.title)
        grams_vals = [i.estimated_grams for i in entry.items if i.estimated_grams is not None]
        serving_grams = round(sum(grams_vals) * entry.scale_factor, 1) if grams_vals else None
        serving_desc = (
            "1 serving"
            if abs(entry.scale_factor - 1.0) < 1e-6
            else f"{entry.scale_factor:g}x portion"
        )
        return self.upsert_saved_food(
            slug=target_slug,
            name=entry.title,
            serving_desc=serving_desc,
            serving_grams=serving_grams,
            calories_kcal=entry.total_calories_kcal,
            protein_g=entry.total_protein_g,
            carbs_g=entry.total_carbs_g,
            fat_g=entry.total_fat_g,
            fiber_g=entry.total_fiber_g,
        )

    def get_daily_summary(
        self,
        local_date: str,
        fallback_cfg: FoodTrackerConfig | None = None,
    ) -> DailySummary:
        cfg = fallback_cfg or FoodTrackerConfig()
        target, dynamic_info = self.resolve_effective_target(local_date, cfg)
        garmin_metric = self.get_garmin_daily(local_date)
        with self._connect() as conn:
            entry_rows = conn.execute(
                """
                SELECT * FROM meal_entries
                WHERE local_date = ? AND status != 'deleted'
                ORDER BY logged_at ASC, id ASC
                """,
                (local_date,),
            ).fetchall()
            meals: list[MealEntryRecord] = []
            if entry_rows:
                entry_ids = [int(r["id"]) for r in entry_rows]
                placeholders = ",".join("?" for _ in entry_ids)
                item_rows = conn.execute(
                    f"SELECT * FROM meal_items WHERE meal_entry_id IN ({placeholders}) ORDER BY id ASC",
                    entry_ids,
                ).fetchall()
                items_by_entry: dict[int, list[MealItemRecord]] = {eid: [] for eid in entry_ids}
                for ir in item_rows:
                    item = self._row_to_item(ir)
                    items_by_entry[item.meal_entry_id].append(item)
                for er in entry_rows:
                    eid = int(er["id"])
                    meals.append(self._row_to_entry(er, items_by_entry[eid]))

        kcal = round(sum(m.total_calories_kcal for m in meals), 1)
        protein = round(sum(m.total_protein_g for m in meals), 1)
        carbs = round(sum(m.total_carbs_g for m in meals), 1)
        fat = round(sum(m.total_fat_g for m in meals), 1)
        fiber = round(sum(m.total_fiber_g for m in meals), 1)

        return DailySummary(
            local_date=local_date,
            target=target,
            meals=meals,
            total_calories_kcal=kcal,
            total_protein_g=protein,
            total_carbs_g=carbs,
            total_fat_g=fat,
            total_fiber_g=fiber,
            remaining_calories_kcal=round(target.calories_kcal - kcal, 1),
            remaining_protein_g=round(target.protein_g - protein, 1),
            remaining_carbs_g=round(target.carbs_g - carbs, 1),
            remaining_fat_g=round(target.fat_g - fat, 1),
            remaining_fiber_g=round(target.fiber_g - fiber, 1),
            garmin=garmin_metric,
            dynamic_target=dynamic_info,
        )

    def get_weekly_summary(
        self,
        end_local_date: str,
        fallback_cfg: FoodTrackerConfig | None = None,
        days: int = 7,
    ) -> WeeklySummary:
        end_d = date.fromisoformat(end_local_date)
        span = max(1, days)
        start_d = end_d - timedelta(days=span - 1)
        summaries = [
            self.get_daily_summary((start_d + timedelta(days=offset)).isoformat(), fallback_cfg)
            for offset in range(span)
        ]
        logged_days = [s for s in summaries if s.meals]
        n = len(logged_days)
        target, _ = self.resolve_effective_target(end_local_date, fallback_cfg)

        garmin_days = [
            s.garmin for s in summaries if s.garmin is not None and s.garmin.bmr_full_day_kcal > 0
        ]
        g_count = len(garmin_days)
        avg_g_burn = (
            round(sum(g.total_burn_kcal for g in garmin_days) / g_count, 1) if g_count else None
        )
        avg_g_bmr = (
            round(sum(g.bmr_full_day_kcal for g in garmin_days) / g_count, 1) if g_count else None
        )
        avg_g_active = (
            round(sum(g.active_kcal for g in garmin_days) / g_count, 1) if g_count else None
        )

        if n == 0:
            return WeeklySummary(
                start_date=start_d.isoformat(),
                end_date=end_d.isoformat(),
                days_logged=0,
                target=target,
                avg_calories_kcal=0.0,
                avg_protein_g=0.0,
                avg_carbs_g=0.0,
                avg_fat_g=0.0,
                avg_fiber_g=0.0,
                protein_hit_days=0,
                calorie_adherent_days=0,
                daily_totals=summaries,
                avg_garmin_burn_kcal=avg_g_burn,
                avg_garmin_bmr_kcal=avg_g_bmr,
                avg_garmin_active_kcal=avg_g_active,
                garmin_days_synced=g_count,
            )

        protein_hit = sum(1 for d in logged_days if d.total_protein_g >= d.target.protein_g * 0.95)
        calorie_adherent = sum(
            1
            for d in logged_days
            if abs(d.total_calories_kcal - d.target.calories_kcal) <= d.target.calories_kcal * 0.10
        )

        return WeeklySummary(
            start_date=start_d.isoformat(),
            end_date=end_d.isoformat(),
            days_logged=n,
            target=target,
            avg_calories_kcal=round(sum(d.total_calories_kcal for d in logged_days) / n, 1),
            avg_protein_g=round(sum(d.total_protein_g for d in logged_days) / n, 1),
            avg_carbs_g=round(sum(d.total_carbs_g for d in logged_days) / n, 1),
            avg_fat_g=round(sum(d.total_fat_g for d in logged_days) / n, 1),
            avg_fiber_g=round(sum(d.total_fiber_g for d in logged_days) / n, 1),
            protein_hit_days=protein_hit,
            calorie_adherent_days=calorie_adherent,
            daily_totals=summaries,
            avg_garmin_burn_kcal=avg_g_burn,
            avg_garmin_bmr_kcal=avg_g_bmr,
            avg_garmin_active_kcal=avg_g_active,
            garmin_days_synced=g_count,
        )

    def log_weight(
        self,
        *,
        local_date: str,
        weight_lbs: float,
        notes: str = "",
        logged_at: str | None = None,
    ) -> BodyMetricRecord:
        now = logged_at or self._now()
        clean_weight = round(float(weight_lbs), 2)
        with self._connect() as conn:
            prev = conn.execute(
                """
                SELECT ema_weight_lbs FROM body_metrics
                WHERE local_date < ?
                ORDER BY local_date DESC
                LIMIT 1
                """,
                (local_date,),
            ).fetchone()
            if prev is None:
                ema = clean_weight
            else:
                alpha = 0.25
                ema = round(alpha * clean_weight + (1.0 - alpha) * float(prev["ema_weight_lbs"]), 2)
            conn.execute(
                """
                INSERT INTO body_metrics (local_date, weight_lbs, ema_weight_lbs, notes, logged_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(local_date) DO UPDATE SET
                    weight_lbs = excluded.weight_lbs,
                    ema_weight_lbs = excluded.ema_weight_lbs,
                    notes = excluded.notes,
                    logged_at = excluded.logged_at
                """,
                (local_date, clean_weight, ema, notes.strip() or None, now),
            )
        return BodyMetricRecord(
            local_date=local_date,
            weight_lbs=clean_weight,
            ema_weight_lbs=ema,
            notes=notes.strip() or None,
            logged_at=now,
        )

    def get_latest_weight(self) -> BodyMetricRecord | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM body_metrics ORDER BY local_date DESC LIMIT 1"
            ).fetchone()
        if row is None:
            return None
        return BodyMetricRecord(
            local_date=str(row["local_date"]),
            weight_lbs=float(row["weight_lbs"]),
            ema_weight_lbs=float(row["ema_weight_lbs"]),
            notes=row["notes"],
            logged_at=str(row["logged_at"]),
        )

    def estimate_adaptive_tdee(
        self,
        end_local_date: str,
        window_days: int = 14,
        *,
        exclude_end_date_meals: bool = False,
    ) -> AdaptiveTdeeEstimate | None:
        end_d = date.fromisoformat(end_local_date)
        start_d = end_d - timedelta(days=max(7, window_days) - 1)
        start_str = start_d.isoformat()
        end_str = end_d.isoformat()
        with self._connect() as conn:
            meal_rows = conn.execute(
                """
                SELECT local_date, SUM(total_calories_kcal) AS day_kcal
                FROM meal_entries
                WHERE local_date BETWEEN ? AND ? AND status != 'deleted'
                GROUP BY local_date
                """,
                (start_str, end_str),
            ).fetchall()
            weight_rows = conn.execute(
                """
                SELECT local_date, ema_weight_lbs
                FROM body_metrics
                WHERE local_date BETWEEN ? AND ?
                ORDER BY local_date ASC
                """,
                (start_str, end_str),
            ).fetchall()

        if exclude_end_date_meals:
            completed_meals = [r for r in meal_rows if str(r["local_date"]) < end_str]
            if len(completed_meals) >= 7:
                meal_rows = completed_meals

        days_logged = len(meal_rows)
        if days_logged < 7 or len(weight_rows) < 2:
            return None

        start_w = weight_rows[0]
        end_w = weight_rows[-1]
        span_days = (
            date.fromisoformat(str(end_w["local_date"]))
            - date.fromisoformat(str(start_w["local_date"]))
        ).days
        if span_days < 7:
            return None

        avg_daily_kcal = sum(float(r["day_kcal"]) for r in meal_rows) / days_logged
        start_ema = float(start_w["ema_weight_lbs"])
        end_ema = float(end_w["ema_weight_lbs"])
        weight_delta = round(end_ema - start_ema, 2)
        estimated_tdee = round(avg_daily_kcal - (weight_delta / span_days) * 3500.0, 0)

        return AdaptiveTdeeEstimate(
            window_days=window_days,
            days_logged=days_logged,
            avg_daily_kcal=round(avg_daily_kcal, 1),
            start_ema_lbs=start_ema,
            end_ema_lbs=end_ema,
            weight_delta_lbs=weight_delta,
            span_days=span_days,
            estimated_tdee_kcal=estimated_tdee,
        )

    def has_sent_digest(self, local_date: str) -> bool:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT 1 FROM food_digest_log WHERE local_date = ?", (local_date,)
            ).fetchone()
            return row is not None

    def mark_digest_sent(self, local_date: str) -> None:
        with self._connect() as conn:
            conn.execute(
                "INSERT OR IGNORE INTO food_digest_log (local_date, sent_at) VALUES (?, ?)",
                (local_date, self._now()),
            )

    def create_backup_snapshot(
        self,
        backup_dir: Path,
        local_date: str,
        keep_days: int = 14,
    ) -> Path:
        """Create a consistent gzip-compressed SQLite backup and prune expired archives."""
        backup_dir.mkdir(parents=True, exist_ok=True)
        tmp_sqlite = backup_dir / f".nutrition-{local_date}.sqlite3.tmp"
        dest_gz = backup_dir / f"nutrition-{local_date}.sqlite3.gz"
        if tmp_sqlite.exists():
            tmp_sqlite.unlink()

        src_conn = self._connect()
        try:
            dst_conn = sqlite3.connect(tmp_sqlite)
            try:
                src_conn.backup(dst_conn)
            finally:
                dst_conn.close()
        finally:
            src_conn.close()

        with tmp_sqlite.open("rb") as f_in, gzip.open(dest_gz, "wb", compresslevel=6) as f_out:
            shutil.copyfileobj(f_in, f_out)
        tmp_sqlite.unlink(missing_ok=True)

        if keep_days > 0:
            cutoff = date.fromisoformat(local_date) - timedelta(days=keep_days)
            for path in sorted(backup_dir.glob("nutrition-*.sqlite3.gz")):
                match = re.match(r"^nutrition-(\d{4}-\d{2}-\d{2})\.sqlite3\.gz$", path.name)
                if match:
                    try:
                        if date.fromisoformat(match.group(1)) < cutoff:
                            path.unlink(missing_ok=True)
                    except ValueError:
                        continue
        return dest_gz

    @staticmethod
    def _row_to_garmin_metric(row: sqlite3.Row) -> GarminDailyMetric:
        raw_acts = json.loads(str(row["activities_json"] or "[]"))
        activities = [
            GarminActivityRecord(
                activity_id=int(a["activity_id"]),
                name=str(a["name"]),
                activity_type=str(a["activity_type"]),
                calories_kcal=float(a["calories_kcal"]),
                duration_seconds=float(a["duration_seconds"]),
                distance_meters=float(a.get("distance_meters") or 0.0),
                average_hr=float(a["average_hr"]) if a.get("average_hr") is not None else None,
                start_time_local=str(a.get("start_time_local") or ""),
            )
            for a in raw_acts
            if isinstance(a, dict)
        ]
        rec_raw = row["recovery_json"]
        rec = json.loads(str(rec_raw or "{}"))
        if not isinstance(rec, dict):
            rec = {}
        return GarminDailyMetric(
            local_date=str(row["local_date"]),
            bmr_raw_kcal=float(row["bmr_raw_kcal"]),
            bmr_full_day_kcal=float(row["bmr_full_day_kcal"]),
            active_kcal=float(row["active_kcal"]),
            total_burn_kcal=float(row["total_burn_kcal"]),
            steps=int(row["steps"]),
            resting_hr=int(row["resting_hr"]) if row["resting_hr"] is not None else None,
            moderate_min=int(row["moderate_min"]),
            vigorous_min=int(row["vigorous_min"]),
            duration_ms=int(row["duration_ms"]),
            activities=activities,
            synced_at=str(row["synced_at"]),
            step_goal=int(rec.get("step_goal") or 10000),
            intensity_goal_min=int(rec.get("intensity_goal_min") or 150),
            resting_hr_7d_avg=rec.get("resting_hr_7d_avg"),
            body_battery_high=rec.get("body_battery_high"),
            body_battery_low=rec.get("body_battery_low"),
            body_battery_current=rec.get("body_battery_current"),
            body_battery_charged=rec.get("body_battery_charged"),
            avg_stress=rec.get("avg_stress"),
            sleep_seconds=rec.get("sleep_seconds"),
            sleep_deep_seconds=rec.get("sleep_deep_seconds"),
            sleep_rem_seconds=rec.get("sleep_rem_seconds"),
            sleep_score=rec.get("sleep_score"),
            sleep_qualifier=rec.get("sleep_qualifier"),
            hrv_last_night_avg=rec.get("hrv_last_night_avg"),
            hrv_weekly_avg=rec.get("hrv_weekly_avg"),
            hrv_status=rec.get("hrv_status"),
        )

    def upsert_garmin_daily(self, metric: GarminDailyMetric) -> None:
        synced_at = metric.synced_at or self._now()
        acts_json = json.dumps([asdict(a) for a in metric.activities])
        recovery_json = json.dumps(
            {
                "step_goal": metric.step_goal,
                "intensity_goal_min": metric.intensity_goal_min,
                "resting_hr_7d_avg": metric.resting_hr_7d_avg,
                "body_battery_high": metric.body_battery_high,
                "body_battery_low": metric.body_battery_low,
                "body_battery_current": metric.body_battery_current,
                "body_battery_charged": metric.body_battery_charged,
                "avg_stress": metric.avg_stress,
                "sleep_seconds": metric.sleep_seconds,
                "sleep_deep_seconds": metric.sleep_deep_seconds,
                "sleep_rem_seconds": metric.sleep_rem_seconds,
                "sleep_score": metric.sleep_score,
                "sleep_qualifier": metric.sleep_qualifier,
                "hrv_last_night_avg": metric.hrv_last_night_avg,
                "hrv_weekly_avg": metric.hrv_weekly_avg,
                "hrv_status": metric.hrv_status,
            }
        )
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO garmin_daily_metrics (
                    local_date, bmr_raw_kcal, bmr_full_day_kcal, active_kcal, total_burn_kcal,
                    steps, resting_hr, moderate_min, vigorous_min, duration_ms,
                    activities_json, recovery_json, synced_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(local_date) DO UPDATE SET
                    bmr_raw_kcal = excluded.bmr_raw_kcal,
                    bmr_full_day_kcal = excluded.bmr_full_day_kcal,
                    active_kcal = excluded.active_kcal,
                    total_burn_kcal = excluded.total_burn_kcal,
                    steps = excluded.steps,
                    resting_hr = excluded.resting_hr,
                    moderate_min = excluded.moderate_min,
                    vigorous_min = excluded.vigorous_min,
                    duration_ms = excluded.duration_ms,
                    activities_json = excluded.activities_json,
                    recovery_json = excluded.recovery_json,
                    synced_at = excluded.synced_at
                """,
                (
                    metric.local_date,
                    round(float(metric.bmr_raw_kcal), 1),
                    round(float(metric.bmr_full_day_kcal), 1),
                    round(float(metric.active_kcal), 1),
                    round(float(metric.total_burn_kcal), 1),
                    int(metric.steps),
                    int(metric.resting_hr) if metric.resting_hr is not None else None,
                    int(metric.moderate_min),
                    int(metric.vigorous_min),
                    int(metric.duration_ms),
                    acts_json,
                    recovery_json,
                    synced_at,
                ),
            )

    def get_garmin_daily(self, local_date: str) -> GarminDailyMetric | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM garmin_daily_metrics WHERE local_date = ?",
                (local_date,),
            ).fetchone()
        return self._row_to_garmin_metric(row) if row is not None else None

    def list_garmin_range(self, start_date: str, end_date: str) -> list[GarminDailyMetric]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT * FROM garmin_daily_metrics
                WHERE local_date BETWEEN ? AND ?
                ORDER BY local_date ASC
                """,
                (start_date, end_date),
            ).fetchall()
        return [self._row_to_garmin_metric(r) for r in rows]

    def get_latest_full_day_bmr(self, before_date: str | None = None) -> float | None:
        with self._connect() as conn:
            if before_date:
                row = conn.execute(
                    """
                    SELECT bmr_full_day_kcal FROM garmin_daily_metrics
                    WHERE local_date < ? AND bmr_full_day_kcal > 0
                    ORDER BY local_date DESC
                    LIMIT 1
                    """,
                    (before_date,),
                ).fetchone()
            else:
                row = conn.execute(
                    """
                    SELECT bmr_full_day_kcal FROM garmin_daily_metrics
                    WHERE bmr_full_day_kcal > 0
                    ORDER BY local_date DESC
                    LIMIT 1
                    """
                ).fetchone()
        return float(row["bmr_full_day_kcal"]) if row is not None else None

    def get_food_setting(self, key: str) -> str | None:
        with self._connect() as conn:
            row = conn.execute("SELECT value FROM food_settings WHERE key = ?", (key,)).fetchone()
        return str(row["value"]) if row is not None else None

    def set_food_setting(self, key: str, value: str) -> None:
        now = self._now()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO food_settings (key, value, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(key) DO UPDATE SET
                    value = excluded.value,
                    updated_at = excluded.updated_at
                """,
                (key, value, now),
            )

    def sync_garmin_weights(self, entries: list[tuple[str, float]]) -> int:
        """Persist Garmin weights chronologically when no manual `!weight` exists for that day."""
        synced = 0
        for local_date, weight_lbs in sorted(entries, key=lambda item: item[0]):
            with self._connect() as conn:
                existing = conn.execute(
                    "SELECT notes FROM body_metrics WHERE local_date = ?",
                    (local_date,),
                ).fetchone()
            if existing is not None and (existing["notes"] or "") != "garmin":
                continue
            self.log_weight(local_date=local_date, weight_lbs=weight_lbs, notes="garmin")
            synced += 1
        return synced

    def get_garmin_runtime_settings(
        self,
        fallback_cfg: FoodTrackerConfig | None = None,
        *,
        local_date: str | None = None,
    ) -> dict[str, object]:
        cfg = fallback_cfg or FoodTrackerConfig()
        if local_date is not None:
            with self._connect() as conn:
                hist_row = conn.execute(
                    """
                    SELECT dynamic_enabled, goal_delta_kcal, active_factor,
                           burn_offset_kcal, dynamic_mode
                    FROM garmin_settings_history
                    WHERE effective_date <= ?
                    ORDER BY effective_date DESC
                    LIMIT 1
                    """,
                    (local_date,),
                ).fetchone()
            if hist_row is not None:
                mode_val = str(hist_row["dynamic_mode"])
                return {
                    "dynamic_enabled": bool(hist_row["dynamic_enabled"]),
                    "goal_delta_kcal": max(-1500, min(1500, int(hist_row["goal_delta_kcal"]))),
                    "active_factor": round(max(0.0, min(1.5, float(hist_row["active_factor"]))), 2),
                    "burn_offset_kcal": max(-600, min(600, int(hist_row["burn_offset_kcal"]))),
                    "dynamic_mode": (
                        mode_val
                        if mode_val in ("rolling_baseline", "daily_burn")
                        else cfg.garmin_dynamic_mode
                    ),
                }

        dyn_override = self.get_food_setting("garmin_dynamic_enabled")
        dyn_enabled = (
            dyn_override.strip().lower() in {"1", "true", "yes", "on"}
            if dyn_override is not None
            else cfg.garmin_dynamic_targets_enabled
        )

        delta_override = self.get_food_setting("garmin_goal_delta_kcal")
        goal_delta = int(cfg.garmin_goal_delta_kcal)
        if delta_override is not None:
            try:
                goal_delta = int(delta_override)
            except ValueError:
                pass
        goal_delta = max(-1500, min(1500, goal_delta))

        factor_override = self.get_food_setting("garmin_active_factor")
        active_factor = float(cfg.garmin_active_factor)
        if factor_override is not None:
            try:
                active_factor = float(factor_override)
            except ValueError:
                pass
        active_factor = round(max(0.0, min(1.5, active_factor)), 2)

        burn_offset_override = self.get_food_setting("garmin_burn_offset_kcal")
        burn_offset = int(getattr(cfg, "garmin_burn_offset_kcal", 0))
        if burn_offset_override is not None:
            try:
                burn_offset = int(round(float(burn_offset_override)))
            except ValueError:
                pass
        burn_offset = max(-600, min(600, burn_offset))

        mode_override = self.get_food_setting("garmin_dynamic_mode")
        mode = (
            mode_override.strip()
            if mode_override in ("rolling_baseline", "daily_burn")
            else cfg.garmin_dynamic_mode
        )
        return {
            "dynamic_enabled": dyn_enabled,
            "goal_delta_kcal": goal_delta,
            "active_factor": active_factor,
            "burn_offset_kcal": burn_offset,
            "dynamic_mode": mode,
        }

    def save_garmin_settings(
        self,
        *,
        local_date: str,
        fallback_cfg: FoodTrackerConfig | None = None,
        dynamic_enabled: bool | None = None,
        goal_delta_kcal: int | None = None,
        active_factor: float | None = None,
        burn_offset_kcal: int | None = None,
        dynamic_mode: str | None = None,
        backfill: bool = False,
    ) -> dict[str, object]:
        """Persist Garmin dynamic target settings and record a date-versioned history entry."""
        cfg = fallback_cfg or FoodTrackerConfig()
        prev = self.get_garmin_runtime_settings(cfg)
        new_dyn = bool(prev["dynamic_enabled"]) if dynamic_enabled is None else bool(dynamic_enabled)
        new_delta = (
            int(prev["goal_delta_kcal"])  # type: ignore[arg-type]
            if goal_delta_kcal is None
            else max(-1500, min(1500, int(goal_delta_kcal)))
        )
        new_factor = (
            float(prev["active_factor"])  # type: ignore[arg-type]
            if active_factor is None
            else round(max(0.0, min(1.5, float(active_factor))), 2)
        )
        new_offset = (
            int(prev["burn_offset_kcal"])  # type: ignore[arg-type]
            if burn_offset_kcal is None
            else max(-600, min(600, int(round(burn_offset_kcal))))
        )
        if dynamic_mode is not None:
            if dynamic_mode not in ("rolling_baseline", "daily_burn"):
                raise ValueError(f"Invalid dynamic_mode: {dynamic_mode!r}")
            new_mode = dynamic_mode
        else:
            new_mode = str(prev["dynamic_mode"])

        now = self._now()
        with self._connect() as conn:
            for k, v in (
                ("garmin_dynamic_enabled", "true" if new_dyn else "false"),
                ("garmin_goal_delta_kcal", str(new_delta)),
                ("garmin_active_factor", str(new_factor)),
                ("garmin_burn_offset_kcal", str(new_offset)),
                ("garmin_dynamic_mode", new_mode),
            ):
                conn.execute(
                    """
                    INSERT INTO food_settings (key, value, updated_at)
                    VALUES (?, ?, ?)
                    ON CONFLICT(key) DO UPDATE SET
                        value = excluded.value,
                        updated_at = excluded.updated_at
                    """,
                    (k, v, now),
                )

            if backfill:
                conn.execute(
                    "DELETE FROM garmin_settings_history WHERE effective_date <= ?",
                    (local_date,),
                )
                conn.execute(
                    """
                    INSERT INTO garmin_settings_history (
                        effective_date, dynamic_enabled, goal_delta_kcal,
                        active_factor, burn_offset_kcal, dynamic_mode, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(effective_date) DO UPDATE SET
                        dynamic_enabled = excluded.dynamic_enabled,
                        goal_delta_kcal = excluded.goal_delta_kcal,
                        active_factor = excluded.active_factor,
                        burn_offset_kcal = excluded.burn_offset_kcal,
                        dynamic_mode = excluded.dynamic_mode,
                        updated_at = excluded.updated_at
                    """,
                    (
                        "1970-01-01",
                        1 if new_dyn else 0,
                        new_delta,
                        new_factor,
                        new_offset,
                        new_mode,
                        now,
                    ),
                )
            else:
                hist_count = int(
                    conn.execute("SELECT COUNT(*) FROM garmin_settings_history").fetchone()[0]
                )
                if hist_count == 0:
                    conn.execute(
                        """
                        INSERT INTO garmin_settings_history (
                            effective_date, dynamic_enabled, goal_delta_kcal,
                            active_factor, burn_offset_kcal, dynamic_mode, updated_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            "1970-01-01",
                            1 if bool(prev["dynamic_enabled"]) else 0,
                            int(prev["goal_delta_kcal"]),  # type: ignore[arg-type]
                            float(prev["active_factor"]),  # type: ignore[arg-type]
                            int(prev["burn_offset_kcal"]),  # type: ignore[arg-type]
                            str(prev["dynamic_mode"]),
                            now,
                        ),
                    )
                conn.execute(
                    """
                    INSERT INTO garmin_settings_history (
                        effective_date, dynamic_enabled, goal_delta_kcal,
                        active_factor, burn_offset_kcal, dynamic_mode, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(effective_date) DO UPDATE SET
                        dynamic_enabled = excluded.dynamic_enabled,
                        goal_delta_kcal = excluded.goal_delta_kcal,
                        active_factor = excluded.active_factor,
                        burn_offset_kcal = excluded.burn_offset_kcal,
                        dynamic_mode = excluded.dynamic_mode,
                        updated_at = excluded.updated_at
                    """,
                    (
                        local_date,
                        1 if new_dyn else 0,
                        new_delta,
                        new_factor,
                        new_offset,
                        new_mode,
                        now,
                    ),
                )

        return {
            "dynamic_enabled": new_dyn,
            "goal_delta_kcal": new_delta,
            "active_factor": new_factor,
            "burn_offset_kcal": new_offset,
            "dynamic_mode": new_mode,
        }

    def compute_burn_calibration(
        self,
        end_local_date: str,
        fallback_cfg: FoodTrackerConfig | None = None,
        *,
        window_days: int = 14,
        active_factor_override: float | None = None,
    ) -> dict[str, object] | None:
        """Compute additive BMR/TDEE burn calibration comparing Garmin burn to scale Adaptive TDEE."""
        cfg = fallback_cfg or FoodTrackerConfig()
        end_d = date.fromisoformat(end_local_date)
        span = max(7, min(60, window_days))
        start_str = (end_d - timedelta(days=span - 1)).isoformat()

        tdee = self.estimate_adaptive_tdee(
            end_local_date, window_days=span, exclude_end_date_meals=True
        )
        all_garmin = [
            d for d in self.list_garmin_range(start_str, end_local_date) if d.bmr_full_day_kcal > 0
        ]
        completed_garmin = [d for d in all_garmin if d.local_date < end_local_date]
        garmin_rows = completed_garmin if len(completed_garmin) >= 5 else all_garmin
        if tdee is None or len(garmin_rows) < 5:
            return None

        avg_raw_burn = sum(d.total_burn_kcal for d in garmin_rows) / len(garmin_rows)
        avg_bmr = sum(d.bmr_full_day_kcal for d in garmin_rows) / len(garmin_rows)
        avg_active = sum(d.active_kcal for d in garmin_rows) / len(garmin_rows)
        if avg_raw_burn <= 0 or tdee.estimated_tdee_kcal <= 0:
            return None

        rt = self.get_garmin_runtime_settings(cfg, local_date=end_local_date)
        current_active_factor = float(rt["active_factor"])  # type: ignore[arg-type]
        current_burn_offset = int(rt["burn_offset_kcal"])  # type: ignore[arg-type]
        if active_factor_override is not None:
            target_active_factor = round(max(0.0, min(1.5, float(active_factor_override))), 2)
        elif current_active_factor > 1.0:
            target_active_factor = round(max(0.0, min(1.5, float(cfg.garmin_active_factor))), 2)
        else:
            target_active_factor = current_active_factor

        avg_model_burn = avg_bmr + avg_active * target_active_factor
        avg_current_burn = avg_bmr + avg_active * current_active_factor + current_burn_offset
        raw_offset = tdee.estimated_tdee_kcal - avg_model_burn
        rec_burn_offset = int(round(max(-600.0, min(600.0, raw_offset))))
        raw_bias_pct = round(
            ((avg_raw_burn - tdee.estimated_tdee_kcal) / tdee.estimated_tdee_kcal) * 100.0, 1
        )
        calibrated_bias_pct = round(
            ((avg_current_burn - tdee.estimated_tdee_kcal) / tdee.estimated_tdee_kcal) * 100.0, 1
        )
        needs_calibration = (
            abs(rec_burn_offset - current_burn_offset) >= 25 or current_active_factor > 1.0
        )

        return {
            "adaptive_tdee_kcal": tdee.estimated_tdee_kcal,
            "avg_garmin_burn_kcal": round(avg_raw_burn, 1),
            "avg_model_burn_kcal": round(avg_model_burn, 1),
            "avg_calibrated_burn_kcal": round(avg_current_burn, 1),
            "bias_pct": raw_bias_pct,
            "calibrated_bias_pct": calibrated_bias_pct,
            "current_burn_offset_kcal": current_burn_offset,
            "recommended_burn_offset_kcal": rec_burn_offset,
            "recommended_active_factor": target_active_factor,
            "needs_calibration": needs_calibration,
        }

    def run_burn_calibration_backfill_once(
        self,
        today_local_date: str,
        fallback_cfg: FoodTrackerConfig | None = None,
    ) -> dict[str, object] | None:
        """One-time migration: replace legacy active_factor calibration with additive burn offset and backfill history."""
        if self.get_food_setting("burn_calibration_backfill_v1_done") is not None:
            return None

        cfg = fallback_cfg or FoodTrackerConfig()
        rt = self.get_garmin_runtime_settings(cfg)
        current_factor = float(rt["active_factor"])  # type: ignore[arg-type]
        clean_factor = (
            round(max(0.0, min(1.5, float(cfg.garmin_active_factor))), 2)
            if current_factor > 1.0
            else current_factor
        )
        cal = self.compute_burn_calibration(
            today_local_date,
            cfg,
            window_days=14,
            active_factor_override=clean_factor,
        )
        if cal is None:
            return None

        rec_offset = int(cal["recommended_burn_offset_kcal"])  # type: ignore[arg-type]
        saved = self.save_garmin_settings(
            local_date=today_local_date,
            fallback_cfg=cfg,
            active_factor=clean_factor,
            burn_offset_kcal=rec_offset,
            backfill=True,
        )
        self.set_food_setting("burn_calibration_backfill_v1_done", self._now())
        return {
            "active_factor": saved["active_factor"],
            "burn_offset_kcal": saved["burn_offset_kcal"],
            "adaptive_tdee_kcal": cal["adaptive_tdee_kcal"],
            "avg_model_burn_kcal": cal["avg_model_burn_kcal"],
        }

    def resolve_effective_target(
        self,
        local_date: str,
        fallback_cfg: FoodTrackerConfig | None = None,
    ) -> tuple[NutritionTarget, DynamicTargetInfo | None]:
        """Compute static or Garmin-adjusted daily calorie & carb targets."""
        cfg = fallback_cfg or FoodTrackerConfig()
        base = self.get_target_for_date(local_date, cfg)
        rt = self.get_garmin_runtime_settings(cfg, local_date=local_date)
        dyn_enabled = bool(rt["dynamic_enabled"])
        goal_delta = int(rt["goal_delta_kcal"])  # type: ignore[arg-type]
        active_factor = float(rt["active_factor"])  # type: ignore[arg-type]
        burn_offset = int(rt["burn_offset_kcal"])  # type: ignore[arg-type]
        mode = str(rt["dynamic_mode"])

        if not dyn_enabled or not cfg.garmin_enabled:
            return base, None

        today_g = self.get_garmin_daily(local_date)
        end_d = date.fromisoformat(local_date)
        prev_day = (end_d - timedelta(days=1)).isoformat()
        start_7d = (end_d - timedelta(days=7)).isoformat()
        history = [d for d in self.list_garmin_range(start_7d, prev_day) if d.bmr_full_day_kcal > 0]

        if (today_g is None or today_g.bmr_full_day_kcal <= 0) and not history:
            return base, None

        if today_g is not None and today_g.bmr_full_day_kcal > 0:
            baseline_bmr = today_g.bmr_full_day_kcal
        else:
            baseline_bmr = sum(d.bmr_full_day_kcal for d in history) / len(history)

        if history:
            baseline_active = sum(d.active_kcal for d in history) / len(history)
        elif today_g is not None:
            baseline_active = today_g.active_kcal
        else:
            baseline_active = 0.0

        today_active = today_g.active_kcal if today_g is not None else baseline_active

        if mode == "daily_burn":
            workout_surplus = 0.0
            raw_kcal = baseline_bmr + today_active * active_factor + burn_offset + goal_delta
            source = "garmin_daily"
        else:
            workout_surplus = max(0.0, today_active - baseline_active)
            raw_kcal = (
                baseline_bmr
                + (baseline_active + workout_surplus) * active_factor
                + burn_offset
                + goal_delta
            )
            source = "garmin_rolling"

        min_kcal = max(500, int(cfg.garmin_min_calories_kcal))
        max_kcal = max(min_kcal, int(cfg.garmin_max_calories_kcal))
        effective_kcal = round(max(min_kcal, min(max_kcal, raw_kcal)))
        kcal_diff = effective_kcal - base.calories_kcal
        effective_carbs_g = round(max(0.0, base.carbs_g + kcal_diff / 4.0), 1)

        effective = NutritionTarget(
            effective_date=base.effective_date,
            calories_kcal=effective_kcal,
            protein_g=base.protein_g,
            carbs_g=effective_carbs_g,
            fat_g=base.fat_g,
            fiber_g=base.fiber_g,
            notes=base.notes,
            updated_at=base.updated_at,
        )
        info = DynamicTargetInfo(
            base_calories_kcal=base.calories_kcal,
            effective_calories_kcal=effective_kcal,
            adjustment_kcal=kcal_diff,
            baseline_bmr_kcal=round(baseline_bmr, 1),
            baseline_active_kcal=round(baseline_active, 1),
            today_active_kcal=round(today_active, 1),
            workout_surplus_kcal=round(workout_surplus, 1),
            goal_delta_kcal=goal_delta,
            active_factor=round(active_factor, 2),
            mode=mode,
            source=source,
            burn_offset_kcal=burn_offset,
        )
        return effective, info

    def get_food_dashboard_trends(
        self,
        end_local_date: str,
        fallback_cfg: FoodTrackerConfig | None = None,
        days: int = 14,
    ) -> dict[str, object]:
        """Build 14d energy-balance, weight EMA, weekly banking, and TDEE calibration payload."""
        cfg = fallback_cfg or FoodTrackerConfig()
        end_d = date.fromisoformat(end_local_date)
        span = max(7, min(60, days))
        start_d = end_d - timedelta(days=span - 1)
        start_str = start_d.isoformat()

        with self._connect() as conn:
            weight_rows = conn.execute(
                """
                SELECT local_date, weight_lbs, ema_weight_lbs
                FROM body_metrics
                WHERE local_date BETWEEN ? AND ?
                ORDER BY local_date ASC
                """,
                (start_str, end_local_date),
            ).fetchall()
        weights_by_date = {
            str(r["local_date"]): (float(r["weight_lbs"]), float(r["ema_weight_lbs"]))
            for r in weight_rows
        }

        series: list[dict[str, object]] = []
        weekly_net_kcal = 0.0
        weekly_net_days = 0
        weekly_intensity_min = 0
        intensity_goal_min = 150
        recent_7d_start = (end_d - timedelta(days=6)).isoformat()

        for offset in range(span):
            d_str = (start_d + timedelta(days=offset)).isoformat()
            day_sum = self.get_daily_summary(d_str, cfg)
            day_rt = self.get_garmin_runtime_settings(cfg, local_date=d_str)
            day_factor = float(day_rt["active_factor"])  # type: ignore[arg-type]
            day_offset = int(day_rt["burn_offset_kcal"])  # type: ignore[arg-type]
            g = day_sum.garmin
            w_pair = weights_by_date.get(d_str)
            intake = day_sum.total_calories_kcal if day_sum.meals else None
            raw_burn = g.total_burn_kcal if g is not None and g.bmr_full_day_kcal > 0 else None
            cal_burn = (
                round(g.bmr_full_day_kcal + g.active_kcal * day_factor + day_offset, 1)
                if g is not None and g.bmr_full_day_kcal > 0
                else None
            )
            burn = cal_burn if (cal_burn is not None and day_offset != 0) else raw_burn

            if d_str >= recent_7d_start:
                if g is not None:
                    weekly_intensity_min += int(g.moderate_min + 2 * g.vigorous_min)
                    if g.intensity_goal_min > 0:
                        intensity_goal_min = g.intensity_goal_min
                if intake is not None and burn is not None:
                    weekly_net_kcal += intake - burn
                    weekly_net_days += 1

            series.append(
                {
                    "local_date": d_str,
                    "intake_kcal": round(intake, 1) if intake is not None else None,
                    "target_kcal": day_sum.target.calories_kcal,
                    "garmin_burn_kcal": round(burn, 1) if burn is not None else None,
                    "raw_garmin_burn_kcal": round(raw_burn, 1) if raw_burn is not None else None,
                    "calibrated_burn_kcal": cal_burn,
                    "garmin_bmr_kcal": g.bmr_full_day_kcal if g else None,
                    "garmin_active_kcal": g.active_kcal if g else None,
                    "steps": g.steps if g else None,
                    "weight_lbs": w_pair[0] if w_pair else None,
                    "ema_weight_lbs": w_pair[1] if w_pair else None,
                }
            )

        calibration = self.compute_burn_calibration(end_local_date, cfg, window_days=14)

        return {
            "start_date": start_str,
            "end_date": end_local_date,
            "series": series,
            "weekly_net_kcal": round(weekly_net_kcal, 1) if weekly_net_days else None,
            "weekly_net_days": weekly_net_days,
            "weekly_projected_lbs": (
                round(weekly_net_kcal / 3500.0, 2) if weekly_net_days else None
            ),
            "weekly_intensity_min": weekly_intensity_min,
            "intensity_goal_min": intensity_goal_min,
            "calibration": calibration,
        }

    def build_coach_context(
        self,
        end_local_date: str,
        fallback_cfg: FoodTrackerConfig | None = None,
        days: int = 7,
    ) -> str:
        """Build a concise 7-day nutrition, weight, activity, sleep & HRV context block for AI chat."""
        cfg = fallback_cfg or FoodTrackerConfig()
        weekly = self.get_weekly_summary(end_local_date, cfg, days=days)
        today_sum = self.get_daily_summary(end_local_date, cfg)
        latest_w = self.get_latest_weight()
        tdee = self.estimate_adaptive_tdee(end_local_date)

        lines = [
            f"=== 7-Day Health & Nutrition Context (through {end_local_date}) ===",
            (
                f"Today intake: {today_sum.total_calories_kcal:.0f} / {today_sum.target.calories_kcal} kcal "
                f"({today_sum.total_protein_g:.0f}g P / {today_sum.total_carbs_g:.0f}g C / "
                f"{today_sum.total_fat_g:.0f}g F)"
            ),
        ]
        if weekly.days_logged > 0:
            lines.append(
                f"7d nutrition avg ({weekly.days_logged}d): {weekly.avg_calories_kcal:.0f} kcal/day | "
                f"{weekly.avg_protein_g:.0f}g P / {weekly.avg_carbs_g:.0f}g C / {weekly.avg_fat_g:.0f}g F"
            )
        if latest_w is not None:
            lines.append(
                f"Latest weight ({latest_w.local_date}): {latest_w.weight_lbs:.1f} lbs "
                f"(7d EMA: {latest_w.ema_weight_lbs:.1f} lbs)"
            )
        if tdee is not None:
            lines.append(
                f"Adaptive empirical TDEE ({tdee.window_days}d): ~{tdee.estimated_tdee_kcal:.0f} kcal/day"
            )
        if weekly.garmin_days_synced > 0 and weekly.avg_garmin_burn_kcal is not None:
            lines.append(
                f"Garmin 7d avg burn ({weekly.garmin_days_synced}d): {weekly.avg_garmin_burn_kcal:.0f} kcal/day "
                f"({weekly.avg_garmin_bmr_kcal or 0:.0f} BMR + {weekly.avg_garmin_active_kcal or 0:.0f} active)"
            )
        if today_sum.garmin is not None:
            g = today_sum.garmin
            sleep_h = f"{g.sleep_seconds / 3600:.1f}h" if g.sleep_seconds else "n/a"
            score_s = f" (score {g.sleep_score})" if g.sleep_score is not None else ""
            hrv_s = (
                f"{g.hrv_last_night_avg} ms ({g.hrv_status or 'n/a'})"
                if g.hrv_last_night_avg is not None
                else "n/a"
            )
            bb_s = f"{g.body_battery_current}/100" if g.body_battery_current is not None else "n/a"
            lines.append(
                f"Today recovery: Sleep {sleep_h}{score_s} | HRV {hrv_s} | "
                f"RHR {g.resting_hr or 'n/a'} bpm (7d avg {g.resting_hr_7d_avg or 'n/a'}) | "
                f"Body Battery {bb_s} | Steps {g.steps:,}"
            )
            if g.activities:
                acts = "; ".join(
                    f"{a.name} ({a.activity_type}, {round(a.duration_seconds / 60)}m, {a.calories_kcal:.0f} kcal)"
                    for a in g.activities
                )
                lines.append(f"Today workouts: {acts}")
        return "\n".join(lines)
