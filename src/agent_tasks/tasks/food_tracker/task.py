from __future__ import annotations

import asyncio
import hashlib
import io
import re
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import httpx
import structlog

from agent_tasks.channels.base import FoodChannelAdapter
from agent_tasks.common.config import (
    ControllerSettings,
    FoodTrackerConfig,
    TargetsConfig,
    parse_model_override,
)
from agent_tasks.common.models import TokenUsageSummary
from agent_tasks.common.state import StateStore
from agent_tasks.tasks.billing_guard.pricing import (
    compute_billing_cycle,
    compute_incremental_cost,
)
from agent_tasks.tasks.food_tracker.estimator import AgyNutritionEstimator, NutritionEstimator
from agent_tasks.tasks.food_tracker.garmin import (
    GarminAuthError,
    GarminClient,
    GarminPersonalClient,
)
from agent_tasks.tasks.food_tracker.models import (
    DailySummary,
    EstimatedMealItem,
    MealEntryRecord,
    MealEstimateSchema,
    NutritionTarget,
    WeeklySummary,
)
from agent_tasks.tasks.food_tracker.store import (
    NutritionStore,
    default_meal_slot_for_time,
    effective_local_date,
    slugify_food_name,
)

log = structlog.get_logger(__name__)

SCALE_MULTIPLIERS: tuple[float, ...] = (0.5, 0.75, 1.0, 1.25, 1.5)
_STAPLE_FASTPATH_RE = re.compile(
    r"^(?:(\d+(?:\.\d+)?)\s*x\s+)?([a-z0-9][a-z0-9_-]*)$",
    re.IGNORECASE,
)
_BARE_WEIGHT_RE = re.compile(r"^weight(?:\s+(\d+(?:\.\d+)?)(?:\s+(.*))?)?$", re.IGNORECASE)
_KEY_VAL_RE = re.compile(r"^([a-z]+)\s*[:=]\s*(\d+(?:\.\d+)?)$", re.IGNORECASE)

_SLOT_EMOJI = {
    "breakfast": "🌅",
    "lunch": "🥗",
    "dinner": "🍽️",
    "snack": "🍎",
}

FOOD_HELP_TEXT = (
    "*`#agy-food` Calorie & Macro Tracker Commands*\n"
    "• *Log a meal:* Drop a food photo (with optional caption) or type a meal description directly in `#agy-food`, or use `!eat <meal>` anywhere\n"
    '• *Revise a meal:* Reply inside any meal thread (`"no oil, 8oz chicken"`, or drop another photo) to update the estimate in place\n'
    "• *Portion scaling:* Tap `[0.5×]` `[0.75×]` `[1.0×]` `[1.25×]` `[1.5×]`, `[⭐ Save Staple]`, or `[🗑️ Delete]` on any meal card\n"
    "• *Instant saved staples (0 tokens):* `!eat <slug>` or `!eat 1.5x <slug>`\n"
    "• `!macros` / `!today [yesterday|YYYY-MM-DD]` — daily macro progress & meal list\n"
    "• `!week` — 7-day rolling averages, protein hit rate, & adaptive TDEE\n"
    "• `!goal` / `!goal <kcal> <protein_g> <carbs_g> <fat_g> [fiber_g]` — view or update targets\n"
    "• `!goal delta <±kcal>` — set Garmin calorie offset (e.g. `-500` cut, `0` maint, `+250` lean bulk)\n"
    "• `!garmin` / `!garmin sync` / `!garmin dynamic on|off` — Garmin burn, workouts, & dynamic targets\n"
    "• `!staples` / `!staple add <slug> | <name> | <serving> | <kcal> <p> <c> <f>` / `!staple delete <slug>`\n"
    "• `!weight [<lbs> [notes]]` — log morning weight & track 7-day EMA trend\n"
    "• `!remaining` / `!suggest [craving]` — remaining budget + Hydrogen meal suggestions"
)


class SlackFileScopeError(RuntimeError):
    """Raised when downloading a Slack file fails due to missing `files:read` OAuth scope."""


def _progress_bar(current: float, target: float, width: int = 10) -> str:
    if target <= 0:
        return "[" + "░" * width + "]"
    ratio = max(0.0, min(1.0, current / target))
    filled = round(ratio * width)
    return "[" + ("█" * filled) + ("░" * (width - filled)) + "]"


def _fmt_remaining(val: float, unit: str) -> str:
    if val >= 0:
        return f"{val:.0f}{unit} left"
    return f"{abs(val):.0f}{unit} over"


def _format_garmin_context_line(summary: DailySummary) -> str:
    parts: list[str] = []
    if summary.garmin is not None:
        g = summary.garmin
        parts.append(
            f"⌚ *Garmin Burn:* `{g.bmr_full_day_kcal:.0f}` BMR + `{g.active_kcal:.0f}` active "
            f"= *{g.total_burn_kcal:.0f} kcal* ({g.steps:,} steps)"
        )
    if summary.dynamic_target is not None:
        dt = summary.dynamic_target
        delta_sign = f"{dt.goal_delta_kcal:+d}" if dt.goal_delta_kcal != 0 else "±0"
        surplus = (
            f", +{dt.workout_surplus_kcal:.0f} workout" if dt.workout_surplus_kcal >= 1.0 else ""
        )
        cal_note = f", cal `{dt.burn_offset_kcal:+d}`" if dt.burn_offset_kcal else ""
        parts.append(
            f"🎯 *Dynamic Goal:* `{dt.effective_calories_kcal} kcal` "
            f"(baseline `{dt.baseline_bmr_kcal:.0f}` BMR + `{dt.baseline_active_kcal:.0f}` "
            f"active ×{dt.active_factor:g}{surplus}{cal_note}, offset `{delta_sign}`)"
        )
    return "\n".join(parts)


def build_meal_card_blocks(
    entry: MealEntryRecord,
    summary: DailySummary,
    *,
    cost_line: str = "",
) -> tuple[str, list[dict[str, Any]]]:
    """Build Slack Block Kit blocks and fallback notification text for a logged meal."""
    fallback = (
        f"{entry.title}: {entry.total_calories_kcal:.0f} kcal "
        f"({entry.total_protein_g:.0f}P/{entry.total_carbs_g:.0f}C/{entry.total_fat_g:.0f}F)"
    )

    bar = _progress_bar(summary.total_calories_kcal, summary.target.calories_kcal)
    daily_text = (
        f"📊 *Daily Total ({summary.local_date}):* `{bar}` "
        f"*{summary.total_calories_kcal:.0f} / {summary.target.calories_kcal} kcal* "
        f"({_fmt_remaining(summary.remaining_calories_kcal, ' kcal')})\n"
        f"Protein: *{summary.total_protein_g:.0f} / {summary.target.protein_g:.0f}g* "
        f"({_fmt_remaining(summary.remaining_protein_g, 'g')})  |  "
        f"Carbs: {summary.total_carbs_g:.0f} / {summary.target.carbs_g:.0f}g  |  "
        f"Fat: {summary.total_fat_g:.0f} / {summary.target.fat_g:.0f}g"
    )
    garmin_ctx = _format_garmin_context_line(summary)
    if garmin_ctx:
        daily_text = f"{daily_text}\n{garmin_ctx}"

    if entry.status == "deleted":
        return (
            f"Deleted meal #{entry.id}: {entry.title}",
            [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"🗑️ ~*{entry.title}*~ _(meal #{entry.id} deleted)_\n\n{daily_text}",
                    },
                }
            ],
        )

    slot_icon = _SLOT_EMOJI.get(entry.meal_slot, "🍽️")
    scale_badge = f" • *{entry.scale_factor:g}×*" if abs(entry.scale_factor - 1.0) > 1e-6 else ""
    header = (
        f"{slot_icon} *#{entry.id} — {entry.title}*\n"
        f"`{entry.meal_slot}` • `{entry.local_date}` • confidence: `{entry.confidence}`{scale_badge}\n"
        f"*{entry.total_calories_kcal:.0f} kcal*  |  "
        f"*{entry.total_protein_g:.0f}g P* • {entry.total_carbs_g:.0f}g C • "
        f"{entry.total_fat_g:.0f}g F • {entry.total_fiber_g:.0f}g fiber"
    )

    item_lines: list[str] = []
    for item in entry.items:
        k = item.calories_kcal * entry.scale_factor
        p = item.protein_g * entry.scale_factor
        c = item.carbs_g * entry.scale_factor
        f = item.fat_g * entry.scale_factor
        grams = (
            f", {item.estimated_grams * entry.scale_factor:.0f}g"
            if item.estimated_grams is not None
            else ""
        )
        item_lines.append(
            f"• *{item.name}* ({item.quantity_desc}{grams}) — "
            f"{k:.0f} kcal ({p:.0f}P/{c:.0f}C/{f:.0f}F)"
        )

    body_parts = [header]
    if item_lines:
        body_parts.append("\n".join(item_lines))
    if entry.assumptions_note:
        body_parts.append(f"_Assumptions: {entry.assumptions_note}_")
    if entry.status == "needs_clarification" and entry.clarification_question:
        body_parts.append(f"❓ *Quick check:* {entry.clarification_question}")

    blocks: list[dict[str, Any]] = [
        {"type": "section", "text": {"type": "mrkdwn", "text": "\n\n".join(body_parts)[:2950]}},
        {"type": "section", "text": {"type": "mrkdwn", "text": daily_text[:2950]}},
    ]

    scale_buttons: list[dict[str, Any]] = []
    for mult in SCALE_MULTIPLIERS:
        btn: dict[str, Any] = {
            "type": "button",
            "text": {"type": "plain_text", "text": f"{mult:g}×"},
            "action_id": f"food_scale_{round(mult * 100):03d}",
            "value": f"{entry.id}:{mult:g}",
        }
        if abs(entry.scale_factor - mult) < 0.01:
            btn["style"] = "primary"
        scale_buttons.append(btn)

    blocks.append({"type": "actions", "elements": scale_buttons})
    blocks.append(
        {
            "type": "actions",
            "elements": [
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "⭐ Save Staple"},
                    "action_id": "food_save_staple",
                    "value": str(entry.id),
                },
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "🗑️ Delete"},
                    "style": "danger",
                    "action_id": "food_delete",
                    "value": str(entry.id),
                },
            ],
        }
    )
    blocks.append(
        {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": (
                        f"{cost_line}Model: `{entry.model_used}` • "
                        "Reply in thread (or drop a photo) to adjust ingredients/portions."
                    ),
                }
            ],
        }
    )
    return fallback, blocks


def format_daily_summary(summary: DailySummary, *, heading: str = "Daily Nutrition") -> str:
    """Format a concise Slack mrkdwn daily summary."""
    bar_cal = _progress_bar(summary.total_calories_kcal, summary.target.calories_kcal)
    bar_p = _progress_bar(summary.total_protein_g, summary.target.protein_g)
    lines = [
        f"*{heading} (`{summary.local_date}`)*",
        (
            f"• Calories: `{bar_cal}` *{summary.total_calories_kcal:.0f} / "
            f"{summary.target.calories_kcal} kcal* "
            f"({_fmt_remaining(summary.remaining_calories_kcal, ' kcal')})"
        ),
        (
            f"• Protein: `{bar_p}` *{summary.total_protein_g:.0f} / "
            f"{summary.target.protein_g:.0f}g* "
            f"({_fmt_remaining(summary.remaining_protein_g, 'g')})"
        ),
        (
            f"• Carbs: *{summary.total_carbs_g:.0f} / {summary.target.carbs_g:.0f}g*  |  "
            f"Fat: *{summary.total_fat_g:.0f} / {summary.target.fat_g:.0f}g*  |  "
            f"Fiber: *{summary.total_fiber_g:.0f} / {summary.target.fiber_g:.0f}g*"
        ),
    ]
    garmin_ctx = _format_garmin_context_line(summary)
    if garmin_ctx:
        lines.append(garmin_ctx)
    if not summary.meals:
        lines.append("\n_No meals logged yet for this day._")
    else:
        lines.append("\n*Logged Meals:*")
        for meal in summary.meals:
            scale = f" [{meal.scale_factor:g}×]" if abs(meal.scale_factor - 1.0) > 1e-6 else ""
            lines.append(
                f"• `#{meal.id}` [{meal.meal_slot}] *{meal.title}*{scale} — "
                f"{meal.total_calories_kcal:.0f} kcal "
                f"({meal.total_protein_g:.0f}P/{meal.total_carbs_g:.0f}C/{meal.total_fat_g:.0f}F)"
            )
    return "\n".join(lines)


def format_weekly_summary(
    weekly: WeeklySummary,
    *,
    latest_weight_line: str = "",
    tdee_line: str = "",
) -> str:
    """Format a 7-day nutrition adherence & body trend report."""
    lines = [
        f"*Weekly Nutrition Summary (`{weekly.start_date}` → `{weekly.end_date}`)*",
        f"• Days logged: *{weekly.days_logged} / {len(weekly.daily_totals)}*",
    ]
    if weekly.days_logged > 0:
        lines.extend(
            [
                (
                    f"• Daily average (logged days): *{weekly.avg_calories_kcal:.0f} kcal*  |  "
                    f"*{weekly.avg_protein_g:.0f}g P* / {weekly.avg_carbs_g:.0f}g C / "
                    f"{weekly.avg_fat_g:.0f}g F / {weekly.avg_fiber_g:.0f}g fiber"
                ),
                (
                    f"• Protein target hit (≥95%): *{weekly.protein_hit_days} / "
                    f"{weekly.days_logged} days*  |  "
                    f"Calories within ±10%: *{weekly.calorie_adherent_days} / "
                    f"{weekly.days_logged} days*"
                ),
            ]
        )
        lines.append("\n*Daily Breakdown:*")
        for day in weekly.daily_totals:
            if not day.meals:
                lines.append(f"• `{day.local_date}` — _no meals logged_")
            else:
                lines.append(
                    f"• `{day.local_date}` — *{day.total_calories_kcal:.0f}* / "
                    f"{day.target.calories_kcal} kcal ({day.total_protein_g:.0f}P / "
                    f"{day.total_carbs_g:.0f}C / {day.total_fat_g:.0f}F)"
                )
    if latest_weight_line:
        lines.append(f"\n{latest_weight_line}")
    if tdee_line:
        lines.append(tdee_line)
    if weekly.garmin_days_synced > 0 and weekly.avg_garmin_burn_kcal is not None:
        lines.append(
            f"⌚ *Garmin Avg Burn ({weekly.garmin_days_synced}d):* "
            f"`~{weekly.avg_garmin_burn_kcal:.0f} kcal/day` "
            f"(`{weekly.avg_garmin_bmr_kcal or 0:.0f}` BMR + "
            f"`{weekly.avg_garmin_active_kcal or 0:.0f}` active)"
        )
    return "\n".join(lines)


def _parse_goal_args(args: str, current: NutritionTarget) -> NutritionTarget:
    tokens = args.strip().split()
    if not tokens:
        raise ValueError("Empty goal arguments")

    numeric: list[float] = []
    kv: dict[str, float] = {}
    for tok in tokens:
        m = _KEY_VAL_RE.match(tok)
        if m:
            kv[m.group(1).lower()] = float(m.group(2))
        else:
            try:
                numeric.append(float(tok))
            except ValueError as exc:
                raise ValueError(
                    "Invalid `!goal` syntax. Use `!goal <kcal> <protein_g> <carbs_g> <fat_g> "
                    "[fiber_g]` or `!goal kcal=2100 p=180 c=200 f=65`."
                ) from exc

    if kv:
        kcal = round(kv.get("kcal", kv.get("cal", kv.get("calories", current.calories_kcal))))
        protein = float(kv.get("p", kv.get("protein", current.protein_g)))
        carbs = float(kv.get("c", kv.get("carbs", current.carbs_g)))
        fat = float(kv.get("f", kv.get("fat", current.fat_g)))
        fiber = float(kv.get("fiber", current.fiber_g))
        return NutritionTarget(
            effective_date=current.effective_date,
            calories_kcal=max(500, kcal),
            protein_g=max(0.0, protein),
            carbs_g=max(0.0, carbs),
            fat_g=max(0.0, fat),
            fiber_g=max(0.0, fiber),
        )

    if len(numeric) not in (4, 5):
        raise ValueError(
            "Expected 4 or 5 numbers: `!goal <kcal> <protein_g> <carbs_g> <fat_g> [fiber_g]`."
        )
    return NutritionTarget(
        effective_date=current.effective_date,
        calories_kcal=max(500, round(numeric[0])),
        protein_g=max(0.0, numeric[1]),
        carbs_g=max(0.0, numeric[2]),
        fat_g=max(0.0, numeric[3]),
        fiber_g=max(0.0, numeric[4]) if len(numeric) == 5 else current.fiber_g,
    )


def _save_compressed_image(raw_bytes: bytes, dest_base: Path, orig_ext: str) -> Path:
    dest_base.parent.mkdir(parents=True, exist_ok=True)
    try:
        from PIL import Image as PILImage
        from PIL import ImageOps

        with PILImage.open(io.BytesIO(raw_bytes)) as img:
            transposed = ImageOps.exif_transpose(img)
            if transposed.mode not in ("RGB", "RGBA"):
                transposed = transposed.convert("RGB")
            elif transposed.mode == "RGBA":
                bg = PILImage.new("RGB", transposed.size, (255, 255, 255))
                bg.paste(transposed, mask=transposed.split()[3])
                transposed = bg
            transposed.thumbnail((1280, 1280))
            out_path = dest_base.with_suffix(".webp")
            transposed.save(out_path, format="WEBP", quality=82, method=4)
            return out_path
    except (ImportError, OSError, ValueError):
        clean_ext = orig_ext if orig_ext.startswith(".") else f".{orig_ext or 'jpg'}"
        out_path = dest_base.with_suffix(clean_ext)
        out_path.write_bytes(raw_bytes)
        return out_path


class FoodTrackerTask:
    """Long-lived multimodal food, macro, and bodyweight tracker for `#agy-food`."""

    name = "food-tracker"

    def __init__(
        self,
        *,
        settings: ControllerSettings,
        state: StateStore,
        channel: FoodChannelAdapter,
        nutrition: NutritionStore | None = None,
        estimator: NutritionEstimator | None = None,
        garmin: GarminClient | None = None,
        push_manager: Any | None = None,
        heartbeat_cb: Callable[[], None] | None = None,
        http_transport: httpx.AsyncBaseTransport | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.settings = settings
        self.state = state
        self.channel = channel
        plugin_nutrition_db = settings.state_dir / "plugins" / "health" / "nutrition.sqlite3"
        default_db = (
            plugin_nutrition_db
            if plugin_nutrition_db.parent.exists()
            else (settings.state_dir / "nutrition.sqlite3")
        )
        self.nutrition = nutrition or NutritionStore(default_db)
        self.estimator = estimator or AgyNutritionEstimator(default_model=settings.worker_model)
        self.push_manager = push_manager
        self.heartbeat_cb = heartbeat_cb
        self._http_transport = http_transport
        self._clock = clock or (lambda: datetime.now(UTC))
        self.photos_dir = settings.state_dir / "food_photos"
        self.backups_dir = settings.state_dir / "backups" / "nutrition"
        if garmin is not None:
            self.garmin: GarminClient | None = garmin
        elif settings.garmin_api_key.strip():
            cfg = self.load_targets().food_tracker
            self.garmin = GarminPersonalClient(
                api_key=settings.garmin_api_key.strip(),
                base_url=cfg.garmin_base_url,
                http_transport=http_transport,
            )
        else:
            self.garmin = None
        self._last_garmin_sync_at: datetime | None = None
        self._last_garmin_error_at: datetime | None = None
        self._garmin_bootstrapped: bool = False

    def load_targets(self) -> TargetsConfig:
        if self.settings.targets_config_path.exists():
            return TargetsConfig.from_yaml(self.settings.targets_config_path)
        return TargetsConfig()

    def _now_utc(self) -> datetime:
        now = self._clock()
        return now.replace(tzinfo=UTC) if now.tzinfo is None else now.astimezone(UTC)

    def _today_local_date(self, cfg: FoodTrackerConfig, now_utc: datetime | None = None) -> str:
        return effective_local_date(
            now_utc or self._now_utc(),
            tz_name=cfg.timezone,
            rollover_hour=cfg.day_rollover_hour,
        )

    def _record_usage(
        self,
        *,
        issue_id: str,
        turn: int,
        model: str,
        usage: TokenUsageSummary,
        targets: TargetsConfig,
        note: str,
    ) -> str:
        if usage.total_tokens <= 0 and usage.prompt_tokens <= 0 and usage.output_tokens <= 0:
            return ""
        cycle = compute_billing_cycle(self._now_utc(), targets.billing.cycle_anchor_day)
        delta, cost_usd = compute_incremental_cost(
            model=model,
            cumulative_usage=usage,
            prior_tokens=(0, 0, 0, 0),
            billing_cfg=targets.billing,
        )
        self.state.record_usage_entry(
            cycle_key=cycle.cycle_key,
            issue_id=issue_id,
            turn=turn,
            conversation_id=None,
            task_kind="food_tracker",
            model=model,
            prompt_tokens=delta.prompt_tokens,
            cached_tokens=delta.cached_tokens,
            output_tokens=delta.output_tokens,
            thoughts_tokens=delta.thoughts_tokens,
            cost_usd=cost_usd,
            note=note,
        )
        return f"Est. cost: `${cost_usd:.4f}` ({delta.total_tokens:,} tok) • "

    async def _download_slack_images(
        self,
        image_files: list[dict[str, Any]],
        *,
        ts: str,
        now_utc: datetime,
    ) -> list[tuple[Path, str]]:
        if not image_files:
            return []
        month_dir = self.photos_dir / now_utc.strftime("%Y-%m")
        ts_slug = re.sub(r"[^a-zA-Z0-9_-]+", "-", ts or "img").strip("-")
        saved: list[tuple[Path, str]] = []
        remote_items: list[tuple[int, dict[str, Any]]] = []

        for idx, file_obj in enumerate(image_files):
            raw_bytes = file_obj.get("raw_bytes")
            local_path_raw = file_obj.get("local_path")
            if isinstance(raw_bytes, (bytes, bytearray)) and len(raw_bytes) > 0:
                raw_name = str(file_obj.get("name") or file_obj.get("filetype") or "meal.jpg")
                ext = Path(raw_name).suffix or f".{file_obj.get('filetype') or 'jpg'}"
                dest_base = month_dir / f"{ts_slug}-{idx + 1}"
                out_path = _save_compressed_image(bytes(raw_bytes), dest_base, ext)
                relpath = str(out_path.relative_to(self.settings.state_dir))
                saved.append((out_path, relpath))
                continue
            if local_path_raw:
                src_path = Path(str(local_path_raw))
                if src_path.is_file():
                    raw_name = str(file_obj.get("name") or src_path.name or "meal.jpg")
                    ext = Path(raw_name).suffix or src_path.suffix or ".jpg"
                    dest_base = month_dir / f"{ts_slug}-{idx + 1}"
                    out_path = _save_compressed_image(src_path.read_bytes(), dest_base, ext)
                    relpath = str(out_path.relative_to(self.settings.state_dir))
                    saved.append((out_path, relpath))
                    continue
            remote_items.append((idx, file_obj))

        if not remote_items:
            return saved

        client_kwargs: dict[str, Any] = {"follow_redirects": True, "timeout": 30.0}
        if self._http_transport is not None:
            client_kwargs["transport"] = self._http_transport

        async with httpx.AsyncClient(**client_kwargs) as client:
            for idx, file_obj in remote_items:
                url = str(
                    file_obj.get("url_private_download") or file_obj.get("url_private") or ""
                ).strip()
                if not url:
                    continue
                resp = await client.get(
                    url,
                    headers={"Authorization": f"Bearer {self.settings.slack_bot_token}"},
                )
                ctype = str(resp.headers.get("content-type") or "").lower()
                if resp.status_code in (401, 403) or ctype.startswith("text/html"):
                    raise SlackFileScopeError(
                        "Cannot download your meal photo (Slack returned HTTP "
                        f"{resp.status_code} / `{ctype or 'html'}`). Add the `files:read` bot "
                        "scope from `slack-app-manifest.yaml` and reinstall the Slack app."
                    )
                resp.raise_for_status()
                raw_name = str(file_obj.get("name") or file_obj.get("filetype") or "meal.jpg")
                ext = Path(raw_name).suffix or f".{file_obj.get('filetype') or 'jpg'}"
                dest_base = month_dir / f"{ts_slug}-{idx + 1}"
                out_path = _save_compressed_image(resp.content, dest_base, ext)
                relpath = str(out_path.relative_to(self.settings.state_dir))
                saved.append((out_path, relpath))
        return saved

    async def handle_message(
        self,
        cleaned_text: str,
        user: str,
        channel_id: str,
        message_ts: str,
        image_files: list[dict[str, Any]],
        is_food_channel: bool,
    ) -> bool:
        """Handle explicit food commands anywhere or zero-prefix logs in `#agy-food`."""
        text = cleaned_text.strip()
        lower = text.lower()
        targets = self.load_targets()
        cfg = targets.food_tracker

        cmd, _, _rest = lower.partition(" ")
        raw_rest = text[len(cmd) :].strip() if " " in text else ""

        if not image_files:
            if cmd == "!food":
                if not raw_rest or raw_rest.lower() == "help":
                    await self.channel.post_thread_message(
                        message_ts, FOOD_HELP_TEXT, channel_id=channel_id
                    )
                    return True
                # Support `!food macros`, `!food week`, etc. by rewriting to `!<sub>`
                return await self.handle_message(
                    f"!{raw_rest}",
                    user,
                    channel_id,
                    message_ts,
                    image_files,
                    is_food_channel,
                )

            if cmd in ("!macros", "!today") or (is_food_channel and lower in ("macros", "today")):
                if not cfg.enabled:
                    await self.channel.post_thread_message(
                        message_ts, "Food tracker is currently disabled.", channel_id=channel_id
                    )
                    return True
                target_date = self._resolve_query_date(raw_rest, cfg)
                summary = self.nutrition.get_daily_summary(target_date, cfg)
                await self.channel.post_thread_message(
                    message_ts, format_daily_summary(summary), channel_id=channel_id
                )
                return True

            if cmd == "!week" or (is_food_channel and lower == "week"):
                target_date = self._today_local_date(cfg)
                weekly = self.nutrition.get_weekly_summary(target_date, cfg)
                latest_w = self.nutrition.get_latest_weight()
                tdee = self.nutrition.estimate_adaptive_tdee(target_date)
                weight_line = (
                    f"⚖️ *Latest Weight (`{latest_w.local_date}`):* `{latest_w.weight_lbs:.1f} lbs` "
                    f"(7d EMA trend: `{latest_w.ema_weight_lbs:.1f} lbs`)"
                    if latest_w
                    else ""
                )
                tdee_line = (
                    f"🔥 *Adaptive TDEE ({tdee.window_days}d):* `~{tdee.estimated_tdee_kcal:.0f} kcal/day` "
                    f"(avg intake `{tdee.avg_daily_kcal:.0f} kcal`, EMA Δ `{tdee.weight_delta_lbs:+.2f} lbs` "
                    f"over `{tdee.span_days}d`)"
                    if tdee
                    else ""
                )
                await self.channel.post_thread_message(
                    message_ts,
                    format_weekly_summary(
                        weekly, latest_weight_line=weight_line, tdee_line=tdee_line
                    ),
                    channel_id=channel_id,
                )
                return True

            if cmd == "!goal":
                today = self._today_local_date(cfg)
                current = self.nutrition.get_target_for_date(today, cfg)
                if not raw_rest:
                    effective, dyn_info = self.nutrition.resolve_effective_target(today, cfg)
                    dyn_line = ""
                    if dyn_info is not None:
                        sign = (
                            f"{dyn_info.goal_delta_kcal:+d}" if dyn_info.goal_delta_kcal else "±0"
                        )
                        dyn_line = (
                            f"\n🎯 *Effective Today (Garmin `{dyn_info.mode}`):* "
                            f"`{effective.calories_kcal} kcal` | `{effective.protein_g:.0f}g P` / "
                            f"`{effective.carbs_g:.0f}g C` / `{effective.fat_g:.0f}g F` "
                            f"(offset `{sign} kcal`)"
                        )
                    await self.channel.post_thread_message(
                        message_ts,
                        (
                            f"*Base Nutrition Target (`{current.effective_date}`):* "
                            f"`{current.calories_kcal} kcal` | `{current.protein_g:.0f}g P` / "
                            f"`{current.carbs_g:.0f}g C` / `{current.fat_g:.0f}g F` / "
                            f"`{current.fiber_g:.0f}g fiber`"
                            f"{dyn_line}\n"
                            "Update base with `!goal <kcal> <protein_g> <carbs_g> <fat_g> [fiber_g]` "
                            "or set Garmin offset with `!goal delta <±kcal>`."
                        ),
                        channel_id=channel_id,
                    )
                    return True
                goal_tokens = raw_rest.split()
                if len(goal_tokens) == 2 and goal_tokens[0].lower() in (
                    "delta",
                    "offset",
                    "deficit",
                    "surplus",
                ):
                    try:
                        delta_val = round(float(goal_tokens[1]))
                    except ValueError:
                        await self.channel.post_thread_message(
                            message_ts,
                            "Usage: `!goal delta <±kcal>`, e.g. `!goal delta -500` or `!goal delta 0`.",
                            channel_id=channel_id,
                        )
                        return True
                    if goal_tokens[0].lower() == "deficit" and delta_val > 0:
                        delta_val = -delta_val
                    self.nutrition.save_garmin_settings(
                        local_date=today, fallback_cfg=cfg, goal_delta_kcal=delta_val
                    )
                    effective, dyn_info = self.nutrition.resolve_effective_target(today, cfg)
                    eff_str = (
                        f" Effective today: *{effective.calories_kcal} kcal* "
                        f"({effective.protein_g:.0f}P/{effective.carbs_g:.0f}C/{effective.fat_g:.0f}F)."
                        if dyn_info is not None
                        else " (Garmin dynamic targets are currently inactive or not yet synced.)"
                    )
                    await self.channel.post_thread_message(
                        message_ts,
                        f"Updated Garmin calorie offset to `{delta_val:+d} kcal`.{eff_str}",
                        channel_id=channel_id,
                    )
                    return True
                try:
                    parsed = _parse_goal_args(raw_rest, current)
                except ValueError as exc:
                    await self.channel.post_thread_message(
                        message_ts, str(exc), channel_id=channel_id
                    )
                    return True
                saved_target = self.nutrition.set_target(
                    effective_date=today,
                    calories_kcal=parsed.calories_kcal,
                    protein_g=parsed.protein_g,
                    carbs_g=parsed.carbs_g,
                    fat_g=parsed.fat_g,
                    fiber_g=parsed.fiber_g,
                    notes=f"updated by {user}",
                )
                await self.channel.post_thread_message(
                    message_ts,
                    (
                        f"Updated nutrition targets effective `{saved_target.effective_date}`: "
                        f"*{saved_target.calories_kcal} kcal* | *{saved_target.protein_g:.0f}g P* / "
                        f"{saved_target.carbs_g:.0f}g C / {saved_target.fat_g:.0f}g F / "
                        f"{saved_target.fiber_g:.0f}g fiber."
                    ),
                    channel_id=channel_id,
                )
                return True

            if cmd in ("!garmin", "!sync") or (
                is_food_channel and (lower in ("garmin", "sync") or lower.startswith("garmin "))
            ):
                garmin_args = (
                    "sync"
                    if cmd == "!sync" or lower == "sync"
                    else (raw_rest if cmd == "!garmin" else text[len("garmin") :].strip())
                )
                msg = await self._handle_garmin_command(garmin_args, cfg)
                await self.channel.post_thread_message(message_ts, msg, channel_id=channel_id)
                return True

            if cmd == "!staples" or (is_food_channel and lower == "staples"):
                staples = self.nutrition.list_saved_foods()
                if not staples:
                    msg = (
                        "No saved staples yet. Click `[⭐ Save Staple]` on any meal card "
                        "or add one via `!staple add <slug> | <name> | <serving> | <kcal> <p> <c> <f>`."
                    )
                else:
                    lines = [
                        "*Saved Food Staples* (log anytime with `!eat <slug>` or `1.5x <slug>`):"
                    ]
                    for sf in staples:
                        lines.append(
                            f"• `{sf.slug}` — *{sf.name}* ({sf.serving_desc}): "
                            f"{sf.calories_kcal:.0f} kcal ({sf.protein_g:.0f}P/{sf.carbs_g:.0f}C/{sf.fat_g:.0f}F)"
                        )
                    msg = "\n".join(lines)
                await self.channel.post_thread_message(message_ts, msg, channel_id=channel_id)
                return True

            if cmd == "!staple":
                msg = self._handle_staple_admin(raw_rest)
                await self.channel.post_thread_message(message_ts, msg, channel_id=channel_id)
                return True

            weight_match = _BARE_WEIGHT_RE.match(text) if is_food_channel else None
            if cmd == "!weight" or (weight_match is not None and text.lower() != "weight"):
                weight_args = raw_rest if cmd == "!weight" else text[len("weight") :].strip()
                msg = self._handle_weight_command(weight_args, cfg)
                await self.channel.post_thread_message(message_ts, msg, channel_id=channel_id)
                return True
            if is_food_channel and lower == "weight":
                msg = self._handle_weight_command("", cfg)
                await self.channel.post_thread_message(message_ts, msg, channel_id=channel_id)
                return True

            if cmd in ("!remaining", "!suggest") or (
                is_food_channel
                and (lower in ("remaining", "suggest") or lower.startswith("suggest "))
            ):
                note = (
                    raw_rest
                    if cmd.startswith("!")
                    else (text[len("suggest") :].strip() if lower.startswith("suggest") else "")
                )
                await self._handle_remaining_or_suggest(
                    suggest_mode=(cmd == "!suggest" or lower.startswith("suggest") or bool(note)),
                    note=note,
                    channel_id=channel_id,
                    message_ts=message_ts,
                    targets=targets,
                )
                return True

        # Meal logging triggers: `!eat ...`, `!log ...`, or ANY message/photo in `#agy-food`
        meal_text: str | None = None
        if cmd in ("!eat", "!log"):
            meal_text = raw_rest
        elif is_food_channel and not lower.startswith("!"):
            meal_text = text

        if meal_text is None:
            return False

        if not cfg.enabled:
            await self.channel.post_thread_message(
                message_ts, "Food tracker is currently disabled.", channel_id=channel_id
            )
            return True

        if not meal_text and not image_files:
            await self.channel.post_thread_message(
                message_ts,
                "Include a meal photo or description, e.g. `!eat 2 eggs and sourdough toast`.",
                channel_id=channel_id,
            )
            return True

        await self._log_new_meal(
            raw_text=meal_text,
            user=user,
            channel_id=channel_id,
            message_ts=message_ts,
            image_files=image_files,
            targets=targets,
        )
        return True

    def _resolve_query_date(self, arg: str, cfg: FoodTrackerConfig) -> str:
        clean = arg.strip().lower()
        today_str = self._today_local_date(cfg)
        if not clean or clean == "today":
            return today_str
        if clean == "yesterday":
            return (datetime.fromisoformat(today_str).date() - timedelta(days=1)).isoformat()
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", clean):
            return clean
        return today_str

    def _handle_staple_admin(self, args: str) -> str:
        clean = args.strip()
        if not clean:
            return (
                "Usage:\n"
                "• `!staple add <slug> | <name> | <serving_desc> | <kcal> <p> <c> <f> [fiber]`\n"
                "• `!staple delete <slug>`"
            )
        sub, _, rest = clean.partition(" ")
        sub_l = sub.lower()
        if sub_l in ("delete", "del", "rm", "remove"):
            slug = rest.strip()
            if not slug:
                return "Usage: `!staple delete <slug>`"
            if self.nutrition.delete_saved_food(slug):
                return f"Deleted saved staple `{slugify_food_name(slug)}`."
            return f"No saved staple found for `{slugify_food_name(slug)}`."

        if sub_l == "add":
            parts = [p.strip() for p in rest.split("|")]
            if len(parts) == 4:
                slug_raw, name, serving_desc, macro_str = parts
                nums = [float(x) for x in macro_str.split()]
            else:
                tokens = rest.split()
                if len(tokens) not in (5, 6):
                    return (
                        "Usage: `!staple add <slug> | <name> | <serving_desc> | "
                        "<kcal> <p> <c> <f> [fiber]`"
                    )
                slug_raw = tokens[0]
                name = tokens[0].replace("-", " ").title()
                serving_desc = "1 serving"
                nums = [float(x) for x in tokens[1:]]

            if len(nums) not in (4, 5):
                return "Macros must provide 4 or 5 numbers: `<kcal> <p> <c> <f> [fiber]`."
            sf = self.nutrition.upsert_saved_food(
                slug=slug_raw,
                name=name,
                serving_desc=serving_desc,
                calories_kcal=nums[0],
                protein_g=nums[1],
                carbs_g=nums[2],
                fat_g=nums[3],
                fiber_g=nums[4] if len(nums) == 5 else 0.0,
            )
            return (
                f"Saved staple `{sf.slug}` (*{sf.name}*, {sf.serving_desc}): "
                f"{sf.calories_kcal:.0f} kcal ({sf.protein_g:.0f}P/{sf.carbs_g:.0f}C/{sf.fat_g:.0f}F)."
            )

        return "Unknown `!staple` subcommand. Use `!staple add ...` or `!staple delete <slug>`."

    def _handle_weight_command(self, args: str, cfg: FoodTrackerConfig) -> str:
        clean = args.strip()
        today = self._today_local_date(cfg)
        if not clean:
            latest = self.nutrition.get_latest_weight()
            if latest is None:
                return "No weights logged yet. Log with `!weight 182.4`."
            tdee = self.nutrition.estimate_adaptive_tdee(today)
            msg = (
                f"⚖️ *Latest Weight (`{latest.local_date}`):* `{latest.weight_lbs:.1f} lbs` "
                f"(7d EMA trend: `{latest.ema_weight_lbs:.1f} lbs`)"
            )
            if tdee is not None:
                msg += f"\n🔥 *Adaptive TDEE ({tdee.window_days}d):* `~{tdee.estimated_tdee_kcal:.0f} kcal/day`"
            return msg

        tokens = clean.split(maxsplit=1)
        try:
            weight_val = float(tokens[0])
        except ValueError:
            return "Usage: `!weight <lbs> [notes]`, e.g. `!weight 182.4 morning fasted`."
        if weight_val <= 40 or weight_val >= 800:
            return "Weight must be between 40 and 800 lbs."
        notes = tokens[1] if len(tokens) > 1 else ""
        prev = self.nutrition.get_latest_weight()
        rec = self.nutrition.log_weight(local_date=today, weight_lbs=weight_val, notes=notes)
        delta_str = ""
        if prev is not None and prev.local_date != rec.local_date:
            delta_str = (
                f" ({rec.ema_weight_lbs - prev.ema_weight_lbs:+.2f} lbs EMA vs {prev.local_date})"
            )
        msg = (
            f"⚖️ Logged *{rec.weight_lbs:.1f} lbs* for `{rec.local_date}` — "
            f"7d EMA trend: `{rec.ema_weight_lbs:.1f} lbs`{delta_str}"
        )
        tdee = self.nutrition.estimate_adaptive_tdee(today)
        if tdee is not None:
            msg += f"\n🔥 *Adaptive TDEE ({tdee.window_days}d):* `~{tdee.estimated_tdee_kcal:.0f} kcal/day`"
        return msg

    async def _handle_remaining_or_suggest(
        self,
        *,
        suggest_mode: bool,
        note: str,
        channel_id: str,
        message_ts: str,
        targets: TargetsConfig,
    ) -> None:
        cfg = targets.food_tracker
        today = self._today_local_date(cfg)
        summary = self.nutrition.get_daily_summary(today, cfg)
        staples = self.nutrition.list_saved_foods()

        if not suggest_mode:
            lines = [
                (
                    f"*Remaining Today (`{summary.local_date}`):* "
                    f"*{summary.remaining_calories_kcal:.0f} kcal* | "
                    f"*{summary.remaining_protein_g:.0f}g P* | "
                    f"{summary.remaining_carbs_g:.0f}g C | "
                    f"{summary.remaining_fat_g:.0f}g F | "
                    f"{summary.remaining_fiber_g:.0f}g fiber"
                )
            ]
            fitting = [
                sf
                for sf in staples
                if summary.remaining_calories_kcal > 0
                and sf.calories_kcal <= summary.remaining_calories_kcal + 25
            ]
            if fitting:
                fitting.sort(key=lambda s: s.protein_g, reverse=True)
                lines.append("\n*Saved Staples That Fit:*")
                for sf in fitting[:5]:
                    lines.append(
                        f"• `!eat {sf.slug}` — *{sf.name}*: {sf.calories_kcal:.0f} kcal "
                        f"({sf.protein_g:.0f}P/{sf.carbs_g:.0f}C/{sf.fat_g:.0f}F)"
                    )
            lines.append("\nRun `!suggest [craving]` for tailored Hydrogen meal ideas.")
            await self.channel.post_thread_message(
                message_ts, "\n".join(lines), channel_id=channel_id
            )
            return

        model_override, clean_note = parse_model_override(note)
        await self.channel.add_reaction(
            channel_id=channel_id, ts=message_ts, name="hourglass_flowing_sand"
        )
        try:
            suggestion, usage, model_used = await self.estimator.suggest_remaining(
                summary=summary,
                saved_foods=staples,
                note=clean_note,
                model_override=model_override,
            )
        except Exception as exc:
            log.exception("food_tracker.suggest_failed", error=str(exc))
            await self.channel.post_thread_message(
                message_ts,
                f"Failed to generate macro suggestions: `{type(exc).__name__}: {exc}`",
                channel_id=channel_id,
            )
            return
        finally:
            await self.channel.remove_reaction(
                channel_id=channel_id, ts=message_ts, name="hourglass_flowing_sand"
            )

        cost_line = self._record_usage(
            issue_id=f"food-suggest-{hashlib.sha1(message_ts.encode()).hexdigest()[:8]}",
            turn=1,
            model=model_used,
            usage=usage,
            targets=targets,
            note="food_suggest",
        )
        header = (
            f"*Remaining:* {summary.remaining_calories_kcal:.0f} kcal | "
            f"{summary.remaining_protein_g:.0f}g P / {summary.remaining_carbs_g:.0f}g C / "
            f"{summary.remaining_fat_g:.0f}g F\n\n"
        )
        footer = f"\n\n_{cost_line}Model: `{model_used}`_"
        await self.channel.post_thread_message(
            message_ts, f"{header}{suggestion}{footer}", channel_id=channel_id
        )

    async def _log_new_meal(
        self,
        *,
        raw_text: str,
        user: str,
        channel_id: str,
        message_ts: str,
        image_files: list[dict[str, Any]],
        targets: TargetsConfig,
    ) -> None:
        cfg = targets.food_tracker
        now_utc = self._now_utc()
        local_date = self._today_local_date(cfg, now_utc)
        default_slot = default_meal_slot_for_time(now_utc, cfg.timezone)

        model_override, cleaned_desc = parse_model_override(raw_text)

        # Zero-token fast path when logging a saved staple by slug (e.g. `morning-shake`, `1.5x morning-shake`)
        if not image_files and cleaned_desc:
            m = _STAPLE_FASTPATH_RE.fullmatch(cleaned_desc)
            if m:
                mult = float(m.group(1)) if m.group(1) else 1.0
                sf = self.nutrition.get_saved_food(m.group(2))
                if sf is not None:
                    est = MealEstimateSchema(
                        title=sf.name,
                        meal_slot=default_slot,
                        confidence="high",
                        items=[
                            EstimatedMealItem(
                                name=sf.name,
                                quantity_desc=sf.serving_desc,
                                estimated_grams=sf.serving_grams,
                                calories_kcal=sf.calories_kcal,
                                protein_g=sf.protein_g,
                                carbs_g=sf.carbs_g,
                                fat_g=sf.fat_g,
                                fiber_g=sf.fiber_g,
                                source="saved_food",
                            )
                        ],
                        assumptions_note=f"Matched saved staple `{sf.slug}` (0 tokens).",
                    )
                    entry = self.nutrition.create_meal_entry(
                        local_date=local_date,
                        estimate=est,
                        raw_description=raw_text,
                        model_used="saved-food-fastpath",
                        slack_channel_id=channel_id,
                        slack_thread_ts=message_ts,
                        scale_factor=mult,
                        saved_food_ids={0: sf.id},
                    )
                    await self._post_or_update_meal_card(
                        entry=entry,
                        cfg=cfg,
                        channel_id=channel_id,
                        thread_ts=message_ts,
                        cost_line="Saved staple (0 tokens) • ",
                    )
                    return

        await self.channel.add_reaction(
            channel_id=channel_id, ts=message_ts, name="hourglass_flowing_sand"
        )
        try:
            saved_photos = await self._download_slack_images(
                image_files, ts=message_ts, now_utc=now_utc
            )
            photo_paths = [p for p, _ in saved_photos]
            photo_relpath = saved_photos[0][1] if saved_photos else None
            staples = self.nutrition.list_saved_foods()
            estimate, usage, model_used = await self.estimator.estimate_meal(
                description=cleaned_desc,
                photo_paths=photo_paths,
                local_date=local_date,
                default_slot=default_slot,
                saved_foods=staples,
                model_override=model_override,
            )
        except SlackFileScopeError as exc:
            await self.channel.post_thread_message(message_ts, str(exc), channel_id=channel_id)
            return
        except Exception as exc:
            log.exception("food_tracker.estimate_failed", user=user, error=str(exc))
            await self.channel.post_thread_message(
                message_ts,
                f"Failed to estimate meal macros: `{type(exc).__name__}: {exc}`",
                channel_id=channel_id,
            )
            return
        finally:
            await self.channel.remove_reaction(
                channel_id=channel_id, ts=message_ts, name="hourglass_flowing_sand"
            )

        entry = self.nutrition.create_meal_entry(
            local_date=local_date,
            estimate=estimate,
            raw_description=raw_text,
            model_used=model_used,
            photo_relpath=photo_relpath,
            slack_channel_id=channel_id,
            slack_thread_ts=message_ts,
        )
        cost_line = self._record_usage(
            issue_id=f"food-{entry.id}",
            turn=1,
            model=model_used,
            usage=usage,
            targets=targets,
            note=f"meal:{entry.title[:60]}",
        )
        await self._post_or_update_meal_card(
            entry=entry,
            cfg=cfg,
            channel_id=channel_id,
            thread_ts=message_ts,
            cost_line=cost_line,
        )

    async def handle_thread_reply(
        self,
        thread_ts: str,
        cleaned_text: str,
        user: str,
        channel_id: str,
        reply_ts: str,
        image_files: list[dict[str, Any]],
    ) -> bool:
        """Revise an existing meal when the user replies inside its Slack thread."""
        existing = self.nutrition.get_meal_by_thread_ts(thread_ts)
        if existing is None:
            return False

        if existing.status == "deleted":
            await self.channel.post_thread_message(
                thread_ts,
                f"Meal #{existing.id} was deleted. Post a new message to log a meal.",
                channel_id=channel_id,
            )
            return True

        targets = self.load_targets()
        cfg = targets.food_tracker
        now_utc = self._now_utc()
        model_override, feedback = parse_model_override(cleaned_text)
        if not feedback and not image_files:
            return True

        await self.channel.add_reaction(
            channel_id=channel_id, ts=reply_ts, name="hourglass_flowing_sand"
        )
        try:
            new_photos = await self._download_slack_images(
                image_files, ts=reply_ts, now_utc=now_utc
            )
            photo_paths = [p for p, _ in new_photos]
            new_relpath = new_photos[0][1] if new_photos else None
            if not photo_paths and existing.photo_relpath:
                orig_path = self.settings.state_dir / existing.photo_relpath
                if orig_path.exists():
                    photo_paths = [orig_path]

            staples = self.nutrition.list_saved_foods()
            estimate, usage, model_used = await self.estimator.revise_meal(
                existing_meal=existing,
                feedback_text=feedback or "(updated photo attached)",
                new_photo_paths=photo_paths,
                saved_foods=staples,
                model_override=model_override,
            )
        except SlackFileScopeError as exc:
            await self.channel.post_thread_message(thread_ts, str(exc), channel_id=channel_id)
            return True
        except Exception as exc:
            log.exception("food_tracker.revise_failed", entry_id=existing.id, error=str(exc))
            await self.channel.post_thread_message(
                thread_ts,
                f"Failed to revise meal #{existing.id}: `{type(exc).__name__}: {exc}`",
                channel_id=channel_id,
            )
            return True
        finally:
            await self.channel.remove_reaction(
                channel_id=channel_id, ts=reply_ts, name="hourglass_flowing_sand"
            )

        updated = self.nutrition.replace_meal_items(
            existing.id,
            estimate,
            model_used,
            photo_relpath=new_relpath,
        )
        cost_line = self._record_usage(
            issue_id=f"food-{updated.id}",
            turn=2,
            model=model_used,
            usage=usage,
            targets=targets,
            note=f"revise:{updated.title[:60]}",
        )
        await self._post_or_update_meal_card(
            entry=updated,
            cfg=cfg,
            channel_id=updated.slack_channel_id or channel_id,
            thread_ts=thread_ts,
            cost_line=cost_line,
            update_existing=True,
        )
        await self.channel.post_thread_message(
            thread_ts,
            (
                f"Updated meal `#{updated.id}` (*{updated.title}*): "
                f"*{updated.total_calories_kcal:.0f} kcal* "
                f"({updated.total_protein_g:.0f}P/{updated.total_carbs_g:.0f}C/{updated.total_fat_g:.0f}F)."
            ),
            channel_id=updated.slack_channel_id or channel_id,
        )
        return True

    async def handle_scale_action(self, entry_id: int, scale_factor: float, user: str) -> None:
        existing = self.nutrition.get_meal_entry(entry_id)
        if existing is None or existing.status == "deleted":
            return
        updated = self.nutrition.scale_meal_entry(entry_id, scale_factor)
        cfg = self.load_targets().food_tracker
        await self._post_or_update_meal_card(
            entry=updated,
            cfg=cfg,
            channel_id=updated.slack_channel_id or self.channel.resolve_channel("food"),
            thread_ts=updated.slack_thread_ts or "",
            update_existing=True,
        )

    async def handle_save_staple_action(self, entry_id: int, user: str) -> None:
        entry = self.nutrition.get_meal_entry(entry_id)
        if entry is None or entry.status == "deleted":
            return
        saved = self.nutrition.save_meal_as_staple(entry_id)
        if entry.slack_thread_ts:
            await self.channel.post_thread_message(
                entry.slack_thread_ts,
                (
                    f"⭐ Saved staple `{saved.slug}` (*{saved.name}* — "
                    f"{saved.calories_kcal:.0f} kcal | {saved.protein_g:.0f}P/"
                    f"{saved.carbs_g:.0f}C/{saved.fat_g:.0f}F). "
                    f"Log anytime with `!eat {saved.slug}` or `1.5x {saved.slug}`."
                ),
                channel_id=entry.slack_channel_id or self.channel.resolve_channel("food"),
            )

    async def handle_delete_action(self, entry_id: int, user: str) -> None:
        existing = self.nutrition.get_meal_entry(entry_id)
        if existing is None:
            return
        deleted = self.nutrition.soft_delete_meal(entry_id)
        cfg = self.load_targets().food_tracker
        await self._post_or_update_meal_card(
            entry=deleted,
            cfg=cfg,
            channel_id=deleted.slack_channel_id or self.channel.resolve_channel("food"),
            thread_ts=deleted.slack_thread_ts or "",
            update_existing=True,
        )

    async def _post_or_update_meal_card(
        self,
        *,
        entry: MealEntryRecord,
        cfg: FoodTrackerConfig,
        channel_id: str,
        thread_ts: str,
        cost_line: str = "",
        update_existing: bool = False,
    ) -> None:
        summary = self.nutrition.get_daily_summary(entry.local_date, cfg)
        fallback, blocks = build_meal_card_blocks(entry, summary, cost_line=cost_line)
        if update_existing and entry.slack_card_ts:
            await self.channel.update_blocks_message(
                channel_id=channel_id,
                ts=entry.slack_card_ts,
                text=fallback,
                blocks=blocks,
            )
            return
        card_ts = await self.channel.post_blocks_message(
            channel_id=channel_id,
            thread_ts=thread_ts or None,
            text=fallback,
            blocks=blocks,
        )
        self.nutrition.set_slack_card_ts(
            entry.id,
            card_ts,
            thread_ts=thread_ts or card_ts,
            channel_id=channel_id,
        )

    async def sync_garmin(
        self,
        *,
        days: int | None = None,
        now_utc: datetime | None = None,
    ) -> tuple[int, int]:
        """Pull recent Garmin daily burn, activities, and weight into SQLite."""
        if self.garmin is None:
            return 0, 0
        cfg = self.load_targets().food_tracker
        if not cfg.garmin_enabled:
            return 0, 0

        now = now_utc or self._now_utc()
        if now.tzinfo is None:
            now = now.replace(tzinfo=UTC)
        today_str = self._today_local_date(cfg, now)
        end_d = datetime.fromisoformat(today_str).date()
        if days is not None:
            span = max(1, days)
        elif not self._garmin_bootstrapped:
            span = max(1, cfg.garmin_lookback_days)
        else:
            span = 2
        start_str = (end_d - timedelta(days=span - 1)).isoformat()
        fallback_bmr = self.nutrition.get_latest_full_day_bmr(start_str)

        prev_today = self.nutrition.get_garmin_daily(today_str)
        known_today_act_ids = (
            {a.activity_id for a in prev_today.activities} if prev_today is not None else set()
        )
        was_bootstrapped = self._garmin_bootstrapped

        result = await self.garmin.sync_range(
            start_date=start_str,
            end_date=today_str,
            fallback_bmr_kcal=fallback_bmr,
        )
        for metric in result.daily_metrics:
            self.nutrition.upsert_garmin_daily(metric)

        weights_synced = 0
        if cfg.garmin_sync_weight and result.weights_lbs:
            weights_synced = self.nutrition.sync_garmin_weights(result.weights_lbs)

        self._garmin_bootstrapped = True
        self._last_garmin_sync_at = now
        self._last_garmin_error_at = None

        if self.push_manager is not None and (was_bootstrapped or prev_today is not None):
            today_metric = next(
                (m for m in result.daily_metrics if m.local_date == today_str), None
            )
            if today_metric is not None:
                new_big_acts = [
                    a
                    for a in today_metric.activities
                    if a.activity_id not in known_today_act_ids and a.calories_kcal >= 200.0
                ]
                if new_big_acts:
                    summary_now = self.nutrition.get_daily_summary(today_str, cfg)
                    for act in new_big_acts[:2]:
                        mins = max(1, round(act.duration_seconds / 60.0))
                        try:
                            await self.push_manager.notify(
                                title=f"⌚ Workout Synced: {act.name}",
                                body=(
                                    f"+{act.calories_kcal:.0f} kcal burned ({mins}m) — "
                                    f"today's budget is {summary_now.target.calories_kcal} kcal "
                                    f"({summary_now.remaining_calories_kcal:.0f} left)."
                                ),
                                kind="food",
                                url="./?tab=food",
                            )
                        except Exception as exc:  # noqa: BLE001
                            log.warning("food_tracker.workout_push_failed", error=str(exc))

        log.info(
            "food_tracker.garmin_synced",
            start_date=start_str,
            end_date=today_str,
            days_synced=len(result.daily_metrics),
            weights_synced=weights_synced,
        )
        return len(result.daily_metrics), weights_synced

    async def _handle_garmin_command(self, args: str, cfg: FoodTrackerConfig) -> str:
        tokens = args.strip().split()
        sub = tokens[0].lower() if tokens else ""

        if sub == "dynamic" and len(tokens) >= 2:
            today = self._today_local_date(cfg)
            choice = tokens[1].lower()
            if choice in ("on", "true", "1", "enable"):
                self.nutrition.save_garmin_settings(
                    local_date=today, fallback_cfg=cfg, dynamic_enabled=True
                )
                state_lbl = "enabled"
            elif choice in ("off", "false", "0", "disable"):
                self.nutrition.save_garmin_settings(
                    local_date=today, fallback_cfg=cfg, dynamic_enabled=False
                )
                state_lbl = "disabled"
            else:
                return "Usage: `!garmin dynamic on` or `!garmin dynamic off`."
            effective, _ = self.nutrition.resolve_effective_target(today, cfg)
            return (
                f"Garmin dynamic calorie targets *{state_lbl}*. "
                f"Today's target: *{effective.calories_kcal} kcal* "
                f"({effective.protein_g:.0f}P/{effective.carbs_g:.0f}C/{effective.fat_g:.0f}F)."
            )

        if sub == "sync":
            if self.garmin is None:
                return "Garmin is not configured (`GARMIN_API_KEY` is missing)."
            try:
                days_cnt, weights_cnt = await self.sync_garmin(days=cfg.garmin_lookback_days)
            except GarminAuthError as exc:
                return f"Garmin auth failed: {exc}"
            except Exception as exc:  # noqa: BLE001
                return f"Garmin sync failed: `{type(exc).__name__}: {exc}`"
            today = self._today_local_date(cfg)
            summary = self.nutrition.get_daily_summary(today, cfg)
            ctx = _format_garmin_context_line(summary)
            return (
                f"⌚ Synced *{days_cnt}* day(s) and *{weights_cnt}* weight entry(ies) from Garmin.\n"
                f"{ctx or 'No activity recorded for today yet.'}"
            )

        today = self._today_local_date(cfg)
        summary = self.nutrition.get_daily_summary(today, cfg)
        if summary.garmin is None:
            if self.garmin is None:
                return "Garmin is not configured (`GARMIN_API_KEY` is not set)."
            return "No Garmin data synced for today yet. Run `!garmin sync` to pull now."

        g = summary.garmin
        lines = [
            f"*Garmin Activity & Metabolic Summary (`{g.local_date}`)*",
            f"• Baseline BMR (24h): *{g.bmr_full_day_kcal:.0f} kcal* (accrued so far: {g.bmr_raw_kcal:.0f} kcal)",
            f"• Active Burn: *{g.active_kcal:.0f} kcal*  |  Projected Total Burn: *{g.total_burn_kcal:.0f} kcal*",
            f"• Steps: *{g.steps:,}*"
            + (f"  |  Resting HR: *{g.resting_hr} bpm*" if g.resting_hr is not None else "")
            + (
                f"  |  Intensity min: {g.moderate_min} mod / {g.vigorous_min} vig"
                if (g.moderate_min or g.vigorous_min)
                else ""
            ),
        ]
        if g.activities:
            lines.append("• *Workouts:*")
            for act in g.activities:
                mins = round(act.duration_seconds / 60.0)
                hr_str = f", avg {act.average_hr:.0f} bpm" if act.average_hr else ""
                lines.append(
                    f"  - *{act.name}* (`{act.activity_type}`): "
                    f"{act.calories_kcal:.0f} kcal in {mins} min{hr_str}"
                )
        rec_bits: list[str] = []
        if g.sleep_seconds:
            s_str = f"Sleep {g.sleep_seconds / 3600:.1f}h"
            if g.sleep_score is not None:
                s_str += f" ({g.sleep_score})"
            rec_bits.append(s_str)
        if g.hrv_last_night_avg is not None:
            rec_bits.append(f"HRV {g.hrv_last_night_avg}ms ({g.hrv_status or 'n/a'})")
        if g.body_battery_current is not None:
            rec_bits.append(f"Body Battery {g.body_battery_current}/100")
        if rec_bits:
            lines.append("• *Recovery:* " + "  |  ".join(rec_bits))
        if summary.dynamic_target is not None:
            dt = summary.dynamic_target
            cal_note = f", cal `{dt.burn_offset_kcal:+d}`" if dt.burn_offset_kcal else ""
            lines.append(
                f"🎯 *Dynamic Target (`{dt.mode}`):* *{dt.effective_calories_kcal} kcal* "
                f"(base {dt.base_calories_kcal}, offset `{dt.goal_delta_kcal:+d}`, "
                f"active ×{dt.active_factor:g}{cal_note}) → `{summary.target.protein_g:.0f}P / "
                f"{summary.target.carbs_g:.0f}C / {summary.target.fat_g:.0f}F`"
            )
        else:
            lines.append(f"🎯 *Static Target:* *{summary.target.calories_kcal} kcal*")
        lines.append("_Commands: `!garmin sync`, `!garmin dynamic on|off`, `!goal delta <±kcal>`_")
        return "\n".join(lines)

    async def run_once(self, now_utc: datetime | None = None) -> None:
        """Run one periodic tick: heartbeat, Garmin sync, daily SQLite backup, and evening digest."""
        if self.heartbeat_cb is not None:
            self.heartbeat_cb()

        cfg = self.load_targets().food_tracker
        if not cfg.enabled:
            return

        now = now_utc or self._now_utc()
        if now.tzinfo is None:
            now = now.replace(tzinfo=UTC)
        local_now = now.astimezone(ZoneInfo(cfg.timezone))
        local_date = self._today_local_date(cfg, now)

        if self.garmin is not None and cfg.garmin_enabled:
            interval_ok = self._last_garmin_sync_at is None or (
                now - self._last_garmin_sync_at
            ).total_seconds() >= max(60, cfg.garmin_sync_interval_seconds)
            backoff_ok = (
                self._last_garmin_error_at is None
                or (now - self._last_garmin_error_at).total_seconds() >= 300
            )
            if interval_ok and backoff_ok:
                try:
                    await self.sync_garmin(now_utc=now)
                except GarminAuthError as exc:
                    self._last_garmin_error_at = now
                    log.warning("food_tracker.garmin_auth_failed", error=str(exc))
                except Exception as exc:  # noqa: BLE001
                    self._last_garmin_error_at = now
                    log.warning("food_tracker.garmin_sync_failed", error=str(exc))

        backfill_res = self.nutrition.run_burn_calibration_backfill_once(local_date, cfg)
        if backfill_res is not None:
            log.info("food_tracker.burn_calibration_backfilled", **backfill_res)

        if cfg.daily_backup_enabled:
            backup_path = self.backups_dir / f"nutrition-{local_date}.sqlite3.gz"
            if not backup_path.exists():
                self.nutrition.create_backup_snapshot(
                    self.backups_dir, local_date, keep_days=cfg.backup_keep_days
                )
                log.info("food_tracker.backup_created", path=str(backup_path))

        food_channel = self.channel.resolve_channel("food")
        if (
            cfg.evening_digest_enabled
            and self.channel.is_explicit_role_channel("food", food_channel)
            and local_date == local_now.date().isoformat()
            and (local_now.hour, local_now.minute)
            >= (cfg.evening_digest_hour, cfg.evening_digest_minute)
            and not self.nutrition.has_sent_digest(local_date)
        ):
            summary = self.nutrition.get_daily_summary(local_date, cfg)
            if summary.meals:
                text = format_daily_summary(summary, heading="Evening Macro Check-In")
                await self.channel.post_channel_message(text, channel_id=food_channel)
                self.nutrition.mark_digest_sent(local_date)
                log.info("food_tracker.evening_digest_sent", local_date=local_date)

    async def run(self) -> None:
        log.info("food_tracker.loop_started")
        try:
            while True:
                await self.run_once()
                await asyncio.sleep(60)
        except asyncio.CancelledError:
            log.info("food_tracker.loop_stopped")
            raise
