from __future__ import annotations

import gzip
import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import AsyncMock

import httpx

from agent_tasks.channels.slack_socket import SlackSocketAdapter
from agent_tasks.common.config import ControllerSettings, FoodTrackerConfig, TargetsConfig
from agent_tasks.common.models import TokenUsageSummary
from agent_tasks.common.state import StateStore
from agent_tasks.tasks.food_tracker.garmin import GarminAuthError, GarminPersonalClient
from agent_tasks.tasks.food_tracker.models import (
    DailySummary,
    EstimatedMealItem,
    MealEntryRecord,
    MealEstimateSchema,
    MealSlot,
    SavedFood,
)
from agent_tasks.tasks.food_tracker.store import (
    NutritionStore,
    effective_local_date,
    normalize_item_macros,
)
from agent_tasks.tasks.food_tracker.task import FoodTrackerTask


class FakeEstimator:
    """Deterministic fake multimodal estimator matching `NutritionEstimator`."""

    def __init__(self) -> None:
        self.estimate_calls: list[dict] = []
        self.revise_calls: list[dict] = []
        self.suggest_calls: list[dict] = []
        self.next_estimate = MealEstimateSchema(
            title="Salmon Rice Bowl",
            meal_slot="dinner",
            confidence="medium",
            items=[
                EstimatedMealItem(
                    name="Atlantic salmon fillet",
                    quantity_desc="6 oz cooked",
                    estimated_grams=170.0,
                    calories_kcal=350.0,
                    protein_g=38.0,
                    carbs_g=0.0,
                    fat_g=21.0,
                    fiber_g=0.0,
                    source="vision_estimate",
                ),
                EstimatedMealItem(
                    name="Jasmine rice",
                    quantity_desc="1 cup cooked",
                    estimated_grams=158.0,
                    calories_kcal=205.0,
                    protein_g=4.0,
                    carbs_g=45.0,
                    fat_g=0.5,
                    fiber_g=0.6,
                    source="vision_estimate",
                ),
                EstimatedMealItem(
                    name="Olive oil (cooking)",
                    quantity_desc="1 tbsp",
                    estimated_grams=13.5,
                    calories_kcal=120.0,
                    protein_g=0.0,
                    carbs_g=0.0,
                    fat_g=13.5,
                    fiber_g=0.0,
                    source="vision_estimate",
                ),
            ],
            assumptions_note="Assumed 1 tbsp olive oil for searing salmon.",
        )
        self.next_revision = MealEstimateSchema(
            title="Air-Fried Salmon Rice Bowl",
            meal_slot="dinner",
            confidence="high",
            items=[
                EstimatedMealItem(
                    name="Atlantic salmon fillet",
                    quantity_desc="8 oz cooked",
                    estimated_grams=226.0,
                    calories_kcal=465.0,
                    protein_g=50.0,
                    carbs_g=0.0,
                    fat_g=28.0,
                    fiber_g=0.0,
                    source="user_edit",
                ),
                EstimatedMealItem(
                    name="Jasmine rice",
                    quantity_desc="1 cup cooked",
                    estimated_grams=158.0,
                    calories_kcal=205.0,
                    protein_g=4.0,
                    carbs_g=45.0,
                    fat_g=0.5,
                    fiber_g=0.6,
                    source="user_edit",
                ),
            ],
            assumptions_note="Removed cooking oil; bumped salmon to 8 oz per user correction.",
        )

    async def estimate_meal(
        self,
        *,
        description: str,
        photo_paths: list[Path],
        local_date: str,
        default_slot: MealSlot,
        saved_foods: list[SavedFood],
        model_override: str | None = None,
    ) -> tuple[MealEstimateSchema, TokenUsageSummary, str]:
        self.estimate_calls.append(
            {
                "description": description,
                "photo_paths": photo_paths,
                "local_date": local_date,
                "default_slot": default_slot,
                "saved_foods": saved_foods,
                "model_override": model_override,
            }
        )
        usage = TokenUsageSummary(
            prompt_tokens=1200,
            output_tokens=240,
            thoughts_tokens=80,
            total_tokens=1520,
        )
        return self.next_estimate, usage, model_override or "gemini-4-hydrogen"

    async def revise_meal(
        self,
        *,
        existing_meal: MealEntryRecord,
        feedback_text: str,
        new_photo_paths: list[Path],
        saved_foods: list[SavedFood],
        model_override: str | None = None,
    ) -> tuple[MealEstimateSchema, TokenUsageSummary, str]:
        self.revise_calls.append(
            {
                "entry_id": existing_meal.id,
                "feedback_text": feedback_text,
                "new_photo_paths": new_photo_paths,
                "model_override": model_override,
            }
        )
        usage = TokenUsageSummary(
            prompt_tokens=800,
            output_tokens=200,
            total_tokens=1000,
        )
        return self.next_revision, usage, model_override or "gemini-4-hydrogen"

    async def suggest_remaining(
        self,
        *,
        summary: DailySummary,
        saved_foods: list[SavedFood],
        note: str = "",
        model_override: str | None = None,
    ) -> tuple[str, TokenUsageSummary, str]:
        self.suggest_calls.append(
            {
                "remaining_kcal": summary.remaining_calories_kcal,
                "remaining_protein_g": summary.remaining_protein_g,
                "note": note,
            }
        )
        usage = TokenUsageSummary(prompt_tokens=400, output_tokens=120, total_tokens=520)
        return (
            (
                "• *Greek yogurt + whey*: ~280 kcal (42P / 14C / 5F)\n"
                "• *Cottage cheese + berries*: ~220 kcal (28P / 16C / 4F)"
            ),
            usage,
            model_override or "gemini-4-hydrogen",
        )


def _make_settings(tmp_path: Path) -> ControllerSettings:
    targets_yaml = tmp_path / "targets.yaml"
    targets_yaml.write_text("food_tracker:\n  enabled: true\n", encoding="utf-8")
    return ControllerSettings(
        state_dir=tmp_path,
        targets_config_path=targets_yaml,
        target_github_token="ghp_test",
        slack_bot_token="xoxb-test",
        slack_app_token="xapp-test",
        slack_channel_id="C_DEFAULT",
    )


def _make_adapter_harness(
    tmp_path: Path,
    *,
    bound_channels: dict[str, str] | None = None,
    estimator: FakeEstimator | None = None,
    http_transport: httpx.AsyncBaseTransport | None = None,
    clock_now: datetime | None = None,
) -> tuple[
    SlackSocketAdapter,
    FoodTrackerTask,
    StateStore,
    list[dict],
    list[dict],
    AsyncMock,
    AsyncMock,
]:
    settings = _make_settings(tmp_path)
    state = StateStore(tmp_path / "state.sqlite3")
    bound = dict(bound_channels or {})
    for role, chan in bound.items():
        state.set_channel_binding(role, chan)

    posted: list[dict] = []
    updated: list[dict] = []
    on_thread_feedback = AsyncMock()
    on_coding_request = AsyncMock(return_value="coding-queued")
    est = estimator or FakeEstimator()

    food_ref: list[FoodTrackerTask] = []
    adapter = SlackSocketAdapter(
        bot_token="xoxb-test",
        app_token="xapp-test",
        channel_id="C_DEFAULT",
        on_open_draft_pr=AsyncMock(),
        on_resolve=AsyncMock(),
        on_ignore=AsyncMock(),
        on_thread_feedback=on_thread_feedback,
        on_coding_request=on_coding_request,
        channel_resolver=lambda role: state.get_channel_bindings().get(role, ""),
        on_food_message=lambda text, user, chan, ts, files, is_food: food_ref[0].handle_message(
            text, user, chan, ts, files, is_food
        ),
        on_food_thread_reply=lambda t_ts, text, user, chan, r_ts, files: food_ref[
            0
        ].handle_thread_reply(t_ts, text, user, chan, r_ts, files),
        on_food_scale=lambda eid, mult, user: food_ref[0].handle_scale_action(eid, mult, user),
        on_food_save_staple=lambda eid, user: food_ref[0].handle_save_staple_action(eid, user),
        on_food_delete=lambda eid, user: food_ref[0].handle_delete_action(eid, user),
    )

    msg_counter = 0

    async def _post_message(**kwargs):  # type: ignore[no-untyped-def]
        nonlocal msg_counter
        msg_counter += 1
        ts = f"bot.{msg_counter}"
        posted.append({"ts": ts, **kwargs})
        return {"ts": ts}

    async def _update_message(**kwargs):  # type: ignore[no-untyped-def]
        updated.append(dict(kwargs))
        return {"ts": kwargs.get("ts")}

    adapter.app.client.chat_postMessage = _post_message  # type: ignore[method-assign]
    adapter.app.client.chat_update = _update_message  # type: ignore[method-assign]
    adapter.app.client.reactions_add = AsyncMock(return_value={"ok": True})  # type: ignore[method-assign]
    adapter.app.client.reactions_remove = AsyncMock(return_value={"ok": True})  # type: ignore[method-assign]

    now_val = clock_now or datetime(2026, 11, 16, 20, 0, tzinfo=UTC)  # 12:00 PM PST
    food_task = FoodTrackerTask(
        settings=settings,
        state=state,
        channel=adapter,
        estimator=est,
        http_transport=http_transport,
        clock=lambda: now_val,
    )
    food_ref.append(food_task)
    return adapter, food_task, state, posted, updated, on_thread_feedback, on_coding_request


def test_store_targets_atwater_scaling_delete_staple_rollover_and_tdee(tmp_path: Path) -> None:
    store = NutritionStore(tmp_path / "nutrition.sqlite3")
    cfg = FoodTrackerConfig()

    # 3:00 AM rollover math in America/Los_Angeles (PST = UTC-8 in mid-Nov)
    assert (
        effective_local_date(datetime(2026, 11, 16, 9, 30, tzinfo=UTC), "America/Los_Angeles", 3)
        == "2026-11-15"
    )  # 01:30 AM PST -> previous day
    assert (
        effective_local_date(datetime(2026, 11, 16, 11, 30, tzinfo=UTC), "America/Los_Angeles", 3)
        == "2026-11-16"
    )  # 03:30 AM PST -> current day

    # Effective-dated targets
    assert store.get_target_for_date("2026-11-09", cfg).calories_kcal == 2200
    store.set_target(
        effective_date="2026-11-10",
        calories_kcal=2000,
        protein_g=180.0,
        carbs_g=190.0,
        fat_g=60.0,
    )
    store.set_target(
        effective_date="2026-11-15",
        calories_kcal=2400,
        protein_g=190.0,
        carbs_g=250.0,
        fat_g=70.0,
    )
    assert store.get_target_for_date("2026-11-12", cfg).calories_kcal == 2000
    assert store.get_target_for_date("2026-11-16", cfg).calories_kcal == 2400

    # Atwater reconciliation fixes >25% hallucinated kcal on vision_estimate, preserves label kcal
    bad_vision = EstimatedMealItem(
        name="Mystery Bowl",
        quantity_desc="1 bowl",
        calories_kcal=100.0,
        protein_g=30.0,
        carbs_g=50.0,
        fat_g=10.0,
        source="vision_estimate",
    )
    reconciled_item, was_reconciled = normalize_item_macros(bad_vision)
    assert was_reconciled is True
    assert reconciled_item.calories_kcal == 410.0  # 4*30 + 4*50 + 9*10

    label_item = EstimatedMealItem(
        name="Fiber Bar",
        quantity_desc="1 bar",
        calories_kcal=140.0,
        protein_g=10.0,
        carbs_g=25.0,
        fat_g=4.0,
        fiber_g=12.0,
        source="nutrition_label",
    )
    kept_label, label_reconciled = normalize_item_macros(label_item)
    assert label_reconciled is False
    assert kept_label.calories_kcal == 140.0

    # Non-compounding portion scaling (0.5x then 0.75x -> 0.75x base, NOT 0.375x)
    entry = store.create_meal_entry(
        local_date="2026-11-16",
        estimate=MealEstimateSchema(
            title="Chicken & Rice",
            meal_slot="lunch",
            confidence="high",
            items=[
                EstimatedMealItem(
                    name="Chicken breast",
                    quantity_desc="200g",
                    estimated_grams=200.0,
                    calories_kcal=330.0,
                    protein_g=62.0,
                    carbs_g=0.0,
                    fat_g=7.2,
                ),
                EstimatedMealItem(
                    name="White rice",
                    quantity_desc="200g",
                    estimated_grams=200.0,
                    calories_kcal=260.0,
                    protein_g=5.0,
                    carbs_g=56.0,
                    fat_g=0.6,
                ),
            ],
        ),
        raw_description="chicken and rice",
        model_used="gemini-4-hydrogen",
    )
    assert entry.total_calories_kcal == 590.0
    half = store.scale_meal_entry(entry.id, 0.5)
    assert half.scale_factor == 0.5
    assert half.total_calories_kcal == 295.0
    three_quarter = store.scale_meal_entry(entry.id, 0.75)
    assert three_quarter.scale_factor == 0.75
    assert three_quarter.total_calories_kcal == 442.5

    # Save scaled meal as staple and verify soft delete excludes from daily summary
    staple = store.save_meal_as_staple(entry.id)
    assert staple.slug == "chicken-rice"
    assert staple.calories_kcal == 442.5
    store.soft_delete_meal(entry.id)
    summary_after_delete = store.get_daily_summary("2026-11-16", cfg)
    assert summary_after_delete.meals == []
    assert summary_after_delete.total_calories_kcal == 0.0

    # Weight EMA & adaptive TDEE over 10 logged days
    start_day = datetime(2026, 11, 1, tzinfo=UTC).date()
    for offset in range(10):
        day_str = (start_day + timedelta(days=offset)).isoformat()
        store.create_meal_entry(
            local_date=day_str,
            estimate=MealEstimateSchema(
                title="Daily Intake",
                meal_slot="dinner",
                confidence="high",
                items=[
                    EstimatedMealItem(
                        name="Full day",
                        quantity_desc="1 day",
                        calories_kcal=2000.0,
                        protein_g=170.0,
                        carbs_g=200.0,
                        fat_g=58.0,
                    )
                ],
            ),
            raw_description="daily intake",
            model_used="gemini-4-hydrogen",
        )
        # Steady ~0.1 lb/day drop -> EMA drops ~0.7-0.9 lbs across span -> TDEE > 2000
        store.log_weight(local_date=day_str, weight_lbs=185.0 - 0.1 * offset)

    tdee = store.estimate_adaptive_tdee("2026-11-10", window_days=14)
    assert tdee is not None
    assert tdee.days_logged == 10
    assert tdee.avg_daily_kcal == 2000.0
    assert tdee.weight_delta_lbs < 0
    assert tdee.estimated_tdee_kcal > 2000.0

    # Gzip SQLite backup & retention pruning
    backup_dir = tmp_path / "backups"
    backup_dir.mkdir()
    expired = backup_dir / "nutrition-2026-10-01.sqlite3.gz"
    expired.write_bytes(b"old-archive")
    recent_other = backup_dir / "nutrition-2026-11-09.sqlite3.gz"
    recent_other.write_bytes(b"recent-archive")

    snapshot = store.create_backup_snapshot(backup_dir, "2026-11-10", keep_days=14)
    assert snapshot.exists()
    assert not expired.exists()
    assert recent_other.exists()

    # Verify decompressed backup is a valid SQLite DB with our targets & weights
    restored = tmp_path / "restored.sqlite3"
    with gzip.open(snapshot, "rb") as gz_in:
        restored.write_bytes(gz_in.read())
    with sqlite3.connect(restored) as conn:
        cnt = conn.execute("SELECT COUNT(*) FROM body_metrics").fetchone()[0]
    assert cnt == 10


async def test_photo_upload_flow_in_bound_food_channel_and_usage_billing(tmp_path: Path) -> None:
    def _handler(request: httpx.Request) -> httpx.Response:
        assert request.headers["Authorization"] == "Bearer xoxb-test"
        return httpx.Response(
            200, content=b"\xff\xd8\xff\xe0fake-jpeg-bytes", headers={"content-type": "image/jpeg"}
        )

    est = FakeEstimator()
    adapter, _food_task, state, posted, _, _, _ = _make_adapter_harness(
        tmp_path,
        bound_channels={"food": "C_FOOD"},
        estimator=est,
        http_transport=httpx.MockTransport(_handler),
    )

    # Photo + caption with subtype="file_share"
    await adapter.process_incoming_message(
        {
            "channel": "C_FOOD",
            "user": "U_LUCAS",
            "ts": "100.1",
            "subtype": "file_share",
            "text": "pan-seared salmon with 1 cup rice",
            "files": [
                {
                    "name": "dinner.jpg",
                    "mimetype": "image/jpeg",
                    "url_private_download": "https://files.slack.com/files-pri/T1-F1/dinner.jpg",
                }
            ],
        },
        from_mention=False,
    )

    assert len(est.estimate_calls) == 1
    assert len(est.estimate_calls[0]["photo_paths"]) == 1
    assert est.estimate_calls[0]["photo_paths"][0].exists()

    # Photo-only upload (empty text) must NOT be dropped by SlackSocketAdapter
    await adapter.process_incoming_message(
        {
            "channel": "C_FOOD",
            "user": "U_LUCAS",
            "ts": "100.2",
            "subtype": "file_share",
            "text": "",
            "files": [
                {
                    "name": "snack.jpg",
                    "mimetype": "image/jpeg",
                    "url_private_download": "https://files.slack.com/files-pri/T1-F2/snack.jpg",
                }
            ],
        },
        from_mention=False,
    )
    assert len(est.estimate_calls) == 2

    # Check Block Kit card structure (2 actions blocks, 5 scale buttons + 2 meal action buttons)
    assert len(posted) == 2
    card = posted[0]
    assert card["channel"] == "C_FOOD"
    assert card["thread_ts"] == "100.1"
    action_blocks = [b for b in card["blocks"] if b["type"] == "actions"]
    assert len(action_blocks) == 2
    assert len(action_blocks[0]["elements"]) == 5
    assert [e["action_id"] for e in action_blocks[0]["elements"]] == [
        "food_scale_050",
        "food_scale_075",
        "food_scale_100",
        "food_scale_125",
        "food_scale_150",
    ]
    assert [e["action_id"] for e in action_blocks[1]["elements"]] == [
        "food_save_staple",
        "food_delete",
    ]

    # Verify token usage recorded in StateStore.usage_ledger under food_tracker
    with state._connect() as conn:
        rows = conn.execute(
            "SELECT task_kind, model, prompt_tokens, output_tokens FROM usage_ledger"
        ).fetchall()
    assert len(rows) == 2
    assert rows[0]["task_kind"] == "food_tracker"
    assert rows[0]["model"] == "gemini-4-hydrogen"
    assert rows[0]["prompt_tokens"] == 1200


async def test_missing_files_read_scope_surfaces_actionable_slack_error(tmp_path: Path) -> None:
    def _handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            text="<html><body>Slack sign in</body></html>",
            headers={"content-type": "text/html; charset=utf-8"},
        )

    est = FakeEstimator()
    adapter, _, _, posted, _, _, _ = _make_adapter_harness(
        tmp_path,
        bound_channels={"food": "C_FOOD"},
        estimator=est,
        http_transport=httpx.MockTransport(_handler),
    )

    await adapter.process_incoming_message(
        {
            "channel": "C_FOOD",
            "user": "U_LUCAS",
            "ts": "101.1",
            "subtype": "file_share",
            "text": "breakfast",
            "files": [
                {
                    "name": "eggs.jpg",
                    "mimetype": "image/jpeg",
                    "url_private_download": "https://files.slack.com/files-pri/T1-F3/eggs.jpg",
                }
            ],
        },
        from_mention=False,
    )

    assert len(est.estimate_calls) == 0
    assert len(posted) == 1
    assert "files:read" in posted[0]["text"]
    assert "slack-app-manifest.yaml" in posted[0]["text"]


async def test_staple_fastpath_button_actions_and_thread_revision_routing(tmp_path: Path) -> None:
    est = FakeEstimator()
    adapter, food_task, _, _posted, updated, on_thread_feedback, on_coding = _make_adapter_harness(
        tmp_path,
        bound_channels={"food": "C_FOOD"},
        estimator=est,
    )

    # Pre-save a staple
    food_task.nutrition.upsert_saved_food(
        slug="morning-shake",
        name="Morning Protein Shake",
        serving_desc="1 blender bottle",
        calories_kcal=340.0,
        protein_g=48.0,
        carbs_g=22.0,
        fat_g=6.0,
    )

    # Zero-token staple fast path (1.5x morning-shake)
    await adapter.process_incoming_message(
        {"channel": "C_FOOD", "user": "U_LUCAS", "ts": "200.1", "text": "1.5x morning-shake"},
        from_mention=False,
    )
    assert len(est.estimate_calls) == 0
    meal = food_task.nutrition.get_meal_by_thread_ts("200.1")
    assert meal is not None
    assert meal.total_calories_kcal == 510.0  # 1.5 * 340
    assert meal.model_used == "saved-food-fastpath"

    # Log an LLM meal, then scale 0.5x -> 0.75x, save staple, revise in thread, then delete
    await adapter.process_incoming_message(
        {"channel": "C_FOOD", "user": "U_LUCAS", "ts": "200.2", "text": "salmon rice bowl"},
        from_mention=False,
    )
    assert len(est.estimate_calls) == 1
    llm_meal = food_task.nutrition.get_meal_by_thread_ts("200.2")
    assert llm_meal is not None
    base_kcal = llm_meal.total_calories_kcal  # 350 + 205 + 120 = 675

    await food_task.handle_scale_action(llm_meal.id, 0.5, "U_LUCAS")
    scaled_half = food_task.nutrition.get_meal_entry(llm_meal.id)
    assert scaled_half is not None
    assert scaled_half.total_calories_kcal == round(base_kcal * 0.5, 1)

    await food_task.handle_scale_action(llm_meal.id, 0.75, "U_LUCAS")
    scaled_tq = food_task.nutrition.get_meal_entry(llm_meal.id)
    assert scaled_tq is not None
    assert scaled_tq.total_calories_kcal == round(base_kcal * 0.75, 1)
    assert len(updated) == 2

    await food_task.handle_save_staple_action(llm_meal.id, "U_LUCAS")
    assert food_task.nutrition.get_saved_food("salmon-rice-bowl") is not None

    # Thread reply routes to FoodTrackerTask & edits Slack card in place
    await adapter.process_incoming_message(
        {
            "channel": "C_FOOD",
            "user": "U_LUCAS",
            "ts": "200.21",
            "thread_ts": "200.2",
            "text": "air fried, no oil, 8oz salmon",
        },
        from_mention=False,
    )
    assert len(est.revise_calls) == 1
    assert on_thread_feedback.await_count == 0
    revised = food_task.nutrition.get_meal_entry(llm_meal.id)
    assert revised is not None
    assert revised.title == "Air-Fried Salmon Rice Bowl"
    assert revised.total_calories_kcal == 670.0
    assert len(updated) == 3

    # Non-food thread reply must still fall through to on_thread_feedback
    await adapter.process_incoming_message(
        {
            "channel": "C_DEFAULT",
            "user": "U_LUCAS",
            "ts": "999.1",
            "thread_ts": "999.0",
            "text": "retry [carbon]",
        },
        from_mention=False,
    )
    on_thread_feedback.assert_awaited_once_with("999.0", "retry [carbon]", "U_LUCAS")

    # Delete action strikes through card & removes action buttons
    await food_task.handle_delete_action(llm_meal.id, "U_LUCAS")
    deleted = food_task.nutrition.get_meal_entry(llm_meal.id)
    assert deleted is not None
    assert deleted.status == "deleted"
    assert [b["type"] for b in updated[-1]["blocks"]] == ["section"]
    assert on_coding.await_count == 0


async def test_unbound_channel_safety_commands_and_evening_digest(tmp_path: Path) -> None:
    est = FakeEstimator()
    # Notice `food` is NOT bound yet
    adapter, food_task, state, posted, _, _, on_coding = _make_adapter_harness(
        tmp_path,
        bound_channels={},
        estimator=est,
    )

    # Bare text in C_DEFAULT must NOT auto-log as food
    await adapter.process_incoming_message(
        {"channel": "C_DEFAULT", "user": "U_LUCAS", "ts": "300.1", "text": "2 eggs and toast"},
        from_mention=False,
    )
    assert len(est.estimate_calls) == 0

    # @agytasks prompt in C_DEFAULT still routes to coding
    await adapter.process_incoming_message(
        {
            "channel": "C_DEFAULT",
            "user": "U_LUCAS",
            "ts": "300.2",
            "text": "<@U_BOT> fix parser bug",
        },
        from_mention=True,
    )
    on_coding.assert_awaited_once()

    # Evening digest must NOT fire while `food` is unbound
    await food_task.run_once(datetime(2026, 11, 17, 4, 35, tzinfo=UTC))  # 20:35 PST
    assert len(posted) == 1
    assert posted[0]["text"] == "coding-queued"

    # Bind C_FOOD and test commands
    state.set_channel_binding("food", "C_FOOD")
    await adapter.process_incoming_message(
        {"channel": "C_FOOD", "user": "U_LUCAS", "ts": "301.1", "text": "!goal 2100 180 200 65"},
        from_mention=False,
    )
    assert "2100 kcal" in posted[-1]["text"]

    await adapter.process_incoming_message(
        {"channel": "C_FOOD", "user": "U_LUCAS", "ts": "301.2", "text": "!weight 182.4 fasted"},
        from_mention=False,
    )
    assert "182.4 lbs" in posted[-1]["text"]

    await adapter.process_incoming_message(
        {
            "channel": "C_FOOD",
            "user": "U_LUCAS",
            "ts": "301.3",
            "text": "!eat morning protein bowl",
        },
        from_mention=False,
    )
    assert len(est.estimate_calls) == 1

    await adapter.process_incoming_message(
        {"channel": "C_FOOD", "user": "U_LUCAS", "ts": "301.4", "text": "!macros"},
        from_mention=False,
    )
    assert "Daily Nutrition (`2026-11-16`)" in posted[-1]["text"]
    assert "Salmon Rice Bowl" in posted[-1]["text"]

    await adapter.process_incoming_message(
        {"channel": "C_FOOD", "user": "U_LUCAS", "ts": "301.5", "text": "!week"},
        from_mention=False,
    )
    assert "Weekly Nutrition Summary" in posted[-1]["text"]
    assert "182.4 lbs" in posted[-1]["text"]

    await adapter.process_incoming_message(
        {"channel": "C_FOOD", "user": "U_LUCAS", "ts": "301.6", "text": "!suggest quick dessert"},
        from_mention=False,
    )
    assert len(est.suggest_calls) == 1
    assert "Greek yogurt + whey" in posted[-1]["text"]

    # Evening digest sends once on 2026-11-16 at 20:35 PST (2026-11-17 04:35Z) and deduplicates
    count_before = len(posted)
    await food_task.run_once(datetime(2026, 11, 17, 4, 35, tzinfo=UTC))
    assert len(posted) == count_before + 1
    assert posted[-1]["channel"] == "C_FOOD"
    assert "Evening Macro Check-In" in posted[-1]["text"]

    await food_task.run_once(datetime(2026, 11, 17, 4, 36, tzinfo=UTC))
    assert len(posted) == count_before + 1

    # Also validate TargetsConfig default food_tracker config loading
    cfg = TargetsConfig()
    assert cfg.food_tracker.timezone == "America/Los_Angeles"
    assert cfg.channels.food == ""


async def test_garmin_personal_api_client_token_refresh_bmr_projection_and_weights() -> None:
    token_calls = 0
    expired_once = False

    def _handler(request: httpx.Request) -> httpx.Response:
        nonlocal token_calls, expired_once
        path = request.url.path
        if path.endswith("/personal-api-key/token"):
            assert request.headers["Authorization"] == "ApiKey test-garmin-key"
            token_calls += 1
            return httpx.Response(200, text=f"jwt-tok-{token_calls}")

        auth = request.headers.get("Authorization", "")
        if not expired_once and path.endswith("/usersummary/daily"):
            expired_once = True
            return httpx.Response(401, json={"error": "expired"})
        assert auth == f"Bearer jwt-tok-{token_calls}"

        if path.endswith("/activities/search/activities"):
            return httpx.Response(
                200,
                json=[
                    {
                        "activityId": 9001,
                        "activityName": "Tempo Run",
                        "activityType": {"typeKey": "running"},
                        "calories": 420.0,
                        "duration": 1800.0,
                        "distance": 5000.0,
                        "averageHR": 156.0,
                        "startTimeLocal": "2026-11-16 07:30:00",
                    }
                ],
            )
        if path.endswith("/usersummary/daily"):
            cal_date = request.url.params["calendarDate"]
            if cal_date == "2026-11-15":
                return httpx.Response(
                    200,
                    json={
                        "calendarDate": "2026-11-15",
                        "bmrKilocalories": 2380.0,
                        "activeKilocalories": 250.0,
                        "totalKilocalories": 2630.0,
                        "totalSteps": 5100,
                        "restingHeartRate": 59,
                        "durationInMilliseconds": 86_400_000,
                    },
                )
            # Mid-day prorated BMR: 1200 kcal over 12h (43_200_000 ms) -> projects to 2400.0 kcal
            return httpx.Response(
                200,
                json={
                    "calendarDate": "2026-11-16",
                    "bmrKilocalories": 1200.0,
                    "activeKilocalories": 450.0,
                    "totalKilocalories": 1650.0,
                    "totalSteps": 8400,
                    "restingHeartRate": 58,
                    "moderateIntensityMinutes": 20,
                    "vigorousIntensityMinutes": 25,
                    "durationInMilliseconds": 43_200_000,
                },
            )
        if "/weight-service/weight/range/" in path:
            return httpx.Response(
                200,
                json={
                    "dailyWeightSummaries": [
                        {
                            "summaryDate": "2026-11-16",
                            "latestWeight": {
                                "calendarDate": "2026-11-16",
                                "weight": 101059.0,  # grams -> ~222.80 lbs
                            },
                        }
                    ]
                },
            )
        if path.endswith("/sleep-service/sleep/dailySleep"):
            return httpx.Response(
                200,
                json={
                    "sleepTimeSeconds": 28800,
                    "deepSleepSeconds": 4500,
                    "remSleepSeconds": 6200,
                    "sleepScores": {"overall": {"value": 86, "qualifierKey": "GOOD"}},
                },
            )
        if "/hrv-service/hrv/" in path:
            return httpx.Response(
                200,
                json={
                    "hrvSummary": {
                        "lastNightAvg": 44,
                        "weeklyAvg": 41,
                        "status": "BALANCED",
                    }
                },
            )
        return httpx.Response(404)

    client = GarminPersonalClient(
        api_key="test-garmin-key",
        http_transport=httpx.MockTransport(_handler),
    )
    res = await client.sync_range(start_date="2026-11-15", end_date="2026-11-16")
    assert token_calls == 2  # initial + 401 refresh
    assert len(res.daily_metrics) == 2

    yest, today = res.daily_metrics
    assert yest.bmr_full_day_kcal == 2380.0
    assert yest.total_burn_kcal == 2630.0
    assert today.bmr_raw_kcal == 1200.0
    assert today.bmr_full_day_kcal == 2400.0
    assert today.total_burn_kcal == 2850.0
    assert today.sleep_seconds == 28800
    assert today.sleep_score == 86
    assert today.hrv_last_night_avg == 44
    assert today.hrv_status == "BALANCED"
    assert len(today.activities) == 1
    assert today.activities[0].name == "Tempo Run"
    assert res.weights_lbs == [("2026-11-16", 222.8)]

    bad_client = GarminPersonalClient(
        api_key="bad-key",
        http_transport=httpx.MockTransport(lambda _req: httpx.Response(401)),
    )
    try:
        await bad_client.sync_range(start_date="2026-11-16", end_date="2026-11-16")
        raise AssertionError("Expected GarminAuthError")
    except GarminAuthError:
        pass


async def test_garmin_dynamic_calorie_goals_commands_and_sync_loop(tmp_path: Path) -> None:
    def _garmin_handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path.endswith("/personal-api-key/token"):
            return httpx.Response(200, text="jwt-ok")
        if path.endswith("/activities/search/activities"):
            return httpx.Response(
                200,
                json=[
                    {
                        "activityId": 42,
                        "activityName": "Morning Run",
                        "activityType": {"typeKey": "running"},
                        "calories": 380.0,
                        "duration": 1800.0,
                        "distance": 5000.0,
                        "averageHR": 152.0,
                        "startTimeLocal": "2026-11-16 07:00:00",
                    }
                ],
            )
        if path.endswith("/usersummary/daily"):
            cal_date = request.url.params["calendarDate"]
            if cal_date == "2026-11-16":
                return httpx.Response(
                    200,
                    json={
                        "calendarDate": cal_date,
                        "bmrKilocalories": 1200.0,
                        "activeKilocalories": 500.0,
                        "totalSteps": 9100,
                        "restingHeartRate": 57,
                        "durationInMilliseconds": 43_200_000,
                    },
                )
            return httpx.Response(
                200,
                json={
                    "calendarDate": cal_date,
                    "bmrKilocalories": 2400.0,
                    "activeKilocalories": 200.0,
                    "totalSteps": 4200,
                    "restingHeartRate": 58,
                    "durationInMilliseconds": 86_400_000,
                },
            )
        if "/weight-service/weight/range/" in path:
            return httpx.Response(
                200,
                json={
                    "dailyWeightSummaries": [
                        {
                            "summaryDate": "2026-11-15",
                            "latestWeight": {
                                "calendarDate": "2026-11-15",
                                "weight": 90718.5,
                            },  # 200.0 lbs
                        },
                        {
                            "summaryDate": "2026-11-16",
                            "latestWeight": {"calendarDate": "2026-11-16", "weight": 90000.0},
                        },
                    ]
                },
            )
        return httpx.Response(404)

    settings = _make_settings(tmp_path)
    settings.targets_config_path.write_text(
        "food_tracker:\n"
        "  enabled: true\n"
        "  garmin_enabled: true\n"
        "  garmin_lookback_days: 4\n"
        "  garmin_dynamic_targets_enabled: true\n"
        "  garmin_dynamic_mode: rolling_baseline\n"
        "  garmin_active_factor: 0.80\n"
        "  garmin_goal_delta_kcal: -500\n"
        "  garmin_min_calories_kcal: 1600\n"
        "  garmin_max_calories_kcal: 4000\n",
        encoding="utf-8",
    )
    settings = settings.model_copy(update={"garmin_api_key": "test-key"})
    state = StateStore(tmp_path / "state.sqlite3")
    state.set_channel_binding("food", "C_FOOD")
    est = FakeEstimator()
    posted: list[dict] = []

    food_ref: list[FoodTrackerTask] = []
    adapter = SlackSocketAdapter(
        bot_token="xoxb-test",
        app_token="xapp-test",
        channel_id="C_DEFAULT",
        on_open_draft_pr=AsyncMock(),
        on_resolve=AsyncMock(),
        on_ignore=AsyncMock(),
        on_thread_feedback=AsyncMock(),
        on_coding_request=AsyncMock(),
        channel_resolver=lambda role: state.get_channel_bindings().get(role, ""),
        on_food_message=lambda text, user, chan, ts, files, is_food: food_ref[0].handle_message(
            text, user, chan, ts, files, is_food
        ),
    )

    async def _post_message(**kwargs):  # type: ignore[no-untyped-def]
        ts = f"bot.{len(posted) + 1}"
        posted.append({"ts": ts, **kwargs})
        return {"ts": ts}

    adapter.app.client.chat_postMessage = _post_message  # type: ignore[method-assign]
    adapter.app.client.reactions_add = AsyncMock(return_value={"ok": True})  # type: ignore[method-assign]
    adapter.app.client.reactions_remove = AsyncMock(return_value={"ok": True})  # type: ignore[method-assign]

    now_val = datetime(2026, 11, 16, 20, 0, tzinfo=UTC)
    food_task = FoodTrackerTask(
        settings=settings,
        state=state,
        channel=adapter,
        estimator=est,
        http_transport=httpx.MockTransport(_garmin_handler),
        clock=lambda: now_val,
    )
    food_ref.append(food_task)

    # Pre-log a manual weight for 2026-11-16; Garmin sync must NOT overwrite manual weights
    food_task.nutrition.log_weight(local_date="2026-11-16", weight_lbs=195.5, notes="manual")

    # First run_once triggers Garmin backfill sync
    await food_task.run_once(now_val)
    latest_w = food_task.nutrition.get_latest_weight()
    assert latest_w is not None
    assert latest_w.local_date == "2026-11-16"
    assert latest_w.weight_lbs == 195.5  # manual preserved over Garmin 2026-11-16

    # Check dynamic target math for 2026-11-16:
    # baseline_bmr = 2400.0, baseline_active (Nov 13..15) = 200.0, today_active = 500.0
    # workout_surplus = 300.0 -> 2400 + (200 + 300) * 0.80 - 500 = 2300 kcal
    cfg = food_task.load_targets().food_tracker
    summary = food_task.nutrition.get_daily_summary("2026-11-16", cfg)
    assert summary.garmin is not None
    assert summary.dynamic_target is not None
    assert summary.target.calories_kcal == 2300
    assert summary.dynamic_target.adjustment_kcal == 100  # 2300 - 2200 default
    assert summary.target.protein_g == 170.0  # protein anchored
    assert summary.target.carbs_g == 245.0  # 220 + 100 / 4

    # `!garmin` command shows workouts + dynamic goal
    await adapter.process_incoming_message(
        {"channel": "C_FOOD", "user": "U_LUCAS", "ts": "400.1", "text": "!garmin"},
        from_mention=False,
    )
    assert "Morning Run" in posted[-1]["text"]
    assert "2300 kcal" in posted[-1]["text"]

    # `!goal delta 0` switches to maintenance immediately (2400 + 500*0.8 = 2800 kcal)
    await adapter.process_incoming_message(
        {"channel": "C_FOOD", "user": "U_LUCAS", "ts": "400.2", "text": "!goal delta 0"},
        from_mention=False,
    )
    assert "2800 kcal" in posted[-1]["text"]

    # `!garmin dynamic off` reverts to static base target (2200 kcal)
    await adapter.process_incoming_message(
        {"channel": "C_FOOD", "user": "U_LUCAS", "ts": "400.3", "text": "!garmin dynamic off"},
        from_mention=False,
    )
    assert "2200 kcal" in posted[-1]["text"]

    # `!week` includes Garmin avg burn alongside meals
    await adapter.process_incoming_message(
        {"channel": "C_FOOD", "user": "U_LUCAS", "ts": "400.4", "text": "!week"},
        from_mention=False,
    )
    assert "Garmin Avg Burn (4d)" in posted[-1]["text"]


async def test_garmin_post_workout_push_alert_only_fires_for_new_workouts(tmp_path: Path) -> None:
    from agent_tasks.tasks.food_tracker.garmin import GarminSyncResult
    from agent_tasks.tasks.food_tracker.models import GarminActivityRecord, GarminDailyMetric

    class _FakeGarmin:
        def __init__(self) -> None:
            self.activities: list[GarminActivityRecord] = []

        async def sync_range(
            self,
            *,
            start_date: str,
            end_date: str,
            fallback_bmr_kcal: float | None = None,
        ) -> GarminSyncResult:
            return GarminSyncResult(
                daily_metrics=[
                    GarminDailyMetric(
                        local_date=end_date,
                        bmr_raw_kcal=1200.0,
                        bmr_full_day_kcal=2400.0,
                        active_kcal=350.0 if self.activities else 50.0,
                        total_burn_kcal=2750.0 if self.activities else 2450.0,
                        steps=7000,
                        activities=list(self.activities),
                    )
                ]
            )

    settings = _make_settings(tmp_path)
    state = StateStore(tmp_path / "state.sqlite3")
    push_mgr = AsyncMock()
    fake_g = _FakeGarmin()
    now_val = datetime(2026, 11, 16, 20, 0, tzinfo=UTC)
    adapter, _, _, _, _, _, _ = _make_adapter_harness(tmp_path, clock_now=now_val)

    food_task = FoodTrackerTask(
        settings=settings,
        state=state,
        channel=adapter,
        estimator=FakeEstimator(),
        garmin=fake_g,
        push_manager=push_mgr,
        clock=lambda: now_val,
    )

    # First sync (baseline): no workout push
    await food_task.sync_garmin(now_utc=now_val)
    assert push_mgr.notify.await_count == 0

    # New 320 kcal workout appears -> triggers push notification once
    fake_g.activities = [
        GarminActivityRecord(
            activity_id=777,
            name="Evening Lift",
            activity_type="strength_training",
            calories_kcal=320.0,
            duration_seconds=2700.0,
        )
    ]
    await food_task.sync_garmin(now_utc=now_val)
    assert push_mgr.notify.await_count == 1
    assert "Evening Lift" in push_mgr.notify.await_args.kwargs["title"]

    # Re-syncing same workout does not duplicate push
    await food_task.sync_garmin(now_utc=now_val)
    assert push_mgr.notify.await_count == 1


def test_meal_reaction_emoji_persistence_and_validation(tmp_path: Path) -> None:
    import pytest

    store = NutritionStore(tmp_path / "nutrition.sqlite3")
    estimate = MealEstimateSchema(
        title="Chicken Bowl",
        meal_slot="lunch",
        confidence="high",
        items=[
            EstimatedMealItem(
                name="Chicken breast",
                quantity_desc="6 oz",
                calories_kcal=280.0,
                protein_g=52.0,
                carbs_g=0.0,
                fat_g=6.0,
            )
        ],
    )
    entry = store.create_meal_entry(
        local_date="2026-11-17",
        raw_description="Chicken bowl",
        estimate=estimate,
        model_used="test-model",
    )
    assert entry.reaction_emoji is None

    for emoji in ("🤮", "🤤", "💪"):
        updated = store.set_meal_reaction(entry.id, emoji)
        assert updated.reaction_emoji == emoji

    cleared = store.set_meal_reaction(entry.id, None)
    assert cleared.reaction_emoji is None

    with pytest.raises(ValueError):
        store.set_meal_reaction(entry.id, "🍕")

    with pytest.raises(KeyError):
        store.set_meal_reaction(999999, "💪")


def test_additive_burn_calibration_one_time_backfill_and_date_versioned_settings(
    tmp_path: Path,
) -> None:
    from datetime import date

    from agent_tasks.tasks.food_tracker.models import GarminDailyMetric

    store = NutritionStore(tmp_path / "nutrition.sqlite3")
    cfg = FoodTrackerConfig(
        garmin_enabled=True,
        garmin_dynamic_targets_enabled=True,
        garmin_dynamic_mode="rolling_baseline",
        garmin_active_factor=0.80,
        garmin_goal_delta_kcal=-250,
    )

    # Simulate legacy state: active_factor pegged at 1.3 from old calibrator
    store.set_food_setting("garmin_goal_delta_kcal", "-250")
    store.set_food_setting("garmin_dynamic_mode", "rolling_baseline")
    store.set_food_setting("garmin_active_factor", "1.3")

    start_day = date(2026, 11, 14)
    for offset in range(13):
        d_str = (start_day + timedelta(days=offset)).isoformat()
        store.upsert_garmin_daily(
            GarminDailyMetric(
                local_date=d_str,
                bmr_raw_kcal=2370.0,
                bmr_full_day_kcal=2370.0,
                active_kcal=250.0,
                total_burn_kcal=2620.0,
                steps=5000,
            )
        )
        if offset >= 2:
            store.create_meal_entry(
                local_date=d_str,
                raw_description="full day meals",
                estimate=MealEstimateSchema(
                    title="Daily Meals",
                    meal_slot="dinner",
                    confidence="high",
                    items=[
                        EstimatedMealItem(
                            name="Meals",
                            quantity_desc="1 day",
                            calories_kcal=2480.0,
                            protein_g=160.0,
                            carbs_g=240.0,
                            fat_g=70.0,
                        )
                    ],
                ),
                model_used="test-model",
            )
    # Add partial intake on today (2026-11-27) that must NOT drag down Adaptive TDEE
    today_str = "2026-11-27"
    store.create_meal_entry(
        local_date=today_str,
        raw_description="coffee and snack",
        estimate=MealEstimateSchema(
            title="Morning Snack",
            meal_slot="breakfast",
            confidence="high",
            items=[
                EstimatedMealItem(
                    name="Snack",
                    quantity_desc="1",
                    calories_kcal=400.0,
                    protein_g=10.0,
                    carbs_g=50.0,
                    fat_g=15.0,
                )
            ],
        ),
        model_used="test-model",
    )
    # Steady weight drop: 222.8 -> 221.3 over 13 days
    store.log_weight(local_date="2026-11-14", weight_lbs=222.8)
    store.log_weight(local_date="2026-11-20", weight_lbs=221.6)
    store.log_weight(local_date=today_str, weight_lbs=221.0)

    # Excluding today's partial 400 kcal meal keeps avg_daily_kcal at 2480.0
    tdee_complete = store.estimate_adaptive_tdee(today_str, exclude_end_date_meals=True)
    tdee_with_today = store.estimate_adaptive_tdee(today_str, exclude_end_date_meals=False)
    assert tdee_complete is not None and tdee_with_today is not None
    assert tdee_complete.avg_daily_kcal == 2480.0
    assert tdee_complete.estimated_tdee_kcal > tdee_with_today.estimated_tdee_kcal

    # Calibration recommends an additive offset relative to default 0.80 active factor
    cal_before = store.compute_burn_calibration(today_str, cfg)
    assert cal_before is not None
    assert cal_before["recommended_active_factor"] == 0.8
    assert cal_before["needs_calibration"] is True
    rec_offset = int(cal_before["recommended_burn_offset_kcal"])  # type: ignore[arg-type]
    assert 0 < rec_offset <= 600

    # One-time backfill resets 1.3 -> 0.80, applies burn_offset_kcal, and backfills historical days
    backfill_res = store.run_burn_calibration_backfill_once(today_str, cfg)
    assert backfill_res is not None
    assert backfill_res["active_factor"] == 0.8
    assert backfill_res["burn_offset_kcal"] == rec_offset

    # Running a second time is a strict no-op
    assert store.run_burn_calibration_backfill_once(today_str, cfg) is None

    # Past day (2026-11-25) now reflects the backfilled calibrated allowance & burn
    past_sum = store.get_daily_summary("2026-11-25", cfg)
    assert past_sum.dynamic_target is not None
    assert past_sum.dynamic_target.active_factor == 0.8
    assert past_sum.dynamic_target.burn_offset_kcal == rec_offset
    expected_past_kcal = round(2370.0 + 250.0 * 0.8 + rec_offset - 250)
    assert past_sum.target.calories_kcal == expected_past_kcal

    # Trends use calibrated burn and report calibrated status
    trends = store.get_food_dashboard_trends(today_str, cfg, days=14)
    assert trends["calibration"] is not None
    cal_after = trends["calibration"]
    assert isinstance(cal_after, dict)
    assert cal_after["current_burn_offset_kcal"] == rec_offset
    assert cal_after["needs_calibration"] is False
    series = trends["series"]
    assert isinstance(series, list)
    nov25 = next(row for row in series if row["local_date"] == "2026-11-25")
    assert nov25["raw_garmin_burn_kcal"] == 2620.0
    assert nov25["calibrated_burn_kcal"] == round(2370.0 + 250.0 * 0.8 + rec_offset, 1)
    assert nov25["garmin_burn_kcal"] == nov25["calibrated_burn_kcal"]

    # Future settings changes apply from today forward WITHOUT rewriting past days
    store.save_garmin_settings(local_date=today_str, fallback_cfg=cfg, goal_delta_kcal=-500)
    past_after = store.get_daily_summary("2026-11-25", cfg)
    today_after = store.get_daily_summary(today_str, cfg)
    assert past_after.target.calories_kcal == expected_past_kcal
    assert today_after.dynamic_target is not None
    assert today_after.dynamic_target.goal_delta_kcal == -500
    assert today_after.target.calories_kcal == expected_past_kcal - 250
