from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from agent_tasks.common.config import BillingConfig, ControllerSettings, TargetsConfig
from agent_tasks.common.models import IssueStatus, TokenUsageSummary, WorkerEnvelope
from agent_tasks.common.state import IssueRecord, StateStore
from agent_tasks.tasks.billing_guard import (
    BillingCycleWindow,
    BillingMonitorTask,
    compute_billing_cycle,
    compute_incremental_cost,
)


class FakeChannel:
    """Minimal ChannelAdapter stub recording billing channel posts."""

    def __init__(self, billing_channel: str = "C-BILLING-99") -> None:
        self.billing_channel = billing_channel
        self.posted: list[tuple[str, str, str | None]] = []

    async def start(self) -> None:
        return None

    async def stop(self) -> None:
        return None

    def resolve_channel(self, role: str) -> str:
        return self.billing_channel if role == "billing" else "C-DEFAULT"

    def resolve_channel_for_task(self, task_kind: str) -> str:
        return "C-DEFAULT"

    async def post_channel_message(
        self,
        text: str,
        *,
        role: str = "",
        channel_id: str | None = None,
    ) -> str:
        self.posted.append((role, text, channel_id))
        return f"ts-{len(self.posted)}"

    async def post_triage_result(
        self,
        record: IssueRecord,
        report: object,
        diff_text: str,
        *,
        model_used: str = "",
        cost_line: str = "",
    ) -> str:
        return "ts-triage"

    async def post_thread_message(
        self,
        thread_ts: str,
        text: str,
        *,
        channel_id: str | None = None,
    ) -> None:
        return None


def _make_settings(tmp_path: Path, targets: TargetsConfig | None = None) -> ControllerSettings:
    targets_path = tmp_path / "targets.yaml"
    if targets is not None:
        import yaml

        targets_path.write_text(yaml.safe_dump(targets.model_dump()), encoding="utf-8")
    return ControllerSettings(
        state_dir=tmp_path,
        targets_config_path=targets_path,
        target_github_token="gh-secret",
        slack_bot_token="xoxb-secret",
        slack_app_token="xapp-secret",
        slack_channel_id="C-DEFAULT",
    )


def _make_issue_record(
    issue_id: str = "iss-1",
    *,
    task_kind: str = "sentry",
    turn: int = 1,
    model_override: str | None = None,
) -> IssueRecord:
    return IssueRecord(
        issue_id=issue_id,
        short_id=issue_id.upper(),
        project_slug="python-fastapi",
        github_repo="lodbrok-app/rattlsnek",
        default_branch="main",
        title="Test issue",
        permalink="",
        status=IssueStatus.RUNNING,
        turn=turn,
        conversation_id="conv-1",
        thread_ts="100.1",
        channel_id="C-DEFAULT",
        pr_url=None,
        last_error=None,
        updated_at="2026-11-15T00:00:00Z",
        task_kind=task_kind,
        model_override=model_override,
    )


def test_compute_billing_cycle_boundaries_rollover_and_short_month_clamping() -> None:
    # Before anchor day (2026-12-05 -> cycle 2026-11-15..2026-12-15)
    before = compute_billing_cycle(datetime(2026, 12, 5, 12, 0, tzinfo=UTC), anchor_day=15)
    assert isinstance(before, BillingCycleWindow)
    assert before.cycle_key == "2026-11-15"
    assert before.start_utc == datetime(2026, 11, 15, 0, 0, tzinfo=UTC)
    assert before.end_utc == datetime(2026, 12, 15, 0, 0, tzinfo=UTC)
    assert before.total_days == pytest.approx(30.0)
    assert before.elapsed_days == pytest.approx(20.5)
    assert before.remaining_days == pytest.approx(9.5)

    # Exactly on anchor day at 00:00 UTC
    on_anchor = compute_billing_cycle(datetime(2026, 11, 15, 0, 0, tzinfo=UTC), anchor_day=15)
    assert on_anchor.cycle_key == "2026-11-15"
    assert on_anchor.start_utc == datetime(2026, 11, 15, 0, 0, tzinfo=UTC)
    assert on_anchor.end_utc == datetime(2026, 12, 15, 0, 0, tzinfo=UTC)
    assert on_anchor.elapsed_days == pytest.approx(0.0)
    assert on_anchor.remaining_days == pytest.approx(30.0)

    # Dec -> Jan year rollover
    dec_after = compute_billing_cycle(datetime(2026, 12, 20, 6, 0, tzinfo=UTC), anchor_day=15)
    assert dec_after.cycle_key == "2026-12-15"
    assert dec_after.start_utc == datetime(2026, 12, 15, 0, 0, tzinfo=UTC)
    assert dec_after.end_utc == datetime(2027, 1, 15, 0, 0, tzinfo=UTC)
    assert dec_after.total_days == pytest.approx(31.0)

    jan_before = compute_billing_cycle(datetime(2027, 1, 5, 0, 0, tzinfo=UTC), anchor_day=15)
    assert jan_before.cycle_key == "2026-12-15"
    assert jan_before.start_utc == datetime(2026, 12, 15, 0, 0, tzinfo=UTC)
    assert jan_before.end_utc == datetime(2027, 1, 15, 0, 0, tzinfo=UTC)

    # anchor_day=31 clamping in February (non-leap 2026 and leap 2024)
    feb_before = compute_billing_cycle(datetime(2026, 2, 15, 0, 0, tzinfo=UTC), anchor_day=31)
    assert feb_before.cycle_key == "2026-01-31"
    assert feb_before.start_utc == datetime(2026, 1, 31, 0, 0, tzinfo=UTC)
    assert feb_before.end_utc == datetime(2026, 2, 28, 0, 0, tzinfo=UTC)

    feb_on_clamped = compute_billing_cycle(datetime(2026, 2, 28, 12, 0, tzinfo=UTC), anchor_day=31)
    assert feb_on_clamped.cycle_key == "2026-02-28"
    assert feb_on_clamped.start_utc == datetime(2026, 2, 28, 0, 0, tzinfo=UTC)
    assert feb_on_clamped.end_utc == datetime(2026, 3, 31, 0, 0, tzinfo=UTC)

    leap_feb = compute_billing_cycle(datetime(2024, 2, 29, 0, 0, tzinfo=UTC), anchor_day=31)
    assert leap_feb.cycle_key == "2024-02-29"
    assert leap_feb.start_utc == datetime(2024, 2, 29, 0, 0, tzinfo=UTC)
    assert leap_feb.end_utc == datetime(2024, 3, 31, 0, 0, tzinfo=UTC)

    # Naive datetime assumes UTC, offset timezone converts to UTC before resolving boundary
    naive_window = compute_billing_cycle(datetime(2026, 12, 5, 12, 0), anchor_day=15)
    assert naive_window == before

    plus2 = timezone(timedelta(hours=2))
    # 2026-12-15 00:00+02:00 is 2026-12-14 22:00 UTC -> still inside Nov 15 cycle
    tz_window = compute_billing_cycle(datetime(2026, 12, 15, 0, 0, tzinfo=plus2), anchor_day=15)
    assert tz_window.cycle_key == "2026-11-15"


def test_compute_incremental_cost_pricing_and_turn2_deduplication(tmp_path: Path) -> None:
    cfg = BillingConfig()

    # Hydrogen: $1.50/1M uncached input, $0.15/1M cached input, $7.50/1M output+thoughts
    usage_1m = TokenUsageSummary(
        prompt_tokens=1_000_000,
        cached_tokens=400_000,
        output_tokens=100_000,
        thoughts_tokens=100_000,
        total_tokens=1_200_000,
    )
    delta_h, cost_h = compute_incremental_cost(
        model="gemini-4-hydrogen",
        cumulative_usage=usage_1m,
        prior_tokens=(0, 0, 0, 0),
        billing_cfg=cfg,
    )
    # 600k * 1.50 + 400k * 0.15 + 200k * 7.50 = 0.90 + 0.06 + 1.50 = 2.46
    assert cost_h == pytest.approx(2.46)
    assert delta_h.prompt_tokens == 1_000_000
    assert delta_h.cached_tokens == 400_000
    assert delta_h.output_tokens == 100_000
    assert delta_h.thoughts_tokens == 100_000
    assert delta_h.total_tokens == 1_200_000

    # Carbon: $4.00/1M uncached input, $0.40/1M cached input, $20.00/1M output+thoughts
    _, cost_c = compute_incremental_cost(
        model="gemini-4-carbon",
        cumulative_usage=usage_1m,
        prior_tokens=(0, 0, 0, 0),
        billing_cfg=cfg,
    )
    # 600k * 4.00 + 400k * 0.40 + 200k * 20.00 = 2.40 + 0.16 + 4.00 = 6.56
    assert cost_c == pytest.approx(6.56)

    # Cached tokens clamped to prompt delta when cumulative cached exceeds prompt delta
    clamped_delta, clamped_cost = compute_incremental_cost(
        model="gemini-4-hydrogen",
        cumulative_usage=TokenUsageSummary(
            prompt_tokens=1500,
            cached_tokens=1400,
            output_tokens=200,
            thoughts_tokens=0,
            total_tokens=1700,
        ),
        prior_tokens=(1000, 200, 100, 0),
        billing_cfg=cfg,
    )
    assert clamped_delta.prompt_tokens == 500
    assert clamped_delta.cached_tokens == 500
    assert clamped_delta.output_tokens == 100
    assert clamped_cost == pytest.approx((500 * 0.15 + 100 * 7.50) / 1_000_000.0)


async def test_record_worker_turn_never_double_counts_prior_turns(tmp_path: Path) -> None:
    store = StateStore(tmp_path / "state.sqlite3")
    channel = FakeChannel()
    settings = _make_settings(tmp_path)
    task = BillingMonitorTask(settings=settings, state=store, channel=channel)

    now = datetime(2026, 11, 20, 12, 0, tzinfo=UTC)
    turn1_rec = _make_issue_record("iss-42", task_kind="sentry", turn=1)
    turn1_env = WorkerEnvelope(
        issue_id="iss-42",
        turn=1,
        conversation_id="conv-xyz",
        model="gemini-4-hydrogen",
        usage=TokenUsageSummary(
            prompt_tokens=1_000_000,
            cached_tokens=200_000,
            output_tokens=100_000,
            thoughts_tokens=50_000,
            total_tokens=1_150_000,
        ),
    )
    cost_t1 = await task.record_worker_turn(
        record=turn1_rec,
        envelope=turn1_env,
        default_model="gemini-4-hydrogen",
        now=now,
    )
    # 800k*1.50 + 200k*0.15 + 150k*7.50 = 1.20 + 0.03 + 1.125 = 2.355
    assert cost_t1 == pytest.approx(2.355)

    # Turn 2 reports CUMULATIVE usage across Turn 1 + Turn 2 on the same conversation_id
    turn2_rec = _make_issue_record("iss-42", task_kind="sentry", turn=2)
    turn2_env = WorkerEnvelope(
        issue_id="iss-42",
        turn=2,
        conversation_id="conv-xyz",
        model="gemini-4-hydrogen",
        usage=TokenUsageSummary(
            prompt_tokens=1_500_000,
            cached_tokens=600_000,
            output_tokens=150_000,
            thoughts_tokens=100_000,
            total_tokens=1_750_000,
        ),
    )
    cost_t2 = await task.record_worker_turn(
        record=turn2_rec,
        envelope=turn2_env,
        default_model="gemini-4-hydrogen",
        now=now,
    )
    # Turn 2 deltas: prompt=500k, cached=400k (uncached=100k), output=50k, thoughts=50k
    # Cost: 100k*1.50 + 400k*0.15 + 100k*7.50 = 0.15 + 0.06 + 0.75 = 0.96
    assert cost_t2 == pytest.approx(0.96)

    summary = store.get_cycle_usage_summary("2026-11-15")
    assert summary["spent_usd"] == pytest.approx(2.355 + 0.96)
    assert summary["prompt_tokens"] == 1_500_000
    assert summary["cached_tokens"] == 600_000
    assert summary["output_tokens"] == 150_000
    assert summary["thoughts_tokens"] == 100_000
    assert summary["job_count"] == 2


async def test_evaluate_and_alert_thresholds_dedup_hard_stop_and_resume(tmp_path: Path) -> None:
    store = StateStore(tmp_path / "state.sqlite3")
    channel = FakeChannel()
    dispatch_cb = AsyncMock()
    targets = TargetsConfig(billing=BillingConfig(projected_overrun_warn=False))
    settings = _make_settings(tmp_path, targets)
    task = BillingMonitorTask(
        settings=settings,
        state=store,
        channel=channel,
        dispatch_callback=dispatch_cb,
    )

    now = datetime(2026, 11, 20, 0, 0, tzinfo=UTC)
    cycle_key = "2026-11-15"

    # Below 50% -> no alerts
    store.record_usage_entry(
        cycle_key=cycle_key,
        issue_id="j1",
        turn=1,
        conversation_id=None,
        task_kind="sentry",
        model="gemini-4-hydrogen",
        prompt_tokens=0,
        cached_tokens=0,
        output_tokens=0,
        thoughts_tokens=0,
        cost_usd=49.0,
    )
    assert await task.evaluate_and_alert(now=now) == []
    assert task.is_background_paused(now=now) == (False, "")

    # Cross 50% -> pct-50 fires once, deduped on repeat
    store.record_usage_entry(
        cycle_key=cycle_key,
        issue_id="j2",
        turn=1,
        conversation_id=None,
        task_kind="sentry",
        model="gemini-4-hydrogen",
        prompt_tokens=0,
        cached_tokens=0,
        output_tokens=0,
        thoughts_tokens=0,
        cost_usd=2.0,
    )
    assert await task.evaluate_and_alert(now=now) == ["pct-50"]
    assert await task.evaluate_and_alert(now=now) == []

    # Cross 75% -> pct-75
    store.record_usage_entry(
        cycle_key=cycle_key,
        issue_id="j3",
        turn=1,
        conversation_id=None,
        task_kind="ci_healer",
        model="gemini-4-carbon",
        prompt_tokens=0,
        cached_tokens=0,
        output_tokens=0,
        thoughts_tokens=0,
        cost_usd=25.0,
    )
    assert await task.evaluate_and_alert(now=now) == ["pct-75"]

    # Cross 90% and 95% -> pct-90 and hard-stop; background paused
    store.record_usage_entry(
        cycle_key=cycle_key,
        issue_id="j4",
        turn=1,
        conversation_id=None,
        task_kind="k8s_watchdog",
        model="gemini-4-hydrogen",
        prompt_tokens=0,
        cached_tokens=0,
        output_tokens=0,
        thoughts_tokens=0,
        cost_usd=19.0,
    )
    fired_95 = await task.evaluate_and_alert(now=now)
    assert fired_95 == ["pct-90", "hard-stop"]
    assert await task.evaluate_and_alert(now=now) == []
    paused, reason = task.is_background_paused(now=now)
    assert paused is True
    assert "95%" in reason
    assert "$95.00" in reason
    assert "Credit Safety Brake Engaged" in channel.posted[-1][1]
    assert all(role == "billing" for role, _, _ in channel.posted)

    # !billing resume overrides hard-stop for the current cycle and calls dispatch_callback
    resume_msg = await task.handle_billing_command("resume", now=now)
    assert "2026-11-15" in resume_msg
    dispatch_cb.assert_awaited_once()
    assert task.is_background_paused(now=now) == (False, "")

    # Cross 100% -> pct-100 fires, but hard-stop stays suppressed in resumed cycle
    store.record_usage_entry(
        cycle_key=cycle_key,
        issue_id="j5",
        turn=1,
        conversation_id=None,
        task_kind="dep_upgrader",
        model="gemini-4-hydrogen",
        prompt_tokens=0,
        cached_tokens=0,
        output_tokens=0,
        thoughts_tokens=0,
        cost_usd=5.0,
    )
    assert await task.evaluate_and_alert(now=now) == ["pct-100"]
    assert task.is_background_paused(now=now) == (False, "")

    # Next cycle starts fresh: override from 2026-11-15 does NOT carry over to 2026-12-15
    next_cycle_now = datetime(2026, 12, 16, 0, 0, tzinfo=UTC)
    store.record_usage_entry(
        cycle_key="2026-12-15",
        issue_id="j6",
        turn=1,
        conversation_id=None,
        task_kind="sentry",
        model="gemini-4-hydrogen",
        prompt_tokens=0,
        cached_tokens=0,
        output_tokens=0,
        thoughts_tokens=0,
        cost_usd=96.0,
    )
    paused_next, _ = task.is_background_paused(now=next_cycle_now)
    assert paused_next is True
    assert "hard-stop" in await task.evaluate_and_alert(now=next_cycle_now)


async def test_projected_overrun_respects_24h_gate_and_fires_after_24h(tmp_path: Path) -> None:
    store = StateStore(tmp_path / "state.sqlite3")
    channel = FakeChannel()
    settings = _make_settings(tmp_path)
    task = BillingMonitorTask(settings=settings, state=store, channel=channel)

    cycle_key = "2026-11-15"
    store.record_usage_entry(
        cycle_key=cycle_key,
        issue_id="j1",
        turn=1,
        conversation_id=None,
        task_kind="ci_healer",
        model="gemini-4-carbon",
        prompt_tokens=0,
        cached_tokens=0,
        output_tokens=0,
        thoughts_tokens=0,
        cost_usd=7.50,
    )

    # At 12h elapsed (<24h gate): no projection alert, report states collecting baseline
    t_12h = datetime(2026, 11, 15, 12, 0, tzinfo=UTC)
    assert await task.evaluate_and_alert(now=t_12h) == []
    assert "collecting baseline (<24h or <$3)" in task.format_report(now=t_12h)

    # At 48h elapsed (2.0 days): $7.50 / 2.0 * 30.0 = $112.50 -> proj-100 fires once
    t_48h = datetime(2026, 11, 17, 0, 0, tzinfo=UTC)
    fired = await task.evaluate_and_alert(now=t_48h)
    assert fired == ["proj-100"]
    assert await task.evaluate_and_alert(now=t_48h) == []

    role, alert_text, _ = channel.posted[-1]
    assert role == "billing"
    assert "Projected Credit Overrun Warning" in alert_text
    assert "$3.75/day" in alert_text
    assert "$112.50" in alert_text
    assert "ci_healer" in alert_text
    assert "gemini-4-carbon" in alert_text

    # Escalating burn to $10.50 at 48h -> $157.50 projected -> proj-150 fires
    store.record_usage_entry(
        cycle_key=cycle_key,
        issue_id="j2",
        turn=1,
        conversation_id=None,
        task_kind="sentry",
        model="gemini-4-hydrogen",
        prompt_tokens=0,
        cached_tokens=0,
        output_tokens=0,
        thoughts_tokens=0,
        cost_usd=3.00,
    )
    assert await task.evaluate_and_alert(now=t_48h) == ["proj-150"]
    assert "$157.50" in task.format_report(now=t_48h)


async def test_billing_commands_sync_pause_status_test_alert_and_help(tmp_path: Path) -> None:
    store = StateStore(tmp_path / "state.sqlite3")
    channel = FakeChannel(billing_channel="C-BILLING-42")
    targets = TargetsConfig(billing=BillingConfig(projected_overrun_warn=False))
    settings = _make_settings(tmp_path, targets)
    task = BillingMonitorTask(settings=settings, state=store, channel=channel)

    now = datetime(2026, 11, 20, 0, 0, tzinfo=UTC)

    # !billing sync $48.50
    sync_out = await task.handle_billing_command("sync $48.50", now=now)
    assert "$48.50 / $100.00" in sync_out
    assert "48.5%" in sync_out
    assert "manual_sync" in sync_out
    assert store.get_cycle_usage_summary("2026-11-15")["spent_usd"] == pytest.approx(48.50)
    assert store.get_cycle_usage_summary("2026-11-15")["job_count"] == 0

    # Syncing past 50% triggers alert automatically
    sync_up = await task.handle_billing_command("sync 52.00", now=now)
    assert "$52.00 / $100.00" in sync_up
    assert len(channel.posted) == 1
    assert channel.posted[0][0] == "billing"

    # Invalid sync rejected cleanly
    invalid_out = await task.handle_billing_command("sync -5", now=now)
    assert "non-negative" in invalid_out
    assert store.get_cycle_usage_summary("2026-11-15")["spent_usd"] == pytest.approx(52.00)

    # !billing pause sets manual pause even after an earlier resume
    await task.handle_billing_command("resume", now=now)
    assert task.is_background_paused(now=now) == (False, "")
    pause_out = await task.handle_billing_command("pause", now=now)
    assert "paused" in pause_out.lower()
    paused, reason = task.is_background_paused(now=now)
    assert paused is True
    assert "!billing pause" in reason
    assert "PAUSED:" in task.format_short_status(now=now)

    # !billing test-alert routes to role="billing" and names channel ID
    test_out = await task.handle_billing_command("test-alert", now=now)
    assert "C-BILLING-42" in test_out
    last_role, last_msg, _ = channel.posted[-1]
    assert last_role == "billing"
    assert "[TEST] Credit Budget Alert Wiring Check" in last_msg

    # !billing status and unknown subcommands
    status_out = await task.handle_billing_command("status", now=now)
    assert "Credit & Token Billing Report" in status_out
    help_out = await task.handle_billing_command("bogus", now=now)
    assert "!billing sync <usd>" in help_out


async def test_billing_monitor_run_loop_handles_errors_and_cancellation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = StateStore(tmp_path / "state.sqlite3")
    channel = FakeChannel()
    targets = TargetsConfig(billing=BillingConfig(check_interval_seconds=1))
    settings = _make_settings(tmp_path, targets)
    heartbeats = 0

    def bump_heartbeat() -> None:
        nonlocal heartbeats
        heartbeats += 1

    task = BillingMonitorTask(
        settings=settings,
        state=store,
        channel=channel,
        heartbeat_cb=bump_heartbeat,
    )

    calls = 0

    async def flaky_eval(*, targets: TargetsConfig | None = None) -> list[str]:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("transient error")
        return []

    monkeypatch.setattr(task, "evaluate_and_alert", flaky_eval)

    sleep_calls = 0

    async def fake_sleep(seconds: float) -> None:
        nonlocal sleep_calls
        sleep_calls += 1
        if sleep_calls >= 2:
            raise asyncio.CancelledError

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)

    with pytest.raises(asyncio.CancelledError):
        await task.run()

    assert heartbeats == 2
    assert calls == 2
