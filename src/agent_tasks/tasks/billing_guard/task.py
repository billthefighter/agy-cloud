from __future__ import annotations

import asyncio
import math
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from typing import Any

import structlog

from agent_tasks.channels.base import ChannelAdapter
from agent_tasks.common.config import BillingConfig, ControllerSettings, TargetsConfig
from agent_tasks.common.models import WorkerEnvelope
from agent_tasks.common.state import IssueRecord, StateStore
from agent_tasks.tasks.billing_guard.pricing import (
    BillingCycleWindow,
    compute_billing_cycle,
    compute_incremental_cost,
)

log = structlog.get_logger(__name__)


def _fmt_usd(amount: float) -> str:
    abs_val = abs(amount)
    sign = "-" if amount < 0 else ""
    if 0 < abs_val < 0.01:
        return f"{sign}${abs_val:.4f}"
    return f"{sign}${abs_val:.2f}"


def _format_top_spenders(mapping: dict[str, float], limit: int = 5) -> str:
    if not mapping:
        return "_none_"
    ranked = sorted(mapping.items(), key=lambda item: item[1], reverse=True)[:limit]
    return ", ".join(f"`{name}` ({_fmt_usd(usd)})" for name, usd in ranked)


class BillingMonitorTask:
    """Long-lived credit monitor, overrun predictor, safety brake, and `!billing` handler."""

    name = "billing-monitor"

    def __init__(
        self,
        *,
        settings: ControllerSettings,
        state: StateStore,
        channel: ChannelAdapter,
        heartbeat_cb: Callable[[], None] | None = None,
        dispatch_callback: Callable[[], Awaitable[None]] | None = None,
    ) -> None:
        self.settings = settings
        self.state = state
        self.channel = channel
        self.heartbeat_cb = heartbeat_cb
        self.dispatch_callback = dispatch_callback

    def load_targets(self) -> TargetsConfig:
        if self.settings.targets_config_path.exists():
            return TargetsConfig.from_yaml(self.settings.targets_config_path)
        return TargetsConfig()

    def is_background_paused(
        self,
        now: datetime | None = None,
        targets: TargetsConfig | None = None,
    ) -> tuple[bool, str]:
        """Return `(paused, reason)` indicating if background automations must pause."""
        if self.state.get_kv("billing_manual_pause") == "1":
            return (True, "manual pause (`!billing pause`)")

        cfg = targets or self.load_targets()
        cycle = compute_billing_cycle(now, cfg.billing.cycle_anchor_day)
        if self.state.get_kv("billing_resume_cycle") == cycle.cycle_key:
            return (False, "")

        if (
            cfg.billing.enabled
            and cfg.billing.hard_stop_pct > 0
            and cfg.billing.monthly_credit_usd > 0
        ):
            spent = float(self.state.get_cycle_usage_summary(cycle.cycle_key)["spent_usd"])
            pct = (spent / cfg.billing.monthly_credit_usd) * 100.0
            if pct >= cfg.billing.hard_stop_pct:
                reason = (
                    f"cycle spend `${spent:.2f}` reached `{cfg.billing.hard_stop_pct:.0f}%` "
                    f"safety brake of `${cfg.billing.monthly_credit_usd:.2f}`"
                )
                return (True, reason)

        return (False, "")

    def _compute_projection(
        self,
        *,
        summary: dict[str, Any],
        cycle: BillingCycleWindow,
        cfg: BillingConfig,
    ) -> tuple[float | None, float | None, str | None]:
        """Return `(daily_burn_usd, projected_cycle_usd, estimated_exhaustion_iso)`."""
        elapsed_hours = cycle.elapsed_days * 24.0
        spent = float(summary["spent_usd"])
        if (
            elapsed_hours < cfg.min_hours_for_projection
            or spent < cfg.min_spend_usd_for_projection
            or cycle.elapsed_days <= 0
        ):
            return (None, None, None)

        daily_burn = spent / cycle.elapsed_days
        projected_total = daily_burn * cycle.total_days
        if (
            daily_burn > 0
            and cfg.monthly_credit_usd > 0
            and projected_total > cfg.monthly_credit_usd
        ):
            days_to_cap = cfg.monthly_credit_usd / daily_burn
            exhaust_dt = cycle.start_utc + timedelta(days=days_to_cap)
            exhaust_str = exhaust_dt.strftime("%Y-%m-%d %H:%MZ")
        else:
            exhaust_str = None
        return (daily_burn, projected_total, exhaust_str)

    async def evaluate_and_alert(
        self,
        now: datetime | None = None,
        targets: TargetsConfig | None = None,
    ) -> list[str]:
        """Evaluate threshold, projected overrun, and safety-brake rules; return fired alert keys."""
        cfg = targets or self.load_targets()
        billing_cfg = cfg.billing
        if not billing_cfg.enabled or billing_cfg.monthly_credit_usd <= 0:
            return []

        cycle = compute_billing_cycle(now, billing_cfg.cycle_anchor_day)
        summary = self.state.get_cycle_usage_summary(cycle.cycle_key)
        spent = float(summary["spent_usd"])
        credit = float(billing_cfg.monthly_credit_usd)
        pct = (spent / credit) * 100.0
        remaining_usd = max(0.0, credit - spent)
        newly_fired: list[str] = []

        # 1) Percentage spend threshold alerts (deduplicated per cycle)
        for threshold in sorted(billing_cfg.warn_thresholds_pct):
            thresh_int = round(threshold)
            alert_key = f"pct-{thresh_int}"
            if pct >= threshold and not self.state.has_billing_alert(cycle.cycle_key, alert_key):
                emoji = (
                    ":rotating_light:"
                    if threshold >= 100
                    else (":warning:" if threshold >= 75 else ":bell:")
                )
                msg = (
                    f"{emoji} *Credit Budget Threshold Reached (`{thresh_int}%`)*\n"
                    f"• *Cycle:* `{cycle.cycle_key}` -> `{cycle.end_utc:%Y-%m-%d}` UTC "
                    f"(`{cycle.remaining_days:.1f}d` remaining)\n"
                    f"• *Spend:* `${spent:.2f} / ${credit:.2f}` (`{pct:.1f}%`, "
                    f"`${remaining_usd:.2f}` remaining)\n"
                    f"• *Top Automations:* {_format_top_spenders(summary['by_task_kind'])}\n"
                    f"• *Top Models:* {_format_top_spenders(summary['by_model'])}"
                )
                await self.channel.post_channel_message(msg, role="billing")
                self.state.mark_billing_alert(cycle.cycle_key, alert_key)
                newly_fired.append(alert_key)

        # 2) Projected cycle overrun warning
        if billing_cfg.projected_overrun_warn:
            daily_burn, projected_total, exhaust_str = self._compute_projection(
                summary=summary,
                cycle=cycle,
                cfg=billing_cfg,
            )
            if daily_burn is not None and projected_total is not None and projected_total > credit:
                overrun_pct = (projected_total / credit) * 100.0
                tier = "150" if overrun_pct >= 150 else ("125" if overrun_pct >= 125 else "100")
                alert_key = f"proj-{tier}"
                if not self.state.has_billing_alert(cycle.cycle_key, alert_key):
                    exhaust_label = exhaust_str or "N/A"
                    msg = (
                        f":warning: *Projected Credit Overrun Warning*\n"
                        f"• *Cycle:* `{cycle.cycle_key}` -> `{cycle.end_utc:%Y-%m-%d}` UTC "
                        f"(`{cycle.remaining_days:.1f}d` remaining)\n"
                        f"• *Cycle Spent:* `${spent:.2f} / ${credit:.2f}` (`{pct:.1f}%`)\n"
                        f"• *Daily Burn Rate:* `${daily_burn:.2f}/day`\n"
                        f"• *Projected Total:* `${projected_total:.2f}` (`{overrun_pct:.0f}%` of budget)\n"
                        f"• *Est. Exhaustion Date:* `{exhaust_label}`\n"
                        f"• *Top Automations:* {_format_top_spenders(summary['by_task_kind'])}\n"
                        f"• *Top Models:* {_format_top_spenders(summary['by_model'])}"
                    )
                    await self.channel.post_channel_message(msg, role="billing")
                    self.state.mark_billing_alert(cycle.cycle_key, alert_key)
                    newly_fired.append(alert_key)

        # 3) Hard-stop background safety brake
        if (
            billing_cfg.hard_stop_pct > 0
            and pct >= billing_cfg.hard_stop_pct
            and self.state.get_kv("billing_resume_cycle") != cycle.cycle_key
            and not self.state.has_billing_alert(cycle.cycle_key, "hard-stop")
        ):
            msg = (
                f":octagonal_sign: *Credit Safety Brake Engaged*\n"
                f"Background tasks (`sentry`, `ci_healer`, `k8s_watchdog`, `dep_upgrader`) "
                f"are paused at `${spent:.2f} / ${credit:.2f}` (`{pct:.1f}%` >= "
                f"`{billing_cfg.hard_stop_pct:.0f}%`) until `{cycle.end_utc:%Y-%m-%d}` "
                f"or `!billing resume`.\n"
                f"Interactive `@agytasks` / `!code` requests remain available."
            )
            await self.channel.post_channel_message(msg, role="billing")
            self.state.mark_billing_alert(cycle.cycle_key, "hard-stop")
            newly_fired.append("hard-stop")

        return newly_fired

    async def record_worker_turn(
        self,
        *,
        record: IssueRecord,
        envelope: WorkerEnvelope,
        default_model: str,
        now: datetime | None = None,
    ) -> float:
        """Record incremental token usage & USD cost for one worker turn and evaluate alerts."""
        cfg = self.load_targets()
        cycle = compute_billing_cycle(now, cfg.billing.cycle_anchor_day)
        model = (
            envelope.model or record.model_override or default_model or "gemini-4-hydrogen"
        ).strip()
        conv_id = (envelope.conversation_id or "").strip()
        prior = self.state.prior_conversation_tokens(conv_id) if conv_id else (0, 0, 0, 0)

        delta, cost_usd = compute_incremental_cost(
            model=model,
            cumulative_usage=envelope.usage,
            prior_tokens=prior,
            billing_cfg=cfg.billing,
        )

        recorded_at: str | None = None
        if now is not None:
            now_utc = now.replace(tzinfo=UTC) if now.tzinfo is None else now.astimezone(UTC)
            recorded_at = now_utc.isoformat()

        self.state.record_usage_entry(
            cycle_key=cycle.cycle_key,
            issue_id=record.issue_id,
            turn=record.turn,
            conversation_id=conv_id or None,
            task_kind=record.task_kind,
            model=model,
            prompt_tokens=delta.prompt_tokens,
            cached_tokens=delta.cached_tokens,
            output_tokens=delta.output_tokens,
            thoughts_tokens=delta.thoughts_tokens,
            cost_usd=cost_usd,
            recorded_at=recorded_at,
        )
        await self.evaluate_and_alert(now=now, targets=cfg)
        return cost_usd

    def format_short_status(self, now: datetime | None = None) -> str:
        """Return a concise one-line status string suitable for `!ping` / `!status`."""
        cfg = self.load_targets()
        billing_cfg = cfg.billing
        cycle = compute_billing_cycle(now, billing_cfg.cycle_anchor_day)
        summary = self.state.get_cycle_usage_summary(cycle.cycle_key)
        spent = float(summary["spent_usd"])
        credit = float(billing_cfg.monthly_credit_usd)
        pct = (spent / credit) * 100.0 if credit > 0 else 0.0

        paused, reason = self.is_background_paused(now=now, targets=cfg)
        if paused:
            state_label = f"PAUSED: {reason}"
        elif not billing_cfg.enabled:
            state_label = "disabled"
        elif self.state.get_kv("billing_resume_cycle") == cycle.cycle_key:
            state_label = "resumed"
        else:
            state_label = "active"
        return f"`${spent:.2f} / ${credit:.2f}` (`{pct:.1f}%`, cycle `{cycle.cycle_key}`, {state_label})"

    def format_report(self, now: datetime | None = None) -> str:
        """Format a rich Slack mrkdwn credit & token usage report for `!billing`."""
        cfg = self.load_targets()
        billing_cfg = cfg.billing
        cycle = compute_billing_cycle(now, billing_cfg.cycle_anchor_day)
        summary = self.state.get_cycle_usage_summary(cycle.cycle_key)
        spent = float(summary["spent_usd"])
        credit = float(billing_cfg.monthly_credit_usd)
        pct = (spent / credit) * 100.0 if credit > 0 else 0.0
        remaining_usd = max(0.0, credit - spent)

        daily_burn, projected_total, exhaust_str = self._compute_projection(
            summary=summary,
            cycle=cycle,
            cfg=billing_cfg,
        )
        if daily_burn is None or projected_total is None:
            proj_line = (
                f"collecting baseline (<{billing_cfg.min_hours_for_projection:g}h "
                f"or <${billing_cfg.min_spend_usd_for_projection:g})"
            )
        elif exhaust_str:
            proj_line = (
                f"`${projected_total:.2f}` (`${daily_burn:.2f}/day`, "
                f"est. exhaustion `{exhaust_str}`)"
            )
        else:
            proj_line = f"`${projected_total:.2f}` (`${daily_burn:.2f}/day`, within credit)"

        paused, reason = self.is_background_paused(now=now, targets=cfg)
        resumed_override = self.state.get_kv("billing_resume_cycle") == cycle.cycle_key
        if paused:
            brake_status = f"PAUSED ({reason})"
        elif resumed_override:
            brake_status = f"RESUMED (override active for cycle `{cycle.cycle_key}`)"
        elif billing_cfg.enabled and billing_cfg.hard_stop_pct > 0:
            brake_status = f"ARMED at `{billing_cfg.hard_stop_pct:.0f}%`"
        else:
            brake_status = "DISABLED"

        lines = [
            "*Credit & Token Billing Report*",
            (
                f"• *Cycle:* `{cycle.cycle_key}` (`{cycle.start_utc:%Y-%m-%d}` -> "
                f"`{cycle.end_utc:%Y-%m-%d}` UTC, `{cycle.elapsed_days:.1f}d` elapsed, "
                f"`{cycle.remaining_days:.1f}d` remaining)"
            ),
            (
                f"• *Spent / Monthly Credit:* `${spent:.2f} / ${credit:.2f}` "
                f"(`{pct:.1f}%` used, `${remaining_usd:.2f}` remaining, "
                f"`{summary['job_count']}` worker turns)"
            ),
            (
                f"• *Tokens:* prompt `{summary['prompt_tokens']:,}` "
                f"(cached `{summary['cached_tokens']:,}`), "
                f"output `{summary['output_tokens']:,}`, "
                f"thoughts `{summary['thoughts_tokens']:,}`"
            ),
            f"• *Projected Cycle Total / Daily Burn:* {proj_line}",
            f"• *Safety Brake:* {brake_status}",
        ]

        by_task: dict[str, float] = summary["by_task_kind"]
        lines.append("• *Breakdown by Automation:*")
        if by_task:
            for kind, usd in by_task.items():
                lines.append(f"  ◦ `{kind}`: `{_fmt_usd(usd)}`")
        else:
            lines.append("  ◦ _none recorded yet_")

        by_model: dict[str, float] = summary["by_model"]
        lines.append("• *Breakdown by Model:*")
        if by_model:
            for model_name, usd in by_model.items():
                lines.append(f"  ◦ `{model_name}`: `{_fmt_usd(usd)}`")
        else:
            lines.append("  ◦ _none recorded yet_")

        return "\n".join(lines)

    async def handle_billing_command(
        self,
        args_text: str,
        *,
        now: datetime | None = None,
    ) -> str:
        """Handle `!billing` Slack subcommands."""
        raw = args_text.strip()
        lower = raw.lower()
        if not raw or lower == "status":
            return self.format_report(now=now)

        parts = raw.split()
        verb = parts[0].lower()

        if verb == "sync":
            if len(parts) != 2:
                return "Usage: `!billing sync <usd>` (for example, `!billing sync $18.40`)."
            cleaned_num = parts[1].strip().lstrip("$").replace(",", "")
            try:
                target_usd = float(cleaned_num)
            except ValueError:
                return f"Invalid USD amount `{parts[1]}`. Example: `!billing sync $18.40`."
            if not math.isfinite(target_usd) or target_usd < 0:
                return f"USD amount must be finite and non-negative, got `{parts[1]}`."

            cfg = self.load_targets()
            cycle = compute_billing_cycle(now, cfg.billing.cycle_anchor_day)
            summary = self.state.get_cycle_usage_summary(cycle.cycle_key)
            current_spent = float(summary["spent_usd"])
            delta = target_usd - current_spent

            recorded_at: str | None = None
            if now is not None:
                now_utc = now.replace(tzinfo=UTC) if now.tzinfo is None else now.astimezone(UTC)
                recorded_at = now_utc.isoformat()

            self.state.record_usage_entry(
                cycle_key=cycle.cycle_key,
                issue_id=f"manual-sync-{cycle.cycle_key}",
                turn=1,
                conversation_id=None,
                task_kind="manual_sync",
                model="adjustment",
                prompt_tokens=0,
                cached_tokens=0,
                output_tokens=0,
                thoughts_tokens=0,
                cost_usd=delta,
                note="Manual sync via !billing sync",
                recorded_at=recorded_at,
            )
            await self.evaluate_and_alert(now=now, targets=cfg)
            updated_report = self.format_report(now=now)
            return (
                f"Synced cycle `{cycle.cycle_key}` ledger to `${target_usd:.2f}` "
                f"(adjustment `{delta:+.2f}` from `${current_spent:.2f}`).\n\n{updated_report}"
            )

        if verb == "pause" and len(parts) == 1:
            self.state.set_kv("billing_manual_pause", "1")
            return (
                ":pause_button: Background automations (`sentry`, `ci_healer`, `k8s_watchdog`, "
                "`dep_upgrader`) are now manually paused (`!billing pause`). "
                "Interactive `@agytasks` / `!code` requests remain active; run `!billing resume` to resume."
            )

        if verb == "resume" and len(parts) == 1:
            cfg = self.load_targets()
            cycle = compute_billing_cycle(now, cfg.billing.cycle_anchor_day)
            self.state.set_kv("billing_manual_pause", "0")
            self.state.set_kv("billing_resume_cycle", cycle.cycle_key)
            if self.dispatch_callback is not None:
                await self.dispatch_callback()
            return (
                f":arrow_forward: Background automations resumed for cycle `{cycle.cycle_key}` "
                f"(manual pause cleared and safety-brake override set until `{cycle.end_utc:%Y-%m-%d}` UTC)."
            )

        if verb == "test-alert" and len(parts) == 1:
            target_channel = self.channel.resolve_channel("billing")
            cycle = compute_billing_cycle(now, self.load_targets().billing.cycle_anchor_day)
            await self.channel.post_channel_message(
                (
                    f":bell: *[TEST] Credit Budget Alert Wiring Check*\n"
                    f"Billing alerts are wired and routing to `{target_channel}` "
                    f"(cycle `{cycle.cycle_key}`, status: {self.format_short_status(now=now)})."
                ),
                role="billing",
            )
            return f"Posted test billing alert to channel `{target_channel}`."

        return (
            "*Billing Guard Usage*\n"
            "• `!billing` — show current cycle spend, projection, and breakdowns\n"
            "• `!billing sync <usd>` — calibrate cycle ledger (e.g. `!billing sync $18.40`)\n"
            "• `!billing pause` — manually pause background automations\n"
            "• `!billing resume` — clear pause & override safety brake for current cycle\n"
            "• `!billing test-alert` — post a test alert to the `billing` channel"
        )

    async def run(self) -> None:
        log.info("billing_monitor.loop_started")
        while True:
            if self.heartbeat_cb is not None:
                self.heartbeat_cb()
            interval = 900
            try:
                targets = self.load_targets()
                interval = max(1, int(targets.billing.check_interval_seconds))
                await self.evaluate_and_alert(targets=targets)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("billing_monitor.iteration_failed")
            await asyncio.sleep(interval)
