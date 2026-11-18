from __future__ import annotations

import calendar
from dataclasses import dataclass
from datetime import UTC, datetime

from agent_tasks.common.config import BillingConfig, ModelRateConfig
from agent_tasks.common.models import TokenUsageSummary

_FALLBACK_RATE = ModelRateConfig(
    input_usd_per_1m=1.50,
    cached_input_usd_per_1m=0.15,
    output_usd_per_1m=7.50,
)


@dataclass(frozen=True, slots=True)
class BillingCycleWindow:
    """Monthly billing cycle window aligned to a configured UTC anchor day."""

    cycle_key: str
    start_utc: datetime
    end_utc: datetime
    elapsed_days: float
    total_days: float
    remaining_days: float


def _month_anchor_utc(year: int, month: int, anchor_day: int) -> datetime:
    max_day = calendar.monthrange(year, month)[1]
    clamped_day = min(max(1, anchor_day), max_day)
    return datetime(year, month, clamped_day, 0, 0, 0, tzinfo=UTC)


def compute_billing_cycle(
    now: datetime | None = None,
    anchor_day: int = 15,
) -> BillingCycleWindow:
    """Resolve the active monthly billing cycle window `[start_utc, end_utc)` in UTC."""
    if now is None:
        now_utc = datetime.now(UTC)
    elif now.tzinfo is None:
        now_utc = now.replace(tzinfo=UTC)
    else:
        now_utc = now.astimezone(UTC)

    current_anchor = _month_anchor_utc(now_utc.year, now_utc.month, anchor_day)
    if now_utc >= current_anchor:
        start_utc = current_anchor
        next_year, next_month = (
            (now_utc.year + 1, 1) if now_utc.month == 12 else (now_utc.year, now_utc.month + 1)
        )
        end_utc = _month_anchor_utc(next_year, next_month, anchor_day)
    else:
        prev_year, prev_month = (
            (now_utc.year - 1, 12) if now_utc.month == 1 else (now_utc.year, now_utc.month - 1)
        )
        start_utc = _month_anchor_utc(prev_year, prev_month, anchor_day)
        end_utc = current_anchor

    elapsed_days = max(0.0, (now_utc - start_utc).total_seconds() / 86400.0)
    total_days = max(1.0, (end_utc - start_utc).total_seconds() / 86400.0)
    remaining_days = max(0.0, (end_utc - now_utc).total_seconds() / 86400.0)

    return BillingCycleWindow(
        cycle_key=start_utc.strftime("%Y-%m-%d"),
        start_utc=start_utc,
        end_utc=end_utc,
        elapsed_days=elapsed_days,
        total_days=total_days,
        remaining_days=remaining_days,
    )


def compute_incremental_cost(
    *,
    model: str,
    cumulative_usage: TokenUsageSummary,
    prior_tokens: tuple[int, int, int, int],
    billing_cfg: BillingConfig,
) -> tuple[TokenUsageSummary, float]:
    """Compute incremental token deltas and estimated USD cost for a worker turn.

    `cumulative_usage` reports the session-cumulative totals from the AGY SDK, while
    `prior_tokens` holds `(prompt, cached, output, thoughts)` already recorded for
    the same `conversation_id` so multi-turn follow-ups never double-count Turn 1.
    """
    delta_prompt = max(0, cumulative_usage.prompt_tokens - prior_tokens[0])
    delta_cached = max(0, cumulative_usage.cached_tokens - prior_tokens[1])
    delta_output = max(0, cumulative_usage.output_tokens - prior_tokens[2])
    delta_thoughts = max(0, cumulative_usage.thoughts_tokens - prior_tokens[3])
    delta_total = delta_prompt + delta_output + delta_thoughts

    delta_cached_clamped = min(delta_cached, delta_prompt)
    uncached_input = max(0, delta_prompt - delta_cached_clamped)
    billable_output = delta_output + delta_thoughts

    rate = (
        billing_cfg.model_rates.get(model)
        or billing_cfg.model_rates.get("gemini-4-hydrogen")
        or _FALLBACK_RATE
    )
    cost_usd = (
        uncached_input * rate.input_usd_per_1m
        + delta_cached_clamped * rate.cached_input_usd_per_1m
        + billable_output * rate.output_usd_per_1m
    ) / 1_000_000.0

    delta_summary = TokenUsageSummary(
        prompt_tokens=delta_prompt,
        cached_tokens=delta_cached_clamped,
        output_tokens=delta_output,
        thoughts_tokens=delta_thoughts,
        total_tokens=delta_total,
    )
    return delta_summary, cost_usd
