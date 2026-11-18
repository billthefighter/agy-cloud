from __future__ import annotations

import secrets
import time
from typing import TYPE_CHECKING, Any

import structlog

from agent_tasks.channels.base import FoodChannelAdapter
from agent_tasks.common.models import TriageReport
from agent_tasks.common.state import IssueRecord

if TYPE_CHECKING:
    from agent_tasks.pwa.gateway import PwaGatewayAdapter

log = structlog.get_logger(__name__)


def _synthetic_ts(prefix: str = "pwa") -> str:
    return f"{prefix}-{int(time.time() * 1000)}-{secrets.token_hex(3)}"


class CompositeChannelAdapter:
    """Dual-writes notifications across Slack Socket Mode and the mobile PWA gateway."""

    def __init__(
        self,
        *,
        pwa: PwaGatewayAdapter,
        slack: FoodChannelAdapter | None = None,
        mirror_pwa_to_slack: bool = True,
    ) -> None:
        self.pwa = pwa
        self.slack = slack
        self.mirror_pwa_to_slack = mirror_pwa_to_slack

    async def start(self) -> None:
        if self.slack is not None:
            try:
                await self.slack.start()
            except Exception as exc:  # noqa: BLE001
                log.warning("composite.slack_start_failed", error=str(exc))
        await self.pwa.start()

    async def stop(self) -> None:
        await self.pwa.stop()
        if self.slack is not None:
            try:
                await self.slack.stop()
            except Exception as exc:  # noqa: BLE001
                log.warning("composite.slack_stop_failed", error=str(exc))

    def is_explicit_role_channel(self, role: str, channel_id: str) -> bool:
        if self.slack is not None and not channel_id.startswith("pwa:"):
            return self.slack.is_explicit_role_channel(role, channel_id)
        return self.pwa.is_explicit_role_channel(role, channel_id)

    def resolve_channel(self, role: str) -> str:
        if self.slack is not None:
            return self.slack.resolve_channel(role)
        return self.pwa.resolve_channel(role)

    def resolve_channel_for_task(self, task_kind: str) -> str:
        if self.slack is not None:
            return self.slack.resolve_channel_for_task(task_kind)
        return self.pwa.resolve_channel_for_task(task_kind)

    @staticmethod
    def _should_send_slack(channel_id: str | None) -> bool:
        return not (channel_id and channel_id.startswith("pwa:"))

    async def post_channel_message(
        self,
        text: str,
        *,
        role: str = "",
        channel_id: str | None = None,
    ) -> str:
        ts = ""
        if self.slack is not None and self._should_send_slack(channel_id):
            try:
                ts = await self.slack.post_channel_message(text, role=role, channel_id=channel_id)
            except Exception as exc:  # noqa: BLE001
                log.warning("composite.slack_post_channel_failed", role=role, error=str(exc))

        pwa_ts = await self.pwa.post_channel_message(
            text,
            role=role,
            channel_id=channel_id,
            external_ts=ts or None,
        )
        return ts or pwa_ts

    async def post_triage_result(
        self,
        record: IssueRecord,
        report: TriageReport,
        diff_text: str,
        *,
        model_used: str = "",
        cost_line: str = "",
    ) -> str:
        slack_ts = ""
        if self.slack is not None and self._should_send_slack(record.channel_id):
            try:
                slack_ts = await self.slack.post_triage_result(
                    record,
                    report,
                    diff_text,
                    model_used=model_used,
                    cost_line=cost_line,
                )
            except Exception as exc:  # noqa: BLE001
                log.warning(
                    "composite.slack_post_triage_failed",
                    issue_id=record.issue_id,
                    error=str(exc),
                )

        canonical_ts = record.thread_ts or slack_ts or _synthetic_ts("triage")
        pwa_ts = await self.pwa.post_triage_result(
            record,
            report,
            diff_text,
            model_used=model_used,
            cost_line=cost_line,
            external_ts=canonical_ts,
        )
        return slack_ts or pwa_ts

    async def post_thread_message(
        self,
        thread_ts: str,
        text: str,
        *,
        channel_id: str | None = None,
    ) -> None:
        if (
            self.slack is not None
            and self._should_send_slack(channel_id)
            and not thread_ts.startswith("pwa:")
        ):
            try:
                await self.slack.post_thread_message(thread_ts, text, channel_id=channel_id)
            except Exception as exc:  # noqa: BLE001
                log.warning(
                    "composite.slack_post_thread_failed",
                    thread_ts=thread_ts,
                    error=str(exc),
                )
        await self.pwa.post_thread_message(thread_ts, text, channel_id=channel_id)

    async def post_blocks_message(
        self,
        *,
        channel_id: str,
        text: str,
        blocks: list[dict[str, Any]],
        thread_ts: str | None = None,
    ) -> str:
        slack_ts = ""
        if (
            self.slack is not None
            and self._should_send_slack(channel_id)
            and not (thread_ts or "").startswith("pwa:")
        ):
            try:
                slack_ts = await self.slack.post_blocks_message(
                    channel_id=channel_id,
                    text=text,
                    blocks=blocks,
                    thread_ts=thread_ts,
                )
            except Exception as exc:  # noqa: BLE001
                log.warning("composite.slack_post_blocks_failed", error=str(exc))

        pwa_ts = await self.pwa.post_blocks_message(
            channel_id=channel_id,
            text=text,
            blocks=blocks,
            thread_ts=thread_ts,
            external_ts=slack_ts or None,
        )
        return slack_ts or pwa_ts

    async def update_blocks_message(
        self,
        *,
        channel_id: str,
        ts: str,
        text: str,
        blocks: list[dict[str, Any]],
    ) -> None:
        if (
            self.slack is not None
            and self._should_send_slack(channel_id)
            and not ts.startswith("pwa")
        ):
            try:
                await self.slack.update_blocks_message(
                    channel_id=channel_id,
                    ts=ts,
                    text=text,
                    blocks=blocks,
                )
            except Exception as exc:  # noqa: BLE001
                log.warning("composite.slack_update_blocks_failed", ts=ts, error=str(exc))

        await self.pwa.update_blocks_message(
            channel_id=channel_id,
            ts=ts,
            text=text,
            blocks=blocks,
        )

    async def add_reaction(self, *, channel_id: str, ts: str, name: str) -> None:
        if (
            self.slack is not None
            and self._should_send_slack(channel_id)
            and not ts.startswith("pwa")
        ):
            try:
                await self.slack.add_reaction(channel_id=channel_id, ts=ts, name=name)
            except Exception as exc:  # noqa: BLE001
                log.warning("composite.slack_add_reaction_failed", name=name, error=str(exc))
        await self.pwa.add_reaction(channel_id=channel_id, ts=ts, name=name)

    async def remove_reaction(self, *, channel_id: str, ts: str, name: str) -> None:
        if (
            self.slack is not None
            and self._should_send_slack(channel_id)
            and not ts.startswith("pwa")
        ):
            try:
                await self.slack.remove_reaction(channel_id=channel_id, ts=ts, name=name)
            except Exception as exc:  # noqa: BLE001
                log.warning("composite.slack_remove_reaction_failed", name=name, error=str(exc))
        await self.pwa.remove_reaction(channel_id=channel_id, ts=ts, name=name)
