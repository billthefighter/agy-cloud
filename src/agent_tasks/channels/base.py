from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any, Protocol

from agent_tasks.common.models import TriageReport
from agent_tasks.common.state import IssueRecord

ApproveCallback = Callable[[str, str], Awaitable[None]]
PushBranchCallback = Callable[[str, str], Awaitable[None]]
ResolveCallback = Callable[[str, str], Awaitable[None]]
IgnoreCallback = Callable[[str, str], Awaitable[None]]
SaveMemoryCallback = Callable[[str, str], Awaitable[None]]
FeedbackCallback = Callable[[str, str, str], Awaitable[None]]
SnoozeCallback = Callable[[str, str, str], Awaitable[str]]
TimersCommandCallback = Callable[[str], Awaitable[str]]
MemoryCommandCallback = Callable[[str, str, str], Awaitable[str]]
StatusCallback = Callable[[], Awaitable[str]]
DepsCallback = Callable[[str, str, str], Awaitable[str]]
CodingCallback = Callable[[str, str, str, str], Awaitable[str]]
BillingCommandCallback = Callable[[str], Awaitable[str]]
BindChannelCallback = Callable[[str, str], Awaitable[str]]
ChannelResolver = Callable[[str], str]
FoodMessageCallback = Callable[[str, str, str, str, list[dict[str, Any]], bool], Awaitable[bool]]
FoodThreadReplyCallback = Callable[[str, str, str, str, str, list[dict[str, Any]]], Awaitable[bool]]
FoodScaleCallback = Callable[[int, float, str], Awaitable[None]]
FoodEntryActionCallback = Callable[[int, str], Awaitable[None]]


class ChannelAdapter(Protocol):
    """Transport-agnostic mobile messaging bridge.

    Slack Socket Mode is implemented first; Signal (signal-cli-rest-api) or Telegram
    can implement the exact same protocol without touching task orchestration logic.
    """

    async def start(self) -> None: ...

    async def stop(self) -> None: ...

    def resolve_channel(self, role: str) -> str:
        """Resolve target channel identifier for a named role (`sentry`, `billing`, ...)."""
        ...

    def resolve_channel_for_task(self, task_kind: str) -> str:
        """Resolve target channel identifier for a `task_kind`."""
        ...

    async def post_channel_message(
        self,
        text: str,
        *,
        role: str = "",
        channel_id: str | None = None,
    ) -> str:
        """Post a top-level operational/keepalive/alert message; returns message ts."""
        ...

    async def post_triage_result(
        self,
        record: IssueRecord,
        report: TriageReport,
        diff_text: str,
        *,
        model_used: str = "",
        cost_line: str = "",
    ) -> str:
        """Post or update a triage thread; returns the canonical thread identifier."""
        ...

    async def post_thread_message(
        self,
        thread_ts: str,
        text: str,
        *,
        channel_id: str | None = None,
    ) -> None:
        """Post a plain status/error/PR follow-up inside an existing thread."""
        ...


class FoodChannelAdapter(ChannelAdapter, Protocol):
    """Extended channel interface supporting Block Kit updates, reactions, and role checks."""

    def is_explicit_role_channel(self, role: str, channel_id: str) -> bool:
        """Return True only when `channel_id` is explicitly bound to `role`."""
        ...

    async def post_blocks_message(
        self,
        *,
        channel_id: str,
        text: str,
        blocks: list[dict[str, Any]],
        thread_ts: str | None = None,
    ) -> str:
        """Post a Block Kit message and return its Slack message timestamp (`ts`)."""
        ...

    async def update_blocks_message(
        self,
        *,
        channel_id: str,
        ts: str,
        text: str,
        blocks: list[dict[str, Any]],
    ) -> None:
        """Edit an existing Block Kit message in place via `chat.update`."""
        ...

    async def add_reaction(self, *, channel_id: str, ts: str, name: str) -> None:
        """Best-effort emoji reaction add on a Slack message."""
        ...

    async def remove_reaction(self, *, channel_id: str, ts: str, name: str) -> None:
        """Best-effort emoji reaction remove on a Slack message."""
        ...
