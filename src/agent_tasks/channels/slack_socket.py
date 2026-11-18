from __future__ import annotations

import re
from collections import deque
from typing import Any

import structlog
from slack_bolt.adapter.socket_mode.async_handler import AsyncSocketModeHandler
from slack_bolt.async_app import AsyncApp
from slack_sdk.errors import SlackApiError

from agent_tasks.channels.base import (
    ApproveCallback,
    BillingCommandCallback,
    BindChannelCallback,
    ChannelResolver,
    CodingCallback,
    DepsCallback,
    FeedbackCallback,
    FoodEntryActionCallback,
    FoodMessageCallback,
    FoodScaleCallback,
    FoodThreadReplyCallback,
    IgnoreCallback,
    MemoryCommandCallback,
    PushBranchCallback,
    ResolveCallback,
    SaveMemoryCallback,
    SnoozeCallback,
    StatusCallback,
    TimersCommandCallback,
)
from agent_tasks.common.models import TriageReport
from agent_tasks.common.state import IssueRecord

log = structlog.get_logger(__name__)

MAX_INLINE_DIFF_CHARS = 2400
STATUS_KEYWORDS = frozenset({"ping", "!ping", "status", "!status", "keepalive", "!keepalive"})
HELP_KEYWORDS = frozenset({"help", "!help", "commands", "!commands"})
K8S_KEYWORDS = frozenset({"k8s", "!k8s", "cluster", "!cluster"})
CI_KEYWORDS = frozenset({"ci", "!ci"})
CHANNELS_KEYWORDS = frozenset({"channels", "!channels"})
VALID_CHANNEL_ROLES = ("sentry", "ci", "deps", "k8s", "coding", "billing", "food")
TASK_KIND_TO_ROLE = {
    "sentry": "sentry",
    "ci_healer": "ci",
    "dep_upgrader": "deps",
    "k8s_watchdog": "k8s",
    "slack_coding": "coding",
    "food_tracker": "food",
}

_MENTION_PREFIX_RE = re.compile(r"<@[A-Z0-9]+(?:\|[^>]+)?>")

HELP_TEXT = (
    "*`agent-tasks` Slack Commands*\n"
    "• `!ping` / `!status` — live pod, Sentry RTT, model default, `$100/mo` credit summary, and SQLite counts\n"
    "• `!k8s` — instant `makesense` pod & Argo CD health snapshot\n"
    "• `!ci` — check recent GitHub Actions failures and queue auto-fixes\n"
    "• `!deps [python|web]` — trigger an on-demand dependency & security upgrade PR\n"
    "• `!billing` / `!billing sync <usd>` / `!billing pause` / `!billing resume` / `!billing test-alert`\n"
    "• `!bind <sentry|ci|deps|k8s|coding|billing|food|all>` & `!channels` — route automation alerts to this channel\n"
    "• `@agytasks <prompt>` or `!code <prompt>` — spawn a `gemini-4-hydrogen` coding Job on `lodbrok-app/rattlsnek`\n"
    "• `!timers` / `!timers cancel <id>` & `!snooze <30m|2h|1d> [note]` (inside task thread) — manage follow-up wakeups\n"
    "• `!memory` / `!remember [repo:<slug>|suppress] <note>` / `!forget <mem-id>` — persistent cross-run memory\n"
    "• *Food Tracker (`!bind food` in `#agy-food`):* drop a meal photo/description directly, or use `!eat`, `!macros`, `!week`, `!goal`, `!staples`, `!weight`, `!suggest`\n"
    "• *Model overrides:* include `[carbon]`, `[argon]`, `[flash]`, or `[hydrogen]` in any request or thread reply\n"
    "• *Follow-ups:* reply inside any task thread to request revisions (`retry [carbon]`, etc.)"
)


class SlackSocketAdapter:
    """Outbound-only Slack Socket Mode adapter with multi-channel routing & Block Kit approvals."""

    def __init__(
        self,
        *,
        bot_token: str,
        app_token: str,
        channel_id: str,
        on_open_draft_pr: ApproveCallback,
        on_push_to_pr_branch: PushBranchCallback | None = None,
        on_resolve: ResolveCallback,
        on_ignore: IgnoreCallback,
        on_thread_feedback: FeedbackCallback,
        on_status_query: StatusCallback | None = None,
        on_k8s_query: StatusCallback | None = None,
        on_ci_query: StatusCallback | None = None,
        on_deps_trigger: DepsCallback | None = None,
        on_coding_request: CodingCallback | None = None,
        on_billing_command: BillingCommandCallback | None = None,
        on_bind_channel: BindChannelCallback | None = None,
        on_channels_query: StatusCallback | None = None,
        channel_resolver: ChannelResolver | None = None,
        on_food_message: FoodMessageCallback | None = None,
        on_food_thread_reply: FoodThreadReplyCallback | None = None,
        on_food_scale: FoodScaleCallback | None = None,
        on_food_save_staple: FoodEntryActionCallback | None = None,
        on_food_delete: FoodEntryActionCallback | None = None,
        on_save_memory_note: SaveMemoryCallback | None = None,
        on_snooze: SnoozeCallback | None = None,
        on_timers_command: TimersCommandCallback | None = None,
        on_memory_command: MemoryCommandCallback | None = None,
    ) -> None:
        self.channel_id = channel_id
        self._on_open_draft_pr = on_open_draft_pr
        self._on_push_to_pr_branch = on_push_to_pr_branch
        self._on_resolve = on_resolve
        self._on_ignore = on_ignore
        self._on_thread_feedback = on_thread_feedback
        self._on_status_query = on_status_query
        self._on_k8s_query = on_k8s_query
        self._on_ci_query = on_ci_query
        self._on_deps_trigger = on_deps_trigger
        self._on_coding_request = on_coding_request
        self._on_billing_command = on_billing_command
        self._on_bind_channel = on_bind_channel
        self._on_channels_query = on_channels_query
        self._channel_resolver = channel_resolver
        self._on_food_message = on_food_message
        self._on_food_thread_reply = on_food_thread_reply
        self._on_food_scale = on_food_scale
        self._on_food_save_staple = on_food_save_staple
        self._on_food_delete = on_food_delete
        self._on_save_memory_note = on_save_memory_note
        self._on_snooze = on_snooze
        self._on_timers_command = on_timers_command
        self._on_memory_command = on_memory_command

        self._seen_ts_order: deque[str] = deque()
        self._seen_ts_set: set[str] = set()

        self.app = AsyncApp(token=bot_token)
        self._handler = AsyncSocketModeHandler(self.app, app_token)
        self._register_handlers()

    def is_explicit_role_channel(self, role: str, channel_id: str) -> bool:
        """Return True only when `channel_id` is explicitly bound to `role`."""
        if self._channel_resolver is None or not role or not channel_id:
            return False
        return self._channel_resolver(role).strip() == channel_id

    def resolve_channel(self, role: str) -> str:
        """Resolve Slack channel ID for a named role, falling back to default `channel_id`."""
        if self._channel_resolver is not None and role:
            resolved = self._channel_resolver(role).strip()
            if resolved:
                return resolved
        return self.channel_id

    def resolve_channel_for_task(self, task_kind: str) -> str:
        """Resolve Slack channel ID for a task_kind (with `deps` -> `ci` -> default fallback)."""
        role = TASK_KIND_TO_ROLE.get(task_kind, "")
        if role == "deps" and self._channel_resolver is not None:
            deps_ch = self._channel_resolver("deps").strip()
            if deps_ch:
                return deps_ch
            ci_ch = self._channel_resolver("ci").strip()
            if ci_ch:
                return ci_ch
        if role:
            return self.resolve_channel(role)
        return self.channel_id

    def _mark_event_seen(self, ts: str | None) -> bool:
        """Return True on the first sighting of a Slack event timestamp, False on duplicate delivery."""
        if not ts:
            return True
        if ts in self._seen_ts_set:
            return False
        if len(self._seen_ts_order) >= 512:
            evicted = self._seen_ts_order.popleft()
            self._seen_ts_set.discard(evicted)
        self._seen_ts_order.append(ts)
        self._seen_ts_set.add(ts)
        return True

    @staticmethod
    def strip_bot_mentions(text: str) -> str:
        return " ".join(_MENTION_PREFIX_RE.sub(" ", text).split()).strip()

    async def _reply(self, channel: str, thread_ts: str | None, text: str) -> None:
        await self.app.client.chat_postMessage(
            channel=channel,
            thread_ts=thread_ts,
            text=text,
            unfurl_links=False,
        )

    async def _dispatch_command_or_coding(
        self,
        *,
        cleaned_text: str,
        user: str,
        channel: str,
        event_ts: str,
        event_thread_ts: str | None,
        from_mention: bool,
        image_files: list[dict[str, Any]] | None = None,
    ) -> bool:
        """Handle built-in `!`/mention commands, food tracker logs, and coding prompts."""
        lower = cleaned_text.lower()
        reply_ts = event_thread_ts or event_ts
        attached_images = image_files or []

        # Inside an existing thread, require explicit `!` prefix or direct `@agytasks <kw>`
        # so natural review comments like "check status before returning" still go to the agent.
        allow_bare = (not event_thread_ts) or from_mention

        if lower in STATUS_KEYWORDS and (lower.startswith("!") or allow_bare):
            if self._on_status_query is not None:
                await self._reply(channel, reply_ts, await self._on_status_query())
            return True

        if lower in HELP_KEYWORDS and (lower.startswith("!") or allow_bare):
            await self._reply(channel, reply_ts, HELP_TEXT)
            return True

        if lower in K8S_KEYWORDS and (lower.startswith("!") or allow_bare):
            text = (
                await self._on_k8s_query()
                if self._on_k8s_query is not None
                else "K8s watchdog is not enabled."
            )
            await self._reply(channel, reply_ts, text)
            return True

        if lower in CI_KEYWORDS and (lower.startswith("!") or allow_bare):
            text = (
                await self._on_ci_query()
                if self._on_ci_query is not None
                else "CI healer is not enabled."
            )
            await self._reply(channel, reply_ts, text)
            return True

        if lower in CHANNELS_KEYWORDS and (lower.startswith("!") or allow_bare):
            text = (
                await self._on_channels_query()
                if self._on_channels_query is not None
                else "Channel routing diagnostics are not configured."
            )
            await self._reply(channel, reply_ts, text)
            return True

        billing_tail: str | None = None
        for kw in ("!billing", "!budget", "!cost", "billing", "budget", "cost"):
            if lower == kw and (kw.startswith("!") or allow_bare):
                billing_tail = ""
                break
            if lower.startswith(f"{kw} ") and (kw.startswith("!") or allow_bare):
                billing_tail = cleaned_text[len(kw) :].strip()
                break
        if billing_tail is not None:
            text = (
                await self._on_billing_command(billing_tail)
                if self._on_billing_command is not None
                else "Billing guard is not enabled."
            )
            await self._reply(channel, reply_ts, text)
            return True

        bind_arg: str | None = None
        if lower == "!bind":
            bind_arg = ""
        elif lower.startswith("!bind "):
            bind_arg = cleaned_text[5:].strip()
        elif allow_bare and lower.startswith("bind "):
            bind_arg = cleaned_text[4:].strip()
        if bind_arg is not None:
            if self._on_bind_channel is not None:
                ack = await self._on_bind_channel(bind_arg, channel)
            else:
                ack = "Channel binding is not configured."
            await self._reply(channel, reply_ts, ack)
            return True

        timers_tail: str | None = None
        if lower in {"!timers", "timers"} and (lower.startswith("!") or allow_bare):
            timers_tail = ""
        elif lower.startswith("!timers "):
            timers_tail = cleaned_text[7:].strip()
        elif allow_bare and lower.startswith("timers "):
            timers_tail = cleaned_text[6:].strip()
        if timers_tail is not None:
            text = (
                await self._on_timers_command(timers_tail)
                if self._on_timers_command is not None
                else "Follow-up timers are not enabled."
            )
            await self._reply(channel, reply_ts, text)
            return True

        if lower == "!snooze" or lower.startswith("!snooze "):
            snooze_tail = cleaned_text[7:].strip()
            if not event_thread_ts:
                await self._reply(
                    channel,
                    reply_ts,
                    "Use `!snooze <30m|2h|1d> [note]` inside a task thread, or `!timers` to inspect scheduled timers.",
                )
                return True
            if self._on_snooze is not None:
                ack = await self._on_snooze(event_thread_ts, snooze_tail, user)
            else:
                ack = "Snooze timers are not configured."
            await self._reply(channel, reply_ts, ack)
            return True

        mem_action: str | None = None
        mem_tail = ""
        if lower in {"!memory", "memory"} and (lower.startswith("!") or allow_bare):
            mem_action = "show"
        elif lower == "!remember" or lower.startswith("!remember "):
            mem_action = "remember"
            mem_tail = cleaned_text[9:].strip()
        elif allow_bare and (lower == "remember" or lower.startswith("remember ")):
            mem_action = "remember"
            mem_tail = cleaned_text[8:].strip()
        elif lower == "!forget" or lower.startswith("!forget "):
            mem_action = "forget"
            mem_tail = cleaned_text[7:].strip()
        elif allow_bare and (lower == "forget" or lower.startswith("forget ")):
            mem_action = "forget"
            mem_tail = cleaned_text[6:].strip()
        if mem_action is not None:
            text = (
                await self._on_memory_command(mem_action, mem_tail, user)
                if self._on_memory_command is not None
                else "Persistent memory is not configured."
            )
            await self._reply(channel, reply_ts, text)
            return True

        deps_tail: str | None = None
        if lower in {"!deps", "deps", "upgrade deps"} and (lower.startswith("!") or allow_bare):
            deps_tail = ""
        elif lower.startswith("!deps "):
            deps_tail = cleaned_text[5:].strip()
        elif allow_bare and lower.startswith("deps "):
            deps_tail = cleaned_text[4:].strip()
        elif allow_bare and lower.startswith("upgrade deps "):
            deps_tail = cleaned_text[len("upgrade deps") :].strip()

        if deps_tail is not None:
            if self._on_deps_trigger is not None:
                ack = await self._on_deps_trigger(deps_tail, reply_ts, channel)
            else:
                ack = "Dependency upgrader is not enabled."
            await self._reply(channel, reply_ts, ack)
            return True

        if not event_thread_ts and (lower == "!code" or lower.startswith("!code ")):
            coding_prompt = cleaned_text[5:].strip()
            if not coding_prompt:
                await self._reply(channel, reply_ts, HELP_TEXT)
                return True
            if self._on_coding_request is not None:
                ack = await self._on_coding_request(coding_prompt, user, reply_ts, channel)
            else:
                ack = "On-demand Slack coding is not enabled."
            await self._reply(channel, reply_ts, ack)
            return True

        if self._on_food_message is not None and (
            not event_thread_ts or (lower.startswith("!") and not attached_images)
        ):
            is_food_channel = (not event_thread_ts) and self.is_explicit_role_channel(
                "food", channel
            )
            if await self._on_food_message(
                cleaned_text,
                user,
                channel,
                event_ts,
                attached_images,
                is_food_channel,
            ):
                return True

        if not event_thread_ts and from_mention:
            coding_prompt = cleaned_text
            if not coding_prompt:
                await self._reply(channel, reply_ts, HELP_TEXT)
                return True
            if self._on_coding_request is not None:
                ack = await self._on_coding_request(coding_prompt, user, reply_ts, channel)
            else:
                ack = "On-demand Slack coding is not enabled."
            await self._reply(channel, reply_ts, ack)
            return True

        return False

    async def process_incoming_message(self, event: dict, *, from_mention: bool) -> None:
        """Process a raw Slack `app_mention` or `message` payload across all invited channels."""
        if event.get("bot_id"):
            return
        subtype = str(event.get("subtype") or "")
        if subtype not in ("", "file_share"):
            return
        if not self._mark_event_seen(event.get("ts")):
            return

        channel = str(event.get("channel") or self.channel_id)
        raw_text = (event.get("text") or "").strip()
        cleaned = self.strip_bot_mentions(raw_text)
        user = str(event.get("user") or "user")
        event_ts = str(event.get("ts") or "")
        event_thread_ts = event.get("thread_ts")
        effective_channel = channel or self.channel_id
        image_files = [
            f
            for f in (event.get("files") or [])
            if isinstance(f, dict) and str(f.get("mimetype") or "").lower().startswith("image/")
        ]

        if not cleaned and not image_files:
            if from_mention:
                await self._reply(effective_channel, event_thread_ts or event_ts, HELP_TEXT)
            return

        if await self._dispatch_command_or_coding(
            cleaned_text=cleaned,
            user=user,
            channel=effective_channel,
            event_ts=event_ts,
            event_thread_ts=event_thread_ts,
            from_mention=from_mention,
            image_files=image_files,
        ):
            return

        if event_thread_ts:
            if self._on_food_thread_reply is not None and await self._on_food_thread_reply(
                str(event_thread_ts),
                cleaned,
                user,
                effective_channel,
                event_ts,
                image_files,
            ):
                return
            if cleaned:
                log.info("Received thread feedback in %s from %s", event_thread_ts, user)
                await self._on_thread_feedback(str(event_thread_ts), cleaned, user)

    def _register_handlers(self) -> None:
        @self.app.action("open_draft_pr")
        async def handle_open_pr(ack, body, logger):  # type: ignore[no-untyped-def]
            await ack()
            issue_id = body["actions"][0]["value"]
            user = body.get("user", {}).get("username") or body.get("user", {}).get("id", "user")
            logger.info("Slack Open Draft PR clicked for issue %s by %s", issue_id, user)
            await self._on_open_draft_pr(issue_id, user)

        @self.app.action("push_to_pr_branch")
        async def handle_push_pr_branch(ack, body, logger):  # type: ignore[no-untyped-def]
            await ack()
            if self._on_push_to_pr_branch is None:
                return
            issue_id = body["actions"][0]["value"]
            user = body.get("user", {}).get("username") or body.get("user", {}).get("id", "user")
            logger.info("Slack Push Fix to PR Branch clicked for issue %s by %s", issue_id, user)
            await self._on_push_to_pr_branch(issue_id, user)

        @self.app.action("resolve_issue")
        async def handle_resolve(ack, body, logger):  # type: ignore[no-untyped-def]
            await ack()
            issue_id = body["actions"][0]["value"]
            user = body.get("user", {}).get("username") or body.get("user", {}).get("id", "user")
            logger.info("Slack Resolve clicked for issue %s by %s", issue_id, user)
            await self._on_resolve(issue_id, user)

        @self.app.action("ignore_issue")
        async def handle_ignore(ack, body, logger):  # type: ignore[no-untyped-def]
            await ack()
            issue_id = body["actions"][0]["value"]
            user = body.get("user", {}).get("username") or body.get("user", {}).get("id", "user")
            logger.info("Slack Ignore clicked for issue %s by %s", issue_id, user)
            await self._on_ignore(issue_id, user)

        @self.app.action("save_memory_note")
        async def handle_save_memory(ack, body, logger):  # type: ignore[no-untyped-def]
            await ack()
            if self._on_save_memory_note is None:
                return
            issue_id = body["actions"][0]["value"]
            user = body.get("user", {}).get("username") or body.get("user", {}).get("id", "user")
            logger.info("Slack Save Memory clicked for issue %s by %s", issue_id, user)
            await self._on_save_memory_note(issue_id, user)

        @self.app.action(re.compile(r"^food_scale_"))
        async def handle_food_scale(ack, body, logger):  # type: ignore[no-untyped-def]
            await ack()
            if self._on_food_scale is None:
                return
            raw_val = str(body["actions"][0]["value"])
            entry_s, mult_s = raw_val.split(":", 1)
            user = body.get("user", {}).get("username") or body.get("user", {}).get("id", "user")
            logger.info("Slack Food Scale %sx clicked for meal %s by %s", mult_s, entry_s, user)
            await self._on_food_scale(int(entry_s), float(mult_s), user)

        @self.app.action("food_save_staple")
        async def handle_food_save_staple(ack, body, logger):  # type: ignore[no-untyped-def]
            await ack()
            if self._on_food_save_staple is None:
                return
            entry_id = int(body["actions"][0]["value"])
            user = body.get("user", {}).get("username") or body.get("user", {}).get("id", "user")
            logger.info("Slack Food Save Staple clicked for meal %s by %s", entry_id, user)
            await self._on_food_save_staple(entry_id, user)

        @self.app.action("food_delete")
        async def handle_food_delete(ack, body, logger):  # type: ignore[no-untyped-def]
            await ack()
            if self._on_food_delete is None:
                return
            entry_id = int(body["actions"][0]["value"])
            user = body.get("user", {}).get("username") or body.get("user", {}).get("id", "user")
            logger.info("Slack Food Delete clicked for meal %s by %s", entry_id, user)
            await self._on_food_delete(entry_id, user)

        @self.app.event("app_mention")
        async def handle_app_mention(event, logger):  # type: ignore[no-untyped-def]
            logger.info("Received app_mention in %s", event.get("channel"))
            await self.process_incoming_message(event, from_mention=True)

        @self.app.event("message")
        async def handle_message_events(event, logger):  # type: ignore[no-untyped-def]
            await self.process_incoming_message(event, from_mention=False)

    async def start(self) -> None:
        log.info("slack_socket.starting", channel_id=self.channel_id)
        await self._handler.connect_async()

    async def stop(self) -> None:
        log.info("slack_socket.stopping")
        await self._handler.close_async()

    async def post_channel_message(
        self,
        text: str,
        *,
        role: str = "",
        channel_id: str | None = None,
    ) -> str:
        target_channel = channel_id or (self.resolve_channel(role) if role else self.channel_id)
        resp = await self.app.client.chat_postMessage(
            channel=target_channel,
            text=text,
            unfurl_links=False,
        )
        return str(resp["ts"])

    async def post_triage_result(
        self,
        record: IssueRecord,
        report: TriageReport,
        diff_text: str,
        *,
        model_used: str = "",
        cost_line: str = "",
    ) -> str:
        tests_badge = "PASSED" if report.tests_passed else "FAILED / UNVERIFIED"
        model_label = model_used or record.model_override or "gemini-4-hydrogen"
        title_ref = (
            f"*<{record.permalink}|{record.short_id}>*"
            if record.permalink
            else f"*{record.short_id}*"
        )
        header_line = (
            f"{title_ref} — `{record.project_slug}` "
            f"(`{record.task_kind}`, turn {record.turn}, `{model_label}`) | "
            f"Confidence: `{report.confidence}` | Tests: *{tests_badge}*"
        )
        files_str = ", ".join(f"`{f}`" for f in report.affected_files) or "_none_"
        summary_heading = "Root Cause" if record.task_kind == "sentry" else "Analysis"
        fix_heading = "Proposed Fix" if diff_text.strip() else "Summary"
        body_md = (
            f"{header_line}\n"
            f"*{record.title}*\n\n"
            f"*{summary_heading}:*\n{report.root_cause}\n\n"
            f"*{fix_heading} ({files_str}):*\n{report.fix_summary}"
        )
        if report.risk_notes:
            body_md += f"\n\n*Risk / Notes:* {report.risk_notes}"
        if report.proposed_memory_note and report.proposed_memory_note.strip():
            body_md += f"\n\n🧠 *Proposed Repo Memory:* _{report.proposed_memory_note.strip()}_"

        blocks: list[dict] = []
        if record.banner_note and record.banner_note.strip():
            blocks.append(
                {
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": record.banner_note.strip()[:2950]},
                }
            )
        blocks.append(
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": body_md[:2950]},
            }
        )

        if diff_text.strip():
            preview = diff_text[:MAX_INLINE_DIFF_CHARS]
            truncated_note = (
                "\n… _(truncated; full patch attached in thread)_"
                if len(diff_text) > MAX_INLINE_DIFF_CHARS
                else ""
            )
            blocks.append(
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"```diff\n{preview}\n```{truncated_note}",
                    },
                }
            )

        action_elements: list[dict] = []
        has_diff = bool(diff_text.strip())
        if has_diff and record.target_branch and self._on_push_to_pr_branch is not None:
            action_elements.append(
                {
                    "type": "button",
                    "text": {
                        "type": "plain_text",
                        "text": f"Push Fix to {record.target_branch}"[:75],
                    },
                    "style": "primary",
                    "action_id": "push_to_pr_branch",
                    "value": record.issue_id,
                }
            )
        if has_diff:
            pr_btn: dict[str, object] = {
                "type": "button",
                "text": {"type": "plain_text", "text": "Open Draft PR"},
                "action_id": "open_draft_pr",
                "value": record.issue_id,
            }
            if not record.target_branch:
                pr_btn["style"] = "primary"
            action_elements.append(pr_btn)

        if record.task_kind == "sentry":
            resolve_btn: dict[str, object] = {
                "type": "button",
                "text": {"type": "plain_text", "text": "Resolve in Sentry"},
                "action_id": "resolve_issue",
                "value": record.issue_id,
            }
            if not has_diff:
                resolve_btn["style"] = "primary"
            action_elements.append(resolve_btn)
            action_elements.append(
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "Ignore in Sentry"},
                    "style": "danger",
                    "action_id": "ignore_issue",
                    "value": record.issue_id,
                }
            )

        if (
            report.proposed_memory_note
            and report.proposed_memory_note.strip()
            and self._on_save_memory_note is not None
        ):
            action_elements.append(
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "Save Repo Memory"},
                    "action_id": "save_memory_note",
                    "value": record.issue_id,
                }
            )

        if action_elements:
            blocks.append({"type": "actions", "elements": action_elements})

        blocks.append(
            {
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": (
                            f"{cost_line}"
                            "Reply in this thread to request changes (`[carbon]`/`[argon]`/`[flash]` "
                            "to switch models) and re-run verification."
                        ),
                    }
                ],
            }
        )

        target_channel = record.channel_id or self.resolve_channel_for_task(record.task_kind)
        resp = await self.app.client.chat_postMessage(
            channel=target_channel,
            thread_ts=record.thread_ts,
            text=f"{record.short_id} ({record.task_kind}): {record.title}",
            blocks=blocks,
            unfurl_links=False,
        )
        thread_ts = record.thread_ts or str(resp["ts"])

        if len(diff_text) > MAX_INLINE_DIFF_CHARS:
            await self.app.client.files_upload_v2(
                channel=target_channel,
                thread_ts=thread_ts,
                filename=f"{record.short_id.lower()}-turn-{record.turn}.patch",
                title=f"Full patch for {record.short_id} (turn {record.turn})",
                content=diff_text,
            )

        return thread_ts

    async def post_thread_message(
        self,
        thread_ts: str,
        text: str,
        *,
        channel_id: str | None = None,
    ) -> None:
        await self.app.client.chat_postMessage(
            channel=channel_id or self.channel_id,
            thread_ts=thread_ts,
            text=text,
            unfurl_links=False,
        )

    async def post_blocks_message(
        self,
        *,
        channel_id: str,
        text: str,
        blocks: list[dict[str, Any]],
        thread_ts: str | None = None,
    ) -> str:
        resp = await self.app.client.chat_postMessage(
            channel=channel_id,
            thread_ts=thread_ts,
            text=text,
            blocks=blocks,
            unfurl_links=False,
        )
        return str(resp["ts"])

    async def update_blocks_message(
        self,
        *,
        channel_id: str,
        ts: str,
        text: str,
        blocks: list[dict[str, Any]],
    ) -> None:
        await self.app.client.chat_update(
            channel=channel_id,
            ts=ts,
            text=text,
            blocks=blocks,
        )

    async def add_reaction(self, *, channel_id: str, ts: str, name: str) -> None:
        try:
            await self.app.client.reactions_add(channel=channel_id, timestamp=ts, name=name)
        except (SlackApiError, OSError):
            log.debug("slack_socket.add_reaction_failed", channel=channel_id, ts=ts, name=name)

    async def remove_reaction(self, *, channel_id: str, ts: str, name: str) -> None:
        try:
            await self.app.client.reactions_remove(channel=channel_id, timestamp=ts, name=name)
        except (SlackApiError, OSError):
            log.debug("slack_socket.remove_reaction_failed", channel=channel_id, ts=ts, name=name)
