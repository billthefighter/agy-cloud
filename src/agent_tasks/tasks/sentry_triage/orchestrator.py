from __future__ import annotations

import asyncio
import json
import re
import socket
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING

import httpx
import pydantic
import structlog

from agent_tasks.channels.base import ChannelAdapter
from agent_tasks.common.config import (
    ControllerSettings,
    ProjectTargetConfig,
    TargetsConfig,
    parse_model_override,
    resolve_worker_backend,
)
from agent_tasks.common.dedup import (
    compute_sentry_dedup_key,
    parse_duration_minutes,
    parse_iso_utc,
)
from agent_tasks.common.memory import MemoryStore
from agent_tasks.common.models import (
    IssueStatus,
    SanitizedSentryBundle,
    TriageReport,
    WorkerEnvelope,
)
from agent_tasks.common.state import IssueRecord, StateStore
from agent_tasks.tasks.sentry_triage.git_ops import (
    create_draft_pull_request,
    push_patch_to_branch,
    sync_agy_config_repo,
    sync_bare_mirror,
)
from agent_tasks.tasks.sentry_triage.k8s_jobs import K8sJobRunner
from agent_tasks.tasks.sentry_triage.release_check import check_sentry_release_lag
from agent_tasks.tasks.sentry_triage.sentry_client import SentryClient

if TYPE_CHECKING:
    from agent_tasks.tasks.billing_guard import BillingMonitorTask

log = structlog.get_logger(__name__)


def resolve_target_config(
    record: IssueRecord, targets: TargetsConfig
) -> ProjectTargetConfig | None:
    """Resolve `ProjectTargetConfig` for any task_kind stored in `StateStore`."""
    if record.task_kind == "sentry":
        return targets.projects.get(record.project_slug)
    if record.task_kind == "slack_coding":
        sc = targets.slack_coding
        same_repo = record.github_repo == sc.github_repo
        return ProjectTargetConfig(
            github_repo=record.github_repo,
            default_branch=record.default_branch,
            focus_subdir=sc.focus_subdir if same_repo else "",
            verify_hint=sc.verify_hint if same_repo else "",
        )
    if record.task_kind == "ci_healer":
        ci_cfg = targets.ci_healer
        matched = next((r for r in ci_cfg.repos if r.github_repo == record.github_repo), None)
        return ProjectTargetConfig(
            github_repo=record.github_repo,
            default_branch=record.default_branch,
            focus_subdir="",
            verify_hint=matched.verify_hint
            if matched
            else "cd rattlsnek && uv run ruff check . && uv run ty check",
        )
    if record.task_kind == "k8s_watchdog":
        kw = targets.k8s_watchdog
        return ProjectTargetConfig(
            github_repo=record.github_repo,
            default_branch=record.default_branch,
            focus_subdir="",
            verify_hint=kw.verify_hint,
        )
    if record.task_kind == "dep_upgrader":
        du = targets.dep_upgrader
        return ProjectTargetConfig(
            github_repo=record.github_repo,
            default_branch=record.default_branch,
            focus_subdir="",
            verify_hint=du.verify_hint,
        )
    return targets.projects.get(record.project_slug)


MAX_AUTO_FOLLOWUPS = 3


class SentryTriageTask:
    """Long-lived Sentry polling, K8s worker Job orchestration, and mobile review loop."""

    name = "sentry-triage"

    def __init__(
        self,
        *,
        settings: ControllerSettings,
        state: StateStore,
        channel: ChannelAdapter,
        job_runner: K8sJobRunner,
        heartbeat_cb: Callable[[], None],
        billing_guard: BillingMonitorTask | None = None,
    ) -> None:
        self.settings = settings
        self.state = state
        self.channel = channel
        self.job_runner = job_runner
        self._heartbeat = heartbeat_cb
        self.billing_guard = billing_guard
        self.memory = MemoryStore(settings.state_dir / "memory")
        self._dispatch_lock = asyncio.Lock()
        self.started_at = datetime.now(UTC)
        self.poll_count = 0
        self.last_poll_at: datetime | None = None
        self.last_poll_ok: bool | None = None

    def _extract_current_ref(self, record: IssueRecord) -> str | None:
        if record.last_event_meta:
            try:
                meta = json.loads(record.last_event_meta)
                if isinstance(meta, dict):
                    for key in ("release", "head_sha", "image", "fingerprint"):
                        val = str(meta.get(key) or "").strip()
                        if val:
                            return val
            except (ValueError, TypeError):
                pass
        bundle_file = self.artifact_dir(record.issue_id) / "issue_bundle.json"
        if bundle_file.exists():
            try:
                raw = json.loads(bundle_file.read_text(encoding="utf-8"))
                if isinstance(raw, dict) and raw.get("release"):
                    return str(raw["release"]).strip()
            except (OSError, ValueError):
                pass
        return None

    def _task_channel(self, record: IssueRecord) -> str:
        return record.channel_id or self.channel.resolve_channel_for_task(record.task_kind)

    @property
    def mirrors_root(self) -> Path:
        return self.settings.state_dir / "mirrors"

    def artifact_dir(self, issue_id: str) -> Path:
        path = self.settings.state_dir / "artifacts" / issue_id
        path.mkdir(parents=True, exist_ok=True)
        return path

    def session_dir(self, issue_id: str) -> Path:
        path = self.settings.state_dir / "sessions" / issue_id
        path.mkdir(parents=True, exist_ok=True)
        return path

    def load_targets(self) -> TargetsConfig:
        return TargetsConfig.from_yaml(self.settings.targets_config_path)

    def _make_sentry_client(self, targets: TargetsConfig | None = None) -> SentryClient | None:
        if not self.settings.sentry_auth_token.strip():
            return None
        cfg = targets or self.load_targets()
        return SentryClient(
            base_url=cfg.sentry.base_url,
            organization_slug=cfg.sentry.organization_slug,
            auth_token=self.settings.sentry_auth_token,
        )

    async def _post_sentry_note_best_effort(self, issue_id: str, text: str) -> None:
        sentry = self._make_sentry_client()
        if sentry is None:
            return
        try:
            await sentry.add_issue_note(issue_id, text)
        except httpx.HTTPError as exc:
            log.warning("sentry.note_failed", issue_id=issue_id, error=str(exc))

    async def handle_open_draft_pr(self, issue_id: str, user: str) -> None:
        record = self.state.get_by_issue_id(issue_id)
        if not record or not record.thread_ts:
            return
        ch = self._task_channel(record)
        if record.status == IssueStatus.PR_OPENED and record.pr_url:
            await self.channel.post_thread_message(
                record.thread_ts, f"Draft PR already open: {record.pr_url}", channel_id=ch
            )
            return

        art_dir = self.artifact_dir(issue_id)
        patch_path = art_dir / "fix.patch"
        envelope_path = art_dir / "result.json"
        report: TriageReport | None = None
        if envelope_path.exists():
            envelope = WorkerEnvelope.model_validate_json(envelope_path.read_text(encoding="utf-8"))
            report = envelope.report

        slug = re.sub(r"[^a-z0-9._-]+", "-", record.short_id.lower()).strip("-")
        task_prefix = (
            "sentry" if record.task_kind == "sentry" else record.task_kind.replace("_", "-")
        )
        branch_name = f"agy/{task_prefix}-{slug}"
        await self.channel.post_thread_message(
            record.thread_ts,
            f"Pushing branch `{branch_name}` and opening Draft PR (requested by `{user}`)…",
            channel_id=ch,
        )
        try:
            pr_url = await create_draft_pull_request(
                mirrors_root=self.mirrors_root,
                record=record,
                report=report,
                patch_path=patch_path,
                github_token=self.settings.resolve_github_token(record.github_repo),
            )
            self.state.cancel_timers_for_issue(issue_id)
            self.state.mark_status(
                issue_id,
                IssueStatus.PR_OPENED,
                pr_url=pr_url,
                resolved_ref=self._extract_current_ref(record),
            )
            if record.task_kind == "sentry":
                await self._post_sentry_note_best_effort(
                    issue_id,
                    f"Draft PR opened by @{user} via AGY Slack triage: {pr_url}",
                )
                msg = f"Draft PR opened (and linked on Sentry): {pr_url}"
            else:
                msg = f"Draft PR opened: {pr_url}"
            await self.channel.post_thread_message(record.thread_ts, msg, channel_id=ch)
        except Exception as exc:
            log.exception("draft_pr.failed", issue_id=issue_id)
            await self.channel.post_thread_message(
                record.thread_ts,
                f"Failed to open Draft PR for `{record.short_id}`: `{exc}`",
                channel_id=ch,
            )

    async def handle_push_to_pr_branch(self, issue_id: str, user: str) -> None:
        record = self.state.get_by_issue_id(issue_id)
        if not record or not record.thread_ts or not record.target_branch:
            return
        ch = self._task_channel(record)

        art_dir = self.artifact_dir(issue_id)
        patch_path = art_dir / "fix.patch"
        envelope_path = art_dir / "result.json"
        report: TriageReport | None = None
        if envelope_path.exists():
            envelope = WorkerEnvelope.model_validate_json(envelope_path.read_text(encoding="utf-8"))
            report = envelope.report

        await self.channel.post_thread_message(
            record.thread_ts,
            f"Applying fix and pushing commit to branch `{record.target_branch}` (requested by `{user}`)…",
            channel_id=ch,
        )
        try:
            short_sha = await push_patch_to_branch(
                mirrors_root=self.mirrors_root,
                record=record,
                report=report,
                patch_path=patch_path,
                branch_name=record.target_branch,
                github_token=self.settings.resolve_github_token(record.github_repo),
            )
            self.state.cancel_timers_for_issue(issue_id)
            self.state.mark_status(
                issue_id,
                IssueStatus.PR_OPENED,
                pr_url=record.permalink or None,
                resolved_ref=short_sha,
            )
            await self.channel.post_thread_message(
                record.thread_ts,
                f"Pushed commit `{short_sha}` to branch `{record.target_branch}`.",
                channel_id=ch,
            )
        except Exception as exc:
            log.exception("push_branch.failed", issue_id=issue_id, branch=record.target_branch)
            await self.channel.post_thread_message(
                record.thread_ts,
                f"Failed to push fix to `{record.target_branch}`: `{exc}`",
                channel_id=ch,
            )

    async def handle_resolve(self, issue_id: str, user: str) -> None:
        record = self.state.get_by_issue_id(issue_id)
        if not record:
            return
        ch = self._task_channel(record)
        sentry = self._make_sentry_client()
        sentry_msg = "locally"
        if sentry is not None:
            try:
                await sentry.update_issue_status(issue_id, status="resolved")
                await self._post_sentry_note_best_effort(
                    issue_id, f"Resolved via AGY Slack triage by @{user}."
                )
                sentry_msg = "in Sentry"
            except Exception as exc:
                log.exception("sentry.resolve_failed", issue_id=issue_id)
                if record.thread_ts:
                    await self.channel.post_thread_message(
                        record.thread_ts,
                        f"Failed to resolve `{record.short_id}` in Sentry: `{exc}`",
                        channel_id=ch,
                    )
                return

        self.state.cancel_timers_for_issue(issue_id)
        self.state.mark_status(
            issue_id,
            IssueStatus.RESOLVED,
            resolved_ref=self._extract_current_ref(record),
        )
        if record.thread_ts:
            await self.channel.post_thread_message(
                record.thread_ts,
                f"Marked `{record.short_id}` *resolved* {sentry_msg} (by `{user}`).",
                channel_id=ch,
            )

    async def handle_ignore(self, issue_id: str, user: str) -> None:
        record = self.state.get_by_issue_id(issue_id)
        if not record:
            return
        ch = self._task_channel(record)
        sentry = self._make_sentry_client()
        sentry_msg = "locally"
        if sentry is not None:
            try:
                await sentry.update_issue_status(
                    issue_id,
                    status="ignored",
                    substatus="archived_until_escalating",
                )
                await self._post_sentry_note_best_effort(
                    issue_id,
                    f"Archived (until escalating) via AGY Slack triage by @{user}.",
                )
                sentry_msg = "in Sentry (until escalating)"
            except httpx.HTTPError as exc:
                log.warning("sentry.ignore_failed", issue_id=issue_id, error=str(exc))
                sentry_msg = f"locally (Sentry API returned `{exc}`)"

        self.state.cancel_timers_for_issue(issue_id)
        self.state.mark_status(issue_id, IssueStatus.IGNORED)
        if record.thread_ts:
            await self.channel.post_thread_message(
                record.thread_ts,
                f"Marked `{record.short_id}` *ignored* {sentry_msg} (by `{user}`).",
                channel_id=ch,
            )

    async def handle_reject(self, issue_id: str, user: str) -> None:
        record = self.state.get_by_issue_id(issue_id)
        if not record:
            return
        ch = self._task_channel(record)
        self.state.cancel_timers_for_issue(issue_id)
        self.state.mark_status(issue_id, IssueStatus.REJECTED)
        if record.thread_ts:
            await self.channel.post_thread_message(
                record.thread_ts,
                f"Rejected proposal for `{record.short_id}` (by `{user}`). No PR or branch push will be performed.",
                channel_id=ch,
            )

    async def handle_save_memory_note(self, issue_id: str, user: str) -> None:
        record = self.state.get_by_issue_id(issue_id)
        if not record:
            return
        ch = self._task_channel(record)
        envelope_path = self.artifact_dir(issue_id) / "result.json"
        note = ""
        if envelope_path.exists():
            try:
                envelope = WorkerEnvelope.model_validate_json(
                    envelope_path.read_text(encoding="utf-8")
                )
                if envelope.report is not None:
                    note = envelope.report.proposed_memory_note.strip()
            except (OSError, ValueError):
                note = ""
        if not note:
            if record.thread_ts:
                await self.channel.post_thread_message(
                    record.thread_ts,
                    f"No proposed memory note found on `{record.short_id}`.",
                    channel_id=ch,
                )
            return

        entry = self.memory.add_repo_note(
            record.github_repo,
            note,
            author=user,
            source_ref=record.short_id,
        )
        if record.thread_ts:
            await self.channel.post_thread_message(
                record.thread_ts,
                (
                    f"Saved to persistent repo memory (`{record.github_repo}`, `{entry.entry_id}`): "
                    f"{entry.text}"
                ),
                channel_id=ch,
            )

    async def handle_snooze(self, thread_ts: str, args_text: str, user: str) -> str:
        record = self.state.get_by_thread_ts(thread_ts)
        if record is None:
            return "Snooze must be used inside an existing task thread (e.g. `!snooze 2h check after deploy`)."

        parts = args_text.strip().split(maxsplit=1)
        if not parts:
            mins = 120
            reason = "Follow up on snoozed task and verify current status."
        else:
            parsed = parse_duration_minutes(parts[0])
            if parsed is not None:
                mins = parsed
                reason = (
                    parts[1].strip()
                    if len(parts) > 1 and parts[1].strip()
                    else "Follow up on snoozed task and verify current status."
                )
            else:
                mins = 120
                reason = args_text.strip()

        fire_dt = datetime.now(UTC) + timedelta(minutes=mins)
        timer = self.state.schedule_timer(
            dedup_key=f"issue:{record.issue_id}",
            issue_id=record.issue_id,
            fire_at=fire_dt.isoformat(),
            prompt=reason,
            kind="followup",
            created_by=user,
        )
        if record.status != IssueStatus.RUNNING:
            self.state.mark_status(record.issue_id, IssueStatus.SNOOZED)

        fire_str = fire_dt.strftime("%Y-%m-%d %H:%M UTC")
        return (
            f"⏰ Snoozed `{record.short_id}` for `{mins}m` "
            f"(timer `#{timer.id}`, fires `{fire_str}`): _{reason}_"
        )

    async def handle_timers_command(self, args_text: str) -> str:
        clean = args_text.strip()
        lower = clean.lower()
        if lower.startswith("cancel "):
            raw_id = clean.split(maxsplit=1)[1].strip().lstrip("#")
            if not raw_id.isdigit():
                return "Usage: `!timers cancel <id>`"
            tid = int(raw_id)
            if self.state.cancel_timer_by_id(tid):
                return f"Cancelled pending timer `#{tid}`."
            return f"No pending timer with ID `#{tid}`."

        pending = self.state.list_pending_timers()
        if not pending:
            return "*Pending Follow-Up Timers:* _none scheduled_ (use `!snooze <30m|2h|1d> [note]` inside any task thread)."

        lines = [f"*Pending Follow-Up Timers ({len(pending)}):*"]
        for t in pending[:25]:
            issue_label = f"`{t.issue_id}`" if t.issue_id else "_standalone_"
            lines.append(
                f"• `#{t.id}` → `{t.fire_at}` | {issue_label} | by `{t.created_by}` — {t.prompt or '_no note_'}"
            )
        lines.append("_Cancel any timer with `!timers cancel <id>`._")
        return "\n".join(lines)

    async def handle_memory_command(self, action: str, args_text: str, user: str) -> str:
        act = action.strip().lower()
        if act == "show":
            return self.memory.format_slack_summary()

        if act == "forget":
            target = args_text.strip()
            if not target:
                return "Usage: `!forget <mem-xxxxxx>`"
            removed = self.memory.remove_entry(target)
            if removed is None:
                return f"No persistent memory entry matching `{target}`."
            return f"Removed `{removed.entry_id}` (`{removed.scope}`): _{removed.text}_"

        body = args_text.strip()
        if not body:
            return (
                "Usage:\n"
                "• `!remember <standing instruction>`\n"
                "• `!remember repo:lodbrok-app/rattlsnek <repo note>`\n"
                "• `!remember suppress <non-actionable pattern>`"
            )

        first, _, rest = body.partition(" ")
        if first.lower().startswith("repo:"):
            repo_slug = first[5:].strip()
            if not rest.strip():
                return (
                    f"Provide a note after `{first}`, e.g. `!remember {first} run uv lock first`."
                )
            try:
                entry = self.memory.add_repo_note(repo_slug, rest.strip(), author=user)
            except ValueError as exc:
                return str(exc)
            return f"Saved repo memory `{entry.entry_id}` (`{repo_slug}`): {entry.text}"

        if first.lower() in {"suppress", "ignore", "noise"} and rest.strip():
            entry = self.memory.add_suppressed_pattern(rest.strip(), author=user)
            return f"Saved suppressed pattern `{entry.entry_id}`: {entry.text}"

        entry = self.memory.add_instruction(body, author=user)
        return f"Saved standing instruction `{entry.entry_id}`: {entry.text}"

    async def handle_thread_feedback(self, thread_ts: str, feedback_text: str, user: str) -> None:
        record = self.state.get_by_thread_ts(thread_ts)
        if not record:
            return
        ch = self._task_channel(record)
        stripped = feedback_text.strip()
        lower = stripped.lower()
        if lower in {"!snooze", "snooze"} or lower.startswith(("!snooze ", "snooze ")):
            tail = stripped.split(maxsplit=1)[1] if " " in stripped else ""
            ack = await self.handle_snooze(thread_ts, tail, user)
            await self.channel.post_thread_message(thread_ts, ack, channel_id=ch)
            return

        model_override, cleaned_feedback = parse_model_override(feedback_text)
        prompt_feedback = cleaned_feedback or "Please retry and refine the current proposal."

        if record.status == IssueStatus.RUNNING:
            buffered_n = self.state.append_pending_feedback(
                record.issue_id,
                prompt_feedback,
                model_override=model_override,
            )
            await self.channel.post_thread_message(
                thread_ts,
                (
                    f"Buffered follow-up #{buffered_n} for `{record.short_id}` "
                    f"(turn {record.turn} is running) — I'll fold it into turn {record.turn + 1} "
                    f"automatically as soon as turn {record.turn} completes."
                ),
                channel_id=ch,
            )
            return

        self.state.pop_pending_feedback(record.issue_id)
        self.state.cancel_timers_for_issue(record.issue_id)
        updated = self.state.bump_turn_for_feedback(
            record.issue_id,
            model_override=model_override,
        )
        art_dir = self.artifact_dir(record.issue_id)
        (art_dir / f"feedback-turn-{updated.turn}.txt").write_text(
            prompt_feedback, encoding="utf-8"
        )
        model_label = updated.model_override or self.settings.worker_model
        await self.channel.post_thread_message(
            thread_ts,
            f"Queued revision turn {updated.turn} for `{updated.short_id}` (`{model_label}`) with your feedback…",
            channel_id=ch,
        )
        await self.dispatch_pending()

    async def dispatch_pending(self) -> None:
        await self._dispatch_queued_jobs(self.load_targets())

    async def _poll_sentry_once(self, targets: TargetsConfig) -> None:
        self.poll_count += 1
        self.last_poll_at = datetime.now(UTC)
        if not self.settings.sentry_auth_token.strip():
            self.last_poll_ok = False
            log.warning(
                "sentry.polling_skipped",
                poll_count=self.poll_count,
                reason="SENTRY_AUTH_TOKEN is empty; Slack Socket Mode remains connected",
            )
            return

        sentry = SentryClient(
            base_url=targets.sentry.base_url,
            organization_slug=targets.sentry.organization_slug,
            auth_token=self.settings.sentry_auth_token,
        )
        now = self.last_poll_at
        projects_polled = 0
        poll_errors = 0
        new_issues = 0
        debounced_issues = 0
        deduped_issues = 0
        for project_slug, proj_cfg in targets.projects.items():
            if not proj_cfg.enabled:
                continue
            projects_polled += 1
            try:
                issues = await sentry.fetch_recent_unresolved_issues(
                    project_slug,
                    query=targets.sentry.lookback_query,
                    limit=targets.sentry.max_issues_per_poll,
                )
            except Exception:
                poll_errors += 1
                log.exception("sentry.poll_failed", project=project_slug)
                continue

            for raw_issue in issues:
                issue_id = str(raw_issue.get("id") or "")
                if not issue_id:
                    continue

                # Stage A: Settle / debounce transient 1-event deploy blips
                first_seen_dt = parse_iso_utc(str(raw_issue.get("firstSeen") or ""))
                try:
                    event_count = int(raw_issue.get("count") or 0)
                except (TypeError, ValueError):
                    event_count = 0

                if (
                    targets.sentry.settle_seconds > 0
                    and first_seen_dt is not None
                    and (now - first_seen_dt).total_seconds() < targets.sentry.settle_seconds
                    and event_count < max(1, targets.sentry.min_events_bypass_settle)
                ):
                    debounced_issues += 1
                    log.info(
                        "sentry.issue_debounced_settling",
                        issue_id=issue_id,
                        project=project_slug,
                        event_count=event_count,
                        age_s=round((now - first_seen_dt).total_seconds(), 1),
                    )
                    continue

                exact_existing = self.state.get_by_issue_id(issue_id)
                if exact_existing is not None:
                    if exact_existing.status not in (IssueStatus.RESOLVED, IssueStatus.SILENT):
                        self.state.record_duplicate_hit(issue_id)
                        deduped_issues += 1
                        continue
                    ext_dt = parse_iso_utc(exact_existing.updated_at)
                    last_seen_dt = parse_iso_utc(str(raw_issue.get("lastSeen") or ""))
                    within_cd = (
                        ext_dt is not None
                        and (now - ext_dt).total_seconds()
                        < targets.sentry.regression_cooldown_seconds
                    )
                    no_newer_sighting = (
                        ext_dt is not None and last_seen_dt is not None and last_seen_dt <= ext_dt
                    )
                    if within_cd or no_newer_sighting:
                        self.state.record_duplicate_hit(issue_id)
                        deduped_issues += 1
                        continue

                try:
                    bundle = await sentry.fetch_sanitized_bundle(raw_issue, project_slug)
                except Exception:
                    log.exception("sentry.bundle_failed", issue_id=issue_id)
                    continue

                dedup_key = compute_sentry_dedup_key(proj_cfg.github_repo, bundle)
                event_meta = json.dumps(
                    {
                        "sentry_issue_id": bundle.issue_id,
                        "short_id": bundle.short_id,
                        "project": project_slug,
                        "release": bundle.release,
                    }
                )

                extant = exact_existing or self.state.get_latest_by_dedup_key(dedup_key)
                if extant is not None:
                    if extant.status in (
                        IssueStatus.QUEUED,
                        IssueStatus.RUNNING,
                        IssueStatus.SNOOZED,
                        IssueStatus.AWAITING_REVIEW,
                        IssueStatus.PR_OPENED,
                        IssueStatus.IGNORED,
                    ):
                        self.state.record_duplicate_hit(extant.issue_id, last_event_meta=event_meta)
                        deduped_issues += 1
                        log.info(
                            "sentry.deduplicated_against_extant",
                            issue_id=issue_id,
                            extant_issue_id=extant.issue_id,
                            extant_status=extant.status.value,
                            dedup_key=dedup_key,
                        )
                        continue

                    ext_dt = parse_iso_utc(extant.updated_at)
                    within_cd = (
                        ext_dt is not None
                        and (now - ext_dt).total_seconds()
                        < targets.sentry.regression_cooldown_seconds
                    )
                    same_release = bool(bundle.release and extant.resolved_ref == bundle.release)
                    if extant.status == IssueStatus.FAILED or within_cd or same_release:
                        self.state.record_duplicate_hit(extant.issue_id, last_event_meta=event_meta)
                        deduped_issues += 1
                        continue

                    # Genuine regression on a newer release after cooldown -> reopen extant thread
                    extant_art = self.artifact_dir(extant.issue_id)
                    (extant_art / "issue_bundle.json").write_text(
                        bundle.model_dump_json(indent=2), encoding="utf-8"
                    )
                    reopened = self.state.reopen_for_regression(
                        extant.issue_id,
                        banner_note=(
                            f":arrows_counterclockwise: Regressed in `{project_slug}` "
                            f"(`{bundle.short_id}`, release `{bundle.release or 'unknown'}`)"
                        ),
                        last_event_meta=event_meta,
                        permalink=bundle.permalink,
                    )
                    (extant_art / f"feedback-turn-{reopened.turn}.txt").write_text(
                        (
                            f"Issue regressed in `{project_slug}` (`{bundle.short_id}`, "
                            f"release `{bundle.release or 'unknown'}`). Re-investigate with the "
                            f"updated Sentry event payload:\n\n"
                            f"<untrusted_sentry_bundle_json>\n"
                            f"{bundle.model_dump_json(indent=2)}\n"
                            f"</untrusted_sentry_bundle_json>"
                        ),
                        encoding="utf-8",
                    )
                    new_issues += 1
                    continue

                art_dir = self.artifact_dir(issue_id)
                (art_dir / "issue_bundle.json").write_text(
                    bundle.model_dump_json(indent=2), encoding="utf-8"
                )
                self.session_dir(issue_id)

                rec = self.state.register_if_new(
                    issue_id=bundle.issue_id,
                    short_id=bundle.short_id,
                    project_slug=project_slug,
                    github_repo=proj_cfg.github_repo,
                    default_branch=proj_cfg.default_branch,
                    title=bundle.title,
                    permalink=bundle.permalink,
                    dedup_key=dedup_key,
                    last_event_meta=event_meta,
                )
                if rec:
                    new_issues += 1
                    log.info(
                        "sentry.issue_queued",
                        issue_id=rec.issue_id,
                        short_id=rec.short_id,
                        project=project_slug,
                        dedup_key=dedup_key,
                    )

        self.last_poll_ok = poll_errors == 0
        log.info(
            "sentry.poll_tick",
            poll_count=self.poll_count,
            projects_polled=projects_polled,
            poll_errors=poll_errors,
            new_issues=new_issues,
            debounced_issues=debounced_issues,
            deduped_issues=deduped_issues,
            running_jobs=self.state.count_running(),
        )

    async def build_keepalive_report(self) -> str:
        """Build a live operational keepalive summary for Slack (`!ping` / `@agytasks`)."""
        targets = self.load_targets()
        uptime_s = int((datetime.now(UTC) - self.started_at).total_seconds())
        hours, rem = divmod(uptime_s, 3600)
        mins, secs = divmod(rem, 60)
        uptime_str = f"{hours}h {mins}m {secs}s" if hours else f"{mins}m {secs}s"

        sentry = self._make_sentry_client(targets)
        first_slug = next((s for s, p in targets.projects.items() if p.enabled), None)
        if sentry is None or first_slug is None:
            sentry_status = "disabled (`SENTRY_AUTH_TOKEN` empty)"
        else:
            try:
                latency_ms = await sentry.ping_project(first_slug)
                sentry_status = (
                    f"OK (`{targets.sentry.organization_slug}/{first_slug}`, "
                    f"`{latency_ms:.0f}ms` RTT)"
                )
            except httpx.HTTPError as exc:
                sentry_status = f"ERROR (`{exc}`)"

        last_poll_str = (
            f"`{self.last_poll_at.strftime('%Y-%m-%d %H:%M:%SZ')}` "
            f"({'OK' if self.last_poll_ok else 'WARN'})"
            if self.last_poll_at
            else "_pending_"
        )
        counts = self.state.status_counts()
        counts_str = (
            ", ".join(f"`{k}`={v}" for k, v in sorted(counts.items())) if counts else "_none yet_"
        )
        pod_name = socket.gethostname()
        node_label = self.settings.node_name or "unknown-node"

        billing_line = (
            f"\n• *Credit Ledger:* {self.billing_guard.format_short_status()}"
            if self.billing_guard is not None
            else ""
        )

        pending_timers = len(self.state.list_pending_timers())
        memory_entries = len(self.memory.list_all())

        return (
            f"*`agent-tasks` Keepalive — Healthy*\n"
            f"• *Pod / Node:* `{pod_name}` on `{node_label}` (ns `{self.settings.namespace}`)\n"
            f"• *Default Worker Model:* `{self.settings.worker_model}` "
            f"(override per message with `[carbon]`, `[argon]`, `[flash]`, or `[hydrogen]`)\n"
            f"• *Active Automations:* `sentry-triage`, `slack-coding`, `ci-healer`, "
            f"`k8s-watchdog`, `dep-upgrader`, `billing-monitor` (send `!help` for Slack commands)"
            f"{billing_line}\n"
            f"• *Uptime:* `{uptime_str}`\n"
            f"• *Sentry Schedule:* every `{targets.sentry.poll_interval_seconds}s` across "
            f"`{len(targets.projects)}` projects | Polls completed: `{self.poll_count}` | "
            f"Last poll: {last_poll_str}\n"
            f"• *Sentry Live Ping:* {sentry_status}\n"
            f"• *Tracked Tasks (SQLite):* {counts_str} | Pending Timers: `{pending_timers}` | "
            f"Memory Entries: `{memory_entries}`"
        )

    async def _dispatch_queued_jobs(self, targets: TargetsConfig) -> None:
        async with self._dispatch_lock:
            running = self.state.count_running()
            slots = max(0, self.settings.max_concurrent_jobs - running)
            if slots == 0:
                return

            queued = self.state.list_by_status(IssueStatus.QUEUED)
            mirrors_by_repo: dict[str, Path] = {}
            bg_paused = False
            bg_pause_reason = ""
            if self.billing_guard is not None:
                bg_paused, bg_pause_reason = self.billing_guard.is_background_paused(
                    targets=targets
                )
                if bg_paused:
                    await self.billing_guard.evaluate_and_alert()

            spawned = 0
            for record in queued:
                if spawned >= slots:
                    break
                if bg_paused and record.task_kind != "slack_coding":
                    log.warning(
                        "dispatch.paused_by_billing_guard",
                        issue_id=record.issue_id,
                        task_kind=record.task_kind,
                        reason=bg_pause_reason,
                    )
                    continue

                target = resolve_target_config(record, targets)
                if not target:
                    self.state.mark_status(
                        record.issue_id,
                        IssueStatus.FAILED,
                        last_error=f"Unconfigured target for task_kind={record.task_kind} project={record.project_slug}",
                    )
                    continue

                try:
                    (self.settings.state_dir / "caches" / "uv").mkdir(parents=True, exist_ok=True)
                    (self.settings.state_dir / "caches" / "npm").mkdir(parents=True, exist_ok=True)
                    (self.settings.state_dir / "agy-auth").mkdir(parents=True, exist_ok=True)
                    self.memory.ensure_dirs()
                    self.session_dir(record.issue_id)
                    art_dir = self.artifact_dir(record.issue_id)

                    snapshot_file = art_dir / "repo_snapshot.tar.gz"
                    has_repo_snapshot = snapshot_file.is_file() and snapshot_file.stat().st_size > 0
                    mirror_path: Path | None = None
                    if not has_repo_snapshot:
                        mirror_path = mirrors_by_repo.get(record.github_repo)
                        if mirror_path is None:
                            mirror_path = await sync_bare_mirror(
                                self.mirrors_root,
                                record.github_repo,
                                self.settings.resolve_github_token(record.github_repo),
                            )
                            mirrors_by_repo[record.github_repo] = mirror_path

                    is_handoff = False
                    handoff_backend_override: str | None = None
                    meta_file = art_dir / "handoff_meta.json"
                    if meta_file.exists():
                        try:
                            meta_obj = json.loads(meta_file.read_text(encoding="utf-8"))
                            if isinstance(meta_obj, dict):
                                is_handoff = bool(meta_obj.get("is_handoff"))
                                raw_b = meta_obj.get("worker_backend")
                                if isinstance(raw_b, str) and raw_b.strip():
                                    handoff_backend_override = raw_b.strip()
                        except (OSError, json.JSONDecodeError):
                            pass

                    worker_backend = resolve_worker_backend(
                        targets,
                        task_kind=record.task_kind,
                        is_handoff=is_handoff,
                        override=handoff_backend_override,
                    )
                    if (
                        worker_backend == "agy_cli"
                        and targets.agy_config.enabled
                        and targets.agy_config.github_repo.strip()
                    ):
                        agy_cfg_repo = targets.agy_config.github_repo.strip()
                        try:
                            await sync_agy_config_repo(
                                self.settings.state_dir / "agy-auth",
                                agy_cfg_repo,
                                self.settings.resolve_github_token(agy_cfg_repo),
                                branch=targets.agy_config.branch.strip() or "main",
                            )
                        except Exception as cfg_exc:  # noqa: BLE001
                            log.warning(
                                "agy_config.sync_failed",
                                repo=targets.agy_config.github_repo,
                                error=str(cfg_exc),
                            )

                    if (
                        record.task_kind == "sentry"
                        and record.turn == 1
                        and mirror_path is not None
                    ):
                        bundle_file = art_dir / "issue_bundle.json"
                        if bundle_file.exists():
                            bundle = SanitizedSentryBundle.model_validate_json(
                                bundle_file.read_text(encoding="utf-8")
                            )
                            lag_info = await check_sentry_release_lag(
                                mirror_path,
                                bundle,
                                default_branch=record.default_branch,
                            )
                            if lag_info is not None:
                                prompt_note = lag_info.to_prompt_note()
                                if prompt_note:
                                    (art_dir / "release_context.txt").write_text(
                                        prompt_note, encoding="utf-8"
                                    )
                                slack_banner = lag_info.to_slack_banner()
                                if slack_banner:
                                    record = self.state.mark_status(
                                        record.issue_id,
                                        record.status,
                                        banner_note=slack_banner,
                                    )

                    # Clear stale result.json from previous turn before spawning
                    result_file = art_dir / "result.json"
                    if result_file.exists():
                        result_file.unlink()

                    try:
                        await self.job_runner.spawn_job(
                            record,
                            target,
                            worker_backend=worker_backend,
                            use_repo_snapshot=has_repo_snapshot,
                        )
                    except TypeError:
                        try:
                            await self.job_runner.spawn_job(
                                record, target, worker_backend=worker_backend
                            )
                        except TypeError:
                            await self.job_runner.spawn_job(record, target)
                    self.state.mark_status(record.issue_id, IssueStatus.RUNNING)
                    spawned += 1
                except Exception as exc:
                    log.exception("dispatch.failed", issue_id=record.issue_id)
                    self.state.mark_status(
                        record.issue_id,
                        IssueStatus.FAILED,
                        last_error=str(exc),
                    )
                    if record.thread_ts:
                        await self.channel.post_thread_message(
                            record.thread_ts,
                            f"Failed to launch worker Job for `{record.short_id}`: `{exc}`",
                            channel_id=self._task_channel(record),
                        )

    async def _reconcile_running_jobs(self) -> None:
        for record in self.state.list_by_status(IssueStatus.RUNNING):
            terminal = await self.job_runner.get_job_terminal_state(record.issue_id, record.turn)
            if terminal is None:
                continue

            art_dir = self.artifact_dir(record.issue_id)
            result_file = art_dir / "result.json"
            patch_file = art_dir / "fix.patch"
            diff_text = patch_file.read_text(encoding="utf-8") if patch_file.exists() else ""
            target_channel = self._task_channel(record)

            envelope: WorkerEnvelope | None = None
            raw_error: str | None = None
            if result_file.exists():
                try:
                    raw_payload = json.loads(result_file.read_text(encoding="utf-8"))
                    if isinstance(raw_payload, dict) and raw_payload.get("error"):
                        raw_error = str(raw_payload["error"])
                    envelope = WorkerEnvelope.model_validate(raw_payload)
                except (OSError, json.JSONDecodeError, pydantic.ValidationError) as parse_exc:
                    log.warning("triage.envelope_unreadable", error=str(parse_exc))

            model_used = (
                (envelope.model if envelope else "")
                or record.model_override
                or self.settings.worker_model
            )
            turn_cost = 0.0
            is_account_billed = envelope is not None and envelope.worker_backend == "agy_cli"
            if (
                self.billing_guard is not None
                and envelope is not None
                and not is_account_billed
                and envelope.usage.total_tokens > 0
            ):
                turn_cost = await self.billing_guard.record_worker_turn(
                    record=record,
                    envelope=envelope,
                    default_model=model_used,
                )

            if terminal == "succeeded" and envelope is not None and envelope.report is not None:
                report = envelope.report
                has_patch = bool(diff_text.strip())
                scheduled_timer = None
                if (
                    report.followup_in_minutes is not None
                    and report.followup_in_minutes > 0
                    and record.auto_followup_count < MAX_AUTO_FOLLOWUPS
                ):
                    mins = max(1, min(int(report.followup_in_minutes), 10080))
                    fire_dt = datetime.now(UTC) + timedelta(minutes=mins)
                    scheduled_timer = self.state.schedule_timer(
                        dedup_key=f"issue:{record.issue_id}",
                        issue_id=record.issue_id,
                        fire_at=fire_dt.isoformat(),
                        prompt=report.followup_reason
                        or f"Re-verify status for {record.short_id} after {mins}m.",
                        kind="followup",
                        created_by="agent",
                    )

                # Post-triage file-locus deduplication: if an unattended Turn 1 background task
                # touches files already covered by an open AWAITING_REVIEW / PR_OPENED thread in
                # the same repository, fold a 1-line footnote into that existing thread instead
                # of opening a competing top-level alert.
                if (
                    record.turn == 1
                    and record.thread_ts is None
                    and record.task_kind != "slack_coding"
                    and report.affected_files
                ):
                    overlaps = self.state.find_overlapping_open_issues(
                        record.github_repo,
                        report.affected_files,
                        exclude_issue_id=record.issue_id,
                    )
                    primary_match = next(
                        ((ext, files) for ext, files in overlaps if ext.thread_ts),
                        None,
                    )
                    if primary_match is not None:
                        ext_rec, shared_files = primary_match
                        files_md = ", ".join(f"`{f}`" for f in shared_files[:4])
                        ref_md = (
                            f"<{record.permalink}|{record.short_id}>"
                            if record.permalink
                            else f"`{record.short_id}`"
                        )
                        await self.channel.post_thread_message(
                            ext_rec.thread_ts,  # type: ignore[arg-type]
                            (
                                f"🔗 *Coalesced overlapping `{record.task_kind}` signal* {ref_md} "
                                f'(_"{record.title}"_ — shared locus: {files_md}). '
                                f"Suppressed duplicate top-level thread."
                            ),
                            channel_id=self._task_channel(ext_rec),
                        )
                        self.state.cancel_timers_for_issue(record.issue_id)
                        self.state.mark_status(
                            record.issue_id,
                            IssueStatus.SILENT,
                            conversation_id=envelope.conversation_id,
                            thread_ts=ext_rec.thread_ts,
                            channel_id=self._task_channel(ext_rec),
                            affected_files=report.affected_files,
                            resolved_ref=self._extract_current_ref(record),
                        )
                        log.info(
                            "triage.coalesced_into_extant_thread",
                            issue_id=record.issue_id,
                            extant_issue_id=ext_rec.issue_id,
                            shared_files=shared_files,
                        )
                        continue

                can_stay_silent = (
                    not report.should_notify
                    and not has_patch
                    and record.turn == 1
                    and record.task_kind != "slack_coding"
                    and record.thread_ts is None
                )
                if can_stay_silent:
                    next_status = (
                        IssueStatus.SNOOZED if scheduled_timer is not None else IssueStatus.SILENT
                    )
                    self.state.mark_status(
                        record.issue_id,
                        next_status,
                        conversation_id=envelope.conversation_id,
                        channel_id=target_channel,
                        affected_files=report.affected_files,
                        resolved_ref=self._extract_current_ref(record),
                    )
                    log.info(
                        "triage.silent_exit",
                        issue_id=record.issue_id,
                        short_id=record.short_id,
                        task_kind=record.task_kind,
                        status=next_status.value,
                        timer_id=scheduled_timer.id if scheduled_timer else None,
                    )
                    continue

                if scheduled_timer is not None:
                    timer_banner = (
                        f"⏰ *Auto follow-up scheduled* (`#{scheduled_timer.id}`, "
                        f"fires `{scheduled_timer.fire_at}`): _{scheduled_timer.prompt}_"
                    )
                    merged_banner = (
                        f"{record.banner_note}\n{timer_banner}"
                        if record.banner_note
                        else timer_banner
                    )
                    record = self.state.mark_status(
                        record.issue_id,
                        record.status,
                        banner_note=merged_banner,
                    )

                if is_account_billed:
                    cost_line = (
                        f"Auth: `agy` account (`{envelope.usage.total_tokens:,}` tok) • "
                        if envelope.usage.total_tokens > 0
                        else "Auth: `agy` account • "
                    )
                else:
                    cost_line = (
                        f"Cost: `${turn_cost:.4f}` (`{envelope.usage.prompt_tokens:,}` in / "
                        f"`{envelope.usage.output_tokens + envelope.usage.thoughts_tokens:,}` out+think) • "
                        if envelope.usage.total_tokens > 0
                        else ""
                    )
                thread_ts = await self.channel.post_triage_result(
                    record=record,
                    report=report,
                    diff_text=diff_text,
                    model_used=model_used,
                    cost_line=cost_line,
                )
                next_review_status = (
                    IssueStatus.SNOOZED
                    if (scheduled_timer is not None and not has_patch)
                    else IssueStatus.AWAITING_REVIEW
                )
                self.state.mark_status(
                    record.issue_id,
                    next_review_status,
                    conversation_id=envelope.conversation_id,
                    thread_ts=thread_ts,
                    channel_id=target_channel,
                    affected_files=report.affected_files,
                )
                if record.turn == 1 and record.task_kind == "sentry":
                    tests_label = "PASSED" if report.tests_passed else "FAILED/UNVERIFIED"
                    await self._post_sentry_note_best_effort(
                        record.issue_id,
                        (
                            f"AGY automated triage completed ({model_used}) "
                            f"(confidence: {report.confidence}, tests: {tests_label}).\n\n"
                            f"Root cause: {report.root_cause}\n\n"
                            f"Proposed fix: {report.fix_summary}"
                        ),
                    )
                log.info(
                    "triage.posted",
                    issue_id=record.issue_id,
                    short_id=record.short_id,
                    task_kind=record.task_kind,
                    model=model_used,
                    cost_usd=round(turn_cost, 6),
                    turn=record.turn,
                )

                buffered_replies = self.state.pop_pending_feedback(record.issue_id)
                if buffered_replies:
                    self.state.cancel_timers_for_issue(record.issue_id)
                    combined_fb = "\n\n---\n\n".join(buffered_replies)
                    bumped = self.state.bump_turn_for_feedback(record.issue_id)
                    (art_dir / f"feedback-turn-{bumped.turn}.txt").write_text(
                        combined_fb, encoding="utf-8"
                    )
                    await self.dispatch_pending()
                continue

            err_msg = raw_error or f"Worker Job ended with status={terminal}"
            self.state.mark_status(
                record.issue_id,
                IssueStatus.FAILED,
                last_error=err_msg,
                channel_id=target_channel,
            )
            log.error("triage.failed", issue_id=record.issue_id, error=err_msg)
            id_link = (
                f"*<{record.permalink}|{record.short_id}>*"
                if record.permalink
                else f"*{record.short_id}*"
            )
            failure_text = (
                f"Worker turn {record.turn} failed for "
                f"{id_link} (`{record.project_slug}`): "
                f"`{err_msg}` — reply in this thread to retry."
            )
            if record.thread_ts:
                await self.channel.post_thread_message(
                    record.thread_ts, failure_text, channel_id=target_channel
                )
            else:
                new_ts = await self.channel.post_channel_message(
                    failure_text, channel_id=target_channel
                )
                self.state.mark_status(
                    record.issue_id,
                    IssueStatus.FAILED,
                    thread_ts=new_ts,
                    channel_id=target_channel,
                    last_error=err_msg,
                )

    async def process_due_timers(self, now: datetime | None = None) -> int:
        """Fire any due agent-set or user-set follow-up timers and queue wake-up turns."""
        current = now or datetime.now(UTC)
        due = self.state.pop_due_timers(current.isoformat())
        if not due:
            return 0

        woken = 0
        for timer in due:
            if not timer.issue_id:
                continue
            record = self.state.get_by_issue_id(timer.issue_id)
            if record is None or record.status in (
                IssueStatus.RESOLVED,
                IssueStatus.IGNORED,
                IssueStatus.PR_OPENED,
            ):
                continue

            if record.status == IssueStatus.RUNNING:
                defer_dt = current + timedelta(minutes=2)
                self.state.schedule_timer(
                    dedup_key=timer.dedup_key,
                    issue_id=timer.issue_id,
                    fire_at=defer_dt.isoformat(),
                    prompt=timer.prompt,
                    kind=timer.kind,
                    created_by=timer.created_by,
                )
                continue

            is_agent_timer = timer.created_by == "agent"
            if is_agent_timer and record.auto_followup_count >= MAX_AUTO_FOLLOWUPS:
                log.warning(
                    "timer.auto_followup_cap_reached",
                    issue_id=record.issue_id,
                    count=record.auto_followup_count,
                )
                continue

            bumped = self.state.bump_turn_for_feedback(
                record.issue_id,
                reset_auto_followups=not is_agent_timer,
            )
            if is_agent_timer:
                self.state.mark_status(
                    record.issue_id,
                    IssueStatus.QUEUED,
                    increment_auto_followup=True,
                )

            art_dir = self.artifact_dir(record.issue_id)
            (art_dir / f"feedback-turn-{bumped.turn}.txt").write_text(
                (
                    f"[Scheduled Follow-Up Timer #{timer.id} (created by {timer.created_by})]\n"
                    f"{timer.prompt or 'Re-verify current status and update your TriageReport.'}"
                ),
                encoding="utf-8",
            )
            woken += 1
            log.info(
                "timer.woke_issue",
                timer_id=timer.id,
                issue_id=record.issue_id,
                turn=bumped.turn,
                created_by=timer.created_by,
            )

        if woken > 0:
            await self.dispatch_pending()
        return woken

    async def run(self) -> None:
        log.info("sentry_triage.loop_started")
        last_poll_monotonic: float | None = None
        loop = asyncio.get_running_loop()

        while True:
            self._heartbeat()
            try:
                targets = self.load_targets()
                now = loop.time()

                if (
                    last_poll_monotonic is None
                    or now - last_poll_monotonic >= targets.sentry.poll_interval_seconds
                ):
                    await self._poll_sentry_once(targets)
                    last_poll_monotonic = now

                await self.process_due_timers()
                await self._dispatch_queued_jobs(targets)
                await self._reconcile_running_jobs()
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("sentry_triage.iteration_failed")
            await asyncio.sleep(10)
