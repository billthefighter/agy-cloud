from __future__ import annotations

import asyncio
import hashlib
import html
import json
import re
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

import structlog

from agent_tasks.common.config import ControllerSettings, TargetsConfig, parse_model_override
from agent_tasks.common.models import IssueStatus
from agent_tasks.common.state import StateStore

log = structlog.get_logger(__name__)

_REPO_SLUG_RE = re.compile(r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$")

USAGE_HELP = (
    "Provide a task description after `@agytasks` or `!code`, "
    "e.g. `!code [carbon] add unit tests for parser`."
)


def _parse_repo_override(text: str, default_repo: str) -> tuple[str, str]:
    """Extract an optional leading `repo:<owner>/<name>` token from cleaned prompt text."""
    if not text.lower().startswith("repo:"):
        return default_repo, text

    first_token, _, remainder = text.partition(" ")
    candidate = first_token[5:]
    if not _REPO_SLUG_RE.fullmatch(candidate) or any(
        part in {".", ".."} for part in candidate.split("/")
    ):
        raise ValueError(
            f"Invalid repository override `{candidate}`. "
            "Expected `repo:<owner>/<name>`, e.g. `repo:lodbrok-app/rattlsnek`."
        )
    return candidate, remainder.strip()


def _build_task_prompt(
    *,
    short_id: str,
    github_repo: str,
    default_branch: str,
    focus_subdir: str,
    verify_hint: str,
    user: str,
    request_text: str,
    base_sha: str = "",
    had_base_patch: bool = False,
    source_conversation_id: str = "",
) -> str:
    """Build `/state/artifacts/<issue_id>/task_prompt.md` instructions for the worker pod."""
    safe_user = html.escape(user, quote=True)
    heading_kind = (
        "Laptop Session Handoff" if short_id.startswith("HANDOFF-") else "Slack Coding Task"
    )
    lines = [
        f"# {heading_kind} `{short_id}`",
        "",
        f"- **Repository:** `{github_repo}` (base branch `{default_branch}`)",
    ]
    if base_sha:
        lines.append(f"- **Base Commit SHA:** `{base_sha}`")
    if source_conversation_id:
        lines.append(f"- **Source Laptop Conversation ID:** `{source_conversation_id}`")
    if focus_subdir:
        lines.append(f"- **Focus Subdirectory:** `/work/{focus_subdir}`")
    if verify_hint:
        lines.append(f"- **Verification Hint:** `{verify_hint}`")
    if had_base_patch:
        lines.append(
            "- **Pre-Applied Laptop Diff:** Uncommitted working-tree changes from the laptop "
            "session have ALREADY been applied to `/work` via `fix.patch`. Run `git status` and "
            "`git diff HEAD` first to inspect the in-progress work before making further edits."
        )

    verify_step = (
        f"3. Run verification commands in `/work` (`{verify_hint}` and relevant tests) "
        "before concluding."
        if verify_hint
        else "3. Run the repository's linters, typecheckers, and relevant test suite "
        "in `/work` before concluding."
    )

    lines.extend(
        [
            "",
            "## Requested Change",
            "",
            f'<slack_request user="{safe_user}">',
            request_text,
            "</slack_request>",
            "",
            "## Engineering Instructions",
            "",
            (
                "1. Inspect the relevant codebase in `/work` and implement the requested change "
                "with a minimal, surgical, and idiomatic patch."
            ),
            "2. Add or update focused unit/regression tests covering the changed behavior.",
            verify_step,
            (
                "4. Leave all proposed changes uncommitted (or staged) in `/work` — never push "
                "git branches or modify git remotes; the harness captures `git diff HEAD` "
                "automatically."
            ),
            (
                "5. Populate `TriageReport` accurately (`root_cause` summarizing your technical "
                "analysis/approach, `fix_summary` describing the changes applied, "
                "`affected_files`, `verification_commands`, `tests_passed`, `confidence`, "
                "and `risk_notes`)."
            ),
            "",
        ]
    )
    return "\n".join(lines)


class SlackCodingTask:
    """On-demand Slack mobile coding task handler satisfying `LongLivedTask`."""

    name = "slack-coding"

    def __init__(
        self,
        *,
        settings: ControllerSettings,
        state: StateStore,
        heartbeat_cb: Callable[[], None] | None = None,
        dispatch_callback: Callable[[], Awaitable[None]] | None = None,
    ) -> None:
        self.settings = settings
        self.state = state
        self._heartbeat_cb = heartbeat_cb
        self._dispatch_callback = dispatch_callback

    def artifact_dir(self, issue_id: str) -> Path:
        path = self.settings.state_dir / "artifacts" / issue_id
        path.mkdir(parents=True, exist_ok=True)
        return path

    def session_dir(self, issue_id: str) -> Path:
        path = self.settings.state_dir / "sessions" / issue_id
        path.mkdir(parents=True, exist_ok=True)
        return path

    def load_targets(self) -> TargetsConfig:
        if self.settings.targets_config_path.exists():
            return TargetsConfig.from_yaml(self.settings.targets_config_path)
        return TargetsConfig()

    async def handle_coding_request(
        self,
        *,
        prompt_text: str,
        user: str,
        thread_ts: str,
        channel_id: str,
    ) -> str:
        """Validate, enqueue (or queue revision for) an on-demand Slack coding request."""
        targets = self.load_targets()
        slack_cfg = targets.slack_coding
        if not slack_cfg.enabled:
            return "On-demand Slack coding is currently disabled (`targets.slack_coding.enabled=false`)."

        model_override, cleaned = parse_model_override(prompt_text)
        try:
            github_repo, cleaned_prompt = _parse_repo_override(cleaned, slack_cfg.github_repo)
        except ValueError as exc:
            return str(exc)

        if not cleaned_prompt:
            return USAGE_HELP

        existing = self.state.get_by_thread_ts(thread_ts)
        if existing is not None:
            if existing.status == IssueStatus.RUNNING:
                buffered_n = self.state.append_pending_feedback(
                    existing.issue_id,
                    cleaned_prompt,
                    model_override=model_override,
                )
                return (
                    f"A worker Job is already running for `{existing.short_id}` "
                    f"(turn {existing.turn}) — buffered follow-up #{buffered_n} to fold into "
                    f"turn {existing.turn + 1} automatically when it finishes."
                )

            self.state.pop_pending_feedback(existing.issue_id)
            updated = self.state.bump_turn_for_feedback(
                existing.issue_id,
                model_override=model_override,
            )
            art_dir = self.artifact_dir(existing.issue_id)
            (art_dir / f"feedback-turn-{updated.turn}.txt").write_text(
                cleaned_prompt, encoding="utf-8"
            )
            if self._dispatch_callback is not None:
                await self._dispatch_callback()

            model_label = updated.model_override or self.settings.worker_model
            log.info(
                "slack_coding.revision_queued",
                issue_id=updated.issue_id,
                short_id=updated.short_id,
                turn=updated.turn,
                model=model_label,
                user=user,
            )
            return (
                f"Queued revision turn {updated.turn} for `{updated.short_id}` "
                f"(`{model_label}`) on `{updated.github_repo}` — "
                "I'll post the updated diff & Draft PR button in this thread when ready."
            )

        if github_repo == slack_cfg.github_repo:
            default_branch = slack_cfg.default_branch
            focus_subdir = slack_cfg.focus_subdir or ""
            verify_hint = slack_cfg.verify_hint
        else:
            default_branch = "main"
            focus_subdir = ""
            verify_hint = ""

        digest = hashlib.sha1(f"{thread_ts}:{cleaned_prompt}".encode()).hexdigest()[:6].upper()
        issue_id = f"slack-{thread_ts.replace('.', '-')}"
        art_dir = self.artifact_dir(issue_id)

        handoff_meta: dict[str, Any] = {}
        meta_path = art_dir / "handoff_meta.json"
        if meta_path.is_file():
            try:
                raw_meta = json.loads(meta_path.read_text(encoding="utf-8"))
                if isinstance(raw_meta, dict):
                    handoff_meta = raw_meta
            except (OSError, ValueError):
                handoff_meta = {}

        is_handoff = bool(handoff_meta.get("is_handoff"))
        short_id = f"HANDOFF-{digest}" if is_handoff else f"CODE-{digest}"
        branch_override = str(handoff_meta.get("branch") or "").strip()
        if branch_override:
            default_branch = branch_override
        if str(handoff_meta.get("focus_subdir") or "").strip():
            focus_subdir = str(handoff_meta["focus_subdir"]).strip()
        if str(handoff_meta.get("verify_hint") or "").strip():
            verify_hint = str(handoff_meta["verify_hint"]).strip()
        custom_title = str(handoff_meta.get("title") or "").strip()
        handoff_model = str(handoff_meta.get("model") or "").strip()
        effective_model_override = model_override or handoff_model or None
        base_sha = str(handoff_meta.get("base_sha") or "").strip()
        had_base_patch = bool(handoff_meta.get("had_base_patch"))
        source_conv_id = str(handoff_meta.get("source_conversation_id") or "").strip()

        (art_dir / "task_prompt.md").write_text(
            _build_task_prompt(
                short_id=short_id,
                github_repo=github_repo,
                default_branch=default_branch,
                focus_subdir=focus_subdir,
                verify_hint=verify_hint,
                user=user,
                request_text=cleaned_prompt,
                base_sha=base_sha,
                had_base_patch=had_base_patch,
                source_conversation_id=source_conv_id,
            ),
            encoding="utf-8",
        )
        self.session_dir(issue_id)

        first_line = next(
            (ln.strip("#* ") for ln in cleaned_prompt.splitlines() if ln.strip()), cleaned_prompt
        )
        record = self.state.register_if_new(
            issue_id=issue_id,
            short_id=short_id,
            project_slug=github_repo.split("/")[-1],
            github_repo=github_repo,
            default_branch=default_branch,
            title=(custom_title or first_line)[:120],
            permalink=f"https://github.com/{github_repo}",
            task_kind="slack_coding",
            base_ref=branch_override or None,
            model_override=effective_model_override,
            thread_ts=thread_ts,
            channel_id=channel_id,
        )
        if record is None:
            log.warning("slack_coding.duplicate_issue_id", issue_id=issue_id, thread_ts=thread_ts)
            return f"A coding task already exists for `{issue_id}`."

        if self._dispatch_callback is not None:
            await self._dispatch_callback()

        model_label = effective_model_override or self.settings.worker_model
        log.info(
            "slack_coding.queued",
            issue_id=issue_id,
            short_id=short_id,
            github_repo=github_repo,
            model=model_label,
            user=user,
        )
        return (
            f"Queued coding task `{short_id}` (`{model_label}`) for `{github_repo}` "
            "— I'll post the diff & Draft PR button in this thread when ready."
        )

    async def run(self) -> None:
        log.info("slack_coding.loop_started")
        try:
            while True:
                if self._heartbeat_cb is not None:
                    self._heartbeat_cb()
                await asyncio.sleep(60)
        except asyncio.CancelledError:
            log.info("slack_coding.loop_stopped")
            raise
