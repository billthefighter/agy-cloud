from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from pathlib import Path

import structlog

from agent_tasks.common.config import ControllerSettings, TargetsConfig
from agent_tasks.common.state import StateStore

log = structlog.get_logger(__name__)

STARTUP_DELAY_SECONDS = 30
CHECK_INTERVAL_SECONDS = 3600

_PYTHON_FOCUS_ALIASES = frozenset({"python", "py", "uv", "rattlsnek", "backend", "edge", "api"})
_WEB_FOCUS_ALIASES = frozenset(
    {"web", "node", "npm", "js", "ts", "javascript", "typescript", "astro", "frontend"}
)


def _build_task_prompt(*, focus: str, verify_hint: str, scheduled: bool) -> str:
    focus_clean = focus.strip()
    focus_lower = focus_clean.lower()
    run_kind = "scheduled weekly" if scheduled else "on-demand"

    include_python = not focus_clean or focus_lower in _PYTHON_FOCUS_ALIASES
    include_web = not focus_clean or focus_lower in _WEB_FOCUS_ALIASES
    custom_focus = bool(
        focus_clean
        and focus_lower not in _PYTHON_FOCUS_ALIASES
        and focus_lower not in _WEB_FOCUS_ALIASES
    )

    if custom_focus:
        include_python = True
        include_web = True
        scope_line = (
            f"Focus directive: `{focus_clean}`. Prioritize packages and ecosystems matching "
            f"this focus across Python (`rattlsnek/`) and/or Node (`web/`), avoiding unrelated churn."
        )
    elif include_python and include_web:
        scope_line = (
            "Scope: Audit and safely update BOTH Python (`rattlsnek/`) and Node (`web/`) "
            "dependencies."
        )
    elif include_python:
        scope_line = (
            f"Scope (`focus={focus_clean}`): Audit and safely update Python dependencies in "
            "`rattlsnek/` (`rattlsnek/pyproject.toml` + `rattlsnek/uv.lock`). Leave `web/` "
            "untouched unless explicitly required."
        )
    else:
        scope_line = (
            f"Scope (`focus={focus_clean}`): Audit and safely update Node/Web dependencies in "
            "`web/` (`web/package.json` + `web/package-lock.json`). Leave `rattlsnek/` "
            "untouched unless explicitly required."
        )

    sections: list[str] = [
        f"Run a {run_kind} dependency & security audit in `/work`.",
        scope_line,
        "",
        "Guidelines:",
        "- Prefer security vulnerability fixes and safe compatible patch/minor upgrades.",
        (
            "- Avoid risky unrequested major-version rewrites unless required to fix a CVE or "
            "unblock compatibility; document any held-back major bumps in `risk_notes`."
        ),
        "- Never weaken, delete, or skip tests, lint rules, or type checks to force an upgrade.",
        "- If all in-scope dependencies are already current and clean, leave `/work` unmodified.",
        "",
        "Audit & Update Steps:",
    ]

    step_num = 1
    if include_python:
        sections.extend(
            [
                f"{step_num}. **Python (`rattlsnek/`)**:",
                (
                    "   - Inspect `rattlsnek/pyproject.toml` (plus workspace member "
                    "`pyproject.toml` files if applicable) and `rattlsnek/uv.lock`."
                ),
                (
                    "   - Check outdated or vulnerable Python packages (`cd /work/rattlsnek`, "
                    "`uv lock --upgrade` or targeted `uv lock --upgrade-package <pkg>`, and "
                    "inspect compatible version bounds in `rattlsnek/pyproject.toml`)."
                ),
                (
                    "   - Update `rattlsnek/pyproject.toml` constraints where appropriate and "
                    "refresh `rattlsnek/uv.lock` via `uv lock` / `uv sync`."
                ),
            ]
        )
        step_num += 1

    if include_web:
        sections.extend(
            [
                f"{step_num}. **Node / Web (`web/`)**:",
                (
                    "   - Run `cd /work/web && npm ci --ignore-scripts` and inspect "
                    "`web/package.json`, `npm audit`, and `npm outdated`."
                ),
                (
                    "   - Apply safe security and compatible version updates in `web/package.json` "
                    "and `web/package-lock.json` (using `--ignore-scripts` when running `npm`)."
                ),
            ]
        )
        step_num += 1

    verify_lines = [
        f"{step_num}. **Fix Breakage & Verify**:",
        "   - Resolve any resulting lint, typecheck, or test failures caused by updated packages.",
    ]
    if include_python:
        verify_lines.append(
            "   - Verify Python changes in `/work/rattlsnek` (e.g., `uv sync --package edge "
            "--package edge_utils && uv run ruff check . && uv run ty check && "
            "uv run pytest edge edge_utils`, plus `uv sync --package api --package shared` if "
            "API/shared packages changed)."
        )
    if include_web:
        verify_lines.append(
            "   - Verify Web changes in `/work/web` (`npm test && npm run typecheck` and build "
            "check if configured)."
        )
    if verify_hint:
        verify_lines.append(f"   - Configured verify hint: `{verify_hint}`")
    sections.extend(verify_lines)

    step_num += 1
    sections.extend(
        [
            "",
            f"{step_num}. **Report Format (`TriageReport`)**:",
            (
                "   - `root_cause`: Summary of outdated packages, advisories (`npm audit` / "
                "Python vulnerabilities), and constraints found."
            ),
            (
                "   - `fix_summary`: Explicitly list EVERY bumped package and its version delta "
                "(`package: old_version -> new_version`), along with any code fixes or skipped "
                "majors."
            ),
            (
                "   - `affected_files`: All modified dependency manifests, lockfiles, and "
                "source/test files."
            ),
            (
                "   - `verification_commands` & `tests_passed`: Exact verification commands "
                "executed and their true status."
            ),
            "   - `risk_notes`: Caveats, held-back major versions, or rollout considerations.",
        ]
    )

    return "\n".join(sections)


class DepUpgraderTask:
    """Weekly scheduled and on-demand Slack-triggered dependency & security auditor."""

    name = "dep-upgrader"

    def __init__(
        self,
        settings: ControllerSettings,
        state: StateStore,
        heartbeat_cb: Callable[[], None] | None = None,
        dispatch_callback: Callable[[], Awaitable[None]] | None = None,
    ) -> None:
        self.settings = settings
        self.state = state
        self.heartbeat_cb = heartbeat_cb
        self.dispatch_callback = dispatch_callback
        self._trigger_lock = asyncio.Lock()

    @property
    def last_run_path(self) -> Path:
        return self.settings.state_dir / "dep_upgrader_last_run.txt"

    def load_targets(self) -> TargetsConfig:
        if self.settings.targets_config_path.exists():
            return TargetsConfig.from_yaml(self.settings.targets_config_path)
        return TargetsConfig()

    def get_last_run_at(self) -> datetime | None:
        if not self.last_run_path.exists():
            return None
        raw = self.last_run_path.read_text(encoding="utf-8").strip()
        if not raw:
            return None
        try:
            parsed = datetime.fromisoformat(raw)
        except ValueError:
            log.warning("dep_upgrader.invalid_last_run_file", value=raw)
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        return parsed.astimezone(UTC)

    def _write_last_run_at(self, when: datetime) -> None:
        utc_when = when.replace(tzinfo=UTC) if when.tzinfo is None else when.astimezone(UTC)
        self.last_run_path.parent.mkdir(parents=True, exist_ok=True)
        self.last_run_path.write_text(utc_when.isoformat(), encoding="utf-8")

    def is_schedule_due(
        self,
        now: datetime | None = None,
        targets: TargetsConfig | None = None,
    ) -> bool:
        """Return True if weekly dependency audit is enabled and interval has elapsed."""
        cfg = (targets or self.load_targets()).dep_upgrader
        if not cfg.enabled:
            return False

        last_run = self.get_last_run_at()
        if last_run is None:
            return True

        current = now or datetime.now(UTC)
        if current.tzinfo is None:
            current = current.replace(tzinfo=UTC)
        else:
            current = current.astimezone(UTC)

        elapsed = (current - last_run).total_seconds()
        return elapsed >= cfg.interval_seconds

    def _allocate_unique_issue_id(self, now: datetime) -> str:
        base_id = f"deps-{now.strftime('%Y%m%d-%H%M%S')}"
        candidate = base_id
        suffix = 2
        while (
            self.state.get_by_issue_id(candidate) is not None
            or (self.settings.state_dir / "artifacts" / candidate).exists()
        ):
            candidate = f"{base_id}-{suffix}"
            suffix += 1
        return candidate

    async def trigger_upgrade_job(
        self,
        *,
        focus: str = "",
        model_override: str | None = None,
        thread_ts: str | None = None,
        channel_id: str | None = None,
        scheduled: bool = False,
    ) -> str:
        """Queue a dependency & security audit job in `StateStore` and invoke dispatch."""
        async with self._trigger_lock:
            now = datetime.now(UTC)
            targets = self.load_targets()
            dep_cfg = targets.dep_upgrader

            issue_id = self._allocate_unique_issue_id(now)
            short_id = f"DEPS-{now.strftime('%Y%m%d')}"
            focus_clean = focus.strip()

            art_dir = self.settings.state_dir / "artifacts" / issue_id
            art_dir.mkdir(parents=True, exist_ok=True)
            (self.settings.state_dir / "sessions" / issue_id).mkdir(parents=True, exist_ok=True)

            prompt_text = _build_task_prompt(
                focus=focus_clean,
                verify_hint=dep_cfg.verify_hint,
                scheduled=scheduled,
            )
            (art_dir / "task_prompt.md").write_text(prompt_text, encoding="utf-8")

            kind_label = "Weekly" if scheduled else "On-demand"
            focus_title = f" ({focus_clean})" if focus_clean else ""
            title = f"{kind_label} dependency & security audit{focus_title}"

            record = self.state.register_if_new(
                issue_id=issue_id,
                short_id=short_id,
                project_slug="dependencies",
                github_repo=dep_cfg.github_repo,
                default_branch=dep_cfg.default_branch,
                title=title,
                permalink="",
                task_kind="dep_upgrader",
                model_override=model_override,
                thread_ts=thread_ts,
                channel_id=channel_id,
            )
            if record is None:
                raise RuntimeError(f"Failed to register unique dep_upgrader issue_id={issue_id}")

            self._write_last_run_at(now)
            log.info(
                "dep_upgrader.job_queued",
                issue_id=issue_id,
                short_id=short_id,
                scheduled=scheduled,
                focus=focus_clean or "all",
                model_override=model_override,
            )

        if self.dispatch_callback is not None:
            await self.dispatch_callback()

        focus_note = f" (focus: `{focus_clean}`)" if focus_clean else ""
        model_note = f" using `{model_override}`" if model_override else ""
        return (
            f"Queued dependency & security audit `{short_id}` (`{issue_id}`) "
            f"for `{dep_cfg.github_repo}`{focus_note}{model_note}."
        )

    async def run(self) -> None:
        log.info("dep_upgrader.loop_started")
        try:
            await asyncio.sleep(STARTUP_DELAY_SECONDS)
            while True:
                if self.heartbeat_cb is not None:
                    self.heartbeat_cb()
                try:
                    targets = self.load_targets()
                    if self.is_schedule_due(targets=targets):
                        await self.trigger_upgrade_job(scheduled=True)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    log.exception("dep_upgrader.iteration_failed")
                await asyncio.sleep(CHECK_INTERVAL_SECONDS)
        except asyncio.CancelledError:
            log.info("dep_upgrader.loop_stopped")
            raise
