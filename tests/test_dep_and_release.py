from __future__ import annotations

import asyncio
import subprocess
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from agent_tasks.common.config import ControllerSettings, DepUpgraderConfig, TargetsConfig
from agent_tasks.common.models import (
    ExceptionSummary,
    IssueStatus,
    SanitizedSentryBundle,
    StackFrameSummary,
)
from agent_tasks.common.state import StateStore
from agent_tasks.tasks import LongLivedTask
from agent_tasks.tasks.dep_upgrader import DepUpgraderTask
from agent_tasks.tasks.dep_upgrader import task as dep_task_module
from agent_tasks.tasks.sentry_triage.release_check import (
    ReleaseLagInfo,
    check_sentry_release_lag,
    extract_git_sha,
)


def _git(repo: Path, *args: str) -> str:
    proc = subprocess.run(
        ["git", "-C", str(repo), *args],
        check=True,
        capture_output=True,
        text=True,
    )
    return proc.stdout.strip()


def _init_git_repo(repo: Path) -> None:
    repo.mkdir(parents=True, exist_ok=True)
    subprocess.run(["git", "init", "-b", "main", str(repo)], check=True, capture_output=True)
    _git(repo, "config", "user.name", "Test Author")
    _git(repo, "config", "user.email", "test@example.com")


def _commit_file(repo: Path, rel_path: str, content: str, message: str) -> str:
    target = repo / rel_path
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(content, encoding="utf-8")
    _git(repo, "add", rel_path)
    _git(repo, "commit", "-m", message)
    return _git(repo, "rev-parse", "HEAD")


def _clone_mirror(origin: Path, mirror: Path) -> None:
    mirror.parent.mkdir(parents=True, exist_ok=True)
    subprocess.run(
        ["git", "clone", "--mirror", str(origin), str(mirror)],
        check=True,
        capture_output=True,
    )


def _make_bundle(
    *,
    release: str,
    filenames: list[tuple[str, bool]] | None = None,
) -> SanitizedSentryBundle:
    frames = [
        StackFrameSummary(filename=name, function="parse", lineno=12, in_app=in_app)
        for name, in_app in (filenames or [])
    ]
    exceptions = (
        [ExceptionSummary(exc_type="ValueError", exc_value="bad payload", frames=frames)]
        if frames
        else []
    )
    return SanitizedSentryBundle(
        issue_id="5001",
        short_id="PYTHON-EDGE-50",
        project_slug="python-edge",
        title="ValueError: bad payload",
        release=release,
        exceptions=exceptions,
    )


def _make_settings(tmp_path: Path, targets_path: Path | None = None) -> ControllerSettings:
    return ControllerSettings(
        namespace="agent-tasks",
        node_name="k3s-worker-1",
        state_dir=tmp_path,
        targets_config_path=targets_path or (tmp_path / "targets.yaml"),
        sentry_auth_token="",
        target_github_token="gh-test-token",
        slack_bot_token="xoxb-test",
        slack_app_token="xapp-test",
        slack_channel_id="C001",
    )


def test_extract_git_sha_parses_standard_formats_and_rejects_versions() -> None:
    full_sha = "9ae559106c01b3782150856db78f5f2af32f0475"
    assert extract_git_sha(full_sha) == full_sha
    assert extract_git_sha("9ae5591") == "9ae5591"
    assert extract_git_sha("edge@9AE55910") == "9ae55910"
    assert extract_git_sha("rattlsnek-9ae55910") == "9ae55910"
    assert extract_git_sha("rattlsnek@1.4.0+9ae55910") == "9ae55910"
    assert extract_git_sha("v1.2.3-4-g9ae55910") == "9ae55910"

    assert extract_git_sha("") is None
    assert extract_git_sha("1.2.3") is None
    assert extract_git_sha("v1.2.3") is None
    assert extract_git_sha("edge@1.2.3") is None
    assert extract_git_sha("abc123") is None
    assert extract_git_sha("0" * 41) is None


async def test_check_sentry_release_lag_counts_commits_and_finds_touching_commit(
    tmp_path: Path,
) -> None:
    origin = tmp_path / "origin-work"
    _init_git_repo(origin)

    old_sha = _commit_file(
        origin,
        "edge/src/edge/plugins/mtconnect/parser.py",
        "def parse(xml: str) -> str:\n    return xml\n",
        "feat(mtconnect): initial parser",
    )
    _commit_file(
        origin,
        "edge/src/edge/other.py",
        "VALUE = 1\n",
        "chore: add helper",
    )
    touch_sha = _commit_file(
        origin,
        "edge/src/edge/plugins/mtconnect/parser.py",
        "def parse(xml: str) -> str:\n    return xml.strip()\n",
        "fix(mtconnect): handle missing DeviceStream node",
    )
    head_sha = _commit_file(
        origin,
        "README.md",
        "# rattlsnek\n",
        "docs: update readme",
    )

    mirror = tmp_path / "mirrors" / "lodbrok-app__rattlsnek.git"
    _clone_mirror(origin, mirror)

    bundle = _make_bundle(
        release=f"edge@{old_sha[:8]}",
        filenames=[
            ("/app/edge/src/edge/plugins/mtconnect/parser.py", True),
            ("/usr/lib/python3.12/json/__init__.py", False),
        ],
    )
    info = await check_sentry_release_lag(mirror, bundle, default_branch="main")
    assert isinstance(info, ReleaseLagInfo)
    assert info.release_sha == old_sha[:8]
    assert info.commits_behind == 3
    assert len(info.touching_commits) == 1
    assert touch_sha[:7] in info.touching_commits[0]
    assert "fix(mtconnect): handle missing DeviceStream node" in info.touching_commits[0]
    assert "edge/src/edge/plugins/mtconnect/parser.py" in info.checked_paths
    assert "rattlsnek/edge/src/edge/plugins/mtconnect/parser.py" in info.checked_paths

    banner = info.to_slack_banner()
    assert old_sha[:8] in banner
    assert "3 commits behind" in banner
    assert "modified the implicated file" in banner
    assert "fix(mtconnect): handle missing DeviceStream node" in banner

    prompt_note = info.to_prompt_note()
    assert old_sha[:8] in prompt_note
    assert "3 commits behind" in prompt_note
    assert "already fixed on `main`" in prompt_note
    assert "fix(mtconnect): handle missing DeviceStream node" in prompt_note

    # Up-to-date release returns None
    up_to_date = _make_bundle(
        release=head_sha,
        filenames=[("edge/src/edge/plugins/mtconnect/parser.py", True)],
    )
    assert await check_sentry_release_lag(mirror, up_to_date) is None

    # Non-SHA, unknown SHA, missing mirror, and missing branch return None
    assert await check_sentry_release_lag(mirror, _make_bundle(release="1.2.3")) is None
    assert await check_sentry_release_lag(mirror, _make_bundle(release="deadbeef0000")) is None
    assert await check_sentry_release_lag(tmp_path / "missing.git", bundle) is None
    assert await check_sentry_release_lag(mirror, bundle, default_branch="nope") is None


async def test_check_sentry_release_lag_matches_rattlsnek_subdir_and_empty_frames(
    tmp_path: Path,
) -> None:
    origin = tmp_path / "monorepo"
    _init_git_repo(origin)

    old_sha = _commit_file(
        origin,
        "rattlsnek/edge/src/edge/plugins/mtconnect/parser.py",
        "def parse() -> None:\n    pass\n",
        "feat: initial monorepo parser",
    )
    _commit_file(
        origin,
        "rattlsnek/edge/src/edge/plugins/mtconnect/parser.py",
        "def parse() -> str:\n    return 'ok'\n",
        "fix(edge): guard empty mtconnect stream",
    )

    mirror = tmp_path / "monorepo.git"
    _clone_mirror(origin, mirror)

    # Frame path omits leading `rattlsnek/` (container WORKDIR /app)
    bundle = _make_bundle(
        release=f"rattlsnek-{old_sha}",
        filenames=[("/work/edge/src/edge/plugins/mtconnect/parser.py", True)],
    )
    info = await check_sentry_release_lag(mirror, bundle)
    assert info is not None
    assert info.commits_behind == 1
    assert len(info.touching_commits) == 1
    assert "fix(edge): guard empty mtconnect stream" in info.touching_commits[0]

    # Empty stacktrace still reports lag safely without passing bare `--` to git log
    no_frames_bundle = _make_bundle(release=old_sha, filenames=[])
    lag_only = await check_sentry_release_lag(mirror, no_frames_bundle)
    assert lag_only is not None
    assert lag_only.commits_behind == 1
    assert lag_only.touching_commits == []
    assert lag_only.checked_paths == []
    assert "1 commit behind" in lag_only.to_slack_banner()
    assert "1 commit behind" in lag_only.to_prompt_note()


async def test_dep_upgrader_schedule_persistence_across_restarts(tmp_path: Path) -> None:
    state = StateStore(tmp_path / "state.sqlite3")
    settings = _make_settings(tmp_path)

    upgrader = DepUpgraderTask(settings=settings, state=state)
    # Protocol conformance check
    task_proto: LongLivedTask = upgrader
    assert task_proto.name == "dep-upgrader"

    base_now = datetime(2026, 11, 15, 12, 0, tzinfo=UTC)
    assert upgrader.is_schedule_due(now=base_now) is True

    # Disabled config suppresses scheduled runs
    disabled_targets = TargetsConfig(dep_upgrader=DepUpgraderConfig(enabled=False))
    assert upgrader.is_schedule_due(now=base_now, targets=disabled_targets) is False

    await upgrader.trigger_upgrade_job(scheduled=True)
    last_run_file = tmp_path / "dep_upgrader_last_run.txt"
    assert last_run_file.exists()
    persisted_ts = datetime.fromisoformat(last_run_file.read_text(encoding="utf-8").strip())

    # Simulate controller restart with a fresh DepUpgraderTask instance
    restarted = DepUpgraderTask(settings=settings, state=state)
    assert restarted.is_schedule_due(now=persisted_ts + timedelta(hours=1)) is False
    assert restarted.is_schedule_due(now=persisted_ts + timedelta(days=6, hours=23)) is False
    assert restarted.is_schedule_due(now=persisted_ts + timedelta(days=7)) is True

    # Corrupt file falls back cleanly to due=True
    last_run_file.write_text("not-a-timestamp", encoding="utf-8")
    assert restarted.is_schedule_due(now=persisted_ts + timedelta(hours=1)) is True


async def test_dep_upgrader_trigger_writes_prompt_registers_state_and_dispatches(
    tmp_path: Path,
) -> None:
    state = StateStore(tmp_path / "state.sqlite3")
    settings = _make_settings(tmp_path)

    dispatches = 0

    async def on_dispatch() -> None:
        nonlocal dispatches
        dispatches += 1

    upgrader = DepUpgraderTask(
        settings=settings,
        state=state,
        dispatch_callback=on_dispatch,
    )

    ack_py = await upgrader.trigger_upgrade_job(
        focus="python",
        model_override="gemini-4-carbon",
        thread_ts="1731620000.000100",
        channel_id="C001",
    )
    assert ack_py.startswith("Queued dependency & security audit `DEPS-")
    assert "focus: `python`" in ack_py
    assert dispatches == 1

    queued = state.list_by_status(IssueStatus.QUEUED)
    assert len(queued) == 1
    rec = queued[0]
    assert rec.task_kind == "dep_upgrader"
    assert rec.project_slug == "dependencies"
    assert rec.github_repo == "lodbrok-app/rattlsnek"
    assert rec.default_branch == "main"
    assert rec.model_override == "gemini-4-carbon"
    assert rec.thread_ts == "1731620000.000100"
    assert rec.channel_id == "C001"
    assert rec.issue_id.startswith("deps-")
    assert rec.short_id.startswith("DEPS-")

    prompt_path = tmp_path / "artifacts" / rec.issue_id / "task_prompt.md"
    assert prompt_path.exists()
    prompt_text = prompt_path.read_text(encoding="utf-8")
    assert "rattlsnek/pyproject.toml" in prompt_text
    assert "uv.lock" in prompt_text
    assert "TriageReport" in prompt_text
    assert "focus=python" in prompt_text

    # Triggering again in the same second allocates a unique suffixed issue_id
    ack_web = await upgrader.trigger_upgrade_job(focus="web")
    assert dispatches == 2
    assert "focus: `web`" in ack_web
    queued_all = state.list_by_status(IssueStatus.QUEUED)
    assert len(queued_all) == 2
    assert len({r.issue_id for r in queued_all}) == 2
    second = next(r for r in queued_all if r.issue_id != rec.issue_id)
    web_prompt = (tmp_path / "artifacts" / second.issue_id / "task_prompt.md").read_text(
        encoding="utf-8"
    )
    assert "web/package.json" in web_prompt
    assert "npm audit" in web_prompt


async def test_dep_upgrader_run_loop_heartbeats_triggers_and_cancels_cleanly(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = StateStore(tmp_path / "state.sqlite3")
    settings = _make_settings(tmp_path)

    monkeypatch.setattr(dep_task_module, "STARTUP_DELAY_SECONDS", 0)
    monkeypatch.setattr(dep_task_module, "CHECK_INTERVAL_SECONDS", 0.01)

    heartbeats = 0
    dispatches = 0

    def on_heartbeat() -> None:
        nonlocal heartbeats
        heartbeats += 1

    async def on_dispatch() -> None:
        nonlocal dispatches
        dispatches += 1

    upgrader = DepUpgraderTask(
        settings=settings,
        state=state,
        heartbeat_cb=on_heartbeat,
        dispatch_callback=on_dispatch,
    )

    # First iteration triggers once; subsequent loops skip while not due
    runner = asyncio.create_task(upgrader.run())
    await asyncio.sleep(0.06)
    runner.cancel()
    with pytest.raises(asyncio.CancelledError):
        await runner

    assert heartbeats >= 2
    assert dispatches == 1
    assert len(state.list_by_status(IssueStatus.QUEUED)) == 1


def _clone_origin_remote_only(origin: Path, repo: Path) -> None:
    subprocess.run(
        ["git", "clone", "--bare", str(origin), str(repo)], check=True, capture_output=True
    )
    head_sha = _git(repo, "rev-parse", "refs/heads/main")
    _git(repo, "update-ref", "refs/remotes/origin/main", head_sha)
    _git(repo, "update-ref", "-d", "refs/heads/main")


async def test_check_sentry_release_lag_remote_fallback_and_five_commit_cap(
    tmp_path: Path,
) -> None:
    origin = tmp_path / "origin"
    _init_git_repo(origin)

    base_sha = _commit_file(
        origin,
        "edge/src/edge/plugins/mtconnect/parser.py",
        "v0\n",
        "chore: base",
    )
    for idx in range(1, 8):
        _commit_file(
            origin,
            "edge/src/edge/plugins/mtconnect/parser.py",
            f"v{idx}\n",
            f"fix: update {idx}",
        )

    clone = tmp_path / "remote-only"
    _clone_origin_remote_only(origin, clone)

    bundle = _make_bundle(
        release=base_sha,
        filenames=[("edge/src/edge/plugins/mtconnect/parser.py", True)],
    )
    info = await check_sentry_release_lag(clone, bundle, default_branch="main")
    assert info is not None
    assert info.commits_behind == 7
    assert len(info.touching_commits) == 5
    assert "fix: update 7" in info.touching_commits[0]


async def test_dep_upgrader_run_survives_transient_error_and_reads_targets_yaml(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    targets_file = tmp_path / "targets.yaml"
    targets_file.write_text(
        "dep_upgrader:\n"
        "  enabled: true\n"
        "  interval_seconds: 86400\n"
        "  github_repo: custom-org/custom-repo\n"
        "  default_branch: develop\n"
        "  verify_hint: uv run pytest\n",
        encoding="utf-8",
    )
    state = StateStore(tmp_path / "state.sqlite3")
    settings = _make_settings(tmp_path, targets_path=targets_file)

    monkeypatch.setattr(dep_task_module, "STARTUP_DELAY_SECONDS", 0)
    monkeypatch.setattr(dep_task_module, "CHECK_INTERVAL_SECONDS", 0.01)

    upgrader = DepUpgraderTask(settings=settings, state=state)

    attempts = 0
    original_trigger = upgrader.trigger_upgrade_job

    async def flaky_trigger(*, scheduled: bool = False, **kwargs: object) -> str:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise RuntimeError("transient error")
        return await original_trigger(scheduled=scheduled)

    monkeypatch.setattr(upgrader, "trigger_upgrade_job", flaky_trigger)

    runner = asyncio.create_task(upgrader.run())
    await asyncio.sleep(0.06)
    runner.cancel()
    with pytest.raises(asyncio.CancelledError):
        await runner

    assert attempts == 2
    queued = state.list_by_status(IssueStatus.QUEUED)
    assert len(queued) == 1
    assert queued[0].github_repo == "custom-org/custom-repo"
    assert queued[0].default_branch == "develop"

    # Custom focus directive should appear in prompt
    await original_trigger(focus="pydantic security")
    latest = next(
        r for r in state.list_by_status(IssueStatus.QUEUED) if r.issue_id != queued[0].issue_id
    )
    prompt = (tmp_path / "artifacts" / latest.issue_id / "task_prompt.md").read_text(
        encoding="utf-8"
    )
    assert "Focus directive: `pydantic security`" in prompt
