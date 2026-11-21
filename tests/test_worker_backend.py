from __future__ import annotations

import stat
import subprocess
from pathlib import Path

import pytest

from agent_tasks.common.config import (
    ControllerSettings,
    ProjectTargetConfig,
    TargetsConfig,
    resolve_worker_backend,
    resolve_worker_model_for_cli,
    resolve_worker_model_for_sdk,
)
from agent_tasks.common.models import IssueStatus, WorkerEnvelope
from agent_tasks.common.state import IssueRecord
from agent_tasks.tasks.sentry_triage.git_ops import sync_agy_config_repo_sync
from agent_tasks.tasks.sentry_triage.k8s_jobs import build_triage_job_manifest
from agent_tasks.worker_main import run_worker
from agy_cloud.core.gateway import MemoryChannelBus, RecordingWorkerJobService
from agy_cloud.core.handoff import HandoffCreateRequest, HandoffService, HandoffStore
from agy_cloud.core.workers import build_k8s_worker_job_dict
from agy_cloud.sdk.spec import JobSpec, WorkerRunnerConfig


def test_resolve_worker_backend_defaults_and_overrides() -> None:
    targets = TargetsConfig()
    # Handoff defaults to agy_cli (Google account subscription)
    assert resolve_worker_backend(targets, task_kind="slack_coding", is_handoff=True) == "agy_cli"
    # Background plugins default to sdk (API key)
    assert resolve_worker_backend(targets, task_kind="sentry") == "sdk"
    assert resolve_worker_backend(targets, task_kind="slack_coding") == "sdk"
    assert resolve_worker_backend(targets, task_kind="ci_healer") == "sdk"
    assert resolve_worker_backend(targets, task_kind="k8s_watchdog") == "sdk"
    assert resolve_worker_backend(targets, task_kind="dep_upgrader") == "sdk"

    # Per-plugin switchable overrides
    targets.sentry.worker_backend = "agy_cli"
    targets.ci_healer.worker_backend = "agy_cli"
    assert resolve_worker_backend(targets, task_kind="sentry") == "agy_cli"
    assert resolve_worker_backend(targets, task_kind="ci_healer") == "agy_cli"
    # Explicit per-run override beats config
    assert (
        resolve_worker_backend(targets, task_kind="slack_coding", is_handoff=True, override="sdk")
        == "sdk"
    )


def test_resolve_worker_model_for_cli() -> None:
    assert resolve_worker_model_for_cli("gemini-4-hydrogen") == "gemini-4-hydrogen-medium"
    assert resolve_worker_model_for_cli("gemini-4-neon") == "gemini-4-neon-medium"
    assert resolve_worker_model_for_cli("neon") == "gemini-4-neon-medium"
    assert resolve_worker_model_for_cli("neon-high") == "gemini-4-neon-high"
    assert resolve_worker_model_for_cli("gemini-4-neon-high") == "gemini-4-neon-high"
    assert resolve_worker_model_for_cli("claude-sonnet-5-5-medium") == "claude-sonnet-5-5-medium"
    # SDK strips effort tiers on Gemini models so effort-qualified handoffs still work on sdk
    assert resolve_worker_model_for_sdk("gemini-4-neon-high") == "gemini-4-neon"
    assert resolve_worker_model_for_sdk("neon-low") == "gemini-4-neon"
    assert resolve_worker_model_for_sdk("gemini-4-hydrogen") == "gemini-4-hydrogen"


def test_k8s_job_manifest_mounts_agy_auth_for_agy_cli() -> None:
    settings = ControllerSettings(namespace="agent-tasks", target_github_token="ghp_test")
    record = IssueRecord(
        issue_id="slack-123",
        short_id="HANDOFF-1",
        project_slug="rattlsnek",
        github_repo="lodbrok-app/rattlsnek",
        default_branch="main",
        title="Handoff test",
        permalink="https://github.com/lodbrok-app/rattlsnek",
        status=IssueStatus.QUEUED,
        turn=1,
        conversation_id=None,
        thread_ts=None,
        channel_id=None,
        pr_url=None,
        last_error=None,
        updated_at="2026-11-18T00:00:00Z",
        task_kind="slack_coding",
    )
    target = ProjectTargetConfig(github_repo="lodbrok-app/rattlsnek")

    cli_job = build_triage_job_manifest(
        settings=settings, record=record, target=target, worker_backend="agy_cli"
    )
    worker = cli_job.spec.template.spec.containers[0]
    env_map = {e.name: e.value for e in worker.env}
    assert env_map["WORKER_BACKEND"] == "agy_cli"
    assert env_map["HOME"] == "/home/agy"
    assert env_map["TZ"] == "UTC"
    assert env_map["GIT_CONFIG_COUNT"] == "1"
    assert env_map["GIT_CONFIG_KEY_0"] == "safe.directory"
    assert env_map["GIT_CONFIG_VALUE_0"] == "*"
    init_c = cli_job.spec.template.spec.init_containers[0]
    init_env = {e.name: e.value for e in init_c.env}
    assert init_env["GIT_CONFIG_VALUE_0"] == "*"
    assert "exit 1" in init_c.command[2]
    assert worker.env_from[0].secret_ref.optional is True
    assert any(
        m.mount_path == "/home/agy/.gemini" and m.sub_path == "agy-auth"
        for m in worker.volume_mounts
    )

    sdk_job = build_triage_job_manifest(
        settings=settings, record=record, target=target, worker_backend="sdk"
    )
    sdk_worker = sdk_job.spec.template.spec.containers[0]
    sdk_env = {e.name: e.value for e in sdk_worker.env}
    assert sdk_env["WORKER_BACKEND"] == "sdk"
    assert sdk_worker.env_from[0].secret_ref.optional is False
    assert not any(m.sub_path == "agy-auth" for m in sdk_worker.volume_mounts)


def test_microkernel_worker_job_dict_and_handoff_default_agy_cli(tmp_path: Path) -> None:
    cfg = WorkerRunnerConfig(mode="k8s")
    spec = JobSpec(
        task_id="handoff-1",
        short_id="HO-1",
        plugin_id="core.handoff",
        github_repo="org/repo",
        prompt="Fix bug",
        worker_backend="agy_cli",
    )
    manifest = build_k8s_worker_job_dict(spec, cfg)
    container = manifest["spec"]["template"]["spec"]["containers"][0]
    env_map = {e["name"]: e["value"] for e in container["env"]}
    assert env_map["WORKER_BACKEND"] == "agy_cli"
    assert env_map["GIT_CONFIG_COUNT"] == "1"
    assert env_map["GIT_CONFIG_KEY_0"] == "safe.directory"
    assert env_map["GIT_CONFIG_VALUE_0"] == "*"
    assert container["envFrom"][0]["secretRef"]["optional"] is True
    assert any(
        m.get("subPath") == "agy-auth" and m.get("mountPath") == "/home/agy/.gemini"
        for m in container["volumeMounts"]
    )


@pytest.mark.asyncio
async def test_handoff_service_defaults_to_agy_cli(tmp_path: Path) -> None:
    store = HandoffStore(tmp_path / "handoffs.sqlite3")
    jobs = RecordingWorkerJobService()
    channels = MemoryChannelBus()
    svc = HandoffService(store=store, jobs=jobs, channels=channels, state_root=tmp_path)

    await svc.create_handoff(HandoffCreateRequest(github_repo="org/repo", prompt="Continue work"))
    assert len(jobs.spawned_jobs) == 1
    assert jobs.spawned_jobs[0].worker_backend == "agy_cli"


@pytest.mark.asyncio
async def test_run_worker_with_agy_cli_backend(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    work_dir = tmp_path / "work"
    session_dir = tmp_path / "session"
    artifacts_dir = tmp_path / "artifacts"
    memory_dir = tmp_path / "memory"
    work_dir.mkdir()
    artifacts_dir.mkdir()
    memory_dir.mkdir()

    # Initialize a tiny git repo in work_dir
    subprocess.run(["git", "init"], cwd=work_dir, check=True, capture_output=True)
    subprocess.run(["git", "config", "user.email", "test@example.com"], cwd=work_dir, check=True)
    subprocess.run(["git", "config", "user.name", "Test"], cwd=work_dir, check=True)
    (work_dir / "hello.txt").write_text("before\n", encoding="utf-8")
    subprocess.run(["git", "add", "."], cwd=work_dir, check=True)
    subprocess.run(["git", "commit", "-m", "init"], cwd=work_dir, check=True, capture_output=True)

    (artifacts_dir / "task_prompt.md").write_text("Update hello.txt", encoding="utf-8")

    # Fake agy CLI script that verifies API keys were stripped and modifies hello.txt
    fake_agy = tmp_path / "fake-agy"
    fake_agy.write_text(
        """#!/usr/bin/env python3
import json, os, sys
from pathlib import Path

assert "GEMINI_API_KEY" not in os.environ, "GEMINI_API_KEY must be stripped for agy_cli"
assert "--model" in sys.argv
model = sys.argv[sys.argv.index("--model") + 1]
assert model == "gemini-4-hydrogen-medium", f"unexpected model: {model}"

Path("hello.txt").write_text("after\\n", encoding="utf-8")
print(json.dumps({
    "conversation_id": "conv-agy-123",
    "status": "SUCCESS",
    "structured_output": {
        "root_cause": "Outdated greeting",
        "confidence": "high",
        "affected_files": ["hello.txt"],
        "fix_summary": "Updated hello.txt to after",
        "verification_commands": ["pytest"],
        "tests_passed": True,
        "risk_notes": "",
        "should_notify": True,
    },
    "usage": {
        "input_tokens": 1200,
        "output_tokens": 150,
        "thinking_tokens": 50,
        "cache_read_tokens": 200,
        "total_tokens": 1400,
    },
}))
""",
        encoding="utf-8",
    )
    fake_agy.chmod(fake_agy.stat().st_mode | stat.S_IEXEC)

    monkeypatch.setenv("ISSUE_ID", "handoff-test-1")
    monkeypatch.setenv("SHORT_ID", "HANDOFF-1")
    monkeypatch.setenv("PROJECT_SLUG", "demo")
    monkeypatch.setenv("TURN", "1")
    monkeypatch.setenv("WORK_DIR", str(work_dir))
    monkeypatch.setenv("SESSION_DIR", str(session_dir))
    monkeypatch.setenv("ARTIFACTS_DIR", str(artifacts_dir))
    monkeypatch.setenv("MEMORY_DIR", str(memory_dir))
    monkeypatch.setenv("WORKER_BACKEND", "agy_cli")
    monkeypatch.setenv("WORKER_MODEL", "gemini-4-hydrogen")
    monkeypatch.setenv("AGY_CLI_BIN", str(fake_agy))
    monkeypatch.setenv("GEMINI_API_KEY", "should-be-stripped")

    await run_worker()

    envelope = WorkerEnvelope.model_validate_json(
        (artifacts_dir / "result.json").read_text(encoding="utf-8")
    )
    assert envelope.worker_backend == "agy_cli"
    assert envelope.conversation_id == "conv-agy-123"
    assert envelope.model == "gemini-4-hydrogen-medium"
    assert envelope.has_patch is True
    assert envelope.report is not None
    assert envelope.report.tests_passed is True
    assert envelope.usage.total_tokens == 1400
    patch_text = (artifacts_dir / "fix.patch").read_text(encoding="utf-8")
    assert "+after" in patch_text


@pytest.mark.asyncio
async def test_run_worker_with_agy_cli_stream_json_records_completed_steps(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import json

    work_dir = tmp_path / "work"
    session_dir = tmp_path / "session"
    artifacts_dir = tmp_path / "artifacts"
    memory_dir = tmp_path / "memory"
    work_dir.mkdir()
    artifacts_dir.mkdir()
    memory_dir.mkdir()

    subprocess.run(["git", "init"], cwd=work_dir, check=True, capture_output=True)
    subprocess.run(["git", "config", "user.email", "test@example.com"], cwd=work_dir, check=True)
    subprocess.run(["git", "config", "user.name", "Test"], cwd=work_dir, check=True)
    (work_dir / "hello.txt").write_text("before\n", encoding="utf-8")
    subprocess.run(["git", "add", "."], cwd=work_dir, check=True)
    subprocess.run(["git", "commit", "-m", "init"], cwd=work_dir, check=True, capture_output=True)
    (artifacts_dir / "task_prompt.md").write_text("Update hello.txt", encoding="utf-8")

    fake_agy = tmp_path / "fake-agy-stream"
    fake_agy.write_text(
        """#!/usr/bin/env python3
import json, sys
from pathlib import Path

assert "--output-format" in sys.argv
assert sys.argv[sys.argv.index("--output-format") + 1] == "stream-json"

Path("hello.txt").write_text("after\\n", encoding="utf-8")
events = [
    {"event": "init", "conversation_id": "conv-stream-1", "init": {"model": "gemini-4-hydrogen-medium"}},
    {"event": "step_update", "step_update": {"step_index": 0, "state": "DONE", "step_type": "user_input"}},
    {"event": "step_update", "step_update": {"step_index": 1, "state": "ACTIVE", "step_type": "agent_response", "text_delta": "Inspecting "}},
    {"event": "step_update", "step_update": {"step_index": 1, "state": "DONE", "step_type": "agent_response", "text_delta": "repo.", "duration_seconds": 1.2}},
    {"event": "step_update", "step_update": {"step_index": 2, "state": "ACTIVE", "step_type": "agent_response", "text_delta": "Still working"}},
    {"event": "result", "result": {
        "conversation_id": "conv-stream-1",
        "status": "SUCCESS",
        "structured_output": {
            "root_cause": "Streamed fix",
            "confidence": "high",
            "affected_files": ["hello.txt"],
            "fix_summary": "Updated hello.txt",
            "verification_commands": ["pytest"],
            "tests_passed": True,
            "risk_notes": "",
            "should_notify": True,
        },
        "usage": {"input_tokens": 100, "output_tokens": 20, "thinking_tokens": 5, "cache_read_tokens": 0, "total_tokens": 120},
    }},
]
for ev in events:
    print(json.dumps(ev), flush=True)
""",
        encoding="utf-8",
    )
    fake_agy.chmod(fake_agy.stat().st_mode | stat.S_IEXEC)

    monkeypatch.setenv("ISSUE_ID", "handoff-stream-1")
    monkeypatch.setenv("SHORT_ID", "HANDOFF-S1")
    monkeypatch.setenv("PROJECT_SLUG", "demo")
    monkeypatch.setenv("TURN", "1")
    monkeypatch.setenv("WORK_DIR", str(work_dir))
    monkeypatch.setenv("SESSION_DIR", str(session_dir))
    monkeypatch.setenv("ARTIFACTS_DIR", str(artifacts_dir))
    monkeypatch.setenv("MEMORY_DIR", str(memory_dir))
    monkeypatch.setenv("WORKER_BACKEND", "agy_cli")
    monkeypatch.setenv("WORKER_MODEL", "gemini-4-hydrogen")
    monkeypatch.setenv("AGY_CLI_BIN", str(fake_agy))

    await run_worker()

    progress_lines = [
        json.loads(line)
        for line in (artifacts_dir / "progress.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert len(progress_lines) == 1
    assert progress_lines[0]["step_index"] == 1
    assert progress_lines[0]["step_type"] == "agent_response"
    assert progress_lines[0]["text"] == "Inspecting repo."
    assert progress_lines[0]["duration_seconds"] == 1.2

    envelope = WorkerEnvelope.model_validate_json(
        (artifacts_dir / "result.json").read_text(encoding="utf-8")
    )
    assert envelope.conversation_id == "conv-stream-1"
    assert envelope.has_patch is True


def test_sync_agy_config_repo_creates_relative_symlinks(tmp_path: Path) -> None:
    import os

    src_repo = tmp_path / "src_agy_config"
    (src_repo / "hooks").mkdir(parents=True)
    (src_repo / "hooks" / "runner.py").write_text("# runner\n", encoding="utf-8")
    (src_repo / "hooks.json").write_text('{"agy-global-hooks": {}}\n', encoding="utf-8")
    (src_repo / "skills" / "doomloop").mkdir(parents=True)
    (src_repo / "skills" / "doomloop" / "SKILL.md").write_text("# doomloop\n", encoding="utf-8")
    (src_repo / "rules").mkdir(parents=True)
    (src_repo / "rules" / "stream_safety.md").write_text("# rules\n", encoding="utf-8")

    subprocess.run(["git", "init", "-b", "main"], cwd=src_repo, check=True, capture_output=True)
    subprocess.run(["git", "config", "user.email", "t@example.com"], cwd=src_repo, check=True)
    subprocess.run(["git", "config", "user.name", "T"], cwd=src_repo, check=True)
    subprocess.run(["git", "add", "."], cwd=src_repo, check=True)
    subprocess.run(["git", "commit", "-m", "init"], cwd=src_repo, check=True, capture_output=True)

    agy_auth = tmp_path / "agy-auth"
    config_dir = sync_agy_config_repo_sync(agy_auth, str(src_repo), "", branch="main")

    assert (config_dir / "hooks.json").is_symlink()
    assert os.readlink(config_dir / "hooks.json") == "../agy-config/hooks.json"
    assert (config_dir / "hooks").is_symlink()
    assert os.readlink(config_dir / "hooks") == "../agy-config/hooks"
    assert os.readlink(config_dir / "skills" / "doomloop") == "../../agy-config/skills/doomloop"
    assert (
        os.readlink(config_dir / "rules" / "stream_safety.md")
        == "../../agy-config/rules/stream_safety.md"
    )
    assert (agy_auth / "antigravity" / "hook_state").is_dir()

    # Re-sync idempotently after removing a skill
    subprocess.run(
        ["git", "rm", "-r", "skills/doomloop"], cwd=src_repo, check=True, capture_output=True
    )
    subprocess.run(
        ["git", "commit", "-m", "rm skill"], cwd=src_repo, check=True, capture_output=True
    )
    sync_agy_config_repo_sync(agy_auth, str(src_repo), "", branch="main")
    assert not (config_dir / "skills" / "doomloop").exists()
    assert not (config_dir / "skills" / "doomloop").is_symlink()


@pytest.mark.asyncio
async def test_run_worker_preserves_report_and_usage_when_git_diff_capture_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    work_dir = tmp_path / "work"
    session_dir = tmp_path / "session"
    artifacts_dir = tmp_path / "artifacts"
    memory_dir = tmp_path / "memory"
    work_dir.mkdir()
    artifacts_dir.mkdir()
    memory_dir.mkdir()
    # Deliberately do NOT init a git repo in work_dir so _capture_git_diff fails
    (artifacts_dir / "task_prompt.md").write_text("Diagnose missing repo", encoding="utf-8")

    fake_agy = tmp_path / "fake-agy"
    fake_agy.write_text(
        """#!/usr/bin/env python3
import json
print(json.dumps({
    "conversation_id": "conv-preserve-1",
    "status": "SUCCESS",
    "structured_output": {
        "root_cause": "Missing base commit in workspace",
        "confidence": "high",
        "affected_files": [],
        "fix_summary": "No changes applied.",
        "verification_commands": [],
        "tests_passed": False,
        "risk_notes": "Re-dispatch with pushed branch.",
        "should_notify": True,
    },
    "usage": {
        "input_tokens": 500,
        "output_tokens": 100,
        "thinking_tokens": 25,
        "cache_read_tokens": 0,
        "total_tokens": 625,
    },
}))
""",
        encoding="utf-8",
    )
    fake_agy.chmod(fake_agy.stat().st_mode | stat.S_IEXEC)

    monkeypatch.setenv("ISSUE_ID", "handoff-preserve-1")
    monkeypatch.setenv("SHORT_ID", "HANDOFF-PRESERVE")
    monkeypatch.setenv("PROJECT_SLUG", "demo")
    monkeypatch.setenv("TURN", "1")
    monkeypatch.setenv("WORK_DIR", str(work_dir))
    monkeypatch.setenv("SESSION_DIR", str(session_dir))
    monkeypatch.setenv("ARTIFACTS_DIR", str(artifacts_dir))
    monkeypatch.setenv("MEMORY_DIR", str(memory_dir))
    monkeypatch.setenv("WORKER_BACKEND", "agy_cli")
    monkeypatch.setenv("WORKER_MODEL", "gemini-4-hydrogen")
    monkeypatch.setenv("AGY_CLI_BIN", str(fake_agy))

    with pytest.raises(RuntimeError, match="git diff capture failed"):
        await run_worker()

    envelope = WorkerEnvelope.model_validate_json(
        (artifacts_dir / "result.json").read_text(encoding="utf-8")
    )
    assert envelope.error is not None
    assert "git diff capture failed" in envelope.error
    assert envelope.conversation_id == "conv-preserve-1"
    assert envelope.report is not None
    assert "Missing base commit" in envelope.report.root_cause
    assert envelope.usage.total_tokens == 625


@pytest.mark.asyncio
async def test_slack_coding_handoff_preserves_model_and_effort_from_handoff_meta(
    tmp_path: Path,
) -> None:
    import json

    from agent_tasks.common.state import StateStore
    from agent_tasks.tasks.slack_coding.task import SlackCodingTask

    state = StateStore(tmp_path / "state.db")
    settings = ControllerSettings(
        state_dir=tmp_path,
        targets_config_path=tmp_path / "missing-targets.yaml",
        target_github_token="ghp_test",
    )
    task = SlackCodingTask(settings=settings, state=state)

    thread_ts = "1795060000.123456"
    issue_id = f"slack-{thread_ts.replace('.', '-')}"
    art_dir = tmp_path / "artifacts" / issue_id
    art_dir.mkdir(parents=True, exist_ok=True)
    (art_dir / "handoff_meta.json").write_text(
        json.dumps(
            {
                "is_handoff": True,
                "repo": "lodbrok-app/rattlsnek",
                "branch": "main",
                "model": "gemini-4-neon-high",
            }
        ),
        encoding="utf-8",
    )

    ack = await task.handle_coding_request(
        prompt_text="repo:lodbrok-app/rattlsnek Finish burn down",
        user="laptop-handoff",
        thread_ts=thread_ts,
        channel_id="pwa:coding",
    )
    assert "gemini-4-neon-high" in ack
    rec = state.get_by_thread_ts(thread_ts)
    assert rec is not None
    assert rec.model_override == "gemini-4-neon-high"


@pytest.mark.asyncio
async def test_agy_cli_timeout_cleans_stderr_task_and_preserves_conv_and_partial_patch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Regression test for HANDOFF-87C982: timeout must raise TimeoutError (not RuntimeError
    from concurrent stderr read), preserve streamed conversation_id, and capture partial diff."""
    work_dir = tmp_path / "work"
    session_dir = tmp_path / "session"
    artifacts_dir = tmp_path / "artifacts"
    memory_dir = tmp_path / "memory"
    work_dir.mkdir()
    artifacts_dir.mkdir()
    memory_dir.mkdir()

    subprocess.run(["git", "init"], cwd=work_dir, check=True, capture_output=True)
    subprocess.run(["git", "config", "user.email", "test@example.com"], cwd=work_dir, check=True)
    subprocess.run(["git", "config", "user.name", "Test"], cwd=work_dir, check=True)
    (work_dir / "app.py").write_text("x = 1\n", encoding="utf-8")
    subprocess.run(["git", "add", "."], cwd=work_dir, check=True)
    subprocess.run(["git", "commit", "-m", "init"], cwd=work_dir, check=True, capture_output=True)

    (artifacts_dir / "task_prompt.md").write_text("Long task", encoding="utf-8")

    fake_agy = tmp_path / "fake-agy-hang"
    fake_agy.write_text(
        """#!/usr/bin/env python3
import json, sys, time
from pathlib import Path

print(json.dumps({"event": "init", "conversation_id": "conv-timeout-42"}), flush=True)
Path("app.py").write_text("x = 2\\n", encoding="utf-8")
print("stderr progress line", file=sys.stderr, flush=True)
time.sleep(30)
""",
        encoding="utf-8",
    )
    fake_agy.chmod(fake_agy.stat().st_mode | stat.S_IEXEC)

    monkeypatch.setenv("ISSUE_ID", "handoff-timeout-1")
    monkeypatch.setenv("SHORT_ID", "HANDOFF-TIMEOUT")
    monkeypatch.setenv("PROJECT_SLUG", "sdf_cam")
    monkeypatch.setenv("TURN", "1")
    monkeypatch.setenv("WORK_DIR", str(work_dir))
    monkeypatch.setenv("SESSION_DIR", str(session_dir))
    monkeypatch.setenv("ARTIFACTS_DIR", str(artifacts_dir))
    monkeypatch.setenv("MEMORY_DIR", str(memory_dir))
    monkeypatch.setenv("WORKER_BACKEND", "agy_cli")
    monkeypatch.setenv("WORKER_MODEL", "gemini-4-neon-medium")
    monkeypatch.setenv("WORKER_TIMEOUT_SECONDS", "1")
    monkeypatch.setenv("AGY_CLI_BIN", str(fake_agy))

    with pytest.raises(TimeoutError, match="timed out after 1s"):
        await run_worker()

    envelope = WorkerEnvelope.model_validate_json(
        (artifacts_dir / "result.json").read_text(encoding="utf-8")
    )
    assert envelope.error is not None
    assert envelope.error.startswith("TimeoutError:")
    assert "already waiting for incoming data" not in envelope.error
    assert envelope.conversation_id == "conv-timeout-42"
    assert envelope.has_patch is True
    assert "app.py" in envelope.patch_stats
    patch_text = (artifacts_dir / "fix.patch").read_text(encoding="utf-8")
    assert "+x = 2" in patch_text


def test_capture_git_diff_excludes_pycache_pyc_and_pytest_cache(tmp_path: Path) -> None:
    """Regression test for HANDOFF-6C5CEB: both untracked and tracked .pyc/cache files must
    be excluded from captured worker patches."""
    from agent_tasks.worker_main import _capture_git_diff

    subprocess.run(["git", "init"], cwd=tmp_path, check=True, capture_output=True)
    subprocess.run(["git", "config", "user.email", "test@example.com"], cwd=tmp_path, check=True)
    subprocess.run(["git", "config", "user.name", "Test"], cwd=tmp_path, check=True)

    (tmp_path / "pkg").mkdir()
    (tmp_path / "pkg" / "mod.py").write_text("v = 1\n", encoding="utf-8")
    (tmp_path / "pkg" / "__pycache__").mkdir()
    (tmp_path / "pkg" / "__pycache__" / "mod.cpython-314.pyc").write_bytes(b"\x00\x01old")
    subprocess.run(["git", "add", "."], cwd=tmp_path, check=True)
    subprocess.run(["git", "commit", "-m", "init"], cwd=tmp_path, check=True, capture_output=True)

    # Modify real source, modify tracked .pyc, and create untracked .pyc + .pytest_cache
    (tmp_path / "pkg" / "mod.py").write_text("v = 2\n", encoding="utf-8")
    (tmp_path / "pkg" / "__pycache__" / "mod.cpython-314.pyc").write_bytes(b"\x00\x02new")
    (tmp_path / "pkg" / "__pycache__" / "mod.cpython-312.pyc").write_bytes(b"\x00\x03added")
    (tmp_path / ".pytest_cache").mkdir()
    (tmp_path / ".pytest_cache" / "README.md").write_text("cache\n", encoding="utf-8")

    diff_text, stat_text = _capture_git_diff(tmp_path)
    assert "pkg/mod.py" in diff_text
    assert "+v = 2" in diff_text
    assert "__pycache__" not in diff_text
    assert ".pyc" not in diff_text
    assert ".pytest_cache" not in diff_text
    assert "__pycache__" not in stat_text
    assert "pkg/mod.py" in stat_text


def test_k8s_job_manifest_wires_worker_timeout_and_dontwritebytecode() -> None:
    settings = ControllerSettings(
        namespace="agent-tasks",
        target_github_token="ghp_test",
        job_active_deadline_seconds=3600,
    )
    record = IssueRecord(
        issue_id="slack-456",
        short_id="HANDOFF-2",
        project_slug="sdf_cam",
        github_repo="billthefighter/sdf_cam",
        default_branch="main",
        title="Long build",
        permalink="https://github.com/billthefighter/sdf_cam",
        status=IssueStatus.QUEUED,
        turn=1,
        conversation_id=None,
        thread_ts=None,
        channel_id=None,
        pr_url=None,
        last_error=None,
        updated_at="2026-11-21T00:00:00Z",
        task_kind="slack_coding",
    )
    target = ProjectTargetConfig(github_repo="billthefighter/sdf_cam")

    job = build_triage_job_manifest(
        settings=settings, record=record, target=target, worker_backend="agy_cli"
    )
    worker = job.spec.template.spec.containers[0]
    env_map = {e.name: e.value for e in worker.env}
    assert job.spec.active_deadline_seconds == 3600
    assert env_map["WORKER_TIMEOUT_SECONDS"] == "3420"
    assert env_map["PYTHONDONTWRITEBYTECODE"] == "1"


@pytest.mark.asyncio
async def test_reconcile_surfaces_stop_reason_and_preserves_conversation_id(
    tmp_path: Path,
) -> None:
    """Regression test for CI-44874125707: when job succeeds with report=None and
    stop_reason=MAX_MODEL_CALLS_EXCEEDED, error message must name stop_reason and
    preserve conversation_id."""
    from unittest.mock import AsyncMock, MagicMock

    from agent_tasks.common.state import StateStore
    from agent_tasks.tasks.sentry_triage.orchestrator import SentryTriageTask

    state = StateStore(tmp_path / "state.sqlite3")
    settings = ControllerSettings(
        state_dir=tmp_path,
        targets_config_path=tmp_path / "missing.yaml",
        target_github_token="ghp_test",
    )
    channel = MagicMock()
    channel.resolve_channel_for_task.return_value = "C_CI"
    channel.post_thread_message = AsyncMock(return_value="ts-1")
    job_runner = AsyncMock()
    job_runner.get_job_terminal_state = AsyncMock(return_value="succeeded")

    orch = SentryTriageTask(
        settings=settings,
        state=state,
        channel=channel,
        job_runner=job_runner,
        heartbeat_cb=lambda: None,
    )
    rec = state.register_if_new(
        issue_id="ci-44874125707",
        short_id="CI-44874125707",
        project_slug="rattlsnek",
        github_repo="lodbrok-app/rattlsnek",
        default_branch="main",
        title="Verify Changes failed",
        permalink="https://github.com/lodbrok-app/rattlsnek/actions/runs/44874125707",
        task_kind="ci_healer",
    )
    assert rec is not None
    state.mark_status(rec.issue_id, IssueStatus.RUNNING, thread_ts="1795242078.265219")

    art_dir = tmp_path / "artifacts" / rec.issue_id
    art_dir.mkdir(parents=True, exist_ok=True)
    envelope = WorkerEnvelope(
        issue_id=rec.issue_id,
        turn=1,
        conversation_id="conv-ci-max-calls",
        model="gemini-4-hydrogen",
        worker_backend="sdk",
        stop_reason="MAX_MODEL_CALLS_EXCEEDED",
        has_patch=True,
        patch_stats="1 file changed",
        report=None,
        error=None,
    )
    (art_dir / "result.json").write_text(envelope.model_dump_json(), encoding="utf-8")

    await orch._reconcile_running_jobs()

    updated = state.get_by_issue_id(rec.issue_id)
    assert updated is not None
    assert updated.status == IssueStatus.FAILED
    assert updated.conversation_id == "conv-ci-max-calls"
    assert updated.last_error is not None
    assert "MAX_MODEL_CALLS_EXCEEDED" in updated.last_error
    assert "status=succeeded" not in updated.last_error


def test_migration_syncs_existing_handoff_status_and_ignores_unchanged_initial_patch(
    tmp_path: Path,
) -> None:
    """Regression test for stale 'running' handoffs and false has_result_patch=True
    when fix.patch still equals initial.patch or result.json says has_patch=False."""
    import json

    from agent_tasks.common.state import StateStore
    from agy_cloud.core.migration import migrate_agent_tasks_pvc

    state = StateStore(tmp_path / "state.sqlite3")
    rec1 = state.register_if_new(
        issue_id="slack-stale-run",
        short_id="HANDOFF-STALE",
        project_slug="rattlsnek",
        github_repo="lodbrok-app/rattlsnek",
        default_branch="main",
        title="Stale running test",
        permalink="",
        task_kind="slack_coding",
    )
    assert rec1 is not None
    state.mark_status(rec1.issue_id, IssueStatus.RUNNING)

    # First migration imports it as running
    migrate_agent_tasks_pvc(tmp_path)
    hstore = HandoffStore(tmp_path / "handoffs.sqlite3")
    imported = hstore.get(rec1.issue_id)
    assert imported is not None
    assert imported.status == "running"

    # Now issue fails in state.sqlite3 AND has initial.patch == fix.patch + result.json has_patch=False
    state.mark_status(
        rec1.issue_id,
        IssueStatus.FAILED,
        conversation_id="conv-resumable",
        last_error="TimeoutError: agy CLI timed out after 1620s",
    )
    art_dir = tmp_path / "artifacts" / rec1.issue_id
    art_dir.mkdir(parents=True, exist_ok=True)
    (art_dir / "initial.patch").write_text("diff --git a/a b/a\n+laptop\n", encoding="utf-8")
    (art_dir / "fix.patch").write_text("diff --git a/a b/a\n+laptop\n", encoding="utf-8")
    (art_dir / "result.json").write_text(
        json.dumps({"has_patch": False, "error": "TimeoutError"}), encoding="utf-8"
    )

    # Second migration must sync status, last_error, conversation_id, and has_result_patch=False
    migrate_agent_tasks_pvc(tmp_path)
    synced = hstore.get(rec1.issue_id)
    assert synced is not None
    assert synced.status == "failed"
    assert synced.remote_conversation_id == "conv-resumable"
    assert synced.last_error == "TimeoutError: agy CLI timed out after 1620s"
    assert synced.has_result_patch is False
