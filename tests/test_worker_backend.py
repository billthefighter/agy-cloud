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
        resolve_worker_backend(
            targets, task_kind="slack_coding", is_handoff=True, override="sdk"
        )
        == "sdk"
    )


def test_resolve_worker_model_for_cli() -> None:
    assert resolve_worker_model_for_cli("gemini-4-hydrogen") == "gemini-4-hydrogen-medium"
    assert resolve_worker_model_for_cli("gemini-4-neon") == "gemini-4-neon-medium"
    assert resolve_worker_model_for_cli("claude-sonnet-5-5-medium") == "claude-sonnet-5-5-medium"


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

    await svc.create_handoff(
        HandoffCreateRequest(github_repo="org/repo", prompt="Continue work")
    )
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
    subprocess.run(
        ["git", "config", "user.email", "test@example.com"], cwd=work_dir, check=True
    )
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
    subprocess.run(["git", "rm", "-r", "skills/doomloop"], cwd=src_repo, check=True, capture_output=True)
    subprocess.run(["git", "commit", "-m", "rm skill"], cwd=src_repo, check=True, capture_output=True)
    sync_agy_config_repo_sync(agy_auth, str(src_repo), "", branch="main")
    assert not (config_dir / "skills" / "doomloop").exists()
    assert not (config_dir / "skills" / "doomloop").is_symlink()
