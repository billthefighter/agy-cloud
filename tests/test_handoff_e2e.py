from __future__ import annotations

import os
import stat
import subprocess
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

from agent_tasks.common.config import ControllerSettings, ProjectTargetConfig
from agent_tasks.common.models import IssueStatus, WorkerEnvelope
from agent_tasks.common.state import StateStore
from agent_tasks.handoff_cli import (
    _push_handoff,
    apply_remote_handoff_patch,
)
from agent_tasks.pwa.gateway import PwaGatewayAdapter, create_pwa_app
from agent_tasks.tasks.sentry_triage.k8s_jobs import build_triage_job_manifest
from agent_tasks.tasks.slack_coding.task import SlackCodingTask
from agent_tasks.worker_main import run_worker


class _TestClientHandoffHttpAdapter:
    """Adapter that routes `handoff_cli` HTTP calls directly into FastAPI's `TestClient`."""

    def __init__(self, client: TestClient, api_token: str) -> None:
        self.base_url = "http://testserver/agy"
        self._client = client
        self._headers = {
            "Authorization": f"Bearer {api_token}",
            "X-AGY-Client": "pwa",
        }

    def create_handoff(self, payload: dict[str, Any]) -> dict[str, Any]:
        resp = self._client.post("/agy/api/handoff", json=payload, headers=self._headers)
        assert resp.status_code == 200, f"POST /agy/api/handoff failed: {resp.status_code} {resp.text}"
        return resp.json()

    def get_handoff(
        self, handoff_id: str, *, repo: str = "", include_diff: bool = True
    ) -> dict[str, Any]:
        resp = self._client.get(
            f"/agy/api/handoff/{handoff_id}",
            params={"repo": repo, "include_diff": "true" if include_diff else "false"},
            headers=self._headers,
        )
        assert resp.status_code == 200, f"GET /agy/api/handoff failed: {resp.status_code} {resp.text}"
        return resp.json()


def _git(cwd: Path, *args: str) -> str:
    proc = subprocess.run(
        ["git", "-C", str(cwd), *args],
        check=True,
        capture_output=True,
        text=True,
    )
    return proc.stdout.strip()


def _setup_origin_and_laptop(tmp_path: Path) -> tuple[Path, Path, str, str]:
    """Create a bare origin mirror + laptop clone with an unpushed branch & uncommitted WIP."""
    origin_mirror = tmp_path / "origin.git"
    subprocess.run(
        ["git", "init", "--bare", "-b", "main", str(origin_mirror)],
        check=True,
        capture_output=True,
    )

    seed = tmp_path / "seed"
    subprocess.run(["git", "clone", str(origin_mirror), str(seed)], check=True, capture_output=True)
    _git(seed, "config", "user.name", "Test User")
    _git(seed, "config", "user.email", "test@example.com")
    (seed / "app.py").write_text("VALUE = 1\n", encoding="utf-8")
    _git(seed, "add", ".")
    _git(seed, "commit", "-m", "initial main")
    _git(seed, "push", "-u", "origin", "main")
    main_sha = _git(seed, "rev-parse", "HEAD")

    laptop = tmp_path / "laptop"
    subprocess.run(
        ["git", "clone", str(origin_mirror), str(laptop)], check=True, capture_output=True
    )
    _git(laptop, "config", "user.name", "Test User")
    _git(laptop, "config", "user.email", "test@example.com")
    _git(laptop, "checkout", "-b", "burn_down_xfails")
    (laptop / "app.py").write_text("VALUE = 2\n", encoding="utf-8")
    _git(laptop, "add", "app.py")
    _git(laptop, "commit", "-m", "unpushed commit 1")
    (laptop / "analyzer.py").write_text("def analyze():\n    return 'v1'\n", encoding="utf-8")
    _git(laptop, "add", "analyzer.py")
    _git(laptop, "commit", "-m", "unpushed commit 2")
    local_head_sha = _git(laptop, "rev-parse", "HEAD")

    # Uncommitted WIP on top of unpushed branch
    (laptop / "wip_notes.txt").write_text("laptop wip\n", encoding="utf-8")
    return origin_mirror, laptop, main_sha, local_head_sha


def _run_k8s_init_script(
    *,
    init_script: str,
    init_env: dict[str, str],
    mirror_dir: Path,
    work_dir: Path,
    artifacts_dir: Path,
) -> subprocess.CompletedProcess[str]:
    """Execute the actual K8s `git-clone` initContainer bash script against local paths."""
    localized_script = (
        init_script.replace("/mirror", str(mirror_dir))
        .replace("/work", str(work_dir))
        .replace("/artifacts", str(artifacts_dir))
    )
    env = {**os.environ, **init_env}
    return subprocess.run(
        ["/bin/bash", "-lc", localized_script],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )


@pytest.mark.asyncio
async def test_handoff_e2e_unpushed_branch_model_effort_init_script_worker_and_pull(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Full end-to-end handoff flow:
    CLI push (unpushed branch + model/effort) -> FastAPI /api/handoff -> SlackCodingTask
    -> K8s initContainer bash script -> worker_main.run_worker (agy_cli)
    -> FastAPI GET /api/handoff -> laptop detached-worktree rebase & patch apply.
    """
    origin_mirror, laptop, main_sha, local_head_sha = _setup_origin_and_laptop(tmp_path)

    state_dir = tmp_path / "k3s_state"
    state_dir.mkdir(parents=True, exist_ok=True)
    api_token = "e2e-test-token"
    settings = ControllerSettings(
        state_dir=state_dir,
        targets_config_path=tmp_path / "missing-targets.yaml",
        target_github_token="ghp_test",
        pwa_api_token=api_token,
        pwa_mirror_to_slack=False,
    )
    state = StateStore(state_dir / "state.db")
    slack_coding = SlackCodingTask(settings=settings, state=state)

    async def _on_coding_request(prompt: str, user: str, ts: str, chan: str) -> str:
        return await slack_coding.handle_coding_request(
            prompt_text=prompt,
            user=user,
            thread_ts=ts,
            channel_id=chan,
        )

    gateway = PwaGatewayAdapter(
        settings=settings,
        state=state,
        push_manager=MagicMock(),
        direct_chat=MagicMock(),
        on_coding_request=_on_coding_request,
    )
    app = create_pwa_app(gateway)
    test_client = TestClient(app)
    http_adapter = _TestClientHandoffHttpAdapter(test_client, api_token)

    # 1. Laptop pushes handoff from unpushed branch with explicit model + effort
    created = _push_handoff(
        client=http_adapter,  # type: ignore[arg-type]
        repo_dir=laptop,
        prompt="Optimize analyzer.py to return v2 and add test_analyzer.py",
        repo_override="lodbrok-app/rattlsnek",
        model="gemini-4-neon",
        effort="high",
        verify_hint="pytest",
        title="Optimize analyzer",
        source_conversation_id="conv-laptop-e2e",
    )
    issue_id = created["issue_id"]
    short_id = created["short_id"]
    assert created["branch"] == "main"
    assert created["base_sha"] == main_sha
    assert created["had_base_patch"] is True
    assert created["model"] == "gemini-4-neon-high"

    rec = state.get_by_issue_id(issue_id)
    assert rec is not None
    assert rec.model_override == "gemini-4-neon-high"
    assert rec.base_ref == "main"

    # 2. Build real K8s Job manifest and execute the actual git-clone initContainer script
    target = ProjectTargetConfig(github_repo="lodbrok-app/rattlsnek", verify_hint="pytest")
    job = build_triage_job_manifest(
        settings=settings,
        record=rec,
        target=target,
        worker_backend="agy_cli",
    )
    init_c = job.spec.template.spec.init_containers[0]
    worker_c = job.spec.template.spec.containers[0]
    init_env = {e.name: e.value for e in init_c.env}
    worker_env = {e.name: e.value for e in worker_c.env}

    assert init_env["GIT_CONFIG_VALUE_0"] == "*"
    assert worker_env["GIT_CONFIG_VALUE_0"] == "*"
    assert worker_env["WORKER_MODEL"] == "gemini-4-neon-high"
    assert worker_env["WORKER_BACKEND"] == "agy_cli"

    pod_work = tmp_path / "pod_work"
    pod_session = state_dir / "sessions" / issue_id
    pod_artifacts = state_dir / "artifacts" / issue_id
    pod_memory = tmp_path / "pod_memory"
    pod_memory.mkdir()

    init_res = _run_k8s_init_script(
        init_script=init_c.command[2],
        init_env=init_env,
        mirror_dir=origin_mirror,
        work_dir=pod_work,
        artifacts_dir=pod_artifacts,
    )
    assert init_res.returncode == 0, f"initContainer failed: {init_res.stderr}\n{init_res.stdout}"
    # Verify unpushed commits + uncommitted WIP are both materialized in /work!
    assert (pod_work / "app.py").read_text(encoding="utf-8") == "VALUE = 2\n"
    assert "return 'v1'" in (pod_work / "analyzer.py").read_text(encoding="utf-8")
    assert (pod_work / "wip_notes.txt").read_text(encoding="utf-8") == "laptop wip\n"

    # 3. Execute real `run_worker()` with fake `agy` CLI verifying exact `--model` + `/work` state
    fake_agy = tmp_path / "fake-agy-e2e"
    fake_agy.write_text(
        """#!/usr/bin/env python3
import json, sys
from pathlib import Path

assert "--model" in sys.argv
model = sys.argv[sys.argv.index("--model") + 1]
assert model == "gemini-4-neon-high", f"expected gemini-4-neon-high, got {model}"
assert Path("analyzer.py").is_file(), "unpushed branch file missing from /work"
assert Path("wip_notes.txt").is_file(), "uncommitted WIP file missing from /work"

Path("analyzer.py").write_text("def analyze():\\n    return 'v2'\\n", encoding="utf-8")
Path("test_analyzer.py").write_text(
    "from analyzer import analyze\\n\\ndef test_analyze():\\n    assert analyze() == 'v2'\\n",
    encoding="utf-8",
)
print(json.dumps({
    "conversation_id": "conv-pod-e2e-1",
    "status": "SUCCESS",
    "structured_output": {
        "root_cause": "Optimized analyzer to v2",
        "confidence": "high",
        "affected_files": ["analyzer.py", "test_analyzer.py"],
        "fix_summary": "Updated analyzer.py to return v2 and added regression test.",
        "verification_commands": ["pytest"],
        "tests_passed": True,
        "risk_notes": "",
        "should_notify": True,
    },
    "usage": {
        "input_tokens": 900,
        "output_tokens": 200,
        "thinking_tokens": 100,
        "cache_read_tokens": 0,
        "total_tokens": 1200,
    },
}))
""",
        encoding="utf-8",
    )
    fake_agy.chmod(fake_agy.stat().st_mode | stat.S_IEXEC)

    for k, v in worker_env.items():
        if v is not None:
            monkeypatch.setenv(k, v)
    monkeypatch.setenv("WORK_DIR", str(pod_work))
    monkeypatch.setenv("SESSION_DIR", str(pod_session))
    monkeypatch.setenv("ARTIFACTS_DIR", str(pod_artifacts))
    monkeypatch.setenv("MEMORY_DIR", str(pod_memory))
    monkeypatch.setenv("AGY_CLI_BIN", str(fake_agy))

    await run_worker()

    envelope = WorkerEnvelope.model_validate_json(
        (pod_artifacts / "result.json").read_text(encoding="utf-8")
    )
    assert envelope.error is None
    assert envelope.has_patch is True
    assert envelope.model == "gemini-4-neon-high"
    assert envelope.report is not None
    assert envelope.report.tests_passed is True

    # Mark awaiting_review like orchestrator does, then pull via real HTTP gateway
    state.mark_status(issue_id, IssueStatus.AWAITING_REVIEW)
    pulled = http_adapter.get_handoff(short_id, repo="lodbrok-app/rattlsnek", include_diff=True)
    assert pulled["is_terminal"] is True
    assert pulled["has_patch"] is True
    assert pulled["base_sha"] == main_sha

    # 4. Apply remote patch back onto laptop (unpushed commits remain intact!)
    apply_res = apply_remote_handoff_patch(
        laptop,
        remote_diff=pulled["diff"],
        handoff_id=short_id,
        remote_base_sha=pulled["base_sha"],
    )
    assert apply_res["applied"] is True
    assert apply_res["rebased_from_ancestor"] is True
    assert _git(laptop, "rev-parse", "HEAD") == local_head_sha
    assert (laptop / "app.py").read_text(encoding="utf-8") == "VALUE = 2\n"
    assert "return 'v2'" in (laptop / "analyzer.py").read_text(encoding="utf-8")
    assert (laptop / "wip_notes.txt").read_text(encoding="utf-8") == "laptop wip\n"
    assert (laptop / "test_analyzer.py").is_file()


def test_init_script_fails_fast_when_base_sha_missing_from_mirror(tmp_path: Path) -> None:
    """Verify the K8s initContainer script exits non-zero if BASE_SHA is missing from /mirror."""
    origin_mirror, _laptop, _main_sha, local_head_sha = _setup_origin_and_laptop(tmp_path)

    settings = ControllerSettings(namespace="agent-tasks", target_github_token="ghp_test")
    state = StateStore(tmp_path / "state.db")
    rec = state.register_if_new(
        issue_id="slack-missing-sha",
        short_id="HANDOFF-MISS",
        project_slug="rattlsnek",
        github_repo="lodbrok-app/rattlsnek",
        default_branch="main",
        base_ref="main",
        title="Missing SHA test",
        permalink="https://github.com/lodbrok-app/rattlsnek",
        task_kind="slack_coding",
    )
    assert rec is not None
    job = build_triage_job_manifest(
        settings=settings,
        record=rec,
        target=ProjectTargetConfig(github_repo="lodbrok-app/rattlsnek"),
        worker_backend="agy_cli",
    )
    init_c = job.spec.template.spec.init_containers[0]
    init_env = {e.name: e.value for e in init_c.env}

    artifacts_dir = tmp_path / "artifacts"
    artifacts_dir.mkdir()
    # Write unpushed SHA that is NOT in origin_mirror
    (artifacts_dir / "base_sha.txt").write_text(f"{local_head_sha}\n", encoding="utf-8")

    res = _run_k8s_init_script(
        init_script=init_c.command[2],
        init_env=init_env,
        mirror_dir=origin_mirror,
        work_dir=tmp_path / "pod_work",
        artifacts_dir=artifacts_dir,
    )
    assert res.returncode == 1
    assert "ERROR: BASE_SHA" in res.stderr
    assert local_head_sha in res.stderr


def test_workspace_handoff_wrapper_imports_agy_cloud_cli() -> None:
    """Verify `.agents/skills/handoff/scripts/handoff.py` resolves `gitops/apps/agy-cloud/src`."""
    wrapper = (
        Path(__file__).resolve().parents[4]
        / ".agents"
        / "skills"
        / "handoff"
        / "scripts"
        / "handoff.py"
    )
    if not wrapper.is_file():
        pytest.skip("workspace wrapper not present outside parent repo")
    proc = subprocess.run(
        ["python3", str(wrapper), "run", "--help"],
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 0, proc.stderr
    assert "--effort" in proc.stdout
    assert "--model" in proc.stdout
