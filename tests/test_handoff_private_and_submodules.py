from __future__ import annotations

import base64
import io
import os
import subprocess
import tarfile
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi.testclient import TestClient

from agent_tasks.common.config import (
    ControllerSettings,
    ProjectTargetConfig,
    parse_github_tokens_from_env,
)
from agent_tasks.common.models import IssueStatus
from agent_tasks.common.state import StateStore
from agent_tasks.pwa.gateway import PwaGatewayAdapter, create_pwa_app
from agent_tasks.tasks.sentry_triage.git_ops import (
    RepoSnapshotTooLargeError,
    validate_and_write_repo_snapshot,
)
from agent_tasks.tasks.sentry_triage.k8s_jobs import build_triage_job_manifest
from agent_tasks.tasks.sentry_triage.orchestrator import SentryTriageTask
from agent_tasks.tasks.slack_coding.task import SlackCodingTask
from agent_tasks.worker_main import (
    _build_venv_env_overrides,
    _capture_git_diff,
    _prepare_project_venv,
)
from agy_cloud.client import handoff_client


def _git(cwd: Path, *args: str, allow_file_protocol: bool = False) -> str:
    cmd = ["git"]
    if allow_file_protocol:
        cmd.extend(["-c", "protocol.file.allow=always"])
    cmd.extend(["-C", str(cwd), *args])
    proc = subprocess.run(cmd, check=True, capture_output=True, text=True)
    return proc.stdout.strip()


def _init_repo(repo_dir: Path, *, remote_url: str = "") -> str:
    repo_dir.mkdir(parents=True, exist_ok=True)
    _git(repo_dir, "init", "-b", "main")
    _git(repo_dir, "config", "user.name", "Test User")
    _git(repo_dir, "config", "user.email", "test@example.com")
    if remote_url:
        _git(repo_dir, "remote", "add", "origin", remote_url)
    (repo_dir / "app.py").write_text("VALUE = 1\n", encoding="utf-8")
    _git(repo_dir, "add", ".")
    _git(repo_dir, "commit", "-m", "initial")
    return _git(repo_dir, "rev-parse", "HEAD")


def test_parse_and_resolve_per_owner_and_per_repo_github_tokens(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    env = {
        "TARGET_GITHUB_TOKEN": "ghp_default",
        "GITHUB_TOKEN_BILLTHEFIGHTER": "ghp_personal",
        "GITHUB_TOKEN_LODBROK_APP": "ghp_lodbrok",
        "GITHUB_TOKENS_JSON": (
            '{"billthefighter/agy-cloud-private-plugins": "github_pat_private_plugins"}'
        ),
    }
    parsed = parse_github_tokens_from_env(env)
    assert parsed["billthefighter"] == "ghp_personal"
    assert parsed["lodbrok-app"] == "ghp_lodbrok"
    assert parsed["lodbrok_app"] == "ghp_lodbrok"
    assert parsed["billthefighter/agy-cloud-private-plugins"] == "github_pat_private_plugins"

    settings = ControllerSettings(
        target_github_token="ghp_default",
        github_tokens=parsed,
    )
    assert (
        settings.resolve_github_token("billthefighter/agy-cloud-private-plugins")
        == "github_pat_private_plugins"
    )
    assert settings.resolve_github_token("BILLTHEFIGHTER/k3s-proxmox-terraform") == "ghp_personal"
    assert settings.resolve_github_token("lodbrok-app/rattlsnek") == "ghp_lodbrok"
    assert settings.resolve_github_token("other-org/other-repo") == "ghp_default"

    # ControllerSettings.from_env picks up per-owner tokens even without Slack
    monkeypatch.setenv("SLACK_ENABLED", "false")
    monkeypatch.setenv("TARGET_GITHUB_TOKEN", "ghp_default")
    monkeypatch.setenv("GITHUB_TOKEN_BILLTHEFIGHTER", "ghp_personal_env")
    monkeypatch.delenv("GITHUB_TOKENS_JSON", raising=False)
    loaded = ControllerSettings.from_env()
    assert loaded.resolve_github_token("billthefighter/private-repo") == "ghp_personal_env"
    assert loaded.resolve_github_token("someone/else") == "ghp_default"


def test_validate_and_write_repo_snapshot_rejects_unsafe_archives(tmp_path: Path) -> None:
    dest = tmp_path / "repo_snapshot.tar.gz"

    # Traversal member
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tf:
        info = tarfile.TarInfo(name="../escape.txt")
        data = b"boom"
        info.size = len(data)
        tf.addfile(info, io.BytesIO(data))
    bad_b64 = base64.b64encode(buf.getvalue()).decode("ascii")
    with pytest.raises(ValueError, match="Unsafe path"):
        validate_and_write_repo_snapshot(bad_b64, dest)

    # Escaping symlink
    buf2 = io.BytesIO()
    with tarfile.open(fileobj=buf2, mode="w:gz") as tf:
        info = tarfile.TarInfo(name="sub/link")
        info.type = tarfile.SYMTYPE
        info.linkname = "../../etc/passwd"
        tf.addfile(info)
    bad_link_b64 = base64.b64encode(buf2.getvalue()).decode("ascii")
    with pytest.raises(ValueError, match="Escaping link"):
        validate_and_write_repo_snapshot(bad_link_b64, dest)

    # Size limit
    buf3 = io.BytesIO()
    with tarfile.open(fileobj=buf3, mode="w:gz") as tf:
        info = tarfile.TarInfo(name="file.txt")
        payload = b"x" * 2048
        info.size = len(payload)
        tf.addfile(info, io.BytesIO(payload))
    with pytest.raises(RepoSnapshotTooLargeError):
        validate_and_write_repo_snapshot(
            base64.b64encode(buf3.getvalue()).decode("ascii"),
            dest,
            max_bytes=32,
        )

    assert not dest.exists()


@pytest.mark.asyncio
async def test_snapshot_handoff_e2e_skips_mirror_and_applies_patch_cleanly(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """End-to-end test for private/local repo handoff via on-the-fly HEAD snapshot upload."""
    repo_dir = tmp_path / "private_plugins"
    head_sha = _init_repo(
        repo_dir, remote_url="https://github.com/billthefighter/agy-cloud-private-plugins.git"
    )
    (repo_dir / "plugins").mkdir()
    (repo_dir / "plugins" / "health.py").write_text(
        "def score():\n    return 1\n", encoding="utf-8"
    )
    _git(repo_dir, "add", ".")
    _git(repo_dir, "commit", "-m", "add health plugin")
    head_sha = _git(repo_dir, "rev-parse", "HEAD")

    # Local uncommitted WIP on laptop
    (repo_dir / "plugins" / "health.py").write_text(
        "def score():\n    return 2  # laptop wip\n", encoding="utf-8"
    )
    _git(repo_dir, "add", "-N", ".")
    initial_patch = _git(repo_dir, "diff", "--binary", "HEAD")
    snapshot_b64, snap_bytes = handoff_client.create_repo_snapshot_b64(repo_dir, ref="HEAD")
    assert snap_bytes > 0

    state_dir = tmp_path / "state"
    targets_yaml = tmp_path / "targets.yaml"
    targets_yaml.write_text(
        "slack_coding:\n  enabled: true\n  github_repo: lodbrok-app/rattlsnek\n",
        encoding="utf-8",
    )
    settings = ControllerSettings(
        state_dir=state_dir,
        targets_config_path=targets_yaml,
        target_github_token="ghp_only_for_lodbrok",
        slack_enabled=False,
        pwa_api_token="test-token",
    )
    store = StateStore(state_dir / "state.db")
    coding_task = SlackCodingTask(settings=settings, state=store)

    gateway = PwaGatewayAdapter(
        settings=settings,
        state=store,
        push_manager=MagicMock(),
        direct_chat=MagicMock(),
        on_coding_request=lambda prompt, user, ts, ch: coding_task.handle_coding_request(
            prompt_text=prompt, user=user, thread_ts=ts, channel_id=ch
        ),
    )
    app = create_pwa_app(gateway)
    client = TestClient(app)
    headers = {"Authorization": "Bearer test-token", "X-AGY-Client": "pwa"}

    # Preflight repo-access returns accessible=False for local/* repos
    access_resp = client.get("/agy/api/handoff/repo-access?repo=local/demo", headers=headers)
    assert access_resp.status_code == 200
    assert access_resp.json()["accessible"] is False

    # Create handoff with uploaded HEAD snapshot + initial WIP patch
    create_resp = client.post(
        "/agy/api/handoff",
        headers=headers,
        json={
            "repo": "billthefighter/agy-cloud-private-plugins",
            "branch": "main",
            "base_sha": head_sha,
            "patch": initial_patch,
            "repo_snapshot_b64": snapshot_b64,
            "prompt": "Update plugins/health.py to return 42",
        },
    )
    assert create_resp.status_code == 200, create_resp.text
    created = create_resp.json()
    assert created["has_repo_snapshot"] is True
    issue_id = created["handoff_id"]

    # Orchestrator dispatch must NOT call sync_bare_mirror when repo_snapshot.tar.gz is present
    mirror_spy = AsyncMock(side_effect=AssertionError("sync_bare_mirror should not be called!"))
    monkeypatch.setattr("agent_tasks.tasks.sentry_triage.orchestrator.sync_bare_mirror", mirror_spy)
    spawned_calls: list[dict[str, Any]] = []

    class _StubJobRunner:
        async def spawn_job(
            self,
            record: Any,
            target: ProjectTargetConfig,
            *,
            worker_backend: str = "sdk",
            use_repo_snapshot: bool = False,
        ) -> str:
            spawned_calls.append(
                {
                    "record": record,
                    "target": target,
                    "worker_backend": worker_backend,
                    "use_repo_snapshot": use_repo_snapshot,
                }
            )
            return "job-1"

    orchestrator = SentryTriageTask(
        settings=settings,
        state=store,
        channel=MagicMock(),
        job_runner=_StubJobRunner(),  # type: ignore[arg-type]
        heartbeat_cb=lambda: None,
    )
    await orchestrator._dispatch_queued_jobs(orchestrator.load_targets())
    assert mirror_spy.await_count == 0
    assert len(spawned_calls) == 1
    assert spawned_calls[0]["use_repo_snapshot"] is True

    # Build real K8s Job manifest and verify /mirror is NOT mounted when use_repo_snapshot=True
    record = store.get_by_issue_id(issue_id)
    assert record is not None
    assert record.status == IssueStatus.RUNNING
    manifest = build_triage_job_manifest(
        settings=settings,
        record=record,
        target=spawned_calls[0]["target"],
        worker_backend="agy_cli",
        use_repo_snapshot=True,
    )
    init_container = manifest.spec.template.spec.init_containers[0]
    mount_paths = [vm.mount_path for vm in init_container.volume_mounts]
    assert "/mirror" not in mount_paths
    assert "/artifacts" in mount_paths
    assert "/work" in mount_paths

    # Execute the actual K8s initContainer bash script locally against /work and /artifacts
    art_dir = state_dir / "artifacts" / issue_id
    work_dir = tmp_path / "pod_work"
    init_script = init_container.command[2]
    init_env = {e.name: e.value for e in init_container.env}
    localized = init_script.replace("/work", str(work_dir)).replace("/artifacts", str(art_dir))
    proc = subprocess.run(
        ["/bin/bash", "-lc", localized],
        env={**os.environ, **init_env},
        check=False,
        capture_output=True,
        text=True,
    )
    assert proc.returncode == 0, f"init script failed:\nstdout={proc.stdout}\nstderr={proc.stderr}"
    assert "laptop wip" in (work_dir / "plugins" / "health.py").read_text(encoding="utf-8")

    # Worker completes the edit in /work and captures git diff HEAD
    (work_dir / "plugins" / "health.py").write_text(
        "def score():\n    return 42  # completed on k3s\n", encoding="utf-8"
    )
    (work_dir / "plugins" / "autofill.py").write_text("AUTOFILL = True\n", encoding="utf-8")
    remote_patch, _ = _capture_git_diff(work_dir)
    assert "completed on k3s" in remote_patch
    assert "AUTOFILL = True" in remote_patch

    # Laptop applies remote patch cleanly back onto repo_dir
    ok, msg = handoff_client.apply_remote_patch_safely(
        repo_dir, patch_text=remote_patch, expected_base_sha=head_sha
    )
    assert ok, msg
    assert "completed on k3s" in (repo_dir / "plugins" / "health.py").read_text(encoding="utf-8")
    assert (repo_dir / "plugins" / "autofill.py").read_text(encoding="utf-8") == "AUTOFILL = True\n"


def test_submodule_auto_retargeting_and_git_file_state_handling(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Reproduces the 76a368a2 scenario: parent repo + private submodule with `.git` file."""
    sub_origin = tmp_path / "sub_origin.git"
    subprocess.run(
        ["git", "init", "--bare", "-b", "main", str(sub_origin)], check=True, capture_output=True
    )

    sub_seed = tmp_path / "sub_seed"
    _init_repo(sub_seed)
    _git(sub_seed, "remote", "add", "origin", str(sub_origin))
    _git(sub_seed, "push", "-u", "origin", "main")

    parent = tmp_path / "k3s-proxmox-terraform"
    _init_repo(parent, remote_url="https://github.com/billthefighter/k3s-proxmox-terraform.git")
    _git(
        parent,
        "submodule",
        "add",
        str(sub_origin),
        "gitops/apps/agy-cloud-private-plugins",
        allow_file_protocol=True,
    )
    _git(parent, "commit", "-m", "add private plugins submodule")

    sub_dir = parent / "gitops" / "apps" / "agy-cloud-private-plugins"
    assert (sub_dir / ".git").is_file()  # submodule `.git` is a file, not a directory!
    _git(
        sub_dir,
        "remote",
        "set-url",
        "origin",
        "https://github.com/billthefighter/agy-cloud-private-plugins.git",
    )

    # Parent .env.makesense should be discovered even when workdir is inside the submodule
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.delenv("AGY_HANDOFF_URL", raising=False)
    monkeypatch.delenv("AGY_HANDOFF_TOKEN", raising=False)
    monkeypatch.delenv("PWA_API_TOKEN", raising=False)
    monkeypatch.delenv("NETBIRD_HOSTNAME", raising=False)
    (parent / ".env.makesense").write_text(
        'NETBIRD_HOSTNAME="makesense-k3s"\nPWA_API_TOKEN="tok-parent"\n',
        encoding="utf-8",
    )
    url, token, _ = handoff_client.resolve_connection_config(sub_dir)
    assert url == "https://makesense-k3s.netbird.cloud/agy"
    assert token == "tok-parent"

    # Prompt referencing the submodule retargets automatically & rewrites verify_hint
    eff_dir, eff_focus, eff_verify, sub_rel = handoff_client.resolve_effective_workdir(
        parent,
        prompt="Update gitops/apps/agy-cloud-private-plugins to add autofill",
        verify_hint="pytest gitops/apps/agy-cloud-private-plugins/tests/",
    )
    assert eff_dir == sub_dir.resolve()
    assert eff_focus == ""
    assert eff_verify == "pytest tests/"
    assert sub_rel == "gitops/apps/agy-cloud-private-plugins"

    # Explicit --focus-subdir inside the submodule also retargets and preserves inner subpath
    (sub_dir / "plugins" / "health").mkdir(parents=True)
    eff_dir2, eff_focus2, _, sub_rel2 = handoff_client.resolve_effective_workdir(
        parent,
        focus_subdir="gitops/apps/agy-cloud-private-plugins/plugins/health",
    )
    assert eff_dir2 == sub_dir.resolve()
    assert eff_focus2 == "plugins/health"
    assert sub_rel2 == "gitops/apps/agy-cloud-private-plugins"

    # Handoff state file must live inside `.git/modules/...`, NOT pollute submodule working tree
    state_path = handoff_client._state_file(sub_dir)
    assert state_path.parent != sub_dir
    assert ".git" in state_path.parts

    # push_handoff from parent repo auto-retargets to submodule AND falls back to snapshot upload
    # when cluster repo-access reports accessible=False
    posted: list[dict[str, Any]] = []

    def _fake_http_json(
        method: str,
        req_url: str,
        *,
        token: str = "",
        payload: dict[str, Any] | None = None,
        ca_cert: str = "",
        insecure: bool = False,
        timeout: float = 20.0,
    ) -> dict[str, Any]:
        if method == "GET" and "/api/handoff/repo-access" in req_url:
            return {
                "repo": "billthefighter/agy-cloud-private-plugins",
                "accessible": False,
                "has_mirror": False,
                "reason": "Repository not found",
            }
        assert method == "POST" and req_url.endswith("/api/handoff")
        assert payload is not None
        posted.append(payload)
        return {
            "handoff_id": "slack-sub-1",
            "short_id": "HANDOFF-SUB1",
            "status": "running",
            "github_repo": payload["github_repo"],
        }

    monkeypatch.setattr(handoff_client, "http_json", _fake_http_json)
    resp = handoff_client.push_handoff(
        parent,
        base_url="https://makesense-k3s.netbird.cloud/agy",
        token="tok-parent",
        ca_cert="",
        insecure=False,
        prompt="Update gitops/apps/agy-cloud-private-plugins to add autofill",
        verify_hint="pytest gitops/apps/agy-cloud-private-plugins/tests/",
    )
    assert len(posted) == 1
    assert posted[0]["github_repo"] == "billthefighter/agy-cloud-private-plugins"
    assert posted[0]["verify_hint"] == "pytest tests/"
    assert posted[0]["repo_snapshot_b64"]  # automatically packed & uploaded!
    assert resp["effective_workdir"] == str(sub_dir.resolve())

    # Both parent and submodule saved handoff state pointing to target_workdir = sub_dir
    parent_saved = handoff_client.load_local_handoff_state(parent)
    sub_saved = handoff_client.load_local_handoff_state(sub_dir)
    assert parent_saved is not None and parent_saved["target_workdir"] == str(sub_dir.resolve())
    assert sub_saved is not None and sub_saved["handoff_id"] == "slack-sub-1"
    # Submodule working tree stays clean (no stray .agy_handoff_state.json inside workdir!)
    assert not (sub_dir / ".agy_handoff_state.json").exists()


@pytest.mark.asyncio
async def test_prepare_project_venv_installs_out_of_tree_and_skips_workspaces(
    tmp_path: Path,
) -> None:
    # 1. Repo without pyproject.toml -> returns None
    plain_dir = tmp_path / "plain_repo"
    plain_dir.mkdir()
    assert await _prepare_project_venv(plain_dir, venv_dir=tmp_path / "v_plain") is None

    # 2. Multi-member uv workspace -> skipped so we don't blindly sync heavy workspace members
    ws_dir = tmp_path / "workspace_repo"
    ws_dir.mkdir()
    (ws_dir / "pyproject.toml").write_text(
        '[project]\nname = "ws"\nversion = "0.1.0"\n\n[tool.uv.workspace]\nmembers = ["pkg_a", "pkg_b"]\n',
        encoding="utf-8",
    )
    assert await _prepare_project_venv(ws_dir, venv_dir=tmp_path / "v_ws") is None

    # 3. Single-package repo with pyproject.toml -> installs into out-of-tree venv without dirtying /work
    pkg_dir = tmp_path / "single_pkg"
    _init_repo(pkg_dir)
    (pkg_dir / "pyproject.toml").write_text(
        '[project]\nname = "demo-pkg"\nversion = "0.1.0"\nrequires-python = ">=3.12"\ndependencies = []\n',
        encoding="utf-8",
    )
    (pkg_dir / "demo_mod.py").write_text("ANSWER = 42\n", encoding="utf-8")
    _git(pkg_dir, "add", ".")
    _git(pkg_dir, "commit", "-m", "add pyproject")

    out_venv = tmp_path / "out_of_tree_venv"
    installed = await _prepare_project_venv(pkg_dir, venv_dir=out_venv, timeout_seconds=60)
    assert installed == out_venv
    assert (out_venv / "bin" / "python").is_file()
    # Working tree stays completely clean (no .venv or generated uv.lock inside /work!)
    diff_text, _ = _capture_git_diff(pkg_dir)
    assert diff_text == ""
    assert not (pkg_dir / ".venv").exists()
    assert not (pkg_dir / "uv.lock").exists()

    env_ov = _build_venv_env_overrides(installed, {"PATH": "/usr/local/bin:/usr/bin"})
    assert env_ov["VIRTUAL_ENV"] == str(out_venv)
    assert env_ov["UV_PROJECT_ENVIRONMENT"] == str(out_venv)
    assert env_ov["PATH"].startswith(f"{out_venv / 'bin'}:")
