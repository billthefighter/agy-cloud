"""Focused unit tests for Pillars 1 & 3 of the portable `handoff_client.py` redesign."""

from __future__ import annotations

import fcntl
import io
import json
import subprocess
import urllib.error
from pathlib import Path
from typing import Any

import pytest

from agy_cloud.client import handoff_client

REPO_ROOT = Path(__file__).resolve().parents[1]


def _git(cwd: Path, *args: str) -> str:
    proc = subprocess.run(
        ["git", "-C", str(cwd), *args],
        check=True,
        capture_output=True,
        text=True,
    )
    return proc.stdout.strip()


def _init_repo_with_origin(tmp_path: Path) -> tuple[Path, Path, str]:
    origin = tmp_path / "origin.git"
    subprocess.run(
        ["git", "init", "--bare", "-b", "main", str(origin)], check=True, capture_output=True
    )

    work = tmp_path / "work"
    work.mkdir()
    _git(work, "init", "-b", "main")
    _git(work, "config", "user.name", "Tester")
    _git(work, "config", "user.email", "tester@example.com")
    (work / "app.py").write_text("x = 1\n", encoding="utf-8")
    _git(work, "add", "-A")
    _git(work, "commit", "-m", "initial")
    _git(work, "remote", "add", "origin", str(origin))
    _git(work, "push", "-u", "origin", "main")
    sha = _git(work, "rev-parse", "HEAD")
    return origin, work, sha


def test_handoff_client_package_and_skill_script_parity() -> None:
    """`src/agy_cloud/client/handoff_client.py` and `skills/handoff/scripts/handoff_client.py` must be identical."""
    pkg_copy = (REPO_ROOT / "src" / "agy_cloud" / "client" / "handoff_client.py").read_bytes()
    skill_copy = (REPO_ROOT / "skills" / "handoff" / "scripts" / "handoff_client.py").read_bytes()
    assert pkg_copy == skill_copy


def test_resolve_connection_config_prefers_explicit_url_over_parent_netbird_and_ignores_missing_ca(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    for var in (
        "AGY_HANDOFF_URL",
        "AGY_CLOUD_URL",
        "NETBIRD_HOSTNAME",
        "AGY_HANDOFF_TOKEN",
        "AGY_CLOUD_TOKEN",
        "PWA_API_TOKEN",
        "AGY_HANDOFF_CA_CERT",
    ):
        monkeypatch.delenv(var, raising=False)

    fake_home = tmp_path / "home"
    cfg_dir = fake_home / ".config" / "agy-cloud"
    cfg_dir.mkdir(parents=True)
    (cfg_dir / "config.env").write_text(
        "AGY_HANDOFF_URL=https://makesense-k3s.netbird.cloud/agy\n"
        "AGY_HANDOFF_TOKEN=tok-makesense\n"
        "AGY_HANDOFF_CA_CERT=/nonexistent/path/ca.crt\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(Path, "home", staticmethod(lambda: fake_home))

    parent = tmp_path / "k3s-proxmox-terraform"
    child = parent / "gitops" / "apps" / "agy-cloud"
    child.mkdir(parents=True)
    (parent / ".env").write_text("NETBIRD_HOSTNAME=rattlsnek-dev-k3s\n", encoding="utf-8")

    url, token, ca_cert = handoff_client.resolve_connection_config(child)
    assert url == "https://makesense-k3s.netbird.cloud/agy"
    assert token == "tok-makesense"
    assert ca_cert == ""


def test_auto_detect_model_from_newest_transcript_and_fallback(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("AGY_HANDOFF_MODEL", raising=False)
    workdir = tmp_path / "my-repo"
    workdir.mkdir()

    brain = tmp_path / "brain"
    old_logs = brain / "conv-old" / ".system_generated" / "logs"
    old_logs.mkdir(parents=True)
    old_tr = old_logs / "transcript.jsonl"
    old_tr.write_text(
        json.dumps({"type": "USER_INPUT", "content": str(workdir.resolve())})
        + "\n"
        + json.dumps({"type": "PLANNER_RESPONSE", "model": "gemini-4-hydrogen-medium"})
        + "\n",
        encoding="utf-8",
    )

    new_logs = brain / "conv-new" / ".system_generated" / "logs"
    new_logs.mkdir(parents=True)
    new_tr = new_logs / "transcript.jsonl"
    new_tr.write_text(
        json.dumps({"type": "USER_INPUT", "content": f"Working in {workdir.resolve()}"})
        + "\n"
        + json.dumps({"type": "PLANNER_RESPONSE", "model": "Gemini 4 Neon (High)"})
        + "\n",
        encoding="utf-8",
    )

    unrelated_logs = brain / "conv-other" / ".system_generated" / "logs"
    unrelated_logs.mkdir(parents=True)
    (unrelated_logs / "transcript.jsonl").write_text(
        json.dumps({"type": "USER_INPUT", "content": "/some/other/repo"})
        + "\n"
        + json.dumps({"type": "PLANNER_RESPONSE", "model": "gemini-4-carbon-high"})
        + "\n",
        encoding="utf-8",
    )

    detected = handoff_client.resolve_model_for_handoff(workdir, model="auto", brain_dir=brain)
    assert detected == "gemini-4-neon-high"

    # Fallback when no transcript matches
    empty_brain = tmp_path / "empty_brain"
    empty_brain.mkdir()
    assert (
        handoff_client.resolve_model_for_handoff(workdir, model="", brain_dir=empty_brain)
        == "gemini-4-hydrogen"
    )


def test_detect_git_metadata_falls_back_to_ls_remote_when_local_origin_refs_stale(
    tmp_path: Path,
) -> None:
    origin, work, _main_sha = _init_repo_with_origin(tmp_path)

    # Create a new commit on feat/fast-launch, push to origin, then delete local tracking ref
    _git(work, "checkout", "-b", "feat/fast-launch")
    (work / "app.py").write_text("x = 2\n", encoding="utf-8")
    _git(work, "commit", "-am", "feature commit")
    feat_sha = _git(work, "rev-parse", "HEAD")
    _git(work, "push", "origin", "feat/fast-launch")
    # Simulate stale local origin/* refs (e.g. pushed via another worktree)
    _git(work, "update-ref", "-d", "refs/remotes/origin/feat/fast-launch")

    meta = handoff_client.detect_git_metadata(work)
    assert meta["base_branch"] == "feat/fast-launch"
    assert meta["base_sha"] == feat_sha
    assert meta["patch"] == ""


def test_push_context_files_prompt_file_stdin_no_base_patch_and_timeout(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _origin, work, _sha = _init_repo_with_origin(tmp_path)
    (work / "app.py").write_text("x = 42\n", encoding="utf-8")

    prompt_file = tmp_path / "prompt.md"
    prompt_file.write_text("Implement feature X", encoding="utf-8")
    ctx1 = tmp_path / "plan.md"
    ctx1.write_text("# Design Plan", encoding="utf-8")
    ctx2 = tmp_path / "notes.txt"
    ctx2.write_text("Important detail", encoding="utf-8")

    captured: list[tuple[str, str, dict[str, Any] | None, float]] = []

    def _fake_http(
        method: str,
        url: str,
        *,
        token: str = "",
        payload: dict[str, Any] | None = None,
        ca_cert: str = "",
        insecure: bool = False,
        timeout: float = 20.0,
        extra_headers: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        captured.append((method, url, payload, timeout))
        if method == "GET" and "/repo-access" in url:
            return {"accessible": True, "reason": ""}
        return {
            "handoff_id": "slack-ctx-1",
            "short_id": "HANDOFF-CTX1",
            "status": "queued",
            "github_repo": "billthefighter/agy-cloud",
        }

    monkeypatch.setattr(handoff_client, "http_json", _fake_http)

    rc = handoff_client.main(
        [
            "push",
            "--workdir",
            str(work),
            "--url",
            "https://makesense.example/agy",
            "--token",
            "tok",
            "--repo",
            "billthefighter/agy-cloud",
            "--prompt-file",
            str(prompt_file),
            "--context-file",
            str(ctx1),
            "--context-file",
            str(ctx2),
            "--timeout",
            "5400",
            "--model",
            "Gemini 4 Neon (Medium)",
        ]
    )
    assert rc == 0
    post_calls = [c for c in captured if c[0] == "POST"]
    assert len(post_calls) == 1
    _method, _url, body, post_timeout = post_calls[0]
    assert body is not None
    assert post_timeout == 45.0
    assert "base_patch" not in body  # never doubles JSON patch size
    assert "x = 42" in body["patch"]
    assert body["prompt"] == "Implement feature X"
    assert body["timeout_seconds"] == 5400
    assert body["model"] == "gemini-4-neon-medium"
    assert body["idempotency_key"].startswith("handoff-")
    assert body["context_files"] == [
        {"name": "plan.md", "content": "# Design Plan"},
        {"name": "notes.txt", "content": "Important detail"},
    ]
    # Pending draft deleted on success
    assert not (work / ".git" / "agy_handoff_pending.json").exists()

    # Also test `--prompt -` reading from stdin
    monkeypatch.setattr("sys.stdin", io.StringIO("Prompt from stdin"))
    rc2 = handoff_client.main(
        [
            "push",
            "--workdir",
            str(work),
            "--url",
            "https://makesense.example/agy",
            "--token",
            "tok",
            "--repo",
            "billthefighter/agy-cloud",
            "--prompt",
            "-",
            "--model",
            "hydrogen",
        ]
    )
    assert rc2 == 0
    assert captured[-1][2] is not None
    assert captured[-1][2]["prompt"] == "Prompt from stdin"


def test_draft_caching_retry_push_branch_and_oversized_patch_guidance(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _origin, work, _main_sha = _init_repo_with_origin(tmp_path)
    _git(work, "checkout", "-b", "feat/big-work")
    _git(work, "branch", "sibling-branch")  # second local branch at same HEAD
    (work / "big.txt").write_text("A" * 4096 + "\n", encoding="utf-8")
    _git(work, "add", "-A")
    _git(work, "commit", "-m", "unpushed large commit")
    _git(work, "branch", "-f", "sibling-branch", "HEAD")

    posted: list[dict[str, Any]] = []

    def _fake_http(
        method: str,
        url: str,
        *,
        token: str = "",
        payload: dict[str, Any] | None = None,
        ca_cert: str = "",
        insecure: bool = False,
        timeout: float = 20.0,
        extra_headers: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        if method == "GET" and "/repo-access" in url:
            return {
                "accessible": True,
                "reason": "",
                "max_patch_bytes": 100,
                "max_snapshot_bytes": 50,  # smaller than repo archive -> omit --upload-repo!
            }
        assert payload is not None
        posted.append(payload)
        return {"handoff_id": "slack-retry-1", "short_id": "HANDOFF-R1", "status": "queued"}

    monkeypatch.setattr(handoff_client, "http_json", _fake_http)

    # First attempt fails preflight because unpushed commit patch > 100 bytes
    with pytest.raises(RuntimeError) as exc_info:
        handoff_client.main(
            [
                "push",
                "--workdir",
                str(work),
                "--url",
                "https://makesense.example/agy",
                "--token",
                "tok",
                "--repo",
                "billthefighter/agy-cloud",
                "--prompt",
                "Cached prompt text",
                "--model",
                "hydrogen",
            ]
        )
    msg = str(exc_info.value)
    assert "feat/big-work" in msg
    assert "sibling-branch" in msg
    assert "--retry --push-branch feat/big-work" in msg
    assert (
        "--upload-repo" not in msg
    )  # suppressed when accessible=True and repo > max_snapshot_bytes

    pending = work / ".git" / "agy_handoff_pending.json"
    assert pending.is_file()

    # Zero-rewrite retry with --retry --push-branch pushes HEAD and succeeds!
    rc = handoff_client.main(
        [
            "push",
            "--workdir",
            str(work),
            "--url",
            "https://makesense.example/agy",
            "--token",
            "tok",
            "--retry",
            "--push-branch",
            "feat/big-work",
        ]
    )
    assert rc == 0
    assert len(posted) == 1
    assert posted[0]["prompt"] == "Cached prompt text"
    assert posted[0]["base_branch"] == "feat/big-work"
    assert posted[0]["patch"] == ""
    assert not pending.exists()


def test_http_json_formats_http_error_with_body_detail(monkeypatch: pytest.MonkeyPatch) -> None:
    def _fake_urlopen(req: Any, timeout: float = 20.0, context: Any = None) -> Any:
        body = io.BytesIO(json.dumps({"detail": "Context file too large"}).encode("utf-8"))
        raise urllib.error.HTTPError(
            req.full_url,
            413,
            "Payload Too Large",
            hdrs=None,
            fp=body,  # type: ignore[arg-type]
        )

    monkeypatch.setattr(handoff_client.urllib.request, "urlopen", _fake_urlopen)

    with pytest.raises(urllib.error.HTTPError) as exc_info:
        handoff_client.http_json(
            "POST", "https://example.test/api/handoff", payload={"prompt": "hi"}
        )
    assert exc_info.value.code == 413
    assert "Context file too large" in str(exc_info.value)


def test_push_handoff_recovers_after_post_timeout_via_list_endpoint(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _origin, work, sha = _init_repo_with_origin(tmp_path)
    seen_idem_key = ""

    def _fake_http(
        method: str,
        url: str,
        *,
        token: str = "",
        payload: dict[str, Any] | None = None,
        ca_cert: str = "",
        insecure: bool = False,
        timeout: float = 20.0,
        extra_headers: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        nonlocal seen_idem_key
        if method == "GET" and "/repo-access" in url:
            return {"accessible": True, "reason": ""}
        if method == "POST" and url.endswith("/api/handoff"):
            assert payload is not None
            seen_idem_key = payload["idempotency_key"]
            raise TimeoutError("timed out reading response")
        if method == "GET" and "/api/handoff?repo=" in url:
            return {
                "handoffs": [
                    {
                        "handoff_id": "slack-recovered-99",
                        "short_id": "HANDOFF-REC99",
                        "status": "queued",
                        "base_sha": sha,
                        "idempotency_key": seen_idem_key,
                    }
                ]
            }
        raise AssertionError(f"Unexpected call: {method} {url}")

    monkeypatch.setattr(handoff_client, "http_json", _fake_http)

    resp = handoff_client.push_handoff(
        work,
        base_url="https://makesense.example/agy",
        token="tok",
        ca_cert="",
        insecure=False,
        prompt="Survive network drop",
        github_repo="billthefighter/agy-cloud",
        model="hydrogen",
    )
    assert resp["handoff_id"] == "slack-recovered-99"
    assert resp["recovered_after_timeout"] is True
    assert not (work / ".git" / "agy_handoff_pending.json").exists()


def test_singleton_watcher_lock_exits_cleanly_when_already_locked(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    _origin, work, _sha = _init_repo_with_origin(tmp_path)
    handoff_id = "slack-lock-test-1"
    lock_path = handoff_client._watcher_lock_path(work, handoff_id)
    with lock_path.open("a+", encoding="utf-8") as fh:
        fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        fh.seek(0)
        fh.truncate()
        fh.write("42424\n")
        fh.flush()

        res = handoff_client.poll_until_ready_and_pull(
            work,
            base_url="https://makesense.example/agy",
            token="tok",
            ca_cert="",
            insecure=False,
            handoff_id=handoff_id,
            poll_interval=0.1,
        )
        assert res["already_watching"] is True
        assert res["watcher_pid"] == "42424"
        out = capsys.readouterr().out
        assert "already watching" in out
        assert "42424" in out

        # CLI `watch` also exits 0 cleanly
        rc = handoff_client.main(
            [
                "watch",
                "--workdir",
                str(work),
                "--url",
                "https://makesense.example/agy",
                "--handoff-id",
                handoff_id,
            ]
        )
        assert rc == 0
        fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
