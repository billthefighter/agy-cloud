from __future__ import annotations

import subprocess
import urllib.error
from pathlib import Path
from typing import Any

import pytest

from agent_tasks.handoff_cli import (
    apply_remote_handoff_patch,
    capture_local_git_state,
    format_handoff_summary,
    load_local_handoff_state,
    parse_github_repo_from_remote,
    poll_until_terminal,
    resolve_connection_config,
    save_local_handoff_state,
)


def _init_git_repo(repo_dir: Path) -> str:
    repo_dir.mkdir(parents=True, exist_ok=True)
    subprocess.run(
        ["git", "-C", str(repo_dir), "init", "-b", "main"], check=True, capture_output=True
    )
    subprocess.run(
        ["git", "-C", str(repo_dir), "config", "user.name", "Test User"],
        check=True,
        capture_output=True,
    )
    subprocess.run(
        ["git", "-C", str(repo_dir), "config", "user.email", "test@example.com"],
        check=True,
        capture_output=True,
    )
    subprocess.run(
        [
            "git",
            "-C",
            str(repo_dir),
            "remote",
            "add",
            "origin",
            "git@github.com:billthefighter/k3s-proxmox-terraform.git",
        ],
        check=True,
        capture_output=True,
    )
    (repo_dir / "app.py").write_text("def greet():\n    return 'hello'\n", encoding="utf-8")
    subprocess.run(["git", "-C", str(repo_dir), "add", "."], check=True, capture_output=True)
    subprocess.run(
        ["git", "-C", str(repo_dir), "commit", "-m", "initial"],
        check=True,
        capture_output=True,
    )
    sha = subprocess.run(
        ["git", "-C", str(repo_dir), "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    return sha


def test_parse_github_repo_from_remote_handles_ssh_and_https() -> None:
    assert (
        parse_github_repo_from_remote("git@github.com:lodbrok-app/rattlsnek.git")
        == "lodbrok-app/rattlsnek"
    )
    assert (
        parse_github_repo_from_remote("https://github.com/billthefighter/k3s-proxmox-terraform.git")
        == "billthefighter/k3s-proxmox-terraform"
    )
    assert (
        parse_github_repo_from_remote("https://github.com/billthefighter/agent-tasks")
        == "billthefighter/agent-tasks"
    )


def test_resolve_connection_config_discovers_env_makesense(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("AGY_HANDOFF_TOKEN", raising=False)
    monkeypatch.delenv("PWA_API_TOKEN", raising=False)
    monkeypatch.delenv("AGY_HANDOFF_URL", raising=False)
    monkeypatch.delenv("NETBIRD_HOSTNAME", raising=False)

    env_file = tmp_path / ".env.makesense"
    env_file.write_text(
        'NETBIRD_HOSTNAME="makesense-k3s"\nPWA_API_TOKEN="secret-from-env-file"\n',
        encoding="utf-8",
    )

    url, token = resolve_connection_config(repo_dir=tmp_path, env_files=(env_file,))
    assert url == "https://makesense-k3s.netbird.cloud/agy"
    assert token == "secret-from-env-file"


def test_capture_and_apply_cumulative_handoff_patch_reverses_initial_diff_cleanly(
    tmp_path: Path,
) -> None:
    repo_dir = tmp_path / "repo"
    base_sha = _init_git_repo(repo_dir)

    # Laptop makes initial uncommitted edit P0 (including a new untracked file)
    (repo_dir / "app.py").write_text(
        "def greet():\n    return 'hello from laptop'\n", encoding="utf-8"
    )
    (repo_dir / "helper.py").write_text("VALUE = 10\n", encoding="utf-8")

    local_state = capture_local_git_state(repo_dir)
    assert local_state["github_repo"] == "billthefighter/k3s-proxmox-terraform"
    assert local_state["branch"] == "main"
    assert local_state["base_sha"] == base_sha
    assert "hello from laptop" in local_state["base_patch"]
    assert "VALUE = 10" in local_state["base_patch"]

    save_local_handoff_state(
        repo_dir,
        meta={
            "issue_id": "slack-handoff-1",
            "short_id": "HANDOFF-123456",
            "github_repo": local_state["github_repo"],
            "branch": local_state["branch"],
            "base_sha": base_sha,
            "had_base_patch": True,
            "pulled": False,
        },
        initial_patch=local_state["base_patch"],
    )

    # Now simulate k3s pod starting from base_sha, applying P0, and making additional edits P1
    pod_dir = tmp_path / "pod_work"
    subprocess.run(["git", "clone", str(repo_dir), str(pod_dir)], check=True, capture_output=True)
    init_patch_path = tmp_path / "p0.patch"
    init_patch_path.write_text(local_state["base_patch"], encoding="utf-8")
    subprocess.run(
        ["git", "-C", str(pod_dir), "apply", str(init_patch_path)],
        check=True,
        capture_output=True,
    )
    # Pod extends app.py, updates helper.py, and adds test_app.py
    (pod_dir / "app.py").write_text(
        "def greet(name: str = 'world'):\n    return f'hello {name} from k3s pod'\n",
        encoding="utf-8",
    )
    (pod_dir / "helper.py").write_text("VALUE = 42\n", encoding="utf-8")
    (pod_dir / "test_app.py").write_text(
        "from app import greet\n\ndef test_greet():\n    assert 'k3s pod' in greet()\n",
        encoding="utf-8",
    )
    subprocess.run(["git", "-C", str(pod_dir), "add", "-N", "."], check=True, capture_output=True)
    cumulative_pod_diff = subprocess.run(
        ["git", "-C", str(pod_dir), "diff", "--binary", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout

    # Pull cumulative_pod_diff onto laptop (which still has P0 in its working tree!)
    apply_res = apply_remote_handoff_patch(
        repo_dir,
        remote_diff=cumulative_pod_diff,
        handoff_id="HANDOFF-123456",
    )
    assert apply_res["applied"] is True
    assert apply_res["reversed_initial"] is True
    assert "k3s pod" in (repo_dir / "app.py").read_text(encoding="utf-8")
    assert (repo_dir / "helper.py").read_text(encoding="utf-8") == "VALUE = 42\n"
    assert (repo_dir / "test_app.py").is_file()

    saved_meta = load_local_handoff_state(repo_dir)
    assert saved_meta is not None
    assert saved_meta["pulled"] is True
    assert saved_meta["pulled_handoff_id"] == "HANDOFF-123456"


def test_poll_until_terminal_survives_sleep_network_drops_and_fetches_diff_on_completion() -> None:
    calls: list[bool] = []
    ticks: list[str] = []
    now = 1000.0

    def fake_clock() -> float:
        return now

    def fake_sleep(secs: float) -> None:
        nonlocal now
        now += secs

    responses: list[Any] = [
        {"short_id": "HANDOFF-AAA", "turn": 1, "status": "queued", "is_terminal": False},
        urllib.error.URLError("NetBird tunnel suspended during laptop sleep"),
        TimeoutError("DNS lookup timed out on wake"),
        {"short_id": "HANDOFF-AAA", "turn": 1, "status": "running", "is_terminal": False},
        {"short_id": "HANDOFF-AAA", "turn": 1, "status": "awaiting_review", "is_terminal": True},
        {
            "short_id": "HANDOFF-AAA",
            "issue_id": "slack-handoff-aaa",
            "github_repo": "billthefighter/k3s-proxmox-terraform",
            "branch": "main",
            "turn": 1,
            "model": "gemini-4-hydrogen",
            "status": "awaiting_review",
            "is_terminal": True,
            "has_patch": True,
            "diff": "diff --git a/x.py b/x.py\n+ok = True\n",
            "report": {
                "root_cause": "Completed task while driving.",
                "fix_summary": "Added x.py.",
                "affected_files": ["x.py"],
                "verification_commands": ["uv run pytest"],
                "tests_passed": True,
                "confidence": "high",
            },
        },
    ]

    def fake_fetch(include_diff: bool) -> dict[str, Any]:
        calls.append(include_diff)
        item = responses.pop(0)
        if isinstance(item, Exception):
            raise item
        return item

    final = poll_until_terminal(
        fetch_fn=fake_fetch,
        poll_interval_s=10.0,
        max_wait_s=600.0,
        sleep_fn=fake_sleep,
        clock_fn=fake_clock,
        on_tick=ticks.append,
    )

    # First 4 status checks request include_diff=False; final fetch requests include_diff=True
    assert calls == [False, False, False, False, False, True]
    assert final["is_terminal"] is True
    assert final["has_patch"] is True
    assert any("waiting for NetBird/network" in t for t in ticks)
    assert any("status -> awaiting_review" in t for t in ticks)

    md = format_handoff_summary(final, apply_result={"applied": True, "backup_patch": ".git/bak"})
    assert "HANDOFF-AAA" in md
    assert "Completed task while driving." in md
    assert "Applied remote `fix.patch` cleanly" in md
