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
    normalize_handoff_model,
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


def _init_origin_and_clone(tmp_path: Path) -> tuple[Path, Path, str]:
    origin_dir = tmp_path / "origin.git"
    subprocess.run(
        ["git", "init", "--bare", "-b", "main", str(origin_dir)],
        check=True,
        capture_output=True,
    )
    seed_dir = tmp_path / "seed"
    subprocess.run(
        ["git", "clone", str(origin_dir), str(seed_dir)],
        check=True,
        capture_output=True,
    )
    subprocess.run(
        ["git", "-C", str(seed_dir), "config", "user.name", "Test User"],
        check=True,
        capture_output=True,
    )
    subprocess.run(
        ["git", "-C", str(seed_dir), "config", "user.email", "test@example.com"],
        check=True,
        capture_output=True,
    )
    (seed_dir / "app.py").write_text("VALUE = 1\n", encoding="utf-8")
    subprocess.run(["git", "-C", str(seed_dir), "add", "."], check=True, capture_output=True)
    subprocess.run(
        ["git", "-C", str(seed_dir), "commit", "-m", "initial main"],
        check=True,
        capture_output=True,
    )
    subprocess.run(
        ["git", "-C", str(seed_dir), "push", "-u", "origin", "main"],
        check=True,
        capture_output=True,
    )
    main_sha = subprocess.run(
        ["git", "-C", str(seed_dir), "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()

    laptop_dir = tmp_path / "laptop"
    subprocess.run(
        ["git", "clone", str(origin_dir), str(laptop_dir)],
        check=True,
        capture_output=True,
    )
    subprocess.run(
        ["git", "-C", str(laptop_dir), "config", "user.name", "Test User"],
        check=True,
        capture_output=True,
    )
    subprocess.run(
        ["git", "-C", str(laptop_dir), "config", "user.email", "test@example.com"],
        check=True,
        capture_output=True,
    )
    return origin_dir, laptop_dir, main_sha


def test_unpushed_local_branch_squashes_from_origin_merge_base_and_rebases_on_pull(
    tmp_path: Path,
) -> None:
    origin_dir, laptop_dir, main_sha = _init_origin_and_clone(tmp_path)

    # Laptop creates an unpushed local branch with 2 commits + 1 uncommitted edit
    subprocess.run(
        ["git", "-C", str(laptop_dir), "checkout", "-b", "burn_down_xfails"],
        check=True,
        capture_output=True,
    )
    (laptop_dir / "app.py").write_text("VALUE = 2\n", encoding="utf-8")
    subprocess.run(["git", "-C", str(laptop_dir), "add", "app.py"], check=True, capture_output=True)
    subprocess.run(
        ["git", "-C", str(laptop_dir), "commit", "-m", "commit 1 on unpushed branch"],
        check=True,
        capture_output=True,
    )
    (laptop_dir / "feature.py").write_text("def run():\n    return 'local-commit'\n", encoding="utf-8")
    subprocess.run(
        ["git", "-C", str(laptop_dir), "add", "feature.py"], check=True, capture_output=True
    )
    subprocess.run(
        ["git", "-C", str(laptop_dir), "commit", "-m", "commit 2 on unpushed branch"],
        check=True,
        capture_output=True,
    )
    local_head_sha = subprocess.run(
        ["git", "-C", str(laptop_dir), "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()

    # Also add an uncommitted working-tree edit on top of the unpushed branch
    (laptop_dir / "wip.py").write_text("WIP = True\n", encoding="utf-8")

    state = capture_local_git_state(laptop_dir, repo_override="lodbrok-app/rattlsnek")
    assert state["branch"] == "main"
    assert state["base_sha"] == main_sha
    assert state["local_branch"] == "burn_down_xfails"
    assert state["local_head_sha"] == local_head_sha
    assert state["squashed_unpushed"] is True
    assert state["unpushed_commits"] == 2
    assert "VALUE = 2" in state["base_patch"]
    assert "local-commit" in state["base_patch"]
    assert "WIP = True" in state["base_patch"]
    assert "WIP = True" in state["working_tree_patch"]
    assert "local-commit" not in state["working_tree_patch"]

    save_local_handoff_state(
        laptop_dir,
        meta={
            "issue_id": "slack-unpushed-1",
            "short_id": "HANDOFF-UNPUSHED",
            "github_repo": "lodbrok-app/rattlsnek",
            "branch": state["branch"],
            "base_sha": state["base_sha"],
            "local_branch": state["local_branch"],
            "local_head_sha": state["local_head_sha"],
            "squashed_unpushed": True,
            "had_base_patch": True,
            "pulled": False,
        },
        initial_patch=state["base_patch"],
        working_tree_patch=state["working_tree_patch"],
    )

    # Simulate k3s pod cloning origin's main @ main_sha, applying base_patch, and editing further
    pod_dir = tmp_path / "pod_work"
    subprocess.run(
        ["git", "clone", "--branch", state["branch"], "--single-branch", str(origin_dir), str(pod_dir)],
        check=True,
        capture_output=True,
    )
    subprocess.run(
        ["git", "-C", str(pod_dir), "checkout", state["base_sha"]],
        check=True,
        capture_output=True,
    )
    init_patch = tmp_path / "base.patch"
    init_patch.write_text(state["base_patch"], encoding="utf-8")
    subprocess.run(
        ["git", "-C", str(pod_dir), "apply", str(init_patch)],
        check=True,
        capture_output=True,
    )
    # Pod updates feature.py and adds a test file
    (pod_dir / "feature.py").write_text(
        "def run():\n    return 'optimized-in-pod'\n", encoding="utf-8"
    )
    (pod_dir / "test_feature.py").write_text(
        "from feature import run\n\ndef test_run():\n    assert run() == 'optimized-in-pod'\n",
        encoding="utf-8",
    )
    subprocess.run(["git", "-C", str(pod_dir), "add", "-N", "."], check=True, capture_output=True)
    pod_diff = subprocess.run(
        ["git", "-C", str(pod_dir), "diff", "--binary", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout

    # Pull back onto laptop: local unpushed commits remain intact and only delta applies cleanly!
    apply_res = apply_remote_handoff_patch(
        laptop_dir,
        remote_diff=pod_diff,
        handoff_id="HANDOFF-UNPUSHED",
        remote_base_sha=main_sha,
    )
    assert apply_res["applied"] is True
    assert apply_res["rebased_from_ancestor"] is True
    # Local HEAD is untouched on burn_down_xfails
    head_after = subprocess.run(
        ["git", "-C", str(laptop_dir), "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    assert head_after == local_head_sha
    assert (laptop_dir / "app.py").read_text(encoding="utf-8") == "VALUE = 2\n"
    assert "optimized-in-pod" in (laptop_dir / "feature.py").read_text(encoding="utf-8")
    assert (laptop_dir / "wip.py").read_text(encoding="utf-8") == "WIP = True\n"
    assert (laptop_dir / "test_feature.py").is_file()


def test_unpushed_local_branch_clean_working_tree_pulls_only_pod_delta(tmp_path: Path) -> None:
    origin_dir, laptop_dir, main_sha = _init_origin_and_clone(tmp_path)

    subprocess.run(
        ["git", "-C", str(laptop_dir), "checkout", "-b", "burn_down_xfails"],
        check=True,
        capture_output=True,
    )
    (laptop_dir / "app.py").write_text("VALUE = 99\n", encoding="utf-8")
    subprocess.run(["git", "-C", str(laptop_dir), "add", "app.py"], check=True, capture_output=True)
    subprocess.run(
        ["git", "-C", str(laptop_dir), "commit", "-m", "unpushed commit"],
        check=True,
        capture_output=True,
    )

    state = capture_local_git_state(laptop_dir, repo_override="lodbrok-app/rattlsnek")
    assert state["branch"] == "main"
    assert state["base_sha"] == main_sha
    assert state["squashed_unpushed"] is True
    assert state["working_tree_patch"] == ""
    assert "VALUE = 99" in state["base_patch"]

    # Pod applies base_patch and makes zero extra edits -> pull reports already_identical
    res_same = apply_remote_handoff_patch(
        laptop_dir,
        remote_diff=state["base_patch"],
        handoff_id="HANDOFF-NOOP",
        remote_base_sha=main_sha,
    )
    assert res_same["applied"] is True
    assert res_same["already_identical"] is True
    assert res_same["rebased_from_ancestor"] is True


def test_normalize_handoff_model_handles_effort_shorthands_and_human_names(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("AGY_HANDOFF_MODEL", raising=False)
    assert normalize_handoff_model("") == ""
    assert normalize_handoff_model("neon", "high") == "gemini-4-neon-high"
    assert normalize_handoff_model("gemini-4-neon", "medium") == "gemini-4-neon-medium"
    assert normalize_handoff_model("gemini-4-hydrogen-low", "high") == "gemini-4-hydrogen-high"
    assert normalize_handoff_model("Gemini 4 Neon (Medium)") == "gemini-4-neon-medium"
    assert normalize_handoff_model("claude-sonnet-5-5", "medium") == "claude-sonnet-5-5-medium"
    assert normalize_handoff_model("claude-sonnet-5-5-medium") == "claude-sonnet-5-5-medium"

    monkeypatch.setenv("AGY_HANDOFF_MODEL", "gemini-4-neon-medium")
    assert normalize_handoff_model("") == "gemini-4-neon-medium"
    assert normalize_handoff_model("", "high") == "gemini-4-neon-high"

    with pytest.raises(ValueError, match="Invalid --effort"):
        normalize_handoff_model("neon", "ultra")
