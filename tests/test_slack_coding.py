from __future__ import annotations

import asyncio
import hashlib
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from agent_tasks.common.config import ControllerSettings
from agent_tasks.common.models import IssueStatus
from agent_tasks.common.state import StateStore
from agent_tasks.tasks.slack_coding import SlackCodingTask


def _make_settings(tmp_path: Path, targets_path: Path | None = None) -> ControllerSettings:
    return ControllerSettings(
        namespace="agent-tasks",
        node_name="k3s-worker-1",
        state_dir=tmp_path,
        targets_config_path=targets_path or (tmp_path / "targets.yaml"),
        target_github_token="gh-secret",
        slack_bot_token="xoxb-secret",
        slack_app_token="xapp-secret",
        slack_channel_id="C12345",
    )


async def test_basic_coding_request_creates_prompt_registers_state_and_dispatches(
    tmp_path: Path,
) -> None:
    store = StateStore(tmp_path / "state.sqlite3")
    dispatch_cb = AsyncMock()
    task = SlackCodingTask(
        settings=_make_settings(tmp_path),
        state=store,
        dispatch_callback=dispatch_cb,
    )

    thread_ts = "1731620000.000100"
    prompt = "add unit tests for parser"
    ack = await task.handle_coding_request(
        prompt_text=prompt,
        user="lucas",
        thread_ts=thread_ts,
        channel_id="C12345",
    )

    expected_short_id = (
        f"CODE-{hashlib.sha1(f'{thread_ts}:{prompt}'.encode()).hexdigest()[:6].upper()}"
    )
    issue_id = "slack-1731620000-000100"

    assert expected_short_id in ack
    assert "gemini-4-hydrogen" in ack
    assert "lodbrok-app/rattlsnek" in ack
    dispatch_cb.assert_awaited_once()

    prompt_file = tmp_path / "artifacts" / issue_id / "task_prompt.md"
    assert prompt_file.exists()
    prompt_body = prompt_file.read_text(encoding="utf-8")
    assert '<slack_request user="lucas">' in prompt_body
    assert "add unit tests for parser" in prompt_body
    assert "git diff HEAD" in prompt_body
    assert "TriageReport" in prompt_body
    assert "uv sync --package edge" in prompt_body

    assert (tmp_path / "sessions" / issue_id).is_dir()

    record = store.get_by_thread_ts(thread_ts)
    assert record is not None
    assert record.issue_id == issue_id
    assert record.short_id == expected_short_id
    assert record.task_kind == "slack_coding"
    assert record.github_repo == "lodbrok-app/rattlsnek"
    assert record.project_slug == "rattlsnek"
    assert record.default_branch == "main"
    assert record.permalink == "https://github.com/lodbrok-app/rattlsnek"
    assert record.status == IssueStatus.QUEUED
    assert record.turn == 1
    assert record.model_override is None
    assert record.channel_id == "C12345"


@pytest.mark.parametrize(
    "prompt_text",
    [
        "[carbon] repo:billthefighter/agent-tasks implement slack coding router",
        "repo:billthefighter/agent-tasks [carbon] implement slack coding router",
    ],
)
async def test_model_override_and_repo_override_parse_cleanly(
    tmp_path: Path,
    prompt_text: str,
) -> None:
    store = StateStore(tmp_path / "state.sqlite3")
    dispatch_cb = AsyncMock()
    task = SlackCodingTask(
        settings=_make_settings(tmp_path),
        state=store,
        dispatch_callback=dispatch_cb,
    )

    ack = await task.handle_coding_request(
        prompt_text=prompt_text,
        user="lucas",
        thread_ts="1731620111.000200",
        channel_id="C12345",
    )

    assert "gemini-4-carbon" in ack
    assert "billthefighter/agent-tasks" in ack
    dispatch_cb.assert_awaited_once()

    record = store.get_by_thread_ts("1731620111.000200")
    assert record is not None
    assert record.model_override == "gemini-4-carbon"
    assert record.github_repo == "billthefighter/agent-tasks"
    assert record.project_slug == "agent-tasks"
    assert record.permalink == "https://github.com/billthefighter/agent-tasks"
    assert record.title == "implement slack coding router"
    assert "[carbon]" not in record.title
    assert "repo:" not in record.title

    prompt_body = (tmp_path / "artifacts" / record.issue_id / "task_prompt.md").read_text(
        encoding="utf-8"
    )
    assert "[carbon]" not in prompt_body
    assert "repo:" not in prompt_body
    assert "rattlsnek" not in prompt_body
    assert "implement slack coding router" in prompt_body


async def test_existing_thread_bumps_turn_and_rejects_while_running(tmp_path: Path) -> None:
    store = StateStore(tmp_path / "state.sqlite3")
    dispatch_cb = AsyncMock()
    task = SlackCodingTask(
        settings=_make_settings(tmp_path),
        state=store,
        dispatch_callback=dispatch_cb,
    )

    thread_ts = "1731620222.000300"
    await task.handle_coding_request(
        prompt_text="add parser helper",
        user="lucas",
        thread_ts=thread_ts,
        channel_id="C12345",
    )
    record = store.get_by_thread_ts(thread_ts)
    assert record is not None

    # Reject follow-up instructions while worker Job is RUNNING
    store.mark_status(record.issue_id, IssueStatus.RUNNING)
    running_reply = await task.handle_coding_request(
        prompt_text="also support unicode",
        user="lucas",
        thread_ts=thread_ts,
        channel_id="C12345",
    )
    assert record.short_id in running_reply
    assert "1" in running_reply
    assert "already running" in running_reply.lower()
    assert dispatch_cb.await_count == 1
    assert not (tmp_path / "artifacts" / record.issue_id / "feedback-turn-2.txt").exists()

    # Once awaiting review, follow-up bumps turn and writes feedback-turn-2.txt
    store.mark_status(record.issue_id, IssueStatus.AWAITING_REVIEW)
    rev_reply = await task.handle_coding_request(
        prompt_text="[argon] also support unicode",
        user="lucas",
        thread_ts=thread_ts,
        channel_id="C12345",
    )
    assert "turn 2" in rev_reply.lower()
    assert record.short_id in rev_reply
    assert "gemini-4-argon" in rev_reply
    assert dispatch_cb.await_count == 2

    updated = store.get_by_thread_ts(thread_ts)
    assert updated is not None
    assert updated.turn == 2
    assert updated.status == IssueStatus.QUEUED
    assert updated.model_override == "gemini-4-argon"

    feedback_file = tmp_path / "artifacts" / record.issue_id / "feedback-turn-2.txt"
    assert feedback_file.exists()
    assert feedback_file.read_text(encoding="utf-8") == "also support unicode"


@pytest.mark.parametrize(
    "empty_prompt",
    [
        "",
        "   ",
        "[carbon]",
        "repo:billthefighter/agent-tasks",
        "[carbon] repo:billthefighter/agent-tasks   ",
    ],
)
async def test_empty_prompt_returns_usage_without_side_effects(
    tmp_path: Path,
    empty_prompt: str,
) -> None:
    store = StateStore(tmp_path / "state.sqlite3")
    dispatch_cb = AsyncMock()
    task = SlackCodingTask(
        settings=_make_settings(tmp_path),
        state=store,
        dispatch_callback=dispatch_cb,
    )

    reply = await task.handle_coding_request(
        prompt_text=empty_prompt,
        user="lucas",
        thread_ts="1731620333.000400",
        channel_id="C12345",
    )

    assert "Provide a task description" in reply
    assert "!code [carbon]" in reply
    assert store.status_counts() == {}
    dispatch_cb.assert_not_awaited()


async def test_invalid_repo_and_disabled_feature_reject_cleanly(tmp_path: Path) -> None:
    store = StateStore(tmp_path / "state.sqlite3")
    dispatch_cb = AsyncMock()
    task = SlackCodingTask(
        settings=_make_settings(tmp_path),
        state=store,
        dispatch_callback=dispatch_cb,
    )

    bad_repo_reply = await task.handle_coding_request(
        prompt_text="repo:../bad-repo fix bug",
        user="lucas",
        thread_ts="1731620444.000500",
        channel_id="C12345",
    )
    assert "Invalid repository override" in bad_repo_reply
    assert store.status_counts() == {}
    dispatch_cb.assert_not_awaited()

    targets_yaml = tmp_path / "targets.yaml"
    targets_yaml.write_text("slack_coding:\n  enabled: false\n", encoding="utf-8")
    disabled_task = SlackCodingTask(
        settings=_make_settings(tmp_path, targets_path=targets_yaml),
        state=store,
        dispatch_callback=dispatch_cb,
    )
    disabled_reply = await disabled_task.handle_coding_request(
        prompt_text="add unit tests",
        user="lucas",
        thread_ts="1731620555.000600",
        channel_id="C12345",
    )
    assert "disabled" in disabled_reply.lower()
    assert store.status_counts() == {}
    dispatch_cb.assert_not_awaited()


async def test_run_loop_calls_heartbeat_and_handles_cancellation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = StateStore(tmp_path / "state.sqlite3")
    heartbeats = 0
    sleep_durations: list[float] = []

    def heartbeat() -> None:
        nonlocal heartbeats
        heartbeats += 1

    async def fake_sleep(seconds: float) -> None:
        sleep_durations.append(seconds)
        raise asyncio.CancelledError

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)

    task = SlackCodingTask(
        settings=_make_settings(tmp_path),
        state=store,
        heartbeat_cb=heartbeat,
    )
    assert task.name == "slack-coding"

    with pytest.raises(asyncio.CancelledError):
        await task.run()

    assert heartbeats == 1
    assert sleep_durations == [60]


async def test_async_dispatch_returns_immediately_and_lists_context_files(
    tmp_path: Path,
) -> None:
    import json

    store = StateStore(tmp_path / "state.sqlite3")
    dispatch_started = asyncio.Event()
    allow_finish = asyncio.Event()

    async def slow_dispatch() -> None:
        dispatch_started.set()
        await allow_finish.wait()

    task = SlackCodingTask(
        settings=_make_settings(tmp_path),
        state=store,
        dispatch_callback=slow_dispatch,
    )
    thread_ts = "1731620999.000100"
    issue_id = f"slack-{thread_ts.replace('.', '-')}"
    art_dir = tmp_path / "artifacts" / issue_id
    art_dir.mkdir(parents=True, exist_ok=True)
    (art_dir / "handoff_meta.json").write_text(
        json.dumps(
            {
                "is_handoff": True,
                "context_files": ["plan.md", "notes.txt"],
            }
        ),
        encoding="utf-8",
    )

    ack = await task.handle_coding_request(
        prompt_text="Implement feature",
        user="laptop-handoff",
        thread_ts=thread_ts,
        channel_id="pwa:coding",
        async_dispatch=True,
    )
    assert "Queued coding task" in ack
    assert not allow_finish.is_set()
    await asyncio.wait_for(dispatch_started.wait(), timeout=2.0)

    prompt_md = (art_dir / "task_prompt.md").read_text(encoding="utf-8")
    assert "## Attached Context Files" in prompt_md
    assert "/artifacts/context/plan.md" in prompt_md
    assert "/artifacts/context/notes.txt" in prompt_md

    allow_finish.set()
    await asyncio.gather(*list(task._background_tasks))
