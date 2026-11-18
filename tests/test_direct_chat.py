from __future__ import annotations

import io
from datetime import UTC, datetime
from pathlib import Path

from PIL import Image

from agent_tasks.common.config import ControllerSettings
from agent_tasks.common.models import TokenUsageSummary
from agent_tasks.common.state import StateStore
from agent_tasks.tasks.direct_chat import AgyChatRunner, DirectChatService


class FakeChatRunner:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    async def run_turn(
        self,
        *,
        prompt: str,
        image_paths: list[Path],
        model: str,
        session_dir: Path,
        conversation_id: str | None,
    ) -> tuple[str, str, TokenUsageSummary]:
        self.calls.append(
            {
                "prompt": prompt,
                "image_paths": list(image_paths),
                "model": model,
                "session_dir": session_dir,
                "conversation_id": conversation_id,
            }
        )
        turn_num = len(self.calls)
        conv_id = conversation_id or "conv-direct-001"
        return (
            f"Reply #{turn_num} ({model}): {prompt}",
            conv_id,
            TokenUsageSummary(
                prompt_tokens=1000 * turn_num,
                cached_tokens=0,
                output_tokens=200 * turn_num,
                thoughts_tokens=0,
                total_tokens=1200 * turn_num,
            ),
        )


def _png_bytes() -> bytes:
    buf = io.BytesIO()
    Image.new("RGB", (120, 80), (56, 189, 248)).save(buf, format="PNG")
    return buf.getvalue()


def _make_settings(tmp_path: Path) -> ControllerSettings:
    return ControllerSettings(
        state_dir=tmp_path,
        targets_config_path=tmp_path / "missing-targets.yaml",
        target_github_token="gh-secret",
        slack_enabled=False,
        pwa_api_token="test-token",
    )


async def test_direct_chat_multiturn_images_and_billing_ledger(tmp_path: Path) -> None:
    settings = _make_settings(tmp_path)
    state = StateStore(tmp_path / "state.sqlite3")
    runner = FakeChatRunner()
    fixed_now = datetime(2026, 11, 16, 18, 0, tzinfo=UTC)
    svc = DirectChatService(settings=settings, state=state, runner=runner, clock=lambda: fixed_now)

    # Turn 1 with model override tag + attached photo
    t1, m1, img_paths, cleaned, chosen_model = svc.create_chat_and_append_user_msg(
        prompt="[carbon] What is this screenshot showing?",
        uploads=[("shot.png", _png_bytes())],
    )
    assert chosen_model == "gemini-4-carbon"
    assert cleaned == "What is this screenshot showing?"
    assert len(img_paths) == 1
    assert img_paths[0].exists()
    assert m1.payload["media"][0].startswith("pwa_uploads/")

    t1_done, reply1, cost1 = await svc.run_reply_turn(
        thread_id=t1.thread_id,
        prompt=cleaned,
        model=chosen_model,
        image_paths=img_paths,
    )
    assert t1_done.status == "active"
    assert t1_done.conversation_id == "conv-direct-001"
    assert "Reply #1 (gemini-4-carbon)" in reply1.body_md
    assert "gemini-4-carbon" in cost1

    # Turn 2 resumes existing conversation_id and bills incrementally
    t2, _m2, img2, cleaned2, model2 = svc.create_chat_and_append_user_msg(
        prompt="Summarize in one bullet",
        thread_id=t1.thread_id,
    )
    assert model2 == "gemini-4-carbon"
    assert img2 == []
    _t2_done, reply2, _ = await svc.run_reply_turn(
        thread_id=t2.thread_id,
        prompt=cleaned2,
        model=model2,
        image_paths=img2,
    )
    assert runner.calls[1]["conversation_id"] == "conv-direct-001"
    assert "Reply #2" in reply2.body_md

    # Total cumulative tokens reported on turn 2 was 2000 prompt + 400 output;
    # incremental billing must record delta (1000 prompt + 200 output per turn, total 2000+400).
    ledger = state.get_cycle_usage_summary("2026-11-15")
    assert ledger["prompt_tokens"] == 2000
    assert ledger["output_tokens"] == 400
    assert "direct_chat" in ledger["by_task_kind"]


def test_agy_chat_runner_config_enforces_sandboxed_tools(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("GEMINI_API_KEY", "AIzaFakeTestKey")
    runner = AgyChatRunner(default_model="gemini-4-hydrogen")
    conv_uuid = "12345678-1234-5678-1234-567812345678"
    cfg = runner.build_agent_config(
        model="gemini-4-hydrogen",
        session_dir=tmp_path / "sess",
        conversation_id=conv_uuid,
    )
    enabled = [t.name for t in cfg.capabilities.enabled_tools]
    assert set(enabled) == {"SEARCH_WEB", "FINISH"}
    assert cfg.capabilities.enable_subagents is False
    assert cfg.conversation_id == conv_uuid
    assert cfg.save_dir == str(tmp_path / "sess")
