from __future__ import annotations

import io
import json
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

from fastapi.testclient import TestClient
from PIL import Image
from pywebpush import WebPushException

from agent_tasks.channels.composite import CompositeChannelAdapter
from agent_tasks.common.config import ControllerSettings
from agent_tasks.common.models import IssueStatus, TokenUsageSummary, TriageReport
from agent_tasks.common.state import StateStore
from agent_tasks.pwa.gateway import PwaGatewayAdapter, create_pwa_app
from agent_tasks.pwa.push import WebPushManager
from agent_tasks.tasks.billing_guard import BillingMonitorTask
from agent_tasks.tasks.direct_chat import DirectChatService
from agent_tasks.tasks.food_tracker.models import (
    EstimatedMealItem,
    MealEstimateSchema,
)
from agent_tasks.tasks.food_tracker.task import FoodTrackerTask


class FakeChatRunner:
    async def run_turn(
        self,
        *,
        prompt: str,
        image_paths: list[Path],
        model: str,
        session_dir: Path,
        conversation_id: str | None,
    ) -> tuple[str, str, TokenUsageSummary]:
        return (
            f"Mobile answer ({model}): {prompt}",
            conversation_id or "conv-pwa-1",
            TokenUsageSummary(prompt_tokens=200, output_tokens=50, total_tokens=250),
        )


class FakeNutritionEstimator:
    async def estimate_meal(
        self,
        *,
        description: str,
        photo_paths: list[Path],
        local_date: str,
        default_slot: str,
        saved_foods: list,
        model_override: str | None = None,
    ) -> tuple[MealEstimateSchema, TokenUsageSummary, str]:
        est = MealEstimateSchema(
            title="Chicken Rice Bowl",
            meal_slot="lunch",
            confidence="high",
            items=[
                EstimatedMealItem(
                    name="Chicken Breast",
                    quantity_desc="6 oz",
                    estimated_grams=170.0,
                    calories_kcal=280.0,
                    protein_g=52.0,
                    carbs_g=0.0,
                    fat_g=6.0,
                    fiber_g=0.0,
                    source="vision_estimate",
                )
            ],
            assumptions_note="Estimated from mobile photo.",
        )
        return (
            est,
            TokenUsageSummary(prompt_tokens=150, output_tokens=80, total_tokens=230),
            (model_override or "gemini-4-hydrogen"),
        )

    async def revise_meal(self, **kwargs):
        raise NotImplementedError

    async def suggest_remaining(self, **kwargs) -> tuple[str, TokenUsageSummary, str]:
        return (
            "Try Greek yogurt + berries (200 kcal, 20g P).",
            TokenUsageSummary(prompt_tokens=100, output_tokens=40, total_tokens=140),
            "gemini-4-hydrogen",
        )


class FakeSlackAdapter:
    def __init__(self, *, fail_send: bool = False) -> None:
        self.fail_send = fail_send
        self.channel_posts: list[tuple[str, str, str | None]] = []
        self.triage_posts: list[str] = []
        self.thread_posts: list[tuple[str, str]] = []
        self.blocks_posts: list[tuple[str, str]] = []

    async def start(self) -> None:
        return None

    async def stop(self) -> None:
        return None

    def is_explicit_role_channel(self, role: str, channel_id: str) -> bool:
        return role == "food" and channel_id == "C-FOOD"

    def resolve_channel(self, role: str) -> str:
        return "C-FOOD" if role == "food" else f"C-{role.upper()}"

    def resolve_channel_for_task(self, task_kind: str) -> str:
        return "C-CODING"

    async def post_channel_message(
        self, text: str, *, role: str = "", channel_id: str | None = None
    ) -> str:
        if self.fail_send:
            raise RuntimeError("slack down")
        self.channel_posts.append((role, text, channel_id))
        return f"1700000000.{len(self.channel_posts):06d}"

    async def post_triage_result(
        self, record, report, diff_text: str, *, model_used: str = "", cost_line: str = ""
    ) -> str:
        if self.fail_send:
            raise RuntimeError("slack down")
        self.triage_posts.append(record.issue_id)
        return record.thread_ts or "1700000001.000001"

    async def post_thread_message(
        self, thread_ts: str, text: str, *, channel_id: str | None = None
    ) -> None:
        if self.fail_send:
            raise RuntimeError("slack down")
        self.thread_posts.append((thread_ts, text))

    async def post_blocks_message(
        self, *, channel_id: str, text: str, blocks: list[dict], thread_ts: str | None = None
    ) -> str:
        if self.fail_send:
            raise RuntimeError("slack down")
        self.blocks_posts.append((channel_id, text))
        return f"1700000002.{len(self.blocks_posts):06d}"

    async def update_blocks_message(
        self, *, channel_id: str, ts: str, text: str, blocks: list[dict]
    ) -> None:
        return None

    async def add_reaction(self, *, channel_id: str, ts: str, name: str) -> None:
        return None

    async def remove_reaction(self, *, channel_id: str, ts: str, name: str) -> None:
        return None


def _png_bytes() -> bytes:
    buf = io.BytesIO()
    Image.new("RGB", (64, 64), (34, 197, 94)).save(buf, format="PNG")
    return buf.getvalue()


def _make_stack(tmp_path: Path, *, push_sender=None, slack: FakeSlackAdapter | None = None):
    ca_path = tmp_path / "ca.crt"
    ca_path.write_text("-----BEGIN CERTIFICATE-----\nFAKE\n-----END CERTIFICATE-----\n")
    settings = ControllerSettings(
        state_dir=tmp_path,
        targets_config_path=tmp_path / "missing-targets.yaml",
        target_github_token="ghp-test",
        slack_enabled=slack is not None,
        pwa_enabled=True,
        pwa_api_token="super-secret-pwa-token",
        pwa_ca_cert_path=ca_path,
    )
    state = StateStore(tmp_path / "state.sqlite3")
    push_mgr = WebPushManager(settings=settings, state=state, sender=push_sender)
    direct_chat = DirectChatService(
        settings=settings,
        state=state,
        runner=FakeChatRunner(),
        clock=lambda: datetime(2026, 11, 16, 19, 0, tzinfo=UTC),
    )

    actions_called: list[tuple[str, str, str]] = []

    async def fake_coding(prompt: str, user: str, ts: str, chan: str) -> str:
        state.register_if_new(
            issue_id="slack-coding-1",
            short_id="CODE-1111",
            project_slug="rattlsnek",
            github_repo="lodbrok-app/rattlsnek",
            default_branch="main",
            title=prompt[:80],
            permalink="https://github.com/lodbrok-app/rattlsnek",
            task_kind="slack_coding",
            thread_ts=ts,
            channel_id=chan,
        )
        return "Queued `CODE-1111`."

    async def fake_open_pr(issue_id: str, user: str) -> None:
        actions_called.append(("open_draft_pr", issue_id, user))
        state.mark_status(
            issue_id,
            IssueStatus.PR_OPENED,
            pr_url="https://github.com/lodbrok-app/rattlsnek/pull/42",
        )

    gateway = PwaGatewayAdapter(
        settings=settings,
        state=state,
        push_manager=push_mgr,
        direct_chat=direct_chat,
        on_open_draft_pr=fake_open_pr,
        on_thread_feedback=AsyncMock(),
        on_status_query=AsyncMock(return_value="*Keepalive Healthy*"),
        on_k8s_query=AsyncMock(return_value="*K8s Healthy*"),
        on_ci_query=AsyncMock(return_value="*CI Healthy*"),
        on_coding_request=fake_coding,
    )
    composite = CompositeChannelAdapter(pwa=gateway, slack=slack)
    billing = BillingMonitorTask(settings=settings, state=state, channel=composite)
    food = FoodTrackerTask(
        settings=settings,
        state=state,
        channel=composite,
        estimator=FakeNutritionEstimator(),
        clock=lambda: datetime(2026, 11, 16, 19, 0, tzinfo=UTC),
    )
    gateway.billing_task = billing
    gateway.food_task = food
    gateway.slack_adapter = slack  # type: ignore[assignment]
    gateway.on_billing_command = billing.handle_billing_command
    gateway.on_food_message = food.handle_message
    gateway.on_food_thread_reply = food.handle_thread_reply
    gateway.on_food_scale = food.handle_scale_action
    gateway.on_food_save_staple = food.handle_save_staple_action
    gateway.on_food_delete = food.handle_delete_action

    app = create_pwa_app(gateway)
    return settings, state, push_mgr, gateway, composite, app, actions_called


def test_state_store_pwa_threads_and_org_filters(tmp_path: Path) -> None:
    state = StateStore(tmp_path / "state.sqlite3")
    state.upsert_pwa_thread(
        thread_id="t1",
        thread_ts="100.1",
        kind="coding",
        title="Fix bug",
        status="awaiting_review",
        increment_unread=True,
    )
    state.upsert_pwa_thread(
        thread_id="t2",
        thread_ts="100.2",
        kind="chat",
        title="Quick question",
        status="active",
        pinned=True,
    )
    state.append_pwa_message(thread_id="t1", sender="agent", body_md="Found root cause in parser.")

    threads = state.list_pwa_threads(folder="inbox", filter_mode="all")
    assert [t.thread_id for t in threads] == ["t2", "t1"]  # pinned first

    review = state.list_pwa_threads(filter_mode="needs_review")
    assert [t.thread_id for t in review] == ["t1"]

    searched = state.list_pwa_threads(query="parser")
    assert [t.thread_id for t in searched] == ["t1"]

    state.update_pwa_thread_org("t1", folder="archived", mark_read=True)
    counts = state.get_pwa_counts()
    assert counts["inbox"] == 1
    assert counts["archived"] == 1
    assert counts["unread_total"] == 0


async def test_composite_adapter_dual_writes_and_isolates_slack_failures(tmp_path: Path) -> None:
    slack_ok = FakeSlackAdapter(fail_send=False)
    _, state, _, gateway, composite, _, _ = _make_stack(tmp_path, slack=slack_ok)

    ts = await composite.post_channel_message("Urgent 🚨 OOMKilled pod", role="k8s")
    assert ts.startswith("1700000000.")
    assert len(slack_ok.channel_posts) == 1
    msgs = state.list_pwa_messages("feed-k8s")
    assert len(msgs) == 1
    assert msgs[0].external_ts == ts

    # When Slack raises, PWA still receives the message cleanly
    slack_broken = FakeSlackAdapter(fail_send=True)
    composite_broken = CompositeChannelAdapter(pwa=gateway, slack=slack_broken)
    ts2 = await composite_broken.post_channel_message("Secondary alert", role="k8s")
    assert ts2.startswith("feed-k8s-") or ts2
    assert len(state.list_pwa_messages("feed-k8s")) == 2

    # pwa:chat threads never forward to Slack
    await composite.post_thread_message("pwa:chat-123", "Hello", channel_id="pwa:chat")
    assert slack_ok.thread_posts == []


def test_pwa_http_ws_food_coding_billing_and_push_e2e(tmp_path: Path) -> None:
    sent_pushes: list[dict] = []

    def fake_push_sender(sub_info: dict, data: str, priv_key: str, claims: dict) -> None:
        if "gone.example" in sub_info["endpoint"]:
            raise WebPushException("Gone", response=SimpleNamespace(status_code=410))
        sent_pushes.append({"endpoint": sub_info["endpoint"], "data": data, "claims": claims})

    slack = FakeSlackAdapter()
    _, state, push_mgr, gateway, _, app, actions_called = _make_stack(
        tmp_path, push_sender=fake_push_sender, slack=slack
    )
    assert len(push_mgr.public_key) == 87

    with TestClient(app) as client:
        # 1) Public shell & bootstrap routes
        assert client.get("/agy/healthz").json()["ok"] is True
        assert client.get("/agy/manifest.webmanifest").json()["short_name"] == "AGY"
        icon_resp = client.get("/agy/icons/icon-180.png")
        assert icon_resp.status_code == 200
        assert icon_resp.content.startswith(b"\x89PNG")
        sw_resp = client.get("/agy/sw.js")
        assert sw_resp.headers["service-worker-allowed"] == "/agy/"
        assert "BEGIN CERTIFICATE" in client.get("/agy/ca.crt").text

        # Unauthenticated API rejected
        assert client.get("/agy/api/threads").status_code == 401

        # 2) Login sets HttpOnly cookie & enforces CSRF header on cookie mutations
        bad_login = client.post("/agy/api/auth/login", json={"token": "wrong"})
        assert bad_login.status_code == 401
        ok_login = client.post("/agy/api/auth/login", json={"token": "super-secret-pwa-token"})
        assert ok_login.status_code == 200

        csrf_blocked = client.post("/agy/api/ops/billing", json={"command": "pause"})
        assert csrf_blocked.status_code == 403

        hdr = {"X-AGY-Client": "pwa"}

        # 3) WebSocket auth + live Direct Chat creation
        with client.websocket_connect("/agy/api/ws") as ws:
            hello = ws.receive_json()
            assert hello["type"] == "hello"

            chat_resp = client.post(
                "/agy/api/chats",
                headers=hdr,
                data={"prompt": "Explain NetBird WireGuard MTU", "model": "gemini-4-hydrogen"},
                files=[("files", ("diagram.png", _png_bytes(), "image/png"))],
            )
            assert chat_resp.status_code == 200
            chat_tid = chat_resp.json()["thread"]["thread_id"]
            ev1 = ws.receive_json()
            assert ev1["type"] == "message.created"
            assert ev1["data"]["thread"]["thread_id"] == chat_tid

        # 4) Coding task -> simulated triage result -> patch & Open Draft PR action
        code_resp = client.post(
            "/agy/api/coding",
            headers=hdr,
            json={"prompt": "Fix null guard", "repo": "lodbrok-app/rattlsnek", "model": ""},
        )
        assert code_resp.status_code == 200
        issue_id = code_resp.json()["thread"]["issue_id"]
        assert issue_id == "slack-coding-1"

        art_dir = tmp_path / "artifacts" / issue_id
        art_dir.mkdir(parents=True, exist_ok=True)
        (art_dir / "fix.patch").write_text("diff --git a/a.py b/a.py\n+x = 1\n", encoding="utf-8")

        import asyncio

        rec = state.get_by_issue_id(issue_id)
        assert rec is not None
        report = TriageReport(
            root_cause="Missing null guard",
            confidence="high",
            affected_files=["a.py"],
            fix_summary="Added guard",
            verification_commands=["pytest"],
            tests_passed=True,
        )
        asyncio.run(gateway.post_triage_result(rec, report, "+x = 1"))

        patch_resp = client.get(f"/agy/api/threads/{issue_id}/patch")
        assert patch_resp.status_code == 200
        assert "+x = 1" in patch_resp.json()["diff"]

        act_resp = client.post(
            f"/agy/api/threads/{issue_id}/actions",
            headers=hdr,
            json={"action": "open_draft_pr"},
        )
        assert act_resp.status_code == 200
        assert actions_called == [("open_draft_pr", issue_id, "mobile")]

        # 5) Multimodal Food upload + portion scale + media fetch + path-traversal guard
        food_resp = client.post(
            "/agy/api/food/log",
            headers=hdr,
            data={"text": "Grilled chicken and rice"},
            files=[("files", ("plate.png", _png_bytes(), "image/png"))],
        )
        assert food_resp.status_code == 200
        food_data = food_resp.json()
        assert len(food_data["meals"]) == 1
        assert food_data["logged_meal"]["id"] == food_data["meals"][0]["id"]
        meal = food_data["meals"][0]
        assert meal["total_calories_kcal"] == 280.0
        assert meal["photo_url"].startswith("/agy/api/media/food_photos/")

        media_resp = client.get(meal["photo_url"])
        assert media_resp.status_code == 200

        # Traversal protection must block reading state.sqlite3 or escaping media roots
        assert client.get("/agy/api/media/state.sqlite3").status_code == 403
        assert client.get("/agy/api/media/food_photos/../state.sqlite3").status_code == 403

        scale_resp = client.post(
            f"/agy/api/food/entries/{meal['id']}/scale",
            headers=hdr,
            json={"multiplier": 1.25},
        )
        assert scale_resp.status_code == 200
        assert scale_resp.json()["meals"][0]["total_calories_kcal"] == 350.0

        # 6) Ops & Billing pause/resume
        ops_resp = client.get("/agy/api/ops/overview")
        assert ops_resp.status_code == 200
        assert ops_resp.json()["billing"]["paused"] is False

        pause_resp = client.post("/agy/api/ops/billing", headers=hdr, json={"command": "pause"})
        assert pause_resp.status_code == 200
        assert pause_resp.json()["overview"]["billing"]["paused"] is True

        # 7) Web Push subscribe, test notification, and 410 stale pruning
        assert (
            client.post(
                "/agy/api/push/subscribe",
                headers=hdr,
                json={
                    "endpoint": "https://web.push.apple.com/live-device",
                    "keys": {"p256dh": "k1", "auth": "a1"},
                },
            ).status_code
            == 200
        )
        assert (
            client.post(
                "/agy/api/push/subscribe",
                headers=hdr,
                json={
                    "endpoint": "https://gone.example/expired-device",
                    "keys": {"p256dh": "k2", "auth": "a2"},
                },
            ).status_code
            == 200
        )
        push_res = client.post("/agy/api/push/test", headers=hdr).json()
        assert push_res["sent"] == 1
        assert push_res["pruned"] == 1
        assert len(state.list_push_subscriptions()) == 1
        assert len(sent_pushes) >= 1


def test_resolve_image_mime_webp_and_food_log_failure_surfaces_500(tmp_path: Path) -> None:
    from agent_tasks.common.models import resolve_image_mime

    assert resolve_image_mime(Path("plate.webp")) == "image/webp"
    assert resolve_image_mime(Path("plate.JPG")) == "image/jpeg"

    class _FailingEstimator(FakeNutritionEstimator):
        async def estimate_meal(self, **kwargs):
            raise ValueError("Simulated estimator failure")

    _settings, _state, _push_mgr, gateway, _composite, app, _actions = _make_stack(tmp_path)
    assert gateway.food_task is not None
    gateway.food_task.estimator = _FailingEstimator()

    with TestClient(app) as client:
        login = client.post("/agy/api/auth/login", json={"token": "super-secret-pwa-token"})
        assert login.status_code == 200
        fail_resp = client.post(
            "/agy/api/food/log",
            headers={"X-AGY-Client": "pwa"},
            data={"text": "Steak bowl"},
            files=[("files", ("plate.png", _png_bytes(), "image/png"))],
        )
        assert fail_resp.status_code == 500
        assert "Simulated estimator failure" in fail_resp.json()["detail"]


def test_slow_food_log_backgrounds_and_broadcasts_completion(tmp_path: Path) -> None:
    import asyncio
    import time

    class _SlowEstimator(FakeNutritionEstimator):
        async def estimate_meal(self, **kwargs):
            await asyncio.sleep(0.15)
            return await super().estimate_meal(**kwargs)

    _settings, _state, _push_mgr, gateway, _composite, app, _actions = _make_stack(tmp_path)
    assert gateway.food_task is not None
    gateway.food_task.estimator = _SlowEstimator()
    gateway._food_log_sync_timeout_s = 0.03

    with TestClient(app) as client:
        login = client.post("/agy/api/auth/login", json={"token": "super-secret-pwa-token"})
        assert login.status_code == 200

        with client.websocket_connect("/agy/api/ws") as ws:
            queued_resp = client.post(
                "/agy/api/food/log",
                headers={"X-AGY-Client": "pwa"},
                data={"text": "Slow jelly donut"},
            )
            assert queued_resp.status_code == 200
            queued_data = queued_resp.json()
            assert queued_data["queued"] is True
            assert queued_data["thread_ts"]
            assert queued_data["meals"] == []

            deadline = time.monotonic() + 2.0
            ws_event = None
            while time.monotonic() < deadline:
                msg = ws.receive_json()
                if msg.get("type") == "food.updated" and msg.get("data", {}).get("meal_title"):
                    ws_event = msg["data"]
                    break
            assert ws_event is not None
            assert ws_event["thread_ts"] == queued_data["thread_ts"]
            assert ws_event["meal_title"] == "Chicken Rice Bowl"

        today = client.get("/agy/api/food/today").json()
        assert len(today["meals"]) == 1


def test_pwa_garmin_trends_settings_and_coach_health_context(tmp_path: Path) -> None:
    from agent_tasks.tasks.food_tracker.models import GarminActivityRecord, GarminDailyMetric

    _settings, _state, _push_mgr, gateway, _composite, app, _actions = _make_stack(tmp_path)
    assert gateway.food_task is not None
    cfg = gateway._load_targets().food_tracker
    today_str = gateway.food_task._today_local_date(cfg)
    gateway.food_task.nutrition.upsert_garmin_daily(
        GarminDailyMetric(
            local_date=today_str,
            bmr_raw_kcal=1200.0,
            bmr_full_day_kcal=2400.0,
            active_kcal=400.0,
            total_burn_kcal=2800.0,
            steps=8500,
            moderate_min=30,
            vigorous_min=20,
            sleep_seconds=5.5 * 3600,
            sleep_score=64,
            hrv_last_night_avg=28,
            hrv_status="LOW",
            body_battery_current=45,
            activities=[
                GarminActivityRecord(
                    activity_id=101,
                    name="Morning Run",
                    activity_type="running",
                    calories_kcal=350.0,
                    duration_seconds=1800.0,
                )
            ],
        )
    )

    with TestClient(app) as client:
        assert (
            client.post("/agy/api/auth/login", json={"token": "super-secret-pwa-token"}).status_code
            == 200
        )

        # Update Garmin settings via PWA endpoint (deficit -500 triggers readiness warning)
        s_resp = client.post(
            "/agy/api/food/garmin/settings",
            headers={"X-AGY-Client": "pwa"},
            json={"goal_delta_kcal": -500, "dynamic_mode": "daily_burn", "active_factor": 0.75},
        )
        assert s_resp.status_code == 200
        s_data = s_resp.json()
        assert s_data["garmin_settings"]["goal_delta_kcal"] == -500
        assert s_data["garmin_settings"]["dynamic_mode"] == "daily_burn"
        assert s_data["target"]["calories_kcal"] == 2200  # 2400 + 400*0.75 - 500
        assert s_data["readiness_warning"] is not None
        assert "short sleep" in s_data["readiness_warning"]

        # 14d trends endpoint
        t_resp = client.get("/agy/api/food/trends?days=14")
        assert t_resp.status_code == 200
        t_data = t_resp.json()
        assert len(t_data["series"]) == 14
        assert t_data["weekly_intensity_min"] == 70  # 30 + 2*20

        # Direct chat with include_health_context injects Garmin & Nutrition context
        c_resp = client.post(
            "/agy/api/chats",
            headers={"X-AGY-Client": "pwa"},
            data={"prompt": "Should I do intervals today?", "include_health_context": "true"},
        )
        assert c_resp.status_code == 200
        c_data = c_resp.json()
        assert "Morning Run" in c_data["message"]["body_md"]
        assert "HRV 28 ms (LOW)" in c_data["message"]["body_md"]


async def test_dual_pwa_endpoints_share_auth_and_split_manifests(tmp_path: Path) -> None:
    sent_pushes: list[dict] = []

    def fake_push_sender(sub_info: dict, data: str, priv_key: str, claims: dict) -> None:
        sent_pushes.append({"endpoint": sub_info["endpoint"], "data": data})

    _, state, push_mgr, _, _, app, _ = _make_stack(tmp_path, push_sender=fake_push_sender)

    with TestClient(app) as client:
        # Chat shell (/agy) vs Health shell (/health)
        agy_html = client.get("/agy/").text
        health_html = client.get("/health/").text
        assert '<meta name="agy-app-mode" content="chat" />' in agy_html
        assert "<title>AGY Tasks</title>" in agy_html
        assert '<meta name="agy-app-mode" content="health" />' in health_html
        assert "<title>AGY Health</title>" in health_html

        # Distinct PWA manifests & service worker scopes
        agy_manifest = client.get("/agy/manifest.webmanifest").json()
        health_manifest = client.get("/health/manifest.webmanifest").json()
        assert agy_manifest["start_url"] == "/agy/"
        assert agy_manifest["scope"] == "/agy/"
        assert agy_manifest["short_name"] == "AGY"
        assert health_manifest["start_url"] == "/health/"
        assert health_manifest["scope"] == "/health/"
        assert health_manifest["short_name"] == "Health"

        assert client.get("/health/sw.js").headers["service-worker-allowed"] == "/health/"
        assert client.get("/agy/static/chat_view.js").status_code == 200
        assert client.get("/agy/static/app.js").status_code == 200
        assert client.get("/agy/static/health_view.js").status_code == 404
        assert (
            client.get("/health/icons/icon-180.png").content
            != client.get("/agy/icons/icon-180.png").content
        )

        # Single login cookie (path=/) authenticates both /agy and /health
        login_resp = client.post("/agy/api/auth/login", json={"token": "super-secret-pwa-token"})
        assert login_resp.status_code == 200
        assert client.get("/health/api/food/today").status_code == 200
        assert client.get("/agy/api/threads").status_code == 200

        # exclude_kinds=food filters out health/food entries from /agy inbox & counts
        state.upsert_pwa_thread(
            thread_id="food-99",
            thread_ts="1700000099.1",
            kind="food",
            title="Logged: 2 eggs",
            status="resolved",
            increment_unread=True,
        )
        all_resp = client.get("/agy/api/threads").json()
        filtered_resp = client.get("/agy/api/threads?exclude_kinds=food").json()
        assert any(t["kind"] == "food" for t in all_resp["threads"])
        assert all(t["kind"] != "food" for t in filtered_resp["threads"])
        assert filtered_resp["counts"]["unread_total"] < all_resp["counts"]["unread_total"]

        # Subscribe to push and verify food notifications target /health/
        client.post(
            "/health/api/push/subscribe",
            headers={"X-AGY-Client": "pwa"},
            json={
                "endpoint": "https://push.example/health",
                "keys": {"p256dh": "k1", "auth": "a1"},
            },
        )

    await push_mgr.notify(title="Meal logged", body="Salmon bowl", kind="food")
    assert len(sent_pushes) == 1
    assert json.loads(sent_pushes[0]["data"])["url"] == "/health/"


def test_health_meal_reaction_endpoint_and_ui_assets(tmp_path: Path) -> None:
    from agent_tasks.tasks.food_tracker.store import NutritionStore

    nutrition = NutritionStore(tmp_path / "nutrition.sqlite3")
    meal = nutrition.create_meal_entry(
        local_date="2026-11-17",
        raw_description="Salmon plate",
        estimate=MealEstimateSchema(
            title="Salmon Plate",
            meal_slot="dinner",
            confidence="high",
            items=[
                EstimatedMealItem(
                    name="Salmon",
                    quantity_desc="6 oz",
                    calories_kcal=360.0,
                    protein_g=40.0,
                    carbs_g=0.0,
                    fat_g=20.0,
                )
            ],
        ),
        model_used="test-model",
    )

    class _FakeFoodTask:
        def __init__(self, store: NutritionStore) -> None:
            self.nutrition = store

        @staticmethod
        def _today_local_date(_cfg: object) -> str:
            return "2026-11-17"

    _, _, _, _, gateway, app, _ = _make_stack(tmp_path)
    gateway.food_task = _FakeFoodTask(nutrition)

    with TestClient(app) as client:
        headers = {"Authorization": "Bearer super-secret-pwa-token"}

        resp = client.post(
            f"/health/api/food/entries/{meal.id}/reaction",
            headers=headers,
            json={"emoji": "🤤"},
        )
        assert resp.status_code == 200
        assert resp.json()["meals"][0]["reaction_emoji"] == "🤤"

        cleared = client.post(
            f"/health/api/food/entries/{meal.id}/reaction",
            headers=headers,
            json={"emoji": None},
        )
        assert cleared.status_code == 200
        assert cleared.json()["meals"][0]["reaction_emoji"] is None

        bad = client.post(
            f"/health/api/food/entries/{meal.id}/reaction",
            headers=headers,
            json={"emoji": "🍔"},
        )
        assert bad.status_code == 400

        missing = client.post(
            "/health/api/food/entries/999999/reaction",
            headers=headers,
            json={"emoji": "💪"},
        )
        assert missing.status_code == 404


def test_handoff_lifecycle_create_poll_and_pull(tmp_path: Path) -> None:
    from agent_tasks.common.models import TokenUsageSummary, TriageReport, WorkerEnvelope
    from agent_tasks.tasks.slack_coding import SlackCodingTask

    settings, state, _push_mgr, gateway, _composite, app, actions = _make_stack(tmp_path)
    coding_task = SlackCodingTask(
        settings=settings,
        state=state,
        dispatch_callback=AsyncMock(),
    )
    gateway.on_coding_request = lambda prompt, user, ts, chan: coding_task.handle_coding_request(
        prompt_text=prompt,
        user=user,
        thread_ts=ts,
        channel_id=chan,
    )

    async def fake_feedback(ts: str, text: str, user: str) -> None:
        actions.append(("feedback", ts, text))

    gateway.on_thread_feedback = fake_feedback

    with TestClient(app) as client:
        headers = {"Authorization": "Bearer super-secret-pwa-token"}

        create_resp = client.post(
            "/agy/api/handoff",
            headers=headers,
            json={
                "prompt": "Finish implementing the cloud handoff poller and unit tests.",
                "repo": "billthefighter/k3s-proxmox-terraform",
                "branch": "feat/handoff",
                "base_sha": "abc1234def5678",
                "base_patch": "diff --git a/foo.py b/foo.py\n--- a/foo.py\n+++ b/foo.py\n@@ -1 +1,2 @@\n x = 1\n+y = 2\n",
                "verify_hint": "uv run pytest",
                "focus_subdir": "gitops/apps/agent-tasks",
                "model": "gemini-4-argon",
                "title": "Cloud handoff poller",
                "source_conversation_id": "551438cf-f6ee-4f18-ad67-fcf842c07147",
            },
        )
        assert create_resp.status_code == 200
        created = create_resp.json()
        assert created["ok"] is True
        assert created["short_id"].startswith("HANDOFF-")
        assert created["github_repo"] == "billthefighter/k3s-proxmox-terraform"
        assert created["branch"] == "feat/handoff"
        assert created["base_sha"] == "abc1234def5678"
        assert created["had_base_patch"] is True
        assert created["status"] == "queued"
        assert created["is_terminal"] is False
        assert created["model"] == "gemini-4-argon"

        issue_id = created["issue_id"]
        short_id = created["short_id"]
        art_dir = settings.state_dir / "artifacts" / issue_id
        assert (art_dir / "base_sha.txt").read_text(encoding="utf-8").strip() == "abc1234def5678"
        assert "y = 2" in (art_dir / "initial.patch").read_text(encoding="utf-8")
        assert "y = 2" in (art_dir / "fix.patch").read_text(encoding="utf-8")
        prompt_md = (art_dir / "task_prompt.md").read_text(encoding="utf-8")
        assert "Laptop Session Handoff" in prompt_md
        assert "abc1234def5678" in prompt_md
        assert "551438cf-f6ee-4f18-ad67-fcf842c07147" in prompt_md
        assert "Pre-Applied Laptop Diff" in prompt_md

        # Poll while running -> is_terminal is still False
        state.mark_status(issue_id, IssueStatus.RUNNING)
        running_resp = client.get(
            f"/agy/api/handoff/{short_id}?include_diff=false", headers=headers
        )
        assert running_resp.status_code == 200
        assert running_resp.json()["status"] == "running"
        assert running_resp.json()["is_terminal"] is False

        # Simulate worker completion writing updated fix.patch + result.json
        updated_diff = (
            "diff --git a/foo.py b/foo.py\n--- a/foo.py\n+++ b/foo.py\n"
            "@@ -1 +1,3 @@\n x = 1\n+y = 2\n+z = 3\n"
        )
        (art_dir / "fix.patch").write_text(updated_diff, encoding="utf-8")
        envelope = WorkerEnvelope(
            issue_id=issue_id,
            turn=1,
            conversation_id="remote-conv-99",
            model="gemini-4-argon",
            stop_reason="END_TURN",
            has_patch=True,
            patch_stats=" foo.py | 2 ++\n 1 file changed, 2 insertions(+)",
            report=TriageReport(
                root_cause="Implemented remaining poller logic on top of laptop base patch.",
                fix_summary="Added z = 3 and verified unit tests.",
                affected_files=["foo.py"],
                verification_commands=["uv run pytest"],
                tests_passed=True,
                confidence="high",
            ),
            usage=TokenUsageSummary(prompt_tokens=1200, output_tokens=300, total_tokens=1500),
        )
        (art_dir / "result.json").write_text(envelope.model_dump_json(indent=2), encoding="utf-8")
        state.mark_status(
            issue_id,
            IssueStatus.AWAITING_REVIEW,
            conversation_id="remote-conv-99",
            affected_files=["foo.py"],
        )

        # Poll via /api/handoff/latest
        done_resp = client.get("/agy/api/handoff/latest", headers=headers)
        assert done_resp.status_code == 200
        done = done_resp.json()
        assert done["issue_id"] == issue_id
        assert done["short_id"] == short_id
        assert done["status"] == "awaiting_review"
        assert done["is_terminal"] is True
        assert done["has_patch"] is True
        assert "+z = 3" in done["diff"]
        assert done["report"]["fix_summary"] == "Added z = 3 and verified unit tests."
        assert done["report"]["tests_passed"] is True

        # List handoffs endpoint
        list_resp = client.get(
            "/agy/api/handoff?repo=billthefighter/k3s-proxmox-terraform", headers=headers
        )
        assert list_resp.status_code == 200
        assert len(list_resp.json()["handoffs"]) == 1
        assert list_resp.json()["handoffs"][0]["short_id"] == short_id

        # Send follow-up feedback to handoff
        fb_resp = client.post(
            f"/agy/api/handoff/{short_id}/feedback",
            headers=headers,
            json={"feedback": "Also add docstring", "model": "hydrogen"},
        )
        assert fb_resp.status_code == 200
        assert any(a[0] == "feedback" and "[hydrogen] Also add docstring" in a[2] for a in actions)


async def test_dynamic_plugin_ui_manifest_static_view_and_context_provider(tmp_path: Path) -> None:
    from agy_cloud.core.gateway import MemoryChannelBus, RecordingWorkerJobService
    from agy_cloud.core.loader import PluginRegistry

    plugins_root = tmp_path / "demo_plugins"
    plugin_dir = plugins_root / "demo_ui"
    (plugin_dir / "static").mkdir(parents=True)
    (plugin_dir / "static" / "widget.js").write_text(
        "export function init(core) { core.registerAppMode({ initialTab: 'widget' }); }\n",
        encoding="utf-8",
    )
    (plugin_dir / "plugin.yaml").write_text(
        "\n".join(
            [
                "api_version: agy.cloud/v1",
                "id: demo_ui",
                "name: Demo UI Plugin",
                "description: Standalone widget view and context provider.",
                "entrypoint: server:DemoUiPlugin",
                "push_categories:",
                "  - id: demo_alerts",
                "    label: Demo Alerts",
                "    default_enabled: true",
                "thread_kinds:",
                "  - key: demo_private",
                "    label: Demo",
                "    show_in_inbox: false",
                "chat_context_providers:",
                "  - id: demo_ctx",
                "    label: Include Demo telemetry",
                "    default_enabled: false",
                "pwa_views:",
                "  - id: widget",
                "    title: Demo Widget",
                "    short_name: Widget",
                "    mount_path: /widget",
                '    icon: "🧪"',
                "    nav_placement: topbar",
                "    script_path: static/widget.js",
                "    standalone_pwa: true",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    (plugin_dir / "server.py").write_text(
        "\n".join(
            [
                "from agy_cloud.sdk import AgyCloudPlugin",
                "",
                "class DemoUiPlugin(AgyCloudPlugin):",
                "    async def build_chat_context(self, ctx, provider_id: str) -> str:",
                '        if provider_id == "demo_ctx":',
                '            return "DEMO_CONTEXT_BLOCK: battery=99%"',
                '        return ""',
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    # 1) Without plugins: zero plugin bleed in /api/ui/manifest
    no_plugins_dir = tmp_path / "no_plugins"
    no_plugins_dir.mkdir()
    _, _, _, _, _, base_app, _ = _make_stack(no_plugins_dir)
    headers = {"Authorization": "Bearer super-secret-pwa-token"}
    with TestClient(base_app) as client:
        empty_manifest = client.get("/agy/api/ui/manifest", headers=headers).json()
        assert empty_manifest["views"] == []
        assert empty_manifest["exclude_inbox_kinds"] == []
        assert empty_manifest["chat_context_providers"] == []

    # 2) With DemoUiPlugin loaded: dynamic manifest, standalone view, static JS, context provider
    registry = PluginRegistry(
        state_root=tmp_path / "plugin_state",
        channels=MemoryChannelBus(),
        jobs=RecordingWorkerJobService(),
    )
    await registry.load_all(
        plugins_dir=plugins_root,
        include_entrypoints=False,
        install_python_deps=False,
    )

    with_plugins_dir = tmp_path / "with_plugins"
    with_plugins_dir.mkdir()
    _, _, _, gateway, _, _, _ = _make_stack(with_plugins_dir)
    gateway.plugin_registry = registry
    gateway._run_direct_chat_turn_bg = AsyncMock()
    plugin_app = create_pwa_app(gateway)

    with TestClient(plugin_app) as client:
        ui = client.get("/agy/api/ui/manifest", headers=headers).json()
        assert len(ui["views"]) == 1
        assert ui["views"][0]["id"] == "widget"
        assert ui["views"][0]["icon"] == "🧪"
        assert ui["views"][0]["nav_placement"] == "topbar"
        assert ui["views"][0]["href"] == "/widget/"
        assert ui["exclude_inbox_kinds"] == ["demo_private"]
        assert any(c["key"] == "demo_alerts" for c in ui["push_categories"])
        assert ui["chat_context_providers"] == [
            {
                "id": "demo_ctx",
                "label": "Include Demo telemetry",
                "default_enabled": False,
                "plugin_id": "demo_ui",
            }
        ]

        # Standalone PWA shell dynamically injects agy-view-script meta tag
        widget_html = client.get("/widget/").text
        assert '<meta name="agy-app-mode" content="widget" />' in widget_html
        assert (
            '<meta name="agy-view-script" content="./api/plugins/demo_ui/static/widget.js" />'
            in widget_html
        )
        assert "agy-health-base" not in widget_html

        # Plugin static asset served cleanly; path traversal blocked
        js_resp = client.get("/widget/api/plugins/demo_ui/static/widget.js")
        assert js_resp.status_code == 200
        assert "export function init(core)" in js_resp.text
        assert client.get("/widget/api/plugins/demo_ui/static/../secret.txt").status_code == 404

        # Chat context provider prepends plugin context block to prompt
        chat_resp = client.post(
            "/agy/api/chats",
            headers=headers,
            data={"prompt": "How is battery?", "context_providers": ["demo_ctx"]},
        )
        assert chat_resp.status_code == 200
        assert "DEMO_CONTEXT_BLOCK: battery=99%" in chat_resp.json()["message"]["body_md"]
