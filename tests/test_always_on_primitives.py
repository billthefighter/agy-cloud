from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import httpx
import yaml
from fastapi.testclient import TestClient

from agent_tasks.channels.slack_socket import SlackSocketAdapter
from agent_tasks.common.config import (
    CiHealerConfig,
    CiRepoConfig,
    ControllerSettings,
    K8sWatchdogConfig,
    ProjectTargetConfig,
    SentryGlobalConfig,
    TargetsConfig,
)
from agent_tasks.common.dedup import (
    compute_ci_dedup_key,
    compute_k8s_dedup_key,
    compute_sentry_dedup_key,
    parse_duration_minutes,
)
from agent_tasks.common.memory import MemoryStore, load_worker_memory_context
from agent_tasks.common.models import (
    ExceptionSummary,
    IssueStatus,
    SanitizedSentryBundle,
    StackFrameSummary,
    TriageReport,
    WorkerEnvelope,
)
from agent_tasks.common.state import IssueRecord, StateStore
from agent_tasks.pwa.gateway import PwaGatewayAdapter, create_pwa_app
from agent_tasks.pwa.push import WebPushManager
from agent_tasks.tasks.ci_healer import CiHealerTask, GitHubActionsClient
from agent_tasks.tasks.direct_chat import DirectChatService
from agent_tasks.tasks.k8s_watchdog import (
    ClusterHealthInspector,
    ClusterHealthSnapshot,
    K8sWatchdogTask,
    PodAnomaly,
)
from agent_tasks.tasks.sentry_triage.k8s_jobs import build_triage_job_manifest
from agent_tasks.tasks.sentry_triage.orchestrator import SentryTriageTask
from agent_tasks.worker_main import build_effective_system_instructions


def _make_settings(tmp_path: Path, *, sentry_token: str = "sntryu_test") -> ControllerSettings:
    targets_path = tmp_path / "targets.yaml"
    if not targets_path.exists():
        targets_path.write_text(yaml.safe_dump(TargetsConfig().model_dump()), encoding="utf-8")
    return ControllerSettings(
        state_dir=tmp_path,
        targets_config_path=targets_path,
        sentry_auth_token=sentry_token,
        target_github_token="ghp_test",
        slack_bot_token="xoxb-test",
        slack_app_token="xapp-test",
        slack_channel_id="C_DEFAULT",
        pwa_api_token="pwa-secret-token",
    )


def test_duration_parser_and_canonical_dedup_keys() -> None:
    assert parse_duration_minutes("30m") == 30
    assert parse_duration_minutes("2h") == 120
    assert parse_duration_minutes("1h30m") == 90
    assert parse_duration_minutes("1d") == 1440
    assert parse_duration_minutes("45") == 45
    assert parse_duration_minutes("invalid") is None

    staging_bundle = SanitizedSentryBundle(
        issue_id="101",
        short_id="FASTAPI-STAGING-1",
        project_slug="python-fastapi-staging",
        title="KeyError: 'sensor_id'",
        exceptions=[
            ExceptionSummary(
                exc_type="KeyError",
                exc_value="'sensor_id'",
                frames=[
                    StackFrameSummary(
                        filename="/app/rattlsnek/api/routes/sensors.py",
                        function="get_sensor",
                        lineno=42,
                        in_app=True,
                    )
                ],
            )
        ],
    )
    prod_bundle = SanitizedSentryBundle(
        issue_id="999",
        short_id="FASTAPI-PROD-8",
        project_slug="python-fastapi",
        title="KeyError: 'sensor_id'",
        exceptions=[
            ExceptionSummary(
                exc_type="KeyError",
                exc_value="'sensor_id'",
                frames=[
                    StackFrameSummary(
                        filename="rattlsnek/api/routes/sensors.py",
                        function="get_sensor",
                        lineno=44,
                        in_app=True,
                    )
                ],
            )
        ],
    )
    assert compute_sentry_dedup_key("lodbrok-app/rattlsnek", staging_bundle) == (
        compute_sentry_dedup_key("lodbrok-app/rattlsnek", prod_bundle)
    )

    anom_v1 = PodAnomaly(
        namespace="rattlsnek",
        pod_name="api-1",
        owner_name="rattlsnek-api",
        container_name="api",
        reason="CrashLoopBackOff",
        restarts=3,
        image="ghcr.io/lodbrok-app/api:sha-111",
        fingerprint="k8s-rattlsnek-api-crashloopbackoff-11111111",
    )
    anom_v2 = PodAnomaly(
        namespace="rattlsnek",
        pod_name="api-2",
        owner_name="rattlsnek-api",
        container_name="api",
        reason="CrashLoopBackOff",
        restarts=2,
        image="ghcr.io/lodbrok-app/api:sha-222",
        fingerprint="k8s-rattlsnek-api-crashloopbackoff-22222222",
    )
    assert compute_k8s_dedup_key(anom_v1) == compute_k8s_dedup_key(anom_v2)

    assert compute_ci_dedup_key("lodbrok-app/rattlsnek", "feat/x", "CI", ["pytest", "ruff"]) == (
        compute_ci_dedup_key("lodbrok-app/rattlsnek", "feat/x", "CI", ["ruff", "pytest"])
    )


async def test_sentry_settle_debounce_cross_project_dedup_and_regression_reopen(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    now = datetime.now(UTC)
    fresh_iso = (now - timedelta(seconds=30)).isoformat()
    settled_iso = (now - timedelta(minutes=10)).isoformat()

    targets = TargetsConfig(
        sentry=SentryGlobalConfig(
            settle_seconds=180,
            min_events_bypass_settle=2,
            regression_cooldown_seconds=3600,
        ),
        projects={
            "python-fastapi-staging": ProjectTargetConfig(github_repo="lodbrok-app/rattlsnek"),
            "python-fastapi": ProjectTargetConfig(github_repo="lodbrok-app/rattlsnek"),
        },
    )
    settings = _make_settings(tmp_path)
    (tmp_path / "targets.yaml").write_text(yaml.safe_dump(targets.model_dump()), encoding="utf-8")
    store = StateStore(tmp_path / "state.sqlite3")

    staging_issues: list[dict[str, Any]] = [
        # 1-event blip seen 30s ago -> must be debounced!
        {
            "id": "5001",
            "shortId": "STG-1",
            "title": "ZeroDivisionError: division by zero",
            "permalink": "https://sentry.io/issues/5001/",
            "firstSeen": fresh_iso,
            "lastSeen": fresh_iso,
            "count": "1",
        },
        # Settled issue seen 10m ago -> queued
        {
            "id": "5002",
            "shortId": "STG-2",
            "title": "ValueError: invalid payload",
            "permalink": "https://sentry.io/issues/5002/",
            "firstSeen": settled_iso,
            "lastSeen": settled_iso,
            "count": "1",
        },
    ]
    prod_issues: list[dict[str, Any]] = [
        # Different Sentry issue ID in prod, but identical stack frame as 5002 -> deduplicated!
        {
            "id": "6002",
            "shortId": "PROD-2",
            "title": "ValueError: invalid payload",
            "permalink": "https://sentry.io/issues/6002/",
            "firstSeen": settled_iso,
            "lastSeen": settled_iso,
            "count": "4",
        }
    ]
    current_release = "sha-v1"

    class FakeSentryClient:
        def __init__(self, **_kwargs: Any) -> None:
            pass

        async def fetch_recent_unresolved_issues(
            self, project_slug: str, **_kwargs: Any
        ) -> list[dict[str, Any]]:
            return staging_issues if "staging" in project_slug else prod_issues

        async def fetch_sanitized_bundle(
            self, raw_issue: dict[str, Any], project_slug: str
        ) -> SanitizedSentryBundle:
            return SanitizedSentryBundle(
                issue_id=str(raw_issue["id"]),
                short_id=str(raw_issue["shortId"]),
                project_slug=project_slug,
                title=str(raw_issue["title"]),
                permalink=str(raw_issue["permalink"]),
                release=current_release,
                exceptions=[
                    ExceptionSummary(
                        exc_type="ValueError",
                        exc_value="invalid payload",
                        frames=[
                            StackFrameSummary(
                                filename="rattlsnek/api/ingest.py",
                                function="parse_frame",
                                lineno=88,
                                in_app=True,
                            )
                        ],
                    )
                ],
            )

        async def update_issue_status(self, *_args: Any, **_kwargs: Any) -> dict[str, Any]:
            return {}

        async def add_issue_note(self, *_args: Any, **_kwargs: Any) -> dict[str, Any]:
            return {}

    monkeypatch.setattr(
        "agent_tasks.tasks.sentry_triage.orchestrator.SentryClient", FakeSentryClient
    )

    channel = AsyncMock()
    channel.resolve_channel_for_task = lambda _k: "C_SENTRY"
    task = SentryTriageTask(
        settings=settings,
        state=store,
        channel=channel,
        job_runner=MagicMock(),
        heartbeat_cb=lambda: None,
    )

    await task._poll_sentry_once(targets)

    # 5001 was debounced; 5002 was queued; 6002 collapsed into 5002 via canonical dedup_key!
    assert store.get_by_issue_id("5001") is None
    assert store.get_by_issue_id("6002") is None
    rec = store.get_by_issue_id("5002")
    assert rec is not None
    assert rec.occurrence_count == 2
    assert rec.status == IssueStatus.QUEUED

    # Mark 5002 RESOLVED at release sha-v1 (and backdate updated_at past 1h cooldown)
    store.mark_status("5002", IssueStatus.AWAITING_REVIEW, thread_ts="1700.5002")
    await task.handle_resolve("5002", "lucas")
    resolved_rec = store.get_by_issue_id("5002")
    assert resolved_rec is not None and resolved_rec.resolved_ref == "sha-v1"

    two_hours_ago = (now - timedelta(hours=2)).isoformat()
    with store._connect() as conn:
        conn.execute("UPDATE issues SET updated_at = ? WHERE issue_id = '5002'", (two_hours_ago,))

    # Poll again with SAME release sha-v1 -> suppressed (no regression reopen)
    staging_issues.clear()
    await task._poll_sentry_once(targets)
    still_resolved = store.get_by_issue_id("5002")
    assert still_resolved is not None and still_resolved.status == IssueStatus.RESOLVED

    # Now prod throws the bug on a NEW release sha-v2 -> reopens existing thread 5002 on turn 2!
    current_release = "sha-v2"
    await task._poll_sentry_once(targets)
    reopened = store.get_by_issue_id("5002")
    assert reopened is not None
    assert reopened.status == IssueStatus.QUEUED
    assert reopened.turn == 2
    assert reopened.thread_ts == "1700.5002"
    assert "sha-v2" in (reopened.banner_note or "")
    assert (tmp_path / "artifacts" / "5002" / "feedback-turn-2.txt").exists()


async def test_ci_healer_settle_and_branch_workflow_dedup(tmp_path: Path) -> None:
    settings = _make_settings(tmp_path)
    cfg = TargetsConfig(
        ci_healer=CiHealerConfig(
            enabled=True,
            settle_seconds=90,
            cooldown_seconds=3600,
            repos=[CiRepoConfig(github_repo="lodbrok-app/rattlsnek", default_branch="main")],
        )
    )
    settings.targets_config_path.write_text(yaml.safe_dump(cfg.model_dump()), encoding="utf-8")
    store = StateStore(tmp_path / "state.sqlite3")

    now = datetime.now(UTC)
    too_fresh = (now - timedelta(seconds=20)).isoformat()
    settled = (now - timedelta(minutes=5)).isoformat()

    runs_payload: list[dict[str, Any]] = [
        # Too fresh (<90s) -> debounced
        {
            "id": 2003,
            "name": "Python Tests",
            "status": "completed",
            "conclusion": "failure",
            "head_branch": "feat/dedup",
            "head_sha": "sha2003",
            "html_url": "https://github.com/lodbrok-app/rattlsnek/actions/runs/2003",
            "updated_at": too_fresh,
        },
        # Settled newest run on feat/dedup
        {
            "id": 2002,
            "name": "Python Tests",
            "status": "completed",
            "conclusion": "failure",
            "head_branch": "feat/dedup",
            "head_sha": "sha2002",
            "html_url": "https://github.com/lodbrok-app/rattlsnek/actions/runs/2002",
            "updated_at": settled,
        },
        # Older failure on the exact same PR branch & workflow -> collapsed into 2002!
        {
            "id": 2001,
            "name": "Python Tests",
            "status": "completed",
            "conclusion": "failure",
            "head_branch": "feat/dedup",
            "head_sha": "sha2001",
            "html_url": "https://github.com/lodbrok-app/rattlsnek/actions/runs/2001",
            "updated_at": settled,
        },
    ]

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/actions/runs"):
            return httpx.Response(200, json={"workflow_runs": runs_payload})
        if "/jobs" in request.url.path and not request.url.path.endswith("/logs"):
            return httpx.Response(
                200,
                json={"jobs": [{"id": 91, "name": "pytest", "conclusion": "failure", "steps": []}]},
            )
        return httpx.Response(200, text="FAILED test_x.py")

    task = CiHealerTask(
        settings=settings,
        state=store,
        actions_client_factory=lambda tok: GitHubActionsClient(
            github_token=tok, transport=httpx.MockTransport(handler)
        ),
    )

    queued = await task.poll_once(cfg)
    assert queued == 1
    assert store.get_by_issue_id("ci-2003") is None
    assert store.get_by_issue_id("ci-2001") is None
    rec = store.get_by_issue_id("ci-2002")
    assert rec is not None
    assert rec.occurrence_count == 2

    # Once 2003 settles on next poll while ci-2002 is still QUEUED, prompt refreshes in-place!
    runs_payload[0]["updated_at"] = settled
    assert await task.poll_once(cfg) == 0
    refreshed = store.get_by_issue_id("ci-2002")
    assert refreshed is not None
    assert "sha2003" in (tmp_path / "artifacts" / "ci-2002" / "task_prompt.md").read_text(
        encoding="utf-8"
    )

    # Once ci-2002 moves to AWAITING_REVIEW, a 4th failing commit on feat/dedup does NOT spawn a new Job
    store.mark_status("ci-2002", IssueStatus.AWAITING_REVIEW, thread_ts="1700.2002")
    runs_payload.insert(
        0,
        {
            "id": 2004,
            "name": "Python Tests",
            "status": "completed",
            "conclusion": "failure",
            "head_branch": "feat/dedup",
            "head_sha": "sha2004",
            "html_url": "https://github.com/lodbrok-app/rattlsnek/actions/runs/2004",
            "updated_at": settled,
        },
    )
    assert await task.poll_once(cfg) == 0
    assert store.get_by_issue_id("ci-2004") is None


async def test_k8s_watchdog_multi_poll_debounce_image_dedup_and_episode_recovery(
    tmp_path: Path,
) -> None:
    settings = _make_settings(tmp_path)
    store = StateStore(tmp_path / "state.sqlite3")
    targets = TargetsConfig(
        k8s_watchdog=K8sWatchdogConfig(
            enabled=True,
            settle_polls=2,
            recovery_polls=2,
            reopen_cooldown_seconds=60,
        )
    )

    anom_img1 = PodAnomaly(
        namespace="rattlsnek",
        pod_name="rattlsnek-api-111",
        owner_name="rattlsnek-api",
        container_name="api",
        reason="CrashLoopBackOff",
        restarts=3,
        image="ghcr.io/lodbrok-app/api:sha-111",
        fingerprint="k8s-rattlsnek-api-crashloopbackoff-11111111",
        message="crash",
    )
    anom_img2 = PodAnomaly(
        namespace="rattlsnek",
        pod_name="rattlsnek-api-222",
        owner_name="rattlsnek-api",
        container_name="api",
        reason="CrashLoopBackOff",
        restarts=2,
        image="ghcr.io/lodbrok-app/api:sha-222",
        fingerprint="k8s-rattlsnek-api-crashloopbackoff-22222222",
        message="crash",
    )

    current_anoms: list[PodAnomaly] = [anom_img1]
    fake_inspector = MagicMock(spec=ClusterHealthInspector)

    async def _inspect(_ns: list[str], **_kwargs) -> ClusterHealthSnapshot:
        return ClusterHealthSnapshot(
            checked_at=datetime.now(UTC).isoformat(),
            pod_anomalies=list(current_anoms),
        )

    fake_inspector.inspect.side_effect = _inspect
    fake_inspector.collect_diagnostic_context = AsyncMock(return_value="logs")

    task = K8sWatchdogTask(settings=settings, state=store, inspector=fake_inspector)

    # Poll 1: consecutive_hits=1 < settle_polls=2 -> debounced!
    await task.poll_once(targets)
    assert store.get_by_issue_id(anom_img1.fingerprint) is None

    # Poll 2: consecutive_hits=2 == settle_polls=2 -> promoted & queued!
    await task.poll_once(targets)
    rec = store.get_by_issue_id(anom_img1.fingerprint)
    assert rec is not None and rec.status == IssueStatus.QUEUED

    # Poll 3: new image tag `:sha-222` crashes for the SAME owner/reason while awaiting review -> deduplicated!
    store.mark_status(rec.issue_id, IssueStatus.AWAITING_REVIEW, thread_ts="1700.k8s")
    current_anoms = [anom_img2]
    await task.poll_once(targets)
    assert store.get_by_issue_id(anom_img2.fingerprint) is None
    assert (store.get_by_issue_id(rec.issue_id) or rec).occurrence_count == 2

    # Mark resolved, then let cluster heal for 2 clean polls (`recovery_polls=2`)
    store.mark_status(rec.issue_id, IssueStatus.RESOLVED)
    past_iso = (datetime.now(UTC) - timedelta(minutes=10)).isoformat()
    with store._connect() as conn:
        conn.execute(
            "UPDATE issues SET updated_at = ? WHERE issue_id = ?", (past_iso, rec.issue_id)
        )

    current_anoms = []
    await task.poll_once(targets)
    await task.poll_once(targets)
    obs = store.get_signal_observation("k8s:rattlsnek:rattlsnek-api:crashloopbackoff")
    assert obs is not None and obs.episode_open is False and obs.recovered_at is not None

    # Later real outage (2 consecutive polls) reopens existing thread on turn 2!
    current_anoms = [anom_img2]
    await task.poll_once(targets)  # hit 1 (debouncing new episode)
    assert (store.get_by_issue_id(rec.issue_id) or rec).status == IssueStatus.RESOLVED
    await task.poll_once(targets)  # hit 2 -> reopens!
    reopened = store.get_by_issue_id(rec.issue_id)
    assert reopened is not None
    assert reopened.status == IssueStatus.QUEUED
    assert reopened.turn == 2


async def test_timers_silent_exit_locus_coalescing_buffered_replies_and_memory(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    settings = _make_settings(tmp_path, sentry_token="")
    targets = TargetsConfig(
        projects={"python-edge": ProjectTargetConfig(github_repo="lodbrok-app/rattlsnek")}
    )
    settings.targets_config_path.write_text(yaml.safe_dump(targets.model_dump()), encoding="utf-8")
    store = StateStore(tmp_path / "state.sqlite3")

    async def _fake_sync_mirror(mirrors_root: Path, _repo: str, _tok: str) -> Path:
        p = mirrors_root / "lodbrok-app__rattlsnek.git"
        p.mkdir(parents=True, exist_ok=True)
        return p

    monkeypatch.setattr(
        "agent_tasks.tasks.sentry_triage.orchestrator.sync_bare_mirror", _fake_sync_mirror
    )

    spawned: list[IssueRecord] = []
    finished_turns: set[tuple[str, int]] = set()

    class FakeRunner:
        async def spawn_job(self, record: IssueRecord, _target: Any) -> str:
            spawned.append(record)
            return f"job-{record.issue_id}-t{record.turn}"

        async def get_job_terminal_state(self, issue_id: str, turn: int) -> str | None:
            return "succeeded" if (issue_id, turn) in finished_turns else None

    thread_msgs: list[tuple[str, str]] = []
    triage_cards: list[str] = []
    channel = AsyncMock()
    channel.resolve_channel_for_task = lambda _k: "C_OPS"

    async def _post_triage(record: IssueRecord, **_kw: Any) -> str:
        triage_cards.append(record.issue_id)
        return record.thread_ts or f"ts-{record.issue_id}"

    async def _post_thread(ts: str, text: str, **_kw: Any) -> None:
        thread_msgs.append((ts, text))

    channel.post_triage_result = AsyncMock(side_effect=_post_triage)
    channel.post_thread_message = AsyncMock(side_effect=_post_thread)

    task = SentryTriageTask(
        settings=settings,
        state=store,
        channel=channel,
        job_runner=FakeRunner(),  # type: ignore[arg-type]
        heartbeat_cb=lambda: None,
    )

    # 1. Silent exit + agent-set follow-up timer (`should_notify=False`, `followup_in_minutes=15`)
    store.register_if_new(
        issue_id="k8s-1",
        short_id="K8S-001",
        project_slug="rattlsnek",
        github_repo="lodbrok-app/rattlsnek",
        default_branch="main",
        title="Transient migration lock",
        permalink="",
        task_kind="k8s_watchdog",
    )
    store.mark_status("k8s-1", IssueStatus.RUNNING)
    (task.artifact_dir("k8s-1") / "result.json").write_text(
        WorkerEnvelope(
            issue_id="k8s-1",
            turn=1,
            conversation_id="conv-k8s-1",
            model="gemini-4-hydrogen",
            report=TriageReport(
                root_cause="DB migration job still running",
                confidence="high",
                fix_summary="No code change; verify pod stabilizes in 15m.",
                tests_passed=True,
                should_notify=False,
                followup_in_minutes=15,
                followup_reason="Confirm rattlsnek-api pod is Ready after migration.",
            ),
        ).model_dump_json(),
        encoding="utf-8",
    )
    finished_turns.add(("k8s-1", 1))
    await task._reconcile_running_jobs()

    # Zero noisy Slack cards posted; status is SNOOZED with 1 pending timer!
    assert triage_cards == []
    snoozed = store.get_by_issue_id("k8s-1")
    assert snoozed is not None and snoozed.status == IssueStatus.SNOOZED
    pending_timers = store.list_pending_timers()
    assert len(pending_timers) == 1
    assert "Confirm rattlsnek-api" in pending_timers[0].prompt

    # Advance clock +16 minutes -> timer fires, wakes k8s-1 on turn 2, and spawns Job!
    woken = await task.process_due_timers(now=datetime.now(UTC) + timedelta(minutes=16))
    assert woken == 1
    assert spawned[-1].issue_id == "k8s-1" and spawned[-1].turn == 2
    assert "Scheduled Follow-Up Timer" in (
        task.artifact_dir("k8s-1") / "feedback-turn-2.txt"
    ).read_text(encoding="utf-8")

    # 2. Mid-run thread reply buffering + Proposed Repo Memory + Post-triage locus coalescing
    store.register_if_new(
        issue_id="sentry-main",
        short_id="EDGE-10",
        project_slug="python-edge",
        github_repo="lodbrok-app/rattlsnek",
        default_branch="main",
        title="Parser crash",
        permalink="https://sentry.io/issues/10/",
        thread_ts="ts-edge-10",
    )
    store.mark_status("sentry-main", IssueStatus.RUNNING, thread_ts="ts-edge-10")

    # User replies while turn 1 is RUNNING -> buffered instead of dropped!
    await task.handle_thread_feedback("ts-edge-10", "[carbon] add edge case test too", "lucas")
    assert any("Buffered follow-up #1" in msg for _, msg in thread_msgs)

    # Turn 1 finishes touching `rattlsnek/edge/parser.py` and proposing a repo memory note
    (task.artifact_dir("sentry-main") / "fix.patch").write_text(
        "--- a/rattlsnek/edge/parser.py\n+++ b/rattlsnek/edge/parser.py\n", encoding="utf-8"
    )
    (task.artifact_dir("sentry-main") / "result.json").write_text(
        WorkerEnvelope(
            issue_id="sentry-main",
            turn=1,
            conversation_id="conv-edge-10",
            model="gemini-4-hydrogen",
            has_patch=True,
            report=TriageReport(
                root_cause="Null tag",
                confidence="high",
                affected_files=["rattlsnek/edge/parser.py"],
                fix_summary="Guard null tag",
                tests_passed=True,
                proposed_memory_note="Run `uv sync --package edge` before `pytest edge`.",
            ),
        ).model_dump_json(),
        encoding="utf-8",
    )
    finished_turns.add(("sentry-main", 1))
    await task._reconcile_running_jobs()

    # Triage card posted AND buffered feedback immediately dispatched turn 2 with `[carbon]`!
    assert "sentry-main" in triage_cards
    assert spawned[-1].issue_id == "sentry-main"
    assert spawned[-1].turn == 2
    assert spawned[-1].model_override == "gemini-4-carbon"
    assert (task.artifact_dir("sentry-main") / "feedback-turn-2.txt").read_text(
        encoding="utf-8"
    ) == "add edge case test too"

    # Complete turn 2 so `sentry-main` sits in AWAITING_REVIEW touching `rattlsnek/edge/parser.py`
    (task.artifact_dir("sentry-main") / "result.json").write_text(
        WorkerEnvelope(
            issue_id="sentry-main",
            turn=2,
            conversation_id="conv-edge-10",
            model="gemini-4-carbon",
            has_patch=True,
            report=TriageReport(
                root_cause="Null tag",
                confidence="high",
                affected_files=["rattlsnek/edge/parser.py"],
                fix_summary="Guard null tag + edge case test",
                tests_passed=True,
                proposed_memory_note="Run `uv sync --package edge` before `pytest edge`.",
            ),
        ).model_dump_json(),
        encoding="utf-8",
    )
    finished_turns.add(("sentry-main", 2))
    await task._reconcile_running_jobs()

    # Now a separate CI failure (`ci-overlap`) finishes Turn 1 implicating the SAME file -> coalesced!
    store.register_if_new(
        issue_id="ci-overlap",
        short_id="CI-777",
        project_slug="rattlsnek",
        github_repo="lodbrok-app/rattlsnek",
        default_branch="main",
        title="CI failed in parser",
        permalink="https://github.com/lodbrok-app/rattlsnek/actions/runs/777",
        task_kind="ci_healer",
    )
    store.mark_status("ci-overlap", IssueStatus.RUNNING)
    (task.artifact_dir("ci-overlap") / "result.json").write_text(
        WorkerEnvelope(
            issue_id="ci-overlap",
            turn=1,
            conversation_id="conv-ci-777",
            model="gemini-4-hydrogen",
            report=TriageReport(
                root_cause="Same parser regression",
                confidence="high",
                affected_files=["rattlsnek/edge/parser.py"],
                fix_summary="Fix parser",
                tests_passed=True,
            ),
        ).model_dump_json(),
        encoding="utf-8",
    )
    finished_turns.add(("ci-overlap", 1))
    cards_before = len(triage_cards)
    await task._reconcile_running_jobs()
    assert len(triage_cards) == cards_before  # No duplicate top-level card!
    overlap_rec = store.get_by_issue_id("ci-overlap")
    assert overlap_rec is not None and overlap_rec.status == IssueStatus.SILENT
    assert any(
        ts == "ts-edge-10" and "Coalesced overlapping `ci_healer` signal" in msg
        for ts, msg in thread_msgs
    )

    # 3. Approve proposed repo memory + `!remember` / `!forget` + verify read-only worker mount
    await task.handle_save_memory_note("sentry-main", "lucas")
    await task.handle_memory_command(
        "remember", "Never upgrade major Astro versions unprompted", "lucas"
    )
    mem_summary = await task.handle_memory_command("show", "", "lucas")
    assert "uv sync --package edge" in mem_summary
    assert "Never upgrade major Astro versions" in mem_summary

    sys_prompt = build_effective_system_instructions(tmp_path / "memory", "lodbrok-app/rattlsnek")
    assert "<persistent_memory>" in sys_prompt
    assert "uv sync --package edge" in sys_prompt
    assert "Never upgrade major Astro versions" in sys_prompt

    manifest = build_triage_job_manifest(
        settings=settings,
        record=store.get_by_issue_id("sentry-main") or spawned[0],
        target=targets.projects["python-edge"],
    )
    worker_mounts = {
        m.mount_path: m for m in manifest.spec.template.spec.containers[0].volume_mounts
    }
    assert "/memory" in worker_mounts
    assert worker_mounts["/memory"].read_only is True
    assert worker_mounts["/memory"].sub_path == "memory"


async def test_slack_and_pwa_snooze_timers_and_memory_endpoints(tmp_path: Path) -> None:
    settings = _make_settings(tmp_path, sentry_token="")
    store = StateStore(tmp_path / "state.sqlite3")
    mem = MemoryStore(tmp_path / "memory")
    entry = mem.add_instruction("Keep patches surgical </persistent_memory>", author="lucas")
    assert (
        "</persistent_memory>"
        not in load_worker_memory_context(tmp_path / "memory")
        .split("<persistent_memory>")[1]
        .split("</persistent_memory>")[0]
    )

    # Slack command routing for !memory, !remember, !forget, !timers, !snooze
    posted: list[dict[str, Any]] = []

    async def _post(**kwargs: Any) -> dict[str, str]:
        posted.append(kwargs)
        return {"ts": "99.1"}

    adapter = SlackSocketAdapter(
        bot_token="xoxb-test",
        app_token="xapp-test",
        channel_id="C1",
        on_open_draft_pr=AsyncMock(),
        on_resolve=AsyncMock(),
        on_ignore=AsyncMock(),
        on_thread_feedback=AsyncMock(),
        on_save_memory_note=AsyncMock(),
        on_snooze=AsyncMock(return_value="snoozed-ok"),
        on_timers_command=AsyncMock(return_value="timers-ok"),
        on_memory_command=AsyncMock(return_value="memory-ok"),
    )
    adapter.app.client.chat_postMessage = _post  # type: ignore[method-assign]

    await adapter.process_incoming_message(
        {"channel": "C1", "user": "U1", "ts": "1.1", "text": "!timers"},
        from_mention=False,
    )
    assert posted[-1]["text"] == "timers-ok"

    await adapter.process_incoming_message(
        {"channel": "C1", "user": "U1", "ts": "1.2", "thread_ts": "1.0", "text": "!snooze 2h wait"},
        from_mention=False,
    )
    assert posted[-1]["text"] == "snoozed-ok"

    await adapter.process_incoming_message(
        {"channel": "C1", "user": "U1", "ts": "1.3", "text": "!remember always run ruff"},
        from_mention=False,
    )
    assert posted[-1]["text"] == "memory-ok"

    # PWA endpoints for timers + memory + save_memory_note button
    push_mgr = WebPushManager(settings=settings, state=store, sender=lambda *_a: None)
    chat_svc = DirectChatService(settings=settings, state=store)
    saved_notes: list[tuple[str, str]] = []

    async def _save_mem(issue_id: str, user: str) -> None:
        saved_notes.append((issue_id, user))

    gateway = PwaGatewayAdapter(
        settings=settings,
        state=store,
        push_manager=push_mgr,
        direct_chat=chat_svc,
        on_save_memory_note=_save_mem,
        on_snooze=AsyncMock(return_value="⏰ Snoozed via PWA"),
        on_memory_command=AsyncMock(return_value="Saved via PWA"),
    )
    rec = store.register_if_new(
        issue_id="pwa-issue-1",
        short_id="EDGE-55",
        project_slug="python-edge",
        github_repo="lodbrok-app/rattlsnek",
        default_branch="main",
        title="Test PWA memory",
        permalink="",
    )
    assert rec is not None
    await gateway.post_triage_result(
        rec,
        TriageReport(
            root_cause="bug",
            confidence="high",
            fix_summary="fix",
            tests_passed=True,
            proposed_memory_note="Use targeted pytest paths.",
        ),
        diff_text="",
    )
    timer = store.schedule_timer(
        dedup_key="issue:pwa-issue-1",
        issue_id="pwa-issue-1",
        fire_at=(datetime.now(UTC) + timedelta(hours=1)).isoformat(),
        prompt="recheck",
    )

    app = create_pwa_app(gateway)
    headers = {"Authorization": "Bearer pwa-secret-token"}
    with TestClient(app) as client:
        r_mem = client.get("/agy/api/ops/memory", headers=headers)
        assert r_mem.status_code == 200
        assert any(e["entry_id"] == entry.entry_id for e in r_mem.json()["entries"])

        r_add = client.post(
            "/agy/api/ops/memory",
            json={"text": "Prefer uv sync", "scope": "repo:lodbrok-app/rattlsnek"},
            headers=headers,
        )
        assert r_add.status_code == 200

        r_act = client.post(
            "/agy/api/threads/pwa-issue-1/actions",
            json={"action": "save_memory_note"},
            headers=headers,
        )
        assert r_act.status_code == 200
        assert saved_notes == [("pwa-issue-1", "mobile")]

        r_snz = client.post(
            "/agy/api/threads/pwa-issue-1/snooze",
            json={"duration": "45m", "note": "after deploy"},
            headers=headers,
        )
        assert r_snz.status_code == 200

        r_tmr = client.get("/agy/api/ops/timers", headers=headers)
        assert r_tmr.status_code == 200
        assert len(r_tmr.json()["timers"]) == 1

        r_del = client.delete(f"/agy/api/ops/timers/{timer.id}", headers=headers)
        assert r_del.status_code == 200
        assert store.list_pending_timers() == []
