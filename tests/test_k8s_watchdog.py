from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest
from kubernetes import client
from kubernetes.client.rest import ApiException

from agent_tasks.common.config import ControllerSettings, K8sWatchdogConfig, TargetsConfig
from agent_tasks.common.models import IssueStatus
from agent_tasks.common.state import StateStore
from agent_tasks.tasks import LongLivedTask
from agent_tasks.tasks.k8s_watchdog import (
    ArgoAnomaly,
    ClusterHealthInspector,
    ClusterHealthSnapshot,
    K8sWatchdogTask,
    PodAnomaly,
    detect_argo_anomalies,
    detect_pod_anomalies,
    redact_secrets,
)


def _make_settings(tmp_path: Path) -> ControllerSettings:
    targets_path = tmp_path / "targets.yaml"
    return ControllerSettings(
        namespace="agent-tasks",
        node_name="k3s-worker-1",
        state_dir=tmp_path,
        targets_config_path=targets_path,
        target_github_token="ghp_controller_only",
        slack_bot_token="xoxb-controller-only",
        slack_app_token="xapp-controller-only",
        slack_channel_id="C123456",
    )


def _make_pod_dict(
    *,
    name: str,
    owner_name: str | None = None,
    owner_kind: str = "ReplicaSet",
    labels: dict[str, str] | None = None,
    phase: str = "Running",
    ready: bool = False,
    container_name: str = "app",
    image: str = "ghcr.io/lodbrok-app/rattlsnek-api:1.2.3",
    waiting_reason: str | None = None,
    waiting_message: str = "",
    last_terminated_reason: str | None = None,
    last_terminated_message: str = "",
    restart_count: int = 0,
    init_container_statuses: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    state: dict[str, Any] = {}
    if waiting_reason is not None:
        state["waiting"] = {"reason": waiting_reason, "message": waiting_message}
    elif phase == "Running":
        state["running"] = {"startedAt": "2026-11-15T20:00:00Z"}

    last_state: dict[str, Any] = {}
    if last_terminated_reason is not None:
        last_state["terminated"] = {
            "reason": last_terminated_reason,
            "message": last_terminated_message,
        }

    owner_refs = [{"kind": owner_kind, "name": owner_name}] if owner_name else []

    return {
        "metadata": {
            "name": name,
            "labels": labels or {},
            "owner_references": owner_refs,
        },
        "status": {
            "phase": phase,
            "conditions": [{"type": "Ready", "status": "True" if ready else "False"}],
            "init_container_statuses": init_container_statuses or [],
            "container_statuses": [
                {
                    "name": container_name,
                    "image": image,
                    "ready": ready,
                    "restart_count": restart_count,
                    "state": state,
                    "last_state": last_state,
                }
            ],
        },
    }


def test_redact_secrets_strips_tokens_and_dsn_passwords() -> None:
    raw = (
        "Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.payload.sig\n"
        "SLACK_BOT_TOKEN=xoxb-1234567890-abcdef\n"
        "SLACK_USER_TOKEN=xoxp-999-888-secret\n"
        "SLACK_APP_LEGACY=xoxa-111-222-secret\n"
        "SENTRY_AUTH_TOKEN=sntryu_deadbeef1234567890\n"
        "GITHUB_TOKEN=ghp_superSecretToken123\n"
        "GH_ACTIONS=ghs_actionsSecret456\n"
        "GOOGLE_API_KEY=AIzaSyDummyKeyValue_123-abc\n"
        "GEMINI_API_KEY=AQ.Ab8RN6ExampleKey.With_Dash-9\n"
        "DATABASE_URL=postgresql+asyncpg://rattlsnek:myDbPass99@postgres.rattlsnek.svc:5432/db\n"
        "REDIS_URL=redis://:redisSecret@redis.rattlsnek.svc:6379/0\n"
        "PublicRepo=https://github.com/lodbrok-app/rattlsnek\n"
    )
    scrubbed = redact_secrets(raw)

    for secret in (
        "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.payload.sig",
        "xoxb-1234567890-abcdef",
        "xoxp-999-888-secret",
        "xoxa-111-222-secret",
        "sntryu_deadbeef1234567890",
        "ghp_superSecretToken123",
        "ghs_actionsSecret456",
        "AIzaSyDummyKeyValue_123-abc",
        "AQ.Ab8RN6ExampleKey.With_Dash-9",
        "myDbPass99",
        "redisSecret",
    ):
        assert secret not in scrubbed

    assert "Authorization: Bearer ***" in scrubbed
    assert "postgresql+asyncpg://rattlsnek:***@postgres.rattlsnek.svc:5432/db" in scrubbed
    assert "redis://:***@redis.rattlsnek.svc:6379/0" in scrubbed
    assert "https://github.com/lodbrok-app/rattlsnek" in scrubbed


def test_detect_pod_anomalies_ignores_agy_worker_and_handles_crash_pull_oom() -> None:
    # 1. agy-worker pod in CrashLoopBackOff must be ignored
    worker_pod = _make_pod_dict(
        name="agy-triage-k8s-abc-t1-xyz12",
        labels={"app.kubernetes.io/component": "agy-worker"},
        waiting_reason="CrashLoopBackOff",
        waiting_message="back-off restarting failed container",
        restart_count=5,
    )

    # 2. CrashLoopBackOff pod owned by ReplicaSet -> strip RS hash suffix
    crash_pod = _make_pod_dict(
        name="rattlsnek-api-7d9f8c6b4d-x2k9p",
        owner_name="rattlsnek-api-7d9f8c6b4d",
        owner_kind="ReplicaSet",
        waiting_reason="CrashLoopBackOff",
        waiting_message="DSN failed postgres://api:secretpw@db:5432/api",
        restart_count=4,
    )

    # 3. ImagePullBackOff on initContainer using real kubernetes.client.V1Pod
    init_pull_pod = client.V1Pod(
        metadata=client.V1ObjectMeta(
            name="rattlsnek-migrate-abc12",
            owner_references=[
                client.V1OwnerReference(
                    api_version="batch/v1", kind="Job", name="db-migrate", uid="uid-1"
                )
            ],
        ),
        status=client.V1PodStatus(
            phase="Pending",
            init_container_statuses=[
                client.V1ContainerStatus(
                    name="wait-for-db",
                    image="ghcr.io/lodbrok-app/missing:latest",
                    image_id="",
                    ready=False,
                    restart_count=0,
                    state=client.V1ContainerState(
                        waiting=client.V1ContainerStateWaiting(
                            reason="ImagePullBackOff",
                            message="Back-off pulling image",
                        )
                    ),
                )
            ],
            container_statuses=[],
        ),
    )

    # 4. OOMKilled with restart_count == 1 must NOT alert yet
    oom_once = _make_pod_dict(
        name="edge-ingest-0",
        owner_name="edge-ingest",
        owner_kind="StatefulSet",
        last_terminated_reason="OOMKilled",
        restart_count=1,
        ready=True,
    )

    # 5. OOMKilled with restart_count == 2 MUST alert
    oom_twice = _make_pod_dict(
        name="edge-processor-6b7c8d9f4-p4m2q",
        owner_name="edge-processor-6b7c8d9f4",
        owner_kind="ReplicaSet",
        last_terminated_reason="OOMKilled",
        last_terminated_message="Exit code 137",
        restart_count=2,
        ready=True,
    )

    # 6. Healthy running pod -> no anomaly
    healthy_pod = _make_pod_dict(
        name="healthy-web-778899aabb-11223",
        owner_name="healthy-web-778899aabb",
        ready=True,
    )

    anomalies = detect_pod_anomalies(
        [worker_pod, crash_pod, init_pull_pod, oom_once, oom_twice, healthy_pod],
        namespace="rattlsnek",
    )

    reasons_by_owner = {a.owner_name: a for a in anomalies}
    assert set(reasons_by_owner.keys()) == {"rattlsnek-api", "db-migrate", "edge-processor"}

    crash_anom = reasons_by_owner["rattlsnek-api"]
    assert crash_anom.reason == "CrashLoopBackOff"
    assert crash_anom.restarts == 4
    assert "secretpw" not in crash_anom.message
    assert "postgres://api:***@db:5432/api" in crash_anom.message
    assert len(crash_anom.fingerprint) <= 48
    assert crash_anom.fingerprint.startswith("k8s-rattlsnek-")
    assert "-crashloopbackoff-" in crash_anom.fingerprint

    pull_anom = reasons_by_owner["db-migrate"]
    assert pull_anom.reason == "ImagePullBackOff"
    assert pull_anom.container_name == "wait-for-db"
    assert len(pull_anom.fingerprint) <= 48

    oom_anom = reasons_by_owner["edge-processor"]
    assert oom_anom.reason == "OOMKilled"
    assert oom_anom.restarts == 2
    assert len(oom_anom.fingerprint) <= 48

    # Deterministic fingerprint across calls, and distinct if image changes
    again = detect_pod_anomalies([crash_pod], namespace="rattlsnek")
    assert again[0].fingerprint == crash_anom.fingerprint

    new_image_pod = _make_pod_dict(
        name="rattlsnek-api-7d9f8c6b4d-zzzzz",
        owner_name="rattlsnek-api-7d9f8c6b4d",
        waiting_reason="CrashLoopBackOff",
        image="ghcr.io/lodbrok-app/rattlsnek-api:1.2.4",
        restart_count=3,
    )
    different = detect_pod_anomalies([new_image_pod], namespace="rattlsnek")
    assert different[0].fingerprint != crash_anom.fingerprint

    # Fallback to pod_name when owner_references is empty
    standalone = _make_pod_dict(
        name="standalone-pod",
        owner_name=None,
        waiting_reason="ErrImagePull",
    )
    standalone_anoms = detect_pod_anomalies([standalone], namespace="monitoring")
    assert len(standalone_anoms) == 1
    assert standalone_anoms[0].owner_name == "standalone-pod"
    assert standalone_anoms[0].reason == "ErrImagePull"


def test_detect_pod_anomalies_fingerprint_bounded_for_long_names() -> None:
    pod = _make_pod_dict(
        name="very-long-service-name-production-workload-123456-abcde",
        waiting_reason="CreateContainerConfigError",
        image="ghcr.io/lodbrok-app/extremely-long-image-name:v99.99.99",
    )
    anomalies = detect_pod_anomalies([pod], namespace="agent-tasks")
    assert len(anomalies) == 1
    fp = anomalies[0].fingerprint
    assert len(fp) <= 48
    assert fp.startswith("k8s-")
    assert all(ch.isdigit() or ("a" <= ch <= "z") or ch == "-" for ch in fp)


def test_detect_argo_anomalies_root_ignored_and_outofsync_requires_persistent_streak() -> None:
    consecutive: dict[str, int] = {}

    def make_app(name: str, sync: str, health: str, msg: str = "") -> dict[str, Any]:
        return {
            "metadata": {"name": name},
            "status": {
                "sync": {"status": sync},
                "health": {"status": health, "message": msg},
                "conditions": [],
            },
        }

    # Poll 1: root and agent-tasks (even if Degraded/OutOfSync) are ignored;
    # rattlsnek OutOfSync once (count=1 -> not flagged); monitoring Degraded -> flagged immediately.
    poll1 = [
        make_app("root", "OutOfSync", "Healthy"),
        make_app("agent-tasks", "OutOfSync", "Degraded", "Ingress waiting"),
        make_app("rattlsnek", "OutOfSync", "Healthy"),
        make_app("monitoring", "Synced", "Degraded", "Probe failed Bearer topsecret"),
    ]
    res1 = detect_argo_anomalies(poll1, consecutive_out_of_sync=consecutive, out_of_sync_polls=3)
    assert [a.app_name for a in res1] == ["monitoring"]
    assert "topsecret" not in res1[0].message
    assert "Bearer ***" in res1[0].message
    assert "root" not in consecutive
    assert "agent-tasks" not in consecutive
    assert consecutive["rattlsnek"] == 1

    # Custom ignored_apps overrides the default set
    custom_res = detect_argo_anomalies(
        [
            make_app("agent-tasks", "Synced", "Degraded", "Broken"),
            make_app("monitoring", "Synced", "Degraded", "Ignored custom"),
        ],
        consecutive_out_of_sync={},
        ignored_apps=["monitoring"],
    )
    assert [a.app_name for a in custom_res] == ["agent-tasks"]

    # Poll 2: rattlsnek Synced resets streak to 0
    poll2 = [
        make_app("root", "OutOfSync", "Healthy"),
        make_app("rattlsnek", "Synced", "Healthy"),
        make_app("monitoring", "Synced", "Healthy"),
    ]
    res2 = detect_argo_anomalies(poll2, consecutive_out_of_sync=consecutive, out_of_sync_polls=3)
    assert res2 == []
    assert consecutive["rattlsnek"] == 0

    # Polls 3 & 4: rattlsnek OutOfSync twice -> count=2, still below persistent threshold (3)
    poll3 = [
        make_app("root", "OutOfSync", "Healthy"),
        make_app("rattlsnek", "OutOfSync", "Healthy"),
    ]
    assert (
        detect_argo_anomalies(poll3, consecutive_out_of_sync=consecutive, out_of_sync_polls=3) == []
    )
    assert consecutive["rattlsnek"] == 1
    assert (
        detect_argo_anomalies(poll3, consecutive_out_of_sync=consecutive, out_of_sync_polls=3) == []
    )
    assert consecutive["rattlsnek"] == 2

    # Read-only status inspection (update_streaks=False) does not advance streak
    assert (
        detect_argo_anomalies(
            poll3,
            consecutive_out_of_sync=consecutive,
            out_of_sync_polls=3,
            update_streaks=False,
        )
        == []
    )
    assert consecutive["rattlsnek"] == 2

    # Poll 5: rattlsnek OutOfSync third consecutive poll -> flagged!
    res5 = detect_argo_anomalies(poll3, consecutive_out_of_sync=consecutive, out_of_sync_polls=3)
    assert [a.app_name for a in res5] == ["rattlsnek"]
    assert consecutive["rattlsnek"] == 3
    assert len(res5[0].fingerprint) <= 48


@pytest.mark.parametrize("status_code", [403, 404])
async def test_inspector_handles_argo_api_403_404_gracefully(status_code: int) -> None:
    core_v1 = MagicMock()
    core_v1.list_namespaced_pod.return_value = {
        "items": [
            _make_pod_dict(name="api-1", ready=True),
            _make_pod_dict(name="api-2", waiting_reason="CrashLoopBackOff", restart_count=3),
        ]
    }

    custom_objects = MagicMock()
    custom_objects.list_namespaced_custom_object.side_effect = ApiException(
        status=status_code, reason="Forbidden" if status_code == 403 else "Not Found"
    )

    inspector = ClusterHealthInspector(core_v1=core_v1, custom_objects=custom_objects)
    snapshot = await inspector.inspect(["rattlsnek"])

    assert snapshot.namespace_pod_counts == {"rattlsnek": (1, 2)}
    assert len(snapshot.pod_anomalies) == 1
    assert len(snapshot.errors) == 1
    assert str(status_code) in snapshot.errors[0]
    core_v1.list_namespaced_pod.assert_called_once_with(namespace="rattlsnek", _request_timeout=15)


async def test_inspector_collect_diagnostic_context_previous_fallback_and_redaction() -> None:
    core_v1 = MagicMock()
    # First call (previous=True) raises 400 (no previous container), second (previous=False) returns logs
    core_v1.read_namespaced_pod_log.side_effect = [
        ApiException(status=400, reason="Bad Request"),
        "Connecting to postgres://app:pw123@pg:5432/db\nToken ghp_secretABC",
    ]
    core_v1.list_namespaced_event.return_value = {
        "items": [
            {
                "type": "Normal",
                "reason": "Scheduled",
                "message": "Successfully assigned",
                "involved_object": {"name": "api-1"},
            },
            {
                "type": "Warning",
                "reason": "BackOff",
                "count": 5,
                "message": "Back-off restarting with sntryu_tok999",
                "involved_object": {"name": "api-1"},
            },
        ]
    }
    custom_objects = MagicMock()
    inspector = ClusterHealthInspector(core_v1=core_v1, custom_objects=custom_objects)

    pod_anom = PodAnomaly(
        namespace="rattlsnek",
        pod_name="api-1",
        owner_name="api",
        container_name="api",
        reason="CrashLoopBackOff",
        restarts=3,
        image="img:1",
        fingerprint="k8s-rattlsnek-api-crashloopbackoff-12345678",
        message="crash",
    )
    pod_ctx = await inspector.collect_diagnostic_context(pod_anom)

    assert core_v1.read_namespaced_pod_log.call_count == 2
    assert core_v1.read_namespaced_pod_log.call_args_list[0].kwargs["previous"] is True
    assert core_v1.read_namespaced_pod_log.call_args_list[1].kwargs["previous"] is False
    assert "pw123" not in pod_ctx
    assert "ghp_secretABC" not in pod_ctx
    assert "sntryu_tok999" not in pod_ctx
    assert "postgres://app:***@pg:5432/db" in pod_ctx
    assert "[BackOff x5]" in pod_ctx

    argo_anom = ArgoAnomaly(
        app_name="rattlsnek",
        sync_status="OutOfSync",
        health_status="Degraded",
        message="Sync failed with Bearer secretJwt",
        fingerprint="argo-argocd-rattlsnek-degraded-12345678",
    )
    argo_ctx = await inspector.collect_diagnostic_context(argo_anom)
    assert "secretJwt" not in argo_ctx
    assert "Bearer ***" in argo_ctx

    # When previous=True succeeds on CrashLoopBackOff, it is used without falling back
    core_v1.read_namespaced_pod_log.side_effect = None
    core_v1.read_namespaced_pod_log.return_value = "Previous crash traceback AIzaSySecretKey"
    prev_ctx = await inspector.collect_diagnostic_context(pod_anom)
    assert "Previous crash traceback ***" in prev_ctx
    assert "previous container" in prev_ctx


async def test_k8s_watchdog_task_poll_once_dedup_prompt_and_cluster_report(tmp_path: Path) -> None:
    settings = _make_settings(tmp_path)
    state = StateStore(tmp_path / "state.sqlite3")

    pod_anom = PodAnomaly(
        namespace="rattlsnek",
        pod_name="rattlsnek-api-7d9f8c6b4d-x2k9p",
        owner_name="rattlsnek-api",
        container_name="api",
        reason="CrashLoopBackOff",
        restarts=4,
        image="ghcr.io/lodbrok-app/rattlsnek-api:1.0.0",
        fingerprint="k8s-rattlsnek-rattlsnek-api-crashloopbackoff-deadbeef",
        message="Back-off restarting failed container",
    )
    argo_anom = ArgoAnomaly(
        app_name="monitoring",
        sync_status="Synced",
        health_status="Degraded",
        message="Deployment prometheus unhealthy",
        fingerprint="argo-argocd-monitoring-degraded-cafebabe",
    )
    # Trim pod_anom fingerprint to realistic <=48 length for test fidelity
    pod_anom.fingerprint = "k8s-rattlsnek-api-crashloopbackoff-deadbeef"

    snapshot = ClusterHealthSnapshot(
        checked_at="2026-11-15T23:55:00+00:00",
        namespace_pod_counts={"rattlsnek": (2, 3), "monitoring": (4, 5)},
        pod_anomalies=[pod_anom],
        argo_anomalies=[argo_anom],
        argo_apps_summary=[("root", "OutOfSync", "Healthy"), ("monitoring", "Synced", "Degraded")],
        errors=[],
    )

    fake_inspector = MagicMock(spec=ClusterHealthInspector)
    fake_inspector.inspect.return_value = snapshot
    fake_inspector.collect_diagnostic_context.return_value = "### Logs\n`ZeroDivisionError` in api"

    dispatch_calls = 0

    async def on_dispatch() -> None:
        nonlocal dispatch_calls
        dispatch_calls += 1

    task = K8sWatchdogTask(
        settings=settings,
        state=state,
        dispatch_callback=on_dispatch,
        inspector=fake_inspector,
    )
    task_proto: LongLivedTask = task
    assert task_proto.name == "k8s-watchdog"
    assert callable(task_proto.run)

    targets = TargetsConfig(
        k8s_watchdog=K8sWatchdogConfig(
            enabled=True,
            settle_polls=1,
            watched_namespaces=["rattlsnek", "monitoring"],
            github_repo="lodbrok-app/rattlsnek",
            default_branch="main",
        )
    )

    # First poll: registers both anomalies, writes task_prompt.md, dispatches once
    await task.poll_once(targets)
    assert dispatch_calls == 1

    for fp in (pod_anom.fingerprint, argo_anom.fingerprint):
        rec = state.get_by_issue_id(fp)
        assert rec is not None
        assert rec.task_kind == "k8s_watchdog"
        assert rec.project_slug == "rattlsnek"
        assert rec.short_id == f"K8S-{fp[-6:].upper()}"
        assert rec.status == IssueStatus.QUEUED

        prompt_path = tmp_path / "artifacts" / fp / "task_prompt.md"
        assert prompt_path.exists()
        prompt_text = prompt_path.read_text(encoding="utf-8")
        assert "lodbrok-app/rattlsnek" in prompt_text
        assert "ZeroDivisionError" in prompt_text

    # Second poll with identical anomalies: deduplicated, no extra dispatch or context fetch
    await task.poll_once(targets)
    assert dispatch_calls == 1
    assert fake_inspector.collect_diagnostic_context.call_count == 2

    # Cluster report formatting
    report = await task.build_cluster_report()
    assert "*`makesense` Cluster Health Report*" in report
    assert "`rattlsnek`: *2/3* healthy" in report
    assert "`monitoring`: *4/5* healthy" in report
    assert "`root`: `OutOfSync` / `Healthy`" in report
    assert "`monitoring`: `Synced` / `Degraded`" in report
    assert pod_anom.fingerprint in report
    assert argo_anom.fingerprint in report


async def test_k8s_watchdog_task_disabled_skips_queuing_and_run_handles_errors(
    tmp_path: Path,
) -> None:
    settings = _make_settings(tmp_path)
    state = StateStore(tmp_path / "state.sqlite3")

    fake_inspector = MagicMock(spec=ClusterHealthInspector)
    fake_inspector.inspect.return_value = ClusterHealthSnapshot(
        checked_at="2026-11-15T23:55:00+00:00",
        namespace_pod_counts={"rattlsnek": (1, 1)},
        pod_anomalies=[
            PodAnomaly(
                namespace="rattlsnek",
                pod_name="api-1",
                owner_name="api",
                container_name="api",
                reason="CrashLoopBackOff",
                restarts=3,
                image="img:1",
                fingerprint="k8s-rattlsnek-api-crashloopbackoff-12345678",
            )
        ],
    )

    dispatch_calls = 0

    async def on_dispatch() -> None:
        nonlocal dispatch_calls
        dispatch_calls += 1

    beats = 0

    def on_heartbeat() -> None:
        nonlocal beats
        beats += 1

    task = K8sWatchdogTask(
        settings=settings,
        state=state,
        heartbeat_cb=on_heartbeat,
        dispatch_callback=on_dispatch,
        inspector=fake_inspector,
    )

    disabled_targets = TargetsConfig(
        k8s_watchdog=K8sWatchdogConfig(enabled=False, poll_interval_seconds=1)
    )
    await task.poll_once(disabled_targets)
    assert dispatch_calls == 0
    assert state.status_counts() == {}

    # Verify run() calls heartbeat_cb, survives transient errors, and stops on cancel
    settings.targets_config_path.write_text(
        "k8s_watchdog:\n  enabled: true\n  poll_interval_seconds: 1\n",
        encoding="utf-8",
    )
    fake_inspector.inspect.side_effect = [
        RuntimeError("transient api hiccup"),
        RuntimeError("again"),
    ]

    runner = asyncio.create_task(task.run())
    await asyncio.sleep(0.05)
    assert beats >= 1
    assert not runner.done()
    runner.cancel()
    with pytest.raises(asyncio.CancelledError):
        await runner
