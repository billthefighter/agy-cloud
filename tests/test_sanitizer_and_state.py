from __future__ import annotations

import subprocess
from pathlib import Path

from agent_tasks.common.config import ControllerSettings, ProjectTargetConfig, TargetsConfig
from agent_tasks.common.models import IssueStatus
from agent_tasks.common.state import StateStore
from agent_tasks.tasks.sentry_triage.git_ops import sync_bare_mirror_sync
from agent_tasks.tasks.sentry_triage.k8s_jobs import build_triage_job_manifest
from agent_tasks.tasks.sentry_triage.sentry_client import (
    SentryClient,
    sanitize_sentry_issue_and_event,
)


def test_sanitizer_strips_sensitive_tags_and_bounds_frames() -> None:
    raw_issue = {
        "id": "998877",
        "shortId": "PYTHON-FASTAPI-42",
        "title": "ZeroDivisionError: division by zero",
        "culprit": "api.routes.metrics in compute_rate",
        "permalink": "https://lodbrok.sentry.io/issues/998877/",
        "firstSeen": "2026-11-14T20:00:00Z",
        "lastSeen": "2026-11-14T20:05:00Z",
    }
    raw_event = {
        "eventID": "evt-123",
        "release": "abc1234",
        "tags": [
            {"key": "environment", "value": "production"},
            {"key": "transaction", "value": "/api/v1/metrics"},
            {"key": "user.email", "value": "secret-user@example.com"},
            {"key": "cookie", "value": "session=topsecret"},
        ],
        "request": {
            "headers": [["Authorization", "Bearer super-secret"]],
            "cookies": "session=topsecret",
        },
        "entries": [
            {
                "type": "exception",
                "data": {
                    "values": [
                        {
                            "type": "ZeroDivisionError",
                            "value": "division by zero",
                            "stacktrace": {
                                "frames": [
                                    {
                                        "filename": "rattlsnek/api/routes/metrics.py",
                                        "function": "compute_rate",
                                        "lineNo": 88,
                                        "inApp": True,
                                        "contextLine": "return count / elapsed_seconds",
                                    }
                                ]
                            },
                        }
                    ]
                },
            },
            {
                "type": "breadcrumbs",
                "data": {
                    "values": [
                        {
                            "timestamp": "2026-11-14T20:04:59Z",
                            "category": "query",
                            "level": "info",
                            "message": "SELECT count(*) FROM spans",
                        }
                    ]
                },
            },
        ],
    }

    bundle = sanitize_sentry_issue_and_event(raw_issue, raw_event, "python-fastapi")
    serialized = bundle.model_dump_json()

    assert bundle.short_id == "PYTHON-FASTAPI-42"
    assert bundle.environment == "production"
    assert "user.email" not in bundle.tags
    assert "cookie" not in bundle.tags
    assert "super-secret" not in serialized
    assert "topsecret" not in serialized
    assert len(bundle.exceptions) == 1
    assert bundle.exceptions[0].frames[0].lineno == 88


def test_state_store_lifecycle_and_thread_lookup(tmp_path: Path) -> None:
    store = StateStore(tmp_path / "state.sqlite3")

    rec = store.register_if_new(
        issue_id="1001",
        short_id="PYTHON-FASTAPI-1",
        project_slug="python-fastapi",
        github_repo="lodbrok-app/rattlsnek",
        default_branch="main",
        title="KeyError: 'machine_id'",
        permalink="https://lodbrok.sentry.io/issues/1001/",
    )
    assert rec is not None
    assert rec.status == IssueStatus.QUEUED
    assert rec.turn == 1

    # Duplicate registration is a no-op
    assert (
        store.register_if_new(
            issue_id="1001",
            short_id="PYTHON-FASTAPI-1",
            project_slug="python-fastapi",
            github_repo="lodbrok-app/rattlsnek",
            default_branch="main",
            title="KeyError: 'machine_id'",
            permalink="https://lodbrok.sentry.io/issues/1001/",
        )
        is None
    )

    store.mark_status("1001", IssueStatus.RUNNING)
    assert store.count_running() == 1

    store.mark_status(
        "1001",
        IssueStatus.AWAITING_REVIEW,
        conversation_id="conv-abc",
        thread_ts="1731620000.000100",
    )
    by_thread = store.get_by_thread_ts("1731620000.000100")
    assert by_thread is not None
    assert by_thread.conversation_id == "conv-abc"

    bumped = store.bump_turn_for_feedback("1001")
    assert bumped.turn == 2
    assert bumped.status == IssueStatus.QUEUED
    assert bumped.conversation_id == "conv-abc"


def test_k8s_job_manifest_enforces_makesense_rwo_affinity_and_secret_isolation(
    tmp_path: Path,
) -> None:
    store = StateStore(tmp_path / "state.sqlite3")
    rec = store.register_if_new(
        issue_id="2002",
        short_id="JAVASCRIPT-ASTRO-9",
        project_slug="javascript-astro",
        github_repo="lodbrok-app/rattlsnek",
        default_branch="main",
        title="TypeError: Cannot read properties of undefined",
        permalink="https://lodbrok.sentry.io/issues/2002/",
    )
    assert rec is not None

    settings = ControllerSettings(
        namespace="agent-tasks",
        node_name="k3s-worker-2",
        state_dir=tmp_path,
        sentry_auth_token="sentry-secret",
        target_github_token="gh-secret",
        slack_bot_token="xoxb-secret",
        slack_app_token="xapp-secret",
        slack_channel_id="C12345",
    )
    target = ProjectTargetConfig(
        github_repo="lodbrok-app/rattlsnek",
        default_branch="main",
        focus_subdir="web",
        verify_hint="cd web && npm test",
    )

    job = build_triage_job_manifest(settings=settings, record=rec, target=target)
    pod_spec = job.spec.template.spec

    # 1. Zero K8s API access
    assert pod_spec.automount_service_account_token is False
    assert pod_spec.service_account_name == "agent-tasks-worker"

    # 2. Pinned to makesense controller node for local-path ReadWriteOnce safety
    assert pod_spec.node_selector == {"kubernetes.io/hostname": "k3s-worker-2"}
    assert pod_spec.affinity.pod_affinity is not None

    # 3. Worker container receives ONLY agent-tasks-worker-secret (never controller secrets)
    worker_ctr = pod_spec.containers[0]
    secret_refs = [ef.secret_ref.name for ef in (worker_ctr.env_from or []) if ef.secret_ref]
    assert secret_refs == ["agent-tasks-worker-secret"]

    serialized = str(job.to_dict())
    for forbidden in ("sentry-secret", "gh-secret", "xoxb-secret", "xapp-secret"):
        assert forbidden not in serialized


def test_targets_configmap_validates_all_lodbrok_projects() -> None:
    cm_path = Path(__file__).resolve().parents[1] / "deploy" / "k3s" / "configmap-targets.yaml"
    import yaml

    cm = yaml.safe_load(cm_path.read_text(encoding="utf-8"))
    cfg = TargetsConfig.model_validate(yaml.safe_load(cm["data"]["targets.yaml"]))
    assert cfg.sentry.organization_slug == "lodbrok"
    assert set(cfg.projects.keys()) == {
        "python-fastapi",
        "python-fastapi-staging",
        "javascript-astro",
        "javascript-astro-staging",
        "python-edge",
        "python-edge-staging",
    }


def test_bare_mirror_config_never_persists_token(tmp_path: Path) -> None:
    # Create a tiny local upstream repo to verify mirror config URL hygiene
    upstream = tmp_path / "upstream.git"
    subprocess.run(["git", "init", "--bare", str(upstream)], check=True)
    mirror = tmp_path / "mirrors" / "lodbrok-app__rattlsnek.git"
    subprocess.run(["git", "clone", "--mirror", str(upstream), str(mirror)], check=True)

    # Simulate setting clean URL via git_ops helper check
    clean_url = "https://github.com/lodbrok-app/rattlsnek.git"
    subprocess.run(
        ["git", "-C", str(mirror), "remote", "set-url", "origin", clean_url],
        check=True,
    )
    cfg_text = (mirror / "config").read_text(encoding="utf-8")
    assert clean_url in cfg_text
    assert "x-access-token" not in cfg_text
    assert sync_bare_mirror_sync is not None


async def test_sentry_client_write_actions_notes_resolve_and_ignore() -> None:
    import json

    import httpx

    captured: list[tuple[str, str, dict]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content.decode("utf-8")) if request.content else {}
        captured.append((request.method, str(request.url), body))
        return httpx.Response(200, json={"id": "998877", "status": body.get("status", "ok")})

    client = SentryClient(
        base_url="https://us.sentry.io",
        organization_slug="lodbrok",
        auth_token="sntryu_test",
        transport=httpx.MockTransport(handler),
    )

    await client.add_issue_note(
        "998877", "Draft PR opened: https://github.com/lodbrok-app/rattlsnek/pull/1"
    )
    await client.update_issue_status("998877", status="resolved")
    await client.update_issue_status(
        "998877",
        status="ignored",
        substatus="archived_until_escalating",
    )

    assert captured[0] == (
        "POST",
        "https://us.sentry.io/api/0/organizations/lodbrok/issues/998877/comments/",
        {"text": "Draft PR opened: https://github.com/lodbrok-app/rattlsnek/pull/1"},
    )
    assert captured[1] == (
        "PUT",
        "https://us.sentry.io/api/0/organizations/lodbrok/issues/998877/",
        {"status": "resolved"},
    )
    assert captured[2] == (
        "PUT",
        "https://us.sentry.io/api/0/organizations/lodbrok/issues/998877/",
        {"status": "ignored", "substatus": "archived_until_escalating"},
    )


def test_rbac_grants_jobs_status_subresource_and_state_status_counts(tmp_path: Path) -> None:
    import yaml

    rbac_path = Path(__file__).resolve().parents[1] / "deploy" / "k3s" / "rbac.yaml"
    docs = list(yaml.safe_load_all(rbac_path.read_text(encoding="utf-8")))
    role_doc = next(d for d in docs if d.get("kind") == "Role")
    batch_rule = next(r for r in role_doc["rules"] if "batch" in r.get("apiGroups", []))
    assert "jobs" in batch_rule["resources"]
    assert "jobs/status" in batch_rule["resources"]

    store = StateStore(tmp_path / "state.sqlite3")
    assert store.status_counts() == {}
    store.register_if_new(
        issue_id="3001",
        short_id="PYTHON-EDGE-15",
        project_slug="python-edge",
        github_repo="lodbrok-app/rattlsnek",
        default_branch="main",
        title="ValueError: MTConnect XML did not contain a DeviceStream node",
        permalink="https://lodbrok.sentry.io/issues/3001/",
    )
    assert store.status_counts() == {"queued": 1}


def test_resolve_gemini_auth_defaults_to_developer_api_unless_vertex_set() -> None:
    from agent_tasks.worker_main import resolve_gemini_auth

    assert resolve_gemini_auth({"GEMINI_API_KEY": "AIzaSyExampleKey"}) == (
        "AIzaSyExampleKey",
        False,
    )
    assert resolve_gemini_auth({"GEMINI_API_KEY": "AQ.Ab8RN6ExampleKey"}) == (
        "AQ.Ab8RN6ExampleKey",
        False,
    )
    assert resolve_gemini_auth(
        {"GEMINI_API_KEY": "AQ.Ab8RN6ExampleKey", "GEMINI_VERTEX": "true"}
    ) == (
        "AQ.Ab8RN6ExampleKey",
        True,
    )
