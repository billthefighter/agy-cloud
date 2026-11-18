from __future__ import annotations

import json
import sqlite3
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import AsyncMock

import pytest
import yaml

from agent_tasks.channels.slack_socket import SlackSocketAdapter
from agent_tasks.common.config import (
    BillingConfig,
    ControllerSettings,
    TargetsConfig,
    parse_model_override,
)
from agent_tasks.common.models import (
    ExceptionSummary,
    IssueStatus,
    SanitizedSentryBundle,
    StackFrameSummary,
    TokenUsageSummary,
    TriageReport,
    WorkerEnvelope,
)
from agent_tasks.common.state import IssueRecord, StateStore
from agent_tasks.tasks.billing_guard import BillingMonitorTask, compute_billing_cycle
from agent_tasks.tasks.sentry_triage.git_ops import push_patch_to_branch
from agent_tasks.tasks.sentry_triage.k8s_jobs import build_triage_job_manifest
from agent_tasks.tasks.sentry_triage.orchestrator import SentryTriageTask, resolve_target_config


def test_parse_model_override_handles_tags_phrases_and_normal_prose() -> None:
    assert parse_model_override("[carbon] fix parser bug") == (
        "gemini-4-carbon",
        "fix parser bug",
    )
    assert parse_model_override("retry with argon") == ("gemini-4-argon", "retry")
    assert parse_model_override("use flash: add unit tests") == (
        "gemini-3.8-flash",
        "add unit tests",
    )
    assert parse_model_override("[Hydrogen] check deployment") == (
        "gemini-4-hydrogen",
        "check deployment",
    )
    # Normal words like "carbon footprint" should not trigger unintended overrides.
    assert parse_model_override("track carbon emissions metric") == (
        None,
        "track carbon emissions metric",
    )


def test_state_store_migrates_legacy_sqlite_schema_in_place(tmp_path: Path) -> None:
    db_path = tmp_path / "state.sqlite3"
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        CREATE TABLE issues (
            issue_id TEXT PRIMARY KEY,
            short_id TEXT NOT NULL,
            project_slug TEXT NOT NULL,
            github_repo TEXT NOT NULL,
            default_branch TEXT NOT NULL DEFAULT 'main',
            title TEXT NOT NULL,
            permalink TEXT NOT NULL DEFAULT '',
            status TEXT NOT NULL,
            turn INTEGER NOT NULL DEFAULT 0,
            conversation_id TEXT,
            thread_ts TEXT,
            channel_id TEXT,
            pr_url TEXT,
            last_error TEXT,
            updated_at TEXT NOT NULL
        );
        """
    )
    conn.execute(
        """
        INSERT INTO issues VALUES (
            '7848505915', 'PYTHON-EDGE-15', 'python-edge', 'lodbrok-app/rattlsnek',
            'main', 'ValueError', 'https://lodbrok.sentry.io/issues/7848505915/',
            'awaiting_review', 1, 'conv-1', '1794706920.1', 'C1', NULL, NULL,
            '2026-11-15T00:00:00Z'
        );
        """
    )
    conn.commit()
    conn.close()

    store = StateStore(db_path)
    migrated = store.get_by_issue_id("7848505915")
    assert migrated is not None
    assert migrated.task_kind == "sentry"
    assert migrated.base_ref is None
    assert migrated.target_branch is None

    updated = store.bump_turn_for_feedback("7848505915", model_override="gemini-4-carbon")
    assert updated.turn == 2
    assert updated.model_override == "gemini-4-carbon"


def test_k8s_job_manifest_uses_hydrogen_default_override_and_clone_ref(tmp_path: Path) -> None:
    store = StateStore(tmp_path / "state.sqlite3")
    rec = store.register_if_new(
        issue_id="ci-555",
        short_id="CI-555",
        project_slug="rattlsnek",
        github_repo="lodbrok-app/rattlsnek",
        default_branch="main",
        title="pytest failed on feat/branch",
        permalink="https://github.com/lodbrok-app/rattlsnek/actions/runs/555",
        task_kind="ci_healer",
        base_ref="feat/branch",
        target_branch="feat/branch",
        model_override="gemini-4-carbon",
    )
    assert rec is not None

    settings = ControllerSettings(
        state_dir=tmp_path,
        target_github_token="gh-secret",
        slack_bot_token="xoxb-secret",
        slack_app_token="xapp-secret",
        slack_channel_id="C123",
    )
    assert settings.worker_model == "gemini-4-hydrogen"

    targets = TargetsConfig()
    target = resolve_target_config(rec, targets)
    assert target is not None

    job = build_triage_job_manifest(settings=settings, record=rec, target=target)
    init_env = {e.name: e.value for e in job.spec.template.spec.init_containers[0].env}
    worker_env = {e.name: e.value for e in job.spec.template.spec.containers[0].env}

    assert init_env["CLONE_REF"] == "feat/branch"
    assert worker_env["TASK_KIND"] == "ci_healer"
    assert worker_env["WORKER_MODEL"] == "gemini-4-carbon"


def test_manifests_configure_hydrogen_clusterrole_and_all_automations() -> None:
    deploy_dir = Path(__file__).resolve().parents[1] / "deploy" / "k3s"

    cm = yaml.safe_load((deploy_dir / "configmap-targets.yaml").read_text(encoding="utf-8"))
    cfg = TargetsConfig.model_validate(yaml.safe_load(cm["data"]["targets.yaml"]))
    assert cfg.slack_coding.enabled is True
    assert cfg.ci_healer.enabled is True
    assert cfg.k8s_watchdog.enabled is True
    assert cfg.dep_upgrader.enabled is True

    dep = yaml.safe_load((deploy_dir / "controller-deployment.yaml").read_text(encoding="utf-8"))
    env_map = {
        e["name"]: e.get("value") for e in dep["spec"]["template"]["spec"]["containers"][0]["env"]
    }
    assert env_map["WORKER_MODEL"] == "gemini-4-hydrogen"

    rbac_docs = list(yaml.safe_load_all((deploy_dir / "rbac.yaml").read_text(encoding="utf-8")))
    cluster_role = next(d for d in rbac_docs if d.get("kind") == "ClusterRole")
    cluster_binding = next(d for d in rbac_docs if d.get("kind") == "ClusterRoleBinding")
    for rule in cluster_role["rules"]:
        assert rule["verbs"] == ["get", "list", "watch"]
    assert cluster_binding["subjects"] == [
        {
            "kind": "ServiceAccount",
            "name": "agent-tasks-controller",
            "namespace": "agent-tasks",
        }
    ]


async def test_slack_socket_adapter_routes_commands_coding_buttons_and_dedup() -> None:
    posted: list[dict] = []
    feedback_calls: list[tuple[str, str, str]] = []
    coding_calls: list[tuple[str, str, str, str]] = []
    deps_calls: list[tuple[str, str, str]] = []

    async def _post_msg(**kwargs):  # type: ignore[no-untyped-def]
        posted.append(kwargs)
        return {"ts": "1794800000.0001"}

    async def on_feedback(ts: str, text: str, user: str) -> None:
        feedback_calls.append((ts, text, user))

    async def on_coding(prompt: str, user: str, ts: str, chan: str) -> str:
        coding_calls.append((prompt, user, ts, chan))
        return f"Queued coding for {prompt}"

    async def on_deps(args: str, ts: str, chan: str) -> str:
        deps_calls.append((args, ts, chan))
        return f"Queued deps: {args}"

    adapter = SlackSocketAdapter(
        bot_token="xoxb-test",
        app_token="xapp-test",
        channel_id="C123",
        on_open_draft_pr=AsyncMock(),
        on_push_to_pr_branch=AsyncMock(),
        on_resolve=AsyncMock(),
        on_ignore=AsyncMock(),
        on_thread_feedback=on_feedback,
        on_status_query=AsyncMock(return_value="status-ok"),
        on_k8s_query=AsyncMock(return_value="k8s-ok"),
        on_ci_query=AsyncMock(return_value="ci-ok"),
        on_deps_trigger=on_deps,
        on_coding_request=on_coding,
    )
    adapter.app.client.chat_postMessage = _post_msg  # type: ignore[method-assign]

    # Duplicate event delivery (both app_mention and message sharing same ts) only runs once.
    mention_evt = {"channel": "C123", "user": "U1", "ts": "10.1", "text": "<@UBOT> !k8s"}
    await adapter.process_incoming_message(mention_evt, from_mention=True)
    await adapter.process_incoming_message(mention_evt, from_mention=False)
    assert len(posted) == 1
    assert posted[0]["text"] == "k8s-ok"

    await adapter.process_incoming_message(
        {"channel": "C123", "user": "U1", "ts": "10.2", "text": "!deps python [carbon]"},
        from_mention=False,
    )
    assert deps_calls == [("python [carbon]", "10.2", "C123")]

    await adapter.process_incoming_message(
        {"channel": "C123", "user": "U1", "ts": "10.3", "text": "<@UBOT> add test"},
        from_mention=True,
    )
    assert coding_calls == [("add test", "U1", "10.3", "C123")]

    # Normal prose containing "status" inside a thread goes to thread feedback, NOT keepalive.
    await adapter.process_incoming_message(
        {
            "channel": "C123",
            "user": "U1",
            "ts": "10.4",
            "thread_ts": "10.3",
            "text": "check status code before returning",
        },
        from_mention=False,
    )
    assert feedback_calls == [("10.3", "check status code before returning", "U1")]

    # Render non-Sentry CI card with target_branch shows Push Fix + Open Draft PR without Sentry buttons.
    ci_rec = IssueRecord(
        issue_id="ci-77",
        short_id="CI-77",
        project_slug="rattlsnek",
        github_repo="lodbrok-app/rattlsnek",
        default_branch="main",
        title="Workflow failed",
        permalink="https://github.com/lodbrok-app/rattlsnek/actions/runs/77",
        status=IssueStatus.RUNNING,
        turn=1,
        conversation_id=None,
        thread_ts=None,
        channel_id=None,
        pr_url=None,
        last_error=None,
        updated_at="2026-11-15T00:00:00Z",
        task_kind="ci_healer",
        base_ref="fix/ci",
        target_branch="fix/ci",
        banner_note=":warning: Stale release warning",
    )
    report = TriageReport(
        root_cause="Lint error",
        confidence="high",
        affected_files=["rattlsnek/edge/a.py"],
        fix_summary="Fixed import",
        verification_commands=["uv run ruff check ."],
        tests_passed=True,
    )
    await adapter.post_triage_result(
        ci_rec, report, "--- a/x\n+++ b/x\n", model_used="gemini-4-hydrogen"
    )
    card_blocks = posted[-1]["blocks"]
    action_ids = [
        el["action_id"] for blk in card_blocks if blk["type"] == "actions" for el in blk["elements"]
    ]
    assert action_ids == ["push_to_pr_branch", "open_draft_pr"]
    assert card_blocks[0]["text"]["text"] == ":warning: Stale release warning"


def _seed_push_test_repo(tmp_path: Path) -> tuple[Path, Path, Path]:
    upstream = tmp_path / "upstream.git"
    subprocess.run(["git", "init", "--bare", "--initial-branch=main", str(upstream)], check=True)
    work = tmp_path / "seed"
    subprocess.run(["git", "clone", str(upstream), str(work)], check=True)
    subprocess.run(["git", "-C", str(work), "config", "user.name", "Tester"], check=True)
    subprocess.run(["git", "-C", str(work), "config", "user.email", "t@example.com"], check=True)
    (work / "hello.txt").write_text("v1\n", encoding="utf-8")
    subprocess.run(["git", "-C", str(work), "add", "."], check=True)
    subprocess.run(["git", "-C", str(work), "commit", "-m", "init"], check=True)
    subprocess.run(["git", "-C", str(work), "push", "origin", "main"], check=True)
    subprocess.run(["git", "-C", str(work), "checkout", "-b", "feat/fix-ci"], check=True)
    subprocess.run(["git", "-C", str(work), "push", "origin", "feat/fix-ci"], check=True)

    mirror = tmp_path / "mirrors" / "owner__repo.git"
    subprocess.run(["git", "clone", "--mirror", str(upstream), str(mirror)], check=True)

    patch_path = tmp_path / "fix.patch"
    (work / "hello.txt").write_text("v2\n", encoding="utf-8")
    patch = subprocess.run(
        ["git", "-C", str(work), "diff", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    patch_path.write_text(patch, encoding="utf-8")
    return upstream, mirror, patch_path


async def test_push_patch_to_branch_blocks_default_branch_and_pushes_pr_branch(
    tmp_path: Path,
) -> None:
    upstream, mirror, patch_path = _seed_push_test_repo(tmp_path)

    rec = IssueRecord(
        issue_id="ci-1",
        short_id="CI-1",
        project_slug="repo",
        github_repo="owner/repo",
        default_branch="main",
        title="Fix",
        permalink="",
        status=IssueStatus.AWAITING_REVIEW,
        turn=1,
        conversation_id=None,
        thread_ts="1.1",
        channel_id="C1",
        pr_url=None,
        last_error=None,
        updated_at="2026-11-15T00:00:00Z",
        task_kind="ci_healer",
        base_ref="feat/fix-ci",
        target_branch="feat/fix-ci",
    )

    try:
        await push_patch_to_branch(
            mirrors_root=tmp_path / "mirrors",
            record=rec,
            report=None,
            patch_path=patch_path,
            branch_name="main",
            github_token="tok",
        )
        raise AssertionError("Expected ValueError for pushing directly to default branch")
    except ValueError as exc:
        assert "default branch" in str(exc)

    # Test _apply_patch_and_push_sync directly against local file:// upstream
    from agent_tasks.tasks.sentry_triage.git_ops import _apply_patch_and_push_sync

    sha = _apply_patch_and_push_sync(
        mirror_path=mirror,
        github_repo="owner/repo",
        clone_branch="feat/fix-ci",
        branch_name="feat/fix-ci",
        patch_path=patch_path,
        commit_message="fix: apply ci patch",
        github_token="tok",
        force_with_lease=False,
        remote_url=str(upstream),
    )
    assert len(sha) >= 7


def _seed_release_lag_repo(tmp_path: Path) -> tuple[Path, str]:
    repo = tmp_path / "workrepo"
    subprocess.run(["git", "init", "--initial-branch=main", str(repo)], check=True)
    subprocess.run(["git", "-C", str(repo), "config", "user.name", "T"], check=True)
    subprocess.run(["git", "-C", str(repo), "config", "user.email", "t@example.com"], check=True)
    parser_file = (
        repo / "rattlsnek" / "edge" / "src" / "edge" / "plugins" / "mtconnect" / "parser.py"
    )
    parser_file.parent.mkdir(parents=True, exist_ok=True)
    parser_file.write_text("def parse(): pass\n", encoding="utf-8")
    subprocess.run(["git", "-C", str(repo), "add", "."], check=True)
    subprocess.run(["git", "-C", str(repo), "commit", "-m", "initial"], check=True)
    old_sha = subprocess.run(
        ["git", "-C", str(repo), "rev-parse", "HEAD"], check=True, capture_output=True, text=True
    ).stdout.strip()
    parser_file.write_text("def parse(): return None\n", encoding="utf-8")
    subprocess.run(["git", "-C", str(repo), "commit", "-am", "fix mtconnect"], check=True)

    mirror = tmp_path / "mirrors" / "lodbrok-app__rattlsnek.git"
    mirror.parent.mkdir(parents=True, exist_ok=True)
    subprocess.run(["git", "clone", "--mirror", str(repo), str(mirror)], check=True)
    return mirror, old_sha


async def test_orchestrator_populates_release_context_and_overrides_model(tmp_path: Path) -> None:
    mirror, old_sha = _seed_release_lag_repo(tmp_path)

    targets_yaml = tmp_path / "targets.yaml"
    targets_yaml.write_text(
        yaml.safe_dump(TargetsConfig().model_dump()).replace(
            "projects: {}", "projects:\n  python-edge:\n    github_repo: lodbrok-app/rattlsnek"
        ),
        encoding="utf-8",
    )

    settings = ControllerSettings(
        state_dir=tmp_path,
        targets_config_path=targets_yaml,
        target_github_token="tok",
        slack_bot_token="xoxb",
        slack_app_token="xapp",
        slack_channel_id="C1",
    )
    store = StateStore(tmp_path / "state.sqlite3")
    art_dir = tmp_path / "artifacts" / "9001"
    art_dir.mkdir(parents=True, exist_ok=True)
    bundle = SanitizedSentryBundle(
        issue_id="9001",
        short_id="PYTHON-EDGE-99",
        project_slug="python-edge",
        title="ValueError",
        release=old_sha,
        exceptions=[
            ExceptionSummary(
                exc_type="ValueError",
                exc_value="bad xml",
                frames=[
                    StackFrameSummary(
                        filename="edge/src/edge/plugins/mtconnect/parser.py",
                        in_app=True,
                    )
                ],
            )
        ],
    )
    (art_dir / "issue_bundle.json").write_text(bundle.model_dump_json(), encoding="utf-8")
    store.register_if_new(
        issue_id="9001",
        short_id="PYTHON-EDGE-99",
        project_slug="python-edge",
        github_repo="lodbrok-app/rattlsnek",
        default_branch="main",
        title="ValueError",
        permalink="https://lodbrok.sentry.io/issues/9001/",
    )

    spawned: list[IssueRecord] = []

    class FakeRunner:
        async def spawn_job(self, record, target):  # type: ignore[no-untyped-def]
            spawned.append(record)
            return "fake-job"

        async def get_job_terminal_state(self, issue_id, turn):  # type: ignore[no-untyped-def]
            return "succeeded"

    channel = AsyncMock()
    channel.resolve_channel_for_task = lambda task_kind: "C1"
    channel.post_triage_result = AsyncMock(return_value="1700.1")

    task = SentryTriageTask(
        settings=settings,
        state=store,
        channel=channel,
        job_runner=FakeRunner(),  # type: ignore[arg-type]
        heartbeat_cb=lambda: None,
    )

    # Monkeypatch sync_bare_mirror inside orchestrator module to use our local test mirror
    import agent_tasks.tasks.sentry_triage.orchestrator as orch_mod

    orig_sync = orch_mod.sync_bare_mirror

    async def fake_sync(root, repo_slug, tok):  # type: ignore[no-untyped-def]
        return mirror

    orch_mod.sync_bare_mirror = fake_sync
    try:
        await task.dispatch_pending()
    finally:
        orch_mod.sync_bare_mirror = orig_sync

    assert len(spawned) == 1
    assert (art_dir / "release_context.txt").exists()
    assert "Stale release warning" in (spawned[0].banner_note or "")

    # Simulate worker finishing & posting result, then Slack reply `retry [carbon]`
    (art_dir / "result.json").write_text(
        json.dumps(
            WorkerEnvelope(
                issue_id="9001",
                turn=1,
                conversation_id="c-1",
                model="gemini-4-hydrogen",
                report=TriageReport(
                    root_cause="old release",
                    confidence="high",
                    fix_summary="already fixed",
                    tests_passed=True,
                ),
            ).model_dump()
        ),
        encoding="utf-8",
    )
    await task._reconcile_running_jobs()
    channel.post_triage_result.assert_awaited_once()

    orch_mod.sync_bare_mirror = fake_sync
    try:
        await task.handle_thread_feedback("1700.1", "retry [carbon]", "lucas")
    finally:
        orch_mod.sync_bare_mirror = orig_sync

    assert len(spawned) == 2
    assert spawned[1].turn == 2
    assert spawned[1].model_override == "gemini-4-carbon"
    assert (art_dir / "feedback-turn-2.txt").read_text(encoding="utf-8") == "retry"


async def test_slack_adapter_multi_channel_bind_and_billing_routing() -> None:
    posted: list[dict] = []
    bound: dict[str, str] = {}
    billing_calls: list[str] = []

    async def _post_msg(**kwargs):  # type: ignore[no-untyped-def]
        posted.append(kwargs)
        return {"ts": "1795000000.0001"}

    async def on_bind(role_arg: str, chan_id: str) -> str:
        bound[role_arg] = chan_id
        return f"bound {role_arg} -> {chan_id}"

    async def on_billing(args: str) -> str:
        billing_calls.append(args)
        return f"billing:{args}"

    adapter = SlackSocketAdapter(
        bot_token="xoxb-test",
        app_token="xapp-test",
        channel_id="C_DEFAULT",
        on_open_draft_pr=AsyncMock(),
        on_resolve=AsyncMock(),
        on_ignore=AsyncMock(),
        on_thread_feedback=AsyncMock(),
        on_billing_command=on_billing,
        on_bind_channel=on_bind,
        on_channels_query=AsyncMock(return_value="channels-list"),
        channel_resolver=lambda role: bound.get(role, ""),
    )
    adapter.app.client.chat_postMessage = _post_msg  # type: ignore[method-assign]

    # Bind ci and billing in different channels and verify routing + deps -> ci fallback
    await adapter.process_incoming_message(
        {"channel": "C_CI", "user": "U1", "ts": "20.1", "text": "!bind ci"},
        from_mention=False,
    )
    await adapter.process_incoming_message(
        {"channel": "C_BILL", "user": "U1", "ts": "20.2", "text": "!bind billing"},
        from_mention=False,
    )
    assert adapter.resolve_channel_for_task("ci_healer") == "C_CI"
    assert adapter.resolve_channel_for_task("dep_upgrader") == "C_CI"
    assert adapter.resolve_channel_for_task("sentry") == "C_DEFAULT"
    assert adapter.resolve_channel("billing") == "C_BILL"

    await adapter.post_channel_message("budget alert!", role="billing")
    assert posted[-1]["channel"] == "C_BILL"

    await adapter.process_incoming_message(
        {"channel": "C_BILL", "user": "U1", "ts": "20.3", "text": "!billing sync 12.50"},
        from_mention=False,
    )
    assert billing_calls == ["sync 12.50"]
    assert posted[-1]["channel"] == "C_BILL"
    assert posted[-1]["text"] == "billing:sync 12.50"


async def test_orchestrator_billing_records_usage_and_pauses_background_at_safety_brake(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    targets_cfg = TargetsConfig(billing=BillingConfig(monthly_credit_usd=100.0, hard_stop_pct=95.0))
    targets_yaml = tmp_path / "targets.yaml"
    targets_yaml.write_text(
        yaml.safe_dump(targets_cfg.model_dump()).replace(
            "projects: {}",
            "projects:\n  python-edge:\n    github_repo: lodbrok-app/rattlsnek",
        ),
        encoding="utf-8",
    )
    targets = TargetsConfig.model_validate(yaml.safe_load(targets_yaml.read_text(encoding="utf-8")))

    settings = ControllerSettings(
        state_dir=tmp_path,
        targets_config_path=targets_yaml,
        target_github_token="tok",
        slack_bot_token="xoxb",
        slack_app_token="xapp",
        slack_channel_id="C1",
    )
    store = StateStore(tmp_path / "state.sqlite3")

    triage_posts: list[dict] = []
    channel_posts: list[tuple[str, str]] = []
    channel = AsyncMock()
    channel.resolve_channel_for_task = lambda task_kind: (
        "C_CODE" if task_kind == "slack_coding" else "C_SENTRY"
    )
    channel.resolve_channel = lambda role: "C_BILL" if role == "billing" else "C1"

    async def _post_triage_result(**kwargs):  # type: ignore[no-untyped-def]
        triage_posts.append(kwargs)
        return "ts-triage-1"

    async def _post_channel_message(text: str, *, role: str = "", channel_id: str | None = None):
        channel_posts.append((role, text))
        return "ts-chan-1"

    channel.post_triage_result = AsyncMock(side_effect=_post_triage_result)
    channel.post_channel_message = AsyncMock(side_effect=_post_channel_message)

    spawned: list[IssueRecord] = []

    class FakeRunner:
        async def get_job_terminal_state(self, issue_id: str, turn: int) -> str | None:
            return "succeeded" if issue_id == "9002" else None

        async def spawn_job(self, record: IssueRecord, target: object) -> str:
            spawned.append(record)
            return f"job-{record.issue_id}"

    billing = BillingMonitorTask(settings=settings, state=store, channel=channel)
    task = SentryTriageTask(
        settings=settings,
        state=store,
        channel=channel,
        job_runner=FakeRunner(),  # type: ignore[arg-type]
        heartbeat_cb=lambda: None,
        billing_guard=billing,
    )

    # 1) Finished worker job -> _reconcile_running_jobs records incremental token cost & passes cost_line
    store.register_if_new(
        issue_id="9002",
        short_id="PYTHON-EDGE-100",
        project_slug="python-edge",
        github_repo="lodbrok-app/rattlsnek",
        default_branch="main",
        title="KeyError",
        permalink="https://lodbrok.sentry.io/issues/9002/",
        channel_id="C_SENTRY",
    )
    store.mark_status("9002", IssueStatus.RUNNING)

    art_dir = task.artifact_dir("9002")
    envelope = WorkerEnvelope(
        issue_id="9002",
        turn=1,
        conversation_id="conv-9002",
        model="gemini-4-hydrogen",
        report=TriageReport(
            confidence="high",
            root_cause="Missing key",
            fix_summary="Use .get()",
            files_touched=["a.py"],
            tests_run=["pytest"],
            tests_passed=True,
        ),
        usage=TokenUsageSummary(
            prompt_tokens=1_000_000,
            cached_tokens=200_000,
            output_tokens=100_000,
            thoughts_tokens=50_000,
            total_tokens=1_150_000,
        ),
    )
    (art_dir / "result.json").write_text(envelope.model_dump_json(), encoding="utf-8")

    await task._reconcile_running_jobs()

    assert len(triage_posts) == 1
    assert "Cost: `$2.3550`" in triage_posts[0]["cost_line"]
    cycle = compute_billing_cycle(
        datetime.now(UTC),
        anchor_day=targets.billing.cycle_anchor_day,
    )
    summary = store.get_cycle_usage_summary(cycle.cycle_key)
    assert summary["spent_usd"] == pytest.approx(2.355)
    assert summary["job_count"] == 1

    # 2) Crossing the 95% hard-stop via `!billing sync $96.00` pauses background jobs, not slack_coding
    sync_reply = await billing.handle_billing_command("sync $96.00")
    assert "96.00" in sync_reply

    async def _fake_sync_mirror(mirrors_root: Path, github_repo: str, token: str) -> Path:
        p = mirrors_root / "lodbrok-app__rattlsnek.git"
        p.mkdir(parents=True, exist_ok=True)
        return p

    monkeypatch.setattr(
        "agent_tasks.tasks.sentry_triage.orchestrator.sync_bare_mirror",
        _fake_sync_mirror,
    )

    store.register_if_new(
        issue_id="9003",
        short_id="PYTHON-EDGE-101",
        project_slug="python-edge",
        github_repo="lodbrok-app/rattlsnek",
        default_branch="main",
        title="Background Sentry issue",
        permalink="https://lodbrok.sentry.io/issues/9003/",
        task_kind="sentry",
        channel_id="C_SENTRY",
    )
    store.register_if_new(
        issue_id="code-9004",
        short_id="CODE-9004",
        project_slug="lodbrok-app/rattlsnek",
        github_repo="lodbrok-app/rattlsnek",
        default_branch="main",
        title="Interactive Slack fix",
        permalink="",
        task_kind="slack_coding",
        channel_id="C_CODE",
    )

    await task._dispatch_queued_jobs(targets)

    assert [r.issue_id for r in spawned] == ["code-9004"]
    bg_rec = store.get_by_issue_id("9003")
    code_rec = store.get_by_issue_id("code-9004")
    assert bg_rec is not None and bg_rec.status == IssueStatus.QUEUED
    assert code_rec is not None and code_rec.status == IssueStatus.RUNNING
