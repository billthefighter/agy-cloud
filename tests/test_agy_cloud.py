from __future__ import annotations

import subprocess
from pathlib import Path

import httpx
import pytest
from fastapi.testclient import TestClient

from agy_cloud.cli import scaffold_new_plugin
from agy_cloud.client.handoff_client import (
    apply_remote_patch_safely,
    detect_git_metadata,
)
from agy_cloud.core.gateway import (
    MemoryChannelBus,
    RecordingWorkerJobService,
    build_worker_init_script,
    create_microkernel_app,
)
from agy_cloud.core.loader import PluginRegistry, is_pinned_git_ref
from agy_cloud.core.transports import CompositeChannelBus, OutboundHttpTransport
from agy_cloud.core.workers import (
    LocalSubprocessWorkerJobService,
    build_k8s_worker_job_dict,
)
from agy_cloud.sdk import (
    JobSpec,
    PluginPermissionError,
    PluginRepoSource,
    TransportEntryConfig,
    WorkerRunnerConfig,
)


@pytest.fixture()
def repo_root() -> Path:
    return Path(__file__).resolve().parent.parent


@pytest.fixture()
def external_health_plugins_repo() -> Path:
    return Path("/Users/lucaswhipple/Dropbox/code_projects/agy-plugins-health")


async def test_git_repo_plugin_loading_and_4_layer_config_override(
    tmp_path: Path,
    repo_root: Path,
    external_health_plugins_repo: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify loading `garmin` + `health` from a separate Git repository with ConfigMap & Env overrides."""
    monkeypatch.setenv("GARMIN_API_KEY", "secret-garmin-key-from-k8s-secret")
    monkeypatch.setenv("AGY_PLUGIN_HEALTH_DEFAULT_PROTEIN_G", "195.5")

    # Write a cluster ConfigMap YAML pointing `plugin_sources` at the external `agy-plugins-health` Git repo
    # and overriding `garmin.active_factor` (0.85 -> 0.8) and `health.default_calories_kcal` (2200 -> 2000).
    config_yaml_path = tmp_path / "config.yaml"
    config_yaml_path.write_text(
        f"""
plugin_sources:
  - repo: "{external_health_plugins_repo}"
    ref: "main"

plugins:
  garmin:
    active_factor: 0.8
  health:
    default_calories_kcal: 2000
    use_garmin_dynamic_target: true
  repo_healer:
    default_repo: "example-org/service-api"
""",
        encoding="utf-8",
    )

    bus = MemoryChannelBus()
    jobs = RecordingWorkerJobService()
    registry = PluginRegistry(state_root=tmp_path / "state", channels=bus, jobs=jobs)

    loaded = await registry.load_from_config_file(
        config_yaml_path,
        plugins_dir=repo_root / "plugins",
        include_entrypoints=False,
    )

    loaded_ids = [p.manifest.id for p in loaded]
    assert "garmin" in loaded_ids
    assert "health" in loaded_ids
    assert "repo_healer" in loaded_ids
    # Topological sort guarantees `garmin` loads before `health`
    assert loaded_ids.index("garmin") < loaded_ids.index("health")

    # Verify 4-layer config merging:
    # - `garmin.lookback_days` came from the external repo's bundled `config.yaml` (14)
    # - `garmin.active_factor` was overridden by the cluster ConfigMap (0.8)
    # - `garmin.api_key` was interpolated from `${GARMIN_API_KEY:-}` ("secret-garmin-key-from-k8s-secret")
    # - `health.default_protein_g` was overridden by `AGY_PLUGIN_HEALTH_DEFAULT_PROTEIN_G` (195.5)
    garmin_cfg = registry.plugins["garmin"].context.config
    assert garmin_cfg.lookback_days == 14
    assert garmin_cfg.active_factor == 0.8
    assert garmin_cfg.api_key == "secret-garmin-key-from-k8s-secret"

    health_cfg = registry.plugins["health"].context.config
    assert health_cfg.default_calories_kcal == 2000
    assert health_cfg.default_protein_g == 195.5

    app = create_microkernel_app(registry=registry, api_token="secret-token")
    client = TestClient(app, headers={"Authorization": "Bearer secret-token"})

    # 1. Verify /api/plugins metadata and static asset serving from the cloned git repo
    plugins_resp = client.get("/api/plugins")
    assert plugins_resp.status_code == 200
    pdata = plugins_resp.json()
    assert "garmin" in pdata["services"]
    assert any(v["mount_path"] == "/health" for v in pdata["pwa_views"])

    static_js = client.get("/api/plugins/health/static/health_view.js")
    assert static_js.status_code == 200
    assert "mountHealthView" in static_js.text

    # 2. Standalone PWA mount `/health/` rendered by plugin spec
    pwa_html = client.get("/health/")
    assert pwa_html.status_code == 200
    assert 'data-plugin-id="health"' in pwa_html.text

    # 3. Record 500 active kcal in Garmin -> should yield +400 kcal bonus (0.8 * 500) in Health
    g_resp = client.post(
        "/api/plugins/garmin/record",
        json={"local_date": "2026-11-18", "active_kcal": 500, "steps": 11200},
    )
    assert g_resp.status_code == 200
    assert g_resp.json()["dynamic_calorie_bonus_kcal"] == 400

    # 4. Log a meal via the Health plugin REST endpoint
    m_resp = client.post(
        "/api/plugins/health/log",
        json={
            "title": "Salmon & Rice Bowl",
            "calories_kcal": 700,
            "protein_g": 48.0,
            "local_date": "2026-11-18",
            "thread_ts": "t-meal-1",
        },
    )
    assert m_resp.status_code == 200
    today = m_resp.json()["today"]
    assert today["base_target_calories_kcal"] == 2000
    assert today["garmin_bonus_kcal"] == 400
    assert today["effective_target_calories_kcal"] == 2400
    assert today["remaining_calories_kcal"] == 1700
    assert today["target_protein_g"] == 195.5

    # 5. Interactive button action `health:scale:1:1.5` via Unified Channel Bus
    btn_resp = client.post(
        "/api/bus/button-action",
        json={"action_id": "health:scale:1:1.5"},
    )
    assert btn_resp.status_code == 200
    assert "1050 kcal" in btn_resp.json()["reply"]

    # 6. Zero-prefix channel message in `#agy-food` (role="food")
    chan_resp = client.post(
        "/api/bus/channel-message",
        json={"role": "food", "text": "Greek Yogurt & Berries", "thread_ts": "t-meal-2"},
    )
    assert chan_resp.status_code == 200
    assert chan_resp.json()["handled"] is True
    assert "Greek Yogurt & Berries" in chan_resp.json()["reply"]

    # 7. Thread-reply revision handler
    reply_resp = client.post(
        "/api/bus/thread-reply",
        json={"kind": "food", "thread_ts": "t-meal-2", "text": "with honey"},
    )
    assert reply_resp.status_code == 200
    assert "with honey" in reply_resp.json()["reply"]

    # 8. Directory plugin `!code` spawning an ephemeral K8s Worker JobSpec
    cmd_resp = client.post(
        "/api/bus/command",
        json={"command": "!code", "args": "Add handoff polling test"},
    )
    assert cmd_resp.status_code == 200
    assert len(jobs.spawned_jobs) == 1
    assert jobs.spawned_jobs[0].github_repo == "example-org/service-api"
    assert jobs.spawned_jobs[0].plugin_id == "repo_healer"


async def test_handoff_lifecycle_and_safe_git_patch_pull(tmp_path: Path, repo_root: Path) -> None:
    work_repo = tmp_path / "work_repo"
    work_repo.mkdir()
    subprocess.run(["git", "-C", str(work_repo), "init", "-b", "main"], check=True)
    subprocess.run(
        ["git", "-C", str(work_repo), "config", "user.email", "test@example.com"], check=True
    )
    subprocess.run(["git", "-C", str(work_repo), "config", "user.name", "Test User"], check=True)
    subprocess.run(
        [
            "git",
            "-C",
            str(work_repo),
            "remote",
            "add",
            "origin",
            "https://github.com/example-org/example-repo.git",
        ],
        check=True,
    )

    calc_py = work_repo / "calc.py"
    calc_py.write_text("def add(a: int, b: int) -> int:\n    return a + b\n", encoding="utf-8")
    subprocess.run(["git", "-C", str(work_repo), "add", "calc.py"], check=True)
    subprocess.run(["git", "-C", str(work_repo), "commit", "-m", "initial"], check=True)

    # Simulate dirty laptop state before driving: user added `sub()` uncommitted
    calc_py.write_text(
        "def add(a: int, b: int) -> int:\n    return a + b\n\n"
        "def sub(a: int, b: int) -> int:\n    return a - b\n",
        encoding="utf-8",
    )

    meta = detect_git_metadata(work_repo)
    assert meta["github_repo"] == "example-org/example-repo"
    assert "def sub" in meta["patch"]

    bus = MemoryChannelBus()
    jobs = RecordingWorkerJobService()
    registry = PluginRegistry(state_root=tmp_path / "state", channels=bus, jobs=jobs)
    await registry.load_all(plugins_dir=repo_root / "plugins", include_entrypoints=False)
    app = create_microkernel_app(registry=registry)
    client = TestClient(app)

    # 1. Push handoff from laptop
    create_resp = client.post(
        "/api/handoff",
        json={
            "github_repo": meta["github_repo"],
            "base_branch": meta["base_branch"],
            "base_sha": meta["base_sha"],
            "patch": meta["patch"],
            "prompt": "Also add mul(a, b) and unit tests while I drive home.",
            "semantic_state": "Added sub(a, b); still need mul(a, b).",
            "verify_hint": "pytest",
        },
    )
    assert create_resp.status_code == 200
    handoff = create_resp.json()
    handoff_id = handoff["handoff_id"]
    assert handoff["status"] == "running"
    assert handoff["is_terminal"] is False
    assert len(jobs.spawned_jobs) == 1
    assert jobs.spawned_jobs[0].base_sha == meta["base_sha"]
    assert "def sub" in (jobs.spawned_jobs[0].initial_patch or "")

    # 2. Simulate k3s worker pod completing the task
    pod_repo = tmp_path / "pod_work"
    subprocess.run(["git", "clone", str(work_repo), str(pod_repo)], check=True)
    (pod_repo / "calc.py").write_text(
        "def add(a: int, b: int) -> int:\n    return a + b\n\n"
        "def sub(a: int, b: int) -> int:\n    return a - b\n\n"
        "def mul(a: int, b: int) -> int:\n    return a * b\n",
        encoding="utf-8",
    )
    (pod_repo / "test_calc.py").write_text(
        "from calc import add, mul, sub\n\n"
        "def test_all() -> None:\n    assert (add(1, 2), sub(3, 1), mul(2, 4)) == (3, 2, 8)\n",
        encoding="utf-8",
    )
    subprocess.run(["git", "-C", str(pod_repo), "add", "-N", "."], check=True)
    pod_patch = subprocess.run(
        ["git", "-C", str(pod_repo), "diff", "--binary", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout

    complete_resp = client.post(
        f"/api/handoff/{handoff_id}/complete",
        json={
            "status": "awaiting_review",
            "remote_conversation_id": "conv-pod-99",
            "patch": pod_patch,
            "patch_stats": "2 files changed, 8 insertions(+)",
            "report": {
                "root_cause": "Continued from laptop initial.patch",
                "fix_summary": "Added mul(a, b) and test_calc.py",
                "affected_files": ["calc.py", "test_calc.py"],
                "tests_passed": True,
            },
        },
    )
    assert complete_resp.status_code == 200
    assert complete_resp.json()["is_terminal"] is True

    # 3. Laptop wakes up and polls /api/handoff/latest -> applies combined patch onto dirty working tree
    latest_resp = client.get("/api/handoff/latest?only_unpulled=true&include_patch=true")
    assert latest_resp.status_code == 200
    latest_data = latest_resp.json()
    assert latest_data["handoff_id"] == handoff_id

    ok, msg = apply_remote_patch_safely(
        work_repo,
        patch_text=latest_data["patch"],
        expected_base_sha=latest_data["base_sha"],
    )
    assert ok is True, msg
    assert "def mul" in (work_repo / "calc.py").read_text(encoding="utf-8")
    assert (work_repo / "test_calc.py").is_file()
    assert (work_repo / ".git" / "agy_handoff_pre_pull_backup.patch").is_file()

    # 4. Mark pulled
    pulled_resp = client.post(f"/api/handoff/{handoff_id}/mark-pulled")
    assert pulled_resp.status_code == 200
    assert pulled_resp.json()["status"] == "completed"
    assert pulled_resp.json()["pulled_at"] is not None


async def test_scaffold_and_export_client_skills(tmp_path: Path) -> None:
    plugins_dir = tmp_path / "plugins"
    scaffold_new_plugin(plugins_dir, "strava-sync")
    assert (plugins_dir / "strava_sync" / "plugin.yaml").is_file()
    assert (plugins_dir / "strava_sync" / "plugin.json").is_file()
    assert (plugins_dir / "strava_sync" / "skills" / "strava-sync" / "SKILL.md").is_file()

    bus = MemoryChannelBus()
    jobs = RecordingWorkerJobService()
    registry = PluginRegistry(state_root=tmp_path / "state", channels=bus, jobs=jobs)
    await registry.load_all(plugins_dir=plugins_dir, include_entrypoints=False)

    assert "strava_sync" in registry.plugins
    skills_target = tmp_path / "exported_skills"
    exported = registry.export_client_skills(skills_target)
    assert len(exported) == 1
    assert (skills_target / "strava-sync" / "SKILL.md").is_file()

    init_sh = build_worker_init_script()
    assert 'git checkout "$BASE_SHA"' in init_sh
    assert "APPLY_INITIAL_PATCH" in init_sh


async def test_per_plugin_python_deps_permissions_and_pinned_refs(
    tmp_path: Path,
    external_health_plugins_repo: Path,
) -> None:
    # 1. Verify pinned ref validation
    assert is_pinned_git_ref("v0.1.0") is True
    assert is_pinned_git_ref("e5463a0") is True
    assert is_pinned_git_ref("main") is False

    bus = MemoryChannelBus()
    jobs = RecordingWorkerJobService()
    installed_cmds: list[list[str]] = []

    def fake_pip_runner(cmd: list[str], **kwargs) -> None:
        installed_cmds.append(cmd)

    registry = PluginRegistry(
        state_root=tmp_path / "state",
        channels=bus,
        jobs=jobs,
        pip_installer_runner=fake_pip_runner,
    )

    # Reject unpinned ref when require_pinned_refs=True
    with pytest.raises(ValueError, match="unpinned ref"):
        await registry.load_all(
            plugin_sources=[PluginRepoSource(repo=str(external_health_plugins_repo), ref="main")],
            include_entrypoints=False,
            require_pinned_refs=True,
        )

    # 2. Create a plugin with `python_dependencies`, `requirements.txt`, and restricted `permissions`
    plugins_dir = tmp_path / "custom_plugins"
    pdir = scaffold_new_plugin(plugins_dir, "restricted-bot")
    (pdir / "requirements.txt").write_text("httpx>=0.28.0\n# comment\n", encoding="utf-8")
    manifest_txt = (pdir / "plugin.yaml").read_text(encoding="utf-8")
    manifest_txt += "python_dependencies:\n  - pydantic>=2.10.0\npermissions:\n  - channel_bus\n"
    (pdir / "plugin.yaml").write_text(manifest_txt, encoding="utf-8")

    await registry.load_all(plugins_dir=plugins_dir, include_entrypoints=False)
    assert "restricted_bot" in registry.installed_python_packages
    assert registry.installed_python_packages["restricted_bot"] == [
        "pydantic>=2.10.0",
        "httpx>=0.28.0",
    ]
    assert len(installed_cmds) == 1

    ctx = registry.plugins["restricted_bot"].context
    # `channel_bus` was granted, but `sqlite` and `k8s_jobs` were NOT granted:
    _ = ctx.channels
    with pytest.raises(PluginPermissionError, match="sqlite"):
        ctx.sqlite_path("forbidden.sqlite3")
    with pytest.raises(PluginPermissionError, match="k8s_jobs"):
        _ = ctx.jobs


async def test_pluggable_notification_transports_and_local_worker_runner(
    tmp_path: Path,
) -> None:
    # 1. Verify Discord, Telegram, Ntfy, Slack Webhook, and Generic Webhook transports
    recorded_requests: list[tuple[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        recorded_requests.append((str(request.url), request.content.decode("utf-8")))
        return httpx.Response(200, json={"ok": True})

    mock_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    cfgs = [
        TransportEntryConfig(
            kind="discord_webhook",
            url="https://discord.example/api/webhooks/1",
            roles=["coding"],
        ),
        TransportEntryConfig(
            kind="telegram",
            url="https://telegram.example",
            token="tg-tok",
            chat_id="12345",
            roles=["coding"],
        ),
        TransportEntryConfig(
            kind="ntfy",
            url="https://ntfy.example",
            topic="agy-alerts",
            roles=["coding"],
        ),
        TransportEntryConfig(
            kind="webhook",
            url="https://hook.example/events",
            token="hook-secret",
            roles=["coding"],
        ),
    ]
    bus = CompositeChannelBus(
        transports=[OutboundHttpTransport(c, http_client=mock_client) for c in cfgs]
    )
    await bus.post_message(
        "Build succeeded",
        role="coding",
        actions=[{"label": "Open PR", "action_id": "pr:open"}],
    )
    await mock_client.aclose()
    assert len(recorded_requests) == 4
    urls = [u for u, _ in recorded_requests]
    assert "https://discord.example/api/webhooks/1" in urls
    assert "https://telegram.example/bottg-tok/sendMessage" in urls
    assert "https://ntfy.example" in urls
    assert "https://hook.example/events" in urls

    # 2. Verify LocalSubprocessWorkerJobService & Kubernetes batch/v1 Job manifest builder
    origin_repo = tmp_path / "origin_repo"
    origin_repo.mkdir()
    subprocess.run(["git", "-C", str(origin_repo), "init", "-b", "main"], check=True)
    subprocess.run(
        ["git", "-C", str(origin_repo), "config", "user.email", "dev@example.com"], check=True
    )
    subprocess.run(["git", "-C", str(origin_repo), "config", "user.name", "Dev"], check=True)
    (origin_repo / "app.py").write_text("VALUE = 1\n", encoding="utf-8")
    subprocess.run(["git", "-C", str(origin_repo), "add", "app.py"], check=True)
    subprocess.run(["git", "-C", str(origin_repo), "commit", "-m", "init"], check=True)

    runner_cfg = WorkerRunnerConfig(
        mode="local",
        local_command=[
            "python3",
            "-c",
            "from pathlib import Path; Path('app.py').write_text('VALUE = 42\\n')",
        ],
    )
    local_worker = LocalSubprocessWorkerJobService(
        state_root=tmp_path / "state",
        cfg=runner_cfg,
        repo_resolver={"example-org/app": origin_repo},
    )
    job_spec = JobSpec(
        task_id="local-task-1",
        short_id="LOCAL-1",
        plugin_id="core.handoff",
        github_repo="example-org/app",
        prompt="Set VALUE = 42",
    )
    job_name = await local_worker.spawn_agent_job(job_spec)
    assert job_name == "local-job-local-task-1-t1"
    patch_file = tmp_path / "state" / "artifacts" / "local-task-1" / "fix.patch"
    assert patch_file.is_file()
    assert "+VALUE = 42" in patch_file.read_text(encoding="utf-8")

    k8s_manifest = build_k8s_worker_job_dict(job_spec, runner_cfg)
    assert k8s_manifest["kind"] == "Job"
    assert k8s_manifest["spec"]["template"]["spec"]["automountServiceAccountToken"] is False
