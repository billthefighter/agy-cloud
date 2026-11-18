from __future__ import annotations

import subprocess
from pathlib import Path

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
from agy_cloud.core.loader import PluginRegistry


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
