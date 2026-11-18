from __future__ import annotations

import argparse
import asyncio
import json
from dataclasses import asdict
from pathlib import Path

import uvicorn
import yaml

from agy_cloud.core.gateway import (
    create_microkernel_app,
)
from agy_cloud.core.loader import PluginRegistry
from agy_cloud.core.migration import migrate_agent_tasks_pvc
from agy_cloud.core.transports import CompositeChannelBus
from agy_cloud.core.workers import create_worker_job_service
from agy_cloud.sdk.spec import AgyCloudConfig, WorkerRunnerConfig


def scaffold_new_plugin(target_root: Path, plugin_id: str) -> Path:
    """Scaffold a new Dual Server + Client plugin bundle under `target_root/<plugin_id>`."""
    clean_id = plugin_id.strip().lower().replace("-", "_")
    class_prefix = "".join(part.capitalize() for part in clean_id.split("_"))
    plugin_dir = target_root / clean_id
    plugin_dir.mkdir(parents=True, exist_ok=True)
    (plugin_dir / "skills" / clean_id.replace("_", "-")).mkdir(parents=True, exist_ok=True)

    manifest_yaml = {
        "api_version": "agy.cloud/v1",
        "id": clean_id,
        "name": f"{class_prefix} Plugin",
        "version": "0.1.0",
        "description": f"Custom agy-cloud plugin `{clean_id}`.",
        "entrypoint": f"server:{class_prefix}Plugin",
        "channel_roles": [
            {
                "role": clean_id,
                "title": f"{class_prefix} Feed",
                "description": f"Notifications for {clean_id}",
            }
        ],
        "skills_dir": "skills",
    }
    (plugin_dir / "plugin.yaml").write_text(
        yaml.safe_dump(manifest_yaml, sort_keys=False), encoding="utf-8"
    )
    (plugin_dir / "plugin.json").write_text(
        json.dumps(
            {
                "name": f"agy-plugin-{clean_id.replace('_', '-')}",
                "version": "0.1.0",
                "description": manifest_yaml["description"],
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    (plugin_dir / "server.py").write_text(
        f"""from __future__ import annotations

from fastapi import APIRouter
from pydantic import BaseModel

from agy_cloud.sdk import AgyCloudPlugin, CommandEvent, command


class {class_prefix}Config(BaseModel):
    enabled: bool = True


class {class_prefix}Plugin(AgyCloudPlugin[{class_prefix}Config]):
    config_model = {class_prefix}Config

    @command("{clean_id}", role="{clean_id}", help="Run {clean_id} command")
    async def handle_command(self, event: CommandEvent) -> str:
        return f"{class_prefix} plugin received: {{event.args}}"

    def get_api_router(self) -> APIRouter:
        router = APIRouter()

        @router.get("/status")
        async def status() -> dict[str, str]:
            return {{"plugin": "{clean_id}", "status": "ok"}}

        return router
""",
        encoding="utf-8",
    )
    skill_name = clean_id.replace("_", "-")
    (plugin_dir / "skills" / skill_name / "SKILL.md").write_text(
        f"""---
name: {skill_name}
description: Interact with the `{clean_id}` plugin on your agy-cloud k3s cluster.
---

# {class_prefix} Skill

Use this skill to interact with `/api/plugins/{clean_id}` on your `agy-cloud` cluster.
""",
        encoding="utf-8",
    )
    return plugin_dir


async def _load_registry(
    state_dir: Path,
    plugins_dir: Path,
    config_file: Path | None = None,
    worker_runner_mode: str = "",
) -> PluginRegistry:
    cluster_cfg = AgyCloudConfig()
    if config_file is not None and config_file.is_file():
        raw = yaml.safe_load(config_file.read_text(encoding="utf-8")) or {}
        if isinstance(raw, dict):
            cluster_cfg = AgyCloudConfig.model_validate(raw)

    runner_cfg = cluster_cfg.worker_runner
    if worker_runner_mode:
        runner_cfg = WorkerRunnerConfig(**{**runner_cfg.model_dump(), "mode": worker_runner_mode})

    bus = CompositeChannelBus.from_configs(cluster_cfg.transports)
    jobs = create_worker_job_service(runner_cfg, state_root=state_dir)
    registry = PluginRegistry(state_root=state_dir, channels=bus, jobs=jobs)
    if config_file is not None and config_file.is_file():
        await registry.load_from_config_file(
            config_file,
            plugins_dir=plugins_dir,
            include_entrypoints=True,
        )
    else:
        await registry.load_all(plugins_dir=plugins_dir, include_entrypoints=True)
    return registry


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="agy-cloud microkernel & plugin CLI")
    sub = parser.add_subparsers(dest="cmd")

    p_serve = sub.add_parser("serve", help="Run the agy-cloud microkernel HTTP gateway")
    p_serve.add_argument("--host", default="0.0.0.0")
    p_serve.add_argument("--port", type=int, default=8080)
    p_serve.add_argument("--state-dir", default="/tmp/agy-cloud-state")
    p_serve.add_argument("--plugins-dir", default="./plugins")
    p_serve.add_argument("--config", default="", help="Path to cluster config.yaml (ConfigMap)")
    p_serve.add_argument(
        "--worker-runner",
        default="",
        choices=["", "recording", "local", "k8s"],
        help="Override worker runner mode (`local` runs jobs as local subprocesses without K8s)",
    )
    p_serve.add_argument(
        "--auto-migrate-pvc",
        action="store_true",
        help="Run non-destructive agent-tasks PVC migration on startup before loading plugins",
    )
    p_serve.add_argument("--api-token", default="")
    p_serve.add_argument("--base-path", default="")

    p_mig = sub.add_parser(
        "migrate-pvc",
        help="Non-destructively migrate an existing agent-tasks PVC (/state) to agy-cloud layout",
    )
    p_mig.add_argument("--state-dir", default="/state", help="Path to mounted PVC (/state)")

    p_plugins = sub.add_parser("plugins", help="Inspect, scaffold, or export plugins")
    p_sub = p_plugins.add_subparsers(dest="plugins_cmd")

    p_list = p_sub.add_parser(
        "list", help="List discovered entry-point, git, and directory plugins"
    )
    p_list.add_argument("--state-dir", default="/tmp/agy-cloud-state")
    p_list.add_argument("--plugins-dir", default="./plugins")
    p_list.add_argument("--config", default="", help="Path to cluster config.yaml (ConfigMap)")

    p_new = p_sub.add_parser("new", help="Scaffold a new Dual Server+Client plugin bundle")
    p_new.add_argument("plugin_id", help="Plugin identifier (e.g. strava, homeassistant)")
    p_new.add_argument("--plugins-dir", default="./plugins")

    p_export = p_sub.add_parser("install-skills", help="Export client SKILL.md files from plugins")
    p_export.add_argument("--state-dir", default="/tmp/agy-cloud-state")
    p_export.add_argument("--plugins-dir", default="./plugins")
    p_export.add_argument("--config", default="", help="Path to cluster config.yaml (ConfigMap)")
    p_export.add_argument(
        "--target-dir",
        default=str(Path.home() / ".gemini" / "config" / "skills"),
    )

    args = parser.parse_args(argv)
    if args.cmd == "migrate-pvc":
        report = migrate_agent_tasks_pvc(Path(args.state_dir))
        print(json.dumps(asdict(report), indent=2))
        return 0

    if args.cmd == "serve":
        if args.auto_migrate_pvc:
            migrate_agent_tasks_pvc(Path(args.state_dir))
        cfg_path = Path(args.config) if args.config else None
        reg = asyncio.run(
            _load_registry(
                Path(args.state_dir),
                Path(args.plugins_dir),
                cfg_path,
                worker_runner_mode=args.worker_runner,
            )
        )
        app = create_microkernel_app(
            registry=reg,
            api_token=args.api_token,
            base_path=args.base_path,
        )
        uvicorn.run(app, host=args.host, port=args.port)
        return 0

    if args.cmd == "plugins":
        if args.plugins_cmd == "new":
            out = scaffold_new_plugin(Path(args.plugins_dir), args.plugin_id)
            print(f"Scaffolded plugin at {out}")
            return 0
        cfg_path = Path(getattr(args, "config", "")) if getattr(args, "config", "") else None
        if args.plugins_cmd == "install-skills":
            reg = asyncio.run(
                _load_registry(Path(args.state_dir), Path(args.plugins_dir), cfg_path)
            )
            exported = reg.export_client_skills(Path(args.target_dir))
            for p in exported:
                print(f"Exported skill: {p}")
            return 0
        reg = asyncio.run(_load_registry(Path(args.state_dir), Path(args.plugins_dir), cfg_path))
        print(reg.build_help_markdown())
        return 0

    parser.print_help()
    return 0
