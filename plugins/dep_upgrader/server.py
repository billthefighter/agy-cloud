from __future__ import annotations

from typing import Any

from fastapi import APIRouter
from pydantic import BaseModel

from agy_cloud.sdk import AgyCloudPlugin, CommandEvent, command


class DepUpgraderConfig(BaseModel):
    enabled: bool = True


class DepUpgraderPlugin(AgyCloudPlugin[DepUpgraderConfig]):
    config_model = DepUpgraderConfig

    @command("deps", role="deps", help="Trigger an on-demand dependency and security audit job")
    async def handle_deps(self, event: CommandEvent) -> str:
        return f"Dependency upgrader triggered (focus={event.args.strip() or 'all'})."

    def get_api_router(self) -> APIRouter:
        router = APIRouter()

        @router.get("/status")
        async def get_status() -> dict[str, Any]:
            return {"plugin": "dep_upgrader", "config": self.ctx.config.model_dump()}

        return router
