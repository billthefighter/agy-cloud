from __future__ import annotations

from typing import Any

from fastapi import APIRouter
from pydantic import BaseModel

from agy_cloud.sdk import AgyCloudPlugin, CommandEvent, command


class CiHealerConfig(BaseModel):
    enabled: bool = True
    poll_interval_seconds: int = 240


class CiHealerPlugin(AgyCloudPlugin[CiHealerConfig]):
    config_model = CiHealerConfig

    @command("ci", role="ci", help="Summarize recent failed GitHub Actions workflows")
    async def handle_ci(self, event: CommandEvent) -> str:
        return "CI Healer plugin active."

    def get_api_router(self) -> APIRouter:
        router = APIRouter()

        @router.get("/status")
        async def get_status() -> dict[str, Any]:
            return {"plugin": "ci_healer", "config": self.ctx.config.model_dump()}

        return router
