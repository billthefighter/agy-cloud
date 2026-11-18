from __future__ import annotations

from typing import Any

from fastapi import APIRouter
from pydantic import BaseModel

from agy_cloud.sdk import AgyCloudPlugin, CommandEvent, command


class SentryTriageConfig(BaseModel):
    enabled: bool = True
    poll_interval_seconds: int = 180


class SentryTriagePlugin(AgyCloudPlugin[SentryTriageConfig]):
    config_model = SentryTriageConfig

    @command(
        "status",
        aliases=["ping"],
        role="sentry",
        help="Display controller and Sentry triage status",
    )
    async def handle_status(self, event: CommandEvent) -> str:
        return (
            f"Sentry Triage plugin active (poll_interval={self.ctx.config.poll_interval_seconds}s)."
        )

    def get_api_router(self) -> APIRouter:
        router = APIRouter()

        @router.get("/status")
        async def get_status() -> dict[str, Any]:
            return {"plugin": "sentry_triage", "config": self.ctx.config.model_dump()}

        return router
