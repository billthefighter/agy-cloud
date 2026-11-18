from __future__ import annotations

from typing import Any

from fastapi import APIRouter
from pydantic import BaseModel

from agy_cloud.sdk import AgyCloudPlugin, CommandEvent, command


class K8sWatchdogConfig(BaseModel):
    enabled: bool = True
    poll_interval_seconds: int = 120


class K8sWatchdogPlugin(AgyCloudPlugin[K8sWatchdogConfig]):
    config_model = K8sWatchdogConfig

    @command("k8s", role="k8s", help="Display Kubernetes pod and Argo CD sync summary")
    async def handle_k8s(self, event: CommandEvent) -> str:
        return "K8s Watchdog plugin active."

    def get_api_router(self) -> APIRouter:
        router = APIRouter()

        @router.get("/status")
        async def get_status() -> dict[str, Any]:
            return {"plugin": "k8s_watchdog", "config": self.ctx.config.model_dump()}

        return router
