from __future__ import annotations

import hashlib
from typing import Any

from fastapi import APIRouter
from pydantic import BaseModel

from agy_cloud.sdk import (
    AgyCloudPlugin,
    CommandEvent,
    JobSpec,
    command,
)


class RepoHealerConfig(BaseModel):
    enabled: bool = True
    default_repo: str = "example-org/example-repo"
    default_branch: str = "main"
    verify_hint: str = "uv run pytest"
    default_model: str = "gemini-4-hydrogen"


class RepoHealerPlugin(AgyCloudPlugin[RepoHealerConfig]):
    """Directory-loaded plugin demonstrating `ctx.jobs.spawn_agent_job(JobSpec)`."""

    config_model = RepoHealerConfig

    @command(
        "code", aliases=["heal"], role="coding", help="Spawn a k3s worker pod for a coding task"
    )
    async def handle_code_command(self, event: CommandEvent) -> str:
        prompt = event.args.strip()
        if not prompt:
            return "Usage: `!code <task description>`"
        digest = hashlib.sha1(f"{event.thread_ts}:{prompt}".encode()).hexdigest()[:6].upper()
        short_id = f"CODE-{digest}"
        task_id = f"code-{digest.lower()}"
        model = event.model_override or self.ctx.config.default_model

        job_name = await self.ctx.jobs.spawn_agent_job(
            JobSpec(
                task_id=task_id,
                short_id=short_id,
                plugin_id=self.manifest.id,
                github_repo=self.ctx.config.default_repo,
                base_branch=self.ctx.config.default_branch,
                verify_hint=self.ctx.config.verify_hint,
                prompt=prompt,
                model=model,
            )
        )
        return (
            f"Spawned worker Job `{job_name}` for `{short_id}` "
            f"on `{self.ctx.config.default_repo}` (`{model}`)."
        )

    def get_api_router(self) -> APIRouter:
        router = APIRouter()

        @router.get("/config")
        async def get_config() -> dict[str, Any]:
            return self.ctx.config.model_dump()

        return router
