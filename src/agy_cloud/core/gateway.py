from __future__ import annotations

import secrets
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Depends, FastAPI, Header, HTTPException, Request, Response
from fastapi.responses import FileResponse, HTMLResponse
from pydantic import BaseModel, Field

from agy_cloud.core.handoff import HandoffService, HandoffStore, create_handoff_router
from agy_cloud.core.loader import PluginRegistry
from agy_cloud.sdk.spec import (
    ButtonEvent,
    ChannelMessageEvent,
    CommandEvent,
    JobSpec,
    PwaViewSpec,
    ThreadReplyEvent,
)


def build_worker_init_script() -> str:
    """Build the git-clone initContainer script supporting exact BASE_SHA and Turn-1 handoff patches."""
    return (
        "set -euo pipefail\n"
        'git clone --branch "$CLONE_REF" --single-branch /mirror /work\n'
        "cd /work\n"
        "git remote remove origin || true\n"
        'if [ -n "${BASE_SHA:-}" ]; then\n'
        '  git checkout "$BASE_SHA"\n'
        "fi\n"
        'if [ -s /artifacts/fix.patch ] && ( [ "$TURN" -gt 1 ] || [ "${APPLY_INITIAL_PATCH:-0}" = "1" ] ); then\n'
        '  echo "Applying patch in /work (turn=$TURN)..."\n'
        "  git apply /artifacts/fix.patch\n"
        "fi\n"
    )


@dataclass(slots=True)
class PostedBusMessage:
    ts: str
    role: str
    channel_id: str
    thread_ts: str | None
    text: str
    actions: list[dict[str, str]] = field(default_factory=list)


class MemoryChannelBus:
    """Default channel bus recording messages and WebSocket broadcasts in memory."""

    def __init__(self) -> None:
        self.messages: list[PostedBusMessage] = []
        self.ws_events: list[tuple[str, dict[str, Any]]] = []
        self._counter = 0

    def _next_ts(self) -> str:
        self._counter += 1
        return f"bus-{self._counter}"

    async def post_message(
        self,
        text: str,
        *,
        role: str = "",
        channel_id: str | None = None,
        actions: list[dict[str, str]] | None = None,
    ) -> str:
        ts = self._next_ts()
        self.messages.append(
            PostedBusMessage(
                ts=ts,
                role=role or "ops",
                channel_id=channel_id or f"pwa:{role or 'ops'}",
                thread_ts=None,
                text=text,
                actions=actions or [],
            )
        )
        return ts

    async def post_thread_message(
        self,
        thread_ts: str,
        text: str,
        *,
        channel_id: str | None = None,
        actions: list[dict[str, str]] | None = None,
    ) -> str:
        ts = self._next_ts()
        self.messages.append(
            PostedBusMessage(
                ts=ts,
                role="thread",
                channel_id=channel_id or "pwa:thread",
                thread_ts=thread_ts,
                text=text,
                actions=actions or [],
            )
        )
        return ts

    async def broadcast_ws(self, event_type: str, data: dict[str, Any]) -> None:
        self.ws_events.append((event_type, data))


class RecordingWorkerJobService:
    """Worker job dispatcher that records JobSpecs and builds K8s worker init scripts."""

    def __init__(self) -> None:
        self.spawned_jobs: list[JobSpec] = []

    async def spawn_agent_job(self, spec: JobSpec) -> str:
        self.spawned_jobs.append(spec)
        return f"agy-job-{spec.task_id}-t{spec.turn}"


class _BusCommandRequest(BaseModel):
    command: str
    args: str = ""
    user: str = "user"
    channel_id: str = "pwa:ops"
    thread_ts: str = "ts-1"
    model_override: str | None = None


class _BusChannelMessageRequest(BaseModel):
    role: str
    text: str
    user: str = "user"
    channel_id: str = "pwa:food"
    thread_ts: str = "ts-1"


class _BusThreadReplyRequest(BaseModel):
    kind: str
    thread_ts: str
    reply_ts: str = "reply-1"
    text: str
    user: str = "user"
    channel_id: str = "pwa:food"


class _BusButtonRequest(BaseModel):
    action_id: str
    value: str = ""
    user: str = "user"
    channel_id: str = "pwa:food"
    thread_ts: str = "ts-1"
    extra: dict[str, Any] = Field(default_factory=dict)


def _render_plugin_pwa_shell(view: PwaViewSpec, plugin_id: str, base_path: str) -> str:
    script_url = (
        f"{base_path}/api/plugins/{plugin_id}/static/{view.script_path.removeprefix('static/')}"
        if view.script_path
        else ""
    )
    script_tag = f'<script type="module" src="{script_url}"></script>' if script_url else ""
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover" />
  <title>{view.title}</title>
</head>
<body data-plugin-id="{plugin_id}" data-view-id="{view.id}">
  <main id="agy-plugin-root">
    <h1>{view.title}</h1>
  </main>
  {script_tag}
</body>
</html>"""


def create_microkernel_app(
    *,
    registry: PluginRegistry,
    handoff_service: HandoffService | None = None,
    api_token: str = "",
    base_path: str = "",
) -> FastAPI:
    """Create the `agy-cloud` FastAPI microkernel application with all loaded plugins mounted."""
    clean_base = "/" + base_path.strip("/") if base_path.strip("/") else ""
    app = FastAPI(title="agy-cloud Microkernel Gateway", version="0.1.0")

    def require_auth(
        request: Request,
        authorization: str | None = Header(default=None),
        x_agy_token: str | None = Header(default=None),
    ) -> None:
        if not api_token:
            return
        bearer = ""
        if authorization and authorization.lower().startswith("bearer "):
            bearer = authorization[7:].strip()
        candidate = bearer or (x_agy_token or "").strip() or request.query_params.get("token", "")
        if not candidate or not secrets.compare_digest(candidate, api_token):
            raise HTTPException(status_code=401, detail="Unauthorized")

    root_router = APIRouter(dependencies=[Depends(require_auth)])

    if handoff_service is None:
        handoff_store = HandoffStore(registry.state_root / "handoffs.sqlite3")
        handoff_service = HandoffService(
            state_root=registry.state_root,
            store=handoff_store,
            channels=registry.channels,
            jobs=registry.jobs,
        )

    root_router.include_router(create_handoff_router(handoff_service))

    @root_router.get("/api/plugins")
    async def list_loaded_plugins() -> dict[str, Any]:
        plugins_out: list[dict[str, Any]] = []
        pwa_views_out: list[dict[str, Any]] = []
        for pid, loaded in registry.plugins.items():
            m = loaded.manifest
            views = [v.model_dump() for v in m.pwa_views]
            for v in views:
                v["plugin_id"] = pid
                pwa_views_out.append(v)
            plugins_out.append(
                {
                    "id": pid,
                    "name": m.name,
                    "version": m.version,
                    "description": m.description,
                    "commands": [
                        {
                            "name": c.name,
                            "aliases": c.aliases,
                            "role": c.role,
                            "help": c.help_text,
                        }
                        for c in loaded.commands
                    ],
                    "channel_roles": [r.model_dump() for r in m.channel_roles],
                    "push_categories": [p.model_dump() for p in m.push_categories],
                    "pwa_views": views,
                }
            )
        return {
            "plugins": plugins_out,
            "pwa_views": pwa_views_out,
            "services": sorted(registry.services.keys()),
        }

    @root_router.post("/api/plugins/backup")
    async def backup_all_plugins(retain: int = 14) -> dict[str, Any]:
        backups = registry.backup_all_plugin_databases(retain=retain)
        return {"ok": True, "backups": backups}

    # Unified Channel Bus Dispatch Endpoints
    @root_router.post("/api/bus/command")
    async def bus_dispatch_command(body: _BusCommandRequest) -> dict[str, Any]:
        raw = f"!{body.command.lstrip('!')} {body.args}".strip()
        reply = await registry.dispatch_command(
            CommandEvent(
                command=body.command,
                args=body.args,
                raw_text=raw,
                user=body.user,
                channel_id=body.channel_id,
                thread_ts=body.thread_ts,
                model_override=body.model_override,
            )
        )
        if reply is None:
            raise HTTPException(status_code=404, detail=f"Unknown command `{body.command}`.")
        return {"ok": True, "reply": reply}

    @root_router.post("/api/bus/channel-message")
    async def bus_dispatch_channel_message(body: _BusChannelMessageRequest) -> dict[str, Any]:
        reply = await registry.dispatch_channel_message(
            ChannelMessageEvent(
                role=body.role,
                text=body.text,
                user=body.user,
                channel_id=body.channel_id,
                thread_ts=body.thread_ts,
            )
        )
        return {"ok": True, "handled": reply is not None, "reply": reply}

    @root_router.post("/api/bus/thread-reply")
    async def bus_dispatch_thread_reply(body: _BusThreadReplyRequest) -> dict[str, Any]:
        reply = await registry.dispatch_thread_reply(
            ThreadReplyEvent(
                kind=body.kind,
                thread_ts=body.thread_ts,
                reply_ts=body.reply_ts,
                text=body.text,
                user=body.user,
                channel_id=body.channel_id,
            )
        )
        return {"ok": True, "handled": reply is not None, "reply": reply}

    @root_router.post("/api/bus/button-action")
    async def bus_dispatch_button_action(body: _BusButtonRequest) -> dict[str, Any]:
        prefix = body.action_id.partition(":")[0]
        reply = await registry.dispatch_button_action(
            ButtonEvent(
                action_prefix=prefix,
                action_id=body.action_id,
                value=body.value,
                user=body.user,
                channel_id=body.channel_id,
                thread_ts=body.thread_ts,
            )
        )
        if reply is None:
            raise HTTPException(
                status_code=404, detail=f"No plugin handled button action `{body.action_id}`."
            )
        return {"ok": True, "reply": reply}

    # Legacy `/api/media/{relpath:path}` compatibility route for existing `agent-tasks` PVC photos
    @root_router.get("/api/media/{relpath:path}")
    async def serve_legacy_pvc_media(relpath: str) -> Response:
        state_root = registry.state_root.resolve()
        allowed_roots = [
            (state_root / "food_photos").resolve(),
            (state_root / "pwa_uploads").resolve(),
        ]
        candidate = (state_root / relpath).resolve()
        if not any(candidate.is_relative_to(r) for r in allowed_roots) or not candidate.is_file():
            raise HTTPException(status_code=404, detail="Media file not found.")
        return FileResponse(candidate)

    # Mount each plugin's custom APIRouter, isolated media dir, static dir, and standalone PWA views
    for pid, loaded in registry.plugins.items():
        granted = loaded.context.granted_permissions
        plugin_router = loaded.plugin.get_api_router() if "api_router" in granted else None
        if plugin_router is not None:
            root_router.include_router(
                plugin_router,
                prefix=f"/api/plugins/{pid}",
                tags=[f"plugin:{pid}"],
            )

        if "media" in granted:

            def _make_media_handler(plugin_media_root: Path):
                async def serve_plugin_media(relpath: str) -> Response:
                    if ".." in Path(relpath).parts:
                        raise HTTPException(status_code=404, detail="Plugin media file not found.")
                    resolved_root = plugin_media_root.resolve()
                    resolved_state = registry.state_root.resolve()
                    candidate = (plugin_media_root / relpath).resolve()
                    if (
                        not (
                            candidate.is_relative_to(resolved_root)
                            or candidate.is_relative_to(resolved_state)
                        )
                        or not candidate.is_file()
                    ):
                        raise HTTPException(status_code=404, detail="Plugin media file not found.")
                    return FileResponse(candidate)

                return serve_plugin_media

            root_router.add_api_route(
                f"/api/plugins/{pid}/media/{{relpath:path}}",
                _make_media_handler(loaded.context.media_dir),
                methods=["GET"],
                tags=[f"plugin:{pid}"],
            )

        if loaded.source_dir is not None and (loaded.source_dir / "static").is_dir():
            static_root = (loaded.source_dir / "static").resolve()

            def _make_static_handler(sroot: Path):
                async def serve_plugin_static(relpath: str) -> Response:
                    candidate = (sroot / relpath).resolve()
                    if not candidate.is_relative_to(sroot) or not candidate.is_file():
                        raise HTTPException(
                            status_code=404, detail="Plugin static asset not found."
                        )
                    return FileResponse(candidate)

                return serve_plugin_static

            root_router.add_api_route(
                f"/api/plugins/{pid}/static/{{relpath:path}}",
                _make_static_handler(static_root),
                methods=["GET"],
                tags=[f"plugin:{pid}"],
            )

        if "pwa_views" in granted:
            for view in loaded.manifest.pwa_views:
                if view.standalone_pwa and view.mount_path:
                    mount_p = "/" + view.mount_path.strip("/")

                    def _make_view_handler(v: PwaViewSpec, p_id: str):
                        async def serve_pwa_view() -> HTMLResponse:
                            return HTMLResponse(_render_plugin_pwa_shell(v, p_id, clean_base))

                        return serve_pwa_view

                    app.add_api_route(mount_p, _make_view_handler(view, pid), methods=["GET"])
                    app.add_api_route(f"{mount_p}/", _make_view_handler(view, pid), methods=["GET"])

    app.include_router(root_router, prefix=clean_base)
    return app
