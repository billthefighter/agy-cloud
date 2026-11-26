from __future__ import annotations

import asyncio
import hashlib
import io
import json
import os
import re
import secrets
import time
from dataclasses import asdict
from datetime import UTC, datetime
from functools import lru_cache
from pathlib import Path
from typing import TYPE_CHECKING, Annotated, Any

import structlog
from fastapi import (
    APIRouter,
    Depends,
    FastAPI,
    File,
    Form,
    HTTPException,
    Query,
    Request,
    Response,
    UploadFile,
    WebSocket,
    WebSocketDisconnect,
)
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, RedirectResponse
from PIL import Image, ImageDraw
from pydantic import BaseModel, Field

from agent_tasks.channels.base import (
    ApproveCallback,
    BillingCommandCallback,
    CodingCallback,
    DepsCallback,
    FeedbackCallback,
    FoodEntryActionCallback,
    FoodMessageCallback,
    FoodScaleCallback,
    FoodThreadReplyCallback,
    IgnoreCallback,
    MemoryCommandCallback,
    PushBranchCallback,
    ResolveCallback,
    SaveMemoryCallback,
    SnoozeCallback,
    StatusCallback,
    TimersCommandCallback,
)
from agent_tasks.common.config import ControllerSettings, TargetsConfig, parse_model_override
from agent_tasks.common.models import IssueStatus, TriageReport, WorkerEnvelope
from agent_tasks.common.state import IssueRecord, PwaMessageRecord, PwaThreadRecord, StateStore
from agent_tasks.pwa.auth import SESSION_COOKIE_NAME, SESSION_MAX_AGE_SECONDS, PwaAuthManager
from agent_tasks.pwa.push import DEFAULT_PUSH_PREFS, WebPushManager
from agent_tasks.tasks.billing_guard.pricing import compute_billing_cycle
from agent_tasks.tasks.direct_chat import DirectChatService
from agent_tasks.tasks.sentry_triage.git_ops import (
    DEFAULT_MAX_REPO_SNAPSHOT_BYTES,
    RepoSnapshotTooLargeError,
    check_github_repo_access,
    validate_and_write_repo_snapshot,
)
from agy_cloud.core.gateway import build_ui_manifest

if TYPE_CHECKING:
    from agent_tasks.channels.slack_socket import SlackSocketAdapter
    from agent_tasks.tasks.billing_guard import BillingMonitorTask
    from agent_tasks.tasks.food_tracker import FoodTrackerTask

log = structlog.get_logger(__name__)

STATIC_DIR = Path(__file__).resolve().parent / "static"

TASK_KIND_TO_PWA_KIND: dict[str, str] = {
    "sentry": "sentry",
    "slack_coding": "coding",
    "ci_healer": "ci",
    "dep_upgrader": "deps",
    "k8s_watchdog": "k8s",
    "food_tracker": "food",
    "billing": "billing",
    "direct_chat": "chat",
}

ROLE_TO_FEED_TITLE: dict[str, str] = {
    "billing": "Billing & Credit Alerts",
    "k8s": "K8s & Argo CD Watchdog",
    "ci": "CI Healer Feed",
    "deps": "Dependency Upgrader",
    "sentry": "Sentry Triage Feed",
    "coding": "Coding Tasks Feed",
    "food": "Food Tracker Digest",
    "ops": "Cluster Operations",
}

_URGENT_MARKERS = (
    "rotating_light",
    "🚨",
    "warning",
    "⚠️",
    "hard-stop",
    "safety brake",
    "crashloop",
    "oom",
)


def _synth_ts(prefix: str = "pwa") -> str:
    return f"{prefix}-{int(time.time() * 1000)}-{secrets.token_hex(3)}"


def _blocks_to_markdown(
    fallback_text: str, blocks: list[dict[str, Any]]
) -> tuple[str, list[dict[str, str]], int | None]:
    sections: list[str] = []
    actions: list[dict[str, str]] = []
    food_entry_id: int | None = None

    for block in blocks:
        btype = str(block.get("type") or "")
        if btype == "section":
            txt_obj = block.get("text")
            if isinstance(txt_obj, dict) and txt_obj.get("text"):
                sections.append(str(txt_obj["text"]))
        elif btype == "context":
            ctx_parts = [
                str(el["text"])
                for el in (block.get("elements") or [])
                if isinstance(el, dict) and el.get("text")
            ]
            if ctx_parts:
                sections.append("\n".join(ctx_parts))
        elif btype == "actions":
            for el in block.get("elements") or []:
                if not isinstance(el, dict) or el.get("type") != "button":
                    continue
                action_id = str(el.get("action_id") or "")
                value = str(el.get("value") or "")
                label = str((el.get("text") or {}).get("text") or action_id)
                actions.append({"action_id": action_id, "label": label, "value": value})
                if (
                    food_entry_id is None
                    and action_id.startswith(("food_scale_", "food_save_staple_", "food_delete_"))
                    and value.isdigit()
                ):
                    food_entry_id = int(value)

    body = "\n\n".join(sections).strip() or fallback_text
    return body, actions, food_entry_id


@lru_cache(maxsize=12)
def render_pwa_icon_png(size: int, variant: str = "chat") -> bytes:
    """Generate a crisp rounded dark-mode AGY app icon PNG for iOS Home Screen."""
    dim = max(64, min(size, 1024))
    img = Image.new("RGBA", (dim, dim), (11, 16, 32, 255))
    draw = ImageDraw.Draw(img)

    pad = round(dim * 0.12)
    is_health = variant == "health"
    outline_col = (16, 185, 129, 225) if is_health else (56, 189, 248, 220)
    fill_col = (13, 35, 34, 255) if is_health else (19, 27, 49, 255)
    draw.rounded_rectangle(
        [pad, pad, dim - pad, dim - pad],
        radius=round(dim * 0.22),
        fill=fill_col,
        outline=outline_col,
        width=max(2, round(dim * 0.035)),
    )

    cx, cy = dim / 2, dim / 2
    stroke = max(3, round(dim * 0.055))
    if is_health:
        # Health pulse / heart-rate waveform glyph
        r = dim * 0.24
        pts = [
            (cx - r, cy + r * 0.08),
            (cx - r * 0.45, cy + r * 0.08),
            (cx - r * 0.22, cy - r * 0.55),
            (cx + r * 0.08, cy + r * 0.62),
            (cx + r * 0.34, cy - r * 0.24),
            (cx + r * 0.52, cy + r * 0.08),
            (cx + r, cy + r * 0.08),
        ]
        draw.line(pts, fill=(52, 211, 153, 255), width=stroke, joint="curve")
        node_r = max(4, round(dim * 0.06))
        for nx, ny, color in [
            (cx - r * 0.22, cy - r * 0.55, (16, 185, 129, 255)),
            (cx + r * 0.08, cy + r * 0.62, (56, 189, 248, 255)),
        ]:
            draw.ellipse(
                [nx - node_r, ny - node_r, nx + node_r, ny + node_r],
                fill=color,
                outline=(11, 16, 32, 255),
                width=max(1, round(dim * 0.015)),
            )
    else:
        # Geometric AGY triangle / orbital glyph
        r = dim * 0.22
        top = (cx, cy - r)
        bl = (cx - r * 0.88, cy + r * 0.72)
        br = (cx + r * 0.88, cy + r * 0.72)
        draw.line([top, bl, br, top], fill=(56, 189, 248, 255), width=stroke, joint="curve")

        node_r = max(4, round(dim * 0.068))
        for nx, ny, color in [
            (*top, (129, 140, 248, 255)),
            (*bl, (56, 189, 248, 255)),
            (*br, (34, 197, 94, 255)),
        ]:
            draw.ellipse(
                [nx - node_r, ny - node_r, nx + node_r, ny + node_r],
                fill=color,
                outline=(11, 16, 32, 255),
                width=max(1, round(dim * 0.015)),
            )

    buf = io.BytesIO()
    img.convert("RGB").save(buf, format="PNG", optimize=True)
    return buf.getvalue()


class _LoginRequest(BaseModel):
    token: str


class _CodingRequest(BaseModel):
    prompt: str
    repo: str = ""
    model: str = ""


DEFAULT_MAX_INLINE_PATCH_BYTES = 5 * 1024 * 1024  # 5 MiB
MAX_CONTEXT_FILES = 20
MAX_CONTEXT_FILE_BYTES = 512 * 1024  # 512 KiB per context file
MAX_TOTAL_CONTEXT_BYTES = 2 * 1024 * 1024  # 2 MiB total across context files


class _ContextFilesTooLargeError(ValueError):
    """Raised when uploaded context files exceed size limits."""


class _HandoffContextFile(BaseModel):
    name: str
    content: str


def _sanitize_context_filename(raw_name: str, index: int) -> str:
    # Strip any POSIX or Windows path prefixes so traversal is impossible
    leaf = raw_name.replace("\\", "/").rsplit("/", 1)[-1].strip()
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", leaf).lstrip("._-")
    if not cleaned:
        cleaned = f"context_{index}.txt"
    return cleaned[:120]


def _materialize_context_files(
    art_dir: Path, context_files: list[_HandoffContextFile]
) -> list[str]:
    if not context_files:
        return []
    if len(context_files) > MAX_CONTEXT_FILES:
        raise ValueError(f"Too many context_files ({len(context_files)}; max {MAX_CONTEXT_FILES}).")
    ctx_dir = art_dir / "context"
    written_names: list[str] = []
    used: set[str] = set()
    total_bytes = 0
    prepared: list[tuple[str, bytes]] = []
    for idx, cf in enumerate(context_files, start=1):
        base_name = _sanitize_context_filename(cf.name, idx)
        candidate = base_name
        stem, dot, ext = base_name.partition(".")
        suffix_n = 2
        while candidate in used:
            candidate = f"{stem}_{suffix_n}.{ext}" if dot else f"{base_name}_{suffix_n}"
            suffix_n += 1
        used.add(candidate)
        encoded = cf.content.encode("utf-8")
        if len(encoded) > MAX_CONTEXT_FILE_BYTES:
            raise _ContextFilesTooLargeError(
                f"Context file `{candidate}` is {len(encoded)} bytes "
                f"(exceeds {MAX_CONTEXT_FILE_BYTES} byte limit)."
            )
        total_bytes += len(encoded)
        if total_bytes > MAX_TOTAL_CONTEXT_BYTES:
            raise _ContextFilesTooLargeError(
                f"Total context_files size exceeds {MAX_TOTAL_CONTEXT_BYTES} byte limit."
            )
        prepared.append((candidate, encoded))

    ctx_dir.mkdir(parents=True, exist_ok=True)
    for name, encoded in prepared:
        (ctx_dir / name).write_bytes(encoded)
        written_names.append(name)
    return written_names


class _HandoffCreateRequest(BaseModel):
    prompt: str
    repo: str = ""
    github_repo: str = ""
    branch: str = ""
    base_branch: str = ""
    base_sha: str = ""
    base_patch: str = ""
    patch: str = ""
    repo_snapshot_b64: str = ""
    context_files: list[_HandoffContextFile] = Field(default_factory=list)
    semantic_state: str = ""
    verify_hint: str = ""
    focus_subdir: str = ""
    model: str = ""
    worker_backend: str = ""
    title: str = ""
    source_conversation_id: str = ""
    local_conversation_id: str | None = None
    workspace_path: str = ""
    timeout_seconds: int | None = None
    idempotency_key: str = ""


class _HandoffFeedbackRequest(BaseModel):
    feedback: str
    model: str = ""


class _ThreadOrgPatch(BaseModel):
    folder: str | None = None
    pinned: bool | None = None
    mark_read: bool = False
    mark_unread: bool = False
    tags: list[str] | None = None
    title: str | None = None


class _ThreadActionRequest(BaseModel):
    action: str


class _ScaleFoodRequest(BaseModel):
    multiplier: float


class _LogAgainRequest(BaseModel):
    logged_at: str = ""
    local_date: str = ""


class _UpdateMealItemRequest(BaseModel):
    name: str
    quantity_desc: str = "1 serving"
    estimated_grams: float | None = None
    calories_kcal: float
    protein_g: float = 0.0
    carbs_g: float = 0.0
    fat_g: float = 0.0
    fiber_g: float = 0.0
    source: str = "user_edit"


class _UpdateMealRequest(BaseModel):
    local_date: str | None = None
    meal_slot: str | None = None
    title: str | None = None
    scale_factor: float | None = None
    items: list[_UpdateMealItemRequest] | None = None
    view_date: str = ""


class _MealReactionRequest(BaseModel):
    emoji: str | None = None


class _WeightRequest(BaseModel):
    weight_lbs: float
    notes: str = ""


class _GoalsRequest(BaseModel):
    calories_kcal: int
    protein_g: float
    carbs_g: float
    fat_g: float
    fiber_g: float = 30.0


class _SuggestFoodRequest(BaseModel):
    craving: str = ""
    model: str = ""


class _GarminSettingsRequest(BaseModel):
    dynamic_enabled: bool | None = None
    goal_delta_kcal: int | None = None
    dynamic_mode: str | None = None
    active_factor: float | None = None


class _BillingCmdRequest(BaseModel):
    command: str


class _DepsRequest(BaseModel):
    focus: str = ""
    model: str = ""


class _SnoozeRequest(BaseModel):
    duration: str = "2h"
    note: str = ""


class _RememberRequest(BaseModel):
    text: str
    scope: str = "instructions"


class _PushSubKeys(BaseModel):
    p256dh: str
    auth: str


class _PushSubscribeRequest(BaseModel):
    endpoint: str
    keys: _PushSubKeys
    prefs: dict[str, bool] | None = None
    app_mode: str | None = None


class _PushPrefsRequest(BaseModel):
    endpoint: str
    prefs: dict[str, bool]
    app_mode: str | None = None


class _PushSyncRequest(BaseModel):
    endpoint: str
    app_mode: str | None = None


class _PushUnsubscribeRequest(BaseModel):
    endpoint: str


class PendingFoodLog(BaseModel):
    thread_ts: str
    label: str
    model: str = ""
    started_at: str


class _WsClientState:
    def __init__(self, ws: WebSocket) -> None:
        self.ws = ws
        self.active_thread_id: str | None = None
        self.visible: bool = True


class PwaGatewayAdapter:
    """Embedded FastAPI + WebSocket + Web Push mobile gateway implementing `FoodChannelAdapter`."""

    def __init__(
        self,
        *,
        settings: ControllerSettings,
        state: StateStore,
        push_manager: WebPushManager,
        direct_chat: DirectChatService,
        auth: PwaAuthManager | None = None,
        on_open_draft_pr: ApproveCallback | None = None,
        on_push_to_pr_branch: PushBranchCallback | None = None,
        on_resolve: ResolveCallback | None = None,
        on_ignore: IgnoreCallback | None = None,
        on_reject: IgnoreCallback | None = None,
        on_thread_feedback: FeedbackCallback | None = None,
        on_status_query: StatusCallback | None = None,
        on_k8s_query: StatusCallback | None = None,
        on_ci_query: StatusCallback | None = None,
        on_deps_trigger: DepsCallback | None = None,
        on_coding_request: CodingCallback | None = None,
        on_billing_command: BillingCommandCallback | None = None,
        on_food_message: FoodMessageCallback | None = None,
        on_food_thread_reply: FoodThreadReplyCallback | None = None,
        on_food_scale: FoodScaleCallback | None = None,
        on_food_save_staple: FoodEntryActionCallback | None = None,
        on_food_delete: FoodEntryActionCallback | None = None,
        on_save_memory_note: SaveMemoryCallback | None = None,
        on_snooze: SnoozeCallback | None = None,
        on_timers_command: TimersCommandCallback | None = None,
        on_memory_command: MemoryCommandCallback | None = None,
    ) -> None:
        self.settings = settings
        self.state = state
        self.push_manager = push_manager
        self.direct_chat = direct_chat
        self.auth = auth or PwaAuthManager(settings=settings, state=state)

        self.on_open_draft_pr = on_open_draft_pr
        self.on_push_to_pr_branch = on_push_to_pr_branch
        self.on_resolve = on_resolve
        self.on_ignore = on_ignore
        self.on_reject = on_reject
        self.on_thread_feedback = on_thread_feedback
        self.on_status_query = on_status_query
        self.on_k8s_query = on_k8s_query
        self.on_ci_query = on_ci_query
        self.on_deps_trigger = on_deps_trigger
        self.on_coding_request = on_coding_request
        self.on_billing_command = on_billing_command
        self.on_food_message = on_food_message
        self.on_food_thread_reply = on_food_thread_reply
        self.on_food_scale = on_food_scale
        self.on_food_save_staple = on_food_save_staple
        self.on_food_delete = on_food_delete
        self.on_save_memory_note = on_save_memory_note
        self.on_snooze = on_snooze
        self.on_timers_command = on_timers_command
        self.on_memory_command = on_memory_command

        # Optional sibling references wired once all tasks are constructed in controller_main.
        self.slack_adapter: SlackSocketAdapter | None = None
        self.food_task: FoodTrackerTask | None = None
        self.billing_task: BillingMonitorTask | None = None
        self.plugin_registry: Any | None = None

        self._started_at = time.monotonic()
        self._ws_clients: list[_WsClientState] = []
        self._background_tasks: set[asyncio.Task[Any]] = set()
        self._food_log_sync_timeout_s: float = 15.0
        self._pending_food_logs: dict[str, PendingFoodLog] = {}

    def _track_bg(self, coro: Any) -> asyncio.Task[Any]:
        task = asyncio.create_task(coro)
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)
        return task

    async def start(self) -> None:
        log.info(
            "pwa_gateway.ready",
            base_path=self.settings.pwa_base_path,
            port=self.settings.pwa_port,
        )

    async def stop(self) -> None:
        for client in list(self._ws_clients):
            try:
                await client.ws.close(code=1001)
            except (OSError, RuntimeError):
                log.debug("pwa.ws_close_ignored")
        self._ws_clients.clear()
        for task in list(self._background_tasks):
            task.cancel()
        if self._background_tasks:
            await asyncio.gather(*self._background_tasks, return_exceptions=True)

    def is_explicit_role_channel(self, role: str, channel_id: str) -> bool:
        return bool(role and channel_id == f"pwa:{role}")

    def resolve_channel(self, role: str) -> str:
        return f"pwa:{role}" if role else "pwa:ops"

    def resolve_channel_for_task(self, task_kind: str) -> str:
        mapped = TASK_KIND_TO_PWA_KIND.get(task_kind, "ops")
        return f"pwa:{mapped}"

    def _is_thread_viewed_foreground(self, thread_id: str) -> bool:
        return any(c.visible and c.active_thread_id == thread_id for c in self._ws_clients)

    async def broadcast(self, event_type: str, data: dict[str, Any]) -> None:
        if not self._ws_clients:
            return
        payload = json.dumps({"type": event_type, "data": data})
        dead: list[_WsClientState] = []
        for client in list(self._ws_clients):
            try:
                await client.ws.send_text(payload)
            except (OSError, RuntimeError, WebSocketDisconnect):
                dead.append(client)
        for client in dead:
            if client in self._ws_clients:
                self._ws_clients.remove(client)

    def _serialize_thread(self, t: PwaThreadRecord) -> dict[str, Any]:
        return asdict(t)

    def _serialize_message(self, m: PwaMessageRecord) -> dict[str, Any]:
        return asdict(m)

    def _load_targets(self) -> TargetsConfig:
        if self.settings.targets_config_path.exists():
            return TargetsConfig.from_yaml(self.settings.targets_config_path)
        return TargetsConfig()

    def _sync_issue_thread_metadata(self, issue_id: str) -> IssueRecord | None:
        rec = self.state.get_by_issue_id(issue_id)
        if rec is None:
            return None
        pwa_kind = TASK_KIND_TO_PWA_KIND.get(rec.task_kind, "coding")
        subtitle = f"{rec.github_repo} • turn {rec.turn}"
        if rec.pr_url:
            subtitle += " • PR"
        self.state.upsert_pwa_thread(
            thread_id=rec.issue_id,
            thread_ts=rec.thread_ts,
            kind=pwa_kind,
            title=f"{rec.short_id}: {rec.title}",
            subtitle=subtitle,
            status=rec.status.value,
            model=rec.model_override or self.settings.worker_model,
            conversation_id=rec.conversation_id,
            issue_id=rec.issue_id,
            unarchive_on_activity=False,
        )
        return rec

    def _resolve_handoff_record(self, handoff_id: str, *, repo: str = "") -> IssueRecord | None:
        clean = handoff_id.strip()
        if not clean or clean.lower() == "latest":
            candidates = self.state.list_recent_issues(
                task_kind="slack_coding",
                github_repo=repo,
                short_id_prefix="HANDOFF-",
                limit=1,
            )
            if candidates:
                return candidates[0]
            fallback = self.state.list_recent_issues(
                task_kind="slack_coding",
                github_repo=repo,
                limit=1,
            )
            return fallback[0] if fallback else None

        by_issue = self.state.get_by_issue_id(clean)
        if by_issue is not None:
            return by_issue

        by_short = self.state.get_by_short_id(clean)
        if by_short is not None:
            return by_short

        by_ts = self.state.get_by_thread_ts(clean)
        if by_ts is not None:
            return by_ts

        thread = self.state.get_pwa_thread(clean)
        if thread is not None and thread.issue_id:
            return self.state.get_by_issue_id(thread.issue_id)
        return None

    def _build_handoff_payload(
        self, record: IssueRecord, *, include_diff: bool = True
    ) -> dict[str, Any]:
        art_dir = self.settings.state_dir / "artifacts" / record.issue_id
        patch_path = art_dir / "fix.patch"
        init_patch_path = art_dir / "initial.patch"
        envelope_path = art_dir / "result.json"
        meta_path = art_dir / "handoff_meta.json"
        base_sha_path = art_dir / "base_sha.txt"

        diff_text = patch_path.read_text(encoding="utf-8") if patch_path.is_file() else ""
        has_initial_patch = init_patch_path.is_file() and bool(
            init_patch_path.read_text(encoding="utf-8").strip()
        )
        base_sha = (
            base_sha_path.read_text(encoding="utf-8").strip() if base_sha_path.is_file() else ""
        )
        meta: dict[str, Any] = {}
        if meta_path.is_file():
            try:
                raw_meta = json.loads(meta_path.read_text(encoding="utf-8"))
                if isinstance(raw_meta, dict):
                    meta = raw_meta
            except (OSError, ValueError):
                meta = {}
        if not base_sha and meta.get("base_sha"):
            base_sha = str(meta["base_sha"]).strip()

        report_dict: dict[str, Any] | None = None
        usage_dict: dict[str, Any] | None = None
        patch_stats = ""
        stop_reason = ""
        envelope_error = ""
        envelope_model = ""
        if envelope_path.is_file():
            try:
                env = WorkerEnvelope.model_validate_json(envelope_path.read_text(encoding="utf-8"))
                envelope_model = env.model
                stop_reason = env.stop_reason
                patch_stats = env.patch_stats
                envelope_error = env.error or ""
                if env.report is not None:
                    report_dict = env.report.model_dump()
                usage_dict = env.usage.model_dump()
            except (OSError, ValueError):
                log.debug("pwa.handoff_invalid_envelope", issue_id=record.issue_id)

        is_terminal = record.status not in {IssueStatus.QUEUED, IssueStatus.RUNNING}
        model_used = (
            envelope_model
            or record.model_override
            or str(meta.get("model") or "").strip()
            or self.settings.worker_model
        )
        payload: dict[str, Any] = {
            "issue_id": record.issue_id,
            "handoff_id": record.issue_id,
            "short_id": record.short_id,
            "thread_id": record.issue_id,
            "thread_ts": record.thread_ts,
            "github_repo": record.github_repo,
            "branch": record.base_ref or record.default_branch,
            "base_sha": base_sha,
            "title": record.title,
            "status": record.status.value,
            "is_terminal": is_terminal,
            "turn": record.turn,
            "model": model_used,
            "conversation_id": record.conversation_id,
            "source_conversation_id": str(meta.get("source_conversation_id") or ""),
            "pr_url": record.pr_url,
            "last_error": record.last_error or envelope_error or None,
            "updated_at": record.updated_at,
            "had_base_patch": bool(meta.get("had_base_patch") or has_initial_patch),
            "has_repo_snapshot": bool(
                meta.get("has_repo_snapshot") or (art_dir / "repo_snapshot.tar.gz").is_file()
            ),
            "has_patch": bool(diff_text.strip()),
            "patch_stats": patch_stats,
            "stop_reason": stop_reason,
            "affected_files": record.affected_files,
            "report": report_dict,
            "usage": usage_dict,
            "idempotency_key": str(meta.get("idempotency_key") or ""),
            "timeout_seconds": meta.get("timeout_seconds"),
            "context_files": list(meta.get("context_files") or []),
        }
        if include_diff:
            payload["diff"] = diff_text
            payload["patch"] = diff_text
        return payload

    def _ensure_thread_for_ts(
        self, thread_ts: str, *, channel_id: str | None = None
    ) -> PwaThreadRecord:
        existing = self.state.get_pwa_thread_by_ts(thread_ts)
        if existing is not None:
            return existing

        issue_rec = self.state.get_by_thread_ts(thread_ts)
        if issue_rec is not None:
            pwa_kind = TASK_KIND_TO_PWA_KIND.get(issue_rec.task_kind, "coding")
            return self.state.upsert_pwa_thread(
                thread_id=issue_rec.issue_id,
                thread_ts=thread_ts,
                kind=pwa_kind,
                title=f"{issue_rec.short_id}: {issue_rec.title}",
                subtitle=f"{issue_rec.github_repo} • turn {issue_rec.turn}",
                status=issue_rec.status.value,
                model=issue_rec.model_override or self.settings.worker_model,
                conversation_id=issue_rec.conversation_id,
                issue_id=issue_rec.issue_id,
            )

        if self.food_task is not None:
            meal = self.food_task.nutrition.get_meal_by_thread_ts(thread_ts)
            if meal is not None:
                return self.state.upsert_pwa_thread(
                    thread_id=f"food-{meal.id}",
                    thread_ts=thread_ts,
                    kind="food",
                    title=f"#{meal.id} — {meal.title}",
                    subtitle=f"{meal.meal_slot} • {meal.local_date}",
                    status="active",
                    model=meal.model_used,
                    food_entry_id=meal.id,
                )

        if thread_ts.startswith("pwa:"):
            tid = thread_ts[4:]
            found = self.state.get_pwa_thread(tid)
            if found is not None:
                return found

        kind = "ops"
        if channel_id and channel_id.startswith("pwa:"):
            kind = channel_id[4:] or "ops"
        slug = hashlib.sha1(thread_ts.encode("utf-8")).hexdigest()[:10]
        return self.state.upsert_pwa_thread(
            thread_id=f"thread-{slug}",
            thread_ts=thread_ts,
            kind=kind,
            title=ROLE_TO_FEED_TITLE.get(kind, f"Thread {slug}"),
            subtitle=channel_id or "",
            status="active",
        )

    # ------------------------------------------------------------------
    # FoodChannelAdapter implementation
    # ------------------------------------------------------------------

    async def post_channel_message(
        self,
        text: str,
        *,
        role: str = "",
        channel_id: str | None = None,
        external_ts: str | None = None,
    ) -> str:
        kind = role.strip().lower() or (
            channel_id[4:].strip().lower()
            if channel_id and channel_id.startswith("pwa:")
            else "ops"
        )
        if kind not in {"sentry", "ci", "deps", "k8s", "coding", "billing", "food", "chat", "ops"}:
            kind = "ops"

        ts = external_ts or _synth_ts(f"feed-{kind}")
        thread_id = f"feed-{kind}"
        lower = text.lower()
        is_urgent = any(marker in lower for marker in _URGENT_MARKERS) or kind in {
            "billing",
            "k8s",
            "ci",
        }
        viewing = self._is_thread_viewed_foreground(thread_id)

        thread = self.state.upsert_pwa_thread(
            thread_id=thread_id,
            thread_ts=f"pwa:{thread_id}",
            kind=kind,
            title=ROLE_TO_FEED_TITLE.get(kind, f"{kind.title()} Feed"),
            subtitle=f"#{kind} automation stream",
            status="active",
            last_message_preview=text,
            increment_unread=False,
            unarchive_on_activity=is_urgent,
        )
        msg = self.state.append_pwa_message(
            thread_id=thread.thread_id,
            sender="agent",
            body_md=text,
            external_ts=ts,
            payload={"role": kind, "urgent": is_urgent},
            increment_unread=is_urgent and not viewing,
            unarchive_on_activity=is_urgent,
        )
        fresh = self.state.get_pwa_thread(thread.thread_id) or thread
        await self.broadcast(
            "message.created",
            {"thread": self._serialize_thread(fresh), "message": self._serialize_message(msg)},
        )
        if is_urgent and not viewing:
            first_line = next(
                (ln.strip("*_ `:") for ln in text.splitlines() if ln.strip()), thread.title
            )
            await self.push_manager.notify(
                title=thread.title,
                body=first_line,
                kind=kind,
                thread_id=thread.thread_id,
            )
        return ts

    async def post_triage_result(
        self,
        record: IssueRecord,
        report: TriageReport,
        diff_text: str,
        *,
        model_used: str = "",
        cost_line: str = "",
        external_ts: str | None = None,
    ) -> str:
        pwa_kind = TASK_KIND_TO_PWA_KIND.get(record.task_kind, "coding")
        model_label = model_used or record.model_override or self.settings.worker_model
        tests_badge = "Tests PASSED" if report.tests_passed else "Needs Verification"
        has_diff = bool(diff_text.strip())
        canonical_ts = external_ts or record.thread_ts or _synth_ts("triage")
        viewing = self._is_thread_viewed_foreground(record.issue_id)

        summary_heading = "Root Cause" if record.task_kind == "sentry" else "Analysis"
        fix_heading = "Proposed Fix" if has_diff else "Summary"
        files_str = ", ".join(f"`{f}`" for f in report.affected_files) or "_none_"
        body_md = (
            f"**{record.short_id}** — `{record.project_slug}` (`{model_label}` • {tests_badge})\n\n"
            f"**{summary_heading}:**\n{report.root_cause}\n\n"
            f"**{fix_heading} ({files_str}):**\n{report.fix_summary}"
        )
        if report.risk_notes:
            body_md += f"\n\n**Risk / Notes:** {report.risk_notes}"
        if report.proposed_memory_note and report.proposed_memory_note.strip():
            body_md += f"\n\n🧠 **Proposed Repo Memory:** _{report.proposed_memory_note.strip()}_"

        actions: list[dict[str, str]] = []
        if has_diff and record.target_branch and self.on_push_to_pr_branch is not None:
            actions.append(
                {
                    "id": "push_to_pr_branch",
                    "label": f"Push Fix to {record.target_branch}",
                    "style": "primary",
                }
            )
        if has_diff:
            actions.append(
                {
                    "id": "open_draft_pr",
                    "label": "Open Draft PR",
                    "style": "secondary" if record.target_branch else "primary",
                }
            )
        if record.task_kind == "sentry":
            actions.append(
                {
                    "id": "resolve",
                    "label": "Resolve in Sentry",
                    "style": "secondary" if has_diff else "primary",
                }
            )
            actions.append({"id": "ignore", "label": "Ignore", "style": "danger"})
        else:
            actions.append({"id": "reject", "label": "Reject", "style": "danger"})
        if (
            report.proposed_memory_note
            and report.proposed_memory_note.strip()
            and self.on_save_memory_note is not None
        ):
            actions.append(
                {
                    "id": "save_memory_note",
                    "label": "Save Repo Memory",
                    "style": "secondary",
                }
            )

        thread = self.state.upsert_pwa_thread(
            thread_id=record.issue_id,
            thread_ts=canonical_ts,
            kind=pwa_kind,
            title=f"{record.short_id}: {record.title}",
            subtitle=f"{record.github_repo} • turn {record.turn} • {tests_badge}",
            status="awaiting_review" if record.status.value == "running" else record.status.value,
            model=model_label,
            conversation_id=record.conversation_id,
            issue_id=record.issue_id,
            last_message_preview=report.fix_summary or report.root_cause,
            increment_unread=False,
            unarchive_on_activity=True,
        )
        payload = {
            "report": report.model_dump(),
            "diff_preview": diff_text[:12000],
            "has_full_patch": has_diff,
            "patch_len": len(diff_text),
            "cost_line": cost_line,
            "model": model_label,
            "permalink": record.permalink,
            "target_branch": record.target_branch,
            "pr_url": record.pr_url,
            "actions": actions,
        }
        msg = self.state.append_pwa_message(
            thread_id=thread.thread_id,
            sender="agent",
            body_md=body_md,
            external_ts=canonical_ts,
            payload=payload,
            increment_unread=not viewing,
            unarchive_on_activity=True,
        )
        fresh = self.state.get_pwa_thread(thread.thread_id) or thread
        await self.broadcast(
            "message.created",
            {"thread": self._serialize_thread(fresh), "message": self._serialize_message(msg)},
        )
        await self.broadcast("thread.updated", {"thread": self._serialize_thread(fresh)})
        if not viewing:
            await self.push_manager.notify(
                title=f"{record.short_id} ({tests_badge})",
                body=f"{record.title} — {report.fix_summary[:140]}",
                kind=pwa_kind,
                thread_id=thread.thread_id,
            )
        return canonical_ts

    async def post_thread_message(
        self,
        thread_ts: str,
        text: str,
        *,
        channel_id: str | None = None,
        external_ts: str | None = None,
    ) -> None:
        thread = self._ensure_thread_for_ts(thread_ts, channel_id=channel_id)
        if thread.issue_id:
            self._sync_issue_thread_metadata(thread.issue_id)
        viewing = self._is_thread_viewed_foreground(thread.thread_id)
        msg_ts = external_ts or _synth_ts("msg")
        msg = self.state.append_pwa_message(
            thread_id=thread.thread_id,
            sender="agent",
            body_md=text,
            external_ts=msg_ts,
            increment_unread=not viewing,
            unarchive_on_activity=True,
        )
        fresh = self.state.get_pwa_thread(thread.thread_id) or thread
        await self.broadcast(
            "message.created",
            {"thread": self._serialize_thread(fresh), "message": self._serialize_message(msg)},
        )
        lower = text.lower()
        if not viewing and any(
            kw in lower for kw in ("draft pr opened", "pushed commit", "failed", "error")
        ):
            await self.push_manager.notify(
                title=fresh.title,
                body=text,
                kind=fresh.kind,
                thread_id=fresh.thread_id,
            )

    async def post_blocks_message(
        self,
        *,
        channel_id: str,
        text: str,
        blocks: list[dict[str, Any]],
        thread_ts: str | None = None,
        external_ts: str | None = None,
    ) -> str:
        body_md, actions, food_entry_id = _blocks_to_markdown(text, blocks)
        ts = external_ts or _synth_ts("blk")

        if food_entry_id is not None:
            meal = (
                self.food_task.nutrition.get_meal_entry(food_entry_id) if self.food_task else None
            )
            tid = f"food-{food_entry_id}"
            title = f"#{food_entry_id} — {meal.title if meal else text[:60]}"
            subtitle = (
                f"{meal.meal_slot} • {meal.local_date} • {meal.total_calories_kcal:.0f} kcal"
                if meal
                else "Food Tracker"
            )
            thread = self.state.upsert_pwa_thread(
                thread_id=tid,
                thread_ts=thread_ts or ts,
                kind="food",
                title=title,
                subtitle=subtitle,
                status="active",
                food_entry_id=food_entry_id,
                last_message_preview=body_md,
            )
        elif thread_ts:
            thread = self._ensure_thread_for_ts(thread_ts, channel_id=channel_id)
        else:
            kind = "food" if (channel_id == "pwa:food" or "meal" in text.lower()) else "ops"
            tid = f"feed-{kind}"
            thread = self.state.upsert_pwa_thread(
                thread_id=tid,
                thread_ts=f"pwa:{tid}",
                kind=kind,
                title=ROLE_TO_FEED_TITLE.get(kind, "Feed"),
                status="active",
                last_message_preview=body_md,
            )

        viewing = self._is_thread_viewed_foreground(thread.thread_id)
        msg = self.state.append_pwa_message(
            thread_id=thread.thread_id,
            sender="agent",
            body_md=body_md,
            external_ts=ts,
            payload={"actions": actions, "food_entry_id": food_entry_id},
            increment_unread=not viewing and thread.kind != "food",
        )
        fresh = self.state.get_pwa_thread(thread.thread_id) or thread
        await self.broadcast(
            "message.created",
            {"thread": self._serialize_thread(fresh), "message": self._serialize_message(msg)},
        )
        if food_entry_id is not None or thread.kind == "food":
            await self.broadcast("food.updated", {"entry_id": food_entry_id})
        return ts

    async def update_blocks_message(
        self,
        *,
        channel_id: str,
        ts: str,
        text: str,
        blocks: list[dict[str, Any]],
    ) -> None:
        body_md, actions, food_entry_id = _blocks_to_markdown(text, blocks)
        updated = self.state.update_pwa_message_by_ts(
            external_ts=ts,
            body_md=body_md,
            payload={"actions": actions, "food_entry_id": food_entry_id},
        )
        if updated is not None:
            if food_entry_id is not None and self.food_task is not None:
                meal = self.food_task.nutrition.get_meal_entry(food_entry_id)
                if meal is not None:
                    self.state.upsert_pwa_thread(
                        thread_id=updated.thread_id,
                        kind="food",
                        title=f"#{meal.id} — {meal.title}",
                        subtitle=f"{meal.meal_slot} • {meal.local_date} • {meal.total_calories_kcal:.0f} kcal",
                        status="resolved" if meal.status == "deleted" else "active",
                        last_message_preview=body_md,
                        unarchive_on_activity=False,
                    )
            thread = self.state.get_pwa_thread(updated.thread_id)
            await self.broadcast(
                "message.updated",
                {
                    "thread": self._serialize_thread(thread) if thread else None,
                    "message": self._serialize_message(updated),
                },
            )
        if food_entry_id is not None:
            await self.broadcast("food.updated", {"entry_id": food_entry_id})

    async def add_reaction(self, *, channel_id: str, ts: str, name: str) -> None:
        msg = self.state.add_or_remove_pwa_reaction(external_ts=ts, name=name, remove=False)
        if msg is not None:
            await self.broadcast("message.updated", {"message": self._serialize_message(msg)})

    async def remove_reaction(self, *, channel_id: str, ts: str, name: str) -> None:
        msg = self.state.add_or_remove_pwa_reaction(external_ts=ts, name=name, remove=True)
        if msg is not None:
            await self.broadcast("message.updated", {"message": self._serialize_message(msg)})

    # ------------------------------------------------------------------
    # Helpers for FastAPI routes
    # ------------------------------------------------------------------

    def _serialize_meal(self, meal: Any) -> dict[str, Any]:
        base = self.settings.pwa_base_path.rstrip("/") or ""
        data = asdict(meal)
        if meal.photo_relpath:
            data["photo_url"] = f"{base}/api/media/{meal.photo_relpath}"
        else:
            data["photo_url"] = None
        return data

    def _build_food_today_payload(self, local_date: str | None = None) -> dict[str, Any]:
        if self.food_task is None:
            raise HTTPException(status_code=503, detail="Food tracker is not enabled.")
        cfg = self._load_targets().food_tracker
        target_date = (local_date or "").strip() or self.food_task._today_local_date(cfg)
        summary = self.food_task.nutrition.get_daily_summary(target_date, cfg)
        staples = self.food_task.nutrition.list_saved_foods()
        latest_w = self.food_task.nutrition.get_latest_weight()
        tdee = self.food_task.nutrition.estimate_adaptive_tdee(target_date)
        rt_settings = self.food_task.nutrition.get_garmin_runtime_settings(cfg)
        readiness_warning: str | None = None
        if summary.garmin is not None and summary.dynamic_target is not None:
            g = summary.garmin
            delta_kcal = summary.dynamic_target.goal_delta_kcal
            low_sleep = g.sleep_seconds is not None and g.sleep_seconds < 6.5 * 3600
            poor_hrv = (g.hrv_status or "").upper() in {"LOW", "POOR", "UNBALANCED"}
            if delta_kcal <= -400 and (low_sleep or poor_hrv):
                reasons: list[str] = []
                if low_sleep and g.sleep_seconds is not None:
                    reasons.append(f"short sleep ({g.sleep_seconds / 3600:.1f}h)")
                if poor_hrv:
                    reasons.append(f"HRV {g.hrv_status.lower()}")
                readiness_warning = (
                    f"Recovery caution ({', '.join(reasons)}) while running a "
                    f"{delta_kcal:+d} kcal deficit — consider easing to -250 or maintenance."
                )
        return {
            "local_date": summary.local_date,
            "target": asdict(summary.target),
            "total_calories_kcal": summary.total_calories_kcal,
            "total_protein_g": summary.total_protein_g,
            "total_carbs_g": summary.total_carbs_g,
            "total_fat_g": summary.total_fat_g,
            "total_fiber_g": summary.total_fiber_g,
            "remaining_calories_kcal": summary.remaining_calories_kcal,
            "remaining_protein_g": summary.remaining_protein_g,
            "remaining_carbs_g": summary.remaining_carbs_g,
            "remaining_fat_g": summary.remaining_fat_g,
            "remaining_fiber_g": summary.remaining_fiber_g,
            "meals": [self._serialize_meal(m) for m in summary.meals],
            "saved_foods": [asdict(sf) for sf in staples],
            "latest_weight": asdict(latest_w) if latest_w else None,
            "tdee": asdict(tdee) if tdee else None,
            "garmin": asdict(summary.garmin) if summary.garmin else None,
            "dynamic_target": asdict(summary.dynamic_target) if summary.dynamic_target else None,
            "garmin_configured": self.food_task.garmin is not None,
            "garmin_settings": rt_settings,
            "readiness_warning": readiness_warning,
            "pending_logs": [p.model_dump() for p in self._pending_food_logs.values()],
        }

    async def _run_direct_chat_turn_bg(
        self,
        *,
        thread_id: str,
        prompt: str,
        model: str,
        image_paths: list[Path],
    ) -> None:
        thread, msg, _cost = await self.direct_chat.run_reply_turn(
            thread_id=thread_id,
            prompt=prompt,
            model=model,
            image_paths=image_paths,
        )
        viewing = self._is_thread_viewed_foreground(thread_id)
        if not viewing and msg.sender == "agent":
            fresh = self.state.update_pwa_thread_org(thread_id, mark_unread=True) or thread
        else:
            fresh = thread

        await self.broadcast(
            "message.created",
            {"thread": self._serialize_thread(fresh), "message": self._serialize_message(msg)},
        )
        await self.broadcast("thread.updated", {"thread": self._serialize_thread(fresh)})
        if not viewing:
            await self.push_manager.notify(
                title=fresh.title,
                body=msg.body_md,
                kind="chat",
                thread_id=fresh.thread_id,
            )

    def _build_ops_overview_sync(self) -> dict[str, Any]:
        targets = self._load_targets()
        billing_cfg = targets.billing
        now = datetime.now(UTC)
        cycle = compute_billing_cycle(now, billing_cfg.cycle_anchor_day)
        usage_summary = self.state.get_cycle_usage_summary(cycle.cycle_key)
        spent = float(usage_summary["spent_usd"])
        credit = float(billing_cfg.monthly_credit_usd)
        pct = round((spent / credit) * 100.0, 2) if credit > 0 else 0.0

        daily_burn: float | None = None
        projected_total: float | None = None
        exhaustion_str: str | None = None
        paused = False
        pause_reason = ""
        if self.billing_task is not None:
            daily_burn, projected_total, exhaustion_str = self.billing_task._compute_projection(
                summary=usage_summary,
                cycle=cycle,
                cfg=billing_cfg,
            )
            paused, pause_reason = self.billing_task.is_background_paused(now=now, targets=targets)

        return {
            "uptime_seconds": int(time.monotonic() - self._started_at),
            "worker_model": self.settings.worker_model,
            "node_name": self.settings.node_name or "local",
            "namespace": self.settings.namespace,
            "slack_enabled": self.slack_adapter is not None,
            "status_counts": self.state.status_counts(),
            "pwa_counts": self.state.get_pwa_counts(),
            "billing": {
                "enabled": billing_cfg.enabled,
                "cycle_key": cycle.cycle_key,
                "cycle_start": cycle.start_utc.strftime("%Y-%m-%d"),
                "cycle_end": cycle.end_utc.strftime("%Y-%m-%d"),
                "elapsed_days": round(cycle.elapsed_days, 1),
                "remaining_days": round(cycle.remaining_days, 1),
                "spent_usd": round(spent, 4),
                "credit_usd": round(credit, 2),
                "remaining_usd": round(max(0.0, credit - spent), 4),
                "pct": pct,
                "daily_burn_usd": round(daily_burn, 4) if daily_burn is not None else None,
                "projected_usd": round(projected_total, 2) if projected_total is not None else None,
                "exhaustion_str": exhaustion_str,
                "paused": paused,
                "pause_reason": pause_reason,
                "hard_stop_pct": billing_cfg.hard_stop_pct,
                "job_count": int(usage_summary["job_count"]),
                "prompt_tokens": int(usage_summary["prompt_tokens"]),
                "cached_tokens": int(usage_summary["cached_tokens"]),
                "output_tokens": int(usage_summary["output_tokens"]),
                "thoughts_tokens": int(usage_summary["thoughts_tokens"]),
                "by_task_kind": usage_summary["by_task_kind"],
                "by_model": usage_summary["by_model"],
            },
        }


def create_pwa_app(gateway: PwaGatewayAdapter) -> FastAPI:
    """Build the FastAPI ASGI application serving the iOS PWA shells, REST API, and WebSocket."""
    base_path = gateway.settings.pwa_base_path.rstrip("/") or ""
    health_base_path = gateway.settings.pwa_health_base_path.rstrip("/") or ""
    app = FastAPI(title="AGY Tasks Mobile Gateway", docs_url=None, redoc_url=None, openapi_url=None)
    router = APIRouter()

    async def _require_auth(request: Request) -> None:
        gateway.auth.require_http_auth(request)

    if base_path:

        @app.get("/", include_in_schema=False)
        async def root_redirect() -> RedirectResponse:
            return RedirectResponse(url=f"{base_path}/")

    def _mount_pwa_shell(
        prefix: str,
        *,
        mode: str,
        title: str,
        short_name: str,
        description: str,
        view_script_url: str = "",
    ) -> None:
        shell_router = APIRouter(prefix=prefix)
        scope = f"{prefix}/"

        if prefix:

            @app.get(prefix, include_in_schema=False)
            async def prefix_slash_redirect() -> RedirectResponse:
                return RedirectResponse(url=scope)

        @shell_router.get("/manifest.webmanifest")
        async def manifest() -> JSONResponse:
            body = {
                "name": title,
                "short_name": short_name,
                "description": description,
                "start_url": scope,
                "scope": scope,
                "display": "standalone",
                "orientation": "portrait",
                "background_color": "#0b1020",
                "theme_color": "#0b1020",
                "icons": [
                    {
                        "src": f"{prefix}/icons/icon-180.png",
                        "sizes": "180x180",
                        "type": "image/png",
                    },
                    {
                        "src": f"{prefix}/icons/icon-192.png",
                        "sizes": "192x192",
                        "type": "image/png",
                        "purpose": "any maskable",
                    },
                    {
                        "src": f"{prefix}/icons/icon-512.png",
                        "sizes": "512x512",
                        "type": "image/png",
                        "purpose": "any maskable",
                    },
                ],
            }
            return JSONResponse(body, media_type="application/manifest+json")

        @shell_router.get("/icons/icon-{size}.png")
        async def pwa_icon(size: int) -> Response:
            if size not in {180, 192, 512}:
                raise HTTPException(status_code=404, detail="Unsupported icon size.")
            return Response(
                content=render_pwa_icon_png(size, variant=mode),
                media_type="image/png",
                headers={"Cache-Control": "public, max-age=86400"},
            )

        @shell_router.get("/sw.js")
        async def service_worker() -> Response:
            sw_path = STATIC_DIR / "sw.js"
            if not sw_path.is_file():
                raise HTTPException(status_code=404, detail="sw.js not found")
            return Response(
                content=sw_path.read_text(encoding="utf-8"),
                media_type="application/javascript",
                headers={
                    "Service-Worker-Allowed": scope,
                    "Cache-Control": "no-cache, no-store, must-revalidate",
                },
            )

        @shell_router.get("/")
        async def index_page() -> HTMLResponse:
            html_path = STATIC_DIR / "index.html"
            html = html_path.read_text(encoding="utf-8")
            meta_tags = (
                f'    <meta name="agy-app-mode" content="{mode}" />\n'
                f'    <meta name="agy-chat-base" content="{base_path}" />\n'
                f'    <meta name="agy-view-script" content="{view_script_url}" />\n'
            )
            html = html.replace("<title>AGY Tasks</title>", f"<title>{title}</title>\n{meta_tags}")
            html = html.replace(
                '<meta name="apple-mobile-web-app-title" content="AGY Tasks" />',
                f'<meta name="apple-mobile-web-app-title" content="{title}" />',
            )
            return HTMLResponse(content=html, headers={"Cache-Control": "no-cache"})

        app.include_router(shell_router)

    @router.get("/healthz")
    async def healthz() -> dict[str, Any]:
        return {"ok": True, "uptime_s": int(time.monotonic() - gateway._started_at)}

    @router.get("/ca.crt")
    async def ca_cert() -> Response:
        ca_path = gateway.settings.pwa_ca_cert_path
        if not ca_path.is_file():
            raise HTTPException(
                status_code=404,
                detail="homelab-ca certificate file is not mounted on this controller.",
            )
        return Response(
            content=ca_path.read_bytes(),
            media_type="application/x-x509-ca-cert",
            headers={
                "Content-Disposition": 'attachment; filename="homelab-ca.crt"',
                "Cache-Control": "no-cache",
            },
        )

    @router.get("/static/{asset_path:path}")
    async def static_asset(asset_path: str) -> Response:
        root_resolved = STATIC_DIR.resolve()
        candidate = (root_resolved / asset_path).resolve()
        if not candidate.is_relative_to(root_resolved) or not candidate.is_file():
            raise HTTPException(status_code=404, detail="Asset not found")
        return FileResponse(candidate, headers={"Cache-Control": "no-cache"})

    @router.get("/api/auth/session")
    async def auth_session(request: Request) -> dict[str, Any]:
        return {
            "authenticated": gateway.auth.is_authenticated(request),
            "ca_available": gateway.settings.pwa_ca_cert_path.is_file(),
            "slack_enabled": gateway.slack_adapter is not None,
            "vapid_public_key": gateway.push_manager.public_key,
            "base_path": base_path or "/",
        }

    @router.post("/api/auth/login")
    async def auth_login(body: _LoginRequest, request: Request) -> JSONResponse:
        if not gateway.auth.verify_token(body.token):
            raise HTTPException(status_code=401, detail="Invalid access token.")
        cookie_val = gateway.auth.issue_session_cookie()
        resp = JSONResponse(
            {
                "ok": True,
                "vapid_public_key": gateway.push_manager.public_key,
            }
        )
        resp.set_cookie(
            key=SESSION_COOKIE_NAME,
            value=cookie_val,
            max_age=SESSION_MAX_AGE_SECONDS,
            httponly=True,
            secure=gateway.auth.is_https(request),
            samesite="strict",
            path=gateway.auth.cookie_path,
        )
        return resp

    @router.post("/api/auth/logout")
    async def auth_logout() -> JSONResponse:
        resp = JSONResponse({"ok": True})
        resp.delete_cookie(key=SESSION_COOKIE_NAME, path=gateway.auth.cookie_path)
        return resp

    # ------------------------------------------------------------------
    # Threads, Chat, Coding & Actions (Authenticated)
    # ------------------------------------------------------------------

    @router.get("/api/threads", dependencies=[Depends(_require_auth)])
    async def list_threads(
        folder: str = Query(default="inbox"),
        filter_mode: str = Query(default="all", alias="filter"),
        kind: str = Query(default=""),
        exclude_kinds: str = Query(default=""),
        q: str = Query(default=""),
    ) -> dict[str, Any]:
        excluded = tuple(k.strip() for k in exclude_kinds.split(",") if k.strip())
        threads = gateway.state.list_pwa_threads(
            folder=folder,
            filter_mode=filter_mode,
            kind=kind,
            exclude_kinds=excluded,
            query=q,
        )
        targets = gateway._load_targets()
        return {
            "threads": [gateway._serialize_thread(t) for t in threads],
            "counts": gateway.state.get_pwa_counts(exclude_kinds=excluded),
            "default_repo": targets.slack_coding.github_repo,
            "default_model": gateway.settings.worker_model,
        }

    @router.get("/api/threads/{thread_id}", dependencies=[Depends(_require_auth)])
    async def get_thread(thread_id: str) -> dict[str, Any]:
        thread = gateway.state.get_pwa_thread(thread_id)
        if thread is None:
            raise HTTPException(status_code=404, detail="Thread not found.")
        updated = gateway.state.update_pwa_thread_org(thread_id, mark_read=True) or thread
        issue_dict: dict[str, Any] | None = None
        if updated.issue_id:
            rec = gateway._sync_issue_thread_metadata(updated.issue_id)
            if rec is not None:
                updated = gateway.state.get_pwa_thread(thread_id) or updated
                issue_dict = {
                    "issue_id": rec.issue_id,
                    "short_id": rec.short_id,
                    "github_repo": rec.github_repo,
                    "status": rec.status.value,
                    "turn": rec.turn,
                    "pr_url": rec.pr_url,
                    "target_branch": rec.target_branch,
                    "permalink": rec.permalink,
                    "last_error": rec.last_error,
                }
        messages = gateway.state.list_pwa_messages(thread_id)
        return {
            "thread": gateway._serialize_thread(updated),
            "messages": [gateway._serialize_message(m) for m in messages],
            "issue": issue_dict,
            "counts": gateway.state.get_pwa_counts(),
        }

    @router.get("/api/threads/{thread_id}/patch", dependencies=[Depends(_require_auth)])
    async def get_thread_patch(thread_id: str) -> dict[str, Any]:
        thread = gateway.state.get_pwa_thread(thread_id)
        if thread is None:
            raise HTTPException(status_code=404, detail="Thread not found.")
        target_issue_id = thread.issue_id or thread.thread_id
        patch_path = gateway.settings.state_dir / "artifacts" / target_issue_id / "fix.patch"
        envelope_path = gateway.settings.state_dir / "artifacts" / target_issue_id / "result.json"
        diff_text = patch_path.read_text(encoding="utf-8") if patch_path.is_file() else ""
        report_dict: dict[str, Any] | None = None
        if envelope_path.is_file():
            try:
                env = WorkerEnvelope.model_validate_json(envelope_path.read_text(encoding="utf-8"))
                if env.report is not None:
                    report_dict = env.report.model_dump()
            except (OSError, ValueError):
                log.debug("pwa.invalid_envelope_ignored", issue_id=target_issue_id)
        return {
            "issue_id": target_issue_id,
            "has_patch": bool(diff_text.strip()),
            "diff": diff_text,
            "report": report_dict,
        }

    @router.patch("/api/threads/{thread_id}", dependencies=[Depends(_require_auth)])
    async def patch_thread(thread_id: str, body: _ThreadOrgPatch) -> dict[str, Any]:
        updated = gateway.state.update_pwa_thread_org(
            thread_id,
            folder=body.folder,
            pinned=body.pinned,
            mark_read=body.mark_read,
            mark_unread=body.mark_unread,
            tags=body.tags,
            title=body.title,
        )
        if updated is None:
            raise HTTPException(status_code=404, detail="Thread not found.")
        payload = {
            "thread": gateway._serialize_thread(updated),
            "counts": gateway.state.get_pwa_counts(),
        }
        await gateway.broadcast("thread.updated", payload)
        return payload

    @router.post("/api/chats", dependencies=[Depends(_require_auth)])
    async def create_chat(
        prompt: str = Form(default=""),
        model: str = Form(default=""),
        include_health_context: bool = Form(default=False),
        context_providers: Annotated[list[str] | None, Form()] = None,
        files: Annotated[list[UploadFile] | None, File()] = None,
    ) -> dict[str, Any]:
        uploads: list[tuple[str, bytes]] = []
        for f in (files or [])[:4]:
            raw = await f.read()
            if raw:
                uploads.append((f.filename or "image.jpg", raw))
        if not prompt.strip() and not uploads:
            raise HTTPException(status_code=400, detail="Provide a prompt or photo.")

        requested_providers = [p.strip() for p in (context_providers or []) if p and p.strip()]
        if include_health_context and "health" not in requested_providers:
            requested_providers.append("health")

        context_blocks: list[str] = []
        handled_providers: set[str] = set()
        if requested_providers and gateway.plugin_registry is not None:
            for provider_id in requested_providers:
                for loaded in gateway.plugin_registry.plugins.values():
                    if any(cp.id == provider_id for cp in loaded.manifest.chat_context_providers):
                        try:
                            block = await loaded.plugin.build_chat_context(
                                loaded.context, provider_id
                            )
                        except Exception as exc:
                            log.warning(
                                "pwa_chat_context_provider_failed",
                                provider_id=provider_id,
                                plugin_id=loaded.manifest.id,
                                error=str(exc),
                            )
                            block = ""
                        if block and block.strip():
                            context_blocks.append(block.strip())
                            handled_providers.add(provider_id)
                        break

        if (
            "health" in requested_providers
            and "health" not in handled_providers
            and gateway.food_task is not None
        ):
            cfg = gateway._load_targets().food_tracker
            today = gateway.food_task._today_local_date(cfg)
            health_ctx = gateway.food_task.nutrition.build_coach_context(today, cfg, days=7)
            if health_ctx and health_ctx.strip():
                context_blocks.append(health_ctx.strip())

        effective_prompt = (
            f"{'\n\n'.join(context_blocks)}\n\nUser question:\n{prompt.strip()}"
            if context_blocks
            else prompt
        )

        thread, user_msg, image_paths, cleaned_prompt, chosen_model = (
            gateway.direct_chat.create_chat_and_append_user_msg(
                prompt=effective_prompt,
                model=model,
                uploads=uploads,
            )
        )
        await gateway.broadcast(
            "message.created",
            {
                "thread": gateway._serialize_thread(thread),
                "message": gateway._serialize_message(user_msg),
            },
        )
        gateway._track_bg(
            gateway._run_direct_chat_turn_bg(
                thread_id=thread.thread_id,
                prompt=cleaned_prompt,
                model=chosen_model,
                image_paths=image_paths,
            )
        )
        return {
            "thread": gateway._serialize_thread(thread),
            "message": gateway._serialize_message(user_msg),
        }

    @router.post("/api/coding", dependencies=[Depends(_require_auth)])
    async def create_coding_task(body: _CodingRequest) -> dict[str, Any]:
        if gateway.on_coding_request is None:
            raise HTTPException(status_code=503, detail="Coding task handler is not enabled.")
        prompt = body.prompt.strip()
        if not prompt:
            raise HTTPException(status_code=400, detail="Coding prompt cannot be empty.")

        tokens: list[str] = []
        model_override, cleaned = parse_model_override(prompt)
        selected_model = model_override or (body.model.strip() if body.model.strip() else "")
        if selected_model:
            short_alias = selected_model.removeprefix("gemini-4-").removeprefix("gemini-3.8-")
            tokens.append(f"[{short_alias}]")
        if body.repo.strip() and not cleaned.lower().startswith("repo:"):
            tokens.append(f"repo:{body.repo.strip()}")
        tokens.append(cleaned or prompt)
        formatted_prompt = " ".join(tokens).strip()

        thread_ts = ""
        channel_id = "pwa:coding"
        if gateway.slack_adapter is not None and gateway.settings.pwa_mirror_to_slack:
            try:
                channel_id = gateway.slack_adapter.resolve_channel("coding")
                thread_ts = await gateway.slack_adapter.post_channel_message(
                    f"📱 *Mobile Coding Request:* `{formatted_prompt}`",
                    role="coding",
                    channel_id=channel_id,
                )
            except Exception as exc:  # noqa: BLE001
                log.warning("pwa.coding_slack_mirror_failed", error=str(exc))

        if not thread_ts:
            thread_ts = _synth_ts("coding")

        ack = await gateway.on_coding_request(formatted_prompt, "mobile", thread_ts, channel_id)
        issue_rec = gateway.state.get_by_thread_ts(thread_ts)
        tid = (
            issue_rec.issue_id
            if issue_rec
            else f"coding-{hashlib.sha1(thread_ts.encode()).hexdigest()[:10]}"
        )
        thread = gateway.state.upsert_pwa_thread(
            thread_id=tid,
            thread_ts=thread_ts,
            kind="coding",
            title=(f"{issue_rec.short_id}: {issue_rec.title}" if issue_rec else prompt[:90]),
            subtitle=(
                f"{issue_rec.github_repo} • turn {issue_rec.turn}"
                if issue_rec
                else body.repo.strip()
            ),
            status=issue_rec.status.value if issue_rec else "queued",
            model=(
                (issue_rec.model_override if issue_rec else None)
                or selected_model
                or gateway.settings.worker_model
            ),
            issue_id=issue_rec.issue_id if issue_rec else None,
            last_message_preview=ack,
        )
        gateway.state.append_pwa_message(
            thread_id=thread.thread_id,
            sender="user",
            body_md=prompt,
            payload={
                "model": thread.model,
                "repo": issue_rec.github_repo if issue_rec else body.repo,
            },
        )
        ack_msg = gateway.state.append_pwa_message(
            thread_id=thread.thread_id,
            sender="system",
            body_md=ack,
        )
        fresh = gateway.state.get_pwa_thread(thread.thread_id) or thread
        await gateway.broadcast(
            "message.created",
            {
                "thread": gateway._serialize_thread(fresh),
                "message": gateway._serialize_message(ack_msg),
            },
        )
        return {"thread": gateway._serialize_thread(fresh), "ack": ack}

    @router.post("/api/handoff", dependencies=[Depends(_require_auth)])
    async def create_handoff(
        body: _HandoffCreateRequest,
        request: Request,
    ) -> dict[str, Any]:
        if gateway.on_coding_request is None:
            raise HTTPException(status_code=503, detail="Coding task handler is not enabled.")
        prompt = body.prompt.strip()
        if not prompt:
            raise HTTPException(status_code=400, detail="Handoff prompt cannot be empty.")

        idem_key = (request.headers.get("Idempotency-Key") or body.idempotency_key or "").strip()
        if idem_key:
            recent = gateway.state.list_recent_issues(
                task_kind="slack_coding",
                short_id_prefix="HANDOFF-",
                limit=25,
            )
            for rec in recent:
                if rec.status not in {IssueStatus.QUEUED, IssueStatus.RUNNING}:
                    continue
                rec_meta_path = (
                    gateway.settings.state_dir / "artifacts" / rec.issue_id / "handoff_meta.json"
                )
                if not rec_meta_path.is_file():
                    continue
                try:
                    rec_meta = json.loads(rec_meta_path.read_text(encoding="utf-8"))
                except (OSError, ValueError):
                    continue
                if isinstance(rec_meta, dict) and rec_meta.get("idempotency_key") == idem_key:
                    dedup_result = gateway._build_handoff_payload(rec, include_diff=False)
                    dedup_result["ok"] = True
                    dedup_result["deduplicated"] = True
                    return dedup_result

        semantic_state = body.semantic_state.strip()
        if semantic_state and semantic_state not in prompt:
            prompt = f"{prompt}\n\n## Semantic State\n{semantic_state}"

        targets = gateway._load_targets()
        default_repo = targets.slack_coding.github_repo
        model_override, cleaned = parse_model_override(prompt)
        selected_model = model_override or body.model.strip()
        repo = body.repo.strip() or body.github_repo.strip() or default_repo
        branch = (
            body.branch.strip()
            or body.base_branch.strip()
            or (targets.slack_coding.default_branch if repo == default_repo else "main")
        )
        base_sha = body.base_sha.strip()
        base_patch = body.base_patch if body.base_patch.strip() else body.patch
        had_base_patch = bool(base_patch.strip())
        source_conversation_id = (
            body.source_conversation_id.strip() or (body.local_conversation_id or "").strip()
        )
        effective_timeout_seconds = (
            max(300, min(int(body.timeout_seconds), gateway.settings.handoff_max_deadline_seconds))
            if body.timeout_seconds is not None
            else gateway.settings.handoff_active_deadline_seconds
        )

        tokens: list[str] = []
        if model_override:
            short_alias = model_override.removeprefix("gemini-4-").removeprefix("gemini-3.8-")
            tokens.append(f"[{short_alias}]")
        if repo and not cleaned.lower().startswith("repo:"):
            tokens.append(f"repo:{repo}")
        tokens.append(cleaned or prompt)
        formatted_prompt = " ".join(tokens).strip()

        first_line = (
            body.title.strip()
            or next(
                (ln.strip("#* ") for ln in (cleaned or prompt).splitlines() if ln.strip()),
                "Session handoff",
            )
        )[:120]

        thread_ts = ""
        channel_id = "pwa:coding"
        if gateway.slack_adapter is not None and gateway.settings.pwa_mirror_to_slack:
            try:
                channel_id = gateway.slack_adapter.resolve_channel("coding")
                patch_badge = " (+ working-tree patch)" if had_base_patch else ""
                sha_badge = f" @ `{base_sha[:8]}`" if base_sha else ""
                thread_ts = await gateway.slack_adapter.post_channel_message(
                    f"💻 *Laptop Session Handoff:* `{repo}` (`{branch}`{sha_badge}{patch_badge}) — _{first_line}_",
                    role="coding",
                    channel_id=channel_id,
                )
            except Exception as exc:  # noqa: BLE001
                log.warning("pwa.handoff_slack_mirror_failed", error=str(exc))

        if not thread_ts:
            thread_ts = _synth_ts("handoff")

        issue_id = f"slack-{thread_ts.replace('.', '-')}"
        art_dir = gateway.settings.state_dir / "artifacts" / issue_id
        art_dir.mkdir(parents=True, exist_ok=True)
        (gateway.settings.state_dir / "sessions" / issue_id).mkdir(parents=True, exist_ok=True)

        has_repo_snapshot = False
        repo_snapshot_bytes = 0
        if body.repo_snapshot_b64.strip():
            try:
                repo_snapshot_bytes = validate_and_write_repo_snapshot(
                    body.repo_snapshot_b64,
                    art_dir / "repo_snapshot.tar.gz",
                )
            except RepoSnapshotTooLargeError as exc:
                raise HTTPException(status_code=413, detail=str(exc)) from exc
            except ValueError as exc:
                raise HTTPException(status_code=400, detail=str(exc)) from exc
            has_repo_snapshot = True

        materialized_context_files: list[str] = []
        if body.context_files:
            try:
                materialized_context_files = _materialize_context_files(art_dir, body.context_files)
            except _ContextFilesTooLargeError as exc:
                raise HTTPException(status_code=413, detail=str(exc)) from exc
            except ValueError as exc:
                raise HTTPException(status_code=400, detail=str(exc)) from exc

        meta_payload = {
            "is_handoff": True,
            "async_dispatch": True,
            "repo": repo,
            "branch": branch,
            "base_sha": base_sha,
            "had_base_patch": had_base_patch,
            "has_repo_snapshot": has_repo_snapshot,
            "repo_snapshot_bytes": repo_snapshot_bytes,
            "context_files": materialized_context_files,
            "timeout_seconds": effective_timeout_seconds,
            "idempotency_key": idem_key,
            "verify_hint": body.verify_hint.strip(),
            "focus_subdir": body.focus_subdir.strip(),
            "model": selected_model or gateway.settings.worker_model,
            "worker_backend": (
                body.worker_backend.strip() or targets.slack_coding.handoff_worker_backend
            ),
            "title": first_line,
            "source_conversation_id": source_conversation_id,
            "workspace_path": body.workspace_path.strip(),
            "created_at": datetime.now(UTC).isoformat(),
        }
        (art_dir / "handoff_meta.json").write_text(
            json.dumps(meta_payload, indent=2), encoding="utf-8"
        )
        if base_sha:
            (art_dir / "base_sha.txt").write_text(f"{base_sha}\n", encoding="utf-8")
        if had_base_patch:
            patch_content = base_patch if base_patch.endswith("\n") else f"{base_patch}\n"
            (art_dir / "initial.patch").write_text(patch_content, encoding="utf-8")
            (art_dir / "fix.patch").write_text(patch_content, encoding="utf-8")

        ack = await gateway.on_coding_request(
            formatted_prompt, "laptop-handoff", thread_ts, channel_id
        )
        issue_rec = gateway.state.get_by_thread_ts(thread_ts)
        if issue_rec is None and (
            "invalid repository override" in ack.lower() or "disabled" in ack.lower()
        ):
            raise HTTPException(status_code=400, detail=ack)
        if issue_rec is None:
            digest = (
                hashlib.sha1(f"{thread_ts}:{cleaned or prompt}".encode()).hexdigest()[:6].upper()
            )
            issue_rec = gateway.state.register_if_new(
                issue_id=issue_id,
                short_id=f"HANDOFF-{digest}",
                project_slug=repo.split("/")[-1],
                github_repo=repo,
                default_branch=branch,
                base_ref=branch,
                title=first_line,
                permalink=f"https://github.com/{repo}",
                task_kind="slack_coding",
                model_override=selected_model or None,
                thread_ts=thread_ts,
                channel_id=channel_id,
            )

        assert issue_rec is not None
        thread = gateway.state.upsert_pwa_thread(
            thread_id=issue_rec.issue_id,
            thread_ts=thread_ts,
            kind="coding",
            title=f"{issue_rec.short_id}: {issue_rec.title}",
            subtitle=f"{issue_rec.github_repo} • turn {issue_rec.turn} • handoff",
            status=issue_rec.status.value,
            tags=["handoff"],
            model=(issue_rec.model_override or selected_model or gateway.settings.worker_model),
            issue_id=issue_rec.issue_id,
            last_message_preview=ack,
        )
        gateway.state.append_pwa_message(
            thread_id=thread.thread_id,
            sender="user",
            body_md=cleaned or prompt,
            payload={
                "handoff": True,
                "model": thread.model,
                "repo": issue_rec.github_repo,
                "branch": branch,
                "base_sha": base_sha,
                "had_base_patch": had_base_patch,
                "source_conversation_id": source_conversation_id,
            },
        )
        ack_msg = gateway.state.append_pwa_message(
            thread_id=thread.thread_id,
            sender="system",
            body_md=ack,
        )
        fresh = gateway.state.get_pwa_thread(thread.thread_id) or thread
        await gateway.broadcast(
            "message.created",
            {
                "thread": gateway._serialize_thread(fresh),
                "message": gateway._serialize_message(ack_msg),
            },
        )
        result = gateway._build_handoff_payload(issue_rec, include_diff=False)
        result["ok"] = True
        result["ack"] = ack
        return result

    @router.get("/api/handoff", dependencies=[Depends(_require_auth)])
    async def list_handoffs(
        repo: str = Query(default=""),
        limit: int = Query(default=10, ge=1, le=50),
    ) -> dict[str, Any]:
        records = gateway.state.list_recent_issues(
            task_kind="slack_coding",
            github_repo=repo.strip(),
            short_id_prefix="HANDOFF-",
            limit=limit,
        )
        return {
            "handoffs": [gateway._build_handoff_payload(r, include_diff=False) for r in records],
        }

    @router.get("/api/handoff/repo-access", dependencies=[Depends(_require_auth)])
    async def check_handoff_repo_access(
        repo: str = Query(default=""),
    ) -> dict[str, Any]:
        clean_repo = repo.strip()
        if not clean_repo:
            raise HTTPException(status_code=400, detail="Query parameter `repo` is required.")
        token = gateway.settings.resolve_github_token(clean_repo)
        accessible, has_mirror, reason = await check_github_repo_access(
            gateway.settings.state_dir / "mirrors",
            clean_repo,
            token,
        )
        max_patch_bytes = int(
            os.environ.get("HANDOFF_MAX_PATCH_BYTES", str(DEFAULT_MAX_INLINE_PATCH_BYTES))
        )
        return {
            "repo": clean_repo,
            "accessible": accessible,
            "has_mirror": has_mirror,
            "reason": reason,
            "max_patch_bytes": max_patch_bytes,
            "max_snapshot_bytes": DEFAULT_MAX_REPO_SNAPSHOT_BYTES,
            "default_handoff_timeout_seconds": gateway.settings.handoff_active_deadline_seconds,
        }

    @router.get("/api/handoff/{handoff_id}", dependencies=[Depends(_require_auth)])
    async def get_handoff(
        handoff_id: str,
        repo: str = Query(default=""),
        include_diff: bool = Query(default=True),
        include_patch: bool | None = Query(default=None),
    ) -> dict[str, Any]:
        record = gateway._resolve_handoff_record(handoff_id, repo=repo.strip())
        if record is None:
            raise HTTPException(status_code=404, detail=f"Handoff `{handoff_id}` not found.")
        effective_include_diff = include_patch if include_patch is not None else include_diff
        return gateway._build_handoff_payload(record, include_diff=effective_include_diff)

    @router.post("/api/handoff/{handoff_id}/mark-pulled", dependencies=[Depends(_require_auth)])
    async def mark_handoff_pulled(handoff_id: str) -> dict[str, Any]:
        record = gateway._resolve_handoff_record(handoff_id)
        if record is None:
            raise HTTPException(status_code=404, detail=f"Handoff `{handoff_id}` not found.")
        payload = gateway._build_handoff_payload(record, include_diff=False)
        payload["pulled"] = True
        return payload

    @router.post("/api/handoff/{handoff_id}/feedback", dependencies=[Depends(_require_auth)])
    async def reply_to_handoff(
        handoff_id: str,
        body: _HandoffFeedbackRequest,
    ) -> dict[str, Any]:
        record = gateway._resolve_handoff_record(handoff_id)
        if record is None or not record.thread_ts:
            raise HTTPException(status_code=404, detail=f"Handoff `{handoff_id}` not found.")
        feedback = body.feedback.strip()
        if not feedback:
            raise HTTPException(status_code=400, detail="Feedback cannot be empty.")
        if body.model.strip() and "[" not in feedback:
            short_alias = body.model.strip().removeprefix("gemini-4-").removeprefix("gemini-3.8-")
            feedback = f"[{short_alias}] {feedback}".strip()
        if gateway.on_thread_feedback is None:
            raise HTTPException(status_code=503, detail="Thread feedback handler is not enabled.")
        await gateway.on_thread_feedback(record.thread_ts, feedback, "laptop-handoff")
        refreshed = gateway.state.get_by_issue_id(record.issue_id) or record
        return gateway._build_handoff_payload(refreshed, include_diff=False)

    @router.post("/api/threads/{thread_id}/reply", dependencies=[Depends(_require_auth)])
    async def reply_to_thread(
        thread_id: str,
        text: str = Form(default=""),
        model: str = Form(default=""),
        files: Annotated[list[UploadFile] | None, File()] = None,
    ) -> dict[str, Any]:
        thread = gateway.state.get_pwa_thread(thread_id)
        if thread is None:
            raise HTTPException(status_code=404, detail="Thread not found.")

        uploads: list[tuple[str, bytes]] = []
        for f in (files or [])[:4]:
            raw = await f.read()
            if raw:
                uploads.append((f.filename or "image.jpg", raw))

        if thread.kind == "chat":
            if not text.strip() and not uploads:
                raise HTTPException(status_code=400, detail="Reply cannot be empty.")
            updated_thread, user_msg, image_paths, cleaned_prompt, chosen_model = (
                gateway.direct_chat.create_chat_and_append_user_msg(
                    prompt=text,
                    model=model,
                    uploads=uploads,
                    thread_id=thread_id,
                )
            )
            await gateway.broadcast(
                "message.created",
                {
                    "thread": gateway._serialize_thread(updated_thread),
                    "message": gateway._serialize_message(user_msg),
                },
            )
            gateway._track_bg(
                gateway._run_direct_chat_turn_bg(
                    thread_id=thread_id,
                    prompt=cleaned_prompt,
                    model=chosen_model,
                    image_paths=image_paths,
                )
            )
            return {
                "thread": gateway._serialize_thread(updated_thread),
                "message": gateway._serialize_message(user_msg),
            }

        if not text.strip() and not uploads:
            raise HTTPException(status_code=400, detail="Reply cannot be empty.")

        clean_text = text.strip()
        if model.strip() and "[" not in clean_text:
            short_alias = model.strip().removeprefix("gemini-4-").removeprefix("gemini-3.8-")
            clean_text = f"[{short_alias}] {clean_text}".strip()

        user_msg = gateway.state.append_pwa_message(
            thread_id=thread_id,
            sender="user",
            body_md=clean_text or "(photo attached)",
            payload={"model": model.strip()} if model.strip() else {},
        )
        await gateway.broadcast(
            "message.created",
            {
                "thread": gateway._serialize_thread(thread),
                "message": gateway._serialize_message(user_msg),
            },
        )

        # Mirror user reply to Slack thread if backed by a Slack ts
        ts = thread.thread_ts or ""
        if (
            gateway.slack_adapter is not None
            and gateway.settings.pwa_mirror_to_slack
            and ts
            and not ts.startswith("pwa")
        ):
            try:
                ch = gateway.slack_adapter.resolve_channel_for_task(thread.kind)
                await gateway.slack_adapter.post_thread_message(
                    ts, f"📱 *Mobile reply:* {clean_text}", channel_id=ch
                )
            except Exception as exc:  # noqa: BLE001
                log.warning("pwa.reply_slack_mirror_failed", thread_id=thread_id, error=str(exc))

        if thread.kind == "food" and gateway.on_food_thread_reply is not None:
            image_dicts = [{"name": name, "raw_bytes": raw} for name, raw in uploads]
            ch = f"pwa:{thread.kind}"
            if thread.food_entry_id and gateway.food_task is not None:
                meal = gateway.food_task.nutrition.get_meal_entry(thread.food_entry_id)
                if meal and meal.slack_channel_id:
                    ch = meal.slack_channel_id
                if meal and meal.slack_thread_ts:
                    ts = meal.slack_thread_ts
            await gateway.on_food_thread_reply(
                ts, clean_text, "mobile", ch, _synth_ts("reply"), image_dicts
            )
        elif gateway.on_thread_feedback is not None and ts:
            await gateway.on_thread_feedback(ts, clean_text, "mobile")

        fresh = gateway.state.get_pwa_thread(thread_id) or thread
        return {
            "thread": gateway._serialize_thread(fresh),
            "message": gateway._serialize_message(user_msg),
        }

    @router.post("/api/threads/{thread_id}/actions", dependencies=[Depends(_require_auth)])
    async def run_thread_action(thread_id: str, body: _ThreadActionRequest) -> dict[str, Any]:
        thread = gateway.state.get_pwa_thread(thread_id)
        if thread is None:
            raise HTTPException(status_code=404, detail="Thread not found.")
        issue_id = thread.issue_id or thread.thread_id
        act = body.action.strip().lower()

        if act == "open_draft_pr" and gateway.on_open_draft_pr is not None:
            await gateway.on_open_draft_pr(issue_id, "mobile")
        elif act == "push_to_pr_branch" and gateway.on_push_to_pr_branch is not None:
            await gateway.on_push_to_pr_branch(issue_id, "mobile")
        elif act == "resolve" and gateway.on_resolve is not None:
            await gateway.on_resolve(issue_id, "mobile")
        elif act == "ignore" and gateway.on_ignore is not None:
            await gateway.on_ignore(issue_id, "mobile")
        elif act == "reject":
            if gateway.on_reject is not None:
                await gateway.on_reject(issue_id, "mobile")
            else:
                gateway.state.cancel_timers_for_issue(issue_id)
                gateway.state.mark_status(issue_id, IssueStatus.REJECTED)
                gateway.state.append_pwa_message(
                    thread_id=thread_id,
                    sender="system",
                    body_md="Rejected proposal (by `mobile`). No PR or branch push will be performed.",
                )
        elif act == "save_memory_note" and gateway.on_save_memory_note is not None:
            await gateway.on_save_memory_note(issue_id, "mobile")
        else:
            raise HTTPException(status_code=400, detail=f"Unsupported action `{body.action}`.")

        gateway._sync_issue_thread_metadata(issue_id)
        fresh = gateway.state.get_pwa_thread(thread_id) or thread
        await gateway.broadcast("thread.updated", {"thread": gateway._serialize_thread(fresh)})
        return {
            "ok": True,
            "thread": gateway._serialize_thread(fresh),
            "messages": [
                gateway._serialize_message(m) for m in gateway.state.list_pwa_messages(thread_id)
            ],
        }

    # ------------------------------------------------------------------
    # Food Tracker Native Endpoints (Authenticated)
    # ------------------------------------------------------------------

    @router.get("/api/food/today", dependencies=[Depends(_require_auth)])
    async def food_today(date: str = Query(default="")) -> dict[str, Any]:
        return gateway._build_food_today_payload(date or None)

    @router.get("/api/food/week", dependencies=[Depends(_require_auth)])
    async def food_week() -> dict[str, Any]:
        if gateway.food_task is None:
            raise HTTPException(status_code=503, detail="Food tracker is not enabled.")
        cfg = gateway._load_targets().food_tracker
        today = gateway.food_task._today_local_date(cfg)
        weekly = gateway.food_task.nutrition.get_weekly_summary(today, cfg)
        latest_w = gateway.food_task.nutrition.get_latest_weight()
        tdee = gateway.food_task.nutrition.estimate_adaptive_tdee(today)
        data = asdict(weekly)
        data["daily_totals"] = [
            {
                "local_date": d.local_date,
                "total_calories_kcal": d.total_calories_kcal,
                "total_protein_g": d.total_protein_g,
                "total_carbs_g": d.total_carbs_g,
                "total_fat_g": d.total_fat_g,
                "total_fiber_g": d.total_fiber_g,
                "meal_count": len(d.meals),
            }
            for d in weekly.daily_totals
        ]
        data["latest_weight"] = asdict(latest_w) if latest_w else None
        data["tdee"] = asdict(tdee) if tdee else None
        return data

    @router.post("/api/food/log", dependencies=[Depends(_require_auth)])
    async def food_log(
        text: str = Form(default=""),
        model: str = Form(default=""),
        staple_slug: str = Form(default=""),
        multiplier: float = Form(default=1.0),
        async_mode: bool = Form(default=False),
        files: Annotated[list[UploadFile] | None, File()] = None,
    ) -> dict[str, Any]:
        if gateway.on_food_message is None:
            raise HTTPException(status_code=503, detail="Food tracker is not enabled.")

        image_dicts: list[dict[str, Any]] = []
        for f in (files or [])[:4]:
            raw = await f.read()
            if raw:
                image_dicts.append({"name": f.filename or "meal.jpg", "raw_bytes": raw})

        if staple_slug.strip():
            mult_prefix = f"{multiplier:g}x " if abs(multiplier - 1.0) > 1e-6 else ""
            effective_text = f"!eat {mult_prefix}{staple_slug.strip()}"
        else:
            effective_text = text.strip()
            if model.strip() and "[" not in effective_text:
                short_alias = model.strip().removeprefix("gemini-4-").removeprefix("gemini-3.8-")
                effective_text = f"[{short_alias}] {effective_text}".strip()

        if not effective_text and not image_dicts:
            raise HTTPException(
                status_code=400, detail="Provide a meal photo, description, or staple."
            )

        channel_id = "pwa:food"
        message_ts = ""
        if gateway.slack_adapter is not None and gateway.settings.pwa_mirror_to_slack:
            food_ch = gateway.slack_adapter.resolve_channel("food")
            if gateway.slack_adapter.is_explicit_role_channel("food", food_ch):
                try:
                    channel_id = food_ch
                    label = effective_text or "Meal photo"
                    message_ts = await gateway.slack_adapter.post_channel_message(
                        f"📱 *Mobile Meal:* {label}",
                        role="food",
                        channel_id=food_ch,
                    )
                except Exception as exc:  # noqa: BLE001
                    log.warning("pwa.food_slack_mirror_failed", error=str(exc))

        if not message_ts:
            channel_id = "pwa:food"
            message_ts = _synth_ts("food")

        def _resolve_outcome() -> tuple[Any, str | None, str | None]:
            logged = (
                gateway.food_task.nutrition.get_meal_by_thread_ts(message_ts)
                if gateway.food_task is not None
                else None
            )
            if logged is not None:
                return logged, None, None
            err_thread = gateway.state.get_pwa_thread_by_ts(message_ts)
            if err_thread is not None:
                msgs = gateway.state.list_pwa_messages(err_thread.thread_id)
                if msgs:
                    last_body = msgs[-1].body_md.strip()
                    lower_body = last_body.lower()
                    if any(
                        marker in lower_body
                        for marker in ("failed to", "disabled", "cannot ", "error")
                    ):
                        return None, None, last_body
                    return None, last_body, None
            if not staple_slug.strip() and not effective_text.lstrip().startswith("!"):
                return None, None, "Meal estimation did not create an entry."
            return None, None, None

        t0 = time.monotonic()
        enqueue_immediately = bool(async_mode and not staple_slug.strip())
        backgrounded = enqueue_immediately
        pending_label = text.strip() or (
            f"{len(image_dicts)} meal photo(s)" if image_dicts else staple_slug.strip() or "Meal"
        )
        pending_model = (
            model.strip().removeprefix("gemini-4-").removeprefix("gemini-3.8-")
            if model.strip()
            else ""
        )
        gateway._pending_food_logs[message_ts] = PendingFoodLog(
            thread_ts=message_ts,
            label=pending_label,
            model=pending_model,
            started_at=datetime.now(UTC).isoformat(),
        )
        log.info(
            "pwa.food_log_started",
            thread_ts=message_ts,
            has_files=bool(image_dicts),
            is_staple=bool(staple_slug.strip()),
            async_mode=enqueue_immediately,
        )

        async def _run_food_log() -> None:
            try:
                assert gateway.on_food_message is not None
                await gateway.on_food_message(
                    effective_text, "mobile", channel_id, message_ts, image_dicts, True
                )
            except Exception as exc:
                gateway._pending_food_logs.pop(message_ts, None)
                log.exception(
                    "pwa.food_log_failed",
                    thread_ts=message_ts,
                    duration_ms=round((time.monotonic() - t0) * 1000),
                    error=str(exc),
                )
                if backgrounded:
                    await gateway.broadcast(
                        "food.updated",
                        {
                            "source": "log",
                            "thread_ts": message_ts,
                            "error": f"Failed to estimate meal: {exc}",
                        },
                    )
                return

            gateway._pending_food_logs.pop(message_ts, None)
            logged, notice_msg, err_msg = _resolve_outcome()
            duration_ms = round((time.monotonic() - t0) * 1000)
            if err_msg:
                log.warning(
                    "pwa.food_log_error",
                    thread_ts=message_ts,
                    duration_ms=duration_ms,
                    error=err_msg,
                )
            else:
                log.info(
                    "pwa.food_log_completed",
                    thread_ts=message_ts,
                    duration_ms=duration_ms,
                    entry_id=logged.id if logged is not None else None,
                )
            if backgrounded:
                ws_payload: dict[str, Any] = {
                    "source": "log",
                    "thread_ts": message_ts,
                }
                if logged is not None:
                    ws_payload["entry_id"] = logged.id
                    ws_payload["meal_title"] = logged.title
                    ws_payload["calories_kcal"] = logged.total_calories_kcal
                    ws_payload["protein_g"] = logged.total_protein_g
                elif err_msg:
                    ws_payload["error"] = err_msg
                elif notice_msg:
                    ws_payload["notice"] = notice_msg
                await gateway.broadcast("food.updated", ws_payload)

        task = gateway._track_bg(_run_food_log())
        if enqueue_immediately:
            log.info("pwa.food_log_enqueued", thread_ts=message_ts)
            payload = gateway._build_food_today_payload()
            payload["queued"] = True
            payload["thread_ts"] = message_ts
            return payload

        try:
            await asyncio.wait_for(asyncio.shield(task), timeout=gateway._food_log_sync_timeout_s)
        except TimeoutError:
            backgrounded = True
            log.info(
                "pwa.food_log_backgrounded",
                thread_ts=message_ts,
                timeout_s=gateway._food_log_sync_timeout_s,
            )
            payload = gateway._build_food_today_payload()
            payload["queued"] = True
            payload["thread_ts"] = message_ts
            return payload

        logged_meal, notice, err_detail = _resolve_outcome()
        if err_detail:
            raise HTTPException(status_code=500, detail=err_detail)

        await gateway.broadcast("food.updated", {"source": "log", "thread_ts": message_ts})
        payload = gateway._build_food_today_payload()
        if logged_meal is not None:
            payload["logged_meal"] = gateway._serialize_meal(logged_meal)
        if notice:
            payload["notice"] = notice
        return payload

    @router.post("/api/food/entries/{entry_id}/scale", dependencies=[Depends(_require_auth)])
    async def food_scale(entry_id: int, body: _ScaleFoodRequest) -> dict[str, Any]:
        if gateway.on_food_scale is None:
            raise HTTPException(status_code=503, detail="Food tracker is not enabled.")
        await gateway.on_food_scale(entry_id, float(body.multiplier), "mobile")
        return gateway._build_food_today_payload()

    @router.post("/api/food/entries/{entry_id}/reaction", dependencies=[Depends(_require_auth)])
    async def food_reaction(entry_id: int, body: _MealReactionRequest) -> dict[str, Any]:
        if gateway.food_task is None:
            raise HTTPException(status_code=503, detail="Food tracker is not enabled.")
        try:
            updated = gateway.food_task.nutrition.set_meal_reaction(entry_id, body.emoji)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        await gateway.broadcast(
            "food.updated",
            {"source": "reaction", "entry_id": entry_id, "emoji": updated.reaction_emoji},
        )
        return gateway._build_food_today_payload(local_date=updated.local_date)

    @router.post("/api/food/entries/{entry_id}/save-staple", dependencies=[Depends(_require_auth)])
    async def food_save_staple(entry_id: int) -> dict[str, Any]:
        if gateway.on_food_save_staple is None:
            raise HTTPException(status_code=503, detail="Food tracker is not enabled.")
        await gateway.on_food_save_staple(entry_id, "mobile")
        return gateway._build_food_today_payload()

    @router.post("/api/food/entries/{entry_id}/log-again", dependencies=[Depends(_require_auth)])
    async def food_log_again(entry_id: int, body: _LogAgainRequest | None = None) -> dict[str, Any]:
        if gateway.food_task is None:
            raise HTTPException(status_code=503, detail="Food tracker is not enabled.")
        src = gateway.food_task.nutrition.get_meal_entry(entry_id)
        if src is None:
            raise HTTPException(status_code=404, detail=f"Meal entry #{entry_id} not found.")

        from zoneinfo import ZoneInfo

        from agent_tasks.tasks.food_tracker.models import EstimatedMealItem, MealEstimateSchema
        from agent_tasks.tasks.food_tracker.store import effective_local_date

        cfg = gateway._load_targets().food_tracker
        req = body or _LogAgainRequest()
        logged_at_iso: str | None = None
        target_dt_utc: datetime | None = None
        if req.logged_at.strip():
            try:
                parsed = datetime.fromisoformat(req.logged_at.strip().replace("Z", "+00:00"))
            except ValueError as exc:
                raise HTTPException(status_code=400, detail="Invalid logged_at timestamp.") from exc
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=ZoneInfo(cfg.timezone))
            target_dt_utc = parsed.astimezone(UTC)
            logged_at_iso = target_dt_utc.isoformat()

        effective_date = req.local_date.strip() or effective_local_date(
            target_dt_utc or datetime.now(UTC),
            tz_name=cfg.timezone,
            rollover_hour=cfg.day_rollover_hour,
        )

        estimate = MealEstimateSchema(
            title=src.title,
            meal_slot=src.meal_slot,
            confidence=src.confidence,
            assumptions_note=src.assumptions_note or "",
            clarification_question="",
            items=[
                EstimatedMealItem(
                    name=it.name,
                    quantity_desc=it.quantity_desc,
                    estimated_grams=it.estimated_grams,
                    calories_kcal=it.calories_kcal,
                    protein_g=it.protein_g,
                    carbs_g=it.carbs_g,
                    fat_g=it.fat_g,
                    fiber_g=it.fiber_g,
                    source=it.source,
                )
                for it in src.items
            ],
        )
        cloned = gateway.food_task.nutrition.create_meal_entry(
            local_date=effective_date,
            estimate=estimate,
            raw_description=src.raw_description,
            model_used="log-again",
            photo_relpath=src.photo_relpath,
            scale_factor=src.scale_factor,
            logged_at=logged_at_iso,
            reasoning_trace=src.reasoning_trace,
        )
        await gateway.broadcast(
            "food.updated",
            {
                "source": "log_again",
                "entry_id": cloned.id,
                "meal_title": cloned.title,
                "calories_kcal": cloned.total_calories_kcal,
                "protein_g": cloned.total_protein_g,
                "local_date": cloned.local_date,
            },
        )
        return gateway._build_food_today_payload(local_date=cloned.local_date)

    @router.patch("/api/food/entries/{entry_id}", dependencies=[Depends(_require_auth)])
    async def food_update(entry_id: int, body: _UpdateMealRequest) -> dict[str, Any]:
        if gateway.food_task is None:
            raise HTTPException(status_code=503, detail="Food tracker is not enabled.")

        from agent_tasks.tasks.food_tracker.models import EstimatedMealItem

        items_payload: list[EstimatedMealItem] | None = None
        if body.items is not None:
            valid_sources = {"vision_estimate", "nutrition_label", "saved_food", "user_edit"}
            items_payload = [
                EstimatedMealItem(
                    name=it.name.strip() or "Food item",
                    quantity_desc=it.quantity_desc.strip() or "1 serving",
                    estimated_grams=it.estimated_grams,
                    calories_kcal=it.calories_kcal,
                    protein_g=it.protein_g,
                    carbs_g=it.carbs_g,
                    fat_g=it.fat_g,
                    fiber_g=it.fiber_g,
                    source=it.source if it.source in valid_sources else "user_edit",  # type: ignore[arg-type]
                )
                for it in body.items
            ]

        try:
            updated = gateway.food_task.nutrition.update_meal_entry(
                entry_id,
                local_date=body.local_date,
                meal_slot=body.meal_slot,  # type: ignore[arg-type]
                title=body.title,
                scale_factor=body.scale_factor,
                items=items_payload,
            )
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

        await gateway.broadcast(
            "food.updated",
            {
                "source": "edit",
                "entry_id": updated.id,
                "meal_title": updated.title,
                "local_date": updated.local_date,
            },
        )
        view_date = body.view_date.strip() or updated.local_date
        payload = gateway._build_food_today_payload(local_date=view_date)
        payload["meal"] = gateway._serialize_meal(updated)
        return payload

    @router.delete("/api/food/entries/{entry_id}", dependencies=[Depends(_require_auth)])
    async def food_delete(entry_id: int) -> dict[str, Any]:
        if gateway.on_food_delete is None:
            raise HTTPException(status_code=503, detail="Food tracker is not enabled.")
        await gateway.on_food_delete(entry_id, "mobile")
        return gateway._build_food_today_payload()

    @router.post("/api/food/weight", dependencies=[Depends(_require_auth)])
    async def food_weight(body: _WeightRequest) -> dict[str, Any]:
        if gateway.food_task is None:
            raise HTTPException(status_code=503, detail="Food tracker is not enabled.")
        if body.weight_lbs <= 40 or body.weight_lbs >= 800:
            raise HTTPException(status_code=400, detail="Weight must be between 40 and 800 lbs.")
        cfg = gateway._load_targets().food_tracker
        today = gateway.food_task._today_local_date(cfg)
        rec = gateway.food_task.nutrition.log_weight(
            local_date=today, weight_lbs=body.weight_lbs, notes=body.notes
        )
        await gateway.broadcast("food.updated", {"source": "weight"})
        return {"weight": asdict(rec), "today": gateway._build_food_today_payload(today)}

    @router.post("/api/food/goals", dependencies=[Depends(_require_auth)])
    async def food_goals(body: _GoalsRequest) -> dict[str, Any]:
        if gateway.food_task is None:
            raise HTTPException(status_code=503, detail="Food tracker is not enabled.")
        cfg = gateway._load_targets().food_tracker
        today = gateway.food_task._today_local_date(cfg)
        saved = gateway.food_task.nutrition.set_target(
            effective_date=today,
            calories_kcal=max(500, body.calories_kcal),
            protein_g=max(0.0, body.protein_g),
            carbs_g=max(0.0, body.carbs_g),
            fat_g=max(0.0, body.fat_g),
            fiber_g=max(0.0, body.fiber_g),
            notes="updated via mobile PWA",
        )
        await gateway.broadcast("food.updated", {"source": "goals"})
        return {"target": asdict(saved), "today": gateway._build_food_today_payload(today)}

    @router.post("/api/food/garmin/sync", dependencies=[Depends(_require_auth)])
    async def food_garmin_sync() -> dict[str, Any]:
        if gateway.food_task is None:
            raise HTTPException(status_code=503, detail="Food tracker is not enabled.")
        if gateway.food_task.garmin is None:
            raise HTTPException(status_code=400, detail="GARMIN_API_KEY is not configured.")
        cfg = gateway._load_targets().food_tracker
        days_cnt, weights_cnt = await gateway.food_task.sync_garmin(days=cfg.garmin_lookback_days)
        await gateway.broadcast("food.updated", {"source": "garmin"})
        return {
            "days_synced": days_cnt,
            "weights_synced": weights_cnt,
            "today": gateway._build_food_today_payload(),
        }

    @router.get("/api/food/trends", dependencies=[Depends(_require_auth)])
    async def food_trends(days: int = Query(default=14, ge=7, le=60)) -> dict[str, Any]:
        if gateway.food_task is None:
            raise HTTPException(status_code=503, detail="Food tracker is not enabled.")
        cfg = gateway._load_targets().food_tracker
        today = gateway.food_task._today_local_date(cfg)
        return gateway.food_task.nutrition.get_food_dashboard_trends(today, cfg, days=days)

    @router.post("/api/food/garmin/settings", dependencies=[Depends(_require_auth)])
    async def food_garmin_settings(body: _GarminSettingsRequest) -> dict[str, Any]:
        if gateway.food_task is None:
            raise HTTPException(status_code=503, detail="Food tracker is not enabled.")
        if body.dynamic_enabled is not None:
            gateway.food_task.nutrition.set_food_setting(
                "garmin_dynamic_enabled", "true" if body.dynamic_enabled else "false"
            )
        if body.goal_delta_kcal is not None:
            clamped_delta = max(-1500, min(1500, int(body.goal_delta_kcal)))
            gateway.food_task.nutrition.set_food_setting(
                "garmin_goal_delta_kcal", str(clamped_delta)
            )
        if body.dynamic_mode is not None:
            if body.dynamic_mode not in ("rolling_baseline", "daily_burn"):
                raise HTTPException(status_code=400, detail="Invalid dynamic_mode.")
            gateway.food_task.nutrition.set_food_setting("garmin_dynamic_mode", body.dynamic_mode)
        if body.active_factor is not None:
            clamped_factor = round(max(0.0, min(1.5, float(body.active_factor))), 2)
            gateway.food_task.nutrition.set_food_setting(
                "garmin_active_factor", str(clamped_factor)
            )
        await gateway.broadcast("food.updated", {"source": "garmin_settings"})
        return gateway._build_food_today_payload()

    @router.post("/api/food/suggest", dependencies=[Depends(_require_auth)])
    async def food_suggest(body: _SuggestFoodRequest) -> dict[str, Any]:
        if gateway.food_task is None:
            raise HTTPException(status_code=503, detail="Food tracker is not enabled.")
        targets = gateway._load_targets()
        cfg = targets.food_tracker
        today = gateway.food_task._today_local_date(cfg)
        summary = gateway.food_task.nutrition.get_daily_summary(today, cfg)
        staples = gateway.food_task.nutrition.list_saved_foods()
        suggestion, usage, model_used = await gateway.food_task.estimator.suggest_remaining(
            summary=summary,
            saved_foods=staples,
            note=body.craving.strip(),
            model_override=body.model.strip() or None,
        )
        cost_line = gateway.food_task._record_usage(
            issue_id=f"food-suggest-pwa-{int(time.time())}",
            turn=1,
            model=model_used,
            usage=usage,
            targets=targets,
            note="pwa_food_suggest",
        )
        return {"suggestion": suggestion, "model_used": model_used, "cost_line": cost_line}

    @router.get("/api/media/{relpath:path}", dependencies=[Depends(_require_auth)])
    async def serve_media(relpath: str) -> Response:
        state_root = gateway.settings.state_dir.resolve()
        allowed_roots = [
            (state_root / "food_photos").resolve(),
            (state_root / "pwa_uploads").resolve(),
        ]
        candidate = (state_root / relpath).resolve()
        if not any(candidate.is_relative_to(root) for root in allowed_roots):
            raise HTTPException(status_code=403, detail="Forbidden media path.")
        if not candidate.is_file():
            raise HTTPException(status_code=404, detail="Media file not found.")
        return FileResponse(candidate, headers={"Cache-Control": "private, max-age=86400"})

    # ------------------------------------------------------------------
    # Ops, Billing & Web Push (Authenticated)
    # ------------------------------------------------------------------

    @router.get("/api/ops/overview", dependencies=[Depends(_require_auth)])
    async def ops_overview() -> dict[str, Any]:
        overview = gateway._build_ops_overview_sync()

        async def _safe_call(cb: StatusCallback | None) -> str:
            if cb is None:
                return ""
            try:
                return await asyncio.wait_for(cb(), timeout=8.0)
            except Exception as exc:  # noqa: BLE001
                return f"Unavailable: {exc}"

        status_txt, k8s_txt, ci_txt = await asyncio.gather(
            _safe_call(gateway.on_status_query),
            _safe_call(gateway.on_k8s_query),
            _safe_call(gateway.on_ci_query),
        )
        overview["reports"] = {
            "controller": status_txt,
            "k8s": k8s_txt,
            "ci": ci_txt,
        }
        return overview

    @router.post("/api/ops/billing", dependencies=[Depends(_require_auth)])
    async def ops_billing(body: _BillingCmdRequest) -> dict[str, Any]:
        if gateway.on_billing_command is None:
            raise HTTPException(status_code=503, detail="Billing guard is not enabled.")
        reply = await gateway.on_billing_command(body.command.strip())
        return {"reply": reply, "overview": gateway._build_ops_overview_sync()}

    @router.post("/api/ops/deps", dependencies=[Depends(_require_auth)])
    async def ops_deps(body: _DepsRequest) -> dict[str, Any]:
        if gateway.on_deps_trigger is None:
            raise HTTPException(status_code=503, detail="Dependency upgrader is not enabled.")
        args = " ".join(p for p in [body.model.strip(), body.focus.strip()] if p)
        ack = await gateway.on_deps_trigger(args, _synth_ts("deps"), "pwa:deps")
        return {"ack": ack}

    @router.get("/api/ops/timers", dependencies=[Depends(_require_auth)])
    async def ops_list_timers() -> dict[str, Any]:
        timers = [asdict(t) for t in gateway.state.list_pending_timers()]
        return {"timers": timers}

    @router.delete("/api/ops/timers/{timer_id}", dependencies=[Depends(_require_auth)])
    async def ops_cancel_timer(timer_id: int) -> dict[str, Any]:
        cancelled = gateway.state.cancel_timer_by_id(timer_id)
        if not cancelled:
            raise HTTPException(status_code=404, detail=f"Timer #{timer_id} not found.")
        return {"ok": True, "cancelled": timer_id}

    @router.post("/api/threads/{thread_id}/snooze", dependencies=[Depends(_require_auth)])
    async def snooze_thread(thread_id: str, body: _SnoozeRequest) -> dict[str, Any]:
        if gateway.on_snooze is None:
            raise HTTPException(status_code=503, detail="Snooze timers are not enabled.")
        thread = gateway.state.get_pwa_thread(thread_id)
        if thread is None or not thread.thread_ts:
            raise HTTPException(status_code=404, detail="Thread not found.")
        args = f"{body.duration.strip() or '2h'} {body.note.strip()}".strip()
        ack = await gateway.on_snooze(thread.thread_ts, args, "mobile")
        if thread.issue_id:
            gateway._sync_issue_thread_metadata(thread.issue_id)
        sys_msg = gateway.state.append_pwa_message(
            thread_id=thread_id,
            sender="system",
            body_md=ack,
        )
        fresh = gateway.state.get_pwa_thread(thread_id) or thread
        await gateway.broadcast(
            "message.created",
            {
                "thread": gateway._serialize_thread(fresh),
                "message": gateway._serialize_message(sys_msg),
            },
        )
        return {"ok": True, "ack": ack, "thread": gateway._serialize_thread(fresh)}

    @router.get("/api/ops/memory", dependencies=[Depends(_require_auth)])
    async def ops_list_memory() -> dict[str, Any]:
        from agent_tasks.common.memory import MemoryStore

        mem = MemoryStore(gateway.settings.state_dir / "memory")
        return {"entries": [asdict(e) for e in mem.list_all()]}

    @router.post("/api/ops/memory", dependencies=[Depends(_require_auth)])
    async def ops_add_memory(body: _RememberRequest) -> dict[str, Any]:
        if gateway.on_memory_command is None:
            raise HTTPException(status_code=503, detail="Persistent memory is not enabled.")
        scope = body.scope.strip()
        text = body.text.strip()
        if not text:
            raise HTTPException(status_code=400, detail="Memory text cannot be empty.")
        if scope.startswith("repo:"):
            arg = f"{scope} {text}"
        elif scope in {"suppressed", "suppress"}:
            arg = f"suppress {text}"
        else:
            arg = text
        reply = await gateway.on_memory_command("remember", arg, "mobile")
        return {"ok": True, "reply": reply}

    @router.delete("/api/ops/memory/{entry_id}", dependencies=[Depends(_require_auth)])
    async def ops_delete_memory(entry_id: str) -> dict[str, Any]:
        if gateway.on_memory_command is None:
            raise HTTPException(status_code=503, detail="Persistent memory is not enabled.")
        reply = await gateway.on_memory_command("forget", entry_id, "mobile")
        return {"ok": True, "reply": reply}

    prefix_to_mode: dict[str, str] = {}

    def _infer_app_mode(request: Request, explicit: str | None) -> str:
        if explicit and explicit.strip():
            return explicit.strip()
        path = request.url.path or ""
        for pfx, mode in sorted(prefix_to_mode.items(), key=lambda kv: len(kv[0]), reverse=True):
            if pfx and (path == pfx or path.startswith(pfx + "/")):
                return mode
        return "chat"

    def _normalize_push_prefs(
        app_mode: str,
        raw_prefs: dict[str, bool] | None,
    ) -> dict[str, bool]:
        manifest = build_ui_manifest(gateway.plugin_registry, base_path=base_path)
        manifest_cats = manifest.get("push_categories", [])
        all_keys: set[str] = set(DEFAULT_PUSH_PREFS.keys()) | {
            str(c["key"]) for c in manifest_cats if c.get("key")
        }
        cat_defaults = {
            str(c["key"]): bool(c.get("default_enabled", True))
            for c in manifest_cats
            if c.get("key")
        }
        views = manifest.get("views", [])
        if app_mode != "chat":
            view = next((v for v in views if v.get("id") == app_mode), None)
            view_keys = list(view.get("push_category_keys", [])) if view else []
            if not view_keys and app_mode == "health":
                view_keys = ["food"]
            allowed = set(view_keys) | {"ops"}
            src = raw_prefs or {}
            return {
                k: (bool(src.get(k, cat_defaults.get(k, True))) if k in allowed else False)
                for k in sorted(all_keys)
            }

        standalone_only_keys: set[str] = {
            str(k)
            for v in views
            if v.get("standalone_pwa")
            for k in (v.get("push_category_keys") or [])
        }
        if health_base_path and health_base_path != base_path and gateway.food_task is not None:
            standalone_only_keys.add("food")
        merged: dict[str, bool] = {}
        for k in sorted(all_keys):
            default_val = (
                False
                if k in standalone_only_keys
                else cat_defaults.get(k, DEFAULT_PUSH_PREFS.get(k, True))
            )
            if raw_prefs is not None and k in raw_prefs:
                merged[k] = bool(raw_prefs[k])
            else:
                merged[k] = bool(default_val)
        return merged

    @router.get("/api/push/vapid-public-key", dependencies=[Depends(_require_auth)])
    async def push_vapid_key(request: Request) -> dict[str, Any]:
        mode = _infer_app_mode(request, None)
        return {
            "public_key": gateway.push_manager.public_key,
            "default_prefs": _normalize_push_prefs(mode, None),
            "app_mode": mode,
        }

    @router.post("/api/push/subscribe", dependencies=[Depends(_require_auth)])
    async def push_subscribe(body: _PushSubscribeRequest, request: Request) -> dict[str, Any]:
        mode = _infer_app_mode(request, body.app_mode)
        prefs = _normalize_push_prefs(mode, body.prefs)
        sub = gateway.state.upsert_push_subscription(
            endpoint=body.endpoint,
            p256dh=body.keys.p256dh,
            auth=body.keys.auth,
            user_agent=request.headers.get("user-agent", ""),
            prefs=prefs,
            app_mode=mode,
        )
        return {"ok": True, "endpoint": sub.endpoint, "prefs": sub.prefs, "app_mode": sub.app_mode}

    @router.put("/api/push/preferences", dependencies=[Depends(_require_auth)])
    async def push_prefs(body: _PushPrefsRequest, request: Request) -> dict[str, Any]:
        mode = _infer_app_mode(request, body.app_mode)
        prefs = _normalize_push_prefs(mode, body.prefs)
        sub = gateway.state.update_push_prefs(body.endpoint, prefs, app_mode=mode)
        if sub is None:
            raise HTTPException(status_code=404, detail="Push subscription not found.")
        return {"ok": True, "prefs": sub.prefs, "app_mode": sub.app_mode}

    @router.post("/api/push/sync", dependencies=[Depends(_require_auth)])
    async def push_sync(body: _PushSyncRequest, request: Request) -> dict[str, Any]:
        sub = gateway.state.get_push_subscription(body.endpoint)
        if sub is None:
            return {"found": False}
        mode = _infer_app_mode(request, body.app_mode)
        if sub.app_mode != mode:
            normalized = _normalize_push_prefs(mode, sub.prefs)
            updated = gateway.state.update_push_prefs(body.endpoint, normalized, app_mode=mode)
            if updated is not None:
                sub = updated
        elif mode != "chat":
            normalized = _normalize_push_prefs(mode, sub.prefs)
            if normalized != sub.prefs:
                updated = gateway.state.update_push_prefs(body.endpoint, normalized, app_mode=mode)
                if updated is not None:
                    sub = updated
        return {"found": True, "app_mode": sub.app_mode, "prefs": sub.prefs}

    @router.post("/api/push/unsubscribe", dependencies=[Depends(_require_auth)])
    async def push_unsubscribe(body: _PushUnsubscribeRequest) -> dict[str, Any]:
        deleted = gateway.state.delete_push_subscription(body.endpoint)
        return {"ok": True, "deleted": deleted}

    @router.post("/api/push/test", dependencies=[Depends(_require_auth)])
    async def push_test(request: Request) -> dict[str, Any]:
        mode = _infer_app_mode(request, None)
        title = "AGY Health Connected" if mode == "health" else "AGY Tasks Connected"
        counts = await gateway.push_manager.notify(
            title=title,
            body="iOS Web Push notifications are live on makesense.",
            kind="food" if mode == "health" else "ops",
            force=True,
            target_app_mode=mode,
        )
        return counts

    # ------------------------------------------------------------------
    # Live WebSocket
    # ------------------------------------------------------------------

    @router.websocket("/api/ws")
    async def ws_endpoint(websocket: WebSocket) -> None:
        if not gateway.auth.verify_websocket(websocket):
            await websocket.close(code=4401)
            return
        await websocket.accept()
        client = _WsClientState(websocket)
        gateway._ws_clients.append(client)
        try:
            await websocket.send_text(
                json.dumps({"type": "hello", "data": {"counts": gateway.state.get_pwa_counts()}})
            )
            while True:
                raw = await websocket.receive_text()
                try:
                    msg = json.loads(raw)
                except ValueError:
                    continue
                if isinstance(msg, dict):
                    mtype = msg.get("type")
                    if mtype == "view_thread":
                        tid = msg.get("thread_id")
                        client.active_thread_id = str(tid) if tid else None
                        if "visible" in msg:
                            client.visible = bool(msg["visible"])
                    elif mtype == "ping":
                        await websocket.send_text('{"type":"pong"}')
        except WebSocketDisconnect:
            pass
        finally:
            if client in gateway._ws_clients:
                gateway._ws_clients.remove(client)

    @router.get("/api/plugins", dependencies=[Depends(_require_auth)])
    async def list_loaded_plugins() -> dict[str, Any]:
        reg = gateway.plugin_registry
        if reg is None:
            return {"plugins": [], "services": []}
        return {
            "plugins": [
                {
                    "id": pid,
                    "name": loaded.manifest.name,
                    "version": loaded.manifest.version,
                    "description": loaded.manifest.description,
                    "commands": [c.name for c in loaded.commands],
                    "push_categories": [p.model_dump() for p in loaded.manifest.push_categories],
                    "thread_kinds": [t.model_dump() for t in loaded.manifest.thread_kinds],
                    "chat_context_providers": [
                        c.model_dump() for c in loaded.manifest.chat_context_providers
                    ],
                    "pwa_views": [v.model_dump() for v in loaded.manifest.pwa_views],
                }
                for pid, loaded in reg.plugins.items()
            ],
            "services": sorted(reg.services.keys()),
        }

    @router.get("/api/ui/manifest", dependencies=[Depends(_require_auth)])
    async def get_ui_manifest() -> dict[str, Any]:
        return build_ui_manifest(gateway.plugin_registry, base_path=base_path)

    def _make_plugin_static_handler(plugin_static_root: Path):
        resolved_root = plugin_static_root.resolve()

        async def _serve_plugin_static(asset_path: str) -> Response:
            if ".." in Path(asset_path).parts:
                raise HTTPException(status_code=404, detail="Plugin static asset not found")
            candidate = (resolved_root / asset_path).resolve()
            if not candidate.is_relative_to(resolved_root) or not candidate.is_file():
                raise HTTPException(status_code=404, detail="Plugin static asset not found")
            return FileResponse(candidate, headers={"Cache-Control": "no-cache"})

        return _serve_plugin_static

    mounted_standalone_paths: set[str] = set()
    if gateway.plugin_registry is not None:
        for pid, loaded in gateway.plugin_registry.plugins.items():
            granted = loaded.context.granted_permissions
            if "api_router" in granted:
                contribution = loaded.plugin.get_gateway_contribution()
                for route_spec in contribution.http_routes:
                    sub_prefix = (
                        "/" + route_spec.prefix.strip("/") if route_spec.prefix.strip("/") else ""
                    )
                    deps = [Depends(_require_auth)] if route_spec.require_auth else []
                    router.include_router(
                        route_spec.router,
                        prefix=f"/api/plugins/{pid}{sub_prefix}",
                        dependencies=deps,
                    )
            if "pwa_views" in granted and loaded.source_dir is not None:
                p_static = loaded.source_dir / "static"
                if p_static.is_dir():
                    router.add_api_route(
                        f"/api/plugins/{pid}/static/{{asset_path:path}}",
                        _make_plugin_static_handler(p_static),
                        methods=["GET"],
                        name=f"plugin_static_{pid}",
                    )

    _mount_pwa_shell(
        base_path,
        mode="chat",
        title="AGY Tasks",
        short_name="AGY",
        description="Mobile AI chat, repo coding triage, and homelab ops.",
    )
    prefix_to_mode[base_path] = "chat"
    app.include_router(router, prefix=base_path)

    if gateway.plugin_registry is not None:
        for pid, loaded in gateway.plugin_registry.plugins.items():
            if "pwa_views" not in loaded.context.granted_permissions:
                continue
            for view in loaded.manifest.pwa_views:
                view_prefix = "/" + view.mount_path.strip("/") if view.mount_path.strip("/") else ""
                if (
                    not view.standalone_pwa
                    or not view_prefix
                    or view_prefix == base_path
                    or view_prefix in mounted_standalone_paths
                ):
                    continue
                rel_script = view.script_path.removeprefix("static/") if view.script_path else ""
                view_script_url = (
                    f"{view_prefix}/api/plugins/{pid}/static/{rel_script}" if rel_script else ""
                )
                _mount_pwa_shell(
                    view_prefix,
                    mode=view.id,
                    title=view.title,
                    short_name=view.short_name,
                    description=loaded.manifest.description or view.title,
                    view_script_url=view_script_url,
                )
                prefix_to_mode[view_prefix] = view.id
                app.include_router(router, prefix=view_prefix)
                mounted_standalone_paths.add(view_prefix)

    if (
        health_base_path
        and health_base_path != base_path
        and health_base_path not in mounted_standalone_paths
        and gateway.food_task is not None
    ):
        _mount_pwa_shell(
            health_base_path,
            mode="health",
            title="AGY Health",
            short_name="Health",
            description="Food macro tracker, Garmin readiness, weight trends, and AI nutrition coach.",
        )
        prefix_to_mode[health_base_path] = "health"
        app.include_router(router, prefix=health_base_path)

    return app
