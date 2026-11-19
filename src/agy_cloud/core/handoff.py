from __future__ import annotations

import hashlib
import json
import secrets
import sqlite3
import time
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from agy_cloud.sdk.spec import ChannelBusProtocol, JobSpec, WorkerJobServiceProtocol

TERMINAL_HANDOFF_STATUSES = {"awaiting_review", "completed", "failed"}


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


@dataclass(slots=True)
class HandoffRecord:
    handoff_id: str
    short_id: str
    github_repo: str
    base_branch: str
    base_sha: str
    prompt: str
    semantic_state: str
    verify_hint: str
    focus_subdir: str
    model: str
    status: str
    turn: int
    local_conversation_id: str | None = None
    remote_conversation_id: str | None = None
    workspace_path: str = ""
    has_initial_patch: bool = False
    has_result_patch: bool = False
    patch_stats: str = ""
    report: dict[str, Any] = field(default_factory=dict)
    last_error: str | None = None
    pulled_at: str | None = None
    created_at: str = ""
    updated_at: str = ""


class HandoffCreateRequest(BaseModel):
    github_repo: str
    prompt: str
    base_branch: str = "main"
    base_sha: str = ""
    patch: str = ""
    semantic_state: str = ""
    verify_hint: str = ""
    focus_subdir: str = ""
    model: str = "gemini-4-hydrogen"
    worker_backend: str = "agy_cli"
    local_conversation_id: str | None = None
    workspace_path: str = ""


class HandoffReplyRequest(BaseModel):
    feedback: str
    model: str = ""


class HandoffCompleteRequest(BaseModel):
    status: str = "awaiting_review"
    remote_conversation_id: str | None = None
    patch: str = ""
    patch_stats: str = ""
    report: dict[str, Any] = Field(default_factory=dict)
    error: str | None = None


class HandoffStore:
    """SQLite persistence for laptop <-> k3s session handoffs."""

    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL;")
        return conn

    def _init_schema(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS handoffs (
                    handoff_id TEXT PRIMARY KEY,
                    short_id TEXT NOT NULL,
                    github_repo TEXT NOT NULL,
                    base_branch TEXT NOT NULL DEFAULT 'main',
                    base_sha TEXT NOT NULL DEFAULT '',
                    prompt TEXT NOT NULL,
                    semantic_state TEXT NOT NULL DEFAULT '',
                    verify_hint TEXT NOT NULL DEFAULT '',
                    focus_subdir TEXT NOT NULL DEFAULT '',
                    model TEXT NOT NULL DEFAULT 'gemini-4-hydrogen',
                    status TEXT NOT NULL DEFAULT 'queued',
                    turn INTEGER NOT NULL DEFAULT 1,
                    local_conversation_id TEXT,
                    remote_conversation_id TEXT,
                    workspace_path TEXT NOT NULL DEFAULT '',
                    has_initial_patch INTEGER NOT NULL DEFAULT 0,
                    has_result_patch INTEGER NOT NULL DEFAULT 0,
                    patch_stats TEXT NOT NULL DEFAULT '',
                    report_json TEXT NOT NULL DEFAULT '{}',
                    last_error TEXT,
                    pulled_at TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                """
            )

    def _row_to_record(self, row: sqlite3.Row) -> HandoffRecord:
        return HandoffRecord(
            handoff_id=str(row["handoff_id"]),
            short_id=str(row["short_id"]),
            github_repo=str(row["github_repo"]),
            base_branch=str(row["base_branch"]),
            base_sha=str(row["base_sha"]),
            prompt=str(row["prompt"]),
            semantic_state=str(row["semantic_state"]),
            verify_hint=str(row["verify_hint"]),
            focus_subdir=str(row["focus_subdir"]),
            model=str(row["model"]),
            status=str(row["status"]),
            turn=int(row["turn"]),
            local_conversation_id=row["local_conversation_id"],
            remote_conversation_id=row["remote_conversation_id"],
            workspace_path=str(row["workspace_path"]),
            has_initial_patch=bool(row["has_initial_patch"]),
            has_result_patch=bool(row["has_result_patch"]),
            patch_stats=str(row["patch_stats"]),
            report=json.loads(str(row["report_json"] or "{}")),
            last_error=row["last_error"],
            pulled_at=row["pulled_at"],
            created_at=str(row["created_at"]),
            updated_at=str(row["updated_at"]),
        )

    def create(self, rec: HandoffRecord) -> HandoffRecord:
        now = _now_iso()
        rec.created_at = rec.created_at or now
        rec.updated_at = now
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO handoffs (
                    handoff_id, short_id, github_repo, base_branch, base_sha,
                    prompt, semantic_state, verify_hint, focus_subdir, model,
                    status, turn, local_conversation_id, remote_conversation_id,
                    workspace_path, has_initial_patch, has_result_patch, patch_stats,
                    report_json, last_error, pulled_at, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    rec.handoff_id,
                    rec.short_id,
                    rec.github_repo,
                    rec.base_branch,
                    rec.base_sha,
                    rec.prompt,
                    rec.semantic_state,
                    rec.verify_hint,
                    rec.focus_subdir,
                    rec.model,
                    rec.status,
                    rec.turn,
                    rec.local_conversation_id,
                    rec.remote_conversation_id,
                    rec.workspace_path,
                    int(rec.has_initial_patch),
                    int(rec.has_result_patch),
                    rec.patch_stats,
                    json.dumps(rec.report),
                    rec.last_error,
                    rec.pulled_at,
                    rec.created_at,
                    rec.updated_at,
                ),
            )
        return rec

    def get(self, handoff_id: str) -> HandoffRecord | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM handoffs WHERE handoff_id = ? OR short_id = ?",
                (handoff_id, handoff_id.upper()),
            ).fetchone()
            return self._row_to_record(row) if row else None

    def get_latest(
        self,
        *,
        github_repo: str = "",
        only_unpulled: bool = False,
    ) -> HandoffRecord | None:
        clauses: list[str] = []
        params: list[Any] = []
        if github_repo:
            clauses.append("github_repo = ?")
            params.append(github_repo)
        if only_unpulled:
            clauses.append("pulled_at IS NULL")
        where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        with self._connect() as conn:
            row = conn.execute(
                f"SELECT * FROM handoffs {where_sql} ORDER BY created_at DESC LIMIT 1",
                tuple(params),
            ).fetchone()
            return self._row_to_record(row) if row else None

    def list_recent(self, limit: int = 20) -> list[HandoffRecord]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM handoffs ORDER BY created_at DESC LIMIT ?",
                (limit,),
            ).fetchall()
            return [self._row_to_record(r) for r in rows]

    def update_status(
        self,
        handoff_id: str,
        *,
        status: str,
        remote_conversation_id: str | None = None,
        has_result_patch: bool | None = None,
        patch_stats: str | None = None,
        report: dict[str, Any] | None = None,
        last_error: str | None = None,
        turn: int | None = None,
        model: str | None = None,
    ) -> HandoffRecord:
        existing = self.get(handoff_id)
        if existing is None:
            raise KeyError(f"Handoff `{handoff_id}` not found.")
        now = _now_iso()
        new_remote_conv = (
            remote_conversation_id
            if remote_conversation_id is not None
            else existing.remote_conversation_id
        )
        new_has_patch = (
            has_result_patch if has_result_patch is not None else existing.has_result_patch
        )
        new_stats = patch_stats if patch_stats is not None else existing.patch_stats
        new_report = report if report is not None else existing.report
        new_err = last_error if last_error is not None else existing.last_error
        new_turn = turn if turn is not None else existing.turn
        new_model = model if model is not None else existing.model

        with self._connect() as conn:
            conn.execute(
                """
                UPDATE handoffs
                SET status = ?, remote_conversation_id = ?, has_result_patch = ?,
                    patch_stats = ?, report_json = ?, last_error = ?, turn = ?,
                    model = ?, updated_at = ?
                WHERE handoff_id = ?
                """,
                (
                    status,
                    new_remote_conv,
                    int(new_has_patch),
                    new_stats,
                    json.dumps(new_report),
                    new_err,
                    new_turn,
                    new_model,
                    now,
                    existing.handoff_id,
                ),
            )
        updated = self.get(existing.handoff_id)
        assert updated is not None
        return updated

    def mark_pulled(self, handoff_id: str) -> HandoffRecord:
        existing = self.get(handoff_id)
        if existing is None:
            raise KeyError(f"Handoff `{handoff_id}` not found.")
        now = _now_iso()
        with self._connect() as conn:
            conn.execute(
                "UPDATE handoffs SET pulled_at = ?, status = 'completed', updated_at = ? WHERE handoff_id = ?",
                (now, now, existing.handoff_id),
            )
        updated = self.get(existing.handoff_id)
        assert updated is not None
        return updated


def build_handoff_task_prompt(req: HandoffCreateRequest, short_id: str) -> str:
    """Format `/artifacts/<handoff_id>/task_prompt.md` for the remote worker pod."""
    lines = [
        f"# Cloud Handoff Task `{short_id}`",
        "",
        f"- **Repository:** `{req.github_repo}` (branch `{req.base_branch}`"
        + (f", base SHA `{req.base_sha[:12]}`)" if req.base_sha else ")"),
    ]
    if req.focus_subdir:
        lines.append(f"- **Focus Subdirectory:** `/work/{req.focus_subdir}`")
    if req.verify_hint:
        lines.append(f"- **Verification Command:** `{req.verify_hint}`")
    if req.patch.strip():
        lines.append(
            "- **Pre-Applied Working Tree Diff:** The user's uncommitted laptop diff "
            "(`initial.patch`) has already been applied to `/work`."
        )

    if req.semantic_state.strip():
        lines.extend(
            [
                "",
                "## Local Session Semantic State ($s_t$)",
                "",
                "<semantic_state>",
                req.semantic_state.strip(),
                "</semantic_state>",
            ]
        )

    lines.extend(
        [
            "",
            "## Handoff Instructions",
            "",
            "<handoff_instructions>",
            req.prompt.strip(),
            "</handoff_instructions>",
            "",
            "## Execution Rules",
            "",
            "1. Continue the task directly from the pre-applied state in `/work`.",
            "2. Run relevant linters, typecheckers, and unit tests before finishing.",
            "3. Leave all changes uncommitted (or staged) in `/work` so `git diff HEAD` captures the complete combined patch for the user's laptop to pull on wake.",
        ]
    )
    return "\n".join(lines)


class HandoffService:
    """Orchestrates laptop-to-k3s session handoffs, artifacts, and poll-on-wake pulls."""

    def __init__(
        self,
        *,
        state_root: Path,
        store: HandoffStore,
        channels: ChannelBusProtocol,
        jobs: WorkerJobServiceProtocol,
    ) -> None:
        self.state_root = state_root
        self.store = store
        self.channels = channels
        self.jobs = jobs

    def artifact_dir(self, handoff_id: str) -> Path:
        path = self.state_root / "artifacts" / handoff_id
        path.mkdir(parents=True, exist_ok=True)
        return path

    async def create_handoff(self, req: HandoffCreateRequest) -> HandoffRecord:
        ts_ms = int(time.time() * 1000)
        digest = (
            hashlib.sha1(f"{ts_ms}:{req.github_repo}:{req.prompt}:{secrets.token_hex(4)}".encode())
            .hexdigest()[:6]
            .upper()
        )
        short_id = f"HANDOFF-{digest}"
        handoff_id = f"handoff-{ts_ms}-{digest.lower()}"

        art_dir = self.artifact_dir(handoff_id)
        prompt_md = build_handoff_task_prompt(req, short_id)
        (art_dir / "task_prompt.md").write_text(prompt_md, encoding="utf-8")

        has_initial_patch = bool(req.patch.strip())
        if has_initial_patch:
            # Write both initial.patch and fix.patch so turn-1 initContainer can apply it
            (art_dir / "initial.patch").write_text(req.patch, encoding="utf-8")
            (art_dir / "fix.patch").write_text(req.patch, encoding="utf-8")

        rec = HandoffRecord(
            handoff_id=handoff_id,
            short_id=short_id,
            github_repo=req.github_repo,
            base_branch=req.base_branch,
            base_sha=req.base_sha,
            prompt=req.prompt,
            semantic_state=req.semantic_state,
            verify_hint=req.verify_hint,
            focus_subdir=req.focus_subdir,
            model=req.model or "gemini-4-hydrogen",
            status="running",
            turn=1,
            local_conversation_id=req.local_conversation_id,
            workspace_path=req.workspace_path,
            has_initial_patch=has_initial_patch,
        )
        self.store.create(rec)

        handoff_backend = (
            "sdk" if (req.worker_backend or "").strip().lower() in {"sdk", "api_key", "api"} else "agy_cli"
        )
        await self.jobs.spawn_agent_job(
            JobSpec(
                task_id=handoff_id,
                short_id=short_id,
                plugin_id="core.handoff",
                github_repo=req.github_repo,
                base_branch=req.base_branch,
                base_sha=req.base_sha or None,
                initial_patch=req.patch if has_initial_patch else None,
                focus_subdir=req.focus_subdir,
                verify_hint=req.verify_hint,
                prompt=prompt_md,
                model=rec.model,
                worker_backend=handoff_backend,
                turn=1,
            )
        )
        await self.channels.post_message(
            f"🚗 *Cloud Handoff Started:* `{short_id}` (`{req.github_repo}`, model `{rec.model}`)\n"
            f"> {req.prompt[:200]}",
            role="coding",
        )
        return rec

    def serialize_handoff(
        self, rec: HandoffRecord, *, include_patch: bool = True
    ) -> dict[str, Any]:
        data = asdict(rec)
        art_dir = self.artifact_dir(rec.handoff_id)
        patch_file = art_dir / "fix.patch"
        if include_patch and patch_file.is_file():
            data["patch"] = patch_file.read_text(encoding="utf-8")
        else:
            data["patch"] = ""
        progress_file = art_dir / "progress.jsonl"
        progress_steps: list[dict[str, Any]] = []
        if progress_file.is_file():
            for raw_line in progress_file.read_text(encoding="utf-8", errors="replace").splitlines():
                line = raw_line.strip()
                if not line:
                    continue
                try:
                    item = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if isinstance(item, dict):
                    progress_steps.append(item)
        data["progress_steps"] = progress_steps[-20:]
        data["is_terminal"] = rec.status in TERMINAL_HANDOFF_STATUSES
        return data

    async def complete_turn(self, handoff_id: str, body: HandoffCompleteRequest) -> HandoffRecord:
        rec = self.store.get(handoff_id)
        if rec is None:
            raise KeyError(f"Handoff `{handoff_id}` not found.")
        art_dir = self.artifact_dir(rec.handoff_id)
        has_patch = bool(body.patch.strip())
        if has_patch:
            (art_dir / "fix.patch").write_text(body.patch, encoding="utf-8")

        updated = self.store.update_status(
            rec.handoff_id,
            status=body.status,
            remote_conversation_id=body.remote_conversation_id,
            has_result_patch=has_patch or rec.has_result_patch,
            patch_stats=body.patch_stats,
            report=body.report,
            last_error=body.error,
        )
        await self.channels.broadcast_ws(
            "handoff.updated",
            {"handoff": self.serialize_handoff(updated, include_patch=False)},
        )
        return updated

    async def reply_turn(self, handoff_id: str, body: HandoffReplyRequest) -> HandoffRecord:
        rec = self.store.get(handoff_id)
        if rec is None:
            raise KeyError(f"Handoff `{handoff_id}` not found.")
        next_turn = rec.turn + 1
        new_model = body.model.strip() or rec.model
        art_dir = self.artifact_dir(rec.handoff_id)
        (art_dir / f"feedback-turn-{next_turn}.txt").write_text(
            body.feedback.strip(), encoding="utf-8"
        )
        updated = self.store.update_status(
            rec.handoff_id,
            status="running",
            turn=next_turn,
            model=new_model,
        )
        await self.jobs.spawn_agent_job(
            JobSpec(
                task_id=rec.handoff_id,
                short_id=rec.short_id,
                plugin_id="core.handoff",
                github_repo=rec.github_repo,
                base_branch=rec.base_branch,
                base_sha=rec.base_sha or None,
                focus_subdir=rec.focus_subdir,
                verify_hint=rec.verify_hint,
                prompt=body.feedback.strip(),
                model=new_model,
                worker_backend="agy_cli",
                turn=next_turn,
                conversation_id=rec.remote_conversation_id,
            )
        )
        return updated


def create_handoff_router(service: HandoffService) -> APIRouter:
    router = APIRouter(prefix="/api/handoff", tags=["handoff"])

    @router.post("")
    async def create_handoff_endpoint(body: HandoffCreateRequest) -> dict[str, Any]:
        rec = await service.create_handoff(body)
        return service.serialize_handoff(rec, include_patch=False)

    @router.get("")
    async def list_handoffs_endpoint(
        limit: int = Query(default=20, ge=1, le=100),
    ) -> dict[str, Any]:
        items = [
            service.serialize_handoff(r, include_patch=False)
            for r in service.store.list_recent(limit=limit)
        ]
        return {"handoffs": items}

    @router.get("/latest")
    async def get_latest_handoff_endpoint(
        repo: str = Query(default=""),
        only_unpulled: bool = Query(default=False),
        include_patch: bool = Query(default=True),
    ) -> dict[str, Any]:
        rec = service.store.get_latest(github_repo=repo.strip(), only_unpulled=only_unpulled)
        if rec is None:
            raise HTTPException(status_code=404, detail="No matching handoff found.")
        return service.serialize_handoff(rec, include_patch=include_patch)

    @router.get("/{handoff_id}")
    async def get_handoff_endpoint(
        handoff_id: str,
        include_patch: bool = Query(default=True),
    ) -> dict[str, Any]:
        rec = service.store.get(handoff_id)
        if rec is None:
            raise HTTPException(status_code=404, detail=f"Handoff `{handoff_id}` not found.")
        return service.serialize_handoff(rec, include_patch=include_patch)

    @router.post("/{handoff_id}/complete")
    async def complete_handoff_endpoint(
        handoff_id: str, body: HandoffCompleteRequest
    ) -> dict[str, Any]:
        try:
            rec = await service.complete_turn(handoff_id, body)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return service.serialize_handoff(rec, include_patch=True)

    @router.post("/{handoff_id}/reply")
    async def reply_handoff_endpoint(handoff_id: str, body: HandoffReplyRequest) -> dict[str, Any]:
        try:
            rec = await service.reply_turn(handoff_id, body)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return service.serialize_handoff(rec, include_patch=False)

    @router.post("/{handoff_id}/mark-pulled")
    async def mark_pulled_endpoint(handoff_id: str) -> dict[str, Any]:
        try:
            rec = service.store.mark_pulled(handoff_id)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        await service.channels.post_message(
            f"💻 *Handoff Pulled to Laptop:* `{rec.short_id}` (`{rec.github_repo}`)",
            role="coding",
        )
        return service.serialize_handoff(rec, include_patch=False)

    return router
