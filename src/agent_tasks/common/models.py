from __future__ import annotations

import mimetypes
from enum import StrEnum
from pathlib import Path
from typing import Any

import pydantic

_IMAGE_MIME_BY_SUFFIX: dict[str, str] = {
    ".webp": "image/webp",
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".png": "image/png",
    ".bmp": "image/bmp",
}
mimetypes.add_type("image/webp", ".webp")


def resolve_image_mime(path: Path) -> str:
    """Resolve an image MIME type reliably even on minimal Linux images without `/etc/mime.types`."""
    suffix = path.suffix.lower()
    if suffix in _IMAGE_MIME_BY_SUFFIX:
        return _IMAGE_MIME_BY_SUFFIX[suffix]
    guessed, _ = mimetypes.guess_type(str(path))
    if guessed and guessed.startswith("image/"):
        return guessed
    raise ValueError(f"Could not infer a valid image MIME type for '{path.name}'.")


def load_sdk_image(path: Path) -> Any:
    """Load a local image file into `google.antigravity.types.Image` with reliable MIME detection."""
    from google.antigravity.types import Image

    return Image(data=path.read_bytes(), mime_type=resolve_image_mime(path))


class IssueStatus(StrEnum):
    QUEUED = "queued"
    RUNNING = "running"
    AWAITING_REVIEW = "awaiting_review"
    SNOOZED = "snoozed"
    SILENT = "silent"
    PR_OPENED = "pr_opened"
    RESOLVED = "resolved"
    IGNORED = "ignored"
    FAILED = "failed"


class StackFrameSummary(pydantic.BaseModel):
    filename: str = ""
    function: str = ""
    lineno: int | None = None
    in_app: bool = False
    context_line: str = ""


class ExceptionSummary(pydantic.BaseModel):
    exc_type: str = ""
    exc_value: str = ""
    frames: list[StackFrameSummary] = pydantic.Field(default_factory=list)


class BreadcrumbSummary(pydantic.BaseModel):
    timestamp: str = ""
    category: str = ""
    level: str = ""
    message: str = ""


class SanitizedSentryBundle(pydantic.BaseModel):
    """Deterministic, size-bounded Sentry issue context safe to mount into a worker Job.

    Strips cookies, request headers, user PII, and raw payloads before LLM ingestion.
    """

    issue_id: str
    short_id: str
    project_slug: str
    title: str
    culprit: str = ""
    permalink: str = ""
    first_seen: str = ""
    last_seen: str = ""
    event_id: str = ""
    release: str = ""
    environment: str = ""
    tags: dict[str, str] = pydantic.Field(default_factory=dict)
    exceptions: list[ExceptionSummary] = pydantic.Field(default_factory=list)
    breadcrumbs: list[BreadcrumbSummary] = pydantic.Field(default_factory=list)


class TriageReport(pydantic.BaseModel):
    """Structured output enforced via google-antigravity LocalAgentConfig.response_schema."""

    root_cause: str = pydantic.Field(
        description="Concise technical explanation of what caused the Sentry issue."
    )
    confidence: str = pydantic.Field(description="Confidence level: high, medium, or low.")
    affected_files: list[str] = pydantic.Field(
        default_factory=list,
        description="Relative file paths modified or implicated.",
    )
    fix_summary: str = pydantic.Field(
        description="Summary of the proposed code change (or why no code fix should be applied)."
    )
    verification_commands: list[str] = pydantic.Field(
        default_factory=list,
        description="Exact lint/typecheck/test commands executed inside /work.",
    )
    tests_passed: bool = pydantic.Field(
        description="True if verification tests passed cleanly against the proposed patch."
    )
    risk_notes: str = pydantic.Field(
        default="",
        description="Any migration, rollout, or edge-case caveats the reviewer should check.",
    )
    should_notify: bool = pydantic.Field(
        default=True,
        description=(
            "Set False ONLY on background/watchdog runs when no code change is needed and "
            "the signal matches known transient noise requiring zero human action."
        ),
    )
    followup_in_minutes: int | None = pydantic.Field(
        default=None,
        description=(
            "Optional self-scheduled wake-up delay in minutes (1-10080) when the agent should "
            "re-check cluster/CI/Sentry state after a rollout, migration, or backoff window."
        ),
    )
    followup_reason: str = pydantic.Field(
        default="",
        description="Concise check to perform when woken up by `followup_in_minutes`.",
    )
    proposed_memory_note: str = pydantic.Field(
        default="",
        description=(
            "Optional concise reusable repository lesson or verification quirk discovered "
            "during this run, proposed for human-approved persistence to `/memory`."
        ),
    )


class TokenUsageSummary(pydantic.BaseModel):
    """Cumulative token counts captured from `agent.conversation.total_usage`."""

    prompt_tokens: int = 0
    cached_tokens: int = 0
    output_tokens: int = 0
    thoughts_tokens: int = 0
    total_tokens: int = 0


class WorkerEnvelope(pydantic.BaseModel):
    """Written by worker_main.py to /state/artifacts/<issue_id>/result.json."""

    issue_id: str
    turn: int
    conversation_id: str
    model: str = ""
    worker_backend: str = "sdk"
    stop_reason: str = "UNSPECIFIED"
    has_patch: bool = False
    patch_stats: str = ""
    report: TriageReport | None = None
    usage: TokenUsageSummary = pydantic.Field(default_factory=TokenUsageSummary)
    error: str | None = None
