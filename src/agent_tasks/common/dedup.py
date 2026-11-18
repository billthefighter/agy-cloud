from __future__ import annotations

import hashlib
import re
from datetime import UTC, datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from agent_tasks.common.models import SanitizedSentryBundle
    from agent_tasks.tasks.k8s_watchdog.inspector import ArgoAnomaly, PodAnomaly

_NON_ALNUM_RE = re.compile(r"[^a-z0-9./_-]+")
_HEX_OR_NUM_TOKEN_RE = re.compile(r"\b(?:0x[0-9a-f]+|[0-9a-f]{8,}|\d{3,})\b", re.IGNORECASE)
_DURATION_PART_RE = re.compile(r"(\d+)\s*([smhdw])", re.IGNORECASE)


def _norm_token(value: str, max_len: int = 80) -> str:
    lowered = _HEX_OR_NUM_TOKEN_RE.sub("#", (value or "").strip().lower())
    collapsed = _NON_ALNUM_RE.sub("-", lowered).strip("-./_")
    return collapsed[:max_len] or "unknown"


def _norm_filepath(path: str) -> str:
    clean = (path or "").strip().replace("\\", "/")
    while clean.startswith(("./", "/")):
        clean = clean.lstrip("./")
    for prefix in ("work/", "app/", "src/"):
        clean = clean.removeprefix(prefix)
    return _norm_token(clean, max_len=120)


def compute_sentry_dedup_key(github_repo: str, bundle: SanitizedSentryBundle) -> str:
    """Build a cross-environment/cross-project canonical dedup key for a Sentry issue.

    Collapses identical bugs across staging/prod projects (e.g. `python-fastapi` and
    `python-fastapi-staging`) or split Sentry issue IDs that share the same exception
    type and innermost in-app stack frame.
    """
    repo_norm = _norm_token(github_repo, max_len=80)
    if bundle.exceptions:
        inner_exc = bundle.exceptions[-1]
        exc_type = _norm_token(inner_exc.exc_type or bundle.title.split(":", 1)[0], max_len=60)
        in_app_frames = [f for f in inner_exc.frames if f.in_app and f.filename]
        chosen_frame = (
            in_app_frames[-1]
            if in_app_frames
            else (inner_exc.frames[-1] if inner_exc.frames else None)
        )
        if chosen_frame is not None and (chosen_frame.filename or chosen_frame.function):
            file_part = _norm_filepath(chosen_frame.filename)
            func_part = _norm_token(chosen_frame.function, max_len=60)
            return f"sentry:{repo_norm}:{exc_type}:{file_part}:{func_part}"

    culprit_part = _norm_filepath(bundle.culprit) if bundle.culprit else "noculprit"
    title_head = _norm_token(bundle.title.split(":", 1)[0], max_len=60)
    title_digest = hashlib.sha1(
        _HEX_OR_NUM_TOKEN_RE.sub("#", bundle.title.lower()).encode("utf-8")
    ).hexdigest()[:8]
    return f"sentry:{repo_norm}:{culprit_part}:{title_head}:{title_digest}"


def compute_ci_dedup_key(
    github_repo: str,
    head_branch: str,
    workflow_name: str,
    failed_job_names: list[str],
) -> str:
    """Build a canonical dedup key for a failing GitHub Actions workflow on a branch.

    Repeated commits or re-runs on the same `(repo, branch, workflow, failed_jobs)`
    map to one canonical key instead of spawning a new K8s worker per `run_id`.
    """
    repo_norm = _norm_token(github_repo, max_len=80)
    branch_norm = _norm_token(head_branch or "main", max_len=80)
    wf_norm = _norm_token(workflow_name or "ci", max_len=60)
    norm_jobs = sorted({_norm_token(j, max_len=60) for j in failed_job_names if j.strip()})
    jobs_sig = (
        hashlib.sha1("|".join(norm_jobs).encode("utf-8")).hexdigest()[:8] if norm_jobs else "any"
    )
    return f"ci:{repo_norm}:{branch_norm}:{wf_norm}:{jobs_sig}"


def compute_k8s_dedup_key(anomaly: PodAnomaly | ArgoAnomaly) -> str:
    """Build an image-agnostic canonical dedup key for a Kubernetes or Argo CD anomaly.

    Excludes `:image` tags so rolling out successive `:sha-...` builds that hit the
    same underlying CrashLoopBackOff/config failure will not re-triage repeatedly.
    """
    # Avoid top-level runtime cycle with k8s_watchdog.inspector
    if hasattr(anomaly, "owner_name") and hasattr(anomaly, "namespace"):
        ns_norm = _norm_token(str(anomaly.namespace), max_len=40)
        owner_norm = _norm_token(str(anomaly.owner_name), max_len=60)
        reason_norm = _norm_token(str(anomaly.reason), max_len=40)
        return f"k8s:{ns_norm}:{owner_norm}:{reason_norm}"

    app_norm = _norm_token(str(getattr(anomaly, "app_name", "app")), max_len=60)
    health = str(getattr(anomaly, "health_status", "") or "")
    reason_tag = "degraded" if health == "Degraded" else "outofsync"
    return f"argo:{app_norm}:{reason_tag}"


def parse_iso_utc(raw: str | None) -> datetime | None:
    """Parse an ISO-8601 timestamp (including trailing `Z`) into a timezone-aware UTC datetime."""
    if not raw or not raw.strip():
        return None
    clean = raw.strip()
    if clean.endswith("Z"):
        clean = clean[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(clean)
    except ValueError:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def parse_duration_minutes(spec: str) -> int | None:
    """Parse human duration tokens like `30m`, `2h`, `1d`, `1h30m`, or bare minutes `45`."""
    clean = (spec or "").strip().lower()
    if not clean:
        return None
    if clean.isdigit():
        mins = int(clean)
        return mins if 1 <= mins <= 10080 else None

    pos = 0
    total_seconds = 0
    for match in _DURATION_PART_RE.finditer(clean):
        if clean[pos : match.start()].strip():
            return None
        amount = int(match.group(1))
        unit = match.group(2).lower()
        mult = {"s": 1, "m": 60, "h": 3600, "d": 86400, "w": 604800}[unit]
        total_seconds += amount * mult
        pos = match.end()

    if pos == 0 or clean[pos:].strip():
        return None
    mins = max(1, round(total_seconds / 60))
    return min(mins, 10080)
