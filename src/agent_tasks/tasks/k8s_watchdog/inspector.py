from __future__ import annotations

import asyncio
import hashlib
import re
from collections.abc import Collection, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

import structlog
from kubernetes import client, config
from kubernetes.client.rest import ApiException

log = structlog.get_logger(__name__)

CRASH_WAITING_REASONS = frozenset(
    {
        "CrashLoopBackOff",
        "ImagePullBackOff",
        "ErrImagePull",
        "CreateContainerConfigError",
    }
)
_STATIC_OWNER_KINDS = frozenset(
    {"Deployment", "StatefulSet", "DaemonSet", "Job", "CronJob", "Node"}
)
_RS_HASH_SUFFIX_RE = re.compile(
    r"-(?:(?=[a-z0-9]*\d)[a-z0-9]{5,12}|[bcdfghjklmnpqrstvwxz2456789]{7,10}|[0-9a-f]{7,12})$"
)

_DSN_CREDENTIAL_RE = re.compile(r"(://[^:\s/@]*:)([^@\s/]+)(@)")
_BEARER_TOKEN_RE = re.compile(r"\bBearer\s+[^\s\"'`,;)}\]<>]+", re.IGNORECASE)
_SLACK_TOKEN_RE = re.compile(r"\bxox[abp]-[0-9A-Za-z_-]+")
_SENTRY_TOKEN_RE = re.compile(r"\bsntryu_[0-9A-Za-z_-]+")
_GITHUB_TOKEN_RE = re.compile(r"\bgh[pousr]_[0-9A-Za-z_-]+")
_AIZA_KEY_RE = re.compile(r"\bAIza[0-9A-Za-z_-]+")
_AQ_KEY_RE = re.compile(r"\bAQ\.[0-9A-Za-z_.-]*[0-9A-Za-z_-]")

_MAX_LOG_CHARS = 6000
_MAX_EVENT_LINES = 15
_MAX_EVENT_CHARS = 2000


@dataclass(slots=True)
class PodAnomaly:
    namespace: str
    pod_name: str
    owner_name: str
    container_name: str
    reason: str
    restarts: int
    image: str
    fingerprint: str
    message: str = ""


@dataclass(slots=True)
class ArgoAnomaly:
    app_name: str
    sync_status: str
    health_status: str
    message: str
    fingerprint: str


@dataclass(slots=True)
class ClusterHealthSnapshot:
    checked_at: str
    namespace_pod_counts: dict[str, tuple[int, int]] = field(default_factory=dict)
    pod_anomalies: list[PodAnomaly] = field(default_factory=list)
    argo_anomalies: list[ArgoAnomaly] = field(default_factory=list)
    argo_apps_summary: list[tuple[str, str, str]] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)


def redact_secrets(text: str) -> str:
    """Scrub common bearer tokens, API keys, and DSN credentials from diagnostic text."""
    if not text:
        return ""
    scrubbed = _DSN_CREDENTIAL_RE.sub(r"\1***\3", text)
    scrubbed = _BEARER_TOKEN_RE.sub("Bearer ***", scrubbed)
    scrubbed = _SLACK_TOKEN_RE.sub("***", scrubbed)
    scrubbed = _SENTRY_TOKEN_RE.sub("***", scrubbed)
    scrubbed = _GITHUB_TOKEN_RE.sub("***", scrubbed)
    scrubbed = _AIZA_KEY_RE.sub("***", scrubbed)
    scrubbed = _AQ_KEY_RE.sub("***", scrubbed)
    return scrubbed


def _field(obj: Any, *names: str, default: Any = None) -> Any:
    if obj is None:
        return default
    if isinstance(obj, Mapping):
        for name in names:
            if name in obj and obj[name] is not None:
                return obj[name]
        return default
    for name in names:
        val = getattr(obj, name, None)
        if val is not None:
            return val
    return default


def _slug(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-") or "unknown"


def _clip(text: str, limit: int) -> str:
    if len(text) <= limit:
        return text
    return text[: limit - 1] + "…"


def _resolve_owner_name(metadata: Any, pod_name: str) -> str:
    refs = _field(metadata, "owner_references", "ownerReferences", default=[]) or []
    if not refs:
        return pod_name

    first = refs[0]
    raw_name = str(_field(first, "name", default="") or "").strip()
    if not raw_name:
        return pod_name

    kind = str(_field(first, "kind", default="") or "").strip()
    if kind in _STATIC_OWNER_KINDS:
        return raw_name

    stripped = _RS_HASH_SUFFIX_RE.sub("", raw_name).strip("-")
    return stripped or raw_name


def _build_fingerprint(
    *,
    prefix: str,
    scope_slug: str,
    target_slug: str,
    reason_slug: str,
    digest_input: str,
) -> str:
    short_hash = hashlib.sha1(digest_input.encode("utf-8")).hexdigest()[:8]
    # Keep total length <= 48 while always retaining prefix and trailing `-{short_hash}`.
    # Fixed structure: "{prefix}-{scope}-{target}-{reason}-{short_hash}"
    fixed_len = len(prefix) + 4 + len(short_hash)
    budget = 48 - fixed_len

    if len(scope_slug) + len(target_slug) + len(reason_slug) > budget:
        scope_slug = scope_slug[:10].strip("-") or "ns"
        reason_slug = reason_slug[:16].strip("-") or "err"
        target_budget = budget - len(scope_slug) - len(reason_slug)
        if target_budget < 4:
            reason_slug = reason_slug[: max(6, budget - len(scope_slug) - 4)].strip("-") or "err"
            target_budget = budget - len(scope_slug) - len(reason_slug)
        target_slug = target_slug[: max(1, target_budget)].strip("-") or "app"

    return f"{prefix}-{scope_slug}-{target_slug}-{reason_slug}-{short_hash}"[:48]


def _is_pod_ready_or_running(pod: Any) -> bool:
    status = _field(pod, "status")
    phase = str(_field(status, "phase", default="") or "")
    if phase == "Succeeded":
        return True
    if phase != "Running":
        return False

    conditions = _field(status, "conditions", default=[]) or []
    for cond in conditions:
        if _field(cond, "type") == "Ready":
            return str(_field(cond, "status", default="")) == "True"

    container_statuses = _field(status, "container_statuses", "containerStatuses", default=[]) or []
    if container_statuses:
        return all(bool(_field(cs, "ready", default=False)) for cs in container_statuses)

    return True


def detect_pod_anomalies(pods: list[Any], namespace: str) -> list[PodAnomaly]:
    """Inspect a namespace's pods (dicts or V1Pod objects) for crash/pull/OOM anomalies."""
    anomalies_by_fp: dict[str, PodAnomaly] = {}

    for pod in pods:
        metadata = _field(pod, "metadata")
        pod_name = str(_field(metadata, "name", default="") or "")
        labels = _field(metadata, "labels", default={}) or {}
        if (
            isinstance(labels, Mapping)
            and labels.get("app.kubernetes.io/component") == "agy-worker"
        ):
            continue

        status = _field(pod, "status")
        if str(_field(status, "phase", default="") or "") == "Succeeded":
            continue

        owner_name = _resolve_owner_name(metadata, pod_name)
        init_statuses = (
            _field(status, "init_container_statuses", "initContainerStatuses", default=[]) or []
        )
        regular_statuses = (
            _field(status, "container_statuses", "containerStatuses", default=[]) or []
        )

        for cs in [*init_statuses, *regular_statuses]:
            container_name = str(_field(cs, "name", default="") or "unknown")
            image = str(_field(cs, "image", default="") or "")
            restarts = int(_field(cs, "restart_count", "restartCount", default=0) or 0)

            state = _field(cs, "state")
            waiting = _field(state, "waiting")
            waiting_reason = str(_field(waiting, "reason", default="") or "")
            waiting_msg = str(_field(waiting, "message", default="") or "")

            last_state = _field(cs, "last_state", "lastState")
            last_term = _field(last_state, "terminated")
            last_term_reason = str(_field(last_term, "reason", default="") or "")
            last_term_msg = str(_field(last_term, "message", default="") or "")

            if waiting_reason in CRASH_WAITING_REASONS:
                reason = waiting_reason
                raw_msg = waiting_msg
            elif last_term_reason == "OOMKilled" and restarts >= 2:
                reason = "OOMKilled"
                raw_msg = last_term_msg or (
                    f"Container `{container_name}` terminated with OOMKilled ({restarts} restarts)"
                )
            else:
                continue

            fingerprint = _build_fingerprint(
                prefix="k8s",
                scope_slug=_slug(namespace),
                target_slug=_slug(owner_name),
                reason_slug=_slug(reason),
                digest_input=f"{namespace}:{owner_name}:{reason}:{image}",
            )
            anomaly = PodAnomaly(
                namespace=namespace,
                pod_name=pod_name,
                owner_name=owner_name,
                container_name=container_name,
                reason=reason,
                restarts=restarts,
                image=image,
                fingerprint=fingerprint,
                message=_clip(redact_secrets(raw_msg), 500),
            )
            existing = anomalies_by_fp.get(fingerprint)
            if existing is None or anomaly.restarts > existing.restarts:
                anomalies_by_fp[fingerprint] = anomaly

    return list(anomalies_by_fp.values())


def _extract_argo_summary(app: Any) -> tuple[str, str, str]:
    metadata = _field(app, "metadata")
    app_name = str(_field(metadata, "name", default="") or "")
    status = _field(app, "status")
    sync = _field(status, "sync")
    health = _field(status, "health")
    sync_status = str(_field(sync, "status", default="") or "Unknown")
    health_status = str(_field(health, "status", default="") or "Unknown")
    return app_name, sync_status, health_status


def _extract_argo_message(app: Any, app_name: str, sync_status: str, health_status: str) -> str:
    status = _field(app, "status")
    parts: list[str] = []

    health_msg = str(_field(_field(status, "health"), "message", default="") or "").strip()
    if health_msg:
        parts.append(health_msg)

    op_msg = str(
        _field(_field(status, "operation_state", "operationState"), "message", default="") or ""
    ).strip()
    if op_msg and op_msg not in parts:
        parts.append(op_msg)

    for cond in _field(status, "conditions", default=[]) or []:
        cond_type = str(_field(cond, "type", default="") or "").strip()
        cond_msg = str(_field(cond, "message", default="") or "").strip()
        if cond_msg:
            entry = f"{cond_type}: {cond_msg}" if cond_type else cond_msg
            if entry not in parts:
                parts.append(entry)

    combined = (
        "; ".join(parts)
        if parts
        else (f"Argo CD application `{app_name}` is sync={sync_status}, health={health_status}")
    )
    return _clip(redact_secrets(combined), 600)


DEFAULT_ARGO_OUT_OF_SYNC_POLLS = 15
DEFAULT_IGNORED_ARGO_APPS: tuple[str, ...] = ("root", "agent-tasks")


def detect_argo_anomalies(
    apps: list[dict[str, Any]],
    *,
    consecutive_out_of_sync: dict[str, int],
    out_of_sync_polls: int = DEFAULT_ARGO_OUT_OF_SYNC_POLLS,
    update_streaks: bool = True,
    ignored_apps: Collection[str] = DEFAULT_IGNORED_ARGO_APPS,
) -> list[ArgoAnomaly]:
    """Inspect Argo CD Application objects for Degraded health or sustained OutOfSync."""
    anomalies: list[ArgoAnomaly] = []
    min_polls = max(1, out_of_sync_polls)
    ignored = {a.strip() for a in ignored_apps if a.strip()}

    for app in apps:
        app_name, sync_status, health_status = _extract_argo_summary(app)
        if not app_name or app_name in ignored:
            continue

        if update_streaks:
            if sync_status == "OutOfSync":
                consecutive_out_of_sync[app_name] = consecutive_out_of_sync.get(app_name, 0) + 1
            else:
                consecutive_out_of_sync[app_name] = 0
            streak = consecutive_out_of_sync.get(app_name, 0)
        else:
            streak = consecutive_out_of_sync.get(app_name, 0) if sync_status == "OutOfSync" else 0

        is_degraded = health_status == "Degraded"
        is_sustained_out_of_sync = sync_status == "OutOfSync" and streak >= min_polls
        if not (is_degraded or is_sustained_out_of_sync):
            continue

        reason_tag = "degraded" if is_degraded else "outofsync"
        fingerprint = _build_fingerprint(
            prefix="argo",
            scope_slug="argocd",
            target_slug=_slug(app_name),
            reason_slug=reason_tag,
            digest_input=f"argocd:{app_name}:{reason_tag}",
        )
        anomalies.append(
            ArgoAnomaly(
                app_name=app_name,
                sync_status=sync_status,
                health_status=health_status,
                message=_extract_argo_message(app, app_name, sync_status, health_status),
                fingerprint=fingerprint,
            )
        )

    return anomalies


class ClusterHealthInspector:
    """Async Kubernetes & Argo CD health inspector using in-cluster or local kubeconfig."""

    def __init__(
        self,
        *,
        core_v1: client.CoreV1Api | None = None,
        custom_objects: client.CustomObjectsApi | None = None,
    ) -> None:
        if core_v1 is None or custom_objects is None:
            try:
                config.load_incluster_config()
            except config.ConfigException:
                config.load_kube_config()
        self._core_v1 = core_v1 or client.CoreV1Api()
        self._custom_objects = custom_objects or client.CustomObjectsApi()
        self._consecutive_out_of_sync: dict[str, int] = {}

    async def inspect(
        self,
        watched_namespaces: list[str],
        *,
        argo_out_of_sync_polls: int = DEFAULT_ARGO_OUT_OF_SYNC_POLLS,
        update_streaks: bool = True,
        ignored_argo_apps: Collection[str] = DEFAULT_IGNORED_ARGO_APPS,
    ) -> ClusterHealthSnapshot:
        """Collect pod health counts, pod anomalies, and Argo CD status across namespaces."""
        snapshot = ClusterHealthSnapshot(checked_at=datetime.now(UTC).isoformat())

        for ns in watched_namespaces:
            try:
                pod_list = await asyncio.to_thread(
                    self._core_v1.list_namespaced_pod,
                    namespace=ns,
                    _request_timeout=15,
                )
            except ApiException as exc:
                err = f"Failed to list pods in `{ns}` (HTTP {exc.status}): {exc.reason}"
                snapshot.errors.append(err)
                log.warning("k8s_watchdog.pod_list_failed", namespace=ns, status=exc.status)
                continue

            pods = list(_field(pod_list, "items", default=[]) or [])
            ready_count = sum(1 for p in pods if _is_pod_ready_or_running(p))
            snapshot.namespace_pod_counts[ns] = (ready_count, len(pods))
            snapshot.pod_anomalies.extend(detect_pod_anomalies(pods, ns))

        try:
            raw_apps = await asyncio.to_thread(
                self._custom_objects.list_namespaced_custom_object,
                group="argoproj.io",
                version="v1alpha1",
                namespace="argocd",
                plural="applications",
                _request_timeout=15,
            )
        except ApiException as exc:
            if exc.status in (403, 404):
                err = f"Argo CD applications unavailable (HTTP {exc.status}): {exc.reason}"
            else:
                err = f"Failed to list Argo CD applications (HTTP {exc.status}): {exc.reason}"
            snapshot.errors.append(err)
            log.warning("k8s_watchdog.argo_list_failed", status=exc.status, reason=exc.reason)
            return snapshot

        apps = list(_field(raw_apps, "items", default=[]) or [])
        for app in apps:
            app_name, sync_status, health_status = _extract_argo_summary(app)
            if app_name:
                snapshot.argo_apps_summary.append((app_name, sync_status, health_status))

        snapshot.argo_anomalies.extend(
            detect_argo_anomalies(
                apps,
                consecutive_out_of_sync=self._consecutive_out_of_sync,
                out_of_sync_polls=argo_out_of_sync_polls,
                update_streaks=update_streaks,
                ignored_apps=ignored_argo_apps,
            )
        )
        return snapshot

    async def _read_pod_logs(self, anomaly: PodAnomaly) -> tuple[str, bool]:
        """Return `(log_text, from_previous)` trying previous container logs when applicable."""
        prefer_previous = anomaly.reason in {"CrashLoopBackOff", "OOMKilled"}
        attempts = (True, False) if prefer_previous else (False,)

        for prev in attempts:
            try:
                raw_log = await asyncio.to_thread(
                    self._core_v1.read_namespaced_pod_log,
                    name=anomaly.pod_name,
                    namespace=anomaly.namespace,
                    container=anomaly.container_name,
                    previous=prev,
                    tail_lines=80,
                    _request_timeout=15,
                )
                text = str(raw_log or "").strip()
                if text:
                    return text, prev
            except ApiException as exc:
                log.debug(
                    "k8s_watchdog.log_read_skipped",
                    namespace=anomaly.namespace,
                    pod=anomaly.pod_name,
                    container=anomaly.container_name,
                    previous=prev,
                    status=exc.status,
                )
        return "", False

    async def _list_warning_events(
        self,
        namespace: str,
        *,
        involved_name: str | None = None,
    ) -> list[str]:
        kwargs: dict[str, Any] = {"namespace": namespace, "_request_timeout": 15}
        if involved_name:
            kwargs["field_selector"] = f"involvedObject.name={involved_name}"

        try:
            event_list = await asyncio.to_thread(self._core_v1.list_namespaced_event, **kwargs)
        except ApiException as exc:
            log.debug(
                "k8s_watchdog.events_list_failed",
                namespace=namespace,
                involved_name=involved_name,
                status=exc.status,
            )
            return []

        events = list(_field(event_list, "items", default=[]) or [])
        lines: list[str] = []
        for ev in events:
            ev_type = str(_field(ev, "type", default="") or "")
            if ev_type and ev_type != "Warning":
                continue
            reason = str(_field(ev, "reason", default="") or "Warning")
            count = _field(ev, "count")
            inv = _field(ev, "involved_object", "involvedObject")
            inv_name = str(_field(inv, "name", default="") or "")
            if involved_name and inv_name and inv_name != involved_name:
                continue
            msg = redact_secrets(str(_field(ev, "message", default="") or "").strip())
            prefix = f"[{reason} x{count}]" if count else f"[{reason}]"
            obj_prefix = f"{inv_name}: " if inv_name and not involved_name else ""
            lines.append(f"- {prefix} {obj_prefix}{_clip(msg, 280)}")

        return lines[-_MAX_EVENT_LINES:]

    async def collect_diagnostic_context(self, anomaly: PodAnomaly | ArgoAnomaly) -> str:
        """Collect bounded, secret-redacted logs and events for a pod or Argo CD anomaly."""
        if isinstance(anomaly, PodAnomaly):
            log_text, from_previous = await self._read_pod_logs(anomaly)
            events = await self._list_warning_events(
                anomaly.namespace,
                involved_name=anomaly.pod_name,
            )

            scrubbed_log = redact_secrets(log_text)
            if len(scrubbed_log) > _MAX_LOG_CHARS:
                scrubbed_log = "…(truncated)…\n" + scrubbed_log[-_MAX_LOG_CHARS:]

            events_block = "\n".join(events)
            if len(events_block) > _MAX_EVENT_CHARS:
                events_block = events_block[-_MAX_EVENT_CHARS:]

            log_label = "previous container" if from_previous else "current container"
            return (
                f"### Pod Anomaly Summary\n"
                f"- **Namespace:** `{anomaly.namespace}`\n"
                f"- **Pod:** `{anomaly.pod_name}` (owner: `{anomaly.owner_name}`)\n"
                f"- **Container:** `{anomaly.container_name}` (`{anomaly.image}`)\n"
                f"- **Reason:** `{anomaly.reason}` (restarts: {anomaly.restarts})\n"
                f"- **Status Message:** {redact_secrets(anomaly.message) or '_none_'}\n\n"
                f"### Recent Warning Events\n"
                f"{events_block or '_No Warning events recorded._'}\n\n"
                f"### Container Logs (tail 80, {log_label})\n"
                f"```text\n{scrubbed_log or '(no container logs available)'}\n```"
            )

        events = await self._list_warning_events("argocd")
        events_block = "\n".join(events)
        if len(events_block) > _MAX_EVENT_CHARS:
            events_block = events_block[-_MAX_EVENT_CHARS:]

        return (
            f"### Argo CD Anomaly Summary\n"
            f"- **Application:** `{anomaly.app_name}`\n"
            f"- **Sync Status:** `{anomaly.sync_status}`\n"
            f"- **Health Status:** `{anomaly.health_status}`\n"
            f"- **Condition / Health Message:** {redact_secrets(anomaly.message) or '_none_'}\n\n"
            f"### Recent Warning Events (`argocd`)\n"
            f"{events_block or '_No Warning events recorded in argocd._'}"
        )
