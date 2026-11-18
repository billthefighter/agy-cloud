from __future__ import annotations

import time
from typing import Any

import httpx

from agent_tasks.common.models import (
    BreadcrumbSummary,
    ExceptionSummary,
    SanitizedSentryBundle,
    StackFrameSummary,
)

MAX_STR_LEN = 500
MAX_EXCEPTIONS = 4
MAX_FRAMES_PER_EXCEPTION = 20
MAX_BREADCRUMBS = 15
ALLOWED_TAG_PREFIXES = (
    "environment",
    "release",
    "level",
    "logger",
    "transaction",
    "url",
    "runtime",
    "os",
    "server_name",
    "handled",
    "mechanism",
)


def _clip(value: Any, limit: int = MAX_STR_LEN) -> str:
    if value is None:
        return ""
    text = str(value)
    return text if len(text) <= limit else text[: limit - 1] + "…"


def sanitize_sentry_issue_and_event(
    issue: dict[str, Any],
    event: dict[str, Any] | None,
    project_slug: str,
) -> SanitizedSentryBundle:
    """Strip untrusted headers/cookies/user PII and bound payload size before LLM triage."""
    event = event or {}

    tags: dict[str, str] = {}
    for item in event.get("tags") or []:
        if isinstance(item, dict):
            key = str(item.get("key") or "")
            val = _clip(item.get("value"), 200)
            if key and any(key.startswith(p) for p in ALLOWED_TAG_PREFIXES):
                tags[key] = val

    exceptions: list[ExceptionSummary] = []
    breadcrumbs: list[BreadcrumbSummary] = []

    for entry in event.get("entries") or []:
        if not isinstance(entry, dict):
            continue
        entry_type = entry.get("type")
        data = entry.get("data") or {}

        if entry_type == "exception" and isinstance(data, dict):
            raw_values = (data.get("values") or [])[-MAX_EXCEPTIONS:]
            for exc in raw_values:
                if not isinstance(exc, dict):
                    continue
                stacktrace = exc.get("stacktrace") or {}
                raw_frames = (stacktrace.get("frames") or [])[-MAX_FRAMES_PER_EXCEPTION:]
                frames = [
                    StackFrameSummary(
                        filename=_clip(f.get("filename") or f.get("absPath"), 240),
                        function=_clip(f.get("function"), 160),
                        lineno=f.get("lineNo") if isinstance(f.get("lineNo"), int) else None,
                        in_app=bool(f.get("inApp")),
                        context_line=_clip(f.get("contextLine"), 300),
                    )
                    for f in raw_frames
                    if isinstance(f, dict)
                ]
                exceptions.append(
                    ExceptionSummary(
                        exc_type=_clip(exc.get("type"), 160),
                        exc_value=_clip(exc.get("value"), MAX_STR_LEN),
                        frames=frames,
                    )
                )

        elif entry_type == "breadcrumbs" and isinstance(data, dict):
            raw_crumbs = (data.get("values") or [])[-MAX_BREADCRUMBS:]
            for crumb in raw_crumbs:
                if not isinstance(crumb, dict):
                    continue
                breadcrumbs.append(
                    BreadcrumbSummary(
                        timestamp=_clip(crumb.get("timestamp"), 64),
                        category=_clip(crumb.get("category"), 80),
                        level=_clip(crumb.get("level"), 32),
                        message=_clip(crumb.get("message"), 300),
                    )
                )

    return SanitizedSentryBundle(
        issue_id=str(issue["id"]),
        short_id=_clip(issue.get("shortId") or issue["id"], 64),
        project_slug=project_slug,
        title=_clip(issue.get("title"), 300),
        culprit=_clip(issue.get("culprit"), 240),
        permalink=_clip(issue.get("permalink"), 400),
        first_seen=_clip(issue.get("firstSeen"), 64),
        last_seen=_clip(issue.get("lastSeen"), 64),
        event_id=_clip(event.get("eventID") or event.get("id"), 80),
        release=_clip(event.get("release"), 120),
        environment=tags.get("environment", ""),
        tags=tags,
        exceptions=exceptions,
        breadcrumbs=breadcrumbs,
    )


class SentryClient:
    """Sentry REST client (runs strictly inside controller, never inside worker pods)."""

    def __init__(
        self,
        *,
        base_url: str,
        organization_slug: str,
        auth_token: str,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.organization_slug = organization_slug
        self._transport = transport
        self._headers = {
            "Authorization": f"Bearer {auth_token}",
            "Accept": "application/json",
        }

    def _client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(
            timeout=30.0,
            headers=self._headers,
            transport=self._transport,
        )

    async def ping_project(self, project_slug: str) -> float:
        """Verify Sentry API reachability & `project:read` auth, returning RTT in ms."""
        url = f"{self.base_url}/api/0/projects/{self.organization_slug}/{project_slug}/issues/"
        t0 = time.perf_counter()
        async with self._client() as client:
            resp = await client.get(url, params={"limit": "1"})
            resp.raise_for_status()
        return (time.perf_counter() - t0) * 1000.0

    async def fetch_recent_unresolved_issues(
        self,
        project_slug: str,
        *,
        query: str,
        limit: int = 5,
    ) -> list[dict[str, Any]]:
        url = f"{self.base_url}/api/0/projects/{self.organization_slug}/{project_slug}/issues/"
        async with self._client() as client:
            resp = await client.get(
                url,
                params={"query": query, "sort": "date", "limit": str(limit)},
            )
            resp.raise_for_status()
            data = resp.json()
            return data if isinstance(data, list) else []

    async def fetch_sanitized_bundle(
        self,
        issue: dict[str, Any],
        project_slug: str,
    ) -> SanitizedSentryBundle:
        issue_id = str(issue["id"])
        org_url = (
            f"{self.base_url}/api/0/organizations/"
            f"{self.organization_slug}/issues/{issue_id}/events/latest/"
        )
        legacy_url = f"{self.base_url}/api/0/issues/{issue_id}/events/latest/"
        event: dict[str, Any] | None = None
        async with self._client() as client:
            resp = await client.get(org_url)
            if resp.status_code == 404:
                resp = await client.get(legacy_url)
            if resp.status_code == 200:
                payload = resp.json()
                if isinstance(payload, dict):
                    event = payload
        return sanitize_sentry_issue_and_event(issue, event, project_slug)

    async def add_issue_note(self, issue_id: str, text: str) -> dict[str, Any]:
        """Post an activity comment/note onto a Sentry issue (`event:write`)."""
        body = {"text": text[:4000]}
        org_comments_url = (
            f"{self.base_url}/api/0/organizations/"
            f"{self.organization_slug}/issues/{issue_id}/comments/"
        )
        legacy_notes_url = f"{self.base_url}/api/0/issues/{issue_id}/notes/"
        async with self._client() as client:
            resp = await client.post(org_comments_url, json=body)
            if resp.status_code == 404:
                resp = await client.post(legacy_notes_url, json=body)
            resp.raise_for_status()
            data = resp.json()
            return data if isinstance(data, dict) else {}

    async def update_issue_status(
        self,
        issue_id: str,
        *,
        status: str,
        substatus: str | None = None,
        status_details: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Update Sentry issue status (`resolved`, `ignored`, `unresolved`) via `event:write`."""
        payload: dict[str, Any] = {"status": status}
        if substatus:
            payload["substatus"] = substatus
        if status_details:
            payload["statusDetails"] = status_details

        org_issue_url = (
            f"{self.base_url}/api/0/organizations/{self.organization_slug}/issues/{issue_id}/"
        )
        legacy_issue_url = f"{self.base_url}/api/0/issues/{issue_id}/"
        async with self._client() as client:
            resp = await client.put(org_issue_url, json=payload)
            if resp.status_code == 404:
                resp = await client.put(legacy_issue_url, json=payload)
            resp.raise_for_status()
            data = resp.json()
            return data if isinstance(data, dict) else {}
