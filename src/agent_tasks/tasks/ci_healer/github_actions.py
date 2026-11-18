from __future__ import annotations

import re
from typing import Any

import httpx

_ANSI_CSI_RE = re.compile(r"\x1b\[[0-9;?]*[A-Za-z]")
_ANSI_OSC_RE = re.compile(r"\x1b\][^\x07\x1b]*(?:\x07|\x1b\\)")

_SECRET_PATTERNS: tuple[tuple[re.Pattern[str], str], ...] = (
    (re.compile(r"\bxox[abp]-[0-9A-Za-z-]+"), "[REDACTED]"),
    (re.compile(r"\bgh[pousr]_[0-9A-Za-z_]+"), "[REDACTED]"),
    (re.compile(r"\bgithub_pat_[0-9A-Za-z_]+"), "[REDACTED]"),
    (re.compile(r"\bsntryu_[0-9A-Za-z_]+"), "[REDACTED]"),
    (re.compile(r"\bAIza[0-9A-Za-z_-]+"), "[REDACTED]"),
    (re.compile(r"\bAQ\.[0-9A-Za-z_.-]+"), "[REDACTED]"),
    (
        re.compile(r"(?i)\b(Authorization\s*:\s*(?:Bearer|Basic|Token)\s+)[^\s\"'`<>\],)]+"),
        r"\1[REDACTED]",
    ),
    (
        re.compile(r"(?i)\b(Authorization\s*:\s*)(?!(?:Bearer|Basic|Token)\b)[^\s\"'`<>\],)]+"),
        r"\1[REDACTED]",
    ),
    (re.compile(r"(?i)\bBearer\s+(?!\[REDACTED\])[^\s\"'`<>\],)]+"), "Bearer [REDACTED]"),
)

_EXACT_FAILURE_MARKERS = (
    "FAILED",
    "ERROR",
    "Traceback",
    "AssertionError",
    "Process completed with exit code",
)
_TRUNCATION_MARKER = "... [truncated] ..."


def _strip_ansi_and_redact(text: str) -> str:
    cleaned = _ANSI_OSC_RE.sub("", _ANSI_CSI_RE.sub("", text))
    cleaned = cleaned.replace("\r\n", "\n").replace("\r", "\n")
    for pattern, replacement in _SECRET_PATTERNS:
        cleaned = pattern.sub(replacement, cleaned)
    return cleaned


def _is_failure_line(line: str) -> bool:
    return any(marker in line for marker in _EXACT_FAILURE_MARKERS) or "error:" in line.lower()


def _clip_single_line(line: str, max_len: int) -> str:
    if len(line) <= max_len:
        return line
    if max_len <= 3:
        return line[:max_len]
    return line[: max_len - 3] + "..."


def _select_tail_indices(lines: list[str], budget: int, max_lines: int = 30) -> list[int]:
    if not lines or budget <= 0:
        return []
    selected: list[int] = []
    used = 0
    for idx in range(len(lines) - 1, max(-1, len(lines) - max_lines - 1), -1):
        line_len = len(lines[idx]) + (1 if selected else 0)
        if selected and used + line_len > budget:
            break
        selected.append(idx)
        used += line_len
    selected.reverse()
    return selected


def _collect_diagnostic_indices(
    lines: list[str],
    marker_indices: list[int],
    *,
    before: int,
    after: int,
    exclude: set[int],
) -> list[int]:
    chosen: set[int] = set()
    for m_idx in marker_indices:
        start = max(0, m_idx - before)
        end = min(len(lines), m_idx + after + 1)
        for idx in range(start, end):
            if idx not in exclude:
                chosen.add(idx)
    return sorted(chosen)


def _render_indices(lines: list[str], indices: list[int], line_cap: int) -> str:
    parts: list[str] = []
    prev_idx: int | None = None
    for idx in indices:
        if (prev_idx is not None and idx > prev_idx + 1) or (prev_idx is None and idx > 0):
            parts.append(_TRUNCATION_MARKER)
        parts.append(_clip_single_line(lines[idx], line_cap))
        prev_idx = idx
    return "\n".join(parts)


def sanitize_ci_log(raw_log: str, max_chars: int = 6000) -> str:
    """Strip ANSI codes, redact secrets, and keep diagnostic failure lines + tail."""
    if max_chars <= 0 or not raw_log:
        return ""

    cleaned = _strip_ansi_and_redact(raw_log)
    if len(cleaned) <= max_chars:
        return cleaned

    lines = cleaned.splitlines()
    if not lines:
        return cleaned[:max_chars]

    line_cap = max(120, max_chars // 3)
    marker_indices = [i for i, line in enumerate(lines) if _is_failure_line(line)]

    if not marker_indices:
        tail_idx = _select_tail_indices(lines, max_chars)
        rendered = _render_indices(lines, tail_idx, max_chars)
        if len(rendered) > max_chars:
            return rendered[-max_chars:]
        return rendered

    tail_budget = min(max_chars // 3, 1800) if max_chars >= 300 else max_chars // 2
    tail_indices = _select_tail_indices(lines, tail_budget)
    tail_set = set(tail_indices)

    for before, after in ((4, 12), (2, 6), (0, 2)):
        diag_indices = _collect_diagnostic_indices(
            lines, marker_indices, before=before, after=after, exclude=tail_set
        )
        combined = sorted(set(diag_indices) | tail_set)
        rendered = _render_indices(lines, combined, line_cap)
        if len(rendered) <= max_chars:
            return rendered

    # Still over budget (many failure lines): keep tail + as many failure marker lines as fit.
    tail_rendered = _render_indices(lines, tail_indices, min(line_cap, tail_budget or max_chars))
    if len(tail_rendered) > max_chars:
        return tail_rendered[-max_chars:]

    sep = f"\n{_TRUNCATION_MARKER}\n"
    diag_budget = max_chars - len(tail_rendered) - len(sep)
    kept_diag: list[int] = []
    used = 0
    candidate_indices = _collect_diagnostic_indices(
        lines, marker_indices, before=1, after=2, exclude=tail_set
    )
    marker_set = set(marker_indices)
    # Prefer actual marker lines first if even tight context is too big.
    prioritized = [i for i in candidate_indices if i in marker_set] + [
        i for i in candidate_indices if i not in marker_set
    ]
    selected_diag_set: set[int] = set()
    for idx in prioritized:
        line_text = _clip_single_line(lines[idx], min(line_cap, max(80, diag_budget)))
        cost = len(line_text) + (1 if selected_diag_set else 0) + len(_TRUNCATION_MARKER) + 1
        if used + cost > diag_budget:
            continue
        selected_diag_set.add(idx)
        used += len(line_text) + (1 if len(selected_diag_set) > 1 else 0)

    kept_diag = sorted(selected_diag_set)
    combined = sorted(set(kept_diag) | tail_set)
    rendered = _render_indices(lines, combined, min(line_cap, max(80, max_chars // 2)))
    if len(rendered) <= max_chars:
        return rendered

    if diag_budget > 0 and kept_diag:
        diag_part = _render_indices(lines, kept_diag, diag_budget)[:diag_budget].rstrip()
        combined_text = f"{diag_part}{sep}{tail_rendered}" if diag_part else tail_rendered
        if len(combined_text) <= max_chars:
            return combined_text

    return tail_rendered[-max_chars:]


class GitHubActionsClient:
    """GitHub Actions REST client (runs strictly inside controller, never inside worker pods)."""

    def __init__(
        self,
        *,
        github_token: str,
        transport: httpx.AsyncBaseTransport | None = None,
        base_url: str = "https://api.github.com",
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self._transport = transport
        self._headers: dict[str, str] = {
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }
        if github_token.strip():
            self._headers["Authorization"] = f"Bearer {github_token.strip()}"

    def _client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(
            timeout=30.0,
            follow_redirects=True,
            headers=self._headers,
            transport=self._transport,
        )

    async def list_failed_runs(
        self, github_repo: str, *, per_page: int = 5
    ) -> list[dict[str, Any]]:
        repo = github_repo.strip("/")
        url = f"{self.base_url}/repos/{repo}/actions/runs"
        async with self._client() as client:
            resp = await client.get(
                url,
                params={"status": "failure", "per_page": str(per_page)},
            )
            resp.raise_for_status()
            data = resp.json()
            runs = data.get("workflow_runs", []) if isinstance(data, dict) else []
            return [r for r in runs if isinstance(r, dict)]

    async def list_run_jobs(self, github_repo: str, run_id: int) -> list[dict[str, Any]]:
        repo = github_repo.strip("/")
        url = f"{self.base_url}/repos/{repo}/actions/runs/{run_id}/jobs"
        async with self._client() as client:
            resp = await client.get(url)
            resp.raise_for_status()
            data = resp.json()
            jobs = data.get("jobs", []) if isinstance(data, dict) else []
            return [j for j in jobs if isinstance(j, dict)]

    async def fetch_job_logs(self, github_repo: str, job_id: int) -> str:
        repo = github_repo.strip("/")
        url = f"{self.base_url}/repos/{repo}/actions/jobs/{job_id}/logs"
        async with self._client() as client:
            resp = await client.get(url)
            if resp.status_code in (404, 410):
                return ""
            resp.raise_for_status()
            return resp.text
