from __future__ import annotations

import asyncio
import re
import subprocess
from dataclasses import dataclass, field
from pathlib import Path

import structlog

from agent_tasks.common.models import SanitizedSentryBundle

log = structlog.get_logger(__name__)

_SHA_TOKEN_RE = re.compile(r"(?<![0-9a-zA-Z])([0-9a-fA-F]{7,40})(?![0-9a-zA-Z])")
_GIT_DESCRIBE_SHA_RE = re.compile(r"(?<=-g)([0-9a-fA-F]{7,40})(?![0-9a-zA-Z])")
_MAX_TOUCHING_COMMITS = 5


@dataclass(slots=True)
class ReleaseLagInfo:
    """Git release lag context comparing a Sentry event's release SHA to the default branch."""

    release_sha: str
    commits_behind: int
    touching_commits: list[str] = field(default_factory=list)
    checked_paths: list[str] = field(default_factory=list)

    def to_slack_banner(self) -> str:
        """Render a concise Slack warning banner if the release lags behind `main`."""
        if not self.touching_commits and self.commits_behind <= 0:
            return ""

        short_sha = self.release_sha[:8]
        plural = "commit" if self.commits_behind == 1 else "commits"
        if self.touching_commits:
            commits_md = "\n".join(f"• `{line}`" for line in self.touching_commits)
            return (
                f":warning: *Stale release warning:* Sentry event ran on `{short_sha}` "
                f"(*{self.commits_behind} {plural} behind `main`*), and commits on `main` "
                f"already modified the implicated file(s):\n{commits_md}"
            )

        return (
            f":warning: *Stale release warning:* Sentry event ran on `{short_sha}` "
            f"(*{self.commits_behind} {plural} behind `main`*; "
            f"no newer commits touched the extracted stacktrace paths)."
        )

    def to_prompt_note(self) -> str:
        """Render context for `release_context.txt` consumed by the triage worker pod."""
        if not self.touching_commits and self.commits_behind <= 0:
            return ""

        plural = "commit" if self.commits_behind == 1 else "commits"
        lines = [
            (
                f"Crashing release SHA: `{self.release_sha}` "
                f"({self.commits_behind} {plural} behind `main`; `/work` is checked out at `main`)."
            )
        ]
        if self.checked_paths:
            lines.append("Checked stacktrace path(s): " + ", ".join(self.checked_paths))

        if self.touching_commits:
            lines.append(
                "Commits on `main` since this release that touched the implicated file(s):"
            )
            lines.extend(f"  - {commit}" for commit in self.touching_commits)
            lines.append(
                "Check right away whether this bug is already fixed on `main`. "
                "If it is already resolved on `main`, leave `/work` clean (no patch) and cite "
                "the fixing commit(s) in `fix_summary`."
            )
        else:
            lines.append(
                "No commits on `main` since this release touched the checked stacktrace paths, "
                "but verify on `main` whether surrounding changes already resolved the issue."
            )
        return "\n".join(lines)


def extract_git_sha(release_str: str) -> str | None:
    """Extract a 7-40 hex git SHA from a Sentry release string, ignoring version-only values."""
    text = (release_str or "").strip()
    if not text:
        return None

    matches = _SHA_TOKEN_RE.findall(text)
    if matches:
        return matches[-1].lower()

    describe_matches = _GIT_DESCRIBE_SHA_RE.findall(text)
    if describe_matches:
        return describe_matches[-1].lower()

    return None


def _normalize_frame_filename(raw_filename: str) -> str | None:
    path = raw_filename.strip().replace("\\", "/")
    if not path or (path.startswith("<") and path.endswith(">")):
        return None

    for prefix in ("/work/", "/app/"):
        if path.startswith(prefix):
            path = path[len(prefix) :]
            break

    while path.startswith("./"):
        path = path[2:]
    path = path.lstrip("/")

    if not path or path == "." or ".." in Path(path).parts:
        return None
    return path


def _extract_candidate_paths(bundle: SanitizedSentryBundle) -> list[str]:
    candidates: list[str] = []
    seen: set[str] = set()

    def _add(candidate: str) -> None:
        if candidate and candidate not in seen:
            seen.add(candidate)
            candidates.append(candidate)

    for exc in bundle.exceptions:
        for frame in exc.frames:
            if not frame.in_app:
                continue
            norm = _normalize_frame_filename(frame.filename)
            if not norm:
                continue
            _add(norm)
            if norm.startswith("rattlsnek/"):
                stripped = norm.removeprefix("rattlsnek/")
                _add(stripped)
            else:
                _add(f"rattlsnek/{norm}")

    return candidates


def _run_git(mirror_path: Path, args: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", "-C", str(mirror_path), *args],
        check=False,
        capture_output=True,
        text=True,
    )


def _resolve_target_ref(mirror_path: Path, default_branch: str) -> str | None:
    for ref in (f"refs/heads/{default_branch}", f"refs/remotes/origin/{default_branch}"):
        proc = _run_git(mirror_path, ["rev-parse", "--verify", "--quiet", f"{ref}^{{commit}}"])
        if proc.returncode == 0:
            return ref
    return None


def check_sentry_release_lag_sync(
    mirror_path: Path,
    bundle: SanitizedSentryBundle,
    default_branch: str = "main",
) -> ReleaseLagInfo | None:
    """Compare `bundle.release` against `default_branch` in a bare mirror to detect lag."""
    if not (mirror_path / "HEAD").exists():
        return None

    release_sha = extract_git_sha(bundle.release)
    if release_sha is None:
        return None

    exists_proc = _run_git(mirror_path, ["cat-file", "-e", f"{release_sha}^{{commit}}"])
    if exists_proc.returncode != 0:
        return None

    target_ref = _resolve_target_ref(mirror_path, default_branch)
    if target_ref is None:
        return None

    rev_range = f"{release_sha}..{target_ref}"
    count_proc = _run_git(mirror_path, ["rev-list", "--count", rev_range])
    if count_proc.returncode != 0:
        log.warning(
            "release_check.rev_list_failed",
            mirror=str(mirror_path),
            rev_range=rev_range,
            stderr=count_proc.stderr.strip(),
        )
        return None

    try:
        commits_behind = int(count_proc.stdout.strip())
    except ValueError:
        return None

    if commits_behind <= 0:
        return None

    checked_paths = _extract_candidate_paths(bundle)
    touching_commits: list[str] = []
    if checked_paths:
        log_proc = _run_git(
            mirror_path,
            [
                "log",
                "--oneline",
                "-n",
                str(_MAX_TOUCHING_COMMITS),
                rev_range,
                "--",
                *checked_paths,
            ],
        )
        if log_proc.returncode == 0:
            touching_commits = [
                line.strip() for line in log_proc.stdout.splitlines() if line.strip()
            ][:_MAX_TOUCHING_COMMITS]
        else:
            log.warning(
                "release_check.git_log_failed",
                mirror=str(mirror_path),
                rev_range=rev_range,
                stderr=log_proc.stderr.strip(),
            )

    return ReleaseLagInfo(
        release_sha=release_sha,
        commits_behind=commits_behind,
        touching_commits=touching_commits,
        checked_paths=checked_paths,
    )


async def check_sentry_release_lag(
    mirror_path: Path,
    bundle: SanitizedSentryBundle,
    default_branch: str = "main",
) -> ReleaseLagInfo | None:
    """Async wrapper running `check_sentry_release_lag_sync` off the event loop."""
    return await asyncio.to_thread(
        check_sentry_release_lag_sync,
        mirror_path,
        bundle,
        default_branch,
    )
