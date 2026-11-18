from __future__ import annotations

import asyncio
import base64
import re
import subprocess
import tempfile
from pathlib import Path

import httpx
import structlog

from agent_tasks.common.models import TriageReport
from agent_tasks.common.state import IssueRecord

log = structlog.get_logger(__name__)


def _github_basic_auth_header(token: str) -> str:
    encoded = base64.b64encode(f"x-access-token:{token}".encode()).decode("ascii")
    return f"AUTHORIZATION: basic {encoded}"


def mirror_dir_for_repo(mirrors_root: Path, github_repo: str) -> Path:
    safe_slug = github_repo.replace("/", "__")
    return mirrors_root / f"{safe_slug}.git"


def sync_bare_mirror_sync(mirrors_root: Path, github_repo: str, github_token: str) -> Path:
    """Clone or update a bare git mirror WITHOUT persisting credentials into git config.

    Worker Job initContainers mount the bare mirror read-only to clone `/work` in <1s.
    Because `remote.origin.url` stays a clean https://github.com/... URL and auth is
    passed via transient `-c http.https://github.com/.extraheader=...`, zero tokens
    are ever written onto the shared PVC.
    """
    mirrors_root.mkdir(parents=True, exist_ok=True)
    mirror_path = mirror_dir_for_repo(mirrors_root, github_repo)
    clean_url = f"https://github.com/{github_repo}.git"
    auth_header = _github_basic_auth_header(github_token)

    if not (mirror_path / "HEAD").exists():
        log.info("git_mirror.cloning", repo=github_repo, path=str(mirror_path))
        subprocess.run(
            [
                "git",
                "-c",
                f"http.https://github.com/.extraheader={auth_header}",
                "clone",
                "--mirror",
                clean_url,
                str(mirror_path),
            ],
            check=True,
            capture_output=True,
            text=True,
        )
    else:
        # Ensure origin URL never holds embedded credentials, then fetch latest refs.
        subprocess.run(
            ["git", "-C", str(mirror_path), "remote", "set-url", "origin", clean_url],
            check=True,
            capture_output=True,
            text=True,
        )
        subprocess.run(
            [
                "git",
                "-C",
                str(mirror_path),
                "-c",
                f"http.https://github.com/.extraheader={auth_header}",
                "remote",
                "update",
                "--prune",
            ],
            check=True,
            capture_output=True,
            text=True,
        )
    return mirror_path


async def sync_bare_mirror(mirrors_root: Path, github_repo: str, github_token: str) -> Path:
    return await asyncio.to_thread(sync_bare_mirror_sync, mirrors_root, github_repo, github_token)


def _apply_patch_and_push_sync(
    *,
    mirror_path: Path,
    github_repo: str,
    clone_branch: str,
    branch_name: str,
    patch_path: Path,
    commit_message: str,
    github_token: str,
    force_with_lease: bool = True,
    remote_url: str | None = None,
) -> str:
    clean_url = remote_url or f"https://github.com/{github_repo}.git"
    auth_header = _github_basic_auth_header(github_token)

    with tempfile.TemporaryDirectory(prefix="agy-pr-") as tmp:
        workdir = Path(tmp) / "repo"
        subprocess.run(
            [
                "git",
                "clone",
                "--branch",
                clone_branch,
                "--single-branch",
                str(mirror_path),
                str(workdir),
            ],
            check=True,
            capture_output=True,
            text=True,
        )
        if branch_name != clone_branch:
            subprocess.run(
                ["git", "-C", str(workdir), "checkout", "-B", branch_name],
                check=True,
                capture_output=True,
                text=True,
            )
        subprocess.run(
            ["git", "-C", str(workdir), "apply", "--index", str(patch_path)],
            check=True,
            capture_output=True,
            text=True,
        )
        subprocess.run(
            [
                "git",
                "-C",
                str(workdir),
                "-c",
                "user.name=AGY Automated Tasks",
                "-c",
                "user.email=agy-triage@users.noreply.github.com",
                "commit",
                "-m",
                commit_message,
            ],
            check=True,
            capture_output=True,
            text=True,
        )
        sha = subprocess.run(
            ["git", "-C", str(workdir), "rev-parse", "--short", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
        push_cmd = [
            "git",
            "-C",
            str(workdir),
            "-c",
            f"http.https://github.com/.extraheader={auth_header}",
            "push",
        ]
        if force_with_lease:
            push_cmd.append("--force-with-lease")
        push_cmd.extend([clean_url, f"HEAD:refs/heads/{branch_name}"])
        subprocess.run(
            push_cmd,
            check=True,
            capture_output=True,
            text=True,
        )
        return sha


async def push_patch_to_branch(
    *,
    mirrors_root: Path,
    record: IssueRecord,
    report: TriageReport | None,
    patch_path: Path,
    branch_name: str,
    github_token: str,
) -> str:
    """Apply `fix.patch` on top of an existing non-default branch and fast-forward push it."""
    if not patch_path.exists() or not patch_path.read_text(encoding="utf-8").strip():
        raise ValueError(f"No non-empty fix.patch available for {record.short_id}")
    if branch_name == record.default_branch:
        raise ValueError("Refusing direct push to default branch")

    mirror_path = await sync_bare_mirror(mirrors_root, record.github_repo, github_token)
    commit_msg = (
        f"fix({record.project_slug}): {record.short_id} — {record.title[:72]}\n\n"
        f"{report.fix_summary if report else record.title}"
    )
    return await asyncio.to_thread(
        _apply_patch_and_push_sync,
        mirror_path=mirror_path,
        github_repo=record.github_repo,
        clone_branch=branch_name,
        branch_name=branch_name,
        patch_path=patch_path,
        commit_message=commit_msg,
        github_token=github_token,
        force_with_lease=False,
    )


async def create_draft_pull_request(
    *,
    mirrors_root: Path,
    record: IssueRecord,
    report: TriageReport | None,
    patch_path: Path,
    github_token: str,
) -> str:
    """Apply `fix.patch` onto a fresh branch from the bare mirror, push, and open a Draft PR."""
    if not patch_path.exists() or not patch_path.read_text(encoding="utf-8").strip():
        raise ValueError(f"No non-empty fix.patch available for {record.short_id}")

    mirror_path = await sync_bare_mirror(mirrors_root, record.github_repo, github_token)
    slug = re.sub(r"[^a-z0-9._-]+", "-", record.short_id.lower()).strip("-")
    task_prefix = "sentry" if record.task_kind == "sentry" else record.task_kind.replace("_", "-")
    branch_name = f"agy/{task_prefix}-{slug}"
    base_branch = record.base_ref or record.default_branch

    if record.task_kind == "sentry":
        commit_msg = (
            f"fix({record.project_slug}): resolve {record.short_id}\n\n"
            f"{report.fix_summary if report else record.title}\n\n"
            f"Fixes {record.short_id}"
        )
        pr_title = f"fix({record.project_slug}): {record.short_id} — {record.title[:80]}"
        pr_body = (
            f"## Automated Sentry Triage (`{record.short_id}`)\n\n"
            f"- **Sentry Issue:** [{record.short_id}]({record.permalink}) — {record.title}\n"
            f"- **Project:** `{record.project_slug}`\n"
            f"- **Confidence:** `{report.confidence if report else 'unknown'}`\n"
            f"- **Worker Verification Passed:** `{report.tests_passed if report else False}`\n\n"
            f"### Root Cause\n{report.root_cause if report else '_N/A_'}\n\n"
            f"### Proposed Fix\n{report.fix_summary if report else '_See diff_'}\n\n"
            f"Fixes {record.short_id}\n"
        )
    else:
        commit_msg = (
            f"fix({record.project_slug}): {record.title[:72]}\n\n"
            f"Task `{record.short_id}` ({record.task_kind})\n\n"
            f"{report.fix_summary if report else record.title}"
        )
        pr_title = f"fix({record.project_slug}): {record.short_id} — {record.title[:72]}"
        ref_line = f"- **Reference:** {record.permalink}\n" if record.permalink else ""
        pr_body = (
            f"## Automated Task (`{record.task_kind}` — `{record.short_id}`)\n\n"
            f"- **Title:** {record.title}\n"
            f"{ref_line}"
            f"- **Target:** `{record.project_slug}`\n"
            f"- **Confidence:** `{report.confidence if report else 'unknown'}`\n"
            f"- **Worker Verification Passed:** `{report.tests_passed if report else False}`\n\n"
            f"### Summary / Analysis\n{report.root_cause if report else '_N/A_'}\n\n"
            f"### Proposed Changes\n{report.fix_summary if report else '_See diff_'}\n"
        )

    await asyncio.to_thread(
        _apply_patch_and_push_sync,
        mirror_path=mirror_path,
        github_repo=record.github_repo,
        clone_branch=base_branch,
        branch_name=branch_name,
        patch_path=patch_path,
        commit_message=commit_msg,
        github_token=github_token,
        force_with_lease=True,
    )

    headers = {
        "Authorization": f"Bearer {github_token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    async with httpx.AsyncClient(timeout=30.0, headers=headers) as client:
        resp = await client.post(
            f"https://api.github.com/repos/{record.github_repo}/pulls",
            json={
                "title": pr_title,
                "head": branch_name,
                "base": base_branch,
                "body": pr_body,
                "draft": True,
            },
        )
        if resp.status_code == 422:
            # PR for this branch may already exist; look it up.
            owner = record.github_repo.split("/")[0]
            existing = await client.get(
                f"https://api.github.com/repos/{record.github_repo}/pulls",
                params={"head": f"{owner}:{branch_name}", "state": "open"},
            )
            existing.raise_for_status()
            items = existing.json()
            if items:
                return str(items[0]["html_url"])
        resp.raise_for_status()
        return str(resp.json()["html_url"])
