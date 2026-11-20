from __future__ import annotations

import asyncio
import base64
import binascii
import io
import os
import re
import shutil
import subprocess
import tarfile
import tempfile
from pathlib import Path, PurePosixPath

import httpx
import structlog

from agent_tasks.common.models import TriageReport
from agent_tasks.common.state import IssueRecord

log = structlog.get_logger(__name__)


def _github_basic_auth_header(token: str) -> str:
    encoded = base64.b64encode(f"x-access-token:{token}".encode()).decode("ascii")
    return f"AUTHORIZATION: basic {encoded}"


def _ensure_relative_symlink(link_path: Path, target_path: Path) -> None:
    """Create or update a relative symlink so it resolves identically across mount paths."""
    link_path.parent.mkdir(parents=True, exist_ok=True)
    rel_target = os.path.relpath(target_path, start=link_path.parent)
    if link_path.is_symlink():
        if os.readlink(link_path) == rel_target:
            return
        link_path.unlink()
    elif link_path.exists():
        if link_path.is_dir():
            shutil.rmtree(link_path)
        else:
            link_path.unlink()
    link_path.symlink_to(rel_target)


def sync_agy_config_repo_sync(
    agy_auth_root: Path,
    github_repo: str,
    github_token: str,
    *,
    branch: str = "main",
) -> Path:
    """Clone or update an `agy-config` repo into `<agy_auth_root>/agy-config` and wire relative
    symlinks into `<agy_auth_root>/config/` WITHOUT persisting credentials into git config.

    Because `<agy_auth_root>` (`/state/agy-auth` in controller) is mounted as `/home/agy/.gemini`
    inside `agy_cli` worker pods, relative symlinks (`../agy-config/hooks`, etc.) resolve cleanly
    in both controller and ephemeral worker pods.
    """
    agy_auth_root.mkdir(parents=True, exist_ok=True)
    repo_dir = agy_auth_root / "agy-config"
    is_local_source = github_repo.startswith("/") or github_repo.startswith("file://")
    clean_url = github_repo if is_local_source else f"https://github.com/{github_repo}.git"
    auth_args = (
        ["-c", f"http.https://github.com/.extraheader={_github_basic_auth_header(github_token)}"]
        if (github_token and not is_local_source)
        else []
    )

    if not (repo_dir / ".git").exists():
        if repo_dir.exists():
            shutil.rmtree(repo_dir)
        log.info("agy_config.cloning", repo=github_repo, branch=branch, path=str(repo_dir))
        subprocess.run(
            [
                "git",
                *auth_args,
                "clone",
                "--depth",
                "1",
                "--branch",
                branch,
                clean_url,
                str(repo_dir),
            ],
            check=True,
            capture_output=True,
            text=True,
        )
    else:
        subprocess.run(
            ["git", "-C", str(repo_dir), "remote", "set-url", "origin", clean_url],
            check=True,
            capture_output=True,
            text=True,
        )
        subprocess.run(
            [
                "git",
                *auth_args,
                "-C",
                str(repo_dir),
                "fetch",
                "--depth",
                "1",
                "origin",
                branch,
            ],
            check=True,
            capture_output=True,
            text=True,
        )
        subprocess.run(
            ["git", "-C", str(repo_dir), "reset", "--hard", "FETCH_HEAD"],
            check=True,
            capture_output=True,
            text=True,
        )

    config_dir = agy_auth_root / "config"
    config_dir.mkdir(parents=True, exist_ok=True)
    (agy_auth_root / "antigravity" / "hook_state").mkdir(parents=True, exist_ok=True)

    if (repo_dir / "hooks.json").is_file():
        _ensure_relative_symlink(config_dir / "hooks.json", repo_dir / "hooks.json")
    if (repo_dir / "hooks").is_dir():
        _ensure_relative_symlink(config_dir / "hooks", repo_dir / "hooks")

    for subdir_name, want_dir in (("skills", True), ("sidecars", True), ("rules", False)):
        src_parent = repo_dir / subdir_name
        dst_parent = config_dir / subdir_name
        expected_names: set[str] = set()
        if src_parent.is_dir():
            dst_parent.mkdir(parents=True, exist_ok=True)
            for entry in sorted(src_parent.iterdir()):
                if entry.name.startswith("."):
                    continue
                if (want_dir and entry.is_dir()) or (not want_dir and entry.is_file()):
                    expected_names.add(entry.name)
                    _ensure_relative_symlink(dst_parent / entry.name, entry)
        if dst_parent.is_dir():
            for existing in list(dst_parent.iterdir()):
                if existing.is_symlink() and existing.name not in expected_names:
                    existing.unlink()

    log.info("agy_config.synced", repo=github_repo, branch=branch, config_dir=str(config_dir))
    return config_dir


async def sync_agy_config_repo(
    agy_auth_root: Path,
    github_repo: str,
    github_token: str,
    *,
    branch: str = "main",
) -> Path:
    return await asyncio.to_thread(
        sync_agy_config_repo_sync,
        agy_auth_root,
        github_repo,
        github_token,
        branch=branch,
    )


DEFAULT_MAX_REPO_SNAPSHOT_BYTES = 25 * 1024 * 1024  # 25 MiB compressed
_GITHUB_SLUG_RE = re.compile(r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$")


class RepoSnapshotTooLargeError(ValueError):
    """Raised when an uploaded repository snapshot exceeds the configured size limit."""


def validate_and_write_repo_snapshot(
    snapshot_b64: str,
    dest_path: Path,
    *,
    max_bytes: int = DEFAULT_MAX_REPO_SNAPSHOT_BYTES,
) -> int:
    """Decode, validate, and persist a base64-encoded `git archive --format=tar.gz` snapshot."""
    cleaned = "".join(snapshot_b64.split())
    if not cleaned:
        raise ValueError("Repository snapshot payload is empty.")
    # Fast upper-bound check before allocating full decoded bytes
    if max_bytes > 0 and (len(cleaned) * 3) // 4 > max_bytes + 64:
        raise RepoSnapshotTooLargeError(
            f"Repository snapshot exceeds the {max_bytes / (1024 * 1024):.1f} MiB compressed limit."
        )
    try:
        raw = base64.b64decode(cleaned, validate=True)
    except (binascii.Error, ValueError) as exc:
        raise ValueError(f"Invalid base64 repository snapshot: {exc}") from exc

    if not raw:
        raise ValueError("Decoded repository snapshot is empty.")
    if max_bytes > 0 and len(raw) > max_bytes:
        raise RepoSnapshotTooLargeError(
            f"Repository snapshot is {len(raw) / (1024 * 1024):.1f} MiB, "
            f"exceeding the {max_bytes / (1024 * 1024):.1f} MiB limit."
        )

    try:
        with tarfile.open(fileobj=io.BytesIO(raw), mode="r:gz") as tf:
            for member in tf.getmembers():
                posix = PurePosixPath(member.name)
                if posix.is_absolute() or ".." in posix.parts:
                    raise ValueError(f"Unsafe path in repository snapshot: `{member.name}`")
                if member.isdev() or member.isfifo():
                    raise ValueError(
                        f"Unsupported special file in repository snapshot: `{member.name}`"
                    )
                if member.issym() or member.islnk():
                    target_posix = PurePosixPath(member.linkname)
                    if target_posix.is_absolute():
                        raise ValueError(
                            f"Absolute link target in repository snapshot: `{member.name}` -> `{member.linkname}`"
                        )
                    resolved = os.path.normpath(
                        str(posix.parent / target_posix) if member.issym() else str(target_posix)
                    )
                    if resolved == ".." or resolved.startswith("../") or resolved.startswith("/"):
                        raise ValueError(
                            f"Escaping link target in repository snapshot: `{member.name}` -> `{member.linkname}`"
                        )
    except (tarfile.TarError, EOFError, OSError) as exc:
        raise ValueError(f"Invalid gzipped tar repository snapshot: {exc}") from exc

    dest_path.parent.mkdir(parents=True, exist_ok=True)
    dest_path.write_bytes(raw)
    return len(raw)


def check_github_repo_access_sync(
    mirrors_root: Path,
    github_repo: str,
    github_token: str,
    *,
    timeout_seconds: float = 10.0,
) -> tuple[bool, bool, str]:
    """Check whether the controller can fetch `github_repo` via `git ls-remote`.

    Returns `(accessible, has_mirror, reason)`. Never prompts interactively.
    """
    clean_repo = (github_repo or "").strip().removesuffix(".git")
    mirror_path = mirror_dir_for_repo(mirrors_root, clean_repo) if clean_repo else mirrors_root
    has_mirror = bool(clean_repo) and (mirror_path / "HEAD").exists()

    if (
        not clean_repo
        or clean_repo.startswith("local/")
        or not _GITHUB_SLUG_RE.fullmatch(clean_repo)
    ):
        return False, False, "local-only or non-GitHub repository slug"

    clean_url = f"https://github.com/{clean_repo}.git"
    cmd = ["git"]
    if github_token.strip():
        cmd.extend(
            [
                "-c",
                f"http.https://github.com/.extraheader={_github_basic_auth_header(github_token.strip())}",
            ]
        )
    cmd.extend(["ls-remote", "--exit-code", clean_url, "HEAD"])

    env = {**os.environ, "GIT_TERMINAL_PROMPT": "0"}
    try:
        proc = subprocess.run(
            cmd,
            env=env,
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
        )
    except subprocess.TimeoutExpired:
        return False, has_mirror, f"git ls-remote timed out after {int(timeout_seconds)}s"
    except OSError as exc:
        return False, has_mirror, str(exc)

    if proc.returncode == 0:
        return True, has_mirror, "ok"
    stderr = (proc.stderr or proc.stdout or "").strip()
    first_line = (
        stderr.splitlines()[0] if stderr else f"git ls-remote exited with code {proc.returncode}"
    )
    return False, has_mirror, first_line[:240]


async def check_github_repo_access(
    mirrors_root: Path,
    github_repo: str,
    github_token: str,
    *,
    timeout_seconds: float = 10.0,
) -> tuple[bool, bool, str]:
    return await asyncio.to_thread(
        check_github_repo_access_sync,
        mirrors_root,
        github_repo,
        github_token,
        timeout_seconds=timeout_seconds,
    )


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
    auth_args = (
        [
            "-c",
            f"http.https://github.com/.extraheader={_github_basic_auth_header(github_token.strip())}",
        ]
        if github_token.strip()
        else []
    )
    git_env = {**os.environ, "GIT_TERMINAL_PROMPT": "0"}

    if not (mirror_path / "HEAD").exists():
        log.info("git_mirror.cloning", repo=github_repo, path=str(mirror_path))
        subprocess.run(
            [
                "git",
                *auth_args,
                "clone",
                "--mirror",
                clean_url,
                str(mirror_path),
            ],
            env=git_env,
            check=True,
            capture_output=True,
            text=True,
        )
    else:
        # Ensure origin URL never holds embedded credentials, then fetch latest refs.
        subprocess.run(
            ["git", "-C", str(mirror_path), "remote", "set-url", "origin", clean_url],
            env=git_env,
            check=True,
            capture_output=True,
            text=True,
        )
        subprocess.run(
            [
                "git",
                "-C",
                str(mirror_path),
                *auth_args,
                "remote",
                "update",
                "--prune",
            ],
            env=git_env,
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
