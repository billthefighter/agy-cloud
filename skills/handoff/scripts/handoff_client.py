#!/usr/bin/env python3
"""Portable stdlib-only client for `agy-cloud` laptop <-> k3s session handoffs.

Supports:
- `start` (default for `/handoff`): Push current workspace diff + semantic state to k3s
  AND immediately enter sleep-resilient poll-on-wake mode to pull changes back when ready.
- `push` (`/handoff push`): Push handoff to k3s without polling.
- `watch` (`/handoff watch`): Poll an active handoff across laptop sleep/wake and pull when done.
- `pull` (`/handoff pull`): Pull and apply the resulting patch from the latest or specified handoff.
- `status` (`/handoff status`): Inspect remote handoff status.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import ssl
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

TERMINAL_STATUSES = {
    "awaiting_review",
    "completed",
    "failed",
    "pr_opened",
    "resolved",
    "rejected",
    "ignored",
}
DEFAULT_MAX_INLINE_PATCH_BYTES = 5 * 1024 * 1024  # 5 MiB

_MODEL_SHORTHANDS: dict[str, str] = {
    "hydrogen": "gemini-4-hydrogen",
    "neon": "gemini-4-neon",
    "carbon": "gemini-4-carbon",
    "argon": "gemini-4-argon",
    "flash": "gemini-3.8-flash",
}
_EFFORT_TIERS = ("low", "medium", "high")


def normalize_model_with_effort(model: str = "", effort: str = "") -> str:
    raw_model = (model or "").strip() or os.environ.get("AGY_HANDOFF_MODEL", "").strip()
    raw_effort = (effort or "").strip().lower()
    if raw_effort and raw_effort not in _EFFORT_TIERS:
        raise ValueError(f"Invalid --effort `{effort}`; expected one of: low, medium, high.")
    if not raw_model:
        return ""
    cleaned = re.sub(r"[()]", " ", raw_model).strip()
    cleaned = re.sub(r"\s+", "-", cleaned).lower()
    existing_effort = ""
    stem = cleaned
    for tier in _EFFORT_TIERS:
        suffix = f"-{tier}"
        if cleaned.endswith(suffix):
            existing_effort = tier
            stem = cleaned[: -len(suffix)]
            break
    canonical_stem = _MODEL_SHORTHANDS.get(stem, stem)
    final_effort = raw_effort or existing_effort
    return f"{canonical_stem}-{final_effort}" if final_effort else canonical_stem


def _parse_dotenv_file(path: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    if not path.is_file():
        return out
    for raw_line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        key = k.strip().removeprefix("export ").strip()
        val = v.strip().strip('"').strip("'")
        out[key] = val
    return out


def resolve_connection_config(
    workdir: Path,
    *,
    explicit_url: str = "",
    explicit_token: str = "",
) -> tuple[str, str, str]:
    """Resolve `(base_url, api_token, ca_bundle_or_empty)` from flags, env, dotenv, or ~/.config."""
    env_vars: dict[str, str] = {}
    for candidate in [
        Path.home() / ".config" / "agy-cloud" / "config.env",
        workdir / ".agy-handoff.env",
        workdir / ".env",
    ]:
        env_vars.update(_parse_dotenv_file(candidate))
    env_vars.update(os.environ)

    url = (
        explicit_url.strip()
        or env_vars.get("AGY_HANDOFF_URL", "").strip()
        or env_vars.get("AGY_CLOUD_URL", "").strip()
        or "http://localhost:8080"
    ).rstrip("/")

    token = (
        explicit_token.strip()
        or env_vars.get("AGY_HANDOFF_TOKEN", "").strip()
        or env_vars.get("AGY_CLOUD_TOKEN", "").strip()
        or env_vars.get("PWA_API_TOKEN", "").strip()
    )
    ca_cert = env_vars.get("AGY_HANDOFF_CA_CERT", "").strip()
    return url, token, ca_cert


def detect_git_metadata(workdir: Path) -> dict[str, str]:
    """Extract `github_repo`, `base_branch`, `base_sha`, and `patch` from `workdir`.

    When `HEAD` has unpushed local commits not reachable from `origin/*`, resolves the
    closest `origin/*` merge-base so the remote pod can check out a reachable commit and
    receive both unpushed commits and uncommitted edits as a squashed `patch`.
    """

    def _git(*args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            ["git", "-C", str(workdir), *args],
            check=check,
            capture_output=True,
            text=True,
        )

    remote_url = _git("remote", "get-url", "origin", check=False).stdout.strip()
    github_repo = ""
    m = re.search(r"github\.com[:/]([A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+?)(?:\.git)?$", remote_url)
    if m:
        github_repo = m.group(1)

    local_branch = _git("rev-parse", "--abbrev-ref", "HEAD", check=False).stdout.strip() or "main"
    head_sha = _git("rev-parse", "HEAD", check=False).stdout.strip()
    base_branch = local_branch if local_branch != "HEAD" else "main"
    base_sha = head_sha
    diff_target = "HEAD"

    refs_proc = _git("for-each-ref", "--format=%(refname:short)", "refs/remotes/origin", check=False)
    origin_refs = [
        r.strip()
        for r in refs_proc.stdout.splitlines()
        if r.strip() and r.strip() not in {"origin", "origin/HEAD"}
    ]
    if head_sha and refs_proc.returncode == 0 and origin_refs:
        contains_proc = _git(
            "branch", "-r", "--contains", head_sha, "--format=%(refname:short)", check=False
        )
        containing = [
            r.strip()
            for r in contains_proc.stdout.splitlines()
            if r.strip().startswith("origin/") and r.strip() != "origin/HEAD"
        ]
        if containing:
            preferred = [
                f"origin/{local_branch}" if local_branch != "HEAD" else "",
                "origin/main",
                "origin/master",
            ]
            chosen = next((ref for ref in preferred if ref and ref in containing), containing[0])
            base_branch = chosen.removeprefix("origin/")
        else:
            candidates: list[str] = []
            up_proc = _git(
                "rev-parse", "--abbrev-ref", "--symbolic-full-name", "@{u}", check=False
            )
            if up_proc.returncode == 0 and up_proc.stdout.strip() in origin_refs:
                candidates.append(up_proc.stdout.strip())
            if local_branch != "HEAD" and f"origin/{local_branch}" in origin_refs:
                candidates.append(f"origin/{local_branch}")
            sym_proc = _git("symbolic-ref", "--short", "refs/remotes/origin/HEAD", check=False)
            if sym_proc.returncode == 0 and sym_proc.stdout.strip() in origin_refs:
                candidates.append(sym_proc.stdout.strip())
            for d in ("origin/main", "origin/master"):
                if d in origin_refs:
                    candidates.append(d)
            candidates.extend(origin_refs)
            seen: set[str] = set()
            for cand in candidates:
                if cand in seen:
                    continue
                seen.add(cand)
                mb_proc = _git("merge-base", head_sha, cand, check=False)
                if mb_proc.returncode == 0 and mb_proc.stdout.strip():
                    base_branch = cand.removeprefix("origin/")
                    base_sha = mb_proc.stdout.strip()
                    if base_sha != head_sha:
                        diff_target = base_sha
                    break

    # Mark untracked files intent-to-add so `git diff --binary` captures new files
    subprocess.run(["git", "-C", str(workdir), "add", "-N", "."], check=False, capture_output=True)
    patch = _git("diff", "--binary", diff_target, check=False).stdout

    return {
        "github_repo": github_repo,
        "base_branch": base_branch,
        "base_sha": base_sha,
        "local_branch": local_branch,
        "head_sha": head_sha,
        "patch": patch,
    }


def _state_file(workdir: Path) -> Path:
    git_dir = workdir / ".git"
    if git_dir.is_dir():
        return git_dir / "agy_handoff_state.json"
    return workdir / ".agy_handoff_state.json"


def save_local_handoff_state(workdir: Path, state: dict[str, Any]) -> None:
    _state_file(workdir).write_text(json.dumps(state, indent=2), encoding="utf-8")


def load_local_handoff_state(workdir: Path) -> dict[str, Any] | None:
    sf = _state_file(workdir)
    if not sf.is_file():
        return None
    try:
        return json.loads(sf.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


def http_json(
    method: str,
    url: str,
    *,
    token: str = "",
    payload: dict[str, Any] | None = None,
    ca_cert: str = "",
    insecure: bool = False,
    timeout: float = 20.0,
) -> dict[str, Any]:
    headers = {"Accept": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
        headers["X-Agy-Token"] = token
    body_bytes: bytes | None = None
    if payload is not None:
        headers["Content-Type"] = "application/json"
        body_bytes = json.dumps(payload).encode("utf-8")

    req = urllib.request.Request(url, data=body_bytes, headers=headers, method=method)
    ctx: ssl.SSLContext | None = None
    if url.startswith("https://"):
        if insecure:
            ctx = ssl._create_unverified_context()
        elif ca_cert and Path(ca_cert).is_file():
            ctx = ssl.create_default_context(cafile=ca_cert)

    with urllib.request.urlopen(req, timeout=timeout, context=ctx) as resp:
        raw = resp.read().decode("utf-8")
        return json.loads(raw) if raw.strip() else {}


def apply_remote_patch_safely(
    workdir: Path,
    *,
    patch_text: str,
    expected_base_sha: str = "",
) -> tuple[bool, str]:
    """Apply the pod's resulting `fix.patch` cleanly to `workdir` with pre-pull backup & rollback."""
    if not patch_text.strip():
        return True, "Remote task produced no code diff (working tree left unchanged)."

    git_dir = workdir / ".git" if (workdir / ".git").is_dir() else workdir
    backup_patch_path = git_dir / "agy_handoff_pre_pull_backup.patch"
    remote_patch_path = git_dir / "agy_handoff_latest.patch"
    remote_patch_path.write_text(patch_text, encoding="utf-8")

    # Capture current local uncommitted diff into a backup file before resetting
    subprocess.run(["git", "-C", str(workdir), "add", "-N", "."], check=False, capture_output=True)
    local_diff = subprocess.run(
        ["git", "-C", str(workdir), "diff", "--binary", "HEAD"],
        check=False,
        capture_output=True,
        text=True,
    ).stdout
    backup_patch_path.write_text(local_diff, encoding="utf-8")

    current_sha = subprocess.run(
        ["git", "-C", str(workdir), "rev-parse", "HEAD"],
        check=False,
        capture_output=True,
        text=True,
    ).stdout.strip()

    if expected_base_sha and current_sha and current_sha != expected_base_sha:
        # If expected_base_sha is an ancestor of local HEAD (unpushed local commits),
        # rebase the cumulative remote patch onto HEAD via a temporary detached worktree.
        is_anc = subprocess.run(
            ["git", "-C", str(workdir), "merge-base", "--is-ancestor", expected_base_sha, "HEAD"],
            check=False,
            capture_output=True,
        )
        if is_anc.returncode == 0:
            with tempfile.TemporaryDirectory(prefix="agy-handoff-rebase-") as tmp_root:
                wt_dir = Path(tmp_root) / "wt"
                add_wt = subprocess.run(
                    [
                        "git",
                        "-C",
                        str(workdir),
                        "worktree",
                        "add",
                        "--detach",
                        str(wt_dir),
                        expected_base_sha,
                    ],
                    check=False,
                    capture_output=True,
                    text=True,
                )
                if add_wt.returncode == 0:
                    try:
                        ap = subprocess.run(
                            ["git", "-C", str(wt_dir), "apply", "--whitespace=nowarn", str(remote_patch_path)],
                            check=False,
                            capture_output=True,
                            text=True,
                        )
                        if ap.returncode != 0:
                            ap = subprocess.run(
                                ["git", "-C", str(wt_dir), "apply", "--3way", "--whitespace=nowarn", str(remote_patch_path)],
                                check=False,
                                capture_output=True,
                                text=True,
                            )
                        if ap.returncode == 0:
                            subprocess.run(["git", "-C", str(wt_dir), "add", "-A"], check=False, capture_output=True)
                            tree = subprocess.run(
                                ["git", "-C", str(wt_dir), "write-tree"],
                                check=False,
                                capture_output=True,
                                text=True,
                            ).stdout.strip()
                            if tree:
                                rebased = subprocess.run(
                                    ["git", "-C", str(workdir), "diff", "--binary", "HEAD", tree],
                                    check=False,
                                    capture_output=True,
                                    text=True,
                                ).stdout
                                if not rebased.strip():
                                    return True, f"Working tree already matches remote pod state at `{current_sha[:8]}`."
                                rebased_path = git_dir / "agy_handoff_rebased.patch"
                                rebased_path.write_text(rebased, encoding="utf-8")
                                subprocess.run(["git", "-C", str(workdir), "reset", "--hard", "HEAD"], check=False, capture_output=True)
                                subprocess.run(["git", "-C", str(workdir), "clean", "-fd"], check=False, capture_output=True)
                                apply_rebased = subprocess.run(
                                    ["git", "-C", str(workdir), "apply", "--whitespace=nowarn", str(rebased_path)],
                                    check=False,
                                    capture_output=True,
                                    text=True,
                                )
                                if apply_rebased.returncode == 0:
                                    return True, f"Rebased & applied remote delta onto unpushed HEAD `{current_sha[:8]}` (backup at `{backup_patch_path}`)."
                    finally:
                        subprocess.run(
                            ["git", "-C", str(workdir), "worktree", "remove", "--force", str(wt_dir)],
                            check=False,
                            capture_output=True,
                        )

        # Fallback: attempt 3-way apply without resetting HEAD
        proc = subprocess.run(
            ["git", "-C", str(workdir), "apply", "--3way", str(remote_patch_path)],
            check=False,
            capture_output=True,
            text=True,
        )
        if proc.returncode == 0:
            return True, f"Applied remote patch via 3-way merge onto `{current_sha[:8]}`."
        return (
            False,
            f"3-way apply failed (local HEAD `{current_sha[:8]}` != handoff base `{expected_base_sha[:8]}`). "
            f"Patch saved at `{remote_patch_path}`: {proc.stderr.strip()}",
        )

    # Local HEAD matches handoff base_sha: reset working tree to HEAD so combined fix.patch applies cleanly
    subprocess.run(
        ["git", "-C", str(workdir), "reset", "--hard", "HEAD"],
        check=False,
        capture_output=True,
    )
    subprocess.run(
        ["git", "-C", str(workdir), "clean", "-fd"],
        check=False,
        capture_output=True,
    )
    apply_proc = subprocess.run(
        ["git", "-C", str(workdir), "apply", str(remote_patch_path)],
        check=False,
        capture_output=True,
        text=True,
    )
    if apply_proc.returncode == 0:
        return (
            True,
            f"Applied remote patch cleanly to working tree (backup at `{backup_patch_path}`).",
        )

    # Roll back to pre-pull local diff so zero local work is ever lost
    if local_diff.strip():
        subprocess.run(
            ["git", "-C", str(workdir), "apply", str(backup_patch_path)],
            check=False,
            capture_output=True,
        )
    return (
        False,
        f"Failed to apply remote patch (restored pre-pull working tree; remote patch saved at `{remote_patch_path}`): "
        f"{apply_proc.stderr.strip()}",
    )


def push_handoff(
    workdir: Path,
    *,
    base_url: str,
    token: str,
    ca_cert: str,
    insecure: bool,
    prompt: str,
    semantic_state: str = "",
    github_repo: str = "",
    verify_hint: str = "",
    focus_subdir: str = "",
    model: str = "gemini-4-hydrogen",
    local_conversation_id: str = "",
) -> dict[str, Any]:
    git_meta = detect_git_metadata(workdir)
    repo = github_repo.strip() or git_meta["github_repo"]
    if not repo:
        raise ValueError(
            "Could not detect GitHub repository from `git remote get-url origin`. Pass `--repo owner/name`."
        )

    patch_text = git_meta["patch"]
    max_patch_bytes = int(
        os.environ.get("AGY_HANDOFF_MAX_PATCH_BYTES", str(DEFAULT_MAX_INLINE_PATCH_BYTES))
    )
    patch_bytes = len(patch_text.encode("utf-8"))
    if max_patch_bytes > 0 and patch_bytes > max_patch_bytes:
        patch_mb = patch_bytes / (1024 * 1024)
        limit_mb = max_patch_bytes / (1024 * 1024)
        local_branch = git_meta.get("local_branch") or git_meta["base_branch"]
        head_sha = git_meta.get("head_sha") or ""
        if head_sha and head_sha != git_meta["base_sha"] and local_branch != "HEAD":
            raise RuntimeError(
                f"Inline handoff patch is {patch_mb:.1f} MiB (exceeds {limit_mb:.1f} MiB limit) "
                f"because local branch `{local_branch}` ({head_sha[:8]}) has unpushed commits "
                f"diverged from `{git_meta['base_branch']}` ({git_meta['base_sha'][:8]}). "
                f"Run `git push -u origin {local_branch}` first, then retry `/handoff`."
            )
        raise RuntimeError(
            f"Inline handoff patch is {patch_mb:.1f} MiB (exceeds {limit_mb:.1f} MiB limit). "
            "Commit & push large/binary changes or set AGY_HANDOFF_MAX_PATCH_BYTES to override."
        )

    payload = {
        "repo": repo,
        "github_repo": repo,
        "branch": git_meta["base_branch"],
        "base_branch": git_meta["base_branch"],
        "base_sha": git_meta["base_sha"],
        "base_patch": patch_text,
        "patch": patch_text,
        "prompt": prompt,
        "semantic_state": semantic_state,
        "verify_hint": verify_hint,
        "focus_subdir": focus_subdir,
        "model": model,
        "source_conversation_id": local_conversation_id or "",
        "local_conversation_id": local_conversation_id or None,
        "workspace_path": str(workdir),
    }
    resp = http_json(
        "POST",
        f"{base_url}/api/handoff",
        token=token,
        payload=payload,
        ca_cert=ca_cert,
        insecure=insecure,
    )
    resolved_id = resp.get("handoff_id") or resp.get("issue_id") or resp.get("short_id")
    if resolved_id and not resp.get("handoff_id"):
        resp["handoff_id"] = resolved_id
    save_local_handoff_state(
        workdir,
        {
            "handoff_id": resolved_id,
            "short_id": resp.get("short_id"),
            "github_repo": repo,
            "base_sha": git_meta["base_sha"],
            "base_url": base_url,
            "pushed_at": time.time(),
            "pulled": False,
        },
    )
    return resp


def poll_until_ready_and_pull(
    workdir: Path,
    *,
    base_url: str,
    token: str,
    ca_cert: str,
    insecure: bool,
    handoff_id: str,
    poll_interval: float = 15.0,
    max_wait_seconds: float = 86400.0,
    apply_patch: bool = True,
) -> dict[str, Any]:
    """Sleep-resilient poll loop that survives laptop lid close / sleep and pulls on wake."""
    deadline = time.time() + max_wait_seconds
    last_wall = time.time()
    consecutive_net_errors = 0

    print(
        f"[agy-handoff] Watching remote handoff `{handoff_id}` at `{base_url}` "
        f"(sleep-resilient poll every {poll_interval:g}s)...",
        flush=True,
    )

    while True:
        now_wall = time.time()
        elapsed_since_last = now_wall - last_wall
        if elapsed_since_last > max(poll_interval * 2.5, 45.0):
            print(
                f"[agy-handoff] Detected system wake after {int(elapsed_since_last)}s sleep — "
                f"polling `{handoff_id}` immediately...",
                flush=True,
            )
        last_wall = now_wall

        try:
            data = http_json(
                "GET",
                f"{base_url}/api/handoff/{urllib.parse.quote(handoff_id)}?include_patch=true",
                token=token,
                ca_cert=ca_cert,
                insecure=insecure,
                timeout=15.0,
            )
            consecutive_net_errors = 0
            status = str(data.get("status") or "").lower()
            is_terminal = bool(data.get("is_terminal")) or status in TERMINAL_STATUSES

            if is_terminal:
                patch_text = str(data.get("patch") or data.get("diff") or "")
                base_sha = str(data.get("base_sha") or "")
                applied_ok = True
                apply_msg = "Patch application skipped (--no-apply)."
                if apply_patch:
                    applied_ok, apply_msg = apply_remote_patch_safely(
                        workdir,
                        patch_text=patch_text,
                        expected_base_sha=base_sha,
                    )
                    if applied_ok:
                        try:
                            http_json(
                                "POST",
                                f"{base_url}/api/handoff/{urllib.parse.quote(handoff_id)}/mark-pulled",
                                token=token,
                                ca_cert=ca_cert,
                                insecure=insecure,
                            )
                        except Exception:
                            pass
                        saved = load_local_handoff_state(workdir) or {}
                        saved["pulled"] = True
                        saved["pulled_at"] = time.time()
                        save_local_handoff_state(workdir, saved)

                data["applied_ok"] = applied_ok
                data["apply_message"] = apply_msg
                return data
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            consecutive_net_errors += 1
            if consecutive_net_errors == 1 or consecutive_net_errors % 10 == 0:
                print(
                    f"[agy-handoff] Network/VPN unreachable during poll #{consecutive_net_errors} "
                    f"({exc}); waiting for wake/reconnect...",
                    flush=True,
                )

        if time.time() >= deadline:
            raise TimeoutError(f"Timed out waiting for handoff `{handoff_id}`.")
        time.sleep(poll_interval)


def _format_summary(data: dict[str, Any]) -> str:
    short_id = data.get("short_id") or data.get("handoff_id")
    status = data.get("status")
    repo = data.get("github_repo")
    turn = data.get("turn", 1)
    model = data.get("model", "")
    lines = [
        f"## Cloud Handoff `{short_id}` — Status: `{status}`",
        f"- **Handoff ID:** `{data.get('handoff_id')}`",
        f"- **Repository:** `{repo}` (turn `{turn}`, model `{model}`)",
    ]
    if data.get("patch_stats"):
        lines.append(f"- **Diff Stats:** `{data['patch_stats']}`")
    if "apply_message" in data:
        lines.append(f"- **Local Working Tree:** {data['apply_message']}")
    report = data.get("report") or {}
    if isinstance(report, dict) and report:
        if report.get("root_cause"):
            lines.append(f"- **Analysis / Approach:** {report['root_cause']}")
        if report.get("fix_summary"):
            lines.append(f"- **Changes Summary:** {report['fix_summary']}")
        if report.get("affected_files"):
            files_str = ", ".join(f"`{f}`" for f in report["affected_files"])
            lines.append(f"- **Affected Files:** {files_str}")
        if "tests_passed" in report:
            lines.append(f"- **Tests Passed:** `{report['tests_passed']}`")
    progress_steps = data.get("progress_steps") or []
    if isinstance(progress_steps, list) and progress_steps:
        lines.append(f"- **Completed Steps:** `{len(progress_steps)}`")
        for step in progress_steps[-5:]:
            if not isinstance(step, dict):
                continue
            stype = step.get("step_type") or "step"
            sidx = step.get("step_index")
            text = str(step.get("text") or "").strip().replace("\n", " ")
            if len(text) > 180:
                text = text[:177] + "..."
            label = f"#{sidx} `{stype}`" if sidx is not None else f"`{stype}`"
            lines.append(f"  - {label}: {text}" if text else f"  - {label}")
    if data.get("last_error"):
        lines.append(f"- **Error:** `{data['last_error']}`")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="agy-cloud laptop <-> k3s session handoff client")
    sub = parser.add_subparsers(dest="subcommand")

    def _add_common(p: argparse.ArgumentParser) -> None:
        p.add_argument("--workdir", default=".", help="Git workspace directory")
        p.add_argument("--url", default="", help="Base URL of agy-cloud / agent-tasks gateway")
        p.add_argument("--token", default="", help="Bearer API token (or PWA_API_TOKEN)")
        p.add_argument("--ca-cert", default="", help="Optional CA cert path")
        p.add_argument("--insecure", action="store_true", help="Skip TLS verification")
        p.add_argument("--json", action="store_true", help="Emit JSON output")

    # `start`: Default mode for `/handoff` (pushes if prompt provided or no active handoff, then polls on wake)
    p_start = sub.add_parser("start", help="Push handoff (if needed) and start poll-on-wake mode")
    _add_common(p_start)
    p_start.add_argument("--prompt", default="", help="Task instructions for the k3s pod")
    p_start.add_argument("--state", default="", help="Compact Semantic State (s_t) summary")
    p_start.add_argument("--repo", default="", help="Override GitHub repo (owner/name)")
    p_start.add_argument("--verify-hint", default="", help="Verification command for the worker")
    p_start.add_argument("--focus-subdir", default="", help="Focus subdirectory inside repo")
    p_start.add_argument(
        "--model",
        default="",
        help="Worker model ID or shorthand (e.g. gemini-4-neon-medium, claude-sonnet-5-5-medium)",
    )
    p_start.add_argument(
        "--effort",
        default="",
        choices=("", "low", "medium", "high"),
        help="Optional effort tier (low, medium, high) combined with --model",
    )
    p_start.add_argument(
        "--handoff-id", default="", help="Watch an existing handoff ID instead of pushing"
    )
    p_start.add_argument("--poll-interval", type=float, default=15.0, help="Seconds between polls")
    p_start.add_argument(
        "--no-apply", action="store_true", help="Do not auto-apply patch on completion"
    )

    # `push`: Fire-and-forget push without polling
    p_push = sub.add_parser(
        "push", help="Push current session & working tree diff to k3s without polling"
    )
    _add_common(p_push)
    p_push.add_argument("--prompt", required=True, help="Task instructions for the k3s pod")
    p_push.add_argument("--state", default="", help="Compact Semantic State (s_t) summary")
    p_push.add_argument("--repo", default="", help="Override GitHub repo (owner/name)")
    p_push.add_argument("--verify-hint", default="", help="Verification command for the worker")
    p_push.add_argument("--focus-subdir", default="", help="Focus subdirectory inside repo")
    p_push.add_argument(
        "--model",
        default="",
        help="Worker model ID or shorthand (e.g. gemini-4-neon-medium, claude-sonnet-5-5-medium)",
    )
    p_push.add_argument(
        "--effort",
        default="",
        choices=("", "low", "medium", "high"),
        help="Optional effort tier (low, medium, high) combined with --model",
    )

    # `watch`: Poll an existing handoff on wake
    p_watch = sub.add_parser(
        "watch", help="Poll an existing handoff across sleep/wake and pull on completion"
    )
    _add_common(p_watch)
    p_watch.add_argument("--handoff-id", default="", help="Handoff ID (defaults to latest active)")
    p_watch.add_argument("--poll-interval", type=float, default=15.0, help="Seconds between polls")
    p_watch.add_argument(
        "--no-apply", action="store_true", help="Do not auto-apply patch on completion"
    )

    # `pull`: Immediate pull
    p_pull = sub.add_parser("pull", help="Immediately pull and apply the remote patch")
    _add_common(p_pull)
    p_pull.add_argument("--handoff-id", default="", help="Handoff ID (defaults to latest)")
    p_pull.add_argument(
        "--no-apply", action="store_true", help="Fetch report without applying patch"
    )

    # `status`: Inspect status
    p_status = sub.add_parser("status", help="Check remote handoff status")
    _add_common(p_status)
    p_status.add_argument("--handoff-id", default="", help="Handoff ID (defaults to latest)")

    args = parser.parse_args(argv)
    cmd = args.subcommand or "start"
    workdir = Path(getattr(args, "workdir", ".")).resolve()
    base_url, token, env_ca = resolve_connection_config(
        workdir,
        explicit_url=getattr(args, "url", ""),
        explicit_token=getattr(args, "token", ""),
    )
    ca_cert = getattr(args, "ca_cert", "") or env_ca
    insecure = bool(getattr(args, "insecure", False))

    if cmd == "push":
        created = push_handoff(
            workdir,
            base_url=base_url,
            token=token,
            ca_cert=ca_cert,
            insecure=insecure,
            prompt=args.prompt,
            semantic_state=args.state,
            github_repo=args.repo,
            verify_hint=args.verify_hint,
            focus_subdir=args.focus_subdir,
            model=normalize_model_with_effort(args.model, args.effort) or "gemini-4-hydrogen",
        )
        print(json.dumps(created, indent=2) if args.json else _format_summary(created))
        return 0

    if cmd == "start":
        handoff_id = args.handoff_id.strip()
        if not handoff_id and args.prompt.strip():
            created = push_handoff(
                workdir,
                base_url=base_url,
                token=token,
                ca_cert=ca_cert,
                insecure=insecure,
                prompt=args.prompt,
                semantic_state=args.state,
                github_repo=args.repo,
                verify_hint=args.verify_hint,
                focus_subdir=args.focus_subdir,
                model=normalize_model_with_effort(args.model, args.effort) or "gemini-4-hydrogen",
            )
            handoff_id = str(
                created.get("handoff_id") or created.get("issue_id") or created.get("short_id") or ""
            )
            print(_format_summary(created), flush=True)
        elif not handoff_id:
            local_state = load_local_handoff_state(workdir)
            if local_state and local_state.get("handoff_id") and not local_state.get("pulled"):
                handoff_id = str(local_state["handoff_id"])
            else:
                latest = http_json(
                    "GET",
                    f"{base_url}/api/handoff/latest?only_unpulled=true&include_patch=false",
                    token=token,
                    ca_cert=ca_cert,
                    insecure=insecure,
                )
                handoff_id = str(
                    latest.get("handoff_id") or latest.get("issue_id") or latest.get("short_id") or "latest"
                )

        result = poll_until_ready_and_pull(
            workdir,
            base_url=base_url,
            token=token,
            ca_cert=ca_cert,
            insecure=insecure,
            handoff_id=handoff_id,
            poll_interval=args.poll_interval,
            apply_patch=not args.no_apply,
        )
        print(json.dumps(result, indent=2) if args.json else _format_summary(result))
        return 0

    if cmd == "watch":
        handoff_id = args.handoff_id.strip()
        if not handoff_id:
            local_state = load_local_handoff_state(workdir)
            handoff_id = str((local_state or {}).get("handoff_id") or "latest")
        result = poll_until_ready_and_pull(
            workdir,
            base_url=base_url,
            token=token,
            ca_cert=ca_cert,
            insecure=insecure,
            handoff_id=handoff_id,
            poll_interval=args.poll_interval,
            apply_patch=not args.no_apply,
        )
        print(json.dumps(result, indent=2) if args.json else _format_summary(result))
        return 0

    if cmd in {"pull", "status"}:
        handoff_id = args.handoff_id.strip()
        if not handoff_id:
            local_state = load_local_handoff_state(workdir)
            handoff_id = str((local_state or {}).get("handoff_id") or "latest")
        include_patch = "true" if cmd == "pull" else "false"
        endpoint = (
            f"{base_url}/api/handoff/latest?include_patch={include_patch}"
            if handoff_id == "latest"
            else f"{base_url}/api/handoff/{urllib.parse.quote(handoff_id)}?include_patch={include_patch}"
        )
        data = http_json(
            "GET",
            endpoint,
            token=token,
            ca_cert=ca_cert,
            insecure=insecure,
        )
        if cmd == "pull" and not getattr(args, "no_apply", False):
            ok, msg = apply_remote_patch_safely(
                workdir,
                patch_text=str(data.get("patch") or data.get("diff") or ""),
                expected_base_sha=str(data.get("base_sha") or ""),
            )
            data["applied_ok"] = ok
            data["apply_message"] = msg
            resolved_pull_id = data.get("handoff_id") or data.get("issue_id") or data.get("short_id")
            if ok and resolved_pull_id:
                try:
                    http_json(
                        "POST",
                        f"{base_url}/api/handoff/{urllib.parse.quote(str(resolved_pull_id))}/mark-pulled",
                        token=token,
                        ca_cert=ca_cert,
                        insecure=insecure,
                    )
                except Exception:
                    pass
        print(json.dumps(data, indent=2) if args.json else _format_summary(data))
        return 0

    return 1


if __name__ == "__main__":
    sys.exit(main())
