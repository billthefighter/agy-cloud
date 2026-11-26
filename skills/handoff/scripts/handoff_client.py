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
import base64
import hashlib
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

try:
    import fcntl
except ImportError:  # pragma: no cover - non-POSIX fallback
    fcntl = None  # type: ignore[assignment]

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
DEFAULT_MAX_SNAPSHOT_BYTES = 20 * 1024 * 1024  # 20 MiB compressed

_MODEL_SHORTHANDS: dict[str, str] = {
    "hydrogen": "gemini-4-hydrogen",
    "neon": "gemini-4-neon",
    "carbon": "gemini-4-carbon",
    "argon": "gemini-4-argon",
    "flash": "gemini-3.8-flash",
}
_EFFORT_TIERS = ("low", "medium", "high")


def normalize_model_with_effort(model: str = "", effort: str = "") -> str:
    cli_model = (model or "").strip()
    if cli_model.lower() == "auto":
        cli_model = ""
    raw_model = cli_model or os.environ.get("AGY_HANDOFF_MODEL", "").strip()
    if raw_model.lower() == "auto":
        raw_model = ""
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


def detect_active_model_from_transcripts(
    workdir: Path,
    *,
    brain_dir: Path | None = None,
) -> str:
    """Auto-detect the active model from the newest Antigravity `transcript.jsonl` referencing `workdir`."""
    root = (
        brain_dir if brain_dir is not None else (Path.home() / ".gemini" / "antigravity" / "brain")
    )
    if not root.is_dir():
        return ""
    target_path = str(workdir.resolve())
    candidates: list[tuple[float, Path]] = []
    for pattern in ("*/.system_generated/logs/transcript.jsonl", "*/transcript.jsonl"):
        for path in root.glob(pattern):
            try:
                candidates.append((path.stat().st_mtime, path))
            except OSError:
                continue
    candidates.sort(key=lambda item: item[0], reverse=True)

    for _, transcript_path in candidates[:25]:
        try:
            text = transcript_path.read_text(encoding="utf-8", errors="ignore")
        except OSError:
            continue
        if target_path not in text:
            continue
        for raw_line in reversed(text.splitlines()):
            line = raw_line.strip()
            if not line or "PLANNER_RESPONSE" not in line:
                continue
            try:
                obj = json.loads(line)
            except ValueError:
                continue
            if not isinstance(obj, dict) or obj.get("type") != "PLANNER_RESPONSE":
                continue
            raw_model = obj.get("model")
            if isinstance(raw_model, str) and raw_model.strip():
                normalized = normalize_model_with_effort(raw_model.strip())
                if normalized:
                    return normalized
    return ""


def resolve_model_for_handoff(
    workdir: Path,
    *,
    model: str = "",
    effort: str = "",
    brain_dir: Path | None = None,
) -> str:
    """Resolve explicit/env model, else auto-detect from transcripts, else fall back to hydrogen."""
    explicit = normalize_model_with_effort(model, effort)
    if explicit:
        return explicit
    detected = detect_active_model_from_transcripts(workdir, brain_dir=brain_dir)
    if detected:
        return normalize_model_with_effort(detected, effort)
    return normalize_model_with_effort("gemini-4-hydrogen", effort)


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


def _extract_conn_from_mapping(mapping: dict[str, str]) -> tuple[str, str, str, str]:
    explicit_url = (
        mapping.get("AGY_HANDOFF_URL", "").strip() or mapping.get("AGY_CLOUD_URL", "").strip()
    )
    netbird_host = mapping.get("NETBIRD_HOSTNAME", "").strip()
    netbird_url = f"https://{netbird_host}.netbird.cloud/agy" if netbird_host else ""
    token = (
        mapping.get("AGY_HANDOFF_TOKEN", "").strip()
        or mapping.get("AGY_CLOUD_TOKEN", "").strip()
        or mapping.get("PWA_API_TOKEN", "").strip()
    )
    ca_cert = mapping.get("AGY_HANDOFF_CA_CERT", "").strip()
    return explicit_url, netbird_url, token, ca_cert


def resolve_connection_config(
    workdir: Path,
    *,
    explicit_url: str = "",
    explicit_token: str = "",
) -> tuple[str, str, str]:
    """Resolve `(base_url, api_token, ca_bundle_or_empty)` from flags, env, workspace dotenv, or ~/.config.

    Explicit `AGY_HANDOFF_URL` / `AGY_CLOUD_URL` across all layers (including `~/.config/agy-cloud/config.env`)
    takes precedence over generic `NETBIRD_HOSTNAME` in parent `.env` files. Nonexistent
    `AGY_HANDOFF_CA_CERT` paths are ignored.
    """
    # Layers ordered highest -> lowest priority:
    # 1. Explicit CLI flags
    # 2. Process environment variables
    # 3. Workspace (and parent directory) .agy-handoff.env / .env / .env.makesense (innermost first)
    # 4. User config ~/.config/agy-cloud/config.env
    layers: list[dict[str, str]] = [dict(os.environ)]
    for folder in [workdir, *list(workdir.parents)[:4]]:
        for fname in (".agy-handoff.env", ".env", ".env.makesense"):
            parsed = _parse_dotenv_file(folder / fname)
            if parsed:
                layers.append(parsed)
    user_cfg = _parse_dotenv_file(Path.home() / ".config" / "agy-cloud" / "config.env")
    if user_cfg:
        layers.append(user_cfg)

    url = explicit_url.strip()
    netbird_fallback_url = ""
    token = explicit_token.strip()
    ca_cert = ""
    for layer in layers:
        l_explicit_url, l_netbird_url, l_tok, l_ca = _extract_conn_from_mapping(layer)
        if not url and l_explicit_url:
            url = l_explicit_url
        if not netbird_fallback_url and l_netbird_url:
            netbird_fallback_url = l_netbird_url
        if not token and l_tok:
            token = l_tok
        if not ca_cert and l_ca:
            expanded_ca = Path(l_ca).expanduser()
            if expanded_ca.is_file():
                ca_cert = str(expanded_ca)

    final_url = url or netbird_fallback_url or "http://localhost:8080"
    return final_url.rstrip("/"), token, ca_cert


def _git_toplevel(path: Path) -> Path | None:
    proc = subprocess.run(
        ["git", "-C", str(path), "rev-parse", "--show-toplevel"],
        check=False,
        capture_output=True,
        text=True,
    )
    if proc.returncode == 0 and proc.stdout.strip():
        return Path(proc.stdout.strip()).resolve()
    return None


def _list_checked_out_submodules(workdir: Path) -> list[str]:
    """Return relative POSIX paths of checked-out git submodules declared in `.gitmodules`."""
    gitmodules = workdir / ".gitmodules"
    if not gitmodules.is_file():
        return []
    proc = subprocess.run(
        [
            "git",
            "-C",
            str(workdir),
            "config",
            "--file",
            ".gitmodules",
            "--get-regexp",
            r"^submodule\..*\.path$",
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        return []
    submodules: list[str] = []
    for line in proc.stdout.splitlines():
        parts = line.strip().split(None, 1)
        if len(parts) != 2:
            continue
        rel = parts[1].strip().strip("/")
        if rel and (workdir / rel / ".git").exists():
            submodules.append(rel)
    return submodules


def _rewrite_verify_hint_for_submodule(verify_hint: str, sub_rel: str) -> str:
    if not verify_hint or not sub_rel:
        return verify_hint
    prefix = f"{sub_rel.rstrip('/')}/"
    updated = verify_hint.replace(prefix, "")
    updated = re.sub(
        rf"(?<![A-Za-z0-9_./-]){re.escape(sub_rel.rstrip('/'))}(?=$|[\s,:;)'\"])",
        ".",
        updated,
    )
    return updated


def resolve_effective_workdir(
    workdir: Path,
    *,
    focus_subdir: str = "",
    prompt: str = "",
    verify_hint: str = "",
) -> tuple[Path, str, str, str]:
    """Detect if the handoff targets a git submodule inside `workdir` and retarget to it.

    Returns `(effective_workdir, effective_focus_subdir, effective_verify_hint, submodule_rel)`.
    In Git, superproject `git diff HEAD` never captures file edits inside a submodule; retargeting
    ensures both worker diff capture and local patch application operate on the submodule repo.
    """
    base_top = _git_toplevel(workdir) or workdir
    clean_focus = focus_subdir.strip().strip("/")

    if clean_focus:
        candidate = (workdir / clean_focus).resolve()
        probe = candidate if candidate.is_dir() else candidate.parent
        if probe.is_dir():
            sub_top = _git_toplevel(probe)
            if sub_top is not None and sub_top != base_top:
                try:
                    sub_rel = sub_top.relative_to(base_top).as_posix()
                except ValueError:
                    sub_rel = sub_top.name
                try:
                    new_focus = candidate.relative_to(sub_top).as_posix()
                    if new_focus == ".":
                        new_focus = ""
                except ValueError:
                    new_focus = ""
                return (
                    sub_top,
                    new_focus,
                    _rewrite_verify_hint_for_submodule(verify_hint, sub_rel),
                    sub_rel,
                )

    combined_text = f"{prompt}\n{verify_hint}"
    if combined_text.strip():
        submodules = _list_checked_out_submodules(base_top)
        matched = [
            sub_rel
            for sub_rel in submodules
            if re.search(
                rf"(?<![A-Za-z0-9_.-]){re.escape(sub_rel)}(?=$|[/\s,:;)'\"])",
                combined_text,
            )
        ]
        if len(matched) == 1:
            sub_rel = matched[0]
            sub_top = (base_top / sub_rel).resolve()
            return (
                sub_top,
                "",
                _rewrite_verify_hint_for_submodule(verify_hint, sub_rel),
                sub_rel,
            )

    return workdir, focus_subdir, verify_hint, ""


def create_repo_snapshot_b64(
    workdir: Path,
    *,
    ref: str = "HEAD",
    max_bytes: int | None = None,
) -> tuple[str, int]:
    """Pack tracked files at `ref` via `git archive --format=tar.gz` and return `(b64, raw_bytes)`."""
    limit = (
        max_bytes
        if max_bytes is not None
        else int(os.environ.get("AGY_HANDOFF_MAX_SNAPSHOT_BYTES", str(DEFAULT_MAX_SNAPSHOT_BYTES)))
    )
    proc = subprocess.run(
        ["git", "-C", str(workdir), "archive", "--format=tar.gz", ref],
        check=False,
        capture_output=True,
    )
    if proc.returncode != 0:
        err = proc.stderr.decode("utf-8", errors="replace").strip()
        raise RuntimeError(f"`git archive --format=tar.gz {ref}` failed in `{workdir}`: {err}")
    raw = proc.stdout
    if limit > 0 and len(raw) > limit:
        raise RuntimeError(
            f"Compressed repository snapshot is {len(raw) / (1024 * 1024):.1f} MiB "
            f"(exceeds {limit / (1024 * 1024):.1f} MiB limit). "
            "Configure a per-owner GitHub token in k3s or set AGY_HANDOFF_MAX_SNAPSHOT_BYTES."
        )
    return base64.b64encode(raw).decode("ascii"), len(raw)


def fetch_remote_repo_access(
    *,
    base_url: str,
    repo: str,
    token: str = "",
    ca_cert: str = "",
    insecure: bool = False,
) -> dict[str, Any]:
    """Query `GET /api/handoff/repo-access?repo=...` and return full capability metadata."""
    if not repo or repo.startswith("local/"):
        return {"accessible": False, "reason": "local-only repository"}
    url = f"{base_url}/api/handoff/repo-access?repo={urllib.parse.quote(repo)}"
    try:
        resp = http_json(
            "GET",
            url,
            token=token,
            ca_cert=ca_cert,
            insecure=insecure,
            timeout=15.0,
        )
        if isinstance(resp, dict) and "accessible" in resp:
            return resp
        return {"accessible": None, "reason": ""}
    except urllib.error.HTTPError as exc:
        if exc.code in {404, 405}:
            return {"accessible": None, "reason": "preflight endpoint not available"}
        return {"accessible": None, "reason": f"HTTP {exc.code}"}
    except (urllib.error.URLError, TimeoutError, OSError, ValueError) as exc:
        return {"accessible": None, "reason": str(exc)}


def check_remote_repo_access(
    *,
    base_url: str,
    repo: str,
    token: str = "",
    ca_cert: str = "",
    insecure: bool = False,
) -> tuple[bool | None, str]:
    """Query `GET /api/handoff/repo-access?repo=...`.

    Returns `(accessible, reason)` where `accessible` is `None` if the server does not
    expose the preflight endpoint (e.g. older controller) or is momentarily unreachable.
    """
    info = fetch_remote_repo_access(
        base_url=base_url,
        repo=repo,
        token=token,
        ca_cert=ca_cert,
        insecure=insecure,
    )
    acc = info.get("accessible")
    return (bool(acc) if acc is not None else None), str(info.get("reason") or "")


def _probe_remote_head_match(workdir: Path, head_sha: str, local_branch: str) -> str | None:
    """Run bounded `git ls-remote --heads origin` (8s timeout) to check if `head_sha` is a remote tip."""
    if not head_sha:
        return None
    try:
        proc = subprocess.run(
            ["git", "-C", str(workdir), "ls-remote", "--heads", "origin"],
            check=False,
            capture_output=True,
            text=True,
            timeout=8,
        )
    except (subprocess.TimeoutExpired, OSError):
        return None
    if proc.returncode != 0:
        return None
    matched_branches: list[str] = []
    for line in proc.stdout.splitlines():
        parts = line.strip().split()
        if len(parts) < 2:
            continue
        sha, ref = parts[0], parts[1]
        if sha == head_sha and ref.startswith("refs/heads/"):
            matched_branches.append(ref.removeprefix("refs/heads/"))
    if not matched_branches:
        return None
    preferred = [local_branch if local_branch != "HEAD" else "", "main", "master"]
    return next((b for b in preferred if b and b in matched_branches), matched_branches[0])


def _list_branches_at_head(workdir: Path) -> list[str]:
    proc = subprocess.run(
        ["git", "-C", str(workdir), "branch", "--points-at", "HEAD", "--format=%(refname:short)"],
        check=False,
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        return []
    return [line.strip() for line in proc.stdout.splitlines() if line.strip()]


def detect_git_metadata(workdir: Path) -> dict[str, str]:
    """Extract `github_repo`, `base_branch`, `base_sha`, and `patch` from `workdir`.

    When `HEAD` has unpushed local commits not reachable from `origin/*`, probes remote
    branch tips via bounded `git ls-remote --heads origin` first, and otherwise resolves
    the closest `origin/*` merge-base so the remote pod can check out a reachable commit
    and receive both unpushed commits and uncommitted edits as a squashed `patch`.
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

    refs_proc = _git(
        "for-each-ref", "--format=%(refname:short)", "refs/remotes/origin", check=False
    )
    origin_refs = [
        r.strip()
        for r in refs_proc.stdout.splitlines()
        if r.strip() and r.strip() not in {"origin", "origin/HEAD"}
    ]
    if head_sha:
        containing: list[str] = []
        if refs_proc.returncode == 0 and origin_refs:
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
            remote_match = (
                _probe_remote_head_match(workdir, head_sha, local_branch) if remote_url else None
            )
            if remote_match:
                base_branch = remote_match
                base_sha = head_sha
                diff_target = "HEAD"
            elif refs_proc.returncode == 0 and origin_refs:
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


def _resolve_git_dir(workdir: Path) -> Path:
    """Resolve the real git metadata directory (handles submodules & worktrees where `.git` is a file)."""
    proc = subprocess.run(
        ["git", "-C", str(workdir), "rev-parse", "--git-dir"],
        check=False,
        capture_output=True,
        text=True,
    )
    if proc.returncode == 0 and proc.stdout.strip():
        raw = Path(proc.stdout.strip())
        resolved = raw if raw.is_absolute() else (workdir / raw).resolve()
        if resolved.is_dir():
            return resolved
    git_dir = workdir / ".git"
    return git_dir if git_dir.is_dir() else workdir


def _state_file(workdir: Path) -> Path:
    git_dir = _resolve_git_dir(workdir)
    if git_dir != workdir:
        return git_dir / "agy_handoff_state.json"
    return workdir / ".agy_handoff_state.json"


def _pending_draft_file(workdir: Path) -> Path:
    git_dir = _resolve_git_dir(workdir)
    if git_dir != workdir:
        return git_dir / "agy_handoff_pending.json"
    return workdir / ".agy_handoff_pending.json"


def save_pending_handoff_draft(workdir: Path, draft: dict[str, Any]) -> Path:
    path = _pending_draft_file(workdir)
    path.write_text(json.dumps(draft, indent=2), encoding="utf-8")
    return path


def load_pending_handoff_draft(workdir: Path) -> dict[str, Any] | None:
    path = _pending_draft_file(workdir)
    if not path.is_file():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else None
    except (OSError, ValueError):
        return None


def clear_pending_handoff_draft(workdir: Path) -> None:
    _pending_draft_file(workdir).unlink(missing_ok=True)


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


class HandoffHTTPError(urllib.error.HTTPError):
    """HTTPError formatted cleanly with server response body detail."""

    def __init__(
        self,
        url: str,
        code: int,
        reason: str,
        hdrs: Any,
        detail: str = "",
    ) -> None:
        self.detail = detail
        msg = f"{reason}: {detail}" if detail else reason
        super().__init__(url, code, msg, hdrs, None)

    def __str__(self) -> str:
        if self.detail:
            return f"HTTP {self.code} ({self.reason}): {self.detail}"
        return f"HTTP {self.code}: {self.reason}"


def _extract_http_error_detail(exc: urllib.error.HTTPError) -> str:
    raw_body = ""
    try:
        if exc.fp is not None:
            raw_body = exc.read().decode("utf-8", errors="replace").strip()
    except Exception:
        raw_body = ""
    if not raw_body:
        return ""
    try:
        parsed = json.loads(raw_body)
        if isinstance(parsed, dict):
            for key in ("detail", "error", "message"):
                val = parsed.get(key)
                if isinstance(val, str) and val.strip():
                    return val.strip()
                if val is not None:
                    return json.dumps(val)
    except ValueError:
        pass
    return raw_body[:500]


def http_json(
    method: str,
    url: str,
    *,
    token: str = "",
    payload: dict[str, Any] | None = None,
    ca_cert: str = "",
    insecure: bool = False,
    timeout: float = 20.0,
    extra_headers: dict[str, str] | None = None,
) -> dict[str, Any]:
    headers = {"Accept": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
        headers["X-Agy-Token"] = token
    if extra_headers:
        headers.update(extra_headers)
    body_bytes: bytes | None = None
    if payload is not None:
        headers["Content-Type"] = "application/json"
        idem_val = payload.get("idempotency_key")
        if isinstance(idem_val, str) and idem_val.strip() and "Idempotency-Key" not in headers:
            headers["Idempotency-Key"] = idem_val.strip()
        body_bytes = json.dumps(payload).encode("utf-8")

    req = urllib.request.Request(url, data=body_bytes, headers=headers, method=method)
    ctx: ssl.SSLContext | None = None
    if url.startswith("https://"):
        if insecure:
            ctx = ssl._create_unverified_context()
        elif ca_cert and Path(ca_cert).is_file():
            ctx = ssl.create_default_context(cafile=ca_cert)

    try:
        with urllib.request.urlopen(req, timeout=timeout, context=ctx) as resp:
            raw = resp.read().decode("utf-8")
            return json.loads(raw) if raw.strip() else {}
    except urllib.error.HTTPError as exc:
        detail = _extract_http_error_detail(exc)
        raise HandoffHTTPError(exc.url, exc.code, str(exc.reason), exc.headers, detail) from None


def apply_remote_patch_safely(
    workdir: Path,
    *,
    patch_text: str,
    expected_base_sha: str = "",
) -> tuple[bool, str]:
    """Apply the pod's resulting `fix.patch` cleanly to `workdir` with pre-pull backup & rollback."""
    if not patch_text.strip():
        return True, "Remote task produced no code diff (working tree left unchanged)."

    git_dir = _resolve_git_dir(workdir)
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
                            [
                                "git",
                                "-C",
                                str(wt_dir),
                                "apply",
                                "--whitespace=nowarn",
                                str(remote_patch_path),
                            ],
                            check=False,
                            capture_output=True,
                            text=True,
                        )
                        if ap.returncode != 0:
                            ap = subprocess.run(
                                [
                                    "git",
                                    "-C",
                                    str(wt_dir),
                                    "apply",
                                    "--3way",
                                    "--whitespace=nowarn",
                                    str(remote_patch_path),
                                ],
                                check=False,
                                capture_output=True,
                                text=True,
                            )
                        if ap.returncode == 0:
                            subprocess.run(
                                ["git", "-C", str(wt_dir), "add", "-A"],
                                check=False,
                                capture_output=True,
                            )
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
                                    return (
                                        True,
                                        f"Working tree already matches remote pod state at `{current_sha[:8]}`.",
                                    )
                                rebased_path = git_dir / "agy_handoff_rebased.patch"
                                rebased_path.write_text(rebased, encoding="utf-8")
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
                                apply_rebased = subprocess.run(
                                    [
                                        "git",
                                        "-C",
                                        str(workdir),
                                        "apply",
                                        "--whitespace=nowarn",
                                        str(rebased_path),
                                    ],
                                    check=False,
                                    capture_output=True,
                                    text=True,
                                )
                                if apply_rebased.returncode == 0:
                                    return (
                                        True,
                                        f"Rebased & applied remote delta onto unpushed HEAD `{current_sha[:8]}` (backup at `{backup_patch_path}`).",
                                    )
                    finally:
                        subprocess.run(
                            [
                                "git",
                                "-C",
                                str(workdir),
                                "worktree",
                                "remove",
                                "--force",
                                str(wt_dir),
                            ],
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


def _local_repo_slug(workdir: Path) -> str:
    slug = re.sub(r"[^A-Za-z0-9_.-]+", "-", workdir.name).strip("-.") or "workspace"
    return f"local/{slug}"


def _read_context_files(paths: list[str]) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    for raw_path in paths:
        p = Path(raw_path).expanduser()
        if not p.is_file():
            raise ValueError(f"Context file not found: `{raw_path}`")
        content = p.read_text(encoding="utf-8")
        out.append({"name": p.name, "content": content})
    return out


def _compute_idempotency_key(
    *,
    repo: str,
    base_branch: str,
    base_sha: str,
    prompt: str,
    semantic_state: str,
    patch_text: str,
    context_files: list[dict[str, str]] | None,
) -> str:
    h = hashlib.sha256()
    h.update(repo.encode("utf-8"))
    h.update(b"\0")
    h.update(base_branch.encode("utf-8"))
    h.update(b"\0")
    h.update(base_sha.encode("utf-8"))
    h.update(b"\0")
    h.update(prompt.encode("utf-8"))
    h.update(b"\0")
    h.update(semantic_state.encode("utf-8"))
    h.update(b"\0")
    h.update(patch_text.encode("utf-8"))
    for cf in context_files or []:
        h.update(b"\0")
        h.update(str(cf.get("name", "")).encode("utf-8"))
        h.update(b"\0")
        h.update(str(cf.get("content", "")).encode("utf-8"))
    return f"handoff-{h.hexdigest()[:24]}"


def _push_head_to_origin_branch(workdir: Path, branch: str) -> str:
    """Push `HEAD` to `origin/<branch>` before metadata detection and return pushed branch name."""
    target_branch = branch.strip()
    local_branch = subprocess.run(
        ["git", "-C", str(workdir), "rev-parse", "--abbrev-ref", "HEAD"],
        check=False,
        capture_output=True,
        text=True,
    ).stdout.strip()
    if not target_branch:
        if local_branch and local_branch != "HEAD":
            target_branch = local_branch
        else:
            at_head = _list_branches_at_head(workdir)
            if len(at_head) == 1:
                target_branch = at_head[0]
            else:
                hint = f" (branches at HEAD: {', '.join(at_head)})" if at_head else ""
                raise RuntimeError(
                    f"HEAD is detached{hint}; specify an explicit branch name with `--push-branch <branch>`."
                )

    if local_branch == target_branch:
        cmd = ["git", "-C", str(workdir), "push", "-u", "origin", target_branch]
    else:
        cmd = [
            "git",
            "-C",
            str(workdir),
            "push",
            "-u",
            "origin",
            f"HEAD:refs/heads/{target_branch}",
        ]

    proc = subprocess.run(cmd, check=False, capture_output=True, text=True)
    if proc.returncode != 0:
        detail = (proc.stderr or proc.stdout or "").strip()
        raise RuntimeError(f"Failed to push HEAD to `origin/{target_branch}`: {detail}")
    print(f"[agy-handoff] Pushed HEAD to `origin/{target_branch}`.", flush=True)
    return target_branch


def _measure_repo_snapshot_bytes(workdir: Path, ref: str = "HEAD") -> int | None:
    """Return compressed `git archive --format=tar.gz` size in bytes, or None on error."""
    try:
        proc = subprocess.run(
            ["git", "-C", str(workdir), "archive", "--format=tar.gz", ref],
            check=False,
            capture_output=True,
            timeout=15,
        )
    except (subprocess.TimeoutExpired, OSError):
        return None
    if proc.returncode != 0:
        return None
    return len(proc.stdout)


def _recover_recent_handoff_after_timeout(
    *,
    base_url: str,
    repo: str,
    idempotency_key: str,
    base_sha: str,
    token: str,
    ca_cert: str,
    insecure: bool,
) -> dict[str, Any] | None:
    """Recover a handoff created on the server when the POST response timed out or dropped."""
    try:
        listing = http_json(
            "GET",
            f"{base_url}/api/handoff?repo={urllib.parse.quote(repo)}&limit=5",
            token=token,
            ca_cert=ca_cert,
            insecure=insecure,
            timeout=15.0,
        )
    except Exception:
        return None
    handoffs = listing.get("handoffs") if isinstance(listing, dict) else None
    if not isinstance(handoffs, list):
        return None
    for item in handoffs:
        if not isinstance(item, dict):
            continue
        if idempotency_key and str(item.get("idempotency_key") or "").strip() == idempotency_key:
            return item
    # Fallback match for older servers that don't echo idempotency_key
    for item in handoffs:
        if not isinstance(item, dict):
            continue
        status = str(item.get("status") or "").lower()
        if status in {"queued", "running"} and base_sha and item.get("base_sha") == base_sha:
            return item
    return None


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
    model: str = "",
    local_conversation_id: str = "",
    upload_repo: bool | None = None,
    context_files: list[dict[str, str]] | None = None,
    timeout_seconds: int | None = None,
    push_branch: str | None = None,
    idempotency_key: str = "",
    subcommand_for_retry: str = "push",
) -> dict[str, Any]:
    orig_workdir = workdir.resolve()
    effective_model = model.strip() or resolve_model_for_handoff(orig_workdir)

    # Persist raw request inputs BEFORE any git/network preflight so `--retry` never needs re-writing
    draft_payload: dict[str, Any] = {
        "workdir": str(orig_workdir),
        "prompt": prompt,
        "semantic_state": semantic_state,
        "github_repo": github_repo,
        "verify_hint": verify_hint,
        "focus_subdir": focus_subdir,
        "model": effective_model,
        "local_conversation_id": local_conversation_id,
        "upload_repo": upload_repo,
        "context_files": list(context_files or []),
        "timeout_seconds": timeout_seconds,
        "idempotency_key": idempotency_key,
        "saved_at": time.time(),
    }
    try:
        save_pending_handoff_draft(orig_workdir, draft_payload)
    except OSError:
        pass

    effective_workdir, effective_focus, effective_verify_hint, submodule_rel = (
        resolve_effective_workdir(
            orig_workdir,
            focus_subdir=focus_subdir,
            prompt=prompt,
            verify_hint=verify_hint,
        )
    )
    effective_semantic_state = semantic_state
    if submodule_rel and effective_workdir != orig_workdir:
        sub_note = (
            f"Note: Handoff was automatically retargeted to git submodule `{submodule_rel}`. "
            f"`/work` IS the root of `{submodule_rel}` (any `{submodule_rel}/...` paths mentioned "
            "in instructions correspond to paths relative to `/work`)."
        )
        effective_semantic_state = (
            f"{sub_note}\n\n{semantic_state}".strip() if semantic_state.strip() else sub_note
        )
        print(
            f"[agy-handoff] Detected git submodule target `{submodule_rel}` — "
            f"retargeting handoff to `{effective_workdir}` so diff capture & local patch apply work cleanly.",
            flush=True,
        )

    if push_branch is not None:
        _push_head_to_origin_branch(effective_workdir, push_branch)

    git_meta = detect_git_metadata(effective_workdir)
    repo = github_repo.strip() or git_meta["github_repo"]
    max_patch_bytes = int(
        os.environ.get("AGY_HANDOFF_MAX_PATCH_BYTES", str(DEFAULT_MAX_INLINE_PATCH_BYTES))
    )
    server_max_snapshot_bytes = int(
        os.environ.get("AGY_HANDOFF_MAX_SNAPSHOT_BYTES", str(DEFAULT_MAX_SNAPSHOT_BYTES))
    )

    should_upload_snapshot = False
    cluster_repo_accessible: bool | None = None
    if upload_repo is True:
        should_upload_snapshot = True
        if not repo:
            repo = _local_repo_slug(effective_workdir)
    elif upload_repo is False:
        if not repo:
            raise ValueError(
                "Could not detect GitHub repository from `git remote get-url origin`. "
                "Pass `--repo owner/name` or omit `--no-upload-repo` to upload a snapshot."
            )
    else:
        if not repo:
            repo = _local_repo_slug(effective_workdir)
            should_upload_snapshot = True
            print(
                f"[agy-handoff] No GitHub remote detected for `{effective_workdir}` — "
                f"uploading compressed HEAD snapshot as `{repo}`.",
                flush=True,
            )
        else:
            access_info = fetch_remote_repo_access(
                base_url=base_url,
                repo=repo,
                token=token,
                ca_cert=ca_cert,
                insecure=insecure,
            )
            acc = access_info.get("accessible")
            cluster_repo_accessible = bool(acc) if acc is not None else None
            reason = str(access_info.get("reason") or "")
            if isinstance(access_info.get("max_patch_bytes"), int) and not os.environ.get(
                "AGY_HANDOFF_MAX_PATCH_BYTES"
            ):
                max_patch_bytes = int(access_info["max_patch_bytes"])
            if isinstance(access_info.get("max_snapshot_bytes"), int) and not os.environ.get(
                "AGY_HANDOFF_MAX_SNAPSHOT_BYTES"
            ):
                server_max_snapshot_bytes = int(access_info["max_snapshot_bytes"])
            if cluster_repo_accessible is False:
                should_upload_snapshot = True
                reason_suffix = f" ({reason})" if reason else ""
                print(
                    f"[agy-handoff] Cluster cannot mirror `{repo}`{reason_suffix} — "
                    "uploading compressed HEAD snapshot on-the-fly...",
                    flush=True,
                )

    base_branch = git_meta["base_branch"]
    base_sha = git_meta["base_sha"]
    patch_text = git_meta["patch"]
    repo_snapshot_b64 = ""

    if should_upload_snapshot:
        local_branch = git_meta.get("local_branch") or base_branch
        base_branch = local_branch if local_branch != "HEAD" else "main"
        base_sha = git_meta.get("head_sha") or base_sha
        subprocess.run(
            ["git", "-C", str(effective_workdir), "add", "-N", "."],
            check=False,
            capture_output=True,
        )
        patch_text = subprocess.run(
            ["git", "-C", str(effective_workdir), "diff", "--binary", "HEAD"],
            check=False,
            capture_output=True,
            text=True,
        ).stdout
        repo_snapshot_b64, snap_bytes = create_repo_snapshot_b64(
            effective_workdir, ref="HEAD", max_bytes=server_max_snapshot_bytes
        )
        print(
            f"[agy-handoff] Packed HEAD snapshot for `{repo}` ({snap_bytes / 1024:.1f} KiB compressed).",
            flush=True,
        )

    patch_bytes = len(patch_text.encode("utf-8"))
    if max_patch_bytes > 0 and patch_bytes > max_patch_bytes:
        patch_mb = patch_bytes / (1024 * 1024)
        limit_mb = max_patch_bytes / (1024 * 1024)
        local_branch = git_meta.get("local_branch") or base_branch
        head_sha = git_meta.get("head_sha") or ""
        if head_sha and head_sha != base_sha:
            sibling_branches = _list_branches_at_head(effective_workdir)
            suggested_branch = (
                local_branch
                if local_branch and local_branch != "HEAD"
                else (sibling_branches[0] if sibling_branches else "handoff-branch")
            )
            branches_note = ""
            if sibling_branches:
                branches_note = (
                    " Local branches at HEAD: "
                    + ", ".join(f"`{b}`" for b in sibling_branches)
                    + "."
                )
            allow_upload_hint = True
            if cluster_repo_accessible is True:
                snap_size = _measure_repo_snapshot_bytes(effective_workdir, ref="HEAD")
                if snap_size is not None and snap_size > server_max_snapshot_bytes:
                    allow_upload_hint = False
            script_ref = sys.argv[0] if sys.argv and sys.argv[0] else "handoff_client.py"
            retry_cmd = (
                f'python3 "{script_ref}" {subcommand_for_retry} '
                f'--workdir "{orig_workdir}" --retry --push-branch {suggested_branch}'
            )
            upload_suffix = " (or add `--upload-repo`)" if allow_upload_hint else ""
            raise RuntimeError(
                f"Inline handoff patch is {patch_mb:.1f} MiB (exceeds {limit_mb:.1f} MiB limit) "
                f"because HEAD `{head_sha[:8]}` has unpushed commits diverged from `{base_branch}` "
                f"({base_sha[:8]}).{branches_note} "
                f"Draft cached — retry with a single command{upload_suffix}:\n  {retry_cmd}"
            )
        raise RuntimeError(
            f"Inline handoff patch is {patch_mb:.1f} MiB (exceeds {limit_mb:.1f} MiB limit). "
            "Commit & push large/binary changes or set AGY_HANDOFF_MAX_PATCH_BYTES to override."
        )

    effective_idem_key = idempotency_key.strip() or _compute_idempotency_key(
        repo=repo,
        base_branch=base_branch,
        base_sha=base_sha,
        prompt=prompt,
        semantic_state=effective_semantic_state,
        patch_text=patch_text,
        context_files=context_files,
    )

    payload: dict[str, Any] = {
        "repo": repo,
        "github_repo": repo,
        "branch": base_branch,
        "base_branch": base_branch,
        "base_sha": base_sha,
        "patch": patch_text,
        "prompt": prompt,
        "semantic_state": effective_semantic_state,
        "verify_hint": effective_verify_hint,
        "focus_subdir": effective_focus,
        "model": effective_model,
        "source_conversation_id": local_conversation_id or "",
        "local_conversation_id": local_conversation_id or None,
        "workspace_path": str(effective_workdir),
        "idempotency_key": effective_idem_key,
    }
    if context_files:
        payload["context_files"] = context_files
    if timeout_seconds is not None:
        payload["timeout_seconds"] = int(timeout_seconds)
    if repo_snapshot_b64:
        payload["repo_snapshot_b64"] = repo_snapshot_b64

    post_timeout = 60.0 if repo_snapshot_b64 else 45.0
    try:
        resp = http_json(
            "POST",
            f"{base_url}/api/handoff",
            token=token,
            payload=payload,
            ca_cert=ca_cert,
            insecure=insecure,
            timeout=post_timeout,
        )
    except urllib.error.HTTPError:
        raise
    except (TimeoutError, urllib.error.URLError, OSError):
        recovered = _recover_recent_handoff_after_timeout(
            base_url=base_url,
            repo=repo,
            idempotency_key=effective_idem_key,
            base_sha=base_sha,
            token=token,
            ca_cert=ca_cert,
            insecure=insecure,
        )
        if recovered is None:
            raise
        recovered_id = (
            recovered.get("handoff_id") or recovered.get("issue_id") or recovered.get("short_id")
        )
        print(
            f"[agy-handoff] POST timed out/dropped, recovered active handoff `{recovered_id}` from server.",
            flush=True,
        )
        resp = dict(recovered)
        resp["recovered_after_timeout"] = True

    clear_pending_handoff_draft(orig_workdir)

    resolved_id = resp.get("handoff_id") or resp.get("issue_id") or resp.get("short_id")
    if resolved_id and not resp.get("handoff_id"):
        resp["handoff_id"] = resolved_id
    resp["effective_workdir"] = str(effective_workdir)

    state_payload = {
        "handoff_id": resolved_id,
        "short_id": resp.get("short_id"),
        "github_repo": repo,
        "base_sha": base_sha,
        "base_url": base_url,
        "target_workdir": str(effective_workdir),
        "pushed_at": time.time(),
        "pulled": False,
    }
    save_local_handoff_state(effective_workdir, state_payload)
    if effective_workdir != orig_workdir:
        save_local_handoff_state(orig_workdir, state_payload)
    return resp


def _watcher_lock_path(workdir: Path, handoff_id: str) -> Path:
    git_dir = _resolve_git_dir(workdir)
    safe_id = re.sub(r"[^A-Za-z0-9_.-]+", "_", handoff_id).strip("_") or "latest"
    prefix = "" if git_dir != workdir else "."
    return git_dir / f"{prefix}agy_handoff_watch_{safe_id}.lock"


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
    lock_fh = None
    if fcntl is not None:
        lock_path = _watcher_lock_path(workdir, handoff_id)
        lock_fh = lock_path.open("a+", encoding="utf-8")
        try:
            fcntl.flock(lock_fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            lock_fh.seek(0)
            existing_pid = lock_fh.read().strip()
            lock_fh.close()
            pid_note = f" (PID {existing_pid})" if existing_pid else ""
            print(
                f"[agy-handoff] Another watcher{pid_note} is already watching `{handoff_id}`; exiting cleanly.",
                flush=True,
            )
            return {
                "handoff_id": handoff_id,
                "status": "watching",
                "already_watching": True,
                "watcher_pid": existing_pid,
            }
        lock_fh.seek(0)
        lock_fh.truncate()
        lock_fh.write(f"{os.getpid()}\n")
        lock_fh.flush()

    try:
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
    finally:
        if lock_fh is not None:
            try:
                if fcntl is not None:
                    fcntl.flock(lock_fh.fileno(), fcntl.LOCK_UN)
            finally:
                lock_fh.close()


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

    def _add_push_args(p: argparse.ArgumentParser) -> None:
        p.add_argument(
            "--prompt",
            default="",
            help="Task instructions for the k3s pod (pass '-' to read from stdin)",
        )
        p.add_argument(
            "--prompt-file",
            default="",
            help="Read task instructions from a UTF-8 file instead of inline --prompt",
        )
        p.add_argument(
            "--context-file",
            action="append",
            default=[],
            help="Attach a read-only UTF-8 context file (repeatable; mounted at /artifacts/context/<name>)",
        )
        p.add_argument("--state", default="", help="Compact Semantic State (s_t) summary")
        p.add_argument("--repo", default="", help="Override GitHub repo (owner/name)")
        p.add_argument("--verify-hint", default="", help="Verification command for the worker")
        p.add_argument("--focus-subdir", default="", help="Focus subdirectory inside repo")
        p.add_argument(
            "--model",
            default="",
            help="Worker model ID, human name, shorthand, or 'auto' (default: auto-detected)",
        )
        p.add_argument(
            "--effort",
            default="",
            choices=("", "low", "medium", "high"),
            help="Optional effort tier (low, medium, high) combined with --model",
        )
        p.add_argument(
            "--timeout",
            type=int,
            default=None,
            help="Optional worker pod wall-clock timeout in seconds (clamped server-side)",
        )
        p.add_argument(
            "--retry",
            action="store_true",
            help="Reload the cached pending handoff draft instead of rewriting prompt/flags",
        )
        p.add_argument(
            "--push-branch",
            nargs="?",
            const="",
            default=None,
            help="Push HEAD to origin/<branch> (defaults to current branch) before handoff preflight",
        )
        p.add_argument(
            "--upload-repo",
            action="store_true",
            help="Upload a compressed HEAD snapshot of the repository on-the-fly (no k3s GitHub creds required)",
        )
        p.add_argument(
            "--no-upload-repo",
            action="store_true",
            help="Disable automatic repository snapshot upload fallback",
        )

    # `start`: Default mode for `/handoff` (pushes if prompt provided or no active handoff, then polls on wake)
    p_start = sub.add_parser("start", help="Push handoff (if needed) and start poll-on-wake mode")
    _add_common(p_start)
    _add_push_args(p_start)
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
    _add_push_args(p_push)

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

    def _resolve_upload_repo_flag() -> bool | None:
        if getattr(args, "upload_repo", False):
            return True
        if getattr(args, "no_upload_repo", False):
            return False
        return None

    def _resolve_saved_target_workdir(base_dir: Path, saved: dict[str, Any] | None) -> Path:
        raw_target = str((saved or {}).get("target_workdir") or "").strip()
        if raw_target:
            candidate = Path(raw_target)
            if candidate.is_dir():
                return candidate
        return base_dir

    def _resolve_push_inputs(sub_name: str) -> dict[str, Any]:
        draft: dict[str, Any] = {}
        if getattr(args, "retry", False):
            loaded = load_pending_handoff_draft(workdir)
            if not loaded:
                raise ValueError(
                    f"No pending handoff draft found at `{_pending_draft_file(workdir)}`."
                )
            draft = loaded

        raw_prompt = getattr(args, "prompt", "") or ""
        prompt_file = getattr(args, "prompt_file", "") or ""
        if prompt_file:
            pf = Path(prompt_file).expanduser()
            if not pf.is_file():
                raise ValueError(f"Prompt file not found: `{prompt_file}`")
            resolved_prompt = pf.read_text(encoding="utf-8")
        elif raw_prompt == "-":
            resolved_prompt = sys.stdin.read()
        elif raw_prompt:
            resolved_prompt = raw_prompt
        else:
            resolved_prompt = str(draft.get("prompt") or "")

        cli_ctx_paths = list(getattr(args, "context_file", []) or [])
        resolved_context_files = (
            _read_context_files(cli_ctx_paths)
            if cli_ctx_paths
            else list(draft.get("context_files") or [])
        )

        cli_upload = _resolve_upload_repo_flag()
        resolved_upload = cli_upload if cli_upload is not None else draft.get("upload_repo")

        cli_timeout = getattr(args, "timeout", None)
        resolved_timeout = cli_timeout if cli_timeout is not None else draft.get("timeout_seconds")

        cli_model = getattr(args, "model", "") or ""
        cli_effort = getattr(args, "effort", "") or ""
        if cli_model or cli_effort:
            resolved_model = resolve_model_for_handoff(workdir, model=cli_model, effort=cli_effort)
        elif draft.get("model"):
            resolved_model = str(draft["model"])
        else:
            resolved_model = resolve_model_for_handoff(workdir)

        return {
            "prompt": resolved_prompt,
            "semantic_state": getattr(args, "state", "") or str(draft.get("semantic_state") or ""),
            "github_repo": getattr(args, "repo", "") or str(draft.get("github_repo") or ""),
            "verify_hint": getattr(args, "verify_hint", "") or str(draft.get("verify_hint") or ""),
            "focus_subdir": getattr(args, "focus_subdir", "")
            or str(draft.get("focus_subdir") or ""),
            "model": resolved_model,
            "local_conversation_id": str(draft.get("local_conversation_id") or ""),
            "upload_repo": resolved_upload,
            "context_files": resolved_context_files,
            "timeout_seconds": resolved_timeout,
            "push_branch": getattr(args, "push_branch", None),
            "idempotency_key": str(draft.get("idempotency_key") or ""),
            "subcommand_for_retry": sub_name,
        }

    if cmd == "push":
        push_kwargs = _resolve_push_inputs("push")
        if not push_kwargs["prompt"].strip():
            raise ValueError(
                "Handoff prompt is required (pass `--prompt`, `--prompt -`, `--prompt-file`, or `--retry`)."
            )
        created = push_handoff(
            workdir,
            base_url=base_url,
            token=token,
            ca_cert=ca_cert,
            insecure=insecure,
            **push_kwargs,
        )
        print(json.dumps(created, indent=2) if args.json else _format_summary(created))
        return 0

    if cmd == "start":
        handoff_id = args.handoff_id.strip()
        poll_workdir = workdir
        wants_push = bool(
            getattr(args, "retry", False)
            or (getattr(args, "prompt", "") or "").strip()
            or (getattr(args, "prompt_file", "") or "").strip()
        )
        if not handoff_id and wants_push:
            push_kwargs = _resolve_push_inputs("start")
            if not push_kwargs["prompt"].strip():
                raise ValueError("Handoff prompt cannot be empty.")
            created = push_handoff(
                workdir,
                base_url=base_url,
                token=token,
                ca_cert=ca_cert,
                insecure=insecure,
                **push_kwargs,
            )
            handoff_id = str(
                created.get("handoff_id")
                or created.get("issue_id")
                or created.get("short_id")
                or ""
            )
            eff = str(created.get("effective_workdir") or "").strip()
            if eff and Path(eff).is_dir():
                poll_workdir = Path(eff)
            print(_format_summary(created), flush=True)
        elif not handoff_id:
            local_state = load_local_handoff_state(workdir)
            if local_state and local_state.get("handoff_id") and not local_state.get("pulled"):
                handoff_id = str(local_state["handoff_id"])
                poll_workdir = _resolve_saved_target_workdir(workdir, local_state)
            else:
                latest = http_json(
                    "GET",
                    f"{base_url}/api/handoff/latest?only_unpulled=true&include_patch=false",
                    token=token,
                    ca_cert=ca_cert,
                    insecure=insecure,
                )
                handoff_id = str(
                    latest.get("handoff_id")
                    or latest.get("issue_id")
                    or latest.get("short_id")
                    or "latest"
                )

        result = poll_until_ready_and_pull(
            poll_workdir,
            base_url=base_url,
            token=token,
            ca_cert=ca_cert,
            insecure=insecure,
            handoff_id=handoff_id,
            poll_interval=args.poll_interval,
            apply_patch=not args.no_apply,
        )
        if result.get("already_watching"):
            if args.json:
                print(json.dumps(result, indent=2))
            return 0
        print(json.dumps(result, indent=2) if args.json else _format_summary(result))
        return 0

    if cmd == "watch":
        handoff_id = args.handoff_id.strip()
        local_state = load_local_handoff_state(workdir)
        watch_workdir = workdir
        if not handoff_id:
            handoff_id = str((local_state or {}).get("handoff_id") or "latest")
            watch_workdir = _resolve_saved_target_workdir(workdir, local_state)
        elif local_state and local_state.get("handoff_id") == handoff_id:
            watch_workdir = _resolve_saved_target_workdir(workdir, local_state)
        result = poll_until_ready_and_pull(
            watch_workdir,
            base_url=base_url,
            token=token,
            ca_cert=ca_cert,
            insecure=insecure,
            handoff_id=handoff_id,
            poll_interval=args.poll_interval,
            apply_patch=not args.no_apply,
        )
        if result.get("already_watching"):
            if args.json:
                print(json.dumps(result, indent=2))
            return 0
        print(json.dumps(result, indent=2) if args.json else _format_summary(result))
        return 0

    if cmd in {"pull", "status"}:
        handoff_id = args.handoff_id.strip()
        local_state = load_local_handoff_state(workdir)
        pull_workdir = workdir
        if not handoff_id:
            handoff_id = str((local_state or {}).get("handoff_id") or "latest")
            pull_workdir = _resolve_saved_target_workdir(workdir, local_state)
        elif local_state and local_state.get("handoff_id") == handoff_id:
            pull_workdir = _resolve_saved_target_workdir(workdir, local_state)
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
                pull_workdir,
                patch_text=str(data.get("patch") or data.get("diff") or ""),
                expected_base_sha=str(data.get("base_sha") or ""),
            )
            data["applied_ok"] = ok
            data["apply_message"] = msg
            resolved_pull_id = (
                data.get("handoff_id") or data.get("issue_id") or data.get("short_id")
            )
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
    try:
        sys.exit(main())
    except (HandoffHTTPError, urllib.error.URLError, RuntimeError, ValueError, TimeoutError) as exc:
        print(f"[agy-handoff] Error: {exc}", file=sys.stderr)
        sys.exit(1)
