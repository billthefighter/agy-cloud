from __future__ import annotations

import argparse
import json
import os
import re
import ssl
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

DEFAULT_GATEWAY_URL = "https://makesense-k3s.netbird.cloud/agy"
DEFAULT_ENV_FILES = (
    Path.home() / ".gemini" / "config" / "handoff.env",
    Path(
        "/Users/lucaswhipple/Dropbox/code_projects/homelab_k3s/k3s-proxmox-terraform/.env.makesense"
    ),
)

_GITHUB_REMOTE_RE = re.compile(r"(?:github\.com[:/])([A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+?)(?:\.git)?$")


def parse_env_file(path: Path) -> dict[str, str]:
    if not path.is_file():
        return {}
    result: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        if line.startswith("export "):
            line = line[7:].strip()
        key, _, val = line.partition("=")
        k = key.strip()
        v = val.strip()
        if len(v) >= 2 and ((v[0] == v[-1] == '"') or (v[0] == v[-1] == "'")):
            v = v[1:-1]
        if k:
            result[k] = v
    return result


def resolve_connection_config(
    *,
    url_arg: str = "",
    token_arg: str = "",
    repo_dir: Path | None = None,
    env_files: tuple[Path, ...] = DEFAULT_ENV_FILES,
) -> tuple[str, str]:
    merged_env: dict[str, str] = {}
    candidates = list(env_files)
    if repo_dir is not None:
        candidates.insert(0, repo_dir / ".env.makesense")
        candidates.insert(0, repo_dir / ".env")
    for env_path in reversed(candidates):
        merged_env.update(parse_env_file(env_path))
    merged_env.update(os.environ)

    token = (
        token_arg.strip()
        or merged_env.get("AGY_HANDOFF_TOKEN", "").strip()
        or merged_env.get("PWA_API_TOKEN", "").strip()
    )
    url = url_arg.strip() or merged_env.get("AGY_HANDOFF_URL", "").strip()
    if not url:
        nb_host = merged_env.get("NETBIRD_HOSTNAME", "").strip()
        if nb_host:
            host = nb_host if "." in nb_host else f"{nb_host}.netbird.cloud"
            url = f"https://{host}/agy"
        else:
            url = DEFAULT_GATEWAY_URL
    return url.rstrip("/"), token


def parse_github_repo_from_remote(remote_url: str) -> str:
    m = _GITHUB_REMOTE_RE.search(remote_url.strip())
    return m.group(1) if m else ""


def _git(repo_dir: Path, *args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", "-C", str(repo_dir), *args],
        check=check,
        capture_output=True,
        text=True,
    )


def get_git_dir(repo_dir: Path) -> Path:
    proc = _git(repo_dir, "rev-parse", "--git-dir")
    raw = Path(proc.stdout.strip())
    return raw if raw.is_absolute() else (repo_dir / raw).resolve()


def capture_local_git_state(repo_dir: Path, *, repo_override: str = "") -> dict[str, str]:
    top_proc = _git(repo_dir, "rev-parse", "--show-toplevel")
    root = Path(top_proc.stdout.strip())

    github_repo = repo_override.strip()
    if not github_repo:
        rem_proc = _git(root, "remote", "get-url", "origin", check=False)
        if rem_proc.returncode == 0:
            github_repo = parse_github_repo_from_remote(rem_proc.stdout)

    branch_proc = _git(root, "rev-parse", "--abbrev-ref", "HEAD", check=False)
    branch = branch_proc.stdout.strip() if branch_proc.returncode == 0 else "main"
    if branch == "HEAD":
        branch = "main"

    sha_proc = _git(root, "rev-parse", "HEAD", check=False)
    base_sha = sha_proc.stdout.strip() if sha_proc.returncode == 0 else ""

    # Mark untracked files with intent-to-add so `git diff --binary HEAD` includes new files
    _git(root, "add", "-N", ".", check=False)
    diff_proc = _git(root, "diff", "--binary", "HEAD", check=False)
    base_patch = diff_proc.stdout if diff_proc.returncode == 0 else ""

    return {
        "root": str(root),
        "github_repo": github_repo,
        "branch": branch,
        "base_sha": base_sha,
        "base_patch": base_patch,
    }


def save_local_handoff_state(
    repo_dir: Path,
    *,
    meta: dict[str, Any],
    initial_patch: str,
) -> Path:
    git_dir = get_git_dir(repo_dir)
    state_file = git_dir / "agy_handoff.json"
    init_patch_file = git_dir / "agy_handoff_initial.patch"
    state_file.write_text(json.dumps(meta, indent=2), encoding="utf-8")
    init_patch_file.write_text(initial_patch, encoding="utf-8")
    return state_file


def load_local_handoff_state(repo_dir: Path) -> dict[str, Any] | None:
    try:
        git_dir = get_git_dir(repo_dir)
    except subprocess.CalledProcessError:
        return None
    state_file = git_dir / "agy_handoff.json"
    if not state_file.is_file():
        return None
    try:
        data = json.loads(state_file.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else None
    except (OSError, ValueError):
        return None


def apply_remote_handoff_patch(
    repo_dir: Path,
    *,
    remote_diff: str,
    handoff_id: str = "",
) -> dict[str, Any]:
    """Safely apply cumulative `fix.patch` from k3s onto the laptop working tree.

    If the laptop working tree still has the pre-handoff `agy_handoff_initial.patch` applied,
    this reverses the initial patch first (after saving a backup to `agy_handoff_pre_pull.patch`)
    so cumulative hunks in `remote_diff` apply cleanly without duplicate-hunk conflicts.
    """
    git_dir = get_git_dir(repo_dir)
    pre_pull_backup = git_dir / "agy_handoff_pre_pull.patch"
    remote_patch_file = git_dir / "agy_handoff_remote.patch"
    init_patch_file = git_dir / "agy_handoff_initial.patch"
    state_file = git_dir / "agy_handoff.json"

    _git(repo_dir, "add", "-N", ".", check=False)
    current_diff = _git(repo_dir, "diff", "--binary", "HEAD", check=False).stdout
    pre_pull_backup.write_text(current_diff, encoding="utf-8")

    if not remote_diff.strip():
        _mark_local_state_pulled(state_file, handoff_id)
        return {
            "applied": False,
            "reason": "Remote session produced no code diff (empty fix.patch).",
            "backup_patch": str(pre_pull_backup),
        }

    patch_text = remote_diff if remote_diff.endswith("\n") else f"{remote_diff}\n"
    remote_patch_file.write_text(patch_text, encoding="utf-8")

    if current_diff.strip() == patch_text.strip():
        _mark_local_state_pulled(state_file, handoff_id)
        return {
            "applied": True,
            "already_identical": True,
            "remote_patch": str(remote_patch_file),
            "backup_patch": str(pre_pull_backup),
        }

    reversed_initial = False
    if (
        init_patch_file.is_file()
        and init_patch_file.read_text(encoding="utf-8").strip()
        and current_diff.strip()
    ):
        rev_proc = _git(
            repo_dir,
            "apply",
            "-R",
            "--whitespace=nowarn",
            str(init_patch_file),
            check=False,
        )
        if rev_proc.returncode == 0:
            reversed_initial = True

    apply_proc = _git(
        repo_dir,
        "apply",
        "--whitespace=nowarn",
        str(remote_patch_file),
        check=False,
    )
    if apply_proc.returncode != 0:
        apply_proc = _git(
            repo_dir,
            "apply",
            "--3way",
            "--whitespace=nowarn",
            str(remote_patch_file),
            check=False,
        )

    if apply_proc.returncode != 0:
        # Restore pre-pull working tree if we reversed initial patch
        if reversed_initial and pre_pull_backup.is_file():
            _git(repo_dir, "apply", "--whitespace=nowarn", str(init_patch_file), check=False)
        return {
            "applied": False,
            "error": (apply_proc.stderr or apply_proc.stdout).strip(),
            "remote_patch": str(remote_patch_file),
            "backup_patch": str(pre_pull_backup),
        }

    _mark_local_state_pulled(state_file, handoff_id)
    return {
        "applied": True,
        "reversed_initial": reversed_initial,
        "remote_patch": str(remote_patch_file),
        "backup_patch": str(pre_pull_backup),
    }


def _mark_local_state_pulled(state_file: Path, handoff_id: str) -> None:
    if not state_file.is_file():
        return
    try:
        data = json.loads(state_file.read_text(encoding="utf-8"))
        if isinstance(data, dict):
            data["pulled"] = True
            data["pulled_at"] = datetime.now(UTC).isoformat()
            if handoff_id:
                data["pulled_handoff_id"] = handoff_id
            state_file.write_text(json.dumps(data, indent=2), encoding="utf-8")
    except (OSError, ValueError):
        pass


class HandoffHttpClient:
    """Zero-dependency HTTP client for the `makesense` k3s `/agy/api/handoff` gateway."""

    def __init__(
        self,
        *,
        base_url: str,
        api_token: str,
        verify_tls: bool = False,
        timeout_s: float = 20.0,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.api_token = api_token.strip()
        self.timeout_s = timeout_s
        if verify_tls:
            self._ssl_ctx = ssl.create_default_context()
        else:
            self._ssl_ctx = ssl._create_unverified_context()

    def _request(
        self,
        method: str,
        path: str,
        *,
        body: dict[str, Any] | None = None,
        params: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        url = f"{self.base_url}{path}"
        if params:
            clean_params = {k: v for k, v in params.items() if v != ""}
            if clean_params:
                url = f"{url}?{urllib.parse.urlencode(clean_params)}"

        headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {self.api_token}",
            "X-AGY-Client": "pwa",
        }
        data_bytes: bytes | None = None
        if body is not None:
            headers["Content-Type"] = "application/json"
            data_bytes = json.dumps(body).encode("utf-8")

        req = urllib.request.Request(url, data=data_bytes, headers=headers, method=method)
        try:
            with urllib.request.urlopen(req, timeout=self.timeout_s, context=self._ssl_ctx) as resp:
                raw = resp.read().decode("utf-8")
                return json.loads(raw) if raw.strip() else {}
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"HTTP {exc.code} from {url}: {detail}") from exc

    def create_handoff(self, payload: dict[str, Any]) -> dict[str, Any]:
        return self._request("POST", "/api/handoff", body=payload)

    def get_handoff(
        self, handoff_id: str, *, repo: str = "", include_diff: bool = True
    ) -> dict[str, Any]:
        encoded_id = urllib.parse.quote(handoff_id.strip() or "latest", safe="")
        return self._request(
            "GET",
            f"/api/handoff/{encoded_id}",
            params={
                "repo": repo,
                "include_diff": "true" if include_diff else "false",
            },
        )

    def list_handoffs(self, *, repo: str = "", limit: int = 10) -> dict[str, Any]:
        return self._request(
            "GET",
            "/api/handoff",
            params={"repo": repo, "limit": str(limit)},
        )


def poll_until_terminal(
    *,
    fetch_fn: Callable[[bool], dict[str, Any]],
    poll_interval_s: float = 15.0,
    max_wait_s: float = 86400.0,
    sleep_fn: Callable[[float], None] = time.sleep,
    clock_fn: Callable[[], float] = time.time,
    on_tick: Callable[[str], None] | None = None,
) -> dict[str, Any]:
    """Sleep-resilient polling loop that survives laptop suspend/wake and transient offline gaps.

    Uses wall-clock `time.time()` rather than monotonic timers so closing the laptop lid
    for a 45-minute drive immediately triggers an HTTP check on wake and tolerates transient
    Wi-Fi / NetBird reconnection errors.
    """
    started_wall = clock_fn()
    last_status = ""
    consecutive_net_errors = 0

    while True:
        try:
            status_payload = fetch_fn(False)
            consecutive_net_errors = 0
            cur_status = str(status_payload.get("status") or "unknown")
            short_id = str(status_payload.get("short_id") or status_payload.get("issue_id") or "")
            turn = status_payload.get("turn", 1)
            if cur_status != last_status:
                last_status = cur_status
                if on_tick is not None:
                    on_tick(f"[handoff] {short_id} (turn {turn}) status -> {cur_status}")
            if status_payload.get("is_terminal"):
                return fetch_fn(True)
        except (urllib.error.URLError, TimeoutError, OSError, RuntimeError) as exc:
            # HTTP 404/401 should fail fast rather than loop forever
            msg = str(exc)
            if "HTTP 401" in msg or "HTTP 403" in msg or "HTTP 404" in msg:
                raise
            consecutive_net_errors += 1
            if on_tick is not None and (
                consecutive_net_errors == 1 or consecutive_net_errors % 4 == 0
            ):
                on_tick(
                    f"[handoff] waiting for NetBird/network after wake or transit "
                    f"(attempt #{consecutive_net_errors}: {exc})"
                )

        if clock_fn() - started_wall >= max_wait_s:
            raise TimeoutError(f"Timed out after {max_wait_s:.0f}s waiting for remote handoff.")

        sleep_fn(poll_interval_s)


def format_handoff_summary(
    payload: dict[str, Any],
    *,
    apply_result: dict[str, Any] | None = None,
) -> str:
    short_id = payload.get("short_id") or payload.get("issue_id") or "HANDOFF"
    issue_id = payload.get("issue_id") or ""
    status = payload.get("status") or "unknown"
    repo = payload.get("github_repo") or ""
    branch = payload.get("branch") or "main"
    base_sha = str(payload.get("base_sha") or "")[:8]
    model = payload.get("model") or ""
    turn = payload.get("turn") or 1

    lines = [
        f"## Cloud Handoff `{short_id}` (`{issue_id}`)",
        f"- **Status:** `{status}` (turn `{turn}`, model `{model}`)",
        f"- **Target:** `{repo}` (`{branch}`" + (f" @ `{base_sha}`)" if base_sha else ")"),
    ]
    if payload.get("pr_url"):
        lines.append(f"- **Draft PR:** {payload['pr_url']}")
    if payload.get("last_error"):
        lines.append(f"- **Error:** `{payload['last_error']}`")

    report = payload.get("report")
    if isinstance(report, dict):
        tests_str = "PASSED" if report.get("tests_passed") else "NOT VERIFIED / FAILED"
        lines.extend(
            [
                f"- **Tests:** `{tests_str}` (confidence: `{report.get('confidence', 'unknown')}`)",
                "",
                "### Pod Technical Summary (`root_cause`)",
                str(report.get("root_cause") or "_none_"),
                "",
                "### Changes Applied (`fix_summary`)",
                str(report.get("fix_summary") or "_none_"),
            ]
        )
        affected = report.get("affected_files") or []
        if affected:
            lines.append("")
            lines.append("### Affected Files")
            for f in affected:
                lines.append(f"- `{f}`")
        ver_cmds = report.get("verification_commands") or []
        if ver_cmds:
            lines.append("")
            lines.append("### Verification Commands Run in Pod")
            for cmd in ver_cmds:
                lines.append(f"- `{cmd}`")
        if report.get("risk_notes"):
            lines.append("")
            lines.append(f"### Risk Notes\n{report['risk_notes']}")

    if payload.get("patch_stats"):
        lines.extend(["", "### Diff Stats", f"```text\n{payload['patch_stats']}\n```"])

    if apply_result is not None:
        lines.append("")
        lines.append("### Local Working-Tree Sync")
        if apply_result.get("applied"):
            if apply_result.get("already_identical"):
                lines.append("- Local working tree already matches the remote `fix.patch`.")
            else:
                lines.append(
                    "- Applied remote `fix.patch` cleanly to local working tree "
                    f"(pre-pull backup saved at `{apply_result.get('backup_patch')}`)."
                )
        else:
            reason = apply_result.get("error") or apply_result.get("reason") or "Not applied"
            lines.append(f"- Did not auto-apply patch: `{reason}`")
            if apply_result.get("remote_patch"):
                lines.append(f"- Saved remote patch to `{apply_result['remote_patch']}`.")

    return "\n".join(lines)


def _push_handoff(
    *,
    client: HandoffHttpClient,
    repo_dir: Path,
    prompt: str,
    repo_override: str = "",
    branch_override: str = "",
    verify_hint: str = "",
    focus_subdir: str = "",
    model: str = "",
    title: str = "",
    source_conversation_id: str = "",
    include_working_tree_patch: bool = True,
) -> dict[str, Any]:
    git_state = capture_local_git_state(repo_dir, repo_override=repo_override)
    base_patch = git_state["base_patch"] if include_working_tree_patch else ""
    req_payload = {
        "prompt": prompt,
        "repo": git_state["github_repo"],
        "branch": branch_override.strip() or git_state["branch"],
        "base_sha": git_state["base_sha"],
        "base_patch": base_patch,
        "verify_hint": verify_hint.strip(),
        "focus_subdir": focus_subdir.strip(),
        "model": model.strip(),
        "title": title.strip(),
        "source_conversation_id": source_conversation_id.strip(),
    }
    created = client.create_handoff(req_payload)
    save_local_handoff_state(
        Path(git_state["root"]),
        meta={
            "issue_id": created.get("issue_id", ""),
            "short_id": created.get("short_id", ""),
            "github_repo": created.get("github_repo", git_state["github_repo"]),
            "branch": created.get("branch", req_payload["branch"]),
            "base_sha": git_state["base_sha"],
            "had_base_patch": bool(base_patch.strip()),
            "source_conversation_id": source_conversation_id.strip(),
            "gateway_url": client.base_url,
            "created_at": datetime.now(UTC).isoformat(),
            "pulled": False,
        },
        initial_patch=base_patch,
    )
    return created


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="handoff",
        description="Hand off an Antigravity session to makesense k3s and poll/pull results on wake.",
    )
    parser.add_argument("--repo-dir", default=".", help="Local git repository directory.")
    parser.add_argument("--url", default="", help="Override gateway base URL.")
    parser.add_argument("--token", default="", help="Override PWA_API_TOKEN.")
    parser.add_argument(
        "--verify-tls",
        action="store_true",
        help="Verify TLS with system CA bundle (off by default for homelab-ca over NetBird).",
    )

    sub = parser.add_subparsers(dest="subcommand")

    def _add_push_args(p: argparse.ArgumentParser, *, prompt_required: bool = False) -> None:
        p.add_argument(
            "--prompt",
            default="",
            required=prompt_required,
            help="Task instructions / Semantic State (s_t) for the k3s worker pod.",
        )
        p.add_argument("--prompt-file", default="", help="Read handoff prompt from a file.")
        p.add_argument("--repo", default="", help="Override GitHub repo (owner/name).")
        p.add_argument("--branch", default="", help="Override git branch.")
        p.add_argument("--verify-hint", default="", help="Verification command for the worker pod.")
        p.add_argument("--focus-subdir", default="", help="Focus subdirectory inside the repo.")
        p.add_argument(
            "--model", default="", help="Worker model override (hydrogen, argon, carbon)."
        )
        p.add_argument("--title", default="", help="Short title for the handoff thread.")
        p.add_argument(
            "--conversation-id",
            default="",
            help="Local Antigravity conversation ID for traceability.",
        )
        p.add_argument(
            "--no-patch",
            action="store_true",
            help="Do not include local uncommitted git diff HEAD.",
        )

    # 1. `run` (default for `/handoff` without `push`): push (if prompt given or no active handoff) + poll on wake + apply
    run_p = sub.add_parser(
        "run",
        help="Push handoff (if prompt provided) and poll on wake until complete, then apply changes.",
    )
    _add_push_args(run_p, prompt_required=False)
    run_p.add_argument("--id", default="", help="Existing handoff short_id or issue_id to poll.")
    run_p.add_argument("--interval", type=float, default=15.0, help="Poll interval in seconds.")
    run_p.add_argument("--timeout", type=float, default=86400.0, help="Max poll wait in seconds.")
    run_p.add_argument(
        "--no-apply", action="store_true", help="Do not auto-apply fix.patch on completion."
    )

    # 2. `push`: push handoff and exit immediately without polling
    push_p = sub.add_parser(
        "push", help="Push handoff to k3s and exit immediately without polling."
    )
    _add_push_args(push_p, prompt_required=False)

    # 3. `wait` / `poll`: poll an active handoff across sleep/wake and auto-apply when done
    wait_p = sub.add_parser(
        "wait",
        aliases=["poll"],
        help="Poll an active k3s handoff across laptop sleep/wake and pull changes when complete.",
    )
    wait_p.add_argument("--id", default="", help="Handoff short_id, issue_id, or 'latest'.")
    wait_p.add_argument("--repo", default="", help="Filter 'latest' by GitHub repo (owner/name).")
    wait_p.add_argument("--interval", type=float, default=15.0, help="Poll interval in seconds.")
    wait_p.add_argument("--timeout", type=float, default=86400.0, help="Max poll wait in seconds.")
    wait_p.add_argument("--no-apply", action="store_true", help="Do not auto-apply fix.patch.")

    # 4. `pull`: immediately pull and apply fix.patch from a handoff
    pull_p = sub.add_parser("pull", help="Immediately pull and apply changes from a k3s handoff.")
    pull_p.add_argument("--id", default="", help="Handoff short_id, issue_id, or 'latest'.")
    pull_p.add_argument("--repo", default="", help="Filter 'latest' by GitHub repo (owner/name).")
    pull_p.add_argument(
        "--no-apply", action="store_true", help="Print report without applying fix.patch."
    )

    # 5. `status`: inspect remote status
    status_p = sub.add_parser("status", help="Check status of the active or latest k3s handoff.")
    status_p.add_argument("--id", default="", help="Handoff short_id, issue_id, or 'latest'.")
    status_p.add_argument("--repo", default="", help="Filter by GitHub repo (owner/name).")

    return parser


def _resolve_target_id_and_repo(
    repo_dir: Path,
    *,
    explicit_id: str = "",
    explicit_repo: str = "",
) -> tuple[str, str]:
    local_state = load_local_handoff_state(repo_dir)
    target_id = explicit_id.strip()
    target_repo = explicit_repo.strip()
    if not target_id and local_state:
        target_id = str(local_state.get("short_id") or local_state.get("issue_id") or "")
    if not target_repo and local_state:
        target_repo = str(local_state.get("github_repo") or "")
    if not target_repo:
        try:
            git_info = capture_local_git_state(repo_dir)
            target_repo = git_info.get("github_repo", "")
        except subprocess.CalledProcessError:
            pass
    return (target_id or "latest"), target_repo


def main(argv: list[str] | None = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    subcmd = args.subcommand or "run"
    repo_dir = Path(args.repo_dir).resolve()

    base_url, api_token = resolve_connection_config(
        url_arg=args.url,
        token_arg=args.token,
        repo_dir=repo_dir,
    )
    if not api_token:
        print(
            "Error: Missing PWA_API_TOKEN / AGY_HANDOFF_TOKEN. Set it in .env.makesense or ~/.gemini/config/handoff.env.",
            file=sys.stderr,
        )
        return 2

    client = HandoffHttpClient(
        base_url=base_url,
        api_token=api_token,
        verify_tls=bool(args.verify_tls),
    )

    if subcmd in {"push", "run"}:
        prompt_text = getattr(args, "prompt", "").strip()
        prompt_file = getattr(args, "prompt_file", "").strip()
        if prompt_file:
            prompt_text = Path(prompt_file).read_text(encoding="utf-8").strip()

        if subcmd == "push" and not prompt_text:
            print("Error: `handoff push` requires `--prompt` or `--prompt-file`.", file=sys.stderr)
            return 2

        target_id = getattr(args, "id", "").strip()
        if prompt_text:
            created = _push_handoff(
                client=client,
                repo_dir=repo_dir,
                prompt=prompt_text,
                repo_override=getattr(args, "repo", ""),
                branch_override=getattr(args, "branch", ""),
                verify_hint=getattr(args, "verify_hint", ""),
                focus_subdir=getattr(args, "focus_subdir", ""),
                model=getattr(args, "model", ""),
                title=getattr(args, "title", ""),
                source_conversation_id=getattr(args, "conversation_id", ""),
                include_working_tree_patch=not bool(getattr(args, "no_patch", False)),
            )
            target_id = str(created.get("short_id") or created.get("issue_id") or "")
            print(
                f"[handoff] Pushed `{created.get('short_id')}` (`{created.get('issue_id')}`) "
                f"to {base_url} for `{created.get('github_repo')}` "
                f"(`{created.get('branch')}`, model `{created.get('model')}`, "
                f"had_base_patch={created.get('had_base_patch')}).",
                flush=True,
            )
            if subcmd == "push":
                print(json.dumps(created, indent=2))
                return 0

        # `run` mode continues directly into sleep-resilient polling + auto-apply
        poll_id, poll_repo = _resolve_target_id_and_repo(
            repo_dir,
            explicit_id=target_id,
            explicit_repo=getattr(args, "repo", ""),
        )
        print(
            f"[handoff] Entering wake-resilient polling mode for `{poll_id}` "
            f"(interval={args.interval}s). Safe to close laptop lid...",
            flush=True,
        )
        final_payload = poll_until_terminal(
            fetch_fn=lambda inc_diff: client.get_handoff(
                poll_id, repo=poll_repo, include_diff=inc_diff
            ),
            poll_interval_s=float(args.interval),
            max_wait_s=float(args.timeout),
            on_tick=lambda msg: print(msg, flush=True),
        )
        apply_res: dict[str, Any] | None = None
        if not args.no_apply and final_payload.get("has_patch"):
            apply_res = apply_remote_handoff_patch(
                repo_dir,
                remote_diff=str(final_payload.get("diff") or ""),
                handoff_id=str(final_payload.get("short_id") or ""),
            )
        print(format_handoff_summary(final_payload, apply_result=apply_res), flush=True)
        return 0 if final_payload.get("status") != "failed" else 1

    if subcmd in {"wait", "poll"}:
        poll_id, poll_repo = _resolve_target_id_and_repo(
            repo_dir,
            explicit_id=args.id,
            explicit_repo=args.repo,
        )
        print(
            f"[handoff] Polling `{poll_id}` on {base_url} across sleep/wake (interval={args.interval}s)...",
            flush=True,
        )
        final_payload = poll_until_terminal(
            fetch_fn=lambda inc_diff: client.get_handoff(
                poll_id, repo=poll_repo, include_diff=inc_diff
            ),
            poll_interval_s=float(args.interval),
            max_wait_s=float(args.timeout),
            on_tick=lambda msg: print(msg, flush=True),
        )
        apply_res = None
        if not args.no_apply and final_payload.get("has_patch"):
            apply_res = apply_remote_handoff_patch(
                repo_dir,
                remote_diff=str(final_payload.get("diff") or ""),
                handoff_id=str(final_payload.get("short_id") or ""),
            )
        print(format_handoff_summary(final_payload, apply_result=apply_res), flush=True)
        return 0 if final_payload.get("status") != "failed" else 1

    if subcmd == "pull":
        pull_id, pull_repo = _resolve_target_id_and_repo(
            repo_dir,
            explicit_id=args.id,
            explicit_repo=args.repo,
        )
        payload = client.get_handoff(pull_id, repo=pull_repo, include_diff=True)
        apply_res = None
        if not args.no_apply and payload.get("has_patch"):
            apply_res = apply_remote_handoff_patch(
                repo_dir,
                remote_diff=str(payload.get("diff") or ""),
                handoff_id=str(payload.get("short_id") or ""),
            )
        print(format_handoff_summary(payload, apply_result=apply_res), flush=True)
        return 0

    if subcmd == "status":
        stat_id, stat_repo = _resolve_target_id_and_repo(
            repo_dir,
            explicit_id=args.id,
            explicit_repo=args.repo,
        )
        payload = client.get_handoff(stat_id, repo=stat_repo, include_diff=False)
        print(format_handoff_summary(payload), flush=True)
        return 0

    return 2


if __name__ == "__main__":
    raise SystemExit(main())
