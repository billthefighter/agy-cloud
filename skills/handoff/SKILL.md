---
name: handoff
description: Hand off the current Antigravity coding session and uncommitted working-tree diff to a k3s worker pod (/handoff, /handoff push, /handoff pull, /handoff watch, /handoff status). Calling /handoff without push starts in sleep-resilient poll-on-wake mode so the local session automatically pulls changes when the laptop wakes.
---

# Cloud Session Handoff (`/handoff`)

Use this skill when the user invokes `/handoff` (or asks to hand off a task to their k3s cluster while driving / away from their laptop).

## Fast-Launch Protocol (Read First)

Follow this protocol on Turn 1 to launch in 2 tool calls with zero wasted exploration:

1. **DO NOT read `handoff_client.py` source code** and **DO NOT run manual preflight probes** (`git ls-remote`, `git merge-base`, `wc -c`, or `transcript.jsonl` scans) — `handoff_client.py` already handles git ref probing, config URL resolution, model auto-detection, repo-access checks, and draft caching automatically.
2. **Keep `--prompt` concise (5–15 lines).** Attach external plans, design docs, or large logs with repeatable `--context-file <path>` (or pass `--prompt-file <path>` / `--prompt -`). Context files are mounted read-only under `/artifacts/context/<name>` so they never dirty `/work`.
3. **Omit `--model`** unless the user explicitly asks for another model — `handoff_client.py` auto-detects the active session model from Antigravity transcripts (falling back to `gemini-4-hydrogen`). Human names like `"Gemini 4 Neon (High)"` and shorthands (`neon`, `hydrogen`) also work when overriding.
4. **Default Two-Step Flow:**
   - **Step 1 (Synchronous push):** Run `handoff_client.py push` with `WaitMsBeforeAsync=10000` so you immediately get the `handoff_id` (or any preflight message):
     ```bash
     python3 ~/.gemini/config/skills/handoff/scripts/handoff_client.py push \
       --workdir "<workspace_root>" \
       --prompt "<5-15 line task instructions>" \
       --context-file "<optional/plan.md>" \
       --state "<compact semantic state s_t>" \
       --verify-hint "<pytest / linter command>"
     ```
   - **Step 2 (Background watch):** Run `handoff_client.py watch --handoff-id <id>` with `WaitMsBeforeAsync=2000`, then **stop calling tools** to wait for Reactive Wakeup when the remote pod finishes (duplicate watchers exit `0` cleanly via a singleton lock):
     ```bash
     python3 ~/.gemini/config/skills/handoff/scripts/handoff_client.py watch \
       --workdir "<workspace_root>" \
       --handoff-id "<handoff_id>"
     ```
5. **On Preflight Failure — Always Use `--retry` (Never Rewrite the Prompt):** `push` caches raw inputs in `.git/agy_handoff_pending.json` before git/network preflight. If push fails (e.g. unpushed commits exceeding inline patch limit), re-run the printed single-line retry command with `--retry` (and `--push-branch <branch>` if instructed):
   ```bash
   python3 ~/.gemini/config/skills/handoff/scripts/handoff_client.py push \
     --workdir "<workspace_root>" --retry --push-branch <branch>
   ```

## Subcommands & Modes

1. **`/handoff` or `/handoff <instructions>` (Default: Two-Step Push + Background Watch)**
   - Run **Step 1 (`push`)** synchronously (`WaitMsBeforeAsync=10000`), then **Step 2 (`watch --handoff-id <id>`)** in the background (`WaitMsBeforeAsync=2000`), and **stop calling tools** so Reactive Wakeup notifies you when the pod finishes and applies `fix.patch` locally.
   - *(Optional single-command alternative: `handoff_client.py start` pushes and immediately enters sleep-resilient polling mode. If the user runs bare `/handoff` with an active unpulled handoff and no new instructions, omit `--prompt` so `start` attaches directly to the active handoff.)*
   - **Optional Flags (`push` & `start`):**
     - `--context-file <path>` (repeatable): Attach read-only UTF-8 files under `/artifacts/context/<name>` without dirtying `/work`.
     - `--prompt-file <path>` or `--prompt -`: Read prompt from a file or stdin.
     - `--timeout <seconds>`: Override default 2h worker wall-clock deadline (server clamps to `[300, 14400]`; productive wall-clock timeouts auto-continue).
     - `--retry`: Reload cached draft from `.git/agy_handoff_pending.json` without rewriting prompt/context flags.
     - `--push-branch [BRANCH]`: Push `HEAD` to `origin/<branch>` before git metadata detection.
     - `--model <id|human-name|auto>` / `--effort <low|medium|high>`: Override auto-detected model.

2. **`/handoff push <instructions>` (Push Only — No Local Poller)**
   - Pushes the session state and working-tree diff to the k3s cluster and returns immediately without starting a background poll task:
     ```bash
     python3 ~/.gemini/config/skills/handoff/scripts/handoff_client.py push \
       --workdir "<workspace_root>" \
       --prompt "<remaining task instructions>" \
       --state "<compact semantic state s_t>" \
       --verify-hint "<pytest / linter command>"
     ```

3. **`/handoff pull [<handoff_id>]` (Immediate Pull & Patch Apply)**
   - Fetches the latest (or specified) handoff from k3s, safely backs up any local uncommitted diff to `.git/agy_handoff_pre_pull_backup.patch`, applies the remote pod's `fix.patch` to the working tree, and marks the handoff pulled:
     ```bash
     python3 ~/.gemini/config/skills/handoff/scripts/handoff_client.py pull \
       --workdir "<workspace_root>" \
       --handoff-id "<handoff_id_or_empty_for_latest>"
     ```

4. **`/handoff watch [<handoff_id>]` (Attach Poll-on-Wake to Existing Handoff)**
   - Starts the sleep-resilient background poller for an existing remote handoff (run with `WaitMsBeforeAsync=2000` and stop calling tools to wait for Reactive Wakeup). Protected by a per-handoff `flock` lock so duplicate watchers exit `0` cleanly:
     ```bash
     python3 ~/.gemini/config/skills/handoff/scripts/handoff_client.py watch \
       --workdir "<workspace_root>" \
       --handoff-id "<handoff_id_or_empty_for_latest>"
     ```

5. **`/handoff status [<handoff_id>]` (Check Status)**
   - Inspects the remote handoff status and `TriageReport` without modifying the working tree:
     ```bash
     python3 ~/.gemini/config/skills/handoff/scripts/handoff_client.py status \
       --workdir "<workspace_root>" \
       --handoff-id "<handoff_id_or_empty_for_latest>"
     ```

## Submodules, Private Repos & On-the-Fly Snapshots

- **Git Submodules / Nested Repos:** If the user's task targets files inside a git submodule (e.g. `gitops/apps/agy-cloud-private-plugins`), pass that submodule directory directly as `--workdir` (or pass `--focus-subdir <submodule_path>`). In Git, a superproject's `git diff HEAD` omits file edits inside submodules, so targeting the submodule directly ensures remote diffs capture and apply cleanly. (`handoff_client.py` also auto-detects single-submodule targets from `--focus-subdir` or `--prompt`, retargets to the submodule, and remembers `target_workdir` for subsequent `watch`/`pull`/`status` calls.)
- **Private Repos & Local-Only Repos (Automatic Snapshot Fallback):** Before posting to `/api/handoff`, `handoff_client.py` queries `GET /api/handoff/repo-access?repo=<owner/name>`. If the k3s cluster cannot mirror the target repository (e.g. a private repo/submodule whose owner isn't covered by controller secrets, or a local-only git repo), `handoff_client.py` automatically packs a compressed `HEAD` snapshot (`git archive --format=tar.gz HEAD`) and uploads it on-the-fly alongside any uncommitted diff — no k3s credentials needed and zero GitHub tokens ever enter the worker pod. Pass `--upload-repo` to force snapshot upload (or `--no-upload-repo` to disable automatic fallback).
- **Per-Owner / Per-Repo GitHub Secrets on k3s (Optional):** To let the controller mirror private repos directly (and enable Slack/PWA coding tasks & Draft PRs on them), add `GITHUB_TOKEN_<OWNER>` (e.g. `GITHUB_TOKEN_BILLTHEFIGHTER`) or `GITHUB_TOKENS_JSON` (`{"owner": "ghp_...", "owner/repo": "github_pat_..."}`) to `agent-tasks-controller-secret` on k3s.

## Configuration Discovery

`handoff_client.py` resolves connection settings automatically in priority order:
1. CLI flags (`--url`, `--token`, `--ca-cert`, `--insecure`)
2. Explicit `AGY_HANDOFF_URL` / `AGY_CLOUD_URL` across environment variables, workspace/parent `.agy-handoff.env` / `.env` / `.env.makesense`, and `~/.config/agy-cloud/config.env`
3. Fallback `NETBIRD_HOSTNAME` from workspace/parent `.env` files (only if no explicit URL was found in any layer)
4. Nonexistent `AGY_HANDOFF_CA_CERT` paths are ignored automatically
