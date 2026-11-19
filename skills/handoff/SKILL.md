---
name: handoff
description: Hand off the current Antigravity coding session and uncommitted working-tree diff to a k3s worker pod (/handoff, /handoff push, /handoff pull, /handoff watch, /handoff status). Calling /handoff without push starts in sleep-resilient poll-on-wake mode so the local session automatically pulls changes when the laptop wakes.
---

# Cloud Session Handoff (`/handoff`)

Use this skill when the user invokes `/handoff` (or asks to hand off a task to their k3s cluster while driving / away from their laptop).

## Subcommands & Modes

1. **`/handoff` or `/handoff <instructions>` (Default: Push + Poll-on-Wake Mode)**
   - **What it does:**
     1. Synthesize a compact Semantic State ($s_t$) of the current conversation (Goal, Completed Changes, Remaining Tasks, Verification Commands).
     2. Run `handoff_client.py start` with `--prompt` and `--state`. This pushes the current git base SHA, branch, uncommitted working-tree diff (`git diff --binary HEAD`), and prompt to `POST /api/handoff` on the k3s cluster, and **immediately enters sleep-resilient polling mode**.
     3. Use `WaitMsBeforeAsync=3000` on `run_command` so the push confirmation is captured immediately while the sleep-resilient poll loop continues in the background.
     4. **Stop calling tools after launching the background poller.** While the laptop is asleep in the car, `handoff_client.py` ignores transient VPN/network drops. As soon as the laptop wakes and the remote k3s pod reaches a terminal state (`awaiting_review`, `completed`, or `failed`), the background task automatically applies the remote `fix.patch` to the local working tree, marks the handoff pulled, and exits — triggering **Reactive Wakeup** in this conversation so you can immediately summarize the pod's changes!
   - **Model & Effort Selection (Default to Self):** Always pass `--model` matching **your own active model + effort tier** (e.g. `gemini-4-neon-medium`, `gemini-4-hydrogen-high`, `claude-sonnet-5-5-medium`) unless the user explicitly asks for another model or effort tier (in which case pass `--model <model>` and optionally `--effort <low|medium|high>`).
   - **Command:**
     ```bash
     python3 ~/.gemini/config/skills/handoff/scripts/handoff_client.py start \
       --workdir "<workspace_root>" \
       --model "<your-active-model-id>" \
       --effort "<low|medium|high>" \
       --prompt "<remaining task instructions>" \
       --state "<compact semantic state s_t>" \
       --verify-hint "<pytest / linter command>"
     ```
     *(Omit `--effort` if `--model` already includes the effort suffix, e.g. `gemini-4-neon-medium`.)*
   - *(Note: If the user runs bare `/handoff` when an unpulled handoff is already active for this workspace and provides no new task instructions, omit `--prompt` so `handoff_client.py start` attaches directly to the active handoff in poll-on-wake mode.)*

2. **`/handoff push <instructions>` (Push Only — No Local Poller)**
   - Pushes the session state and working-tree diff to the k3s cluster and returns immediately without starting a background poll task:
     ```bash
     python3 ~/.gemini/config/skills/handoff/scripts/handoff_client.py push \
       --workdir "<workspace_root>" \
       --model "<your-active-model-id>" \
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
   - Starts the sleep-resilient background poller for an existing remote handoff (run with `WaitMsBeforeAsync=2000` and stop calling tools to wait for Reactive Wakeup):
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

## Configuration Discovery

`handoff_client.py` resolves connection settings automatically in priority order:
1. CLI flags (`--url`, `--token`, `--ca-cert`, `--insecure`)
2. Environment variables (`AGY_HANDOFF_URL`, `AGY_HANDOFF_TOKEN`, `PWA_API_TOKEN`)
3. Workspace `.agy-handoff.env`, `.env`, or `.env.makesense`
4. User config `~/.config/agy-cloud/config.env`
