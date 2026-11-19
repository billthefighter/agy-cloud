# `agy-cloud` — Kubernetes Cloud Handoff & Extensible Plugin Runtime for Google Antigravity

`agy-cloud` is a self-hosted Kubernetes (k3s / K8s) controller, sandboxed worker pod orchestrator, and extensible plugin microkernel for **Google Antigravity** (`google-antigravity` SDK) agents.

It is designed around two core use cases:
1. **Commute / Sleep-Resilient Session Handoff (`/handoff`)**: Hand off an active coding task—including your uncommitted working-tree `git diff` and conversation state—from your laptop to an isolated Kubernetes pod before you close your laptop lid, monitor or reply from your phone (via an embedded mobile PWA or Slack), and automatically pull the resulting patch back into your local working tree when your laptop wakes up.
2. **Always-On Background Agent Automations & Plugins**: Run long-lived background loops and interactive chat/mobile workflows (Sentry issue triage, GitHub Actions CI healing, Kubernetes/Argo CD crash watchdogs, scheduled dependency upgrades, or custom third-party plugins loaded from external Git repositories).

---

## Table of Contents

1. [How Session Handoff Works](#1-how-session-handoff-works)
2. [Repository Boundary: What Is (and Isn't) in This Repo](#2-repository-boundary-what-is-and-isnt-in-this-repo)
3. [Prerequisites & Infrastructure Requirements](#3-prerequisites--infrastructure-requirements)
4. [Limitations & Architectural Constraints](#4-limitations--architectural-constraints)
5. [Architecture & Security Model](#5-architecture--security-model)
6. [Deployment Guide (Kubernetes / k3s & Local Dev)](#6-deployment-guide-kubernetes--k3s--local-dev)
7. [Laptop Client Setup & Using `/handoff`](#7-laptop-client-setup--using-handoff)
8. [Loading External Plugin Repositories & ConfigMap Overrides](#8-loading-external-plugin-repositories--configmap-overrides)
9. [Authoring Custom Plugins](#9-authoring-custom-plugins)

---

## 1. How Session Handoff Works

When you close your laptop, local Antigravity processes, terminal child processes, and streaming model connections suspend. `agy-cloud` provides continuity without requiring live OS process migration by transferring **code state + semantic state**:

```text
Laptop (Before Closing Lid)                 Kubernetes Cluster (agy-cloud)
───────────────────────────                 ──────────────────────────────
1. Run `/handoff <instructions>`
   • Captures git remote, branch, &
     `base_sha` (`git rev-parse HEAD`)
   • Captures dirty working tree diff
     (`git add -N . && git diff --binary HEAD`)
   • Synthesizes Semantic State (s_t):
     goal, completed work, remaining steps,
     and test verification command
   • POSTs bundle to gateway ────────────► 2. POST /agy/api/handoff
   • Starts background sleep-resilient        • Syncs token-free bare git mirror on PVC
     poller (`handoff_client.py start`)       • Writes `task_prompt.md` + `fix.patch`
                                              • Spawns locked-down K8s Job pod:
   (You close laptop & commute)                 - initContainer clones mirror at `base_sha`
                                                  and applies your pre-handoff `fix.patch`
                                                - agy-worker container runs `google-antigravity`
                                                  SDK turn(s), runs linters/tests, and writes
                                                  cumulative `git diff HEAD` → `fix.patch`
                                              • Posts live progress & diff to Mobile PWA / Slack
Laptop (On Wake at Destination)
───────────────────────────────
3. Poller detects wake & reconnect ◄────── 4. GET /agy/api/handoff/<id>
   • Fetches completed `fix.patch` &          • Returns `is_terminal: true`, `TriageReport`,
     structured `TriageReport`                  token/cost usage, and cumulative `fix.patch`
   • Backs up local pre-pull diff to
     `.git/agy_handoff_pre_pull_backup.patch`
   • Applies remote `fix.patch` to local
     working tree & wakes local AGY session
```

---

## 2. Repository Boundary: What Is (and Isn't) in This Repo

Because `agy-cloud` sits at the boundary between your laptop, your source control, and your Kubernetes cluster, it is important to be explicit about what this repository owns versus what your infrastructure must provide.

### Included in `agy-cloud`
- **Microkernel Controller & Gateway (`src/agy_cloud/`, `src/agent_tasks/`)**:
  - FastAPI HTTP/WebSocket gateway serving `/api/handoff`, `/api/plugins`, `/api/threads`, `/api/ops`, and an installable iOS/mobile Progressive Web App (`/agy/`) with VAPID Web Push notifications.
  - Optional Slack Socket Mode adapter (`SlackSocketAdapter`) for outbound-only WSS chat control (zero public inbound webhooks required).
  - Pluggable outbound notification transports (`discord_webhook`, `telegram`, `ntfy`, `slack_webhook`, generic `webhook`).
  - Monthly AI credit budget ledger & background safety brake (`billing_guard`).
- **Kubernetes Worker Pod Orchestrator (`K8sJobRunner` & `worker_main.py`)**:
  - Token-free bare Git mirror manager (`git clone --mirror` on PVC for `<1s` worker pod checkouts).
  - Sandboxed `batch/v1` Kubernetes `Job` generator with `BASE_SHA` checkout and Turn-1 patch application.
- **Public Plugin SDK (`src/agy_cloud/sdk/`) & Built-In Ops Plugins (`plugins/`)**:
  - Declarative plugin framework (`AgyCloudPlugin`, `PluginContext`, `@command`, `@channel_listener`, `@thread_reply_handler`, `@button_action`, `@cron`).
  - Built-in coding & cluster automation plugins: `repo_healer`, `sentry_triage`, `ci_healer`, `k8s_watchdog`, `dep_upgrader`.
- **Portable Laptop Client Skill (`skills/handoff/`, `src/agy_cloud/client/handoff_client.py`)**:
  - Zero-dependency Python stdlib CLI (`agy-handoff`) + Antigravity `SKILL.md` for `/handoff`, `/handoff push`, `/handoff pull`, `/handoff watch`, and `/handoff status`.
- **Deployment Packaging (`deploy/k8s/`, `deploy/k3s/`, `charts/agy-cloud/`, `Dockerfile.*`)**:
  - Multi-arch Dockerfiles, local `docker-compose.yml`, Kustomize manifests, and a Helm chart.

### NOT Included in `agy-cloud` (External Boundaries)
- **Cluster Provisioning & Private Networking**:
  - This repo does **not** provision VMs, Kubernetes/k3s nodes, StorageClasses, or VPNs. You must bring an existing Kubernetes cluster (or Docker Compose host) and a secure way for your laptop/phone to reach the controller's HTTP port (e.g., **NetBird**, **Tailscale**, **WireGuard**, **Cloudflare Tunnel**, or an authenticated Ingress).
- **Personal / Domain-Specific Private Plugins**:
  - Personal lifestyle or proprietary plugins (for example, Garmin Connect biometrics and multimodal nutrition/macro tracking) live in separate repositories (e.g., `agy-cloud-private-plugins`) and are loaded dynamically at runtime via `plugin_sources:` in your cluster `ConfigMap`.

---

## 3. Prerequisites & Infrastructure Requirements

### A. Kubernetes Cluster Requirements (Production Mode)
1. **Kubernetes 1.27+ (k3s, K8s, EKS/GKE, or Talos)**:
   - A dedicated namespace (default `agent-tasks` or `agy-cloud`) with RBAC allowing the controller `ServiceAccount` to create/watch/delete `batch/v1` `Job`s and read pod logs.
2. **Persistent Volume Claim (`10Gi`–`25Gi`) & Node Co-Location**:
   - Both the long-lived controller pod and short-lived worker `Job` pods mount subdirectories (`subPath: mirrors/...`, `sessions/...`, `artifacts/...`, `caches/...`) of the **same PVC**.
   - **If your cluster uses a `ReadWriteOnce` (`RWO`) node-local StorageClass (such as k3s default `local-path`)**:
     - The controller Deployment **must** run with `replicas: 1` and `strategy: {type: Recreate}` so rollouts never deadlock waiting for volume detachment across nodes.
     - Worker `Job` pods **must** schedule onto the same node as the controller pod. `agy-cloud` handles this automatically via `podAffinity` and `MY_NODE_NAME` (`spec.nodeName` downward API).
3. **Network Reachability from Laptop & Mobile**:
   - Your laptop must be able to reach `AGY_HANDOFF_URL` (e.g. `https://<your-k3s-host>/agy` over Tailscale/NetBird or Ingress) over HTTPS/HTTP when awake.

### B. Required & Optional Credentials

| Secret / Variable | Scope | Required? | Purpose |
|---|---|---|---|
| `GEMINI_API_KEY` | Controller & Worker Pods | **Required** | Google AI Studio / Gemini API key used by the `google-antigravity` SDK inside worker pods (supports Vertex AI via `GOOGLE_GENAI_USE_VERTEXAI=true`). |
| `PWA_API_TOKEN` (or `AGY_HANDOFF_TOKEN`) | Controller & Laptop Client | **Required** | High-entropy shared secret authenticating `/handoff` CLI requests and Mobile PWA sign-in. |
| `TARGET_GITHUB_TOKEN` | Controller Pod **Only** | **Required** (for private repos / PR creation) | GitHub PAT (`Contents: Read/Write`, `Pull requests: Read/Write`) used by the controller to sync bare Git mirrors and open Draft PRs. **Never mounted into worker pods.** |
| `SLACK_BOT_TOKEN` & `SLACK_APP_TOKEN` | Controller Pod **Only** | Optional (`SLACK_ENABLED=false` disables) | Slack Bot (`xoxb-...`) and Socket Mode (`xapp-...`) tokens for `#agy-*` channel interaction. |
| `SENTRY_AUTH_TOKEN` | Controller Pod **Only** | Optional | Read-only Sentry token (`Project:Read`, `Issue & Event:Read`) if using the `sentry_triage` plugin. |

### C. Laptop Client Requirements
- **Google Antigravity** (IDE or `agy` CLI)
- **`git`** and **`python3` (`>= 3.10`)** — `handoff_client.py` uses only the Python standard library (`urllib.request`, `subprocess`, `json`, `ssl`), so no `pip` install or virtualenv is required in your project workspaces.

---

## 4. Limitations & Architectural Constraints

Please read these constraints carefully before deploying:

1. **Git State + Semantic Prompt Handoff (Not Live Process Checkpoint/Restore)**:
   - `agy-cloud` does **not** freeze or migrate running processes, open TCP sockets, local databases, Docker containers, or unpushed git stash entries from your Mac/Linux laptop.
   - It transfers your **uncommitted working-tree diff (`git diff --binary HEAD`, including untracked non-ignored files)** plus a **structured Semantic State ($s_t$) prompt**, and starts a new `google-antigravity` SDK execution turn inside an ephemeral Linux container on Kubernetes.
2. **Your Base Commit (`HEAD`) Must Be Pushed to the Remote Git Origin**:
   - The k3s worker pod clones your repository from the controller's bare Git mirror (`origin`) and checks out your laptop's `base_sha` (`git rev-parse HEAD`) before applying your uncommitted diff.
   - If you have **committed locally without `git push`**, the remote bare mirror will not have your `base_sha` yet (the worker `initContainer` will warn and fall back to the branch tip, which can cause `git apply` conflicts). Always `git push` committed work—or leave in-progress work uncommitted—before running `/handoff`.
3. **Gitignored Files (`.env`, Local Build Artifacts, Credentials) Are Not Transferred**:
   - By design, `handoff_client.py` runs `git add -N .` which obeys your repository's `.gitignore`. Local `.env` files or unversioned credentials on your laptop are never uploaded to the cluster. Tests run by the worker pod must be self-contained (using fixtures/mocks) rather than relying on laptop-local `.env` files.
4. **Linux Container Worker Environment Only**:
   - Worker pods run inside a non-root Debian Linux container (`Python 3.12+` / `uv` / `Node`). Tasks that require macOS hardware/toolchains (`xcodebuild`, iOS Simulator, Metal), physical USB devices, or interactive GUI apps cannot be executed by k3s worker pods.
5. **Single-Replica SQLite Controller**:
   - The controller persists state in WAL-mode SQLite databases (`/state/state.sqlite3`, `/state/handoffs.sqlite3`, `/state/plugins/<id>/*.sqlite3`) on a single PVC. Horizontal scaling (`replicas > 1`) of the controller Deployment is not supported.
6. **In-Process Server Plugins Require Trust**:
   - While *worker jobs* run in heavily sandboxed ephemeral pods, *controller plugins* loaded via `plugin_sources:` execute Python code inside the controller pod. Only load plugin repositories you trust, and set `require_pinned_refs: true` in production ConfigMaps to enforce immutable commit SHAs or release tags.

---

## 5. Architecture & Security Model

```text
Namespace: agent-tasks / agy-cloud
┌────────────────────────────────────────────────────────────────────────────┐
│ Deployment: agy-cloud-controller (1 replica, strategy: Recreate)           │
│  • Core Handoff API : POST/GET /agy/api/handoff (laptop <-> pod handoff)   │
│  • Mobile PWA       : /agy/ & /health/ + WebSocket + VAPID Web Push        │
│  • Channel Bus      : Slack Socket Mode + Discord/Telegram/Ntfy/Webhooks   │
│  • PluginRegistry   : Loads local plugins/ + external Git `plugin_sources` │
│  • Git Mirror Sync  : Updates bare mirrors WITHOUT saving tokens to disk   │
└───────────────┬────────────────────────────────────────────▲───────────────┘
                │ spawns batch/v1 Job (pinned to same node)  │ reads result.json
                ▼                                            │ + fix.patch
┌────────────────────────────────────────────────────────────┴───────────────┐
│ PVC: agent-tasks-state (ReadWriteOnce)                                     │
│  ├── state.sqlite3 & handoffs.sqlite3 (tasks, threads, sessions, billing)  │
│  ├── plugins/<plugin_id>/             (isolated plugin SQLite DBs & media) │
│  ├── plugin_repos/                    (cloned external plugin Git repos)   │
│  ├── plugin_site_packages/            (bootstrapped plugin PyPI deps)      │
│  ├── mirrors/<owner>__<repo>.git      (token-free bare git mirrors)        │
│  ├── caches/{uv,npm}/                 (warm package caches across runs)    │
│  ├── sessions/<task_id>/              (AGY SDK save_dir trajectories)      │
│  └── artifacts/<task_id>/             (task_prompt.md, fix.patch, result)  │
└───────────────▲────────────────────────────────────────────┬───────────────┘
                │ subPath mounts                             │
┌───────────────┴────────────────────────────────────────────▼───────────────┐
│ Job: agy-triage-<task-id>-t<turn> (ephemeral sandboxed worker pod)         │
│  1. initContainer (git-clone):                                             │
│     • Clones read-only `/mirror` → `emptyDir` `/work`, checks out BASE_SHA │
│     • Applies pre-handoff or prior-turn `/artifacts/fix.patch`             │
│  2. container (agy-worker):                                                │
│     • Secret: ONLY `GEMINI_API_KEY` (zero GitHub/Slack/Sentry/K8s tokens)  │
│     • RBAC: `automountServiceAccountToken: false`                          │
│     • NetworkPolicy: blocks private LAN / cluster / mesh CIDRs             │
│     • Policy Hook: denies `git push` and `git remote` mutations            │
│     • Runs `google-antigravity` SDK, tests changes, writes `fix.patch`     │
└────────────────────────────────────────────────────────────────────────────┘
```

---

## 6. Deployment Guide (Kubernetes / k3s & Local Dev)

### Option A: Local Development / Evaluation (No Kubernetes Required)

You can run the microkernel gateway and test plugins or `/handoff` locally on your machine using `--worker-runner=local` (which runs worker jobs in local temporary Git checkouts instead of spawning Kubernetes `batch/v1` Jobs):

```bash
uv venv && uv pip install -e ".[dev,k8s,slack]"
uv run pytest

# Start the gateway locally on http://localhost:8080 with local subprocess worker runner:
uv run agy-cloud serve \
  --port 8080 \
  --state-dir /tmp/agy-cloud-state \
  --plugins-dir ./plugins \
  --config ./config.example.yaml \
  --worker-runner local \
  --api-token dev-local-token
```

Or with Docker Compose:

```bash
GEMINI_API_KEY="AIza..." AGY_CLOUD_TOKEN="dev-local-token" docker compose up --build
```

### Option B: Kubernetes / k3s Deployment

1. **Create Namespace & Secrets**:
   Notice that `agent-tasks-worker-secret` contains **only** `GEMINI_API_KEY`, keeping your GitHub and Slack tokens isolated to the controller pod:
   ```bash
   kubectl create namespace agent-tasks

   # 1. Worker secret (ONLY Gemini API key)
   kubectl -n agent-tasks create secret generic agent-tasks-worker-secret \
     --from-literal=GEMINI_API_KEY="AIza..."

   # 2. Controller secret
   kubectl -n agent-tasks create secret generic agent-tasks-controller-secret \
     --from-literal=GEMINI_API_KEY="AIza..." \
     --from-literal=PWA_API_TOKEN="$(openssl rand -base64 32 | tr '+/' '-_' | tr -d '=\n')" \
     --from-literal=TARGET_GITHUB_TOKEN="ghp_..." \
     --from-literal=SLACK_ENABLED="false"
   ```

2. **Deploy via Kustomize Overlay or Helm**:
   `deploy/k3s/` ships **generic placeholder defaults** (`example-org/example-repo`, `agy-cloud.example.com`, and optional background monitors disabled) so this repository remains reusable and free of private org or cluster config.

   For production/GitOps deployments, keep your real repositories, Sentry org slugs, ingress hostnames, and personal plugin settings in a **private Kustomize overlay** (for example, `deploy/k3s/` inside your private `agy-cloud-private-plugins` repository) that references this repo as a remote base:
   ```yaml
   # In your private repo: deploy/k3s/kustomization.yaml
   apiVersion: kustomize.config.k8s.io/v1beta1
   kind: Kustomization
   namespace: agent-tasks
   resources:
     - https://github.com/billthefighter/agy-cloud//deploy/k3s?ref=main
   patches:
     - path: configmap-targets.yaml
     - path: pwa-ingress.yaml
   ```
   Then point ArgoCD (with read credentials for your private repo) or `kubectl apply -k` at your private overlay instead of modifying `agy-cloud/deploy/k3s/` directly:
   ```bash
   # Apply your private Kustomize overlay (recommended):
   kubectl apply -k path/to/your-private-repo/deploy/k3s

   # Or deploy the generic base directly for quick testing:
   kubectl apply -k deploy/k3s

   # Or via Helm:
   helm upgrade --install agy-cloud ./charts/agy-cloud \
     --namespace agent-tasks \
     -f my-values.yaml
   ```

---

## 7. Laptop Client Setup & Using `/handoff`

### 1. Install the Client Skill in Antigravity
Copy or symlink [`skills/handoff`](skills/handoff/SKILL.md) into your global Antigravity skills directory (or install this repo as a plugin under `~/.gemini/config/plugins/agy-cloud`):

```bash
mkdir -p ~/.gemini/config/skills
cp -R skills/handoff ~/.gemini/config/skills/handoff
```

### 2. Configure Your Gateway URL & Token
Create `~/.config/agy-cloud/config.env` (or set `AGY_HANDOFF_URL` and `AGY_HANDOFF_TOKEN` in your shell / workspace `.env`):

```ini
AGY_HANDOFF_URL="https://your-k3s-host.netbird.cloud/agy"
AGY_HANDOFF_TOKEN="your-pwa-api-token"
```

### 3. Commands Inside Any Antigravity Session

| Slash Command | Behavior |
|---|---|
| `/handoff <instructions>` | **Default (Push + Poll-on-Wake)**: Pushes your current `base_sha`, uncommitted working-tree `git diff HEAD`, and Semantic State ($s_t$) to `POST /api/handoff`, and immediately starts a background sleep-resilient poller. Close your laptop and drive; when your laptop wakes and reconnects, it automatically pulls and applies the pod's `fix.patch` and wakes your local Antigravity conversation. |
| `/handoff` *(no args)* | If an unpulled handoff already exists for this workspace, attaches the sleep-resilient wake poller to it; otherwise hands off the current conversation task. |
| `/handoff push <instructions>` | **Fire-and-Forget Push**: Pushes the task and uncommitted diff to the k3s cluster and exits immediately without starting a local polling task. |
| `/handoff pull [<id>]` | **Immediate Pull**: Fetches the latest (or specified) handoff from k3s, backs up any local uncommitted changes to `.git/agy_handoff_pre_pull_backup.patch`, applies the remote `fix.patch` cleanly, and marks the task pulled. |
| `/handoff status [<id>]` | Displays remote turn status, diff stats, and `TriageReport` without modifying your local working tree. |

---

## 8. Loading External Plugin Repositories & ConfigMap Overrides

To keep personal or proprietary integrations (such as health/nutrition tracking, Garmin biometrics, or internal company tooling) separate from this public repository, `agy-cloud` can clone and load plugins directly from external Git repositories on startup.

In your cluster `ConfigMap` ([`config.example.yaml`](config.example.yaml)):

```yaml
# Optional supply-chain guard: require immutable commit SHAs or semver tags (v0.1.0)
require_pinned_refs: false
install_python_dependencies: true

# 1. External Plugin Git Repositories
plugin_sources:
  - repo: https://github.com/your-user/agy-cloud-private-plugins.git
    ref: main                  # branch, tag, or 40-char commit SHA
    subpath: ""                # supports single-plugin root OR multi-plugin repo (plugins/*)
    auth_env: TARGET_GITHUB_TOKEN  # passed via transient git http.extraheader (never written to disk)

# 2. Cluster ConfigMap Overrides (deep-merged over the plugin repo's bundled config.yaml)
plugins:
  garmin:
    active_factor: 0.85
    api_key: "${GARMIN_API_KEY:-}"
  health:
    timezone: "America/Los_Angeles"
    default_calories_kcal: 2200
```

### 4-Layer Configuration Precedence (Lowest → Highest)
1. **Plugin Pydantic Schema Defaults** (`config_model` in Python).
2. **Plugin Repository Bundled Defaults** (`default_config:` in `plugin.yaml`, `<plugin_dir>/config.yaml`, and `<repo_root>/config.yaml -> plugins.<id>`).
3. **Source-Inline Config** (`plugin_sources[].config.<id>`).
4. **Cluster `ConfigMap` & `Secret` Environment Variables** (`config.yaml -> plugins.<id>`, `AGY_PLUGIN_<ID>_<FIELD>`, and `${ENV_VAR:-default}` interpolation).

---

## 9. Authoring Custom Plugins

Scaffold a new Dual Server + Client plugin bundle in one command:

```bash
uv run agy-cloud plugins new strava
```

This generates:
- `plugins/strava/plugin.yaml` — Server manifest (`id`, `entrypoint`, `permissions`, `python_dependencies`, `channel_roles`, `pwa_views`, `default_config`).
- `plugins/strava/server.py` — `AgyCloudPlugin` subclass with `@command`, `@channel_listener`, `@button_action`, `@cron`, and FastAPI `APIRouter` hooks.
- `plugins/strava/plugin.json` & `plugins/strava/skills/strava/SKILL.md` — Client-side Antigravity skill exportable via `agy-cloud plugins install-skills`.

See **[`docs/PLUGIN_SPEC.md`](docs/PLUGIN_SPEC.md)** for the complete v1 Plugin SDK reference.

### Persistence Layers (Core vs. Plugins)

| Layer | Who Owns Schema & Connections? | Location on PVC (`/state`) | When to Use |
|---|---|---|---|
| **1. Core Platform State** | **Core** (`StateStore`, `HandoffStore`, `MemoryStore`) | `/state/state.sqlite3`, `/state/handoffs.sqlite3`, `/state/sessions/`, `/state/artifacts/` | Automatic for all `/handoff` tasks, K8s worker Jobs, Slack/PWA threads, Web Push subscriptions, timers, and billing usage. |
| **2. Scoped Key-Value Store (`ctx.kv`)** | **Core** (`PluginKeyValueStore`) | `/state/plugins/<plugin_id>/store.sqlite3` (`_agy_plugin_kv`) | Zero-SQL persistence (`ctx.kv.get(key)`, `ctx.kv.set(key, obj)`) for cursors, settings, and JSON documents. |
| **3. Scoped Relational SQLite (`ctx.db()`)** | **Core manages WAL, migrations, & `.sqlite3.gz` backups**; **Plugin defines SQL tables** | `/state/plugins/<plugin_id>/<name>.sqlite3` & `/state/backups/plugins/<plugin_id>/` | Domain tables (`ctx.db("nutrition.sqlite3").migrate({1: "CREATE TABLE ..."})`) with automatic `WAL` mode, `foreign_keys=ON`, `busy_timeout=30000`, and `POST /api/plugins/backup` online snapshots. |
