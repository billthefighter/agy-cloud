# `agy-cloud` Plugin Specification (v1)

`agy-cloud` separates the **Microkernel Core Runtime** (Kubernetes worker pod orchestration, `/handoff` laptop-to-cloud continuity, unified Slack/PWA/Web Push channel bus, billing guard, and persistent memory) from **Domain & Automation Plugins** (such as `health`, `garmin`, `sentry-triage`, `ci-healer`, `k8s-watchdog`, or custom third-party workflows).

A single `agy-cloud` plugin repository can ship **both**:
1. **Server-side capabilities** running inside the k3s controller & ephemeral worker pods (chat commands, channel listeners, interactive button actions, cron loops, FastAPI routes, isolated SQLite databases, K8s `google-antigravity` worker Job templates, and pluggable mobile PWA views) **plus its own default configuration (`config.yaml` / `default_config`)**.
2. **Client-side Antigravity customizations** (`plugin.json` + `skills/<name>/SKILL.md`) that install directly into any user's local Antigravity IDE/CLI (`~/.gemini/config/plugins/` or `.agent/plugins/`).

---

## 1. Architecture Overview

```text
┌──────────────────────────────────────────────────────────────────────────────┐
│                        agy-cloud Microkernel Core                            │
│                                                                              │
│  ┌─────────────────────┐  ┌──────────────────────┐  ┌─────────────────────┐  │
│  │ Cloud Handoff Engine│  │ Unified Channel Bus  │  │ K8s Worker Job Svc  │  │
│  │  • POST /api/handoff│  │  • Slack Socket Mode │  │  • Bare Git Mirrors │  │
│  │  • Poll-on-Wake API │  │  • Mobile PWA + WS   │  │  • Patch Application│  │
│  │  • Patch & Turn Mgmt│  │  • VAPID Web Push    │  │  • Sandboxed Pods   │  │
│  └──────────▲──────────┘  └──────────▲───────────┘  └──────────▲──────────┘  │
│             │                        │                         │             │
│  ┌──────────┴────────────────────────┴─────────────────────────┴──────────┐  │
│  │                    PluginRegistry & PluginContext                      │  │
│  │  • Clones External Plugin Git Repos (`plugin_sources:` in ConfigMap)   │  │
│  │  • 4-Layer Config Merge (Plugin Repo -> ConfigMap -> Secret Env Vars)  │  │
│  │  • Scoped PVC Storage (/state/plugins/<id>/{db.sqlite3,media,art})     │  │
│  │  • Inter-Plugin Service Bus (ctx.register_service / ctx.get_service)   │  │
│  │  • Scoped FastAPI Routers (/api/plugins/<id>/*) & PWA View Mounts      │  │
│  └──────────▲────────────────────────▲─────────────────────────▲──────────┘  │
└─────────────┼────────────────────────┼─────────────────────────┼─────────────┘
              │                        │                         │
   ┌──────────┴──────────┐  ┌──────────┴──────────┐   ┌──────────┴──────────┐
   │  External Git Repo: │  │  External Git Repo: │   │ Local/Submodule:    │
   │  Plugin `garmin`    │◄─┤   Plugin `health`   │   │ Plugin `repo_healer`│
   │  • Service: garmin  │  │  • Uses `garmin` svc│   │  • !code command    │
   │  • !garmin command  │  │  • !eat / #agy-food │   │  • Spawns K8s agent │
   │  • /api/plugins/    │  │  • Standalone PWA   │   │    worker jobs      │
   │    garmin/*         │  │    mount (/health/) │   │                     │
   └─────────────────────┘  └─────────────────────┘   └─────────────────────┘
```

---

## 2. Plugin Repository Layouts (Single-Plugin or Multi-Plugin Repo)

A plugin repository is **self-contained** (code + manifest + default configuration + client skills) and can hold either a **single plugin** at its root or **multiple related plugins** under `plugins/<id>/`:

### Option A: Multi-Plugin Repository (e.g. `agy-plugins-health`)
```text
agy-plugins-health/
├── config.yaml                 # Repo-level bundled config defaults (`plugins: {garmin: ..., health: ...}`)
└── plugins/
    ├── garmin/
    │   ├── plugin.yaml         # Manifest + default_config
    │   ├── plugin.json         # Client-side Antigravity plugin manifest
    │   ├── server.py           # Exports GarminPlugin(AgyCloudPlugin)
    │   └── skills/garmin/SKILL.md
    └── health/
        ├── plugin.yaml         # Manifest (optional_dependencies: [garmin]) + default_config
        ├── plugin.json
        ├── server.py           # Exports HealthPlugin(AgyCloudPlugin)
        ├── static/health_view.js
        └── skills/health/SKILL.md
```

### Option B: Single-Plugin Repository
```text
agy-plugin-strava/
├── plugin.yaml                 # Server-side agy-cloud plugin manifest + default_config
├── config.yaml                 # Optional standalone default config file
├── plugin.json                 # Client-side Antigravity IDE/CLI plugin manifest
├── server.py                   # Exports StravaPlugin(AgyCloudPlugin)
└── skills/strava/SKILL.md
```

---

## 3. Loading Plugins from Git Repositories & 4-Layer Configuration Override

In your Kubernetes `ConfigMap` (`config.yaml`), declare external plugin repositories under `plugin_sources` and cluster-specific overrides under `plugins`:

```yaml
plugin_sources:
  - repo: https://github.com/your-org/agy-plugins-health.git
    ref: main                  # branch, tag, or pinned commit SHA
    subpath: ""                # optional subdirectory inside repo
    auth_env: GITHUB_TOKEN     # optional env var for private git repos (never written to .git/config)

# Cluster ConfigMap overrides (deep-merged on top of the plugin repo's bundled config):
plugins:
  garmin:
    active_factor: 0.85
    api_key: "${GARMIN_API_KEY:-}"
  health:
    timezone: "America/Los_Angeles"
    default_calories_kcal: 2350
```

### 4-Layer Configuration Precedence (Lowest → Highest)

When `PluginRegistry` loads a plugin, its configuration dictionary is deep-merged across 4 layers before validating against `plugin.config_model`:

1. **Layer 1 — Plugin Schema Defaults**: Pydantic field defaults on `config_model`.
2. **Layer 2 — Plugin Repo Bundled Config**: `default_config:` inside `plugin.yaml`, `<plugin_dir>/config.yaml`, and `<repo_root>/config.yaml -> plugins.<id>`.
3. **Layer 3 — Source-Inline Config**: `plugin_sources[].config.<id>`.
4. **Layer 4 — Cluster `ConfigMap` & Secret Environment Variables**:
   - `config.yaml -> plugins.<id>` (from your Kubernetes `ConfigMap`)
   - `AGY_PLUGIN_<ID>_<FIELD>=<value>` environment variables (from your Kubernetes `Secret`)
   - `${ENV_VAR:-default}` string interpolation across all config values.

---

## 4. Python SDK Interface (`agy_cloud.sdk`)

```python
from fastapi import APIRouter
from pydantic import BaseModel
from agy_cloud.sdk import (
    AgyCloudPlugin,
    ButtonEvent,
    ChannelMessageEvent,
    CommandEvent,
    PluginContext,
    ThreadReplyEvent,
    button_action,
    channel_listener,
    command,
    cron,
    thread_reply_handler,
)


class HealthConfig(BaseModel):
    enabled: bool = True
    timezone: str = "UTC"
    default_calories_kcal: int = 2200
    default_protein_g: float = 180.0


class HealthPlugin(AgyCloudPlugin[HealthConfig]):
    config_model = HealthConfig

    async def on_load(self, ctx: PluginContext[HealthConfig]) -> None:
        await super().on_load(ctx)
        self.db_path = ctx.sqlite_path("nutrition.sqlite3")
        self.garmin = ctx.get_service("garmin")

    @command("eat", aliases=["meal"], role="food", help="Log a meal from text or photo")
    async def handle_eat(self, event: CommandEvent) -> str:
        return f"Logged meal for {event.user}: {event.args}"

    @channel_listener(role="food")
    async def handle_food_channel_post(self, event: ChannelMessageEvent) -> str | None: ...

    @button_action("health:scale")
    async def handle_scale_button(self, event: ButtonEvent) -> str | None: ...

    @thread_reply_handler(kind="food")
    async def handle_revision_reply(self, event: ThreadReplyEvent) -> str | None: ...

    @cron("30 20 * * *")
    async def nightly_digest(self) -> None: ...

    def get_api_router(self) -> APIRouter:
        router = APIRouter()

        @router.get("/today")
        async def get_today() -> dict:
            return {"calories_target": self.ctx.config.default_calories_kcal}

        return router
```

---

## 5. Persistence Architecture: Core State vs. Plugin Persistence (`ctx.kv` & `ctx.db`)

Persistence in `agy-cloud` is organized into **three tiers** so plugin authors never have to write low-level SQLite WAL/connection/backup boilerplate unless they want custom relational queries:

### Tier 1 — Core Platform Persistence (Automatic, Zero Plugin Code)
The microkernel Core owns and persists all cross-cutting platform state on `/state`:
- **Handoffs & Worker Jobs** (`/state/handoffs.sqlite3`, `/state/artifacts/<id>/`, `/state/sessions/<id>/`): Every `/handoff` session, `JobSpec`, `TriageReport`, cumulative `fix.patch`, and `google-antigravity` SDK `save_dir` conversation trajectory.
- **Threads, Messages, & Mobile PWA State** (`/state/state.sqlite3`): Every Slack/PWA thread, message, Web Push subscription, channel binding, follow-up timer, and monthly AI credit ledger entry.
- **Persistent Agent Memory** (`/state/memory/`): Standing instructions and repository notes.

### Tier 2 — Core-Managed Zero-SQL Key-Value Store (`ctx.kv`)
For plugins that need to persist cursors, timestamps, OAuth tokens, or JSON documents without designing a relational schema, `ctx.kv` provides a zero-SQL JSON store backed by a Core-managed WAL SQLite table in `/state/plugins/<plugin_id>/store.sqlite3`:

```python
# Save arbitrary JSON-serializable state
self.ctx.kv.set("last_sync", {"cursor": "abc-123", "count": 42})

# Read with default fallback
state = self.ctx.kv.get("last_sync", default={"cursor": None})

# Prefix listing & bulk retrieval
keys = self.ctx.kv.list_keys(prefix="user:")
```

### Tier 3 — Core-Managed Relational SQLite (`ctx.db()` & `ctx.sqlite_path()`)
When a plugin needs relational SQLite tables (such as `health` or `garmin`), `ctx.db(filename="store.sqlite3")` returns a Core-managed `PluginDatabase` pre-configured with `PRAGMA journal_mode=WAL`, `PRAGMA synchronous=NORMAL`, `PRAGMA foreign_keys=ON`, `PRAGMA busy_timeout=30000`, `sqlite3.Row`, idempotent schema migrations, and automatic inclusion in `POST /api/plugins/backup` (`.sqlite3.gz` online snapshots):

```python
db = self.ctx.db("nutrition.sqlite3")

# 1. Idempotent versioned schema migrations (tracked in `_agy_schema_migrations`)
db.migrate(
    {
        1: """
        CREATE TABLE IF NOT EXISTS meals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            calories_kcal INTEGER NOT NULL
        );
    """,
        2: "ALTER TABLE meals ADD COLUMN notes TEXT NOT NULL DEFAULT '';",
    }
)

# 2. Parameterized queries with automatic WAL transaction handling
db.execute("INSERT INTO meals (title, calories_kcal) VALUES (?, ?)", ("Salmon Bowl", 680))
rows = db.fetchall("SELECT * FROM meals WHERE calories_kcal > ?", (400,))

# 3. WAL-safe online compressed backup (`/state/backups/plugins/<id>/nutrition-<ts>.sqlite3.gz`)
snapshot_path = db.backup_compressed(retain=14)
```
*(Advanced plugins with custom store classes can still call `self.ctx.sqlite_path("nutrition.sqlite3")`, which registers the file with the Core's online backup service while returning the raw `Path`.)*

---

## 6. Lightweight Web UI & Manifest Contract (`GET /api/ui/manifest`)

The built-in mobile PWA shell (`src/agent_tasks/pwa/static/`) is intentionally domain-agnostic and zero-build (vanilla ES modules). It adapts dynamically to whatever plugins are loaded by fetching `GET /api/ui/manifest` after authentication.

Plugins opt into UI surfaces purely through `plugin.yaml`:

```yaml
push_categories:
  - id: food
    label: Nutrition & Check-ins
    default_enabled: true

thread_kinds:
  - key: food
    label: Food
    show_in_inbox: false   # false => excluded from the main Chat Inbox automatically

chat_context_providers:
  - id: health
    label: Include 7-day Garmin, Sleep, HRV & Nutrition context
    default_enabled: false

pwa_views:
  - id: health
    title: AGY Health
    short_name: Health
    mount_path: /health
    icon: "🍽️"
    nav_placement: topbar  # "topbar" | "tab" | "none"
    script_path: static/health_view.js
    standalone_pwa: true   # mounts its own /health/ shell & manifest.webmanifest
```

### Custom View ES Module Contract (`export function init(core)`)

When a plugin declares `pwa_views` with `script_path: static/my_view.js` and holds the `pwa_views` permission:
1. The gateway serves the plugin's `static/` directory at `/api/plugins/<plugin_id>/static/<asset>`.
2. For `standalone_pwa: true`, the gateway mounts `<mount_path>/` with `<meta name="agy-view-script" content="./api/plugins/<plugin_id>/static/my_view.js" />`.
3. `app.js` dynamically `import()`s the module and invokes `mod.init(core)`, passing `core` (`{ state, appMode, chatBase, apiFetch, registerAppMode, render, toast, cycleModel, modelShortLabel, escapeHtml, renderMarkdown }`) so plugin views need zero relative imports back to core JS files.

### Chat Context Providers (`build_chat_context`)

Plugins declaring `chat_context_providers` automatically get checkboxes in the `+ New Chat` modal (and `#new_chat=1&context=<provider_id>` deep links). When checked, the gateway calls `await plugin.build_chat_context(ctx, provider_id)` and prepends the returned string to the user's prompt.


