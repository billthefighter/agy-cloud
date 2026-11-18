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
