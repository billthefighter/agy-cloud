# `agy-cloud` — Microkernel Cloud Handoff & Plugin Framework for Google Antigravity

`agy-cloud` is an open, generic microkernel runtime and Dual Server+Client Plugin SDK for running background Google Antigravity worker pods on Kubernetes (k3s / K8s), handing off active laptop coding sessions while commuting (`/handoff`), and loading external plugin repositories (such as [`agy-cloud-private-plugins`](../agy-cloud-private-plugins)) via declarative Kubernetes `ConfigMap` configuration.

See **[`docs/PLUGIN_SPEC.md`](./docs/PLUGIN_SPEC.md)** for the complete v1 Plugin Specification and **[`config.example.yaml`](./config.example.yaml)** for Git plugin loading & ConfigMap overrides.

---

## Key Capabilities

1. **Laptop ↔ Kubernetes Session Handoff (`/handoff`) with Sleep-Resilient Poll-on-Wake**:
   - **`/handoff <instructions>`**: Pushes your active workspace's `base_sha`, uncommitted `git diff HEAD`, and Semantic State ($s_t$) to `POST /api/handoff`, and **immediately starts in sleep-resilient poll-on-wake mode**. Close your laptop and commute; when your laptop wakes and reconnects, the background poller automatically pulls and applies the pod's `fix.patch` to your working tree and wakes your local Antigravity conversation.
   - **`/handoff push <instructions>`**: Fire-and-forget handoff without starting the local poller.
   - **`/handoff pull [<id>]`**: Immediately pull and apply the remote pod's `fix.patch` (with automatic pre-pull backup to `.git/agy_handoff_pre_pull_backup.patch`).
   - **`/handoff watch [<id>]`**: Attach poll-on-wake mode to an already-running remote handoff.
   - **`/handoff status [<id>]`**: Check remote turn progress and `TriageReport`.

2. **Git Repository Plugin Loading + 4-Layer Config Precedence**:
   - Point `config.yaml` (`plugin_sources:`) at any Git repository (single-plugin or multi-plugin repo) holding `plugin.yaml` + bundled `config.yaml`.
   - Override any plugin's bundled configuration cleanly from your Kubernetes `ConfigMap` (`plugins.<id>`) or Kubernetes `Secret` environment variables (`${ENV_VAR}` interpolation and `AGY_PLUGIN_<ID>_<FIELD>`).

---

## Quickstart

```bash
uv venv && uv pip install -e ".[dev]"
uv run pytest
uv run ruff check .

# List discovered plugins (optionally passing --config config.yaml):
uv run agy-cloud plugins list --config config.example.yaml

# Scaffold a new Dual Server+Client plugin bundle:
uv run agy-cloud plugins new strava
```
