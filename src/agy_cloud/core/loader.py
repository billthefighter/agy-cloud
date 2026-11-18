from __future__ import annotations

import base64
import hashlib
import importlib
import importlib.metadata
import importlib.util
import inspect
import os
import re
import shutil
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import structlog
import yaml

from agy_cloud.sdk.spec import (
    ALL_PLUGIN_PERMISSIONS,
    AgyCloudConfig,
    AgyCloudPlugin,
    ButtonActionRegistration,
    ButtonEvent,
    ChannelBusProtocol,
    ChannelListenerRegistration,
    ChannelMessageEvent,
    CommandEvent,
    CommandRegistration,
    CronRegistration,
    PluginContext,
    PluginManifest,
    PluginRepoSource,
    ThreadReplyEvent,
    ThreadReplyRegistration,
    WorkerJobServiceProtocol,
)

log = structlog.get_logger(__name__)

ENTRYPOINT_GROUP = "agy_cloud.plugins"
_ENV_PLACEHOLDER_RE = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]*))?\}")
_PINNED_REF_RE = re.compile(r"^([0-9a-fA-F]{7,40}|v?\d+\.\d+\.\d+[-A-Za-z0-9.]*)$")


def is_pinned_git_ref(ref: str) -> bool:
    """Return True if `ref` is an immutable commit SHA or semver release tag (`v1.2.3`)."""
    return bool(_PINNED_REF_RE.fullmatch(ref.strip()))


@dataclass(slots=True)
class DiscoveredPluginSpec:
    manifest: PluginManifest
    cls: type[AgyCloudPlugin[Any]]
    source_dir: Path | None = None
    bundled_config: dict[str, Any] = field(default_factory=dict)
    allowed_permissions: set[str] | None = None
    install_python_deps: bool = True


@dataclass(slots=True)
class LoadedPlugin:
    manifest: PluginManifest
    plugin: AgyCloudPlugin[Any]
    context: PluginContext[Any]
    source_dir: Path | None = None
    commands: list[CommandRegistration] = field(default_factory=list)
    channel_listeners: list[ChannelListenerRegistration] = field(default_factory=list)
    thread_reply_handlers: list[ThreadReplyRegistration] = field(default_factory=list)
    button_actions: list[ButtonActionRegistration] = field(default_factory=list)
    crons: list[CronRegistration] = field(default_factory=list)


def deep_merge_dicts(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    """Recursively merge `override` on top of `base` without mutating inputs."""
    merged = dict(base)
    for k, v in override.items():
        if isinstance(v, dict) and isinstance(merged.get(k), dict):
            merged[k] = deep_merge_dicts(merged[k], v)
        else:
            merged[k] = v
    return merged


def expand_env_placeholders(value: Any, env: dict[str, str] | None = None) -> Any:
    """Expand `${VAR}` and `${VAR:-default}` placeholders in config values."""
    effective_env = os.environ if env is None else env
    if isinstance(value, str):

        def _repl(match: re.Match[str]) -> str:
            var_name = match.group(1)
            fallback = match.group(2) or ""
            return effective_env.get(var_name, fallback)

        return _ENV_PLACEHOLDER_RE.sub(_repl, value)
    if isinstance(value, dict):
        return {k: expand_env_placeholders(v, effective_env) for k, v in value.items()}
    if isinstance(value, list):
        return [expand_env_placeholders(item, effective_env) for item in value]
    return value


def extract_env_plugin_overrides(
    plugin_id: str, env: dict[str, str] | None = None
) -> dict[str, Any]:
    """Read `AGY_PLUGIN_<ID>_<FIELD>=value` environment variable overrides."""
    effective_env = os.environ if env is None else env
    prefix = f"AGY_PLUGIN_{plugin_id.upper().replace('-', '_')}_"
    overrides: dict[str, Any] = {}
    for k, raw_val in effective_env.items():
        if k.startswith(prefix):
            field_name = k[len(prefix) :].lower()
            if field_name:
                overrides[field_name] = yaml.safe_load(raw_val)
    return overrides


def collect_plugin_python_requirements(
    plugin_dir: Path | None, manifest: PluginManifest
) -> list[str]:
    """Collect PyPI package specs from `manifest.python_dependencies` and `<plugin_dir>/requirements.txt`."""
    reqs: list[str] = [d.strip() for d in manifest.python_dependencies if d.strip()]
    if plugin_dir is not None:
        req_file = plugin_dir / "requirements.txt"
        if req_file.is_file():
            for raw_line in req_file.read_text(encoding="utf-8").splitlines():
                line = raw_line.split("#", 1)[0].strip()
                if line and not line.startswith("-"):
                    reqs.append(line)
    return reqs


def install_plugin_python_dependencies(
    requirements: list[str],
    target_site_packages: Path,
    *,
    installer_runner: Any | None = None,
) -> list[str]:
    """Bootstrap per-plugin Python dependencies into `target_site_packages` and prepend to `sys.path`."""
    if not requirements:
        return []
    target_site_packages.mkdir(parents=True, exist_ok=True)
    site_str = str(target_site_packages.resolve())
    if site_str not in sys.path:
        sys.path.insert(0, site_str)

    uv_bin = shutil.which("uv")
    if uv_bin:
        cmd = [
            uv_bin,
            "pip",
            "install",
            "--python",
            sys.executable,
            "--target",
            site_str,
            *requirements,
        ]
    else:
        cmd = [
            sys.executable,
            "-m",
            "pip",
            "install",
            "--target",
            site_str,
            *requirements,
        ]

    runner = installer_runner or subprocess.run
    log.info("plugin_deps.installing", requirements=requirements, target=site_str)
    runner(cmd, check=True, capture_output=True, text=True)
    return requirements


def sync_plugin_repo(
    source: PluginRepoSource,
    repos_cache_dir: Path,
    *,
    require_pinned_ref: bool = False,
) -> Path:
    """Clone or update a plugin Git repository without persisting tokens into `.git/config`."""
    if require_pinned_ref and not is_pinned_git_ref(source.ref):
        raise ValueError(
            f"Plugin repo `{source.repo}` uses unpinned ref `{source.ref}`, "
            "but `require_pinned_refs=True` requires a commit SHA or semver tag (e.g. `v0.1.0`)."
        )

    repos_cache_dir.mkdir(parents=True, exist_ok=True)
    digest = hashlib.sha1(source.repo.encode("utf-8")).hexdigest()[:10]
    safe_name = re.sub(r"[^A-Za-z0-9_.-]+", "_", Path(source.repo).stem) or "plugin_repo"
    checkout_dir = repos_cache_dir / f"{safe_name}-{digest}"

    git_prefix = ["git"]
    if source.auth_env:
        token = os.environ.get(source.auth_env, "").strip()
        if token:
            encoded = base64.b64encode(f"x-access-token:{token}".encode()).decode("ascii")
            git_prefix.extend(["-c", f"http.extraheader=AUTHORIZATION: basic {encoded}"])

    if not (checkout_dir / ".git").exists():
        log.info("plugin_repo.cloning", repo=source.repo, ref=source.ref, dest=str(checkout_dir))
        subprocess.run(
            [*git_prefix, "clone", source.repo, str(checkout_dir)],
            check=True,
            capture_output=True,
            text=True,
        )
    else:
        subprocess.run(
            [*git_prefix, "-C", str(checkout_dir), "fetch", "--all", "--tags", "--prune"],
            check=True,
            capture_output=True,
            text=True,
        )

    checkout_proc = subprocess.run(
        ["git", "-C", str(checkout_dir), "checkout", source.ref],
        check=False,
        capture_output=True,
        text=True,
    )
    if checkout_proc.returncode != 0:
        subprocess.run(
            ["git", "-C", str(checkout_dir), "checkout", f"origin/{source.ref}"],
            check=True,
            capture_output=True,
            text=True,
        )
    return checkout_dir


def _import_plugin_class_from_dir(plugin_dir: Path, entrypoint: str) -> type[AgyCloudPlugin[Any]]:
    """Import `<module_or_relpath>:<ClassName>` from a local plugin directory."""
    if ":" not in entrypoint:
        raise ValueError(
            f"Invalid plugin entrypoint `{entrypoint}` in `{plugin_dir}` "
            "(expected `<module>:<ClassName>`)."
        )
    mod_part, _, cls_name = entrypoint.partition(":")
    rel_parts = mod_part.replace(".", "/")
    candidates = [
        plugin_dir / f"{rel_parts}.py",
        plugin_dir / rel_parts / "__init__.py",
    ]
    target_file = next((c for c in candidates if c.is_file()), None)
    if target_file is None:
        module = importlib.import_module(mod_part)
    else:
        digest = hashlib.sha1(str(target_file).encode("utf-8")).hexdigest()[:8]
        synth_name = f"agy_ext_plugin_{plugin_dir.name}_{mod_part.replace('.', '_')}_{digest}"
        spec = importlib.util.spec_from_file_location(
            synth_name,
            target_file,
            submodule_search_locations=[str(target_file.parent)],
        )
        if spec is None or spec.loader is None:
            raise ImportError(f"Could not create module spec for `{target_file}`.")
        module = importlib.util.module_from_spec(spec)
        sys.modules[synth_name] = module
        added_sys_path = False
        if str(plugin_dir) not in sys.path:
            sys.path.insert(0, str(plugin_dir))
            added_sys_path = True
        try:
            spec.loader.exec_module(module)
        finally:
            if added_sys_path and str(plugin_dir) in sys.path:
                sys.path.remove(str(plugin_dir))

    cls = getattr(module, cls_name, None)
    if not isinstance(cls, type) or not issubclass(cls, AgyCloudPlugin):
        raise TypeError(f"`{entrypoint}` in `{plugin_dir}` is not an `AgyCloudPlugin` subclass.")
    return cls


def _topological_sort_plugins(
    discovered: dict[str, DiscoveredPluginSpec],
) -> list[DiscoveredPluginSpec]:
    """Sort plugins so required and present optional dependencies always load first."""
    visited: dict[str, str] = {}
    ordered: list[DiscoveredPluginSpec] = []

    def visit(pid: str, stack: list[str]) -> None:
        state = visited.get(pid)
        if state == "done":
            return
        if state == "visiting":
            cycle = " -> ".join([*stack, pid])
            raise ValueError(f"Circular plugin dependency detected: {cycle}")

        visited[pid] = "visiting"
        spec = discovered[pid]
        for req_dep in spec.manifest.dependencies:
            if req_dep not in discovered:
                raise ValueError(f"Plugin `{pid}` requires missing dependency plugin `{req_dep}`.")
            visit(req_dep, [*stack, pid])
        for opt_dep in spec.manifest.optional_dependencies:
            if opt_dep in discovered:
                visit(opt_dep, [*stack, pid])

        visited[pid] = "done"
        ordered.append(spec)

    for pid in sorted(discovered):
        visit(pid, [])
    return ordered


def _load_optional_yaml_dict(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {}
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    return raw if isinstance(raw, dict) else {}


class PluginRegistry:
    """Central microkernel registry discovering, wiring, and dispatching plugins."""

    def __init__(
        self,
        *,
        state_root: Path,
        channels: ChannelBusProtocol,
        jobs: WorkerJobServiceProtocol,
        pip_installer_runner: Any | None = None,
    ) -> None:
        self.state_root = state_root
        self.channels = channels
        self.jobs = jobs
        self._pip_installer_runner = pip_installer_runner
        self.services: dict[str, Any] = {}
        self.plugins: dict[str, LoadedPlugin] = {}
        self.installed_python_packages: dict[str, list[str]] = {}

        self._commands: dict[str, tuple[LoadedPlugin, CommandRegistration]] = {}
        self._channel_listeners: dict[
            str, list[tuple[LoadedPlugin, ChannelListenerRegistration]]
        ] = {}
        self._thread_replies: dict[str, tuple[LoadedPlugin, ThreadReplyRegistration]] = {}
        self._button_actions: list[tuple[str, LoadedPlugin, ButtonActionRegistration]] = []

    @property
    def plugin_site_packages_dir(self) -> Path:
        return self.state_root / "plugin_site_packages"

    def _load_single_plugin_dir(
        self,
        plugin_dir: Path,
        *,
        repo_plugins_cfg: dict[str, dict[str, Any]] | None = None,
        source_overrides: dict[str, dict[str, Any]] | None = None,
        allowed_permissions: set[str] | None = None,
        install_python_deps: bool = True,
    ) -> DiscoveredPluginSpec:
        manifest_file = plugin_dir / "plugin.yaml"
        raw = _load_optional_yaml_dict(manifest_file)
        manifest = PluginManifest.model_validate(raw)

        if install_python_deps:
            reqs = collect_plugin_python_requirements(plugin_dir, manifest)
            if reqs:
                installed = install_plugin_python_dependencies(
                    reqs,
                    self.plugin_site_packages_dir,
                    installer_runner=self._pip_installer_runner,
                )
                self.installed_python_packages[manifest.id] = installed

        cls = _import_plugin_class_from_dir(plugin_dir, manifest.entrypoint)

        bundled: dict[str, Any] = dict(manifest.default_config)
        local_cfg = _load_optional_yaml_dict(plugin_dir / "config.yaml")
        if local_cfg:
            bundled = deep_merge_dicts(bundled, local_cfg)
        if repo_plugins_cfg and isinstance(repo_plugins_cfg.get(manifest.id), dict):
            bundled = deep_merge_dicts(bundled, repo_plugins_cfg[manifest.id])
        if source_overrides and isinstance(source_overrides.get(manifest.id), dict):
            bundled = deep_merge_dicts(bundled, source_overrides[manifest.id])

        return DiscoveredPluginSpec(
            manifest=manifest,
            cls=cls,
            source_dir=plugin_dir,
            bundled_config=bundled,
            allowed_permissions=allowed_permissions,
            install_python_deps=install_python_deps,
        )

    def discover_directory(
        self,
        root_dir: Path,
        *,
        source_overrides: dict[str, dict[str, Any]] | None = None,
        allowed_permissions: set[str] | None = None,
        install_python_deps: bool = True,
    ) -> dict[str, DiscoveredPluginSpec]:
        """Discover either a single-plugin root (`plugin.yaml`) or a multi-plugin tree."""
        found: dict[str, DiscoveredPluginSpec] = {}
        if not root_dir.is_dir():
            return found

        repo_cfg = _load_optional_yaml_dict(root_dir / "config.yaml")
        repo_plugins_cfg: dict[str, dict[str, Any]] = {}
        if isinstance(repo_cfg.get("plugins"), dict):
            repo_plugins_cfg = repo_cfg["plugins"]

        if (root_dir / "plugin.yaml").is_file():
            spec = self._load_single_plugin_dir(
                root_dir,
                repo_plugins_cfg=repo_plugins_cfg,
                source_overrides=source_overrides,
                allowed_permissions=allowed_permissions,
                install_python_deps=install_python_deps,
            )
            found[spec.manifest.id] = spec
            return found

        search_roots = [root_dir]
        if (root_dir / "plugins").is_dir():
            search_roots.append(root_dir / "plugins")

        for sroot in search_roots:
            for child in sorted(sroot.iterdir()):
                if child.is_dir() and (child / "plugin.yaml").is_file():
                    spec = self._load_single_plugin_dir(
                        child,
                        repo_plugins_cfg=repo_plugins_cfg,
                        source_overrides=source_overrides,
                        allowed_permissions=allowed_permissions,
                        install_python_deps=install_python_deps,
                    )
                    found[spec.manifest.id] = spec
        return found

    def discover_git_sources(
        self,
        sources: list[PluginRepoSource],
        *,
        repos_cache_dir: Path | None = None,
        require_pinned_refs: bool = False,
        install_python_deps: bool = True,
    ) -> dict[str, DiscoveredPluginSpec]:
        """Clone/sync external plugin Git repositories and discover their plugins + bundled config."""
        cache_root = repos_cache_dir or (self.state_root / "plugin_repos")
        found: dict[str, DiscoveredPluginSpec] = {}
        for src in sources:
            checkout_dir = sync_plugin_repo(
                src,
                cache_root,
                require_pinned_ref=require_pinned_refs,
            )
            target_dir = (checkout_dir / src.subpath).resolve() if src.subpath else checkout_dir
            if not target_dir.is_relative_to(checkout_dir.resolve()):
                raise ValueError(f"Invalid plugin repo subpath `{src.subpath}`.")
            src_allowed = (
                set(src.allowed_permissions) if src.allowed_permissions is not None else None
            )
            discovered = self.discover_directory(
                target_dir,
                source_overrides=src.config,
                allowed_permissions=src_allowed,
                install_python_deps=install_python_deps and src.install_python_deps,
            )
            found.update(discovered)
        return found

    def discover_entrypoints(
        self,
        *,
        only_ids: set[str] | None = None,
    ) -> dict[str, DiscoveredPluginSpec]:
        found: dict[str, DiscoveredPluginSpec] = {}
        eps = importlib.metadata.entry_points(group=ENTRYPOINT_GROUP)
        for ep in eps:
            if only_ids is not None and ep.name not in only_ids:
                continue
            cls = ep.load()
            if not isinstance(cls, type) or not issubclass(cls, AgyCloudPlugin):
                continue
            manifest = cls.default_manifest or PluginManifest(
                id=ep.name,
                name=ep.name,
                entrypoint=f"{ep.module}:{ep.attr}",
            )
            mod_file = inspect.getfile(cls)
            src_dir = Path(mod_file).resolve().parent
            found[manifest.id] = DiscoveredPluginSpec(
                manifest=manifest,
                cls=cls,
                source_dir=src_dir,
                bundled_config=dict(manifest.default_config),
            )
        return found

    async def load_from_config_file(
        self,
        config_path: Path,
        *,
        plugins_dir: Path | None = None,
        include_entrypoints: bool = True,
    ) -> list[LoadedPlugin]:
        """Load plugins and ConfigMap overrides from a cluster `config.yaml` file."""
        raw = _load_optional_yaml_dict(config_path)
        cluster_cfg = AgyCloudConfig.model_validate(raw)
        return await self.load_all(
            plugins_dir=plugins_dir,
            plugin_sources=cluster_cfg.plugin_sources,
            include_entrypoints=include_entrypoints,
            plugin_configs=cluster_cfg.plugins,
            plugin_permissions=cluster_cfg.plugin_permissions,
            require_pinned_refs=cluster_cfg.require_pinned_refs,
            install_python_deps=cluster_cfg.install_python_dependencies,
        )

    async def load_all(
        self,
        *,
        plugins_dir: Path | None = None,
        plugin_sources: list[PluginRepoSource] | None = None,
        include_entrypoints: bool = True,
        entrypoint_filter: set[str] | None = None,
        plugin_configs: dict[str, dict[str, Any]] | None = None,
        plugin_permissions: dict[str, list[str]] | None = None,
        require_pinned_refs: bool = False,
        install_python_deps: bool = True,
    ) -> list[LoadedPlugin]:
        discovered: dict[str, DiscoveredPluginSpec] = {}
        if include_entrypoints:
            discovered.update(self.discover_entrypoints(only_ids=entrypoint_filter))
        if plugin_sources:
            discovered.update(
                self.discover_git_sources(
                    plugin_sources,
                    require_pinned_refs=require_pinned_refs,
                    install_python_deps=install_python_deps,
                )
            )
        if plugins_dir is not None:
            discovered.update(
                self.discover_directory(
                    plugins_dir,
                    install_python_deps=install_python_deps,
                )
            )

        ordered = _topological_sort_plugins(discovered)
        cluster_cfgs = plugin_configs or {}
        cluster_perms = plugin_permissions or {}
        loaded_list: list[LoadedPlugin] = []

        for spec in ordered:
            manifest = spec.manifest
            cls = spec.cls
            src_dir = spec.source_dir

            merged_cfg = dict(spec.bundled_config)
            if isinstance(cluster_cfgs.get(manifest.id), dict):
                merged_cfg = deep_merge_dicts(merged_cfg, cluster_cfgs[manifest.id])
            env_overrides = extract_env_plugin_overrides(manifest.id)
            if env_overrides:
                merged_cfg = deep_merge_dicts(merged_cfg, env_overrides)
            resolved_cfg = expand_env_placeholders(merged_cfg)

            cfg_obj = cls.config_model.model_validate(resolved_cfg)
            if hasattr(cfg_obj, "enabled") and not bool(cfg_obj.enabled):
                log.info("plugin.disabled_by_config", plugin_id=manifest.id)
                continue

            # Compute effective granted permissions (manifest ∩ source cap ∩ cluster ConfigMap cap)
            granted = set(manifest.permissions) & set(ALL_PLUGIN_PERMISSIONS)
            if spec.allowed_permissions is not None:
                granted &= spec.allowed_permissions
            if manifest.id in cluster_perms:
                granted &= set(cluster_perms[manifest.id])

            plugin_instance = cls(manifest=manifest)
            ctx: PluginContext[Any] = PluginContext(
                plugin_id=manifest.id,
                manifest=manifest,
                config=cfg_obj,
                state_root=self.state_root,
                plugin_source_dir=src_dir,
                services=self.services,
                channels=self.channels,
                jobs=self.jobs,
                granted_permissions=granted,
            )
            await plugin_instance.on_load(ctx)
            loaded = self._index_plugin(manifest, plugin_instance, ctx, src_dir)
            self.plugins[manifest.id] = loaded
            loaded_list.append(loaded)
            log.info(
                "plugin.loaded",
                plugin_id=manifest.id,
                version=manifest.version,
                permissions=sorted(granted),
                commands=[c.name for c in loaded.commands],
                roles=[r.role for r in manifest.channel_roles],
                pwa_views=[v.mount_path for v in manifest.pwa_views],
            )
        return loaded_list

    def _index_plugin(
        self,
        manifest: PluginManifest,
        plugin: AgyCloudPlugin[Any],
        ctx: PluginContext[Any],
        src_dir: Path | None,
    ) -> LoadedPlugin:
        loaded = LoadedPlugin(
            manifest=manifest,
            plugin=plugin,
            context=ctx,
            source_dir=src_dir,
        )
        for _, member in inspect.getmembers(plugin, predicate=callable):
            cmd_reg: CommandRegistration | None = getattr(member, "_agy_command", None)
            if cmd_reg is not None:
                loaded.commands.append(cmd_reg)
                for trigger in [cmd_reg.name, *cmd_reg.aliases]:
                    self._commands[trigger] = (loaded, cmd_reg)

            chan_reg: ChannelListenerRegistration | None = getattr(
                member, "_agy_channel_listener", None
            )
            if chan_reg is not None:
                loaded.channel_listeners.append(chan_reg)
                self._channel_listeners.setdefault(chan_reg.role, []).append((loaded, chan_reg))

            reply_reg: ThreadReplyRegistration | None = getattr(member, "_agy_thread_reply", None)
            if reply_reg is not None:
                loaded.thread_reply_handlers.append(reply_reg)
                self._thread_replies[reply_reg.kind] = (loaded, reply_reg)

            btn_reg: ButtonActionRegistration | None = getattr(member, "_agy_button_action", None)
            if btn_reg is not None:
                loaded.button_actions.append(btn_reg)
                self._button_actions.append((btn_reg.prefix, loaded, btn_reg))

            cron_reg: CronRegistration | None = getattr(member, "_agy_cron", None)
            if cron_reg is not None:
                loaded.crons.append(cron_reg)

        self._button_actions.sort(key=lambda item: len(item[0]), reverse=True)
        return loaded

    async def start_all(self) -> None:
        for loaded in self.plugins.values():
            await loaded.plugin.on_start()

    async def stop_all(self) -> None:
        for loaded in reversed(list(self.plugins.values())):
            await loaded.plugin.on_stop()

    async def dispatch_command(self, event: CommandEvent) -> str | None:
        trigger = event.command.lstrip("!").lower()
        if trigger == "help":
            return self.build_help_markdown()
        match = self._commands.get(trigger)
        if match is None:
            return None
        loaded, reg = match
        method = getattr(loaded.plugin, reg.method_name)
        return await method(event)

    async def dispatch_channel_message(self, event: ChannelMessageEvent) -> str | None:
        listeners = self._channel_listeners.get(event.role.lower(), [])
        for loaded, reg in listeners:
            method = getattr(loaded.plugin, reg.method_name)
            reply = await method(event)
            if reply is not None:
                return reply
        return None

    async def dispatch_thread_reply(self, event: ThreadReplyEvent) -> str | None:
        match = self._thread_replies.get(event.kind.lower())
        if match is None:
            return None
        loaded, reg = match
        method = getattr(loaded.plugin, reg.method_name)
        return await method(event)

    async def dispatch_button_action(self, event: ButtonEvent) -> str | None:
        for prefix, loaded, reg in self._button_actions:
            if event.action_id == prefix or event.action_id.startswith(f"{prefix}:"):
                method = getattr(loaded.plugin, reg.method_name)
                return await method(event)
        return None

    def build_help_markdown(self) -> str:
        lines = ["*`agy-cloud` Loaded Plugins & Commands*"]
        for pid, loaded in self.plugins.items():
            lines.append(f"\n*{loaded.manifest.name}* (`{pid}` v{loaded.manifest.version})")
            for cmd in loaded.commands:
                alias_note = (
                    f" (aliases: {', '.join(f'`!{a}`' for a in cmd.aliases)})"
                    if cmd.aliases
                    else ""
                )
                lines.append(f"• `!{cmd.name}`{alias_note} — {cmd.help_text or 'Run command'}")
        return "\n".join(lines)

    def export_client_skills(self, target_skills_dir: Path) -> list[Path]:
        """Copy or link client-side Antigravity `SKILL.md` folders from loaded plugins."""
        target_skills_dir.mkdir(parents=True, exist_ok=True)
        exported: list[Path] = []
        for loaded in self.plugins.values():
            if loaded.source_dir is None:
                continue
            skills_root = loaded.source_dir / loaded.manifest.skills_dir
            if not skills_root.is_dir():
                continue
            for skill_dir in sorted(skills_root.iterdir()):
                if skill_dir.is_dir() and (skill_dir / "SKILL.md").is_file():
                    dest = target_skills_dir / skill_dir.name
                    if dest.exists():
                        shutil.rmtree(dest)
                    shutil.copytree(skill_dir, dest)
                    exported.append(dest)
        return exported
