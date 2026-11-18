from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol

from fastapi import APIRouter
from pydantic import BaseModel, Field

ALL_PLUGIN_PERMISSIONS: frozenset[str] = frozenset(
    {
        "sqlite",
        "media",
        "channel_bus",
        "k8s_jobs",
        "services",
        "api_router",
        "pwa_views",
    }
)


class PluginPermissionError(PermissionError):
    """Raised when a plugin attempts to use a capability not granted in its permissions."""


class DefaultPluginConfig(BaseModel):
    enabled: bool = True


class ChannelRoleSpec(BaseModel):
    role: str
    title: str
    description: str = ""


class PushCategorySpec(BaseModel):
    id: str
    label: str
    default_enabled: bool = True


class PwaViewSpec(BaseModel):
    id: str
    title: str
    short_name: str
    mount_path: str
    nav_tab: str = ""
    icon: str = "sparkles"
    script_path: str = ""
    standalone_pwa: bool = False


class PluginManifest(BaseModel):
    api_version: str = "agy.cloud/v1"
    id: str
    name: str
    version: str = "0.1.0"
    description: str = ""
    author: str = ""
    entrypoint: str = ""
    dependencies: list[str] = Field(default_factory=list)
    optional_dependencies: list[str] = Field(default_factory=list)
    python_dependencies: list[str] = Field(default_factory=list)
    permissions: list[str] = Field(default_factory=lambda: sorted(ALL_PLUGIN_PERMISSIONS))
    default_config: dict[str, Any] = Field(default_factory=dict)
    channel_roles: list[ChannelRoleSpec] = Field(default_factory=list)
    push_categories: list[PushCategorySpec] = Field(default_factory=list)
    pwa_views: list[PwaViewSpec] = Field(default_factory=list)
    skills_dir: str = "skills"


class PluginRepoSource(BaseModel):
    """Git repository source containing one or more `agy-cloud` plugins + default config."""

    repo: str
    ref: str = "main"
    subpath: str = ""
    auth_env: str = ""
    allowed_permissions: list[str] | None = None
    install_python_deps: bool = True
    config: dict[str, dict[str, Any]] = Field(default_factory=dict)


class TransportEntryConfig(BaseModel):
    """Configuration for an outbound chat/notification transport (Discord, Telegram, Ntfy, Slack, Webhook)."""

    kind: str  # "discord_webhook" | "telegram" | "ntfy" | "slack_webhook" | "webhook"
    enabled: bool = True
    url: str = ""
    token: str = ""
    chat_id: str = ""
    topic: str = ""
    roles: list[str] = Field(default_factory=list)


class WorkerRunnerConfig(BaseModel):
    """Configuration for how `ctx.jobs.spawn_agent_job` executes worker tasks."""

    mode: str = "recording"  # "recording" | "local" | "k8s"
    namespace: str = "agy-cloud"
    worker_image: str = "ghcr.io/agy-cloud/agy-cloud-worker:latest"
    worker_secret_name: str = "agy-cloud-worker-secret"
    worker_service_account: str = "agy-cloud-worker"
    state_pvc_name: str = "agy-cloud-state"
    local_command: list[str] = Field(default_factory=list)


class AgyCloudConfig(BaseModel):
    """Cluster-level configuration file (`config.yaml` / Kubernetes ConfigMap)."""

    require_pinned_refs: bool = False
    install_python_dependencies: bool = True
    plugin_sources: list[PluginRepoSource] = Field(default_factory=list)
    plugin_permissions: dict[str, list[str]] = Field(default_factory=dict)
    transports: list[TransportEntryConfig] = Field(default_factory=list)
    worker_runner: WorkerRunnerConfig = Field(default_factory=WorkerRunnerConfig)
    plugins: dict[str, dict[str, Any]] = Field(default_factory=dict)


@dataclass(slots=True)
class MediaAttachment:
    filename: str
    raw_bytes: bytes
    content_type: str = "application/octet-stream"


@dataclass(slots=True)
class CommandEvent:
    command: str
    args: str
    raw_text: str
    user: str
    channel_id: str
    thread_ts: str
    model_override: str | None = None
    attachments: list[MediaAttachment] = field(default_factory=list)


@dataclass(slots=True)
class ChannelMessageEvent:
    role: str
    text: str
    user: str
    channel_id: str
    thread_ts: str
    attachments: list[MediaAttachment] = field(default_factory=list)


@dataclass(slots=True)
class ThreadReplyEvent:
    kind: str
    thread_ts: str
    reply_ts: str
    text: str
    user: str
    channel_id: str
    attachments: list[MediaAttachment] = field(default_factory=list)


@dataclass(slots=True)
class ButtonEvent:
    action_prefix: str
    action_id: str
    value: str
    user: str
    channel_id: str
    thread_ts: str


@dataclass(slots=True)
class JobSpec:
    task_id: str
    short_id: str
    plugin_id: str
    github_repo: str
    prompt: str
    base_branch: str = "main"
    base_sha: str | None = None
    initial_patch: str | None = None
    focus_subdir: str = ""
    verify_hint: str = ""
    model: str = "gemini-4-hydrogen"
    turn: int = 1
    conversation_id: str | None = None
    system_instructions: str | None = None


@dataclass(slots=True)
class JobResult:
    task_id: str
    turn: int
    status: str
    conversation_id: str | None = None
    model: str = "gemini-4-hydrogen"
    has_patch: bool = False
    patch_text: str = ""
    patch_stats: str = ""
    summary: str = ""
    root_cause: str = ""
    affected_files: list[str] = field(default_factory=list)
    tests_passed: bool = False
    error: str | None = None


class ChannelBusProtocol(Protocol):
    async def post_message(
        self,
        text: str,
        *,
        role: str = "",
        channel_id: str | None = None,
        actions: list[dict[str, str]] | None = None,
    ) -> str: ...

    async def post_thread_message(
        self,
        thread_ts: str,
        text: str,
        *,
        channel_id: str | None = None,
        actions: list[dict[str, str]] | None = None,
    ) -> str: ...

    async def broadcast_ws(self, event_type: str, data: dict[str, Any]) -> None: ...


class WorkerJobServiceProtocol(Protocol):
    async def spawn_agent_job(self, spec: JobSpec) -> str: ...


# ---------------------------------------------------------------------------
# Declarative Handler Decorators
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class CommandRegistration:
    name: str
    aliases: list[str]
    role: str
    help_text: str
    method_name: str


@dataclass(slots=True)
class ChannelListenerRegistration:
    role: str
    method_name: str


@dataclass(slots=True)
class ThreadReplyRegistration:
    kind: str
    method_name: str


@dataclass(slots=True)
class ButtonActionRegistration:
    prefix: str
    method_name: str


@dataclass(slots=True)
class CronRegistration:
    expression: str
    timezone: str
    method_name: str


def command(
    name: str,
    *,
    aliases: list[str] | None = None,
    role: str = "",
    help: str = "",
) -> Callable[[Callable[..., Awaitable[str]]], Callable[..., Awaitable[str]]]:
    """Register a `!command` (and optional aliases) handled by this plugin."""

    def decorator(fn: Callable[..., Awaitable[str]]) -> Callable[..., Awaitable[str]]:
        fn._agy_command = CommandRegistration(  # type: ignore[attr-defined]
            name=name.lstrip("!").lower(),
            aliases=[a.lstrip("!").lower() for a in (aliases or [])],
            role=role,
            help_text=help,
            method_name=fn.__name__,
        )
        return fn

    return decorator


def channel_listener(
    *, role: str
) -> Callable[[Callable[..., Awaitable[str | None]]], Callable[..., Awaitable[str | None]]]:
    """Intercept zero-prefix messages/uploads posted in a channel bound to `role`."""

    def decorator(
        fn: Callable[..., Awaitable[str | None]],
    ) -> Callable[..., Awaitable[str | None]]:
        fn._agy_channel_listener = ChannelListenerRegistration(  # type: ignore[attr-defined]
            role=role.lower(),
            method_name=fn.__name__,
        )
        return fn

    return decorator


def thread_reply_handler(
    *, kind: str
) -> Callable[[Callable[..., Awaitable[str | None]]], Callable[..., Awaitable[str | None]]]:
    """Handle user replies inside a thread created with `kind`."""

    def decorator(
        fn: Callable[..., Awaitable[str | None]],
    ) -> Callable[..., Awaitable[str | None]]:
        fn._agy_thread_reply = ThreadReplyRegistration(  # type: ignore[attr-defined]
            kind=kind.lower(),
            method_name=fn.__name__,
        )
        return fn

    return decorator


def button_action(
    prefix: str,
) -> Callable[[Callable[..., Awaitable[str | None]]], Callable[..., Awaitable[str | None]]]:
    """Handle interactive button actions whose action_id starts with `prefix`."""

    def decorator(
        fn: Callable[..., Awaitable[str | None]],
    ) -> Callable[..., Awaitable[str | None]]:
        fn._agy_button_action = ButtonActionRegistration(  # type: ignore[attr-defined]
            prefix=prefix,
            method_name=fn.__name__,
        )
        return fn

    return decorator


def cron(
    expression: str,
    *,
    timezone: str = "UTC",
) -> Callable[[Callable[..., Awaitable[None]]], Callable[..., Awaitable[None]]]:
    """Schedule a recurring 5-field cron method on the plugin."""

    def decorator(fn: Callable[..., Awaitable[None]]) -> Callable[..., Awaitable[None]]:
        fn._agy_cron = CronRegistration(  # type: ignore[attr-defined]
            expression=expression,
            timezone=timezone,
            method_name=fn.__name__,
        )
        return fn

    return decorator


# ---------------------------------------------------------------------------
# PluginContext & AgyCloudPlugin Base Class
# ---------------------------------------------------------------------------


class PluginContext[ConfigT: BaseModel]:
    """Scoped runtime context injected into each plugin on load with capability checks."""

    def __init__(
        self,
        *,
        plugin_id: str,
        manifest: PluginManifest,
        config: ConfigT,
        state_root: Path,
        plugin_source_dir: Path | None,
        services: dict[str, Any],
        channels: ChannelBusProtocol,
        jobs: WorkerJobServiceProtocol,
        granted_permissions: set[str] | None = None,
    ) -> None:
        self.plugin_id = plugin_id
        self.manifest = manifest
        self.config = config
        self.state_root = state_root
        self.plugin_source_dir = plugin_source_dir
        self._services = services
        self._channels = channels
        self._jobs = jobs
        self.granted_permissions: set[str] = (
            set(granted_permissions)
            if granted_permissions is not None
            else set(manifest.permissions)
        )

    def require_permission(self, permission: str) -> None:
        if permission not in self.granted_permissions:
            raise PluginPermissionError(
                f"Plugin `{self.plugin_id}` does not have permission `{permission}` "
                f"(granted: {sorted(self.granted_permissions)})."
            )

    @property
    def channels(self) -> ChannelBusProtocol:
        self.require_permission("channel_bus")
        return self._channels

    @property
    def jobs(self) -> WorkerJobServiceProtocol:
        self.require_permission("k8s_jobs")
        return self._jobs

    @property
    def data_dir(self) -> Path:
        path = self.state_root / "plugins" / self.plugin_id
        path.mkdir(parents=True, exist_ok=True)
        return path

    @property
    def media_dir(self) -> Path:
        self.require_permission("media")
        path = self.data_dir / "media"
        path.mkdir(parents=True, exist_ok=True)
        return path

    def sqlite_path(self, filename: str = "store.sqlite3") -> Path:
        self.require_permission("sqlite")
        return self.data_dir / filename

    def register_service(self, name: str, service: Any) -> None:
        """Publish an in-process service (e.g. 'garmin') for sibling plugins."""
        self.require_permission("services")
        self._services[name] = service

    def get_service(self, name: str) -> Any | None:
        """Look up an optional sibling plugin service by name."""
        self.require_permission("services")
        return self._services.get(name)


class AgyCloudPlugin[ConfigT: BaseModel]:
    """Base class for all `agy-cloud` server plugins."""

    config_model: type[BaseModel] = DefaultPluginConfig
    default_manifest: PluginManifest | None = None

    def __init__(self, manifest: PluginManifest | None = None) -> None:
        if manifest is not None:
            self.manifest = manifest
        elif self.default_manifest is not None:
            self.manifest = self.default_manifest
        else:
            raise ValueError(f"{type(self).__name__} requires a PluginManifest.")
        self._ctx: PluginContext[ConfigT] | None = None

    @property
    def ctx(self) -> PluginContext[ConfigT]:
        if self._ctx is None:
            raise RuntimeError(f"Plugin `{self.manifest.id}` has not been loaded yet.")
        return self._ctx

    async def on_load(self, ctx: PluginContext[ConfigT]) -> None:
        """Called in topological dependency order when the plugin is loaded."""
        self._ctx = ctx

    async def on_start(self) -> None:
        """Called when the controller starts serving."""

    async def on_stop(self) -> None:
        """Called on graceful shutdown."""

    def get_api_router(self) -> APIRouter | None:
        """Return an optional FastAPI APIRouter mounted at `/api/plugins/<plugin_id>`."""
        return None
