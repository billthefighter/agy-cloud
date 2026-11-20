from __future__ import annotations

import asyncio
import fcntl
import os
import signal
import socket
import time
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any

import structlog
import uvicorn

from agent_tasks.channels.base import FoodChannelAdapter
from agent_tasks.channels.composite import CompositeChannelAdapter
from agent_tasks.channels.slack_socket import VALID_CHANNEL_ROLES, SlackSocketAdapter
from agent_tasks.common.config import ControllerSettings, TargetsConfig, parse_model_override
from agent_tasks.common.state import StateStore
from agent_tasks.pwa.gateway import PwaGatewayAdapter, create_pwa_app
from agent_tasks.pwa.push import WebPushManager
from agent_tasks.tasks import LongLivedTask
from agent_tasks.tasks.billing_guard import BillingMonitorTask
from agent_tasks.tasks.ci_healer import CiHealerTask
from agent_tasks.tasks.dep_upgrader import DepUpgraderTask
from agent_tasks.tasks.direct_chat import DirectChatService
from agent_tasks.tasks.food_tracker import FoodTrackerTask
from agent_tasks.tasks.k8s_watchdog import K8sWatchdogTask
from agent_tasks.tasks.sentry_triage.k8s_jobs import K8sJobRunner
from agent_tasks.tasks.sentry_triage.orchestrator import SentryTriageTask
from agent_tasks.tasks.slack_coding import SlackCodingTask
from agy_cloud.core.gateway import MemoryChannelBus, RecordingWorkerJobService
from agy_cloud.core.loader import PluginRegistry
from agy_cloud.core.migration import migrate_agent_tasks_pvc

log = structlog.get_logger(__name__)

HEARTBEAT_FILE = Path("/tmp/heartbeat")


def touch_heartbeat() -> None:
    HEARTBEAT_FILE.touch(exist_ok=True)


class _ManagedUvicornServer(uvicorn.Server):
    """Run uvicorn inside controller_main's event loop without overriding SIGINT/SIGTERM."""

    def install_signal_handlers(self) -> None:
        return


class _PwaServerTask:
    name = "pwa-http-gateway"

    def __init__(self, server: uvicorn.Server) -> None:
        self.server = server

    async def run(self) -> None:
        await self.server.serve()


async def _supervise_task(task: LongLivedTask) -> None:
    """Keep each long-lived task running independently without bringing down siblings."""
    while True:
        try:
            await task.run()
            return
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("controller.task_crashed_restarting", task=task.name)
            await asyncio.sleep(10)


class ControllerLeaderLock:
    """Exclusive non-blocking POSIX flock on `/state/.controller.lock`.

    Allows a newly-started RollingUpdate surge pod to bind the PWA HTTP server
    and pass readiness probes immediately while waiting for the draining pod to
    stop Slack Socket Mode and background pollers and release leadership.
    Because `flock` is held by the kernel on the open file descriptor, an abrupt
    SIGKILL/OOM automatically releases leadership with zero stale lock files.
    """

    def __init__(self, lock_path: Path) -> None:
        self.lock_path = lock_path
        self._fd: int | None = None

    @property
    def acquired(self) -> bool:
        return self._fd is not None

    def try_acquire(self) -> bool:
        if self._fd is not None:
            return True
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(self.lock_path, os.O_CREAT | os.O_RDWR, 0o644)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            os.close(fd)
            return False
        except OSError:
            os.close(fd)
            raise

        self._fd = fd
        try:
            os.ftruncate(fd, 0)
            os.lseek(fd, 0, os.SEEK_SET)
            payload = (
                f"pid={os.getpid()} host={socket.gethostname()} acquired_at={time.time():.3f}\n"
            )
            os.write(fd, payload.encode("utf-8"))
        except OSError:
            pass
        return True

    async def acquire(
        self,
        stop_event: asyncio.Event,
        *,
        poll_interval: float = 0.25,
        heartbeat_cb: Callable[[], None] | None = None,
    ) -> bool:
        logged_wait = False
        while not stop_event.is_set():
            if self.try_acquire():
                log.info("controller.leader_lock_acquired", lock_path=str(self.lock_path))
                return True
            if not logged_wait:
                log.info("controller.waiting_for_leader_lock", lock_path=str(self.lock_path))
                logged_wait = True
            if heartbeat_cb is not None:
                heartbeat_cb()
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=poll_interval)
                return False
            except TimeoutError:
                continue
        return False

    def release(self) -> None:
        fd = self._fd
        if fd is None:
            return
        self._fd = None
        try:
            fcntl.flock(fd, fcntl.LOCK_UN)
        except OSError:
            pass
        finally:
            try:
                os.close(fd)
            except OSError:
                pass
        log.info("controller.leader_lock_released", lock_path=str(self.lock_path))


async def _run_leader_work(
    *,
    leader_lock: ControllerLeaderLock,
    stop_event: asyncio.Event,
    background_tasks: Sequence[LongLivedTask],
    slack: Any | None = None,
    poll_interval: float = 0.25,
    shutdown_timeout: float = 10.0,
    heartbeat_cb: Callable[[], None] | None = None,
) -> None:
    """Hold exclusive leadership while running Slack Socket Mode and background pollers."""
    acquired = await leader_lock.acquire(
        stop_event,
        poll_interval=poll_interval,
        heartbeat_cb=heartbeat_cb,
    )
    if not acquired:
        return

    slack_started = False
    bg_futures: list[asyncio.Task[None]] = []
    try:
        if slack is not None:
            try:
                await slack.start()
                slack_started = True
            except Exception as exc:  # noqa: BLE001
                log.warning("controller.slack_start_failed", error=str(exc))

        bg_futures = [
            asyncio.create_task(_supervise_task(t), name=f"task-{t.name}") for t in background_tasks
        ]
        log.info(
            "controller.leader_tasks_started",
            slack_started=slack_started,
            tasks=[t.name for t in background_tasks],
        )
        await stop_event.wait()
    finally:
        for fut in bg_futures:
            fut.cancel()
        if bg_futures:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*bg_futures, return_exceptions=True),
                    timeout=shutdown_timeout,
                )
            except TimeoutError:
                log.warning("controller.leader_tasks_shutdown_timeout", timeout_s=shutdown_timeout)
        if slack is not None and slack_started:
            try:
                await asyncio.wait_for(slack.stop(), timeout=5.0)
            except Exception as exc:  # noqa: BLE001
                log.warning("controller.slack_stop_failed", error=str(exc))
        leader_lock.release()


async def async_main() -> None:
    settings = ControllerSettings.from_env()
    touch_heartbeat()

    mig_report = migrate_agent_tasks_pvc(settings.state_dir)
    log.info(
        "controller.pvc_migration_checked",
        meals_count=mig_report.meals_count,
        garmin_days=mig_report.garmin_days_migrated,
        photos_symlinked=mig_report.photos_symlinked,
    )

    plugin_registry = PluginRegistry(
        state_root=settings.state_dir,
        channels=MemoryChannelBus(),
        jobs=RecordingWorkerJobService(),
    )
    for candidate_plugins_dir in (
        settings.state_dir / "plugin_repos" / "agy-cloud-private-plugins",
        settings.state_dir / "plugin_repos" / "agy-plugins-health",
        Path("/app/external_plugins/agy-cloud-private-plugins"),
        Path("/Users/lucaswhipple/Dropbox/code_projects/agy-cloud-private-plugins"),
        Path("/app/plugins"),
        Path(__file__).resolve().parent.parent.parent / "plugins",
    ):
        if candidate_plugins_dir.is_dir():
            await plugin_registry.load_all(
                plugins_dir=candidate_plugins_dir,
                include_entrypoints=False,
            )

    state = StateStore(settings.state_dir / "state.sqlite3")
    job_runner = K8sJobRunner(settings)

    triage_ref: list[SentryTriageTask] = []
    slack_coding_ref: list[SlackCodingTask] = []
    ci_healer_ref: list[CiHealerTask] = []
    k8s_watchdog_ref: list[K8sWatchdogTask] = []
    dep_upgrader_ref: list[DepUpgraderTask] = []
    billing_ref: list[BillingMonitorTask] = []
    food_ref: list[FoodTrackerTask] = []

    def resolve_role_channel(role: str) -> str:
        bindings = state.get_channel_bindings()
        if role in bindings and bindings[role].strip():
            return bindings[role].strip()
        try:
            if settings.targets_config_path.exists():
                cfg = TargetsConfig.from_yaml(settings.targets_config_path)
                static_val = getattr(cfg.channels, role, "")
                if isinstance(static_val, str) and static_val.strip():
                    return static_val.strip()
        except OSError:
            pass
        return ""

    async def handle_bind_channel(role_arg: str, channel_id: str) -> str:
        clean = role_arg.strip().lower()
        if clean == "all":
            for role in VALID_CHANNEL_ROLES:
                state.set_channel_binding(role, channel_id)
            return f"Bound all agent channels ({', '.join(VALID_CHANNEL_ROLES)}) to <#{channel_id}> (`{channel_id}`)."
        if clean in VALID_CHANNEL_ROLES:
            state.set_channel_binding(clean, channel_id)
            return f"Bound `{clean}` notifications to <#{channel_id}> (`{channel_id}`)."
        return (
            f"Unknown role `{role_arg}`. Valid roles: "
            + ", ".join(f"`{r}`" for r in VALID_CHANNEL_ROLES)
            + ", or `all`."
        )

    async def handle_channels_query() -> str:
        lines = [
            f"*`agent-tasks` Channel Routing* (default fallback: <#{settings.slack_channel_id}>)"
        ]
        for role in VALID_CHANNEL_ROLES:
            resolved = resolve_role_channel(role)
            if resolved:
                lines.append(f"• `{role}` -> <#{resolved}> (`{resolved}`)")
            elif role == "deps" and resolve_role_channel("ci"):
                ci_ch = resolve_role_channel("ci")
                lines.append(f"• `{role}` -> <#{ci_ch}> (via `ci`)")
            else:
                lines.append(f"• `{role}` -> <#{settings.slack_channel_id}> _(default)_")
        lines.append("Run `/invite @agytasks` + `!bind <role>` inside any channel to override.")
        return "\n".join(lines)

    async def handle_deps_command(args_text: str, thread_ts: str, channel_id: str) -> str:
        model_override, focus = parse_model_override(args_text)
        return await dep_upgrader_ref[0].trigger_upgrade_job(
            focus=focus,
            model_override=model_override,
            thread_ts=thread_ts,
            channel_id=channel_id,
            scheduled=False,
        )

    slack: SlackSocketAdapter | None = None
    if settings.slack_enabled and settings.slack_bot_token and settings.slack_app_token:
        slack = SlackSocketAdapter(
            bot_token=settings.slack_bot_token,
            app_token=settings.slack_app_token,
            channel_id=settings.slack_channel_id,
            on_open_draft_pr=lambda issue_id, user: triage_ref[0].handle_open_draft_pr(
                issue_id, user
            ),
            on_push_to_pr_branch=lambda issue_id, user: triage_ref[0].handle_push_to_pr_branch(
                issue_id, user
            ),
            on_resolve=lambda issue_id, user: triage_ref[0].handle_resolve(issue_id, user),
            on_ignore=lambda issue_id, user: triage_ref[0].handle_ignore(issue_id, user),
            on_thread_feedback=lambda ts, text, user: triage_ref[0].handle_thread_feedback(
                ts, text, user
            ),
            on_status_query=lambda: triage_ref[0].build_keepalive_report(),
            on_k8s_query=lambda: k8s_watchdog_ref[0].build_cluster_report(),
            on_ci_query=lambda: ci_healer_ref[0].summarize_recent_failures(),
            on_deps_trigger=handle_deps_command,
            on_coding_request=lambda prompt, user, ts, chan: slack_coding_ref[
                0
            ].handle_coding_request(
                prompt_text=prompt,
                user=user,
                thread_ts=ts,
                channel_id=chan,
            ),
            on_billing_command=lambda args: billing_ref[0].handle_billing_command(args),
            on_bind_channel=handle_bind_channel,
            on_channels_query=handle_channels_query,
            channel_resolver=resolve_role_channel,
            on_food_message=lambda text, user, chan, ts, files, is_food: food_ref[0].handle_message(
                text, user, chan, ts, files, is_food
            ),
            on_food_thread_reply=lambda t_ts, text, user, chan, r_ts, files: food_ref[
                0
            ].handle_thread_reply(t_ts, text, user, chan, r_ts, files),
            on_food_scale=lambda entry_id, mult, user: food_ref[0].handle_scale_action(
                entry_id, mult, user
            ),
            on_food_save_staple=lambda entry_id, user: food_ref[0].handle_save_staple_action(
                entry_id, user
            ),
            on_food_delete=lambda entry_id, user: food_ref[0].handle_delete_action(entry_id, user),
            on_save_memory_note=lambda issue_id, user: triage_ref[0].handle_save_memory_note(
                issue_id, user
            ),
            on_snooze=lambda ts, args, user: triage_ref[0].handle_snooze(ts, args, user),
            on_timers_command=lambda args: triage_ref[0].handle_timers_command(args),
            on_memory_command=lambda act, args, user: triage_ref[0].handle_memory_command(
                act, args, user
            ),
        )

    pwa: PwaGatewayAdapter | None = None
    push_manager: WebPushManager | None = None
    pwa_server: _ManagedUvicornServer | None = None
    if settings.pwa_enabled:
        push_manager = WebPushManager(settings=settings, state=state)
        direct_chat = DirectChatService(settings=settings, state=state)
        pwa = PwaGatewayAdapter(
            settings=settings,
            state=state,
            push_manager=push_manager,
            direct_chat=direct_chat,
            on_open_draft_pr=lambda issue_id, user: triage_ref[0].handle_open_draft_pr(
                issue_id, user
            ),
            on_push_to_pr_branch=lambda issue_id, user: triage_ref[0].handle_push_to_pr_branch(
                issue_id, user
            ),
            on_resolve=lambda issue_id, user: triage_ref[0].handle_resolve(issue_id, user),
            on_ignore=lambda issue_id, user: triage_ref[0].handle_ignore(issue_id, user),
            on_reject=lambda issue_id, user: triage_ref[0].handle_reject(issue_id, user),
            on_thread_feedback=lambda ts, text, user: triage_ref[0].handle_thread_feedback(
                ts, text, user
            ),
            on_status_query=lambda: triage_ref[0].build_keepalive_report(),
            on_k8s_query=lambda: k8s_watchdog_ref[0].build_cluster_report(),
            on_ci_query=lambda: ci_healer_ref[0].summarize_recent_failures(),
            on_deps_trigger=handle_deps_command,
            on_coding_request=lambda prompt, user, ts, chan: slack_coding_ref[
                0
            ].handle_coding_request(
                prompt_text=prompt,
                user=user,
                thread_ts=ts,
                channel_id=chan,
            ),
            on_billing_command=lambda args: billing_ref[0].handle_billing_command(args),
            on_food_message=lambda text, user, chan, ts, files, is_food: food_ref[0].handle_message(
                text, user, chan, ts, files, is_food
            ),
            on_food_thread_reply=lambda t_ts, text, user, chan, r_ts, files: food_ref[
                0
            ].handle_thread_reply(t_ts, text, user, chan, r_ts, files),
            on_food_scale=lambda entry_id, mult, user: food_ref[0].handle_scale_action(
                entry_id, mult, user
            ),
            on_food_save_staple=lambda entry_id, user: food_ref[0].handle_save_staple_action(
                entry_id, user
            ),
            on_food_delete=lambda entry_id, user: food_ref[0].handle_delete_action(entry_id, user),
            on_save_memory_note=lambda issue_id, user: triage_ref[0].handle_save_memory_note(
                issue_id, user
            ),
            on_snooze=lambda ts, args, user: triage_ref[0].handle_snooze(ts, args, user),
            on_timers_command=lambda args: triage_ref[0].handle_timers_command(args),
            on_memory_command=lambda act, args, user: triage_ref[0].handle_memory_command(
                act, args, user
            ),
        )
        pwa.slack_adapter = slack

    if pwa is not None:
        channel: FoodChannelAdapter = CompositeChannelAdapter(
            pwa=pwa,
            slack=slack,
            mirror_pwa_to_slack=settings.pwa_mirror_to_slack,
        )
    elif slack is not None:
        channel = slack
    else:
        raise RuntimeError("At least one channel (SLACK_ENABLED or PWA_ENABLED) must be enabled.")

    sentry_task = SentryTriageTask(
        settings=settings,
        state=state,
        channel=channel,
        job_runner=job_runner,
        heartbeat_cb=touch_heartbeat,
    )
    triage_ref.append(sentry_task)

    dispatch_cb = sentry_task.dispatch_pending

    billing_task = BillingMonitorTask(
        settings=settings,
        state=state,
        channel=channel,
        heartbeat_cb=touch_heartbeat,
        dispatch_callback=dispatch_cb,
    )
    billing_ref.append(billing_task)
    sentry_task.billing_guard = billing_task

    slack_coding_task = SlackCodingTask(
        settings=settings,
        state=state,
        heartbeat_cb=touch_heartbeat,
        dispatch_callback=dispatch_cb,
    )
    slack_coding_ref.append(slack_coding_task)

    ci_healer_task = CiHealerTask(
        settings=settings,
        state=state,
        heartbeat_cb=touch_heartbeat,
        dispatch_callback=dispatch_cb,
    )
    ci_healer_ref.append(ci_healer_task)

    k8s_watchdog_task = K8sWatchdogTask(
        settings=settings,
        state=state,
        heartbeat_cb=touch_heartbeat,
        dispatch_callback=dispatch_cb,
    )
    k8s_watchdog_ref.append(k8s_watchdog_task)

    dep_upgrader_task = DepUpgraderTask(
        settings=settings,
        state=state,
        heartbeat_cb=touch_heartbeat,
        dispatch_callback=dispatch_cb,
    )
    dep_upgrader_ref.append(dep_upgrader_task)

    food_task = FoodTrackerTask(
        settings=settings,
        state=state,
        channel=channel,
        push_manager=push_manager,
        heartbeat_cb=touch_heartbeat,
    )
    food_ref.append(food_task)

    if pwa is not None:
        pwa.food_task = food_task
        pwa.billing_task = billing_task
        pwa.plugin_registry = plugin_registry
        pwa_app = create_pwa_app(pwa)
        uv_config = uvicorn.Config(
            pwa_app,
            host=settings.pwa_host,
            port=settings.pwa_port,
            log_config=None,
            ws_ping_interval=25,
            ws_ping_timeout=25,
        )
        pwa_server = _ManagedUvicornServer(uv_config)

    background_tasks: list[LongLivedTask] = [
        sentry_task,
        slack_coding_task,
        ci_healer_task,
        k8s_watchdog_task,
        dep_upgrader_task,
        billing_task,
        food_task,
    ]

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop_event.set)

    pwa_task: asyncio.Task[None] | None = None
    if pwa is not None:
        await pwa.start()
    if pwa_server is not None:
        pwa_task = asyncio.create_task(
            _supervise_task(_PwaServerTask(pwa_server)),
            name="task-pwa-http-gateway",
        )

    leader_lock = ControllerLeaderLock(settings.state_dir / ".controller.lock")
    leader_task = asyncio.create_task(
        _run_leader_work(
            leader_lock=leader_lock,
            stop_event=stop_event,
            background_tasks=background_tasks,
            slack=slack,
            heartbeat_cb=touch_heartbeat,
        ),
        name="controller-leader",
    )

    log.info(
        "controller.started",
        namespace=settings.namespace,
        node_name=settings.node_name,
        worker_model=settings.worker_model,
        slack_enabled=slack is not None,
        pwa_enabled=pwa is not None,
        pwa_port=settings.pwa_port if pwa is not None else None,
        tasks=[t.name for t in background_tasks] + (["pwa-http-gateway"] if pwa_server else []),
    )

    try:
        await stop_event.wait()
    finally:
        if pwa_server is not None:
            pwa_server.should_exit = True
        if pwa is not None:
            await pwa.stop()
        await asyncio.gather(leader_task, return_exceptions=True)
        if pwa_task is not None:
            try:
                await asyncio.wait_for(pwa_task, timeout=10.0)
            except TimeoutError:
                pwa_task.cancel()
                await asyncio.gather(pwa_task, return_exceptions=True)


def main() -> None:
    asyncio.run(async_main())


if __name__ == "__main__":
    main()
