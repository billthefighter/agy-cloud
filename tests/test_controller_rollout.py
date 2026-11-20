from __future__ import annotations

import asyncio
from pathlib import Path

import yaml

from agent_tasks.common.state import StateStore
from agent_tasks.controller_main import ControllerLeaderLock, _run_leader_work
from agent_tasks.tasks.food_tracker.store import NutritionStore
from agy_cloud.core.handoff import HandoffStore


class _FakeLongLivedTask:
    def __init__(self, name: str) -> None:
        self.name = name
        self.started = asyncio.Event()
        self.cancelled = asyncio.Event()

    async def run(self) -> None:
        self.started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            self.cancelled.set()
            raise


class _FakeSlackAdapter:
    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.stopped = asyncio.Event()
        self.start_count = 0
        self.stop_count = 0

    async def start(self) -> None:
        self.start_count += 1
        self.started.set()

    async def stop(self) -> None:
        self.stop_count += 1
        self.stopped.set()


def test_k3s_controller_deployment_uses_zero_downtime_rolling_update() -> None:
    dep_path = Path(__file__).resolve().parents[1] / "deploy" / "k3s" / "controller-deployment.yaml"
    dep = yaml.safe_load(dep_path.read_text(encoding="utf-8"))

    assert dep["spec"]["replicas"] == 1
    assert dep["spec"]["strategy"] == {
        "type": "RollingUpdate",
        "rollingUpdate": {"maxSurge": 1, "maxUnavailable": 0},
    }

    pod_spec = dep["spec"]["template"]["spec"]
    assert pod_spec["terminationGracePeriodSeconds"] >= 20

    container = pod_spec["containers"][0]
    readiness = container["readinessProbe"]
    assert readiness["httpGet"]["path"] == "/agy/healthz"
    assert readiness["initialDelaySeconds"] <= 3
    assert readiness["periodSeconds"] <= 3

    pre_stop_cmd = container["lifecycle"]["preStop"]["exec"]["command"]
    assert "sleep" in " ".join(pre_stop_cmd)


async def test_controller_leader_lock_hands_off_cleanly_between_pods(tmp_path: Path) -> None:
    lock_path = tmp_path / ".controller.lock"
    lock_a = ControllerLeaderLock(lock_path)
    lock_b = ControllerLeaderLock(lock_path)
    stop_a = asyncio.Event()
    stop_b = asyncio.Event()

    assert await lock_a.acquire(stop_a, poll_interval=0.02) is True
    assert lock_a.acquired is True
    assert lock_b.try_acquire() is False

    heartbeats = 0

    def bump_hb() -> None:
        nonlocal heartbeats
        heartbeats += 1

    acquire_b = asyncio.create_task(
        lock_b.acquire(stop_b, poll_interval=0.02, heartbeat_cb=bump_hb)
    )
    await asyncio.sleep(0.06)
    assert not acquire_b.done()
    assert heartbeats >= 1

    lock_a.release()
    assert lock_a.acquired is False

    assert await asyncio.wait_for(acquire_b, timeout=1.0) is True
    assert lock_b.acquired is True
    lock_b.release()
    assert lock_b.acquired is False


async def test_controller_leader_lock_aborts_wait_on_stop_event(tmp_path: Path) -> None:
    lock_path = tmp_path / ".controller.lock"
    holder = ControllerLeaderLock(lock_path)
    waiter = ControllerLeaderLock(lock_path)
    stop_waiter = asyncio.Event()

    assert holder.try_acquire() is True
    try:
        wait_task = asyncio.create_task(waiter.acquire(stop_waiter, poll_interval=0.05))
        await asyncio.sleep(0.04)
        stop_waiter.set()
        assert await asyncio.wait_for(wait_task, timeout=1.0) is False
        assert waiter.acquired is False
    finally:
        holder.release()


async def test_run_leader_work_defers_slack_and_pollers_until_handoff(tmp_path: Path) -> None:
    lock_path = tmp_path / ".controller.lock"

    slack_a = _FakeSlackAdapter()
    task_a = _FakeLongLivedTask("poller-a")
    stop_a = asyncio.Event()

    slack_b = _FakeSlackAdapter()
    task_b = _FakeLongLivedTask("poller-b")
    stop_b = asyncio.Event()

    leader_a = asyncio.create_task(
        _run_leader_work(
            leader_lock=ControllerLeaderLock(lock_path),
            stop_event=stop_a,
            background_tasks=[task_a],
            slack=slack_a,
            poll_interval=0.02,
        )
    )
    await asyncio.wait_for(task_a.started.wait(), timeout=1.0)
    assert slack_a.start_count == 1

    # Surge pod B starts while pod A still holds leadership.
    leader_b = asyncio.create_task(
        _run_leader_work(
            leader_lock=ControllerLeaderLock(lock_path),
            stop_event=stop_b,
            background_tasks=[task_b],
            slack=slack_b,
            poll_interval=0.02,
        )
    )
    await asyncio.sleep(0.06)
    assert not task_b.started.is_set()
    assert slack_b.start_count == 0

    # Pod A receives SIGTERM: stops pollers + Slack and releases lock.
    stop_a.set()
    await asyncio.wait_for(leader_a, timeout=1.0)
    assert task_a.cancelled.is_set()
    assert slack_a.stop_count == 1

    # Pod B takes over immediately.
    await asyncio.wait_for(task_b.started.wait(), timeout=1.0)
    assert slack_b.start_count == 1

    stop_b.set()
    await asyncio.wait_for(leader_b, timeout=1.0)
    assert task_b.cancelled.is_set()
    assert slack_b.stop_count == 1


def test_sqlite_stores_enable_wal_and_busy_timeout(tmp_path: Path) -> None:
    state_store = StateStore(tmp_path / "state.sqlite3")
    nutrition_store = NutritionStore(tmp_path / "nutrition.sqlite3")
    handoff_store = HandoffStore(tmp_path / "handoffs.sqlite3")

    for store in (state_store, nutrition_store, handoff_store):
        with store._connect() as conn:
            journal_mode = str(conn.execute("PRAGMA journal_mode;").fetchone()[0]).lower()
            busy_timeout = int(conn.execute("PRAGMA busy_timeout;").fetchone()[0])
            assert journal_mode == "wal"
            assert busy_timeout >= 30000
