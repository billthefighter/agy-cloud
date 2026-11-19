from __future__ import annotations

import asyncio
from pathlib import Path

from fastapi import APIRouter
from fastapi.testclient import TestClient

from agy_cloud.core.gateway import (
    MemoryChannelBus,
    RecordingWorkerJobService,
    create_microkernel_app,
)
from agy_cloud.core.loader import PluginRegistry
from agy_cloud.sdk import (
    AgyCloudPlugin,
    DefaultPluginConfig,
    EventEnvelope,
    EventSpec,
    GatewayContribution,
    HttpRouteSpec,
    InMemoryJobQueue,
    PluginContext,
    PluginDTO,
    PluginManifest,
    TransportHealth,
    TypedEventBus,
)


class _PingResponseDTO(PluginDTO):
    ok: bool
    source: str


class _PingEventDTO(PluginDTO):
    seq: int
    message: str


class _DummyTransport:
    name = "dummy-webhook"

    def __init__(self) -> None:
        self.started = False

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.started = False

    async def health(self) -> TransportHealth:
        return TransportHealth(name=self.name, connected=True, detail="ready")


class _ComposedPlugin(AgyCloudPlugin):
    default_manifest = PluginManifest(
        id="composed_demo",
        name="Composed Demo",
        version="0.1.0",
        permissions=["api_router", "channel_bus", "services"],
    )

    def __init__(self) -> None:
        super().__init__()
        self.transport = _DummyTransport()

    def get_gateway_contribution(self) -> GatewayContribution:
        router = APIRouter()

        @router.get("/ping", response_model=_PingResponseDTO)
        async def ping() -> _PingResponseDTO:
            await self.ctx.events.publish(
                plugin_id=self.manifest.id,
                topic="demo.ping",
                payload=_PingEventDTO(seq=1, message="hello"),
            )
            return _PingResponseDTO(ok=True, source=self.manifest.id)

        return GatewayContribution(
            http_routes=(HttpRouteSpec(router=router, prefix="/v1", tags=("demo",)),),
            events=(
                EventSpec(
                    topic="demo.ping",
                    payload_model=_PingEventDTO,
                    description="Emitted whenever /v1/ping is called.",
                ),
            ),
            transports=(self.transport,),
        )


def test_gateway_composes_plugin_routes_events_and_transports(tmp_path: Path) -> None:
    bus = MemoryChannelBus()
    jobs = RecordingWorkerJobService()
    registry = PluginRegistry(state_root=tmp_path, channels=bus, jobs=jobs)
    plugin = _ComposedPlugin()
    ctx = PluginContext(
        plugin_id=plugin.manifest.id,
        manifest=plugin.manifest,
        config=DefaultPluginConfig(),
        state_root=tmp_path,
        plugin_source_dir=None,
        services=registry.services,
        channels=bus,
        jobs=jobs,
    )
    asyncio.run(plugin.on_load(ctx))
    registry.plugins[plugin.manifest.id] = registry._index_plugin(
        plugin.manifest, plugin, ctx, None
    )

    app = create_microkernel_app(registry=registry, base_path="/agy")
    with TestClient(app) as client:
        resp = client.get("/agy/api/plugins/composed_demo/v1/ping")
        assert resp.status_code == 200
        assert resp.json() == {"ok": True, "source": "composed_demo"}
        assert bus.ws_events == [("demo.ping", {"seq": 1, "message": "hello"})]

        schemas = client.get("/agy/api/events/schema")
        assert schemas.status_code == 200
        schema_items = schemas.json()
        assert len(schema_items) == 1
        assert schema_items[0]["plugin_id"] == "composed_demo"
        assert schema_items[0]["topic"] == "demo.ping"
        assert schema_items[0]["schema_name"] == "_PingEventDTO"
        assert "seq" in schema_items[0]["json_schema"]["properties"]

        transports = client.get("/agy/api/transports")
        assert transports.status_code == 200
        assert transports.json() == [
            {"name": "dummy-webhook", "connected": True, "detail": "ready"}
        ]


def test_in_memory_job_queue_and_typed_event_bus() -> None:
    async def _exercise() -> None:
        queue = InMemoryJobQueue()
        gate = asyncio.Event()

        async def _work() -> None:
            await gate.wait()

        rec = queue.enqueue(
            job_id="job-1",
            plugin_id="health",
            kind="meal_estimate",
            label="Costco salad",
            model="hydrogen",
            coro=_work(),
        )
        assert rec.job_id == "job-1"
        assert rec.status == "running"
        assert len(queue.list_pending(plugin_id="health", kind="meal_estimate")) == 1
        assert queue.list_pending(plugin_id="other") == []

        gate.set()
        await asyncio.sleep(0.02)
        assert queue.list_pending(plugin_id="health") == []

        captured: list[EventEnvelope] = []

        async def _sink(envelope: EventEnvelope) -> None:
            captured.append(envelope)

        ev_bus = TypedEventBus()
        ev_bus.sink = _sink
        assert ev_bus.sink is _sink
        ev_bus.register("demo", EventSpec(topic="demo.ping", payload_model=_PingEventDTO))
        env = await ev_bus.publish(
            plugin_id="demo",
            topic="demo.ping",
            payload=_PingEventDTO(seq=7, message="ok"),
        )
        assert env.payload.seq == 7
        assert len(captured) == 1
        assert captured[0].topic == "demo.ping"
        assert captured[0].payload == _PingEventDTO(seq=7, message="ok")

    asyncio.run(_exercise())
