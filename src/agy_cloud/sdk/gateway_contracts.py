from __future__ import annotations

import asyncio
from collections.abc import Awaitable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Literal, Protocol, runtime_checkable

from fastapi import APIRouter
from pydantic import BaseModel, Field

PendingJobStatus = Literal["queued", "running", "completed", "failed"]


class PluginDTO(BaseModel):
    """Base Pydantic DTO for plugin HTTP requests, responses, and realtime events."""


class PendingJobRecord(PluginDTO):
    """Typed status record for a plugin-enqueued asynchronous background job."""

    job_id: str
    plugin_id: str
    kind: str
    label: str
    model: str = ""
    status: PendingJobStatus = "queued"
    started_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    finished_at: datetime | None = None
    error: str | None = None


class TransportHealth(PluginDTO):
    """Typed health snapshot reported by a plugin-provided transport adapter."""

    name: str
    connected: bool
    detail: str = ""


class EventSchemaSummary(PluginDTO):
    """OpenAPI/discovery summary of a plugin-declared realtime event topic."""

    plugin_id: str
    topic: str
    schema_name: str
    description: str = ""
    json_schema: dict[str, Any] = Field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class EventSpec[PayloadT: BaseModel]:
    """Declarative specification for a typed realtime event emitted by a plugin."""

    topic: str
    payload_model: type[PayloadT]
    description: str = ""


@dataclass(frozen=True, slots=True)
class EventEnvelope[PayloadT: BaseModel]:
    """Runtime envelope wrapping a validated plugin event DTO."""

    topic: str
    plugin_id: str
    payload: PayloadT
    emitted_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    def to_wire_payload(self) -> dict[str, Any]:
        return self.payload.model_dump(mode="json")


@dataclass(frozen=True, slots=True)
class HttpRouteSpec:
    """Declarative HTTP router contribution mounted by the gateway composition root."""

    router: APIRouter
    prefix: str = ""
    tags: tuple[str, ...] = ()
    require_auth: bool = True


@runtime_checkable
class TransportAdapter(Protocol):
    """Lifecycle contract for a pluggable transport (Slack, Webhook, SSE, MCP, etc.)."""

    name: str

    async def start(self) -> None: ...

    async def stop(self) -> None: ...

    async def health(self) -> TransportHealth: ...


@dataclass(frozen=True, slots=True)
class GatewayContribution:
    """Declarative bundle of APIs, typed realtime events, and transports owned by a plugin."""

    http_routes: tuple[HttpRouteSpec, ...] = ()
    events: tuple[EventSpec[Any], ...] = ()
    transports: tuple[TransportAdapter, ...] = ()


@runtime_checkable
class HttpContributor(Protocol):
    """Capability protocol for plugins that expose HTTP endpoints."""

    def get_http_routes(self) -> tuple[HttpRouteSpec, ...]: ...


@runtime_checkable
class RealtimeContributor(Protocol):
    """Capability protocol for plugins that declare typed realtime event topics."""

    def get_event_specs(self) -> tuple[EventSpec[Any], ...]: ...


@runtime_checkable
class TransportContributor(Protocol):
    """Capability protocol for plugins that contribute external transport adapters."""

    def get_transports(self) -> tuple[TransportAdapter, ...]: ...


@runtime_checkable
class JobQueueProtocol(Protocol):
    """Host-provided async job queue shared across plugins via PluginContext."""

    def enqueue(
        self,
        *,
        job_id: str,
        plugin_id: str,
        kind: str,
        label: str,
        coro: Awaitable[Any],
        model: str = "",
    ) -> PendingJobRecord: ...

    def list_pending(
        self,
        *,
        plugin_id: str | None = None,
        kind: str | None = None,
    ) -> list[PendingJobRecord]: ...

    def get(self, job_id: str) -> PendingJobRecord | None: ...


class InMemoryJobQueue:
    """Typed in-process async job queue tracking active plugin background tasks."""

    def __init__(self) -> None:
        self._records: dict[str, PendingJobRecord] = {}
        self._tasks: set[asyncio.Task[Any]] = set()

    def enqueue(
        self,
        *,
        job_id: str,
        plugin_id: str,
        kind: str,
        label: str,
        coro: Awaitable[Any],
        model: str = "",
    ) -> PendingJobRecord:
        record = PendingJobRecord(
            job_id=job_id,
            plugin_id=plugin_id,
            kind=kind,
            label=label,
            model=model,
            status="running",
        )
        self._records[job_id] = record

        async def _runner() -> None:
            try:
                await coro
            except Exception as exc:
                self._records.pop(job_id, None)
                raise exc
            else:
                self._records.pop(job_id, None)

        task = asyncio.create_task(_runner())
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        return record

    def list_pending(
        self,
        *,
        plugin_id: str | None = None,
        kind: str | None = None,
    ) -> list[PendingJobRecord]:
        results: list[PendingJobRecord] = []
        for rec in self._records.values():
            if plugin_id is not None and rec.plugin_id != plugin_id:
                continue
            if kind is not None and rec.kind != kind:
                continue
            results.append(rec)
        return results

    def get(self, job_id: str) -> PendingJobRecord | None:
        return self._records.get(job_id)


@runtime_checkable
class EventBroadcastSink(Protocol):
    """Receiver for validated plugin event envelopes published on `TypedEventBus`."""

    async def __call__(self, envelope: EventEnvelope[BaseModel]) -> None: ...


class _LegacyWsBroadcaster(Protocol):
    async def broadcast_ws(self, event_type: str, data: dict[str, Any]) -> None: ...


class ChannelBusEventSink:
    """Adapter forwarding typed `EventEnvelope`s to a channel bus `broadcast_ws` method."""

    def __init__(self, broadcaster: _LegacyWsBroadcaster) -> None:
        self._broadcaster = broadcaster

    async def __call__(self, envelope: EventEnvelope[BaseModel]) -> None:
        await self._broadcaster.broadcast_ws(envelope.topic, envelope.to_wire_payload())


class TypedEventBus:
    """Registry and validated publisher for plugin-declared realtime event DTOs."""

    def __init__(self, sink: EventBroadcastSink | None = None) -> None:
        self._specs: dict[str, tuple[str, EventSpec[Any]]] = {}
        self._sink = sink
        self.published: list[EventEnvelope[Any]] = []

    @property
    def sink(self) -> EventBroadcastSink | None:
        return self._sink

    @sink.setter
    def sink(self, value: EventBroadcastSink | None) -> None:
        self._sink = value

    def register(self, plugin_id: str, spec: EventSpec[Any]) -> None:
        self._specs[spec.topic] = (plugin_id, spec)

    def list_schemas(self) -> list[EventSchemaSummary]:
        summaries: list[EventSchemaSummary] = []
        for topic, (plugin_id, spec) in sorted(self._specs.items()):
            summaries.append(
                EventSchemaSummary(
                    plugin_id=plugin_id,
                    topic=topic,
                    schema_name=spec.payload_model.__name__,
                    description=spec.description,
                    json_schema=spec.payload_model.model_json_schema(),
                )
            )
        return summaries

    async def publish[PayloadT: BaseModel](
        self,
        *,
        plugin_id: str,
        topic: str,
        payload: PayloadT,
    ) -> EventEnvelope[PayloadT]:
        registered = self._specs.get(topic)
        validated: Any = payload
        if registered is not None:
            _owner, spec = registered
            if not isinstance(payload, spec.payload_model):
                validated = spec.payload_model.model_validate(payload)
        envelope: EventEnvelope[PayloadT] = EventEnvelope(
            topic=topic,
            plugin_id=plugin_id,
            payload=validated,
        )
        self.published.append(envelope)
        if self._sink is not None:
            await self._sink(envelope)
        return envelope
