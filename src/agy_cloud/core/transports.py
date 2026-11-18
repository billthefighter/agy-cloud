from __future__ import annotations

from typing import Any

import httpx
import structlog

from agy_cloud.core.gateway import MemoryChannelBus
from agy_cloud.core.loader import expand_env_placeholders
from agy_cloud.sdk.spec import ChannelBusProtocol, TransportEntryConfig

log = structlog.get_logger(__name__)


class OutboundHttpTransport:
    """Base HTTP transport for Discord, Telegram, Ntfy, Slack Webhooks, and Generic Webhooks."""

    def __init__(
        self,
        cfg: TransportEntryConfig,
        *,
        http_client: httpx.AsyncClient | None = None,
    ) -> None:
        resolved = expand_env_placeholders(cfg.model_dump())
        self.cfg = TransportEntryConfig.model_validate(resolved)
        self._client = http_client

    def matches_role(self, role: str) -> bool:
        if not self.cfg.enabled:
            return False
        if not self.cfg.roles:
            return True
        return role.lower() in {r.lower() for r in self.cfg.roles}

    async def _post_json(
        self,
        url: str,
        payload: dict[str, Any],
        headers: dict[str, str] | None = None,
    ) -> None:
        if self._client is not None:
            resp = await self._client.post(url, json=payload, headers=headers)
            resp.raise_for_status()
            return
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(url, json=payload, headers=headers)
            resp.raise_for_status()

    async def send(
        self,
        text: str,
        *,
        role: str = "",
        channel_id: str | None = None,
        thread_ts: str | None = None,
        actions: list[dict[str, str]] | None = None,
    ) -> None:
        if not self.matches_role(role):
            return
        kind = self.cfg.kind.strip().lower()
        if kind == "discord_webhook":
            await self._send_discord(text, role=role, actions=actions)
        elif kind == "telegram":
            await self._send_telegram(text, role=role, actions=actions)
        elif kind == "ntfy":
            await self._send_ntfy(text, role=role)
        elif kind == "slack_webhook":
            await self._send_slack_webhook(text, role=role)
        elif kind == "webhook":
            await self._send_generic_webhook(
                text,
                role=role,
                channel_id=channel_id,
                thread_ts=thread_ts,
                actions=actions,
            )
        else:
            raise ValueError(f"Unsupported transport kind `{self.cfg.kind}`.")

    async def _send_discord(
        self,
        text: str,
        *,
        role: str,
        actions: list[dict[str, str]] | None,
    ) -> None:
        prefix = f"**[{role.upper()}]** " if role else ""
        action_note = ""
        if actions:
            labels = ", ".join(f"`{a.get('label', a.get('action_id', 'Action'))}`" for a in actions)
            action_note = f"\nActions: {labels}"
        await self._post_json(self.cfg.url, {"content": f"{prefix}{text}{action_note}"[:2000]})

    async def _send_telegram(
        self,
        text: str,
        *,
        role: str,
        actions: list[dict[str, str]] | None,
    ) -> None:
        base = self.cfg.url.rstrip("/") or "https://api.telegram.org"
        api_url = f"{base}/bot{self.cfg.token}/sendMessage"
        payload: dict[str, Any] = {
            "chat_id": self.cfg.chat_id,
            "text": f"[{role}] {text}" if role else text,
        }
        if actions:
            payload["reply_markup"] = {
                "inline_keyboard": [
                    [
                        {
                            "text": a.get("label", a.get("action_id", "Action")),
                            "callback_data": a.get("action_id", "noop")[:64],
                        }
                        for a in actions
                    ]
                ]
            }
        await self._post_json(api_url, payload)

    async def _send_ntfy(self, text: str, *, role: str) -> None:
        base = self.cfg.url.rstrip("/") or "https://ntfy.sh"
        payload: dict[str, Any] = {
            "topic": self.cfg.topic or "agy-cloud",
            "title": f"agy-cloud ({role or 'ops'})",
            "message": text,
        }
        headers = {"Authorization": f"Bearer {self.cfg.token}"} if self.cfg.token else None
        await self._post_json(base, payload, headers=headers)

    async def _send_slack_webhook(self, text: str, *, role: str) -> None:
        prefix = f"*[{role}]* " if role else ""
        await self._post_json(self.cfg.url, {"text": f"{prefix}{text}"})

    async def _send_generic_webhook(
        self,
        text: str,
        *,
        role: str,
        channel_id: str | None,
        thread_ts: str | None,
        actions: list[dict[str, str]] | None,
    ) -> None:
        headers = {"Authorization": f"Bearer {self.cfg.token}"} if self.cfg.token else None
        await self._post_json(
            self.cfg.url,
            {
                "role": role,
                "channel_id": channel_id,
                "thread_ts": thread_ts,
                "text": text,
                "actions": actions or [],
            },
            headers=headers,
        )


class CompositeChannelBus(ChannelBusProtocol):
    """Unified channel bus combining in-memory/PWA WebSocket state with external transports."""

    def __init__(
        self,
        *,
        primary: MemoryChannelBus | None = None,
        transports: list[OutboundHttpTransport] | None = None,
    ) -> None:
        self.primary = primary or MemoryChannelBus()
        self.transports: list[OutboundHttpTransport] = list(transports or [])

    @classmethod
    def from_configs(
        cls,
        configs: list[TransportEntryConfig],
        *,
        http_client: httpx.AsyncClient | None = None,
    ) -> CompositeChannelBus:
        transports = [
            OutboundHttpTransport(c, http_client=http_client) for c in configs if c.enabled
        ]
        return cls(primary=MemoryChannelBus(), transports=transports)

    @property
    def messages(self):
        return self.primary.messages

    @property
    def ws_events(self):
        return self.primary.ws_events

    async def post_message(
        self,
        text: str,
        *,
        role: str = "",
        channel_id: str | None = None,
        actions: list[dict[str, str]] | None = None,
    ) -> str:
        ts = await self.primary.post_message(
            text,
            role=role,
            channel_id=channel_id,
            actions=actions,
        )
        for tr in self.transports:
            try:
                await tr.send(
                    text,
                    role=role,
                    channel_id=channel_id,
                    thread_ts=None,
                    actions=actions,
                )
            except Exception as exc:  # noqa: BLE001
                log.warning("transport.send_failed", kind=tr.cfg.kind, error=str(exc))
        return ts

    async def post_thread_message(
        self,
        thread_ts: str,
        text: str,
        *,
        channel_id: str | None = None,
        actions: list[dict[str, str]] | None = None,
    ) -> str:
        ts = await self.primary.post_thread_message(
            thread_ts,
            text,
            channel_id=channel_id,
            actions=actions,
        )
        for tr in self.transports:
            try:
                await tr.send(
                    text,
                    role="thread",
                    channel_id=channel_id,
                    thread_ts=thread_ts,
                    actions=actions,
                )
            except Exception as exc:  # noqa: BLE001
                log.warning("transport.thread_send_failed", kind=tr.cfg.kind, error=str(exc))
        return ts

    async def broadcast_ws(self, event_type: str, data: dict[str, Any]) -> None:
        await self.primary.broadcast_ws(event_type, data)
