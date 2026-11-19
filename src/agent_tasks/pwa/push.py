from __future__ import annotations

import asyncio
import base64
import json
from collections.abc import Callable
from typing import Any

import structlog
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat
from pywebpush import WebPushException, webpush

from agent_tasks.common.config import ControllerSettings
from agent_tasks.common.state import StateStore

log = structlog.get_logger(__name__)

PushSender = Callable[[dict[str, Any], str, str, dict[str, str | int]], None]

DEFAULT_PUSH_PREFS: dict[str, bool] = {
    "coding": True,
    "chat": True,
    "sentry": True,
    "ci": True,
    "deps": True,
    "k8s": True,
    "billing": True,
    "food": True,
    "ops": True,
}


def _generate_vapid_keypair() -> tuple[str, str]:
    priv = ec.generate_private_key(ec.SECP256R1())
    pub_raw = priv.public_key().public_bytes(Encoding.X962, PublicFormat.UncompressedPoint)
    priv_raw = priv.private_numbers().private_value.to_bytes(32, "big")
    pub_b64 = base64.urlsafe_b64encode(pub_raw).rstrip(b"=").decode("ascii")
    priv_b64 = base64.urlsafe_b64encode(priv_raw).rstrip(b"=").decode("ascii")
    return pub_b64, priv_b64


def _default_webpush_sender(
    subscription_info: dict[str, Any],
    data: str,
    vapid_private_key: str,
    vapid_claims: dict[str, str | int],
) -> None:
    webpush(
        subscription_info=subscription_info,
        data=data,
        vapid_private_key=vapid_private_key,
        vapid_claims=dict(vapid_claims),
        ttl=3600,
        timeout=15,
        headers={"Urgency": "high"},
    )


class WebPushManager:
    """RFC 8292 VAPID Web Push dispatcher for iOS Home Screen PWA subscriptions."""

    def __init__(
        self,
        *,
        settings: ControllerSettings,
        state: StateStore,
        sender: PushSender | None = None,
    ) -> None:
        self.settings = settings
        self.state = state
        self._sender = sender or _default_webpush_sender
        self.public_key, self.private_key = self._ensure_vapid_keys()

    def _ensure_vapid_keys(self) -> tuple[str, str]:
        if self.settings.vapid_public_key and self.settings.vapid_private_key:
            return self.settings.vapid_public_key, self.settings.vapid_private_key

        stored_pub = (self.state.get_kv("vapid_public_key") or "").strip()
        stored_priv = (self.state.get_kv("vapid_private_key") or "").strip()
        if stored_pub and stored_priv:
            return stored_pub, stored_priv

        pub_b64, priv_b64 = _generate_vapid_keypair()
        self.state.set_kv("vapid_public_key", pub_b64)
        self.state.set_kv("vapid_private_key", priv_b64)
        log.info("pwa.generated_vapid_keypair")
        return pub_b64, priv_b64

    async def notify(
        self,
        *,
        title: str,
        body: str,
        kind: str = "ops",
        thread_id: str | None = None,
        url: str | None = None,
        force: bool = False,
        target_app_mode: str | None = None,
    ) -> dict[str, int]:
        subs = self.state.list_push_subscriptions()
        if not subs:
            return {"sent": 0, "skipped": 0, "pruned": 0, "failed": 0}

        if kind == "food" and not thread_id:
            health_base = self.settings.pwa_health_base_path.rstrip("/") or ""
            default_url = f"{health_base}/"
        else:
            base = self.settings.pwa_base_path.rstrip("/") or ""
            default_url = f"{base}/#thread={thread_id}" if thread_id else f"{base}/"
        target_url = url or default_url
        unread_total = self.state.get_pwa_counts().get("unread_total", 0)
        payload = json.dumps(
            {
                "title": title[:100],
                "body": " ".join((body or "").split())[:220],
                "kind": kind,
                "thread_id": thread_id,
                "url": target_url,
                "tag": thread_id or f"agy-{kind}",
                "unread_total": unread_total,
            }
        )

        sent = 0
        skipped = 0
        pruned = 0
        failed = 0
        subject = self.settings.vapid_subject or "mailto:alerts@lodbrok.app"

        for sub in subs:
            if target_app_mode and sub.app_mode != target_app_mode:
                skipped += 1
                continue
            default_allowed = sub.app_mode == "chat"
            if not force and not sub.prefs.get(kind, default_allowed):
                skipped += 1
                continue

            sub_info = {
                "endpoint": sub.endpoint,
                "keys": {"p256dh": sub.p256dh, "auth": sub.auth},
            }
            try:
                await asyncio.to_thread(
                    self._sender,
                    sub_info,
                    payload,
                    self.private_key,
                    {"sub": subject},
                )
                self.state.touch_push_sent(sub.endpoint)
                sent += 1
            except WebPushException as exc:
                status_code = getattr(getattr(exc, "response", None), "status_code", None)
                if status_code in (404, 410):
                    self.state.delete_push_subscription(sub.endpoint)
                    pruned += 1
                    log.info("pwa.pruned_stale_subscription", status=status_code)
                else:
                    failed += 1
                    log.warning(
                        "pwa.push_failed",
                        status=status_code,
                        error=str(exc),
                    )
            except Exception as exc:  # noqa: BLE001
                failed += 1
                log.warning("pwa.push_error", error=str(exc))

        return {"sent": sent, "skipped": skipped, "pruned": pruned, "failed": failed}
