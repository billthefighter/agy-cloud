from __future__ import annotations

import hashlib
import hmac
import secrets
import time
from urllib.parse import urlparse

import structlog
from fastapi import HTTPException, Request, WebSocket

from agent_tasks.common.config import ControllerSettings
from agent_tasks.common.state import StateStore

log = structlog.get_logger(__name__)

SESSION_COOKIE_NAME = "agy_pwa_session"
SESSION_MAX_AGE_SECONDS = 365 * 24 * 3600
_MUTATING_METHODS = frozenset({"POST", "PUT", "PATCH", "DELETE"})


class PwaAuthManager:
    """Single-user token & HMAC-SHA256 HttpOnly session cookie auth for the iOS PWA."""

    def __init__(self, *, settings: ControllerSettings, state: StateStore) -> None:
        self.settings = settings
        self.state = state
        self.api_token = self._resolve_api_token()
        self._signing_key = hashlib.sha256(
            b"agy-pwa-session-v1:" + self.api_token.encode("utf-8")
        ).digest()

    def _resolve_api_token(self) -> str:
        configured = (self.settings.pwa_api_token or "").strip()
        if configured:
            return configured
        stored = (self.state.get_kv("pwa_api_token_auto") or "").strip()
        if stored:
            return stored
        generated = secrets.token_urlsafe(32)
        self.state.set_kv("pwa_api_token_auto", generated)
        log.warning(
            "pwa.auto_generated_api_token",
            hint="Set PWA_API_TOKEN in environment/secret for deterministic cluster pairing.",
        )
        return generated

    @property
    def cookie_path(self) -> str:
        # Root path so one session cookie authenticates both /agy and /health PWAs.
        return "/"

    def verify_token(self, candidate: str | None) -> bool:
        clean = (candidate or "").strip()
        if not clean:
            return False
        return hmac.compare_digest(clean.encode("utf-8"), self.api_token.encode("utf-8"))

    def issue_session_cookie(self, *, now: int | None = None) -> str:
        ts = int(time.time()) if now is None else now
        nonce = secrets.token_hex(8)
        payload = f"{ts}.{nonce}"
        sig = hmac.new(self._signing_key, payload.encode("utf-8"), hashlib.sha256).hexdigest()
        return f"{payload}.{sig}"

    def verify_session_cookie(self, value: str | None, *, now: int | None = None) -> bool:
        if not value:
            return False
        parts = value.split(".")
        if len(parts) != 3:
            return False
        ts_raw, nonce, sig = parts
        if not ts_raw.isdigit() or not nonce or not sig:
            return False
        issued_at = int(ts_raw)
        current = int(time.time()) if now is None else now
        if issued_at > current + 300 or current - issued_at > SESSION_MAX_AGE_SECONDS:
            return False
        payload = f"{ts_raw}.{nonce}"
        expected = hmac.new(self._signing_key, payload.encode("utf-8"), hashlib.sha256).hexdigest()
        return hmac.compare_digest(expected, sig)

    @staticmethod
    def is_https(request: Request) -> bool:
        proto = request.headers.get("x-forwarded-proto", "").split(",")[0].strip().lower()
        if proto:
            return proto == "https"
        return request.url.scheme == "https"

    def is_authenticated(self, request: Request) -> bool:
        auth_header = request.headers.get("authorization", "")
        if auth_header.lower().startswith("bearer "):
            token = auth_header[7:].strip()
            if self.verify_token(token):
                return True
        cookie_val = request.cookies.get(SESSION_COOKIE_NAME)
        return self.verify_session_cookie(cookie_val)

    def require_http_auth(self, request: Request) -> None:
        auth_header = request.headers.get("authorization", "")
        if auth_header.lower().startswith("bearer "):
            token = auth_header[7:].strip()
            if self.verify_token(token):
                return

        cookie_val = request.cookies.get(SESSION_COOKIE_NAME)
        if self.verify_session_cookie(cookie_val):
            if request.method.upper() in _MUTATING_METHODS:
                client_hdr = request.headers.get("x-agy-client", "").strip().lower()
                if client_hdr != "pwa":
                    raise HTTPException(
                        status_code=403,
                        detail="Missing X-AGY-Client header on cookie-authenticated mutation.",
                    )
            return

        raise HTTPException(status_code=401, detail="Authentication required.")

    def verify_websocket(self, websocket: WebSocket) -> bool:
        origin = websocket.headers.get("origin", "").strip()
        host = websocket.headers.get("host", "").strip()
        if origin and host:
            origin_host = urlparse(origin).netloc.lower()
            if origin_host and origin_host != host.lower():
                log.warning("pwa.ws_origin_mismatch", origin=origin, host=host)
                return False

        token_param = websocket.query_params.get("token", "")
        if self.verify_token(token_param):
            return True

        cookie_val = websocket.cookies.get(SESSION_COOKIE_NAME)
        return self.verify_session_cookie(cookie_val)
