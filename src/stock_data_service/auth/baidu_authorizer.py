from __future__ import annotations

import base64
import hashlib
import os
import secrets
import threading
import time
import urllib.parse
from dataclasses import dataclass
from typing import Any

import requests

from stock_data_service.auth.token_manager import TokenManager


class BaiduAuthorizationRequired(RuntimeError):
    """Raised when a command requires a manual Baidu authorization flow."""


class BaiduAuthorizationError(RuntimeError):
    """Raised when the Baidu OAuth flow cannot complete."""


@dataclass(frozen=True)
class BaiduOAuthSession:
    state: str
    code_verifier: str
    redirect_uri: str
    created_at: float


class BaiduOAuthStateStore:
    def __init__(self, ttl_seconds: int = 600):
        self.ttl_seconds = ttl_seconds
        self._states: dict[str, BaiduOAuthSession] = {}
        self._lock = threading.Lock()

    def create(self, *, redirect_uri: str, code_verifier: str) -> BaiduOAuthSession:
        session = BaiduOAuthSession(
            state=secrets.token_urlsafe(24),
            code_verifier=code_verifier,
            redirect_uri=redirect_uri,
            created_at=time.time(),
        )
        with self._lock:
            self._prune_locked()
            self._states[session.state] = session
        return session

    def pop(self, state: str) -> BaiduOAuthSession | None:
        with self._lock:
            self._prune_locked()
            return self._states.pop(state, None)

    def _prune_locked(self) -> None:
        cutoff = time.time() - self.ttl_seconds
        expired = [state for state, session in self._states.items() if session.created_at < cutoff]
        for state in expired:
            self._states.pop(state, None)


class BaiduWebAuthorizer:
    def __init__(
        self,
        *,
        app_key: str | None,
        app_secret: str | None,
        token_file: str,
        scope: str = "basic,netdisk",
        state_store: BaiduOAuthStateStore | None = None,
        session: requests.Session | None = None,
    ):
        self.app_key = app_key
        self.app_secret = app_secret
        self.token_file = token_file
        self.scope = scope
        self.state_store = state_store or BaiduOAuthStateStore()
        self.session = session or requests.Session()

    def authorization_url(self, redirect_uri: str) -> dict[str, str]:
        self._require_config()
        code_verifier = _b64url(os.urandom(32))
        code_challenge = _b64url(hashlib.sha256(code_verifier.encode()).digest())
        oauth_session = self.state_store.create(redirect_uri=redirect_uri, code_verifier=code_verifier)
        params = {
            "response_type": "code",
            "client_id": self.app_key,
            "redirect_uri": redirect_uri,
            "scope": self.scope,
            "state": oauth_session.state,
            "code_challenge": code_challenge,
            "code_challenge_method": "S256",
        }
        return {
            "authorize_url": "https://openapi.baidu.com/oauth/2.0/authorize?" + urllib.parse.urlencode(params),
            "redirect_uri": redirect_uri,
            "state": oauth_session.state,
        }

    def exchange_code(self, *, code: str, state: str) -> dict[str, Any]:
        self._require_config()
        oauth_session = self.state_store.pop(state)
        if oauth_session is None:
            raise BaiduAuthorizationError("授权会话已失效，请重新发起授权")

        response = self.session.post(
            "https://openapi.baidu.com/oauth/2.0/token",
            data={
                "grant_type": "authorization_code",
                "code": code,
                "client_id": self.app_key,
                "client_secret": self.app_secret,
                "redirect_uri": oauth_session.redirect_uri,
                "code_verifier": oauth_session.code_verifier,
            },
            headers={"User-Agent": "pan.baidu.com"},
            timeout=30,
        )
        response.raise_for_status()
        tokens = response.json()
        if "error" in tokens:
            message = tokens.get("error_description") or tokens.get("error") or "百度授权失败"
            raise BaiduAuthorizationError(str(message))
        TokenManager(
            token_file=self.token_file,
            app_key=self.app_key,
            app_secret=self.app_secret,
            session=self.session,
        ).save_tokens(tokens)
        return tokens

    def _require_config(self) -> None:
        if not self.app_key or not self.app_secret:
            raise BaiduAuthorizationError("缺少 BAIDU_APP_KEY 或 BAIDU_APP_SECRET")


class BaiduDesktopAuthorizer:
    """Placeholder for a future interactive authorization flow.

    The MVP deliberately keeps sync as CLI/cron and unit tests free of desktop
    authorization side effects.
    """

    def authorize(self) -> dict:
        raise BaiduAuthorizationRequired("interactive Baidu authorization is not implemented in the MVP")


def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()
