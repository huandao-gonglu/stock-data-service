from __future__ import annotations

import datetime as dt
import json
import time
from pathlib import Path
from typing import Any

import requests


class TokenManager:
    def __init__(
        self,
        token_file: str | Path = "baidu_token.json",
        *,
        app_key: str | None = None,
        app_secret: str | None = None,
        session: requests.Session | None = None,
    ):
        self.token_file = Path(token_file)
        self.app_key = app_key
        self.app_secret = app_secret
        self.session = session or requests.Session()
        self._tokens: dict[str, Any] | None = None
        self._load_tokens()

    def get_access_token(self, auto_refresh: bool = True) -> str | None:
        if not self._tokens:
            return None
        if auto_refresh and self.is_expiring():
            if not self.refresh_access_token():
                return None
        return self._tokens.get("access_token")

    def save_tokens(self, tokens: dict[str, Any]) -> None:
        payload = dict(tokens)
        if "expires_in" in payload:
            payload["expires_at"] = time.time() + float(payload["expires_in"])
        self._tokens = payload
        self._save_tokens()

    def refresh_access_token(self) -> bool:
        if not self._tokens or not self._tokens.get("refresh_token"):
            return False
        if not self.app_key or not self.app_secret:
            return False
        old_refresh = self._tokens["refresh_token"]
        response = self.session.get(
            "https://openapi.baidu.com/oauth/2.0/token",
            params={
                "grant_type": "refresh_token",
                "refresh_token": old_refresh,
                "client_id": self.app_key,
                "client_secret": self.app_secret,
            },
            timeout=30,
        )
        response.raise_for_status()
        payload = response.json()
        if "error" in payload:
            return False
        if "refresh_token" not in payload:
            payload["refresh_token"] = old_refresh
        self.save_tokens(payload)
        return True

    def is_expiring(self, window_seconds: int = 300) -> bool:
        if not self._tokens:
            return False
        expires_at = float(self._tokens.get("expires_at", 0))
        return time.time() >= expires_at - window_seconds

    def status(self) -> dict[str, Any]:
        if not self._tokens:
            return {
                "has_token": False,
                "expires_at": None,
                "is_expiring": False,
                "has_refresh_token": False,
            }
        expires_at = self._tokens.get("expires_at")
        iso = None
        if expires_at:
            iso = dt.datetime.fromtimestamp(float(expires_at)).isoformat()
        return {
            "has_token": bool(self._tokens.get("access_token")),
            "expires_at": iso,
            "is_expiring": self.is_expiring(),
            "has_refresh_token": bool(self._tokens.get("refresh_token")),
        }

    def clear_tokens(self) -> None:
        self._tokens = None
        try:
            self.token_file.unlink()
        except FileNotFoundError:
            pass

    def _load_tokens(self) -> None:
        if not self.token_file.exists():
            self._tokens = None
            return
        with self.token_file.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
        if payload and "expires_at" not in payload and "expires_in" in payload:
            payload["expires_at"] = time.time() + float(payload["expires_in"])
            self._tokens = payload
            self._save_tokens()
        else:
            self._tokens = payload

    def _save_tokens(self) -> None:
        self.token_file.parent.mkdir(parents=True, exist_ok=True)
        with self.token_file.open("w", encoding="utf-8") as handle:
            json.dump(self._tokens, handle, ensure_ascii=False, indent=2)
