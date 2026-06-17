import json
import time

from stock_data_service.auth.token_manager import TokenManager


class FakeResponse:
    def __init__(self, payload):
        self.payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self.payload


class FakeSession:
    def __init__(self, payload):
        self.payload = payload
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return FakeResponse(self.payload)


def test_loads_legacy_token_and_adds_expires_at(tmp_path):
    token_file = tmp_path / "token.json"
    token_file.write_text(json.dumps({"access_token": "a", "expires_in": 3600}), encoding="utf-8")
    manager = TokenManager(token_file)
    assert manager.get_access_token(auto_refresh=False) == "a"
    assert "expires_at" in json.loads(token_file.read_text(encoding="utf-8"))


def test_refreshes_when_expiring_and_preserves_refresh_token(tmp_path):
    token_file = tmp_path / "token.json"
    token_file.write_text(
        json.dumps({"access_token": "old", "refresh_token": "r", "expires_at": time.time() + 1}),
        encoding="utf-8",
    )
    session = FakeSession({"access_token": "new", "expires_in": 3600})
    manager = TokenManager(token_file, app_key="k", app_secret="s", session=session)
    assert manager.get_access_token() == "new"
    saved = json.loads(token_file.read_text(encoding="utf-8"))
    assert saved["refresh_token"] == "r"


def test_missing_status_and_clear(tmp_path):
    token_file = tmp_path / "token.json"
    manager = TokenManager(token_file)
    assert manager.status()["has_token"] is False
    manager.save_tokens({"access_token": "a", "refresh_token": "r", "expires_in": 3600})
    manager.clear_tokens()
    assert not token_file.exists()
