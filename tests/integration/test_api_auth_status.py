import json
import time

from fastapi.testclient import TestClient

from stock_data_service.config import Settings
from stock_data_service.main import create_app


def test_auth_status_missing_and_valid_token(tmp_path):
    token_file = tmp_path / "token.json"
    client = TestClient(create_app(Settings(data_root=tmp_path / "data", baidu_token_file=token_file)))
    assert client.get("/auth/baidu/status").json()["has_token"] is False
    token_file.write_text(
        json.dumps({"access_token": "a", "refresh_token": "r", "expires_at": time.time() + 60}),
        encoding="utf-8",
    )
    data = client.get("/auth/baidu/status").json()
    assert data["has_token"] is True
    assert data["has_refresh_token"] is True
    assert data["is_expiring"] is True
