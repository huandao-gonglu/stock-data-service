from fastapi.testclient import TestClient

from stock_data_service.config import Settings
from stock_data_service.main import create_app


def test_local_mode_can_disable_auth(tmp_path):
    client = TestClient(create_app(Settings(data_root=tmp_path / "data", server_mode=False)))
    assert client.get("/coverage/summary", params={"symbol": "sh600000", "timeframe": "1m"}).status_code == 200


def test_server_mode_separates_query_and_admin_keys(tmp_path):
    settings = Settings(
        data_root=tmp_path / "data",
        server_mode=True,
        data_api_key="data",
        admin_api_key="admin",
        baidu_token_file=tmp_path / "token.json",
    )
    client = TestClient(create_app(settings))
    assert client.get("/coverage/summary", params={"symbol": "sh600000", "timeframe": "1m"}).status_code == 401
    assert (
        client.get(
            "/coverage/summary",
            params={"symbol": "sh600000", "timeframe": "1m"},
            headers={"X-API-Key": "wrong"},
        ).status_code
        == 401
    )
    assert (
        client.get(
            "/coverage/summary",
            params={"symbol": "sh600000", "timeframe": "1m"},
            headers={"X-API-Key": "data"},
        ).status_code
        == 200
    )
    assert client.get("/auth/baidu/status", headers={"X-API-Key": "data"}).status_code == 401
    assert client.get("/auth/baidu/status", headers={"X-API-Key": "wrong"}).status_code == 401
    assert client.get("/auth/baidu/status", headers={"Authorization": "Bearer admin"}).status_code == 200
