import datetime as dt

import pandas as pd
from fastapi.testclient import TestClient

from stock_data_service.config import Settings
from stock_data_service.main import create_app
from stock_data_service.market.timeframe import Timeframe
from stock_data_service.storage.parquet_writer import ParquetBarWriter
from stock_data_service.storage.sync_metadata import SyncMetadata


def _client_with_data(tmp_path):
    settings = Settings(data_root=tmp_path / "data")
    SyncMetadata(settings.metadata_db).initialize()
    ParquetBarWriter(settings.parquet_root).write_bars(
        pd.DataFrame(
            [
                {"symbol": "sh600000", "ts": dt.datetime(2024, 12, 20, 9, 30), "open": 1, "high": 1, "low": 1, "close": 1},
                {"symbol": "sh600000", "ts": dt.datetime(2024, 12, 20, 9, 31), "open": 2, "high": 2, "low": 2, "close": 2},
            ]
        ),
        Timeframe.M1,
    )
    meta = SyncMetadata(settings.metadata_db)
    meta.update_coverage_daily(
        symbol="sh600000",
        timeframe="1m",
        trade_date=dt.date(2024, 12, 20),
        start_ts=dt.datetime(2024, 12, 20, 9, 30),
        end_ts=dt.datetime(2024, 12, 20, 9, 32),
        row_count=2,
        expected_row_count=240,
        is_complete=False,
        quality_flag="partial",
    )
    return TestClient(create_app(settings))


def test_health_and_bars_endpoint(tmp_path):
    client = _client_with_data(tmp_path)
    assert client.get("/health").json() == {"status": "ok"}
    log_file = tmp_path / "data" / "logs" / f"{dt.date.today().isoformat()}.log"
    assert "request completed method=GET path=/health status_code=200" in log_file.read_text(encoding="utf-8")
    response = client.get(
        "/bars",
        params={
            "symbol": "600000.SH",
            "timeframe": "1m",
            "start": "2024-12-20T09:30:00+08:00",
            "end": "2024-12-20T09:32:00+08:00",
        },
    )
    assert response.status_code == 200
    data = response.json()
    assert data["rows"][0]["ts"] == "2024-12-20T09:30:00+08:00"
    assert len(data["rows"]) == 2


def test_bars_validation_range_and_pagination(tmp_path):
    client = _client_with_data(tmp_path)
    assert client.get("/bars", params={"symbol": "000300", "timeframe": "1m", "start": "x", "end": "y"}).status_code == 400
    assert client.get("/bars", params={"timeframe": "1m", "start": "x", "end": "y"}).status_code == 422
    assert (
        client.get(
            "/bars",
            params={
                "symbol": "123456",
                "timeframe": "1m",
                "start": "2024-12-20T09:30:00+08:00",
                "end": "2024-12-20T09:32:00+08:00",
            },
        ).status_code
        == 400
    )
    too_large = client.get(
        "/bars",
        params={
            "symbol": "sh600000",
            "timeframe": "1m",
            "start": "2024-01-01T00:00:00+08:00",
            "end": "2024-03-01T00:00:00+08:00",
        },
    )
    assert too_large.status_code == 400
    paged = client.get(
        "/bars",
        params={
            "symbol": "sh600000",
            "timeframe": "1m",
            "start": "2024-12-20T09:30:00+08:00",
            "end": "2024-12-20T09:32:00+08:00",
            "limit": 1,
        },
    ).json()
    assert paged["next_cursor"] == "1"


def test_coverage_endpoints(tmp_path):
    client = _client_with_data(tmp_path)
    summary = client.get("/coverage/summary", params={"symbol": "sh600000", "timeframe": "1m"}).json()
    assert summary["partial_trade_dates"] == 1
    gaps = client.get(
        "/coverage/gaps",
        params={"symbol": "sh600000", "timeframe": "1m", "start": "2024-12-20", "end": "2024-12-20"},
    ).json()
    assert gaps["partial_dates"][0]["date"] == "2024-12-20"


def test_empty_coverage_returns_null_boundaries(tmp_path):
    client = TestClient(create_app(Settings(data_root=tmp_path / "data")))
    data = client.get("/coverage/summary", params={"symbol": "sh600000", "timeframe": "1m"}).json()
    assert data["start"] is None
    assert data["end"] is None
    assert data["complete"] is False
