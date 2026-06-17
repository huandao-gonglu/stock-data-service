from __future__ import annotations

import io
import zipfile
from pathlib import Path

import pandas as pd
import pytest

from stock_data_service.config import Settings
from stock_data_service.storage.sync_metadata import SyncMetadata


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    return Settings(data_root=tmp_path / "data", baidu_token_file=tmp_path / "baidu_token.json")


@pytest.fixture
def metadata(tmp_path: Path) -> SyncMetadata:
    meta = SyncMetadata(tmp_path / "meta.duckdb")
    meta.initialize()
    return meta


def make_zip(rows: list[dict], member: str = "sh600000.csv", encoding: str = "utf-8-sig") -> bytes:
    df = pd.DataFrame(rows)
    payload = df.to_csv(index=False).encode(encoding)
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(member, payload)
    return buffer.getvalue()


def sample_rows(symbol: str = "sh600000", name: str = "浦发银行") -> list[dict]:
    return [
        {
            "时间": "2024/12/20 09:30",
            "代码": symbol,
            "名称": name,
            "开盘价": 9.57,
            "最高价": 9.57,
            "最低价": 9.57,
            "收盘价": 9.57,
            "成交量": 2042,
            "成交额": 1954194,
            "涨幅": 0.0,
            "振幅": 0.0,
        },
        {
            "时间": "2024/12/20 09:31",
            "代码": symbol,
            "名称": name,
            "开盘价": 9.58,
            "最高价": 9.59,
            "最低价": 9.56,
            "收盘价": 9.57,
            "成交量": 11169,
            "成交额": 10688733,
            "涨幅": 0.0,
            "振幅": 0.31,
        },
    ]
