import datetime as dt

import pandas as pd

from stock_data_service.market.normalizer import DAILY_COLUMNS, INTRADAY_COLUMNS
from stock_data_service.market.timeframe import Timeframe
from stock_data_service.storage.parquet_writer import ParquetBarWriter


def _bars(close=9.57, ts=dt.datetime(2024, 12, 20, 9, 30), name="test-name"):
    return pd.DataFrame(
        [
            {
                "symbol": "sh600000",
                "code": "sh600000",
                "name": name,
                "ts": ts,
                "open": 9.57,
                "high": 9.57,
                "low": 9.57,
                "close": close,
                "volume": 1,
                "amount": 2,
                "source": "test",
                "source_path": "/x.zip",
                "ingested_at": dt.datetime(2024, 12, 20),
                "quality_flag": "ok",
                "unexpected": "drop me",
            }
        ]
    )


def test_writes_symbol_month_partition_and_deduplicates(tmp_path):
    writer = ParquetBarWriter(tmp_path / "parquet")
    [path] = writer.write_bars(_bars(close=1), Timeframe.M1)
    writer.write_bars(_bars(close=2), Timeframe.M1)
    assert path == tmp_path / "parquet/bars_1m/symbol=sh600000/year=2024/month=12/data.parquet"
    df = pd.read_parquet(path)
    assert len(df) == 1
    assert df.iloc[0]["close"] == 2
    assert not list(path.parent.glob("*.tmp"))


def test_appends_another_day_for_same_symbol_month_and_keeps_canonical_columns(tmp_path):
    writer = ParquetBarWriter(tmp_path / "parquet")
    [path] = writer.write_bars(_bars(close=1, ts=dt.datetime(2024, 12, 20, 9, 30)), Timeframe.M1)
    writer.write_bars(_bars(close=2, ts=dt.datetime(2024, 12, 23, 9, 30)), Timeframe.M1)
    df = pd.read_parquet(path)
    assert len(df) == 2
    assert list(df.columns) == INTRADAY_COLUMNS
    assert list(df["close"]) == [1, 2]


def test_merges_existing_partition_when_new_name_is_numeric(tmp_path):
    writer = ParquetBarWriter(tmp_path / "parquet")
    [path] = writer.write_bars(_bars(name="name-text"), Timeframe.M1)
    writer.write_bars(_bars(name=600000, ts=dt.datetime(2024, 12, 20, 9, 31)), Timeframe.M1)

    df = pd.read_parquet(path)

    assert len(df) == 2
    assert list(df["name"]) == ["name-text", "600000"]


def test_new_partition_fast_path_does_not_read_existing_parquet(tmp_path, monkeypatch):
    def fail_read_parquet(*args, **kwargs):
        raise AssertionError("new partition should not read parquet")

    monkeypatch.setattr(pd, "read_parquet", fail_read_parquet)

    writer = ParquetBarWriter(tmp_path / "parquet")
    [path] = writer.write_bars(_bars(close=1), Timeframe.M1)

    assert path.exists()
    assert not list(path.parent.glob("*.tmp"))


def test_writes_daily_partition_with_quality_columns(tmp_path):
    writer = ParquetBarWriter(tmp_path / "parquet")
    daily = pd.DataFrame(
        [
            {
                "symbol": "sh600000",
                "trade_date": dt.date(2024, 12, 20),
                "open": 1,
                "high": 2,
                "low": 1,
                "close": 2,
                "volume": 10,
                "amount": 20,
                "bar_count": 240,
                "expected_bar_count": 240,
                "is_complete": True,
                "quality_flag": "ok",
            }
        ],
        columns=DAILY_COLUMNS,
    )
    [path] = writer.write_bars(daily, Timeframe.D1)
    df = pd.read_parquet(path)
    assert path == tmp_path / "parquet/bars_1d/symbol=sh600000/year=2024/month=12/data.parquet"
    assert {"bar_count", "expected_bar_count", "is_complete", "quality_flag"}.issubset(df.columns)
    assert df.iloc[0]["is_complete"] is True or bool(df.iloc[0]["is_complete"]) is True
