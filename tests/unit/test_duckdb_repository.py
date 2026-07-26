import datetime as dt

import pandas as pd

from stock_data_service.market.timeframe import Timeframe
from stock_data_service.storage.duckdb_repository import DuckDBRepository
from stock_data_service.storage.parquet_writer import ParquetBarWriter


def test_reads_bars_half_open_ordered_and_missing_symbol(tmp_path, metadata):
    writer = ParquetBarWriter(tmp_path / "parquet")
    writer.write_bars(
        pd.DataFrame(
            [
                {"symbol": "sh600000", "ts": dt.datetime(2024, 12, 20, 9, 31), "open": 2, "high": 2, "low": 2, "close": 2},
                {"symbol": "sh600000", "ts": dt.datetime(2024, 12, 20, 9, 30), "open": 1, "high": 1, "low": 1, "close": 1},
            ]
        ),
        Timeframe.M1,
    )
    repo = DuckDBRepository(tmp_path / "parquet", metadata.db_path)
    df = repo.query_bars(
        symbol="sh600000",
        timeframe=Timeframe.M1,
        start=dt.datetime(2024, 12, 20, 9, 30),
        end=dt.datetime(2024, 12, 20, 9, 31),
    )
    assert list(df["close"]) == [1]
    assert repo.query_bars(
        symbol="sz000001",
        timeframe=Timeframe.M1,
        start=dt.datetime(2024, 12, 20, 9, 30),
        end=dt.datetime(2024, 12, 20, 9, 31),
    ).empty


def test_coverage_summary_and_gaps(tmp_path, metadata):
    metadata.update_coverage_daily(
        symbol="sh600000",
        timeframe="1m",
        trade_date=dt.date(2024, 12, 20),
        start_ts=dt.datetime(2024, 12, 20, 9, 30),
        end_ts=dt.datetime(2024, 12, 20, 15, 0),
        row_count=240,
        expected_row_count=240,
        is_complete=True,
        quality_flag="ok",
    )
    metadata.update_coverage_daily(
        symbol="sh600000",
        timeframe="1m",
        trade_date=dt.date(2024, 12, 23),
        start_ts=dt.datetime(2024, 12, 23, 9, 30),
        end_ts=dt.datetime(2024, 12, 23, 11, 0),
        row_count=90,
        expected_row_count=240,
        is_complete=False,
        quality_flag="partial",
    )
    repo = DuckDBRepository(tmp_path / "parquet", metadata.db_path)
    summary = repo.coverage_summary(symbol="sh600000", timeframe=Timeframe.M1)
    assert summary["complete"] is False
    assert summary["complete_trade_dates"] == 1
    assert summary["partial_trade_dates"] == 1
    gaps = repo.coverage_gaps(
        symbol="sh600000", timeframe=Timeframe.M1, start=dt.date(2024, 12, 20), end=dt.date(2024, 12, 24)
    )
    assert gaps["missing_trade_dates"] == ["2024-12-24"]
    assert gaps["partial_dates"][0]["date"] == "2024-12-23"


def test_queries_daily_bars_by_trade_date_and_resists_symbol_injection(tmp_path, metadata):
    writer = ParquetBarWriter(tmp_path / "parquet")
    writer.write_bars(
        pd.DataFrame(
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
                },
                {
                    "symbol": "sh600000",
                    "trade_date": dt.date(2024, 12, 23),
                    "open": 2,
                    "high": 3,
                    "low": 2,
                    "close": 3,
                    "volume": 30,
                    "amount": 40,
                    "bar_count": 240,
                    "expected_bar_count": 240,
                    "is_complete": True,
                    "quality_flag": "ok",
                },
            ]
        ),
        Timeframe.D1,
    )
    repo = DuckDBRepository(tmp_path / "parquet", metadata.db_path)
    df = repo.query_bars(
        symbol="sh600000",
        timeframe=Timeframe.D1,
        start=dt.datetime(2024, 12, 20),
        end=dt.datetime(2024, 12, 23),
    )
    assert list(df["close"]) == [2]

    injected = repo.query_bars(
        symbol="sh600000' OR 1=1 --",
        timeframe=Timeframe.D1,
        start=dt.datetime(2024, 12, 20),
        end=dt.datetime(2024, 12, 24),
    )
    assert injected.empty


def test_queries_daily_bars_by_aggregating_minute_bars_when_daily_partition_missing(tmp_path, metadata):
    writer = ParquetBarWriter(tmp_path / "parquet")
    writer.write_bars(
        pd.DataFrame(
            [
                {
                    "symbol": "sh600000",
                    "code": "sh600000",
                    "name": "浦发银行",
                    "ts": dt.datetime(2024, 12, 20, 9, 30),
                    "open": 10,
                    "high": 11,
                    "low": 9,
                    "close": 10.5,
                    "volume": 100,
                    "amount": 1000,
                },
                {
                    "symbol": "sh600000",
                    "code": "sh600000",
                    "name": "浦发银行",
                    "ts": dt.datetime(2024, 12, 20, 9, 31),
                    "open": 10.5,
                    "high": 12,
                    "low": 10,
                    "close": 11.5,
                    "volume": 200,
                    "amount": 2500,
                },
                {
                    "symbol": "sh600000",
                    "code": "sh600000",
                    "name": "浦发银行",
                    "ts": dt.datetime(2024, 12, 23, 9, 30),
                    "open": 12,
                    "high": 13,
                    "low": 11,
                    "close": 12.5,
                    "volume": 300,
                    "amount": 3600,
                },
            ]
        ),
        Timeframe.M1,
    )
    repo = DuckDBRepository(tmp_path / "parquet", metadata.db_path)

    df = repo.query_bars(
        symbol="sh600000",
        timeframe=Timeframe.D1,
        start=dt.datetime(2024, 12, 20),
        end=dt.datetime(2024, 12, 24),
    )

    assert [value.date() if hasattr(value, "date") else value for value in df["trade_date"]] == [
        dt.date(2024, 12, 20),
        dt.date(2024, 12, 23),
    ]
    assert list(df["open"]) == [10, 12]
    assert list(df["high"]) == [12, 13]
    assert list(df["low"]) == [9, 11]
    assert list(df["close"]) == [11.5, 12.5]
    assert list(df["volume"]) == [300, 300]
    assert list(df["amount"]) == [3500, 3600]
