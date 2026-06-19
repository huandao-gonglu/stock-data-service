import datetime as dt
import zipfile

from conftest import make_zip, sample_rows
from stock_data_service.market.timeframe import Timeframe
from stock_data_service.storage.duckdb_repository import DuckDBRepository
from stock_data_service.storage.sync_metadata import SyncMetadata
from stock_data_service.sync.job_runner import LocalSyncJobRunner


def test_local_sync_job_ingests_and_is_idempotent(tmp_path):
    raw = tmp_path / "raw/A股_分时数据/1分钟"
    raw.mkdir(parents=True)
    (raw / "20241220_1min.zip").write_bytes(make_zip(sample_rows()))
    metadata = SyncMetadata(tmp_path / "meta.duckdb")
    runner = LocalSyncJobRunner(raw_root=tmp_path / "raw", parquet_root=tmp_path / "parquet", metadata=metadata)
    result = runner.run(
        timeframe=Timeframe.M1,
        start=dt.date(2024, 12, 20),
        end=dt.date(2024, 12, 20),
        symbols=["sh600000"],
    )
    assert result.scanned_count == 1
    assert result.ingested_count == 1
    runner.run(timeframe=Timeframe.M1, start=dt.date(2024, 12, 20), end=dt.date(2024, 12, 20), symbols=["600000"])
    repo = DuckDBRepository(tmp_path / "parquet", metadata.db_path)
    df = repo.query_bars(
        symbol="sh600000",
        timeframe=Timeframe.M1,
        start=dt.datetime(2024, 12, 20, 9, 30),
        end=dt.datetime(2024, 12, 20, 9, 32),
    )
    assert len(df) == 2
    status = metadata.fetchone("SELECT status FROM file_ingests WHERE symbol='sh600000'")[0]
    assert status == "committed"


def test_local_sync_job_can_ingest_another_symbol_and_changed_content(tmp_path):
    raw = tmp_path / "raw/A股_分时数据/1分钟"
    raw.mkdir(parents=True)
    zip_path = raw / "20241220_1min.zip"
    zip_path.write_bytes(_zip_with_symbols(close=1))
    metadata = SyncMetadata(tmp_path / "meta.duckdb")
    runner = LocalSyncJobRunner(raw_root=tmp_path / "raw", parquet_root=tmp_path / "parquet", metadata=metadata)
    runner.run(timeframe=Timeframe.M1, start=dt.date(2024, 12, 20), end=dt.date(2024, 12, 20), symbols=["sh600000"])
    runner.run(timeframe=Timeframe.M1, start=dt.date(2024, 12, 20), end=dt.date(2024, 12, 20), symbols=["sz000001"])

    rows = metadata.fetchall("SELECT symbol, status FROM file_ingests ORDER BY symbol")
    assert rows == [("sh600000", "committed"), ("sz000001", "committed")]

    zip_path.write_bytes(_zip_with_symbols(close=8))
    runner.run(timeframe=Timeframe.M1, start=dt.date(2024, 12, 20), end=dt.date(2024, 12, 20), symbols=["sh600000"])
    repo = DuckDBRepository(tmp_path / "parquet", metadata.db_path)
    df = repo.query_bars(
        symbol="sh600000",
        timeframe=Timeframe.M1,
        start=dt.datetime(2024, 12, 20, 9, 30),
        end=dt.datetime(2024, 12, 20, 9, 31),
    )
    assert len(df) == 1
    assert df.iloc[0]["close"] == 8


def test_local_sync_job_ingests_multiple_symbols_from_one_archive(tmp_path):
    raw = tmp_path / "raw/A鑲鍒嗘椂鏁版嵁/1鍒嗛挓"
    raw.mkdir(parents=True)
    (raw / "20241220_1min.zip").write_bytes(_zip_with_symbols(close=1))
    metadata = SyncMetadata(tmp_path / "meta.duckdb")
    result = LocalSyncJobRunner(raw_root=tmp_path / "raw", parquet_root=tmp_path / "parquet", metadata=metadata).run(
        timeframe=Timeframe.M1,
        start=dt.date(2024, 12, 20),
        end=dt.date(2024, 12, 20),
        symbols=["sh600000", "sz000001", "sh600004"],
    )

    assert result.scanned_count == 1
    assert result.ingested_count == 2
    assert result.failed_count == 0
    rows = metadata.fetchall("SELECT symbol, status FROM file_ingests ORDER BY symbol")
    assert rows == [
        ("sh600000", "committed"),
        ("sh600004", "symbol_missing"),
        ("sz000001", "committed"),
    ]


def test_local_sync_parallel_archive_ingest_is_idempotent(tmp_path):
    raw = tmp_path / "raw/A股_分时数据/1分钟"
    raw.mkdir(parents=True)
    (raw / "20241220_1min.zip").write_bytes(_zip_with_symbols(close=1))
    metadata = SyncMetadata(tmp_path / "meta.duckdb")
    runner = LocalSyncJobRunner(raw_root=tmp_path / "raw", parquet_root=tmp_path / "parquet", metadata=metadata)

    for _ in range(2):
        result = runner.run(
            timeframe=Timeframe.M1,
            start=dt.date(2024, 12, 20),
            end=dt.date(2024, 12, 20),
            symbols=["sh600000", "sz000001"],
        )
        assert result.ingested_count == 2
        assert result.failed_count == 0

    repo = DuckDBRepository(tmp_path / "parquet", metadata.db_path)
    for symbol in ["sh600000", "sz000001"]:
        df = repo.query_bars(
            symbol=symbol,
            timeframe=Timeframe.M1,
            start=dt.datetime(2024, 12, 20, 9, 30),
            end=dt.datetime(2024, 12, 20, 9, 31),
        )
        assert len(df) == 1
    assert metadata.fetchall("SELECT symbol, status FROM file_ingests ORDER BY symbol") == [
        ("sh600000", "committed"),
        ("sz000001", "committed"),
    ]


def test_local_sync_parallel_archive_keeps_success_when_one_symbol_fails(tmp_path):
    raw = tmp_path / "raw/A股_分时数据/1分钟"
    raw.mkdir(parents=True)
    (raw / "20241220_1min.zip").write_bytes(_zip_with_bad_symbol())
    metadata = SyncMetadata(tmp_path / "meta.duckdb")

    result = LocalSyncJobRunner(raw_root=tmp_path / "raw", parquet_root=tmp_path / "parquet", metadata=metadata).run(
        timeframe=Timeframe.M1,
        start=dt.date(2024, 12, 20),
        end=dt.date(2024, 12, 20),
        symbols=["sh600000", "sz000001"],
    )

    assert result.ingested_count == 1
    assert result.failed_count == 1
    assert metadata.fetchall("SELECT symbol, status FROM file_ingests ORDER BY symbol") == [
        ("sh600000", "committed"),
        ("sz000001", "parse_failed"),
    ]


def test_local_sync_missing_symbol_in_one_archive_does_not_skip_later_archive(tmp_path):
    raw = tmp_path / "raw/A股K线分时数据/1分钟"
    raw.mkdir(parents=True)
    (raw / "20241220_1min.zip").write_bytes(make_zip(sample_rows(symbol="sz000001"), member="sz000001.csv"))
    (raw / "20241223_1min.zip").write_bytes(make_zip(sample_rows(symbol="sh600000"), member="sh600000.csv"))
    metadata = SyncMetadata(tmp_path / "meta.duckdb")

    result = LocalSyncJobRunner(raw_root=tmp_path / "raw", parquet_root=tmp_path / "parquet", metadata=metadata).run(
        timeframe=Timeframe.M1,
        start=dt.date(2024, 12, 20),
        end=dt.date(2024, 12, 23),
        symbols=["sh600000"],
    )

    assert result.ingested_count == 1
    assert result.failed_count == 0
    assert metadata.fetchall("SELECT remote_path, symbol, status FROM file_ingests ORDER BY remote_path") == [
        ("/A股K线分时数据/1分钟/20241220_1min.zip", "sh600000", "symbol_missing"),
        ("/A股K线分时数据/1分钟/20241223_1min.zip", "sh600000", "committed"),
    ]


def test_sync_job_continues_on_corrupted_zip(tmp_path):
    raw = tmp_path / "raw/A股_分时数据/1分钟"
    raw.mkdir(parents=True)
    (raw / "20241220_1min.zip").write_bytes(make_zip(sample_rows()))
    (raw / "20241223_1min.zip").write_bytes(b"bad zip")
    metadata = SyncMetadata(tmp_path / "meta.duckdb")
    result = LocalSyncJobRunner(raw_root=tmp_path / "raw", parquet_root=tmp_path / "parquet", metadata=metadata).run(
        timeframe=Timeframe.M1,
        start=dt.date(2024, 12, 20),
        end=dt.date(2024, 12, 23),
        symbols=["sh600000"],
    )
    assert result.scanned_count == 2
    assert result.ingested_count == 1
    assert metadata.fetchone("SELECT status FROM file_ingests WHERE remote_path LIKE '%20241223%'")[0] == "corrupted_zip"


def _zip_with_symbols(close: float) -> bytes:
    import io
    import pandas as pd

    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        for symbol, name in [("sh600000", "浦发银行"), ("sz000001", "平安银行")]:
            rows = sample_rows(symbol=symbol, name=name)
            rows[0]["收盘价"] = close
            archive.writestr(f"{symbol}.csv", pd.DataFrame(rows[:1]).to_csv(index=False).encode("utf-8-sig"))
    return buffer.getvalue()


def _zip_with_bad_symbol() -> bytes:
    import io
    import pandas as pd

    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(
            "sh600000.csv",
            pd.DataFrame(sample_rows(symbol="sh600000")).to_csv(index=False).encode("utf-8-sig"),
        )
        archive.writestr("sz000001.csv", "not,a,valid\n1,2")
    return buffer.getvalue()
