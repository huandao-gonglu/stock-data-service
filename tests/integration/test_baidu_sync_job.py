import datetime as dt
import io
import threading
import zipfile
from pathlib import Path

import pandas as pd
import pytest

from conftest import make_zip, sample_rows
from stock_data_service.baidu.pan_client import BaiduFileMeta
from stock_data_service.market.path_strategy import BaiduStockKPathStrategy
from stock_data_service.market.timeframe import Timeframe
from stock_data_service.storage.duckdb_repository import DuckDBRepository
from stock_data_service.storage.sync_metadata import SyncMetadata
from stock_data_service.sync.job_runner import BaiduSyncJobRunner, SyncCancelled


class FakeBaiduClient:
    def __init__(self, files: dict[str, bytes]):
        self.files = files
        self.downloaded: list[str] = []

    def get_file_meta_by_path(self, path: str):
        if path not in self.files:
            return None
        return BaiduFileMeta(remote_path=path, fs_id=1, size=len(self.files[path]), md5="md5", server_mtime=1734652800)

    def download_by_path(self, path: str, dest_path: str | Path, *, progress_callback=None, cancel_event=None):
        self.downloaded.append(path)
        dest = Path(dest_path)
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(self.files[path])
        if progress_callback:
            progress_callback(
                {
                    "bytes_downloaded": len(self.files[path]),
                    "total_bytes": len(self.files[path]),
                    "speed_bytes_per_sec": 1024,
                }
            )
        return str(dest)


class CancellingBaiduClient(FakeBaiduClient):
    def download_by_path(self, path: str, dest_path: str | Path, *, progress_callback=None, cancel_event=None):
        if cancel_event:
            cancel_event.set()
        raise RuntimeError("download cancelled")


def test_baidu_sync_downloads_records_metadata_and_ingests(tmp_path):
    remote_path = "/A股_分时数据/1分钟/20241220_1min.zip"
    metadata = SyncMetadata(tmp_path / "meta.duckdb")
    runner = BaiduSyncJobRunner(
        client=FakeBaiduClient({remote_path: make_zip(sample_rows())}),
        cache_dir=tmp_path / "raw" / "baidu",
        parquet_root=tmp_path / "parquet",
        metadata=metadata,
    )
    result = runner.run(
        timeframe=Timeframe.M1,
        start=dt.date(2024, 12, 20),
        end=dt.date(2024, 12, 20),
        symbols=["600000.SH"],
    )
    assert result.scanned_count == 1
    assert result.downloaded_count == 1
    assert result.ingested_count == 1
    assert result.failed_count == 0
    assert metadata.fetchone("SELECT type FROM upstream_sources WHERE id = 'baidu-main'")[0] == "baidu_netdisk"
    assert metadata.fetchone("SELECT status FROM remote_files WHERE remote_path = ?", [remote_path])[0] == "downloaded"
    assert metadata.fetchone("SELECT status FROM file_ingests WHERE remote_path = ? AND symbol = 'sh600000'", [remote_path])[0] == "committed"

    repo = DuckDBRepository(tmp_path / "parquet", metadata.db_path)
    df = repo.query_bars(
        symbol="sh600000",
        timeframe=Timeframe.M1,
        start=dt.datetime(2024, 12, 20, 9, 30),
        end=dt.datetime(2024, 12, 20, 9, 32),
    )
    assert len(df) == 2


def test_baidu_sync_reports_live_progress_counts(tmp_path):
    remote_path = BaiduStockKPathStrategy().candidates(Timeframe.M1, dt.date(2024, 12, 20))[0].remote_path
    progress = []
    result = BaiduSyncJobRunner(
        client=FakeBaiduClient({remote_path: make_zip(sample_rows())}),
        cache_dir=tmp_path / "raw" / "baidu",
        parquet_root=tmp_path / "parquet",
        metadata=SyncMetadata(tmp_path / "meta.duckdb"),
    ).run(
        timeframe=Timeframe.M1,
        start=dt.date(2024, 12, 20),
        end=dt.date(2024, 12, 20),
        symbols=["600000.SH"],
        progress_callback=lambda stage, percent, download, counts=None: progress.append(
            {"stage": stage, "percent": percent, "counts": counts}
        ),
    )

    assert result.ingested_count == 1
    assert any(item["counts"] and item["counts"]["scanned_count"] == 1 for item in progress)
    assert any(item["counts"] and item["counts"]["downloaded_count"] == 1 for item in progress)
    assert any(item["counts"] and item["counts"]["ingested_count"] == 1 for item in progress)
    assert any(item["counts"] and item["counts"].get("current_ingest_symbol") == "sh600000" for item in progress)
    assert any(item["counts"] and item["counts"]["ingest_processed_count"] == 1 for item in progress)
    assert any(item["counts"] and item["counts"]["ingest_total_count"] == 1 for item in progress)
    assert any(item["counts"] and item["counts"].get("current_ingest_status") == "ingesting" for item in progress)
    assert any(item["counts"] and item["counts"].get("current_archive_ingest_processed_count") == 1 for item in progress)
    assert any(item["counts"] and item["counts"].get("current_archive_ingest_total_count") == 1 for item in progress)
    assert any(item["stage"] == "入库 sh600000" and item["percent"] > 45 for item in progress)


def test_baidu_sync_reports_current_symbol_for_each_archive_ingest(tmp_path):
    remote_path = BaiduStockKPathStrategy().candidates(Timeframe.M1, dt.date(2024, 12, 20))[0].remote_path
    progress = []
    result = BaiduSyncJobRunner(
        client=FakeBaiduClient({remote_path: _zip_with_symbols(["sh600000", "sz000001"])}),
        cache_dir=tmp_path / "raw" / "baidu",
        parquet_root=tmp_path / "parquet",
        metadata=SyncMetadata(tmp_path / "meta.duckdb"),
    ).run(
        timeframe=Timeframe.M1,
        start=dt.date(2024, 12, 20),
        end=dt.date(2024, 12, 20),
        symbols=["sh600000", "sz000001"],
        progress_callback=lambda stage, percent, download, counts=None: progress.append(
            {"stage": stage, "percent": percent, "counts": counts}
        ),
    )

    assert result.ingested_count == 2
    ingest_events = [item for item in progress if item["counts"] and item["counts"].get("current_ingest_symbol")]
    assert [(item["counts"]["current_ingest_symbol"], item["counts"]["current_ingest_status"]) for item in ingest_events] == [
        ("sh600000", "ingesting"),
        ("sh600000", "committed"),
        ("sz000001", "ingesting"),
        ("sz000001", "committed"),
    ]
    assert ingest_events[0]["counts"]["ingest_processed_count"] == 0
    assert ingest_events[0]["counts"]["current_archive_ingest_processed_count"] == 0
    assert ingest_events[0]["counts"]["current_archive_ingest_total_count"] == 2
    assert ingest_events[1]["counts"]["ingest_processed_count"] == 1
    assert ingest_events[1]["counts"]["ingest_total_count"] == 2
    assert ingest_events[1]["counts"]["current_archive_ingest_processed_count"] == 1
    assert ingest_events[1]["counts"]["current_ingest_path"] == remote_path
    assert ingest_events[3]["counts"]["ingest_processed_count"] == 2
    assert ingest_events[3]["counts"]["current_archive_ingest_processed_count"] == 2
    assert ingest_events[3]["percent"] > ingest_events[1]["percent"]


def test_baidu_sync_skips_missing_symbol_without_failing_job(tmp_path):
    remote_path = BaiduStockKPathStrategy().candidates(Timeframe.M1, dt.date(2024, 12, 20))[0].remote_path
    metadata = SyncMetadata(tmp_path / "meta.duckdb")
    result = BaiduSyncJobRunner(
        client=FakeBaiduClient({remote_path: make_zip(sample_rows(symbol="sz000001"), member="sz000001.csv")}),
        cache_dir=tmp_path / "raw" / "baidu",
        parquet_root=tmp_path / "parquet",
        metadata=metadata,
    ).run(
        timeframe=Timeframe.M1,
        start=dt.date(2024, 12, 20),
        end=dt.date(2024, 12, 20),
        symbols=["sh600000"],
    )

    assert result.downloaded_count == 1
    assert result.ingested_count == 0
    assert result.failed_count == 0
    assert metadata.fetchone("SELECT status FROM file_ingests WHERE remote_path = ?", [remote_path])[0] == "symbol_missing"
    assert metadata.fetchone("SELECT status, failed_count FROM sync_jobs WHERE id = ?", [result.job_id]) == (
        "completed",
        0,
    )


def test_baidu_sync_cancels_during_download(tmp_path):
    remote_path = BaiduStockKPathStrategy().candidates(Timeframe.M1, dt.date(2024, 12, 20))[0].remote_path
    metadata = SyncMetadata(tmp_path / "meta.duckdb")
    cancel_event = threading.Event()
    runner = BaiduSyncJobRunner(
        client=CancellingBaiduClient({remote_path: b"zip"}),
        cache_dir=tmp_path / "raw" / "baidu",
        parquet_root=tmp_path / "parquet",
        metadata=metadata,
    )

    with pytest.raises(SyncCancelled):
        runner.run(
            timeframe=Timeframe.M1,
            start=dt.date(2024, 12, 20),
            end=dt.date(2024, 12, 20),
            symbols=["sh600000"],
            cancel_event=cancel_event,
        )

    assert metadata.fetchone("SELECT status, error_message FROM sync_jobs")[0] == "stopped"


def test_baidu_sync_prefers_annual_archive_for_large_actual_gap(tmp_path):
    candidates = BaiduStockKPathStrategy().candidates(Timeframe.M1, dt.date(2024, 12, 20))
    paths = {candidate.source_kind: candidate.remote_path for candidate in candidates}
    metadata = SyncMetadata(tmp_path / "meta.duckdb")
    client = FakeBaiduClient(
        {
            paths["daily"]: make_zip(sample_rows()),
            paths["annual"]: make_zip(sample_rows()),
        }
    )
    result = BaiduSyncJobRunner(
        client=client,
        cache_dir=tmp_path / "raw" / "baidu",
        parquet_root=tmp_path / "parquet",
        metadata=metadata,
    ).run(
        timeframe=Timeframe.M1,
        start=dt.date(2024, 1, 1),
        end=dt.date(2024, 12, 31),
        symbols=["sh600000"],
    )

    assert result.downloaded_count == 1
    assert result.ingested_count == 1
    assert client.downloaded == [paths["annual"]]


def test_baidu_sync_skips_previously_committed_archive(tmp_path):
    candidates = BaiduStockKPathStrategy().candidates(Timeframe.M1, dt.date(2024, 12, 20))
    paths = {candidate.source_kind: candidate.remote_path for candidate in candidates}
    metadata = SyncMetadata(tmp_path / "meta.duckdb")
    metadata.initialize()
    metadata.commit_file_ingest(
        source_id="baidu-main",
        remote_path=paths["annual"],
        timeframe=Timeframe.M1.value,
        symbol="sh600000",
        start_ts=dt.datetime(2024, 1, 1, 9, 30),
        end_ts=dt.datetime(2024, 12, 31, 15, 0),
        row_count=1,
        expected_row_count=240,
        content_hash="already-ingested",
        parquet_path="data.parquet",
    )
    client = FakeBaiduClient(
        {
            paths["daily"]: make_zip(sample_rows()),
            paths["annual"]: make_zip(sample_rows()),
        }
    )

    BaiduSyncJobRunner(
        client=client,
        cache_dir=tmp_path / "raw" / "baidu",
        parquet_root=tmp_path / "parquet",
        metadata=metadata,
    ).run(
        timeframe=Timeframe.M1,
        start=dt.date(2024, 1, 1),
        end=dt.date(2024, 12, 31),
        symbols=["sh600000"],
    )

    assert paths["annual"] not in client.downloaded


def test_baidu_sync_prefers_monthly_archive_for_month_sized_actual_gap(tmp_path):
    candidates = BaiduStockKPathStrategy().candidates(Timeframe.M1, dt.date(2024, 12, 2))
    paths = {candidate.source_kind: candidate.remote_path for candidate in candidates}
    metadata = SyncMetadata(tmp_path / "meta.duckdb")
    client = FakeBaiduClient(
        {
            paths["daily"]: make_zip(sample_rows()),
            paths["monthly"]: make_zip(sample_rows()),
        }
    )
    result = BaiduSyncJobRunner(
        client=client,
        cache_dir=tmp_path / "raw" / "baidu",
        parquet_root=tmp_path / "parquet",
        metadata=metadata,
    ).run(
        timeframe=Timeframe.M1,
        start=dt.date(2024, 12, 2),
        end=dt.date(2024, 12, 31),
        symbols=["sh600000"],
    )

    assert result.downloaded_count == 1
    assert result.ingested_count == 1
    assert client.downloaded == [paths["monthly"]]


def test_baidu_sync_skips_missing_trade_date_without_failing_job(tmp_path):
    metadata = SyncMetadata(tmp_path / "meta.duckdb")
    result = BaiduSyncJobRunner(
        client=FakeBaiduClient({}),
        cache_dir=tmp_path / "raw" / "baidu",
        parquet_root=tmp_path / "parquet",
        metadata=metadata,
    ).run(
        timeframe=Timeframe.M1,
        start=dt.date(2024, 12, 20),
        end=dt.date(2024, 12, 20),
        symbols=["sh600000"],
    )
    assert result.scanned_count == 1
    assert result.downloaded_count == 0
    assert result.ingested_count == 0
    assert result.failed_count == 0
    assert metadata.fetchone("SELECT status, failed_count FROM sync_jobs WHERE id = ?", [result.job_id]) == (
        "completed",
        0,
    )


def _zip_with_symbols(symbols: list[str]) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        for symbol in symbols:
            payload = pd.DataFrame(sample_rows(symbol=symbol)).to_csv(index=False).encode("utf-8-sig")
            archive.writestr(f"{symbol}.csv", payload)
    return buffer.getvalue()
