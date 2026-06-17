import datetime as dt
from pathlib import Path

from conftest import make_zip, sample_rows
from stock_data_service.baidu.pan_client import BaiduFileMeta
from stock_data_service.market.timeframe import Timeframe
from stock_data_service.storage.duckdb_repository import DuckDBRepository
from stock_data_service.storage.sync_metadata import SyncMetadata
from stock_data_service.sync.job_runner import BaiduSyncJobRunner


class FakeBaiduClient:
    def __init__(self, files: dict[str, bytes]):
        self.files = files

    def get_file_meta_by_path(self, path: str):
        if path not in self.files:
            return None
        return BaiduFileMeta(remote_path=path, fs_id=1, size=len(self.files[path]), md5="md5", server_mtime=1734652800)

    def download_by_path(self, path: str, dest_path: str | Path, *, progress_callback=None):
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


def test_baidu_sync_marks_missing_trade_date_as_job_failure(tmp_path):
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
    assert result.failed_count == 1
    assert metadata.fetchone("SELECT status, failed_count FROM sync_jobs WHERE id = ?", [result.job_id]) == (
        "completed_with_errors",
        1,
    )
