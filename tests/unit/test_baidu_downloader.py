import datetime as dt
import hashlib
from pathlib import Path

from stock_data_service.baidu.pan_client import BaiduFileMeta
from stock_data_service.market.path_strategy import RemoteFileCandidate
from stock_data_service.market.timeframe import Timeframe
from stock_data_service.sync.downloader import BaiduDownloader


class FakeBaiduClient:
    def __init__(self, files: dict[str, bytes]):
        self.files = files
        self.downloaded: list[str] = []

    def get_file_meta_by_path(self, path: str):
        if path not in self.files:
            return None
        return BaiduFileMeta(
            remote_path=path,
            fs_id=1,
            size=len(self.files[path]),
            md5=hashlib.md5(self.files[path]).hexdigest(),
            server_mtime=1734652800,
        )

    def download_by_path(self, path: str, dest_path: str | Path, *, progress_callback=None):
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


def test_download_for_range_falls_back_to_monthly_candidate(tmp_path):
    monthly = "/A股_分时数据/1分钟_按月归档/2024-12/20241220_1min.zip"
    downloader = BaiduDownloader(FakeBaiduClient({monthly: b"zip"}), tmp_path / "raw")
    results = downloader.download_for_range(
        timeframe=Timeframe.M1,
        start=dt.date(2024, 12, 20),
        end=dt.date(2024, 12, 20),
    )
    assert len(results) == 1
    assert results[0].remote_path == monthly
    assert results[0].status == "downloaded"
    assert results[0].local_path.read_bytes() == b"zip"
    assert results[0].content_hash
    assert results[0].server_mtime == dt.datetime(2024, 12, 20, 0, 0)


def test_download_for_range_reuses_annual_archive_for_multiple_days(tmp_path):
    annual = "/A股_分时数据/1分钟_按年汇总/2024_1min.zip"
    fake_client = FakeBaiduClient({annual: b"zip"})
    downloader = BaiduDownloader(fake_client, tmp_path / "raw")
    results = downloader.download_for_range(
        timeframe=Timeframe.M1,
        start=dt.date(2024, 12, 20),
        end=dt.date(2024, 12, 23),
    )
    assert [result.remote_path for result in results] == [annual]
    assert fake_client.downloaded == [annual]


def test_download_for_range_reports_missing_trade_date(tmp_path):
    results = BaiduDownloader(FakeBaiduClient({}), tmp_path / "raw").download_for_range(
        timeframe=Timeframe.M1,
        start=dt.date(2024, 12, 20),
        end=dt.date(2024, 12, 20),
    )
    assert len(results) == 1
    assert results[0].status == "missing"
    assert results[0].remote_path == ""


def test_download_candidate_reuses_matching_cache_and_redownloads_stale_cache(tmp_path):
    remote_path = "/A股_分时数据/1分钟/20241220_1min.zip"
    fake_client = FakeBaiduClient({remote_path: b"fresh"})
    downloader = BaiduDownloader(fake_client, tmp_path / "raw")
    local_path = tmp_path / "raw" / remote_path.lstrip("/")
    local_path.parent.mkdir(parents=True)

    local_path.write_bytes(b"fresh")
    first = downloader.download_for_range(
        timeframe=Timeframe.M1,
        start=dt.date(2024, 12, 20),
        end=dt.date(2024, 12, 20),
    )
    assert first[0].local_path.read_bytes() == b"fresh"
    assert fake_client.downloaded == []

    local_path.write_bytes(b"stale")
    second = downloader.download_for_range(
        timeframe=Timeframe.M1,
        start=dt.date(2024, 12, 20),
        end=dt.date(2024, 12, 20),
    )
    assert second[0].local_path.read_bytes() == b"fresh"
    assert fake_client.downloaded == [remote_path]


def test_download_candidate_reports_remote_progress(tmp_path):
    remote_path = "/A股_分时数据/1分钟/20241220_1min.zip"
    progress = []
    downloader = BaiduDownloader(FakeBaiduClient({remote_path: b"fresh"}), tmp_path / "raw")

    result = downloader.download_candidate(
        timeframe=Timeframe.M1,
        candidate=RemoteFileCandidate(remote_path=remote_path, trade_date=dt.date(2024, 12, 20), source_kind="daily"),
        progress_callback=progress.append,
    )

    assert result.status == "downloaded"
    assert progress[-1]["remote_path"] == remote_path
    assert progress[-1]["trade_date"] == "2024-12-20"
    assert progress[-1]["source_kind"] == "daily"
    assert progress[-1]["speed_bytes_per_sec"] == 1024
