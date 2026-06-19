import datetime as dt
import hashlib
import io
import zipfile
from pathlib import Path

from stock_data_service.baidu.pan_client import BaiduFileMeta
from stock_data_service.market.path_strategy import BaiduStockKPathStrategy, RemoteFileCandidate
from stock_data_service.market.timeframe import Timeframe
from stock_data_service.sync.downloader import BaiduDownloader


class FakeBaiduClient:
    def __init__(self, files: dict[str, bytes], md5_by_path: dict[str, str | None] | None = None):
        self.files = files
        self.md5_by_path = md5_by_path or {}
        self.downloaded: list[str] = []
        self.download_kwargs: list[dict] = []

    def get_file_meta_by_path(self, path: str):
        if path not in self.files:
            return None
        md5 = self.md5_by_path.get(path)
        if path not in self.md5_by_path:
            md5 = hashlib.md5(self.files[path]).hexdigest()
        return BaiduFileMeta(
            remote_path=path,
            fs_id=1,
            size=len(self.files[path]),
            md5=md5,
            server_mtime=1734652800,
        )

    def download_by_path(
        self,
        path: str,
        dest_path: str | Path,
        *,
        progress_callback=None,
        cancel_event=None,
        use_local_cache=True,
    ):
        self.downloaded.append(path)
        self.download_kwargs.append({"use_local_cache": use_local_cache})
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


class FailingBaiduClient(FakeBaiduClient):
    def download_by_path(
        self,
        path: str,
        dest_path: str | Path,
        *,
        progress_callback=None,
        cancel_event=None,
        use_local_cache=True,
    ):
        self.downloaded.append(path)
        self.download_kwargs.append({"use_local_cache": use_local_cache})
        raise RuntimeError("boom")


def make_zip_bytes() -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("sh600000.csv", "date,time,open,high,low,close,volume,amount\n")
    return buffer.getvalue()


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


def test_download_for_trade_dates_can_prefer_annual_archive(tmp_path):
    candidates = BaiduStockKPathStrategy().candidates(Timeframe.M1, dt.date(2024, 12, 20))
    paths = {candidate.source_kind: candidate.remote_path for candidate in candidates}
    fake_client = FakeBaiduClient({paths["daily"]: b"daily", paths["annual"]: b"annual"})
    downloader = BaiduDownloader(fake_client, tmp_path / "raw")

    results = downloader.download_for_trade_dates(
        timeframe=Timeframe.M1,
        trade_dates=[dt.date(2024, 12, 20)],
        source_preferences={dt.date(2024, 12, 20): ["annual", "monthly", "daily"]},
    )

    assert [result.remote_path for result in results] == [paths["annual"]]
    assert fake_client.downloaded == [paths["annual"]]


def test_download_for_trade_dates_skips_already_ingested_archive(tmp_path):
    paths = {
        item.source_kind: item.remote_path
        for item in BaiduStockKPathStrategy().candidates(Timeframe.M1, dt.date(2024, 12, 20))
    }
    fake_client = FakeBaiduClient({paths["daily"]: b"daily", paths["annual"]: b"annual"})
    downloader = BaiduDownloader(fake_client, tmp_path / "raw")

    results = downloader.download_for_trade_dates(
        timeframe=Timeframe.M1,
        trade_dates=[dt.date(2024, 12, 20)],
        source_preferences={dt.date(2024, 12, 20): ["annual", "monthly", "daily"]},
        skip_remote_paths={paths["annual"]},
    )

    assert [result.remote_path for result in results] == [paths["daily"]]
    assert fake_client.downloaded == [paths["daily"]]


def test_download_for_trade_dates_reuses_successful_archive_before_daily_candidate(tmp_path):
    first_paths = {
        item.source_kind: item.remote_path
        for item in BaiduStockKPathStrategy().candidates(Timeframe.M1, dt.date(2024, 12, 20))
    }
    second_paths = {
        item.source_kind: item.remote_path
        for item in BaiduStockKPathStrategy().candidates(Timeframe.M1, dt.date(2024, 12, 23))
    }
    fake_client = FakeBaiduClient({first_paths["annual"]: b"annual", second_paths["daily"]: b"daily"})
    downloader = BaiduDownloader(fake_client, tmp_path / "raw")

    results = downloader.download_for_trade_dates(
        timeframe=Timeframe.M1,
        trade_dates=[dt.date(2024, 12, 20), dt.date(2024, 12, 23)],
        source_preferences={
            dt.date(2024, 12, 20): ["daily", "monthly", "annual"],
            dt.date(2024, 12, 23): ["daily", "monthly", "annual"],
        },
    )

    assert [result.remote_path for result in results] == [first_paths["annual"]]
    assert fake_client.downloaded == [first_paths["annual"]]


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
    assert fake_client.download_kwargs[-1]["use_local_cache"] is False


def test_download_candidate_redownloads_invalid_remote_md5_when_size_mismatches(tmp_path):
    remote_path = "/archive/2000_1min.zip"
    payload = make_zip_bytes()
    fake_client = FakeBaiduClient(
        {remote_path: payload},
        md5_by_path={remote_path: "70e72eb4bn8705087a7cc5e636b8e77e"},
    )
    downloader = BaiduDownloader(fake_client, tmp_path / "raw")
    local_path = tmp_path / "raw" / remote_path.lstrip("/")
    local_path.parent.mkdir(parents=True)
    local_path.write_bytes(payload[:-8])

    result = downloader.download_candidate(
        timeframe=Timeframe.M1,
        candidate=RemoteFileCandidate(remote_path=remote_path, trade_date=dt.date(2000, 1, 4), source_kind="annual"),
    )

    assert result.status == "downloaded"
    assert local_path.read_bytes() == payload
    assert fake_client.downloaded == [remote_path]
    assert fake_client.download_kwargs[-1]["use_local_cache"] is False


def test_download_candidate_redownloads_invalid_remote_md5_when_zip_header_is_invalid(tmp_path):
    remote_path = "/archive/2000_1min.zip"
    payload = make_zip_bytes()
    fake_client = FakeBaiduClient(
        {remote_path: payload},
        md5_by_path={remote_path: "70e72eb4bn8705087a7cc5e636b8e77e"},
    )
    downloader = BaiduDownloader(fake_client, tmp_path / "raw")
    local_path = tmp_path / "raw" / remote_path.lstrip("/")
    local_path.parent.mkdir(parents=True)
    local_path.write_bytes(b"x" * len(payload))

    result = downloader.download_candidate(
        timeframe=Timeframe.M1,
        candidate=RemoteFileCandidate(remote_path=remote_path, trade_date=dt.date(2000, 1, 4), source_kind="annual"),
    )

    assert result.status == "downloaded"
    assert local_path.read_bytes() == payload
    assert fake_client.downloaded == [remote_path]
    assert fake_client.download_kwargs[-1]["use_local_cache"] is False


def test_download_candidate_reuses_weak_cache_when_remote_md5_is_not_standard(tmp_path):
    remote_path = "/A股_分时数据/1分钟_按年汇总/2000_1min.zip"
    payload = make_zip_bytes()
    fake_client = FakeBaiduClient(
        {remote_path: payload},
        md5_by_path={remote_path: "70e72eb4bn8705087a7cc5e636b8e77e"},
    )
    downloader = BaiduDownloader(fake_client, tmp_path / "raw")
    local_path = tmp_path / "raw" / remote_path.lstrip("/")
    local_path.parent.mkdir(parents=True)
    local_path.write_bytes(payload)

    result = downloader.download_candidate(
        timeframe=Timeframe.M1,
        candidate=RemoteFileCandidate(remote_path=remote_path, trade_date=dt.date(2000, 1, 4), source_kind="annual"),
    )

    assert result.status == "downloaded"
    assert result.local_path == local_path
    assert fake_client.downloaded == []


def test_download_candidate_preserves_stale_cache_when_redownload_fails(tmp_path):
    remote_path = "/A股_分时数据/1分钟/20241220_1min.zip"
    fake_client = FailingBaiduClient({remote_path: b"fresh"})
    downloader = BaiduDownloader(fake_client, tmp_path / "raw")
    local_path = tmp_path / "raw" / remote_path.lstrip("/")
    local_path.parent.mkdir(parents=True)
    local_path.write_bytes(b"stale")
    partial_path = local_path.with_name(f"{local_path.name}.part")
    partial_path.write_bytes(b"partial")

    result = downloader.download_candidate(
        timeframe=Timeframe.M1,
        candidate=RemoteFileCandidate(remote_path=remote_path, trade_date=dt.date(2024, 12, 20), source_kind="daily"),
    )

    assert result.status == "failed"
    assert local_path.read_bytes() == b"stale"
    assert not partial_path.exists()
    assert fake_client.downloaded == [remote_path]
    assert fake_client.download_kwargs[-1]["use_local_cache"] is False


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
