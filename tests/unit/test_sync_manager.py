import datetime as dt

from stock_data_service.config import Settings
from stock_data_service.sync.manager import ManagedSyncJob, ManagedSyncRequest, SyncJobManager


def test_managed_sync_job_to_dict_omits_cancel_event():
    job = ManagedSyncJob(
        id="job-1",
        kind="baidu",
        request=ManagedSyncRequest("baidu-main", "1m", "2024-12-20", "2024-12-20", ["sh600000"]),
    )
    payload = job.to_dict()
    assert payload["id"] == "job-1"
    assert payload["request"]["symbols"] == ["sh600000"]
    assert payload["download_speed_bytes_per_sec"] == 0.0
    assert payload["downloaded_bytes"] == 0
    assert payload["download_total_bytes"] is None
    assert payload["current_download_path"] is None
    assert payload["planned_download_count"] is None
    assert payload["completed_archive_count"] == 0
    assert payload["ingest_processed_count"] == 0
    assert payload["ingest_total_count"] is None
    assert payload["current_archive_ingest_processed_count"] == 0
    assert payload["current_archive_ingest_total_count"] is None
    assert payload["current_archive_requested_count"] is None
    assert payload["current_archive_present_count"] is None
    assert payload["current_archive_missing_count"] == 0
    assert payload["current_ingest_symbol"] is None
    assert payload["eta_seconds"] is None
    assert payload["eta_confidence"] == "warming_up"
    assert payload["progress_rate_percent_per_min"] is None
    assert "cancel_event" not in payload


def test_managed_sync_job_to_dict_truncates_large_symbol_lists():
    symbols = [f"sh60{i:04d}" for i in range(150)]
    job = ManagedSyncJob(
        id="job-1",
        kind="baidu",
        request=ManagedSyncRequest("baidu-main", "1m", "2024-12-20", "2024-12-20", symbols),
    )
    payload = job.to_dict()

    assert payload["request"]["symbol_count"] == 150
    assert payload["request"]["symbols"] == symbols[:20]
    assert payload["request"]["symbols_truncated"] is True


def test_sync_manager_empty_status(tmp_path):
    manager = SyncJobManager(Settings(data_root=tmp_path / "data"))
    assert manager.status() == {"active_job": None, "jobs": []}


def test_download_progress_does_not_overwrite_ingest_stage(tmp_path):
    manager = SyncJobManager(Settings(data_root=tmp_path / "data"))
    job = ManagedSyncJob(
        id="job-1",
        kind="baidu",
        request=ManagedSyncRequest("baidu-main", "1m", "2024-12-20", "2024-12-20", ["sh600000"]),
        status="running",
    )
    manager._jobs[job.id] = job
    manager._active_job_id = job.id

    manager._update_progress(
        job.id,
        "下载完成，开始入库",
        45,
        counts={"scanned_count": 1, "downloaded_count": 1, "ingested_count": 0, "failed_count": 0},
    )
    manager._update_progress(
        job.id,
        "下载 2001_1min.zip",
        20,
        download={
            "remote_path": "/A股_分时数据/1分钟_按年汇总/2001_1min.zip",
            "bytes_downloaded": 1024,
            "total_bytes": 2048,
            "speed_bytes_per_sec": 512,
        },
    )

    assert job.stage == "下载完成，开始入库"
    assert job.progress_percent == 45
    assert job.current_download_path == "/A股_分时数据/1分钟_按年汇总/2001_1min.zip"
    assert job.downloaded_bytes == 1024


def test_progress_percent_does_not_regress_between_archives(tmp_path):
    manager = SyncJobManager(Settings(data_root=tmp_path / "data"))
    job = ManagedSyncJob(
        id="job-1",
        kind="baidu",
        request=ManagedSyncRequest("baidu-main", "1m", "2000-01-01", "2026-06-19", ["sh600000"]),
        status="running",
        progress_percent=48,
    )
    manager._jobs[job.id] = job
    manager._active_job_id = job.id

    manager._update_progress(
        job.id,
        "scan next archive",
        35,
        counts={
            "scanned_count": 10,
            "downloaded_count": 9,
            "ingested_count": 39,
            "failed_count": 0,
            "planned_download_count": 135,
            "ingest_processed_count": 50,
            "ingest_total_count": 675,
            "current_archive_ingest_processed_count": 0,
            "current_archive_ingest_total_count": 5,
        },
    )

    assert job.progress_percent == 48
    assert job.stage == "scan next archive"
    assert job.scanned_count == 10
    assert job.current_archive_ingest_processed_count == 0


def test_counts_progress_can_clear_stale_ingest_details(tmp_path):
    manager = SyncJobManager(Settings(data_root=tmp_path / "data"))
    job = ManagedSyncJob(
        id="job-1",
        kind="baidu",
        request=ManagedSyncRequest("baidu-main", "1m", "2000-01-01", "2026-06-19", ["sh600000"]),
        status="running",
        current_ingest_symbol="sh600000",
        current_ingest_path="/A股_分时数据/1分钟_按年汇总/2000_1min.zip",
        current_ingest_status="committed",
    )
    manager._jobs[job.id] = job
    manager._active_job_id = job.id

    manager._update_progress(
        job.id,
        "scan next archive",
        45,
        counts={
            "scanned_count": 2,
            "downloaded_count": 1,
            "ingested_count": 1,
            "failed_count": 0,
            "current_ingest_symbol": None,
            "current_ingest_path": None,
            "current_ingest_status": None,
        },
    )

    assert job.current_ingest_symbol is None
    assert job.current_ingest_path is None
    assert job.current_ingest_status is None


def test_sync_manager_records_ingest_progress_details(tmp_path):
    manager = SyncJobManager(Settings(data_root=tmp_path / "data"))
    job = ManagedSyncJob(
        id="job-1",
        kind="baidu",
        request=ManagedSyncRequest("baidu-main", "1m", "2024-12-20", "2024-12-20", ["sh600000", "sz000001"]),
        status="running",
    )
    manager._jobs[job.id] = job
    manager._active_job_id = job.id

    manager._update_progress(
        job.id,
        "入库 sh600000",
        71,
        counts={
            "scanned_count": 1,
            "downloaded_count": 1,
            "ingested_count": 1,
            "failed_count": 0,
            "planned_download_count": 1,
            "ingest_processed_count": 1,
            "ingest_total_count": 2,
            "current_archive_ingest_processed_count": 1,
            "current_archive_ingest_total_count": 2,
            "current_archive_requested_count": 5,
            "current_archive_present_count": 2,
            "current_archive_missing_count": 3,
            "current_ingest_symbol": "sh600000",
            "current_ingest_path": "/A股_分时数据/1分钟_按年汇总/2024_1min.zip",
            "current_ingest_status": "committed",
        },
    )

    payload = job.to_dict()
    assert payload["stage"] == "入库 sh600000"
    assert payload["progress_percent"] == 71
    assert payload["planned_download_count"] == 1
    assert payload["ingest_processed_count"] == 1
    assert payload["ingest_total_count"] == 2
    assert payload["current_archive_ingest_processed_count"] == 1
    assert payload["current_archive_ingest_total_count"] == 2
    assert payload["current_archive_requested_count"] == 5
    assert payload["current_archive_present_count"] == 2
    assert payload["current_archive_missing_count"] == 3
    assert payload["current_ingest_symbol"] == "sh600000"
    assert payload["current_ingest_path"] == "/A股_分时数据/1分钟_按年汇总/2024_1min.zip"
    assert payload["current_ingest_status"] == "committed"


def test_finish_ingest_marks_total_done_and_clears_active_archive(tmp_path):
    job = ManagedSyncJob(
        id="job-1",
        kind="baidu",
        request=ManagedSyncRequest("baidu-main", "1m", "2000-01-01", "2026-06-19", ["sh600000"]),
        ingest_processed_count=765,
        ingest_total_count=1510,
        current_archive_ingest_processed_count=3,
        current_archive_ingest_total_count=5,
        current_archive_requested_count=5,
        current_archive_present_count=4,
        current_archive_missing_count=1,
        current_ingest_symbol="sh600000",
        current_ingest_path="/x.zip",
        current_ingest_status="committed",
    )

    SyncJobManager._finish_ingest(job)

    payload = job.to_dict()
    assert payload["ingest_processed_count"] == 1510
    assert payload["ingest_total_count"] == 1510
    assert payload["current_archive_ingest_processed_count"] == 0
    assert payload["current_archive_ingest_total_count"] is None
    assert payload["current_archive_requested_count"] is None
    assert payload["current_archive_present_count"] is None
    assert payload["current_archive_missing_count"] == 0
    assert payload["current_ingest_symbol"] is None
    assert payload["current_ingest_path"] is None
    assert payload["current_ingest_status"] is None


def test_eta_progress_uses_completed_archives_for_multi_archive_jobs():
    job = ManagedSyncJob(
        id="job-1",
        kind="baidu",
        request=ManagedSyncRequest("baidu-main", "1m", "2000-01-01", "2026-06-19", ["sh600000"]),
        status="running",
        progress_percent=45,
        scanned_count=1,
        downloaded_count=1,
        completed_archive_count=1,
        planned_download_count=100,
        ingest_processed_count=0,
        ingest_total_count=1000,
    )

    assert round(SyncJobManager._eta_progress_value(job), 2) == 10.88


def test_eta_progress_includes_current_download_fraction_for_file_sync():
    job = ManagedSyncJob(
        id="job-1",
        kind="baidu_file",
        request=ManagedSyncRequest("baidu-main", "1m", "2024-12-20", "2024-12-20", ["sh600000"]),
        status="running",
        progress_percent=35,
        scanned_count=1,
        downloaded_count=0,
        planned_download_count=1,
        ingest_processed_count=0,
        ingest_total_count=2,
        current_download_path="/A/2024_1min.zip",
        downloaded_bytes=500,
        download_total_bytes=1000,
    )

    assert SyncJobManager._eta_progress_value(job) == 27.6


def test_eta_progress_ignores_global_ingest_jump_with_archive_counts():
    job = ManagedSyncJob(
        id="job-1",
        kind="baidu",
        request=ManagedSyncRequest("baidu-main", "1m", "2000-01-01", "2026-06-19", ["sh600000"]),
        status="running",
        progress_percent=80,
        scanned_count=13,
        downloaded_count=13,
        completed_archive_count=12,
        planned_download_count=135,
        ingest_processed_count=400000,
        ingest_total_count=453330,
        current_archive_ingest_processed_count=10,
        current_archive_present_count=100,
    )

    assert round(SyncJobManager._eta_progress_value(job), 2) == 18.12


def test_eta_does_not_treat_first_archive_download_as_whole_download_stage(tmp_path, monkeypatch):
    ticks = iter(
        [
            dt.datetime(2026, 6, 19, 9, 0, 0),
            dt.datetime(2026, 6, 19, 9, 1, 0),
        ]
    )
    monkeypatch.setattr("stock_data_service.sync.manager._now", lambda: next(ticks))
    manager = SyncJobManager(Settings(data_root=tmp_path / "data"))
    job = ManagedSyncJob(
        id="job-1",
        kind="baidu",
        request=ManagedSyncRequest("baidu-main", "1m", "2000-01-01", "2026-06-19", ["sh600000"]),
        status="running",
        progress_percent=5,
    )
    manager._jobs[job.id] = job
    manager._active_job_id = job.id
    SyncJobManager._prime_eta(job)

    manager._update_progress(
        job.id,
        "downloaded first archive",
        45,
        counts={
            "scanned_count": 1,
            "downloaded_count": 1,
            "planned_download_count": 100,
            "ingest_processed_count": 0,
            "ingest_total_count": 1000,
        },
    )

    assert job.progress_percent == 45
    assert job.progress_rate_percent_per_min == 5.35
    assert job.eta_seconds >= 1000
    assert job.eta_confidence == "warming_up"


def test_eta_uses_smoothed_progress_rate(tmp_path, monkeypatch):
    ticks = iter(
        [
            dt.datetime(2026, 6, 19, 9, 0, 0),
            dt.datetime(2026, 6, 19, 9, 1, 0),
            dt.datetime(2026, 6, 19, 9, 2, 0),
        ]
    )
    monkeypatch.setattr("stock_data_service.sync.manager._now", lambda: next(ticks))
    manager = SyncJobManager(Settings(data_root=tmp_path / "data"))
    job = ManagedSyncJob(
        id="job-1",
        kind="baidu",
        request=ManagedSyncRequest("baidu-main", "1m", "2000-01-01", "2026-06-19", ["sh600000"]),
        status="running",
        progress_percent=5,
    )
    manager._jobs[job.id] = job
    manager._active_job_id = job.id
    SyncJobManager._prime_eta(job)

    manager._update_progress(job.id, "scan", 35, counts={"scanned_count": 1})
    assert job.eta_seconds == 130
    assert job.eta_confidence == "warming_up"
    assert job.progress_rate_percent_per_min == 30

    manager._update_progress(job.id, "scan", 65, counts={"scanned_count": 2})
    assert job.eta_seconds == 70
    assert job.eta_confidence == "stable"
    assert job.progress_rate_percent_per_min == 30
