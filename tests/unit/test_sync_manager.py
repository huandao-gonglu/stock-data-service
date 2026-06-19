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
    assert payload["ingest_processed_count"] == 0
    assert payload["ingest_total_count"] is None
    assert payload["current_archive_ingest_processed_count"] == 0
    assert payload["current_archive_ingest_total_count"] is None
    assert payload["current_ingest_symbol"] is None
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
    assert payload["current_ingest_symbol"] == "sh600000"
    assert payload["current_ingest_path"] == "/A股_分时数据/1分钟_按年汇总/2024_1min.zip"
    assert payload["current_ingest_status"] == "committed"
