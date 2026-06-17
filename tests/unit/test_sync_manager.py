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
    assert "cancel_event" not in payload


def test_sync_manager_empty_status(tmp_path):
    manager = SyncJobManager(Settings(data_root=tmp_path / "data"))
    assert manager.status() == {"active_job": None, "jobs": []}
