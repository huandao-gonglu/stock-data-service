import datetime as dt

from fastapi.testclient import TestClient

from stock_data_service.config import Settings
from stock_data_service.main import create_app
from stock_data_service.storage.sync_metadata import SyncMetadata


class FakeSyncManager:
    def __init__(self):
        self.started = None
        self.file_started = None
        self.stopped = None
        self.active = None

    def status(self):
        return {
            "active_job": self.active,
            "jobs": [self.active] if self.active else [],
        }

    def start_baidu_sync(self, request):
        self.started = request
        self.active = {
            "id": "fake-job",
            "kind": "baidu",
            "request": request.__dict__,
            "status": "running",
            "stage": "开始同步",
            "progress_percent": 5,
            "download_speed_bytes_per_sec": 0.0,
            "downloaded_bytes": 0,
            "download_total_bytes": None,
            "current_download_path": None,
            "created_at": dt.datetime(2026, 6, 15).isoformat(),
            "started_at": dt.datetime(2026, 6, 15).isoformat(),
            "finished_at": None,
            "scanned_count": 0,
            "downloaded_count": 0,
            "ingested_count": 0,
            "failed_count": 0,
            "error_message": None,
        }
        return FakeJob(self.active)

    def start_baidu_file_sync(self, request):
        self.file_started = request
        self.active = {
            "id": "fake-file-job",
            "kind": "baidu_file",
            "request": request.__dict__,
            "status": "running",
            "stage": "检查远端文件",
            "progress_percent": 10,
            "download_speed_bytes_per_sec": 0.0,
            "downloaded_bytes": 0,
            "download_total_bytes": None,
            "current_download_path": None,
            "created_at": dt.datetime(2026, 6, 15).isoformat(),
            "started_at": dt.datetime(2026, 6, 15).isoformat(),
            "finished_at": None,
            "scanned_count": 1,
            "downloaded_count": 0,
            "ingested_count": 0,
            "failed_count": 0,
            "error_message": None,
        }
        return FakeJob(self.active)

    def request_stop(self, job_id=None):
        self.stopped = job_id
        self.active["status"] = "stopping"
        return FakeJob(self.active)


class FakeJob:
    def __init__(self, payload):
        self.payload = payload
        self.id = payload["id"]

    def to_dict(self):
        return self.payload


class FakeBaiduListClient:
    def __init__(self, items_by_path):
        self.items_by_path = items_by_path
        self.calls = []

    def list_files(self, dir_path, **kwargs):
        self.calls.append((dir_path, kwargs))
        items = self.items_by_path.get(dir_path, [])
        start = int(kwargs.get("start", 0))
        limit = int(kwargs.get("limit", len(items)))
        return {"list": items[start : start + limit], "total": len(items)}


def _client(tmp_path):
    app = create_app(Settings(data_root=tmp_path / "data"))
    fake = FakeSyncManager()
    app.state.sync_manager = fake
    return TestClient(app), fake


def test_admin_page_and_settings_roundtrip(tmp_path):
    client, _ = _client(tmp_path)
    page = client.get("/admin")
    assert page.status_code == 200
    assert "Stock Data Service 管理台" in page.text
    assert 'href="/admin/calendar"' in page.text
    assert "CALENDAR_COLOR_HAS_DATA" not in page.text

    calendar_page = client.get("/admin/calendar")
    assert calendar_page.status_code == 200
    assert "数据日历" in calendar_page.text
    assert "CALENDAR_COLOR_BACKGROUND" in calendar_page.text
    assert 'CALENDAR_COLOR_HAS_DATA = "#22c55e"' in calendar_page.text
    assert 'CALENDAR_COLOR_MISSING_TRADING_DAY = "rgba(34, 197, 94, .55)"' in calendar_page.text
    assert "CALENDAR_COLOR_NON_TRADING_DAY = CALENDAR_COLOR_BACKGROUND" in calendar_page.text
    assert "color-mix(in srgb, var(--calendar-bg)" not in calendar_page.text
    assert 'class="calendar-legend legend"' in calendar_page.text
    assert 'class="calendar-zoom-controls"' in calendar_page.text
    assert 'id="zoomInput" type="number" min="83" max="225"' in calendar_page.text
    assert 'aria-label="缩放百分比"' in calendar_page.text
    assert 'addEventListener("change", applyZoomInput)' in calendar_page.text
    assert '|| "0.83"' in calendar_page.text
    assert "clamp(Number(value) || 0.83, 0.83, 2.25)" in calendar_page.text
    assert "border: 0;" in calendar_page.text
    assert 'calendar-day[data-status="missing"]' in calendar_page.text
    assert "legendMissing" in calendar_page.text
    assert "calendarProgressBar" in calendar_page.text
    assert "calendarStage" in calendar_page.text
    assert "calendar-mode-dense" in calendar_page.text
    assert "VIRTUAL_BUFFER_YEARS" in calendar_page.text
    assert "calendar-mode-overview" not in calendar_page.text
    assert '"0.24"' not in calendar_page.text
    assert "CALENDAR_DEFAULT_START_YEAR" not in calendar_page.text
    assert "calendarTopSpacer" in calendar_page.text
    assert 'addEventListener("wheel"' not in calendar_page.text
    assert "measureRenderedYearHeight" not in calendar_page.text
    assert "overflow-anchor: none" in calendar_page.text

    settings = client.get("/admin/api/settings").json()
    assert settings["system"]["data_root"].endswith("data")
    assert settings["sync_defaults"]["source_id"] == "baidu-main"

    saved = client.post(
        "/admin/api/settings",
        json={
            "source_id": "baidu-main",
            "timeframe": "5m",
            "start": "2024-12-20",
            "end": "2024-12-21",
            "symbols": ["600000.SH", "000001.SZ"],
        },
    )
    assert saved.status_code == 200
    assert saved.json()["sync_defaults"]["symbols"] == ["sh600000", "sz000001"]
    assert client.get("/admin/api/settings").json()["sync_defaults"]["timeframe"] == "5m"


def test_admin_coverage_calendar_marks_data_missing_and_non_trading(tmp_path):
    settings = Settings(data_root=tmp_path / "data")
    app = create_app(settings)
    metadata = SyncMetadata(settings.metadata_db)
    metadata.initialize()
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

    response = TestClient(app).get(
        "/admin/api/coverage/calendar",
        params={
            "symbol": "600000.SH",
            "timeframe": "1m",
            "start": "2024-12-20",
            "end": "2024-12-24",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    by_date = {item["date"]: item for item in payload["days"]}
    assert payload["symbol"] == "sh600000"
    assert by_date["2024-12-20"]["status"] == "has_data"
    assert by_date["2024-12-20"]["row_count"] == 240
    assert by_date["2024-12-21"]["status"] == "non_trading"
    assert by_date["2024-12-23"]["status"] == "has_data"
    assert by_date["2024-12-23"]["quality_flag"] == "partial"
    assert by_date["2024-12-24"]["status"] == "missing"
    assert payload["counts"]["has_data"] == 2
    assert payload["counts"]["missing"] == 1


def test_admin_sync_start_stop_and_status(tmp_path):
    client, fake = _client(tmp_path)
    start = client.post(
        "/admin/api/sync/start",
        json={
            "source_id": "baidu-main",
            "timeframe": "1m",
            "start": "2024-12-20",
            "end": "2024-12-20",
            "symbols": ["sh600000"],
        },
    )
    assert start.status_code == 200
    assert fake.started.symbols == ["sh600000"]
    status = client.get("/admin/api/sync/status").json()
    assert status["manager"]["active_job"]["status"] == "running"

    stop = client.post("/admin/api/sync/stop", json={"job_id": "fake-job"})
    assert stop.status_code == 200
    assert fake.stopped == "fake-job"
    assert stop.json()["job"]["status"] == "stopping"


def test_admin_baidu_list_marks_sync_statuses(tmp_path):
    settings = Settings(data_root=tmp_path / "data")
    app = create_app(settings)
    fake = FakeSyncManager()
    app.state.sync_manager = fake
    path = "/A股_分时数据/1分钟"
    synced_path = f"{path}/20241220_1min.zip"
    updated_path = f"{path}/20241223_1min.zip"
    unsynced_path = f"{path}/20241224_1min.zip"
    app.state.baidu_client_factory = lambda _: FakeBaiduListClient(
        {
            path: [
                {"server_filename": "按月归档", "path": f"{path}/按月归档", "isdir": 1, "server_mtime": 1734652800},
                {"server_filename": "20241220_1min.zip", "path": synced_path, "isdir": 0, "size": 10, "md5": "same", "server_mtime": 1734652800},
                {"server_filename": "20241223_1min.zip", "path": updated_path, "isdir": 0, "size": 20, "md5": "new", "server_mtime": 1734912000},
                {"server_filename": "20241224_1min.zip", "path": unsynced_path, "isdir": 0, "size": 30, "md5": "fresh", "server_mtime": 1734998400},
            ]
        }
    )
    metadata = SyncMetadata(settings.metadata_db)
    metadata.initialize()
    metadata.upsert_remote_file(
        source_id="baidu-main",
        remote_path=synced_path,
        size=10,
        md5="same",
        server_mtime=dt.datetime.fromtimestamp(1734652800, tz=dt.timezone.utc).replace(tzinfo=None),
        local_raw_path="/tmp/synced.zip",
    )
    metadata.mark_remote_downloaded("baidu-main", synced_path, "/tmp/synced.zip")
    metadata.upsert_remote_file(
        source_id="baidu-main",
        remote_path=updated_path,
        size=20,
        md5="old",
        server_mtime=dt.datetime.fromtimestamp(1734912000, tz=dt.timezone.utc).replace(tzinfo=None),
        local_raw_path="/tmp/updated.zip",
    )
    metadata.mark_remote_downloaded("baidu-main", updated_path, "/tmp/updated.zip")

    response = TestClient(app).get("/admin/api/baidu/list", params={"path": path, "source_id": "baidu-main"})

    assert response.status_code == 200
    payload = response.json()
    statuses = {entry["name"]: entry["sync_status"] for entry in payload["entries"]}
    assert statuses["按月归档"] == "directory"
    assert statuses["20241220_1min.zip"] == "synced"
    assert statuses["20241223_1min.zip"] == "update_available"
    assert statuses["20241224_1min.zip"] == "not_synced"
    updated = next(entry for entry in payload["entries"] if entry["name"] == "20241223_1min.zip")
    assert "md5" in updated["update_reasons"]


def test_admin_baidu_list_paginates_large_directory(tmp_path):
    settings = Settings(data_root=tmp_path / "data")
    app = create_app(settings)
    path = "/A股_分时数据/1分钟"
    fake_client = FakeBaiduListClient(
        {
            path: [
                {
                    "server_filename": f"202412{i + 1:02d}_1min.zip",
                    "path": f"{path}/202412{i + 1:02d}_1min.zip",
                    "isdir": 0,
                    "size": i,
                    "md5": f"md5-{i}",
                    "server_mtime": 1734652800 + i,
                }
                for i in range(55)
            ]
        }
    )
    app.state.baidu_client_factory = lambda _: fake_client

    response = TestClient(app).get("/admin/api/baidu/list", params={"path": path, "page": 2, "limit": 20})

    assert response.status_code == 200
    payload = response.json()
    assert [entry["name"] for entry in payload["entries"][:2]] == ["20241221_1min.zip", "20241222_1min.zip"]
    assert len(payload["entries"]) == 20
    assert payload["pagination"] == {
        "page": 2,
        "limit": 20,
        "start": 20,
        "returned_count": 20,
        "has_more": True,
        "prev_page": 1,
        "next_page": 3,
        "total": 55,
    }
    assert fake_client.calls[-1][1]["start"] == 20
    assert fake_client.calls[-1][1]["limit"] == 21


def test_admin_file_sync_start(tmp_path):
    client, fake = _client(tmp_path)
    response = client.post(
        "/admin/api/sync/file",
        json={
            "source_id": "baidu-main",
            "timeframe": "1m",
            "start": "2024-12-20",
            "end": "2024-12-20",
            "symbols": ["sh600000"],
            "remote_path": "/A股_分时数据/1分钟/20241220_1min.zip",
        },
    )
    assert response.status_code == 200
    assert fake.file_started.remote_path == "/A股_分时数据/1分钟/20241220_1min.zip"
    assert response.json()["job"]["kind"] == "baidu_file"
    assert response.json()["job"]["progress_percent"] == 10


def test_admin_settings_validation_errors(tmp_path):
    client, _ = _client(tmp_path)
    response = client.post(
        "/admin/api/settings",
        json={"source_id": "x", "timeframe": "bad", "start": "2024-12-20", "end": "2024-12-20", "symbols": ["sh600000"]},
    )
    assert response.status_code == 400
