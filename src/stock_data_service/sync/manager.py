from __future__ import annotations

import datetime as dt
import logging
import threading
import uuid
from dataclasses import asdict, dataclass, field
from typing import Literal

from stock_data_service.auth.token_manager import TokenManager
from stock_data_service.baidu.pan_client import BaiduPanClient
from stock_data_service.config import Settings
from stock_data_service.market.timeframe import Timeframe
from stock_data_service.storage.sync_metadata import SyncMetadata
from stock_data_service.sync.job_runner import BaiduSyncJobRunner, SyncCancelled

logger = logging.getLogger(__name__)

ManagedSyncStatus = Literal["queued", "running", "completed", "completed_with_errors", "failed", "stopping", "stopped"]


def _now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)


def _iso(value: dt.datetime | None) -> str | None:
    return value.isoformat() if value else None


@dataclass(frozen=True)
class ManagedSyncRequest:
    source_id: str
    timeframe: str
    start: str
    end: str
    symbols: list[str]


@dataclass(frozen=True)
class ManagedFileSyncRequest:
    source_id: str
    timeframe: str
    start: str
    end: str
    symbols: list[str]
    remote_path: str


@dataclass
class ManagedSyncJob:
    id: str
    kind: str
    request: ManagedSyncRequest | ManagedFileSyncRequest
    status: ManagedSyncStatus = "queued"
    stage: str = "排队中"
    progress_percent: int = 0
    download_speed_bytes_per_sec: float = 0.0
    downloaded_bytes: int = 0
    download_total_bytes: int | None = None
    current_download_path: str | None = None
    created_at: dt.datetime = field(default_factory=_now)
    started_at: dt.datetime | None = None
    finished_at: dt.datetime | None = None
    scanned_count: int = 0
    downloaded_count: int = 0
    ingested_count: int = 0
    failed_count: int = 0
    error_message: str | None = None
    cancel_event: threading.Event = field(default_factory=threading.Event, repr=False)

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "kind": self.kind,
            "request": asdict(self.request),
            "status": self.status,
            "stage": self.stage,
            "progress_percent": self.progress_percent,
            "download_speed_bytes_per_sec": self.download_speed_bytes_per_sec,
            "downloaded_bytes": self.downloaded_bytes,
            "download_total_bytes": self.download_total_bytes,
            "current_download_path": self.current_download_path,
            "created_at": _iso(self.created_at),
            "started_at": _iso(self.started_at),
            "finished_at": _iso(self.finished_at),
            "scanned_count": self.scanned_count,
            "downloaded_count": self.downloaded_count,
            "ingested_count": self.ingested_count,
            "failed_count": self.failed_count,
            "error_message": self.error_message,
        }


class SyncJobManager:
    def __init__(self, settings: Settings):
        self.settings = settings
        self._lock = threading.Lock()
        self._jobs: dict[str, ManagedSyncJob] = {}
        self._active_job_id: str | None = None

    def start_baidu_sync(self, request: ManagedSyncRequest) -> ManagedSyncJob:
        with self._lock:
            if self._active_job_id and self._jobs[self._active_job_id].status in {"queued", "running", "stopping"}:
                raise RuntimeError("a sync job is already running")
            job = ManagedSyncJob(id=f"admin-sync-{uuid.uuid4()}", kind="baidu", request=request)
            self._jobs[job.id] = job
            self._active_job_id = job.id

        thread = threading.Thread(target=self._run_baidu_job, args=(job.id,), daemon=True)
        thread.start()
        return job

    def start_baidu_file_sync(self, request: ManagedFileSyncRequest) -> ManagedSyncJob:
        with self._lock:
            if self._active_job_id and self._jobs[self._active_job_id].status in {"queued", "running", "stopping"}:
                raise RuntimeError("a sync job is already running")
            job = ManagedSyncJob(id=f"admin-file-sync-{uuid.uuid4()}", kind="baidu_file", request=request)
            self._jobs[job.id] = job
            self._active_job_id = job.id

        thread = threading.Thread(target=self._run_baidu_file_job, args=(job.id,), daemon=True)
        thread.start()
        return job

    def request_stop(self, job_id: str | None = None) -> ManagedSyncJob:
        with self._lock:
            active_id = job_id or self._active_job_id
            if not active_id or active_id not in self._jobs:
                raise RuntimeError("no active sync job")
            job = self._jobs[active_id]
            if job.status not in {"queued", "running", "stopping"}:
                return job
            job.status = "stopping"
            job.stage = "正在停止"
            job.cancel_event.set()
            return job

    def status(self) -> dict:
        with self._lock:
            active = self._jobs.get(self._active_job_id) if self._active_job_id else None
            jobs = sorted(self._jobs.values(), key=lambda item: item.created_at, reverse=True)
            return {
                "active_job": active.to_dict() if active else None,
                "jobs": [job.to_dict() for job in jobs[:20]],
            }

    def _run_baidu_job(self, job_id: str) -> None:
        job = self._jobs[job_id]
        with self._lock:
            job.status = "running"
            job.started_at = _now()
            job.stage = "开始同步"
            job.progress_percent = 5
        logger.info("managed baidu sync started job_id=%s request=%s", job.id, job.request)

        try:
            result = self._build_baidu_runner(job.request).run(
                timeframe=Timeframe.parse(job.request.timeframe),
                start=dt.date.fromisoformat(job.request.start),
                end=dt.date.fromisoformat(job.request.end),
                symbols=job.request.symbols,
                cancel_event=job.cancel_event,
                progress_callback=lambda stage, percent, download: self._update_progress(job_id, stage, percent, download),
            )
        except SyncCancelled as exc:
            with self._lock:
                job.status = "stopped"
                job.finished_at = _now()
                job.error_message = str(exc)
                job.stage = "已停止"
                self._clear_download(job)
                self._active_job_id = None
            logger.info("managed baidu sync stopped job_id=%s", job.id)
        except Exception as exc:
            with self._lock:
                job.status = "failed"
                job.finished_at = _now()
                job.error_message = str(exc)
                job.stage = "同步失败"
                job.progress_percent = 100
                self._clear_download(job)
                self._active_job_id = None
            logger.exception("managed baidu sync failed job_id=%s", job.id)
        else:
            with self._lock:
                job.scanned_count = result.scanned_count
                job.downloaded_count = result.downloaded_count
                job.ingested_count = result.ingested_count
                job.failed_count = result.failed_count
                job.status = "completed" if result.failed_count == 0 else "completed_with_errors"
                job.finished_at = _now()
                job.stage = "同步完成"
                job.progress_percent = 100
                self._clear_download(job)
                self._active_job_id = None
            logger.info("managed baidu sync finished job_id=%s status=%s", job.id, job.status)

    def _run_baidu_file_job(self, job_id: str) -> None:
        job = self._jobs[job_id]
        request = job.request
        if not isinstance(request, ManagedFileSyncRequest):
            raise TypeError("baidu file job requires ManagedFileSyncRequest")
        with self._lock:
            job.status = "running"
            job.started_at = _now()
            job.stage = "开始同步文件"
            job.progress_percent = 5
        logger.info("managed baidu file sync started job_id=%s request=%s", job.id, request)

        try:
            result = self._build_baidu_runner(request).run_file(
                remote_path=request.remote_path,
                timeframe=Timeframe.parse(request.timeframe),
                start=dt.date.fromisoformat(request.start),
                end=dt.date.fromisoformat(request.end),
                symbols=request.symbols,
                cancel_event=job.cancel_event,
                progress_callback=lambda stage, percent, download: self._update_progress(job_id, stage, percent, download),
            )
        except SyncCancelled as exc:
            with self._lock:
                job.status = "stopped"
                job.finished_at = _now()
                job.error_message = str(exc)
                job.stage = "已停止"
                self._clear_download(job)
                self._active_job_id = None
            logger.info("managed baidu file sync stopped job_id=%s", job.id)
        except Exception as exc:
            with self._lock:
                job.status = "failed"
                job.finished_at = _now()
                job.error_message = str(exc)
                job.stage = "同步失败"
                job.progress_percent = 100
                self._clear_download(job)
                self._active_job_id = None
            logger.exception("managed baidu file sync failed job_id=%s", job.id)
        else:
            with self._lock:
                job.scanned_count = result.scanned_count
                job.downloaded_count = result.downloaded_count
                job.ingested_count = result.ingested_count
                job.failed_count = result.failed_count
                job.status = "completed" if result.failed_count == 0 else "completed_with_errors"
                job.finished_at = _now()
                job.stage = "同步完成"
                job.progress_percent = 100
                self._clear_download(job)
                self._active_job_id = None
            logger.info("managed baidu file sync finished job_id=%s status=%s", job.id, job.status)

    def _update_progress(self, job_id: str, stage: str, percent: int, download: dict | None = None) -> None:
        with self._lock:
            job = self._jobs.get(job_id)
            if not job or job.status not in {"queued", "running", "stopping"}:
                return
            job.stage = stage
            job.progress_percent = max(0, min(percent, 100))
            if download is not None:
                job.download_speed_bytes_per_sec = float(download.get("speed_bytes_per_sec") or 0)
                job.downloaded_bytes = int(download.get("bytes_downloaded") or 0)
                total = download.get("total_bytes")
                job.download_total_bytes = int(total) if total is not None else None
                job.current_download_path = download.get("remote_path")
            else:
                self._clear_download(job)

    @staticmethod
    def _clear_download(job: ManagedSyncJob) -> None:
        job.download_speed_bytes_per_sec = 0.0
        job.downloaded_bytes = 0
        job.download_total_bytes = None
        job.current_download_path = None

    def _build_baidu_runner(self, request: ManagedSyncRequest | ManagedFileSyncRequest) -> BaiduSyncJobRunner:
        token_manager = TokenManager(
            token_file=self.settings.baidu_token_file,
            app_key=self.settings.baidu_app_key,
            app_secret=self.settings.baidu_app_secret,
        )
        client = BaiduPanClient(token_manager=token_manager, enable_cache=True, cache_dir=self.settings.baidu_cache_dir)
        return BaiduSyncJobRunner(
            client=client,
            cache_dir=self.settings.baidu_cache_dir,
            parquet_root=self.settings.parquet_root,
            metadata=SyncMetadata(self.settings.metadata_db),
            source_id=request.source_id,
        )
