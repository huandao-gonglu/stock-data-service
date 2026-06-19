from __future__ import annotations

import datetime as dt
import logging
import threading
import uuid
from dataclasses import asdict, dataclass, field
from typing import Any, Literal

from stock_data_service.auth.token_manager import TokenManager
from stock_data_service.baidu.pan_client import BaiduPanClient
from stock_data_service.config import Settings
from stock_data_service.market.timeframe import Timeframe
from stock_data_service.storage.sync_metadata import SyncMetadata
from stock_data_service.sync.job_runner import BaiduSyncJobRunner, SyncCancelled

logger = logging.getLogger(__name__)

_ETA_ARCHIVE_START_PERCENT = 10.0
_ETA_ARCHIVE_SPAN_PERCENT = 88.0
_ETA_ARCHIVE_DOWNLOAD_FRACTION = 0.4

ManagedSyncStatus = Literal["queued", "running", "completed", "completed_with_errors", "failed", "stopping", "stopped"]


def _now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)


def _iso(value: dt.datetime | None) -> str | None:
    return value.isoformat() if value else None


def _optional_int(value: Any) -> int | None:
    if value is None:
        return None
    return int(value)


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value)
    return text if text else None


def _request_payload(request: ManagedSyncRequest | ManagedFileSyncRequest) -> dict:
    payload = asdict(request)
    symbols = payload.get("symbols")
    if isinstance(symbols, list) and len(symbols) > 100:
        payload["symbol_count"] = len(symbols)
        payload["symbols"] = symbols[:20]
        payload["symbols_truncated"] = True
    return payload


def _request_log_summary(request: ManagedSyncRequest | ManagedFileSyncRequest) -> dict:
    payload = asdict(request)
    symbols = payload.pop("symbols", [])
    payload["symbol_count"] = len(symbols) if isinstance(symbols, list) else 0
    return payload


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
    planned_download_count: int | None = None
    completed_archive_count: int = 0
    ingest_processed_count: int = 0
    ingest_total_count: int | None = None
    current_archive_ingest_processed_count: int = 0
    current_archive_ingest_total_count: int | None = None
    current_archive_requested_count: int | None = None
    current_archive_present_count: int | None = None
    current_archive_missing_count: int = 0
    current_ingest_symbol: str | None = None
    current_ingest_path: str | None = None
    current_ingest_status: str | None = None
    eta_seconds: int | None = None
    eta_confidence: str = "warming_up"
    progress_rate_percent_per_min: float | None = None
    error_message: str | None = None
    cancel_event: threading.Event = field(default_factory=threading.Event, repr=False)
    _eta_last_at: dt.datetime | None = field(default=None, repr=False)
    _eta_last_progress: float | None = field(default=None, repr=False)
    _eta_rate_percent_per_sec: float | None = field(default=None, repr=False)
    _eta_sample_count: int = field(default=0, repr=False)

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "kind": self.kind,
            "request": _request_payload(self.request),
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
            "planned_download_count": self.planned_download_count,
            "completed_archive_count": self.completed_archive_count,
            "ingest_processed_count": self.ingest_processed_count,
            "ingest_total_count": self.ingest_total_count,
            "current_archive_ingest_processed_count": self.current_archive_ingest_processed_count,
            "current_archive_ingest_total_count": self.current_archive_ingest_total_count,
            "current_archive_requested_count": self.current_archive_requested_count,
            "current_archive_present_count": self.current_archive_present_count,
            "current_archive_missing_count": self.current_archive_missing_count,
            "current_ingest_symbol": self.current_ingest_symbol,
            "current_ingest_path": self.current_ingest_path,
            "current_ingest_status": self.current_ingest_status,
            "eta_seconds": self.eta_seconds,
            "eta_confidence": self.eta_confidence,
            "progress_rate_percent_per_min": self.progress_rate_percent_per_min,
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
            self._clear_eta(job)
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
            self._prime_eta(job)
        logger.info("managed baidu sync started job_id=%s request=%s", job.id, _request_log_summary(job.request))

        try:
            result = self._build_baidu_runner(job.request).run(
                timeframe=Timeframe.parse(job.request.timeframe),
                start=dt.date.fromisoformat(job.request.start),
                end=dt.date.fromisoformat(job.request.end),
                symbols=job.request.symbols,
                cancel_event=job.cancel_event,
                progress_callback=lambda stage, percent, download, counts=None: self._update_progress(
                    job_id, stage, percent, download, counts
                ),
            )
        except SyncCancelled as exc:
            with self._lock:
                job.status = "stopped"
                job.finished_at = _now()
                job.error_message = str(exc)
                job.stage = "已停止"
                self._clear_download(job)
                self._clear_ingest(job)
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
                self._clear_ingest(job)
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
                self._finish_ingest(job)
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
            self._prime_eta(job)
        logger.info("managed baidu file sync started job_id=%s request=%s", job.id, _request_log_summary(request))

        try:
            result = self._build_baidu_runner(request).run_file(
                remote_path=request.remote_path,
                timeframe=Timeframe.parse(request.timeframe),
                start=dt.date.fromisoformat(request.start),
                end=dt.date.fromisoformat(request.end),
                symbols=request.symbols,
                cancel_event=job.cancel_event,
                progress_callback=lambda stage, percent, download, counts=None: self._update_progress(
                    job_id, stage, percent, download, counts
                ),
            )
        except SyncCancelled as exc:
            with self._lock:
                job.status = "stopped"
                job.finished_at = _now()
                job.error_message = str(exc)
                job.stage = "已停止"
                self._clear_download(job)
                self._clear_ingest(job)
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
                self._clear_ingest(job)
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
                self._finish_ingest(job)
                self._active_job_id = None
            logger.info("managed baidu file sync finished job_id=%s status=%s", job.id, job.status)

    def _update_progress(
        self,
        job_id: str,
        stage: str,
        percent: int,
        download: dict | None = None,
        counts: dict | None = None,
    ) -> None:
        with self._lock:
            job = self._jobs.get(job_id)
            if not job or job.status not in {"queued", "running", "stopping"}:
                return
            download_only_update = download is not None and counts is None
            preserve_ingest_stage = download_only_update and (job.downloaded_count > 0 or job.ingested_count > 0)
            if job.status != "stopping" and not preserve_ingest_stage:
                job.stage = stage
            next_percent = max(0, min(percent, 100))
            job.progress_percent = max(job.progress_percent, next_percent)
            if counts is not None:
                if "scanned_count" in counts:
                    job.scanned_count = int(counts.get("scanned_count") or 0)
                if "downloaded_count" in counts:
                    job.downloaded_count = int(counts.get("downloaded_count") or 0)
                if "ingested_count" in counts:
                    job.ingested_count = int(counts.get("ingested_count") or 0)
                if "failed_count" in counts:
                    job.failed_count = int(counts.get("failed_count") or 0)
                if "planned_download_count" in counts:
                    job.planned_download_count = _optional_int(counts.get("planned_download_count"))
                if "completed_archive_count" in counts:
                    job.completed_archive_count = int(counts.get("completed_archive_count") or 0)
                if "ingest_processed_count" in counts:
                    job.ingest_processed_count = int(counts.get("ingest_processed_count") or 0)
                if "ingest_total_count" in counts:
                    job.ingest_total_count = _optional_int(counts.get("ingest_total_count"))
                if "current_archive_ingest_processed_count" in counts:
                    job.current_archive_ingest_processed_count = int(
                        counts.get("current_archive_ingest_processed_count") or 0
                    )
                if "current_archive_ingest_total_count" in counts:
                    job.current_archive_ingest_total_count = _optional_int(
                        counts.get("current_archive_ingest_total_count")
                    )
                if "current_archive_requested_count" in counts:
                    job.current_archive_requested_count = _optional_int(counts.get("current_archive_requested_count"))
                if "current_archive_present_count" in counts:
                    job.current_archive_present_count = _optional_int(counts.get("current_archive_present_count"))
                if "current_archive_missing_count" in counts:
                    job.current_archive_missing_count = int(counts.get("current_archive_missing_count") or 0)
                if "current_ingest_symbol" in counts:
                    job.current_ingest_symbol = _optional_text(counts.get("current_ingest_symbol"))
                if "current_ingest_path" in counts:
                    job.current_ingest_path = _optional_text(counts.get("current_ingest_path"))
                if "current_ingest_status" in counts:
                    job.current_ingest_status = _optional_text(counts.get("current_ingest_status"))
            if download is not None:
                job.download_speed_bytes_per_sec = float(download.get("speed_bytes_per_sec") or 0)
                job.downloaded_bytes = int(download.get("bytes_downloaded") or 0)
                total = download.get("total_bytes")
                job.download_total_bytes = int(total) if total is not None else None
                job.current_download_path = download.get("remote_path")
            else:
                self._clear_download(job)
            self._update_eta(job)

    @staticmethod
    def _clear_download(job: ManagedSyncJob) -> None:
        job.download_speed_bytes_per_sec = 0.0
        job.downloaded_bytes = 0
        job.download_total_bytes = None
        job.current_download_path = None

    @staticmethod
    def _clear_ingest(job: ManagedSyncJob) -> None:
        job.current_ingest_symbol = None
        job.current_ingest_path = None
        job.current_ingest_status = None
        SyncJobManager._clear_eta(job)

    @staticmethod
    def _clear_eta(job: ManagedSyncJob) -> None:
        job.eta_seconds = None
        job.eta_confidence = "none"
        job.progress_rate_percent_per_min = None
        job._eta_last_at = None
        job._eta_last_progress = None
        job._eta_rate_percent_per_sec = None
        job._eta_sample_count = 0

    @staticmethod
    def _prime_eta(job: ManagedSyncJob) -> None:
        job.eta_seconds = None
        job.eta_confidence = "warming_up"
        job.progress_rate_percent_per_min = None
        job._eta_last_at = _now()
        job._eta_last_progress = SyncJobManager._eta_progress_value(job)
        job._eta_rate_percent_per_sec = None
        job._eta_sample_count = 0

    @staticmethod
    def _update_eta(job: ManagedSyncJob) -> None:
        if job.status not in {"queued", "running"}:
            SyncJobManager._clear_eta(job)
            return

        now = _now()
        progress = SyncJobManager._eta_progress_value(job)
        if job._eta_last_at is None or job._eta_last_progress is None:
            job._eta_last_at = now
            job._eta_last_progress = progress
            job.eta_seconds = None
            job.eta_confidence = "warming_up"
            return

        elapsed = (now - job._eta_last_at).total_seconds()
        advanced = progress - job._eta_last_progress
        if elapsed < 1 or advanced <= 0:
            return

        instant_rate = advanced / elapsed
        previous_rate = job._eta_rate_percent_per_sec
        if previous_rate is None:
            rate = instant_rate
        else:
            rate = previous_rate * 0.65 + instant_rate * 0.35
        if rate <= 0:
            return

        job._eta_rate_percent_per_sec = rate
        job._eta_sample_count += 1
        remaining = max(100.0 - progress, 0.0)
        job.eta_seconds = 0 if remaining == 0 else max(1, int(round(remaining / rate)))
        job.progress_rate_percent_per_min = round(rate * 60.0, 2)
        if previous_rate is None or job._eta_sample_count < 2:
            job.eta_confidence = "warming_up"
        else:
            ratio = instant_rate / previous_rate if previous_rate > 0 else 1.0
            job.eta_confidence = "volatile" if ratio < 0.4 or ratio > 2.5 else "stable"
        job._eta_last_at = now
        job._eta_last_progress = progress

    @staticmethod
    def _eta_progress_value(job: ManagedSyncJob) -> float:
        if job.progress_percent >= 100:
            return 100.0
        count_progress = SyncJobManager._eta_count_progress_value(job)
        if count_progress is not None:
            return count_progress
        progress = float(max(0, min(job.progress_percent, 100)))
        if job.ingest_total_count and job.ingest_total_count > 0 and (
            progress >= 45 or job.downloaded_count > 0 or job.ingest_processed_count > 0
        ):
            ingest_ratio = min(max(job.ingest_processed_count, 0) / max(job.ingest_total_count, 1), 1.0)
            progress = max(progress, 45.0 + ingest_ratio * 53.0)
        elif job.planned_download_count and job.planned_download_count > 0:
            scan_ratio = min(max(job.scanned_count, 0) / max(job.planned_download_count, 1), 1.0)
            progress = max(progress, 10.0 + scan_ratio * 35.0)
        return max(0.0, min(progress, 100.0))

    @staticmethod
    def _eta_count_progress_value(job: ManagedSyncJob) -> float | None:
        planned = job.planned_download_count
        if planned is not None and planned > 0:
            completed_units = min(max(job.completed_archive_count, 0), planned)
            current_fraction = SyncJobManager._eta_current_archive_fraction(job, completed_units, planned)
            archive_ratio = min((completed_units + current_fraction) / planned, 1.0)
            progress = _ETA_ARCHIVE_START_PERCENT + archive_ratio * _ETA_ARCHIVE_SPAN_PERCENT
            return max(0.0, min(progress, 98.0))

        ingest_total = job.ingest_total_count
        if ingest_total is not None and ingest_total > 0 and job.ingest_processed_count > 0:
            ingest_ratio = min(max(job.ingest_processed_count, 0) / ingest_total, 1.0)
            return max(0.0, min(45.0 + ingest_ratio * 53.0, 98.0))

        return None

    @staticmethod
    def _eta_current_archive_fraction(job: ManagedSyncJob, completed_units: int, planned_units: int) -> float:
        if completed_units >= planned_units:
            return 0.0

        download_fraction = SyncJobManager._eta_current_download_fraction(job)
        if download_fraction > 0:
            return min(download_fraction * _ETA_ARCHIVE_DOWNLOAD_FRACTION, _ETA_ARCHIVE_DOWNLOAD_FRACTION)

        if job.downloaded_count <= completed_units:
            return 0.0

        ingest_fraction = SyncJobManager._eta_current_archive_ingest_fraction(job)
        return min(_ETA_ARCHIVE_DOWNLOAD_FRACTION + ingest_fraction * (1.0 - _ETA_ARCHIVE_DOWNLOAD_FRACTION), 1.0)

    @staticmethod
    def _eta_current_download_fraction(job: ManagedSyncJob) -> float:
        if not job.current_download_path or not job.download_total_bytes or job.download_total_bytes <= 0:
            return 0.0
        return min(max(job.downloaded_bytes, 0) / job.download_total_bytes, 1.0)

    @staticmethod
    def _eta_current_archive_ingest_fraction(job: ManagedSyncJob) -> float:
        present = job.current_archive_present_count
        if present is None:
            present = job.current_archive_ingest_total_count
        if present is None:
            return 0.0
        if present <= 0:
            return 1.0
        processed = max(job.current_archive_ingest_processed_count, 0)
        return min(processed / present, 1.0)

    @staticmethod
    def _finish_ingest(job: ManagedSyncJob) -> None:
        if job.ingest_total_count is not None:
            job.ingest_processed_count = job.ingest_total_count
        elif job.ingest_processed_count:
            job.ingest_total_count = job.ingest_processed_count
        job.current_archive_ingest_processed_count = 0
        job.current_archive_ingest_total_count = None
        job.current_archive_requested_count = None
        job.current_archive_present_count = None
        job.current_archive_missing_count = 0
        SyncJobManager._clear_ingest(job)

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
