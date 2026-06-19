from __future__ import annotations

import datetime as dt
import logging
import queue
import re
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from stock_data_service.baidu.pan_client import BaiduPanClient
from stock_data_service.market.path_strategy import BaiduStockKPathStrategy, RemoteFileCandidate, RemotePathStrategy
from stock_data_service.market.symbol_normalizer import normalize_symbol
from stock_data_service.market.timeframe import Timeframe
from stock_data_service.storage.parquet_writer import ParquetBarWriter
from stock_data_service.storage.sync_metadata import SyncMetadata
from stock_data_service.sync.downloader import BaiduDownloadResult, BaiduDownloader
from stock_data_service.sync.ingestor import Ingestor
from stock_data_service.sync.local_source import LocalRawFile, LocalRawScanner
from stock_data_service.sync.locks import ProcessLock

logger = logging.getLogger(__name__)
ProgressCallback = Callable[[str, int, dict | None, dict[str, Any] | None], None]
DOWNLOAD_RESULT_QUEUE_SIZE = 2


def _symbols_log_summary(symbols: list[str]) -> dict:
    return {
        "count": len(symbols),
        "sample": symbols[:20],
        "truncated": len(symbols) > 20,
    }


@dataclass
class SyncJobResult:
    job_id: str
    scanned_count: int
    downloaded_count: int
    ingested_count: int
    failed_count: int


class SyncCancelled(RuntimeError):
    pass


class LocalSyncJobRunner:
    def __init__(
        self,
        *,
        raw_root: str | Path,
        parquet_root: str | Path,
        metadata: SyncMetadata,
        source_id: str = "local-raw",
    ):
        self.raw_root = Path(raw_root)
        self.parquet_root = Path(parquet_root)
        self.metadata = metadata
        self.source_id = source_id

    def run(
        self,
        *,
        timeframe: Timeframe,
        start: dt.date,
        end: dt.date,
        symbols: list[str],
        cancel_event: threading.Event | None = None,
        progress_callback: ProgressCallback | None = None,
    ) -> SyncJobResult:
        lock_path = self.metadata.db_path.parent / "sync.lock"
        with ProcessLock(lock_path, timeout_seconds=0.5):
            return self._run_unlocked(
                timeframe=timeframe,
                start=start,
                end=end,
                symbols=symbols,
                cancel_event=cancel_event,
                progress_callback=progress_callback,
            )

    def _run_unlocked(
        self,
        *,
        timeframe: Timeframe,
        start: dt.date,
        end: dt.date,
        symbols: list[str],
        cancel_event: threading.Event | None = None,
        progress_callback: ProgressCallback | None = None,
    ) -> SyncJobResult:
        self.metadata.initialize()
        logger.info(
            "local sync started source_id=%s raw_root=%s parquet_root=%s timeframe=%s start=%s end=%s symbols=%s",
            self.source_id,
            self.raw_root,
            self.parquet_root,
            timeframe.value,
            start,
            end,
            _symbols_log_summary(symbols),
        )
        self.metadata.upsert_source(
            source_id=self.source_id,
            source_type="local_raw",
            name="Local raw zip cache",
            root_path=str(self.raw_root),
        )
        job_id = self.metadata.create_sync_job(self.source_id)
        scanned = 0
        ingested = 0
        failed = 0
        try:
            files = LocalRawScanner(self.raw_root).scan(timeframe=timeframe, start=start, end=end)
            scanned = len(files)
            logger.info("local sync scanned files=%s job_id=%s", scanned, job_id)
            writer = ParquetBarWriter(self.parquet_root)
            ingestor = Ingestor(writer=writer, metadata=self.metadata)
            start_dt = dt.datetime.combine(start, dt.time.min)
            end_dt = dt.datetime.combine(end + dt.timedelta(days=1), dt.time.min)
            normalized_symbols = [normalize_symbol(symbol) for symbol in symbols]
            for raw_file in files:
                _raise_if_cancelled(cancel_event)
                status = self.metadata.upsert_remote_file(
                    source_id=self.source_id,
                    remote_path=raw_file.remote_path,
                    size=raw_file.size,
                    content_hash=raw_file.content_hash,
                    local_raw_path=str(raw_file.local_path),
                )
                self.metadata.mark_remote_downloaded(self.source_id, raw_file.remote_path, str(raw_file.local_path))
                logger.info(
                    "local raw file ready job_id=%s remote_path=%s local_path=%s status=%s",
                    job_id,
                    raw_file.remote_path,
                    raw_file.local_path,
                    status,
                )
                outcome = ingestor.ingest_archive_result(
                    raw_file,
                    symbols=normalized_symbols,
                    source_id=self.source_id,
                    start=start_dt,
                    end=end_dt,
                    cancel_check=lambda: _raise_if_cancelled(cancel_event),
                )
                ingested += outcome.committed_count
                failed += outcome.failed_count
            self.metadata.complete_sync_job(
                job_id,
                scanned_count=scanned,
                downloaded_count=scanned,
                ingested_count=ingested,
                failed_count=failed,
                status="completed" if failed == 0 else "completed_with_errors",
            )
            logger.info(
                "local sync finished job_id=%s scanned=%s ingested=%s failed=%s",
                job_id,
                scanned,
                ingested,
                failed,
            )
        except Exception as exc:
            if isinstance(exc, SyncCancelled):
                logger.info("local sync cancelled job_id=%s", job_id)
                self.metadata.complete_sync_job(
                    job_id,
                    scanned_count=scanned,
                    downloaded_count=scanned,
                    ingested_count=ingested,
                    failed_count=failed,
                    status="stopped",
                    error_message=str(exc),
                )
                raise
            logger.exception("local sync failed job_id=%s", job_id)
            self.metadata.complete_sync_job(
                job_id,
                scanned_count=scanned,
                downloaded_count=scanned,
                ingested_count=ingested,
                failed_count=failed + 1,
                status="failed",
                error_message=str(exc),
            )
            raise
        return SyncJobResult(
            job_id=job_id,
            scanned_count=scanned,
            downloaded_count=scanned,
            ingested_count=ingested,
            failed_count=failed,
        )


class BaiduSyncJobRunner:
    def __init__(
        self,
        *,
        client: BaiduPanClient,
        cache_dir: str | Path,
        parquet_root: str | Path,
        metadata: SyncMetadata,
        source_id: str = "baidu-main",
        path_strategy: RemotePathStrategy | None = None,
    ):
        self.client = client
        self.cache_dir = Path(cache_dir)
        self.parquet_root = Path(parquet_root)
        self.metadata = metadata
        self.source_id = source_id
        self.path_strategy = path_strategy or BaiduStockKPathStrategy()

    def run(
        self,
        *,
        timeframe: Timeframe,
        start: dt.date,
        end: dt.date,
        symbols: list[str],
        cancel_event: threading.Event | None = None,
        progress_callback: ProgressCallback | None = None,
    ) -> SyncJobResult:
        lock_path = self.metadata.db_path.parent / "sync.lock"
        with ProcessLock(lock_path, timeout_seconds=0.5):
            return self._run_unlocked(
                timeframe=timeframe,
                start=start,
                end=end,
                symbols=symbols,
                cancel_event=cancel_event,
                progress_callback=progress_callback,
            )

    def run_file(
        self,
        *,
        remote_path: str,
        timeframe: Timeframe,
        start: dt.date,
        end: dt.date,
        symbols: list[str],
        cancel_event: threading.Event | None = None,
        progress_callback: ProgressCallback | None = None,
    ) -> SyncJobResult:
        lock_path = self.metadata.db_path.parent / "sync.lock"
        with ProcessLock(lock_path, timeout_seconds=0.5):
            return self._run_file_unlocked(
                remote_path=remote_path,
                timeframe=timeframe,
                start=start,
                end=end,
                symbols=symbols,
                cancel_event=cancel_event,
                progress_callback=progress_callback,
            )

    def _run_unlocked(
        self,
        *,
        timeframe: Timeframe,
        start: dt.date,
        end: dt.date,
        symbols: list[str],
        cancel_event: threading.Event | None = None,
        progress_callback: ProgressCallback | None = None,
    ) -> SyncJobResult:
        self.metadata.initialize()
        logger.info(
            "baidu sync started source_id=%s cache_dir=%s parquet_root=%s timeframe=%s start=%s end=%s symbols=%s",
            self.source_id,
            self.cache_dir,
            self.parquet_root,
            timeframe.value,
            start,
            end,
            _symbols_log_summary(symbols),
        )
        self.metadata.upsert_source(
            source_id=self.source_id,
            source_type="baidu_netdisk",
            name="Baidu Netdisk StockK source",
            root_path=getattr(self.path_strategy, "data_dir", "/A股_分时数据"),
        )
        job_id = self.metadata.create_sync_job(self.source_id)
        scanned = 0
        downloaded = 0
        ingested = 0
        failed = 0
        try:
            normalized_symbols = [normalize_symbol(symbol) for symbol in symbols]
            candidate_dates = _candidate_trade_dates(self.path_strategy.candidates_for_range(timeframe, start, end))
            required_dates = self.metadata.dates_requiring_sync(
                symbols=normalized_symbols,
                timeframe=timeframe.value,
                trade_dates=candidate_dates,
            )
            logger.info(
                "baidu sync required dates resolved job_id=%s requested_dates=%s required_dates=%s",
                job_id,
                len(candidate_dates),
                len(required_dates),
            )
            skip_remote_paths = _already_committed_archive_paths(
                metadata=self.metadata,
                source_id=self.source_id,
                timeframe=timeframe,
                symbols=normalized_symbols,
                path_strategy=self.path_strategy,
                trade_dates=required_dates,
            )
            if skip_remote_paths:
                logger.info(
                    "baidu sync skipping already-ingested archives job_id=%s path_count=%s",
                    job_id,
                    len(skip_remote_paths),
                )
            source_preferences = _source_preferences_for_dates(required_dates)
            planned_download_count = len(
                _planned_download_paths_for_dates(
                    path_strategy=self.path_strategy,
                    timeframe=timeframe,
                    trade_dates=required_dates,
                    source_preferences=source_preferences,
                    skip_remote_paths=skip_remote_paths,
                )
            )
            ingest_processed = 0
            ingest_total = planned_download_count * len(normalized_symbols)
            writer = ParquetBarWriter(self.parquet_root)
            ingestor = Ingestor(writer=writer, metadata=self.metadata)
            start_dt = dt.datetime.combine(start, dt.time.min)
            end_dt = dt.datetime.combine(end + dt.timedelta(days=1), dt.time.min)
            active_cancel_event = cancel_event or threading.Event()
            downloader = BaiduDownloader(self.client, self.cache_dir, path_strategy=self.path_strategy)
            download_queue: queue.Queue[BaiduDownloadResult | BaseException | object] = queue.Queue(
                maxsize=DOWNLOAD_RESULT_QUEUE_SIZE
            )
            done_marker = object()
            _report_progress(
                progress_callback,
                "准备下载",
                10,
                counts=_counts_payload(
                    scanned,
                    downloaded,
                    ingested,
                    failed,
                    planned_download_count=planned_download_count,
                            ingest_processed_count=ingest_processed,
                            ingest_total_count=ingest_total,
                            current_archive_ingest_processed_count=0,
                            current_archive_ingest_total_count=len(normalized_symbols),
                        ),
                    )

            def put_download_item(item: BaiduDownloadResult | BaseException | object) -> None:
                while not active_cancel_event.is_set():
                    try:
                        download_queue.put(item, timeout=0.5)
                        return
                    except queue.Full:
                        continue

            def produce_downloads() -> None:
                try:
                    for item in downloader.iter_download_for_trade_dates(
                        timeframe=timeframe,
                        trade_dates=required_dates,
                        source_preferences=source_preferences,
                        skip_remote_paths=skip_remote_paths,
                        cancel_event=active_cancel_event,
                        progress_callback=lambda progress: _report_download_progress(progress_callback, progress, 10, 35),
                    ):
                        put_download_item(item)
                        if active_cancel_event.is_set():
                            return
                except BaseException as exc:
                    put_download_item(exc)
                finally:
                    put_download_item(done_marker)

            producer = threading.Thread(target=produce_downloads, name=f"{job_id}-downloader", daemon=True)
            producer.start()

            stream_finished = False
            try:
                while True:
                    _raise_if_cancelled(active_cancel_event)
                    try:
                        queued = download_queue.get(timeout=0.5)
                    except queue.Empty:
                        if active_cancel_event.is_set():
                            raise SyncCancelled("sync stop requested")
                        if not producer.is_alive():
                            break
                        continue
                    if queued is done_marker:
                        break
                    if isinstance(queued, BaseException):
                        active_cancel_event.set()
                        producer.join(timeout=5)
                        if isinstance(queued, SyncCancelled) or active_cancel_event.is_set():
                            raise SyncCancelled("sync stop requested") from queued
                        raise queued

                    result = queued
                    scanned += 1
                    _report_progress(
                        progress_callback,
                        "已扫描下载结果",
                        35,
                        counts=_counts_payload(
                            scanned,
                            downloaded,
                            ingested,
                            failed,
                            planned_download_count=max(planned_download_count, scanned),
                            ingest_processed_count=ingest_processed,
                            ingest_total_count=max(ingest_total, downloaded * len(normalized_symbols), ingest_processed),
                            current_archive_ingest_processed_count=0,
                            current_archive_ingest_total_count=len(normalized_symbols),
                        ),
                    )
                    if not result.remote_path:
                        _report_progress(
                            progress_callback,
                            "未找到远端文件",
                            35,
                            counts=_counts_payload(
                                scanned,
                                downloaded,
                                ingested,
                                failed,
                                planned_download_count=max(planned_download_count, scanned),
                                ingest_processed_count=ingest_processed,
                                ingest_total_count=max(ingest_total, downloaded * len(normalized_symbols), ingest_processed),
                                current_archive_ingest_processed_count=0,
                                current_archive_ingest_total_count=len(normalized_symbols),
                            ),
                        )
                        logger.info(
                            "baidu sync skipped missing trade date job_id=%s trade_date=%s timeframe=%s reason=%s",
                            job_id,
                            result.trade_date,
                            timeframe.value,
                            result.error_message,
                        )
                        continue

                    if result.status == "missing":
                        logger.info(
                            "baidu sync candidate missing job_id=%s remote_path=%s trade_date=%s",
                            job_id,
                            result.remote_path,
                            result.trade_date,
                        )
                        continue

                    self.metadata.upsert_remote_file(
                        source_id=self.source_id,
                        remote_path=result.remote_path,
                        size=result.size,
                        md5=result.md5,
                        server_mtime=result.server_mtime,
                        content_hash=result.content_hash,
                        local_raw_path=str(result.local_path) if result.local_path else None,
                    )

                    if result.status == "failed" or result.local_path is None or result.content_hash is None:
                        failed += 1
                        _report_progress(
                            progress_callback,
                            "下载失败",
                            35,
                            counts=_counts_payload(
                                scanned,
                                downloaded,
                                ingested,
                                failed,
                                planned_download_count=max(planned_download_count, scanned),
                                ingest_processed_count=ingest_processed,
                                ingest_total_count=max(ingest_total, downloaded * len(normalized_symbols), ingest_processed),
                                current_archive_ingest_processed_count=0,
                                current_archive_ingest_total_count=len(normalized_symbols),
                            ),
                        )
                        logger.warning(
                            "baidu sync download failed job_id=%s remote_path=%s error=%s",
                            job_id,
                            result.remote_path,
                            result.error_message,
                        )
                        self.metadata.mark_remote_failed(
                            self.source_id,
                            result.remote_path,
                            result.error_message or "download failed",
                        )
                        continue

                    downloaded += 1
                    _report_progress(
                        progress_callback,
                        "下载完成，开始入库",
                        45,
                        counts=_counts_payload(
                            scanned,
                            downloaded,
                            ingested,
                            failed,
                            planned_download_count=max(planned_download_count, scanned),
                            ingest_processed_count=ingest_processed,
                            ingest_total_count=max(ingest_total, downloaded * len(normalized_symbols), ingest_processed),
                            current_archive_ingest_processed_count=0,
                            current_archive_ingest_total_count=len(normalized_symbols),
                            current_ingest_path=result.remote_path,
                        ),
                    )
                    self.metadata.mark_remote_downloaded(self.source_id, result.remote_path, str(result.local_path))
                    logger.info(
                        "baidu file downloaded job_id=%s remote_path=%s local_path=%s source_kind=%s size=%s",
                        job_id,
                        result.remote_path,
                        result.local_path,
                        result.source_kind,
                        result.size,
                    )
                    raw_file = _raw_file_from_baidu_result(result)
                    archive_committed = 0
                    archive_failed = 0
                    archive_processed = 0
                    archive_requested = len(normalized_symbols)
                    archive_present = len(normalized_symbols)
                    archive_missing = 0

                    def report_ingest_progress(symbol: str, status_result) -> None:
                        nonlocal ingest_processed, archive_committed, archive_failed, archive_processed
                        nonlocal archive_requested, archive_present, archive_missing
                        if status_result.archive_requested_count is not None:
                            archive_requested = status_result.archive_requested_count
                        if status_result.archive_present_count is not None:
                            archive_present = status_result.archive_present_count
                        if status_result.archive_missing_count is not None:
                            archive_missing = status_result.archive_missing_count
                        if status_result.status != "ingesting":
                            event_count = max(int(getattr(status_result, "count", 1) or 0), 0)
                            ingest_processed += event_count
                            if symbol:
                                archive_processed += event_count
                            if status_result.committed:
                                archive_committed += event_count
                            elif not status_result.skipped:
                                archive_failed += event_count
                        current_ingest_total = max(
                            ingest_total,
                            downloaded * len(normalized_symbols),
                            ingest_processed,
                        )
                        stage_symbol = symbol or ("归档未包含" if status_result.status == "symbol_missing" else status_result.status)
                        _report_progress(
                            progress_callback,
                            f"入库 {stage_symbol}",
                            _ingest_progress_percent(ingest_processed, current_ingest_total),
                            counts=_counts_payload(
                                scanned,
                                downloaded,
                                ingested + archive_committed,
                                failed + archive_failed,
                                planned_download_count=max(planned_download_count, scanned),
                                ingest_processed_count=ingest_processed,
                                ingest_total_count=current_ingest_total,
                                current_archive_ingest_processed_count=archive_processed,
                                current_archive_ingest_total_count=archive_present,
                                current_archive_requested_count=archive_requested,
                                current_archive_present_count=archive_present,
                                current_archive_missing_count=archive_missing,
                                current_ingest_symbol=symbol or None,
                                current_ingest_path=result.remote_path,
                                current_ingest_status=status_result.status,
                            ),
                        )

                    outcome = ingestor.ingest_archive_result(
                        raw_file,
                        symbols=normalized_symbols,
                        source_id=self.source_id,
                        start=start_dt,
                        end=end_dt,
                        cancel_check=lambda: _raise_if_cancelled(active_cancel_event),
                        progress_callback=report_ingest_progress,
                    )
                    ingested += outcome.committed_count
                    failed += outcome.failed_count
                    current_ingest_total = max(ingest_total, downloaded * len(normalized_symbols), ingest_processed)
                    _report_progress(
                        progress_callback,
                        f"Archive processed {Path(result.remote_path).name}",
                        _ingest_progress_percent(ingest_processed, current_ingest_total),
                        counts=_counts_payload(
                            scanned,
                            downloaded,
                            ingested,
                            failed,
                            planned_download_count=max(planned_download_count, scanned),
                            ingest_processed_count=ingest_processed,
                            ingest_total_count=current_ingest_total,
                            current_archive_ingest_processed_count=archive_processed,
                            current_archive_ingest_total_count=archive_present,
                            current_archive_requested_count=archive_requested,
                            current_archive_present_count=archive_present,
                            current_archive_missing_count=archive_missing,
                            current_ingest_path=result.remote_path,
                        ),
                    )
                producer.join(timeout=5)
                stream_finished = True
            finally:
                if not stream_finished:
                    active_cancel_event.set()
                    producer.join(timeout=5)
            logger.info("baidu sync download stream finished job_id=%s result_count=%s", job_id, scanned)
            self.metadata.complete_sync_job(
                job_id,
                scanned_count=scanned,
                downloaded_count=downloaded,
                ingested_count=ingested,
                failed_count=failed,
                status="completed" if failed == 0 else "completed_with_errors",
            )
            logger.info(
                "baidu sync finished job_id=%s scanned=%s downloaded=%s ingested=%s failed=%s",
                job_id,
                scanned,
                downloaded,
                ingested,
                failed,
            )
        except Exception as exc:
            if isinstance(exc, SyncCancelled):
                logger.info("baidu sync cancelled job_id=%s", job_id)
                self.metadata.complete_sync_job(
                    job_id,
                    scanned_count=scanned,
                    downloaded_count=downloaded,
                    ingested_count=ingested,
                    failed_count=failed,
                    status="stopped",
                    error_message=str(exc),
                )
                raise
            logger.exception("baidu sync failed job_id=%s", job_id)
            self.metadata.complete_sync_job(
                job_id,
                scanned_count=scanned,
                downloaded_count=downloaded,
                ingested_count=ingested,
                failed_count=failed + 1,
                status="failed",
                error_message=str(exc),
            )
            raise
        return SyncJobResult(
            job_id=job_id,
            scanned_count=scanned,
            downloaded_count=downloaded,
            ingested_count=ingested,
            failed_count=failed,
        )

    def _run_file_unlocked(
        self,
        *,
        remote_path: str,
        timeframe: Timeframe,
        start: dt.date,
        end: dt.date,
        symbols: list[str],
        cancel_event: threading.Event | None = None,
        progress_callback: ProgressCallback | None = None,
    ) -> SyncJobResult:
        self.metadata.initialize()
        logger.info(
            "baidu file sync started source_id=%s remote_path=%s timeframe=%s start=%s end=%s symbols=%s",
            self.source_id,
            remote_path,
            timeframe.value,
            start,
            end,
            _symbols_log_summary(symbols),
        )
        self.metadata.upsert_source(
            source_id=self.source_id,
            source_type="baidu_netdisk",
            name="Baidu Netdisk StockK source",
            root_path=getattr(self.path_strategy, "data_dir", "/A股_分时数据"),
        )
        job_id = self.metadata.create_sync_job(self.source_id)
        scanned = 1
        downloaded = 0
        ingested = 0
        failed = 0
        ingest_processed = 0
        ingest_total = len(symbols)
        try:
            _report_progress(
                progress_callback,
                "检查远端文件",
                10,
                counts=_counts_payload(
                    scanned,
                    downloaded,
                    ingested,
                    failed,
                    planned_download_count=1,
                    ingest_processed_count=ingest_processed,
                    ingest_total_count=ingest_total,
                    current_archive_ingest_processed_count=0,
                    current_archive_ingest_total_count=ingest_total,
                ),
            )
            _raise_if_cancelled(cancel_event)
            candidate = RemoteFileCandidate(
                remote_path=remote_path,
                trade_date=_trade_date_from_path(remote_path) or start,
                source_kind="manual",
            )
            result = BaiduDownloader(self.client, self.cache_dir, path_strategy=self.path_strategy).download_candidate(
                timeframe=timeframe,
                candidate=candidate,
                cancel_event=cancel_event,
                progress_callback=lambda progress: _report_download_progress(progress_callback, progress, 10, 35),
            )
            self.metadata.upsert_remote_file(
                source_id=self.source_id,
                remote_path=result.remote_path,
                size=result.size,
                md5=result.md5,
                server_mtime=result.server_mtime,
                content_hash=result.content_hash,
                local_raw_path=str(result.local_path) if result.local_path else None,
            )

            if result.status == "missing":
                failed = 1
                _report_progress(
                    progress_callback,
                    "远端文件不存在",
                    100,
                    counts=_counts_payload(
                        scanned,
                        downloaded,
                        ingested,
                        failed,
                        planned_download_count=1,
                        ingest_processed_count=ingest_processed,
                        ingest_total_count=ingest_total,
                        current_archive_ingest_processed_count=0,
                        current_archive_ingest_total_count=ingest_total,
                    ),
                )
                self.metadata.mark_remote_failed(self.source_id, result.remote_path, "remote file not found")
                self.metadata.complete_sync_job(
                    job_id,
                    scanned_count=scanned,
                    downloaded_count=downloaded,
                    ingested_count=ingested,
                    failed_count=failed,
                    status="completed_with_errors",
                    error_message="remote file not found",
                )
                return SyncJobResult(job_id=job_id, scanned_count=scanned, downloaded_count=downloaded, ingested_count=ingested, failed_count=failed)

            if result.status == "failed" or result.local_path is None or result.content_hash is None:
                failed = 1
                _report_progress(
                    progress_callback,
                    "下载失败",
                    100,
                    counts=_counts_payload(
                        scanned,
                        downloaded,
                        ingested,
                        failed,
                        planned_download_count=1,
                        ingest_processed_count=ingest_processed,
                        ingest_total_count=ingest_total,
                        current_archive_ingest_processed_count=0,
                        current_archive_ingest_total_count=ingest_total,
                    ),
                )
                self.metadata.mark_remote_failed(self.source_id, result.remote_path, result.error_message or "download failed")
                self.metadata.complete_sync_job(
                    job_id,
                    scanned_count=scanned,
                    downloaded_count=downloaded,
                    ingested_count=ingested,
                    failed_count=failed,
                    status="completed_with_errors",
                    error_message=result.error_message or "download failed",
                )
                return SyncJobResult(job_id=job_id, scanned_count=scanned, downloaded_count=downloaded, ingested_count=ingested, failed_count=failed)

            downloaded = 1
            self.metadata.mark_remote_downloaded(self.source_id, result.remote_path, str(result.local_path))
            _report_progress(
                progress_callback,
                "下载完成，开始入库",
                45,
                counts=_counts_payload(
                    scanned,
                    downloaded,
                    ingested,
                    failed,
                    planned_download_count=1,
                    ingest_processed_count=ingest_processed,
                    ingest_total_count=ingest_total,
                    current_archive_ingest_processed_count=0,
                    current_archive_ingest_total_count=ingest_total,
                    current_ingest_path=result.remote_path,
                ),
            )
            logger.info(
                "baidu file sync downloaded job_id=%s remote_path=%s local_path=%s size=%s",
                job_id,
                result.remote_path,
                result.local_path,
                result.size,
            )

            writer = ParquetBarWriter(self.parquet_root)
            ingestor = Ingestor(writer=writer, metadata=self.metadata)
            raw_file = _raw_file_from_baidu_result(result)
            start_dt = dt.datetime.combine(start, dt.time.min)
            end_dt = dt.datetime.combine(end + dt.timedelta(days=1), dt.time.min)
            normalized_symbols = [normalize_symbol(symbol) for symbol in symbols]
            _raise_if_cancelled(cancel_event)
            archive_committed = 0
            archive_failed = 0
            archive_processed = 0
            archive_requested = len(normalized_symbols)
            archive_present = len(normalized_symbols)
            archive_missing = 0

            def report_ingest_progress(symbol: str, status_result) -> None:
                nonlocal ingest_processed, archive_committed, archive_failed, archive_processed
                nonlocal archive_requested, archive_present, archive_missing
                if status_result.archive_requested_count is not None:
                    archive_requested = status_result.archive_requested_count
                if status_result.archive_present_count is not None:
                    archive_present = status_result.archive_present_count
                if status_result.archive_missing_count is not None:
                    archive_missing = status_result.archive_missing_count
                if status_result.status != "ingesting":
                    event_count = max(int(getattr(status_result, "count", 1) or 0), 0)
                    ingest_processed += event_count
                    if symbol:
                        archive_processed += event_count
                    if status_result.committed:
                        archive_committed += event_count
                    elif not status_result.skipped:
                        archive_failed += event_count
                stage_symbol = symbol or ("归档未包含" if status_result.status == "symbol_missing" else status_result.status)
                _report_progress(
                    progress_callback,
                    f"入库 {stage_symbol}",
                    _ingest_progress_percent(ingest_processed, ingest_total),
                    counts=_counts_payload(
                        scanned,
                        downloaded,
                        ingested + archive_committed,
                        failed + archive_failed,
                        planned_download_count=1,
                        ingest_processed_count=ingest_processed,
                        ingest_total_count=ingest_total,
                        current_archive_ingest_processed_count=archive_processed,
                        current_archive_ingest_total_count=archive_present,
                        current_archive_requested_count=archive_requested,
                        current_archive_present_count=archive_present,
                        current_archive_missing_count=archive_missing,
                        current_ingest_symbol=symbol or None,
                        current_ingest_path=result.remote_path,
                        current_ingest_status=status_result.status,
                    ),
                )

            outcome = ingestor.ingest_archive_result(
                raw_file,
                symbols=normalized_symbols,
                source_id=self.source_id,
                start=start_dt,
                end=end_dt,
                cancel_check=lambda: _raise_if_cancelled(cancel_event),
                progress_callback=report_ingest_progress,
            )
            ingested += outcome.committed_count
            failed += outcome.failed_count
            _report_progress(
                progress_callback,
                f"Archive processed {Path(result.remote_path).name}",
                _ingest_progress_percent(ingest_processed, ingest_total),
                counts=_counts_payload(
                    scanned,
                    downloaded,
                    ingested,
                    failed,
                    planned_download_count=1,
                    ingest_processed_count=ingest_processed,
                    ingest_total_count=ingest_total,
                    current_archive_ingest_processed_count=archive_processed,
                    current_archive_ingest_total_count=archive_present,
                    current_archive_requested_count=archive_requested,
                    current_archive_present_count=archive_present,
                    current_archive_missing_count=archive_missing,
                    current_ingest_path=result.remote_path,
                ),
            )

            self.metadata.complete_sync_job(
                job_id,
                scanned_count=scanned,
                downloaded_count=downloaded,
                ingested_count=ingested,
                failed_count=failed,
                status="completed" if failed == 0 else "completed_with_errors",
            )
            _report_progress(
                progress_callback,
                "同步完成",
                100,
                counts=_counts_payload(
                    scanned,
                    downloaded,
                    ingested,
                    failed,
                    planned_download_count=1,
                    ingest_processed_count=ingest_processed,
                    ingest_total_count=ingest_total,
                    current_archive_ingest_processed_count=archive_processed,
                    current_archive_ingest_total_count=archive_present,
                    current_archive_requested_count=archive_requested,
                    current_archive_present_count=archive_present,
                    current_archive_missing_count=archive_missing,
                ),
            )
            logger.info(
                "baidu file sync finished job_id=%s remote_path=%s downloaded=%s ingested=%s failed=%s",
                job_id,
                result.remote_path,
                downloaded,
                ingested,
                failed,
            )
        except Exception as exc:
            if isinstance(exc, SyncCancelled):
                logger.info("baidu file sync cancelled job_id=%s remote_path=%s", job_id, remote_path)
                self.metadata.complete_sync_job(
                    job_id,
                    scanned_count=scanned,
                    downloaded_count=downloaded,
                    ingested_count=ingested,
                    failed_count=failed,
                    status="stopped",
                    error_message=str(exc),
                )
                raise
            logger.exception("baidu file sync failed job_id=%s remote_path=%s", job_id, remote_path)
            self.metadata.complete_sync_job(
                job_id,
                scanned_count=scanned,
                downloaded_count=downloaded,
                ingested_count=ingested,
                failed_count=failed + 1,
                status="failed",
                error_message=str(exc),
            )
            raise
        return SyncJobResult(
            job_id=job_id,
            scanned_count=scanned,
            downloaded_count=downloaded,
            ingested_count=ingested,
            failed_count=failed,
        )


def _raw_file_from_baidu_result(result: BaiduDownloadResult) -> LocalRawFile:
    if result.local_path is None or result.content_hash is None:
        raise ValueError("downloaded Baidu result requires local_path and content_hash")
    size = result.local_path.stat().st_size
    return LocalRawFile(
        remote_path=result.remote_path,
        local_path=result.local_path,
        size=size,
        content_hash=result.content_hash,
        trade_date=result.trade_date,
        timeframe=result.timeframe,
    )


def _candidate_trade_dates(candidates: list[RemoteFileCandidate]) -> list[dt.date]:
    return sorted({candidate.trade_date for candidate in candidates})


def _already_committed_archive_paths(
    *,
    metadata: SyncMetadata,
    source_id: str,
    timeframe: Timeframe,
    symbols: list[str],
    path_strategy: RemotePathStrategy,
    trade_dates: list[dt.date],
) -> set[str]:
    archive_paths = sorted(
        {
            candidate.remote_path
            for trade_date in trade_dates
            for candidate in path_strategy.candidates(timeframe, trade_date)
            if candidate.source_kind in {"annual", "monthly"}
        }
    )
    return metadata.committed_ingest_paths(
        source_id=source_id,
        timeframe=timeframe.value,
        symbols=symbols,
        remote_paths=archive_paths,
    )


def _source_preferences_for_dates(trade_dates: list[dt.date]) -> dict[dt.date, list[str]]:
    dates = sorted(set(trade_dates))
    by_year: dict[int, list[dt.date]] = {}
    by_month: dict[tuple[int, int], list[dt.date]] = {}
    for trade_date in dates:
        by_year.setdefault(trade_date.year, []).append(trade_date)
        by_month.setdefault((trade_date.year, trade_date.month), []).append(trade_date)

    preferences: dict[dt.date, list[str]] = {}
    annual_years = {
        year
        for year, items in by_year.items()
        if _date_span_days(items) >= 180 or len(items) >= 120
    }
    monthly_months = {
        month
        for month, items in by_month.items()
        if _date_span_days(items) >= 14 or len(items) >= 10
    }

    for trade_date in dates:
        if trade_date.year in annual_years:
            preferences[trade_date] = ["annual", "monthly", "daily"]
        elif (trade_date.year, trade_date.month) in monthly_months:
            preferences[trade_date] = ["monthly", "daily", "annual"]
        else:
            preferences[trade_date] = ["daily", "monthly", "annual"]
    return preferences


def _sort_by_source_preference(
    candidates: list[RemoteFileCandidate],
    source_preference: list[str],
) -> list[RemoteFileCandidate]:
    order = {source_kind: index for index, source_kind in enumerate(source_preference)}
    fallback = len(order)
    return sorted(candidates, key=lambda candidate: order.get(candidate.source_kind, fallback))


def _planned_download_paths_for_dates(
    *,
    path_strategy: RemotePathStrategy,
    timeframe: Timeframe,
    trade_dates: list[dt.date],
    source_preferences: dict[dt.date, list[str]],
    skip_remote_paths: set[str],
) -> set[str]:
    paths: set[str] = set()
    for trade_date in sorted(set(trade_dates)):
        candidates = path_strategy.candidates(timeframe, trade_date)
        preference = source_preferences.get(trade_date)
        if preference:
            candidates = _sort_by_source_preference(candidates, preference)
        for candidate in candidates:
            if candidate.remote_path in skip_remote_paths:
                continue
            paths.add(candidate.remote_path)
            break
    return paths


def _date_span_days(trade_dates: list[dt.date]) -> int:
    if not trade_dates:
        return 0
    return (max(trade_dates) - min(trade_dates)).days + 1


def _raise_if_cancelled(cancel_event: threading.Event | None) -> None:
    if cancel_event and cancel_event.is_set():
        raise SyncCancelled("sync stop requested")


def _report_progress(
    progress_callback: ProgressCallback | None,
    stage: str,
    percent: int,
    download: dict | None = None,
    counts: dict | None = None,
) -> None:
    if progress_callback:
        progress_callback(stage, max(0, min(percent, 100)), download, counts)


def _report_download_progress(
    progress_callback: ProgressCallback | None,
    download: dict,
    base_percent: int,
    percent_span: int,
) -> None:
    remote_path = str(download.get("remote_path") or "")
    name = Path(remote_path).name or "zip"
    total = download.get("total_bytes")
    downloaded = download.get("bytes_downloaded") or 0
    percent = base_percent
    if total:
        percent += int(min(float(downloaded) / float(total), 1.0) * percent_span)
    _report_progress(progress_callback, f"下载 {name}", percent, download)


def _ingest_progress_percent(processed: int, total: int | None) -> int:
    if not total:
        return 45
    return 45 + int(min(max(processed, 0) / max(total, 1), 1.0) * 53)


def _counts_payload(
    scanned: int,
    downloaded: int,
    ingested: int,
    failed: int,
    *,
    planned_download_count: int | None = None,
    ingest_processed_count: int = 0,
    ingest_total_count: int | None = None,
    current_archive_ingest_processed_count: int = 0,
    current_archive_ingest_total_count: int | None = None,
    current_archive_requested_count: int | None = None,
    current_archive_present_count: int | None = None,
    current_archive_missing_count: int | None = None,
    current_ingest_symbol: str | None = None,
    current_ingest_path: str | None = None,
    current_ingest_status: str | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "scanned_count": scanned,
        "downloaded_count": downloaded,
        "ingested_count": ingested,
        "failed_count": failed,
        "ingest_processed_count": ingest_processed_count,
    }
    if planned_download_count is not None:
        payload["planned_download_count"] = planned_download_count
    if ingest_total_count is not None:
        payload["ingest_total_count"] = ingest_total_count
    payload["current_archive_ingest_processed_count"] = current_archive_ingest_processed_count
    if current_archive_ingest_total_count is not None:
        payload["current_archive_ingest_total_count"] = current_archive_ingest_total_count
    if current_archive_requested_count is not None:
        payload["current_archive_requested_count"] = current_archive_requested_count
    if current_archive_present_count is not None:
        payload["current_archive_present_count"] = current_archive_present_count
    if current_archive_missing_count is not None:
        payload["current_archive_missing_count"] = current_archive_missing_count
    payload["current_ingest_symbol"] = current_ingest_symbol
    payload["current_ingest_path"] = current_ingest_path
    payload["current_ingest_status"] = current_ingest_status
    return payload


def _trade_date_from_path(remote_path: str) -> dt.date | None:
    match = re.search(r"(20\d{6}|19\d{6})", Path(remote_path).name)
    if not match:
        return None
    try:
        return dt.datetime.strptime(match.group(1), "%Y%m%d").date()
    except ValueError:
        return None
