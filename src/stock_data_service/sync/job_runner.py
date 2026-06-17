from __future__ import annotations

import datetime as dt
import logging
import re
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

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
ProgressCallback = Callable[[str, int, dict | None], None]


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
            symbols,
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
                for symbol in normalized_symbols:
                    _raise_if_cancelled(cancel_event)
                    try:
                        ok = ingestor.ingest_file(
                            raw_file,
                            symbol=symbol,
                            source_id=self.source_id,
                            start=start_dt,
                            end=end_dt,
                        )
                    except Exception as exc:
                        failed += 1
                        logger.exception(
                            "local ingest exception job_id=%s remote_path=%s symbol=%s",
                            job_id,
                            raw_file.remote_path,
                            symbol,
                        )
                        self.metadata.mark_file_ingest_status(
                            source_id=self.source_id,
                            remote_path=raw_file.remote_path,
                            timeframe=timeframe.value,
                            symbol=symbol,
                            status="failed",
                            error_message=str(exc),
                            content_hash=raw_file.content_hash,
                        )
                    else:
                        if ok:
                            ingested += 1
                        elif status != "skipped":
                            failed += 1
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
            symbols,
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
            downloader = BaiduDownloader(self.client, self.cache_dir, path_strategy=self.path_strategy)
            download_results = downloader.download_for_range(
                timeframe=timeframe,
                start=start,
                end=end,
                cancel_event=cancel_event,
                progress_callback=lambda progress: _report_download_progress(progress_callback, progress, 10, 35),
            )
            _raise_if_cancelled(cancel_event)
            scanned = len(download_results)
            logger.info("baidu sync download scan finished job_id=%s result_count=%s", job_id, scanned)
            writer = ParquetBarWriter(self.parquet_root)
            ingestor = Ingestor(writer=writer, metadata=self.metadata)
            start_dt = dt.datetime.combine(start, dt.time.min)
            end_dt = dt.datetime.combine(end + dt.timedelta(days=1), dt.time.min)
            normalized_symbols = [normalize_symbol(symbol) for symbol in symbols]

            for result in download_results:
                _raise_if_cancelled(cancel_event)
                if not result.remote_path:
                    failed += 1
                    logger.warning(
                        "baidu sync missing all candidates job_id=%s trade_date=%s timeframe=%s error=%s",
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
                for symbol in normalized_symbols:
                    _raise_if_cancelled(cancel_event)
                    try:
                        ok = ingestor.ingest_file(
                            raw_file,
                            symbol=symbol,
                            source_id=self.source_id,
                            start=start_dt,
                            end=end_dt,
                        )
                    except Exception as exc:
                        failed += 1
                        logger.exception(
                            "baidu ingest exception job_id=%s remote_path=%s symbol=%s",
                            job_id,
                            result.remote_path,
                            symbol,
                        )
                        self.metadata.mark_file_ingest_status(
                            source_id=self.source_id,
                            remote_path=result.remote_path,
                            timeframe=timeframe.value,
                            symbol=symbol,
                            status="failed",
                            error_message=str(exc),
                            content_hash=result.content_hash,
                        )
                    else:
                        if ok:
                            ingested += 1
                        else:
                            failed += 1

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
            symbols,
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
        try:
            _report_progress(progress_callback, "检查远端文件", 10)
            _raise_if_cancelled(cancel_event)
            candidate = RemoteFileCandidate(
                remote_path=remote_path,
                trade_date=_trade_date_from_path(remote_path) or start,
                source_kind="manual",
            )
            result = BaiduDownloader(self.client, self.cache_dir, path_strategy=self.path_strategy).download_candidate(
                timeframe=timeframe,
                candidate=candidate,
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
                _report_progress(progress_callback, "远端文件不存在", 100)
                return SyncJobResult(job_id=job_id, scanned_count=scanned, downloaded_count=downloaded, ingested_count=ingested, failed_count=failed)

            if result.status == "failed" or result.local_path is None or result.content_hash is None:
                failed = 1
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
                _report_progress(progress_callback, "下载失败", 100)
                return SyncJobResult(job_id=job_id, scanned_count=scanned, downloaded_count=downloaded, ingested_count=ingested, failed_count=failed)

            downloaded = 1
            self.metadata.mark_remote_downloaded(self.source_id, result.remote_path, str(result.local_path))
            _report_progress(progress_callback, "下载完成，开始入库", 45)
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
            total_symbols = max(len(normalized_symbols), 1)

            for index, symbol in enumerate(normalized_symbols, start=1):
                _raise_if_cancelled(cancel_event)
                _report_progress(progress_callback, f"入库 {symbol}", min(95, 45 + int((index - 1) / total_symbols * 50)))
                try:
                    ok = ingestor.ingest_file(
                        raw_file,
                        symbol=symbol,
                        source_id=self.source_id,
                        start=start_dt,
                        end=end_dt,
                    )
                except Exception as exc:
                    failed += 1
                    logger.exception(
                        "baidu file sync ingest exception job_id=%s remote_path=%s symbol=%s",
                        job_id,
                        result.remote_path,
                        symbol,
                    )
                    self.metadata.mark_file_ingest_status(
                        source_id=self.source_id,
                        remote_path=result.remote_path,
                        timeframe=timeframe.value,
                        symbol=symbol,
                        status="failed",
                        error_message=str(exc),
                        content_hash=result.content_hash,
                    )
                else:
                    if ok:
                        ingested += 1
                    else:
                        failed += 1
                _report_progress(progress_callback, f"已处理 {index}/{total_symbols}", min(98, 45 + int(index / total_symbols * 50)))

            self.metadata.complete_sync_job(
                job_id,
                scanned_count=scanned,
                downloaded_count=downloaded,
                ingested_count=ingested,
                failed_count=failed,
                status="completed" if failed == 0 else "completed_with_errors",
            )
            _report_progress(progress_callback, "同步完成", 100)
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


def _raise_if_cancelled(cancel_event: threading.Event | None) -> None:
    if cancel_event and cancel_event.is_set():
        raise SyncCancelled("sync stop requested")


def _report_progress(
    progress_callback: ProgressCallback | None,
    stage: str,
    percent: int,
    download: dict | None = None,
) -> None:
    if progress_callback:
        progress_callback(stage, max(0, min(percent, 100)), download)


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


def _trade_date_from_path(remote_path: str) -> dt.date | None:
    match = re.search(r"(20\d{6}|19\d{6})", Path(remote_path).name)
    if not match:
        return None
    try:
        return dt.datetime.strptime(match.group(1), "%Y%m%d").date()
    except ValueError:
        return None
