from __future__ import annotations

import datetime as dt
import hashlib
import logging
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Literal

from stock_data_service.baidu.pan_client import BaiduFileMeta, BaiduPanClient
from stock_data_service.market.path_strategy import BaiduStockKPathStrategy, RemoteFileCandidate, RemotePathStrategy
from stock_data_service.market.timeframe import Timeframe

BaiduDownloadStatus = Literal["downloaded", "missing", "failed"]
DownloadProgressCallback = Callable[[dict[str, Any]], None]
logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class BaiduDownloadResult:
    remote_path: str
    trade_date: dt.date
    timeframe: Timeframe
    source_kind: str
    status: BaiduDownloadStatus
    local_path: Path | None = None
    size: int | None = None
    md5: str | None = None
    server_mtime: dt.datetime | None = None
    content_hash: str | None = None
    error_message: str | None = None

    @property
    def is_downloaded(self) -> bool:
        return self.status == "downloaded" and self.local_path is not None


class BaiduDownloader:
    def __init__(
        self,
        client: BaiduPanClient,
        cache_dir: str | Path,
        path_strategy: RemotePathStrategy | None = None,
    ):
        self.client = client
        self.cache_dir = Path(cache_dir)
        self.path_strategy = path_strategy or BaiduStockKPathStrategy()

    def download_range(self, *, timeframe: Timeframe, start: dt.date, end: dt.date) -> dict[str, Path | None]:
        paths = [result.remote_path for result in self.download_for_range(timeframe=timeframe, start=start, end=end)]
        results: dict[str, Path | None] = {}
        for remote_path in dict.fromkeys(paths):
            if not remote_path:
                continue
            local_path = self.cache_dir / remote_path.lstrip("/")
            results[remote_path] = local_path if local_path.exists() else None
        return results

    def download_for_range(
        self,
        *,
        timeframe: Timeframe,
        start: dt.date,
        end: dt.date,
        cancel_event: threading.Event | None = None,
        progress_callback: DownloadProgressCallback | None = None,
    ) -> list[BaiduDownloadResult]:
        results: list[BaiduDownloadResult] = []
        successful_paths: set[str] = set()
        by_date = _group_candidates_by_date(self.path_strategy.candidates_for_range(timeframe, start, end))

        for trade_date, candidates in by_date.items():
            if cancel_event and cancel_event.is_set():
                break
            found_for_date = False
            for candidate in candidates:
                if cancel_event and cancel_event.is_set():
                    break
                if candidate.remote_path in successful_paths:
                    logger.info(
                        "baidu download reused successful archive trade_date=%s remote_path=%s",
                        trade_date,
                        candidate.remote_path,
                    )
                    found_for_date = True
                    break

                result = self.download_candidate(timeframe=timeframe, candidate=candidate, progress_callback=progress_callback)
                if result.status == "missing":
                    logger.debug("baidu candidate missing remote_path=%s trade_date=%s", candidate.remote_path, trade_date)
                    continue

                results.append(result)
                if result.is_downloaded:
                    logger.info(
                        "baidu candidate downloaded remote_path=%s trade_date=%s source_kind=%s local_path=%s",
                        result.remote_path,
                        result.trade_date,
                        result.source_kind,
                        result.local_path,
                    )
                    successful_paths.add(candidate.remote_path)
                    found_for_date = True
                    break

            if not found_for_date:
                logger.warning("baidu no candidate found trade_date=%s timeframe=%s", trade_date, timeframe.value)
                results.append(
                    BaiduDownloadResult(
                        remote_path="",
                        trade_date=trade_date,
                        timeframe=timeframe,
                        source_kind="none",
                        status="missing",
                        error_message=f"no Baidu zip candidate found for {trade_date.isoformat()}",
                    )
                )

        return results

    def download_candidate(
        self,
        *,
        timeframe: Timeframe,
        candidate: RemoteFileCandidate,
        progress_callback: DownloadProgressCallback | None = None,
    ) -> BaiduDownloadResult:
        meta = self.client.get_file_meta_by_path(candidate.remote_path)
        if meta is None:
            return BaiduDownloadResult(
                remote_path=candidate.remote_path,
                trade_date=candidate.trade_date,
                timeframe=timeframe,
                source_kind=candidate.source_kind,
                status="missing",
            )

        local_path = self.cache_dir / candidate.remote_path.lstrip("/")
        if local_path.exists() and _cache_matches(local_path, meta):
            logger.info("baidu cache hit remote_path=%s local_path=%s", candidate.remote_path, local_path)
            return _result_from_meta(
                meta,
                timeframe=timeframe,
                candidate=candidate,
                status="downloaded",
                local_path=local_path,
                content_hash=_sha256(local_path),
            )
        if local_path.exists():
            logger.info("baidu cache stale remote_path=%s local_path=%s", candidate.remote_path, local_path)
            local_path.unlink()

        try:
            saved = self.client.download_by_path(
                candidate.remote_path,
                local_path,
                progress_callback=_with_candidate_progress(progress_callback, candidate, meta),
            )
        except Exception as exc:
            logger.warning("baidu download failed remote_path=%s error=%s", candidate.remote_path, exc)
            return _result_from_meta(
                meta,
                timeframe=timeframe,
                candidate=candidate,
                status="failed",
                local_path=None,
                error_message=str(exc),
            )

        if not saved:
            return _result_from_meta(
                meta,
                timeframe=timeframe,
                candidate=candidate,
                status="failed",
                local_path=None,
                error_message="download returned no local file",
            )

        path = Path(saved)
        return _result_from_meta(
            meta,
            timeframe=timeframe,
            candidate=candidate,
            status="downloaded",
            local_path=path,
            content_hash=_sha256(path),
        )


def _group_candidates_by_date(candidates: list[RemoteFileCandidate]) -> dict[dt.date, list[RemoteFileCandidate]]:
    grouped: dict[dt.date, list[RemoteFileCandidate]] = {}
    for candidate in candidates:
        grouped.setdefault(candidate.trade_date, []).append(candidate)
    return grouped


def _with_candidate_progress(
    progress_callback: DownloadProgressCallback | None,
    candidate: RemoteFileCandidate,
    meta: BaiduFileMeta,
) -> DownloadProgressCallback | None:
    if progress_callback is None:
        return None

    def report(progress: dict[str, Any]) -> None:
        payload = dict(progress)
        payload["remote_path"] = candidate.remote_path
        payload["trade_date"] = candidate.trade_date.isoformat()
        payload["source_kind"] = candidate.source_kind
        if payload.get("total_bytes") is None:
            payload["total_bytes"] = meta.size
        progress_callback(payload)

    return report


def _result_from_meta(
    meta: BaiduFileMeta,
    *,
    timeframe: Timeframe,
    candidate: RemoteFileCandidate,
    status: BaiduDownloadStatus,
    local_path: Path | None = None,
    content_hash: str | None = None,
    error_message: str | None = None,
) -> BaiduDownloadResult:
    return BaiduDownloadResult(
        remote_path=candidate.remote_path,
        trade_date=candidate.trade_date,
        timeframe=timeframe,
        source_kind=candidate.source_kind,
        status=status,
        local_path=local_path,
        size=meta.size,
        md5=meta.md5,
        server_mtime=_server_mtime(meta.server_mtime),
        content_hash=content_hash,
        error_message=error_message,
    )


def _server_mtime(value: int | None) -> dt.datetime | None:
    if value is None:
        return None
    return dt.datetime.fromtimestamp(value, tz=dt.timezone.utc).replace(tzinfo=None)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _md5(path: Path) -> str:
    digest = hashlib.md5()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _cache_matches(path: Path, meta: BaiduFileMeta) -> bool:
    if meta.size is not None and path.stat().st_size != meta.size:
        return False
    if meta.md5:
        return _md5(path).lower() == meta.md5.lower()
    return True
