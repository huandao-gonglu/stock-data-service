from __future__ import annotations

import datetime as dt
import hashlib
import logging
import threading
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterator, Literal

from stock_data_service.baidu.pan_client import BaiduFileMeta, BaiduPanClient
from stock_data_service.market.path_strategy import BaiduStockKPathStrategy, RemoteFileCandidate, RemotePathStrategy
from stock_data_service.market.timeframe import Timeframe

BaiduDownloadStatus = Literal["downloaded", "missing", "failed"]
DownloadProgressCallback = Callable[[dict[str, Any]], None]
logger = logging.getLogger(__name__)
CacheValidationStrength = Literal["strong", "weak", "miss"]


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


@dataclass(frozen=True)
class CacheValidation:
    matched: bool
    strength: CacheValidationStrength
    reason: str
    local_size: int | None
    remote_size: int | None
    remote_md5: str | None
    remote_md5_usable: bool
    local_md5: str | None = None
    md5_check: str = "skipped"
    zip_check: str = "skipped"


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
        by_date = _group_candidates_by_date(self.path_strategy.candidates_for_range(timeframe, start, end))
        return list(self._iter_download_by_date(
            timeframe=timeframe,
            by_date=by_date,
            cancel_event=cancel_event,
            progress_callback=progress_callback,
        ))

    def download_for_trade_dates(
        self,
        *,
        timeframe: Timeframe,
        trade_dates: list[dt.date],
        source_preferences: dict[dt.date, list[str]] | None = None,
        skip_remote_paths: set[str] | None = None,
        cancel_event: threading.Event | None = None,
        progress_callback: DownloadProgressCallback | None = None,
    ) -> list[BaiduDownloadResult]:
        return list(
            self.iter_download_for_trade_dates(
                timeframe=timeframe,
                trade_dates=trade_dates,
                source_preferences=source_preferences,
                skip_remote_paths=skip_remote_paths,
                cancel_event=cancel_event,
                progress_callback=progress_callback,
            )
        )

    def iter_download_for_trade_dates(
        self,
        *,
        timeframe: Timeframe,
        trade_dates: list[dt.date],
        source_preferences: dict[dt.date, list[str]] | None = None,
        skip_remote_paths: set[str] | None = None,
        cancel_event: threading.Event | None = None,
        progress_callback: DownloadProgressCallback | None = None,
    ) -> Iterator[BaiduDownloadResult]:
        by_date: dict[dt.date, list[RemoteFileCandidate]] = {}
        source_preferences = source_preferences or {}
        for trade_date in sorted(set(trade_dates)):
            candidates = self.path_strategy.candidates(timeframe, trade_date)
            preference = source_preferences.get(trade_date)
            if preference:
                candidates = _sort_by_source_preference(candidates, preference)
            by_date[trade_date] = candidates
        yield from self._iter_download_by_date(
            timeframe=timeframe,
            by_date=by_date,
            skip_remote_paths=skip_remote_paths or set(),
            cancel_event=cancel_event,
            progress_callback=progress_callback,
        )

    def _iter_download_by_date(
        self,
        *,
        timeframe: Timeframe,
        by_date: dict[dt.date, list[RemoteFileCandidate]],
        skip_remote_paths: set[str] | None = None,
        cancel_event: threading.Event | None = None,
        progress_callback: DownloadProgressCallback | None = None,
    ) -> Iterator[BaiduDownloadResult]:
        missing_paths: set[str] = set()
        successful_paths: set[str] = set()
        skip_remote_paths = skip_remote_paths or set()

        for trade_date, candidates in by_date.items():
            if cancel_event and cancel_event.is_set():
                break
            reused_path = next(
                (
                    candidate.remote_path
                    for candidate in candidates
                    if candidate.remote_path in successful_paths and candidate.remote_path not in skip_remote_paths
                ),
                None,
            )
            if reused_path is not None:
                logger.info(
                    "baidu download reused successful archive trade_date=%s remote_path=%s",
                    trade_date,
                    reused_path,
                )
                continue
            found_for_date = False
            for candidate in candidates:
                if cancel_event and cancel_event.is_set():
                    break
                if candidate.remote_path in skip_remote_paths:
                    logger.info(
                        "baidu skipped already-ingested candidate remote_path=%s trade_date=%s source_kind=%s",
                        candidate.remote_path,
                        trade_date,
                        candidate.source_kind,
                    )
                    continue
                if candidate.remote_path in missing_paths:
                    logger.debug("baidu skipped known missing candidate remote_path=%s trade_date=%s", candidate.remote_path, trade_date)
                    continue

                result = self.download_candidate(
                    timeframe=timeframe,
                    candidate=candidate,
                    cancel_event=cancel_event,
                    progress_callback=progress_callback,
                )
                if result.status == "missing":
                    missing_paths.add(candidate.remote_path)
                    logger.debug("baidu candidate missing remote_path=%s trade_date=%s", candidate.remote_path, trade_date)
                    continue

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
                    yield result
                    break
                yield result

            if not found_for_date:
                logger.info("baidu no candidate found, skipping trade_date=%s timeframe=%s", trade_date, timeframe.value)
                yield BaiduDownloadResult(
                    remote_path="",
                    trade_date=trade_date,
                    timeframe=timeframe,
                    source_kind="none",
                    status="missing",
                    error_message=f"no Baidu zip candidate found for {trade_date.isoformat()}",
                )

    def download_candidate(
        self,
        *,
        timeframe: Timeframe,
        candidate: RemoteFileCandidate,
        cancel_event: threading.Event | None = None,
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
        cache_validation: CacheValidation | None = None
        if local_path.exists():
            cache_validation = _validate_cache(local_path, meta)
            _log_cache_validation(candidate.remote_path, local_path, cache_validation)
            if cache_validation.matched:
                return _result_from_meta(
                    meta,
                    timeframe=timeframe,
                    candidate=candidate,
                    status="downloaded",
                    local_path=local_path,
                    content_hash=_sha256(local_path),
                )
            _discard_partial_download(local_path)

        try:
            download_kwargs: dict[str, Any] = {
                "progress_callback": _with_candidate_progress(progress_callback, candidate, meta),
                "cancel_event": cancel_event,
            }
            if cache_validation is not None:
                download_kwargs["use_local_cache"] = False
            saved = self.client.download_by_path(
                candidate.remote_path,
                local_path,
                **download_kwargs,
            )
        except Exception as exc:
            if cancel_event and cancel_event.is_set():
                raise
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


def _sort_by_source_preference(
    candidates: list[RemoteFileCandidate],
    source_preference: list[str],
) -> list[RemoteFileCandidate]:
    order = {source_kind: index for index, source_kind in enumerate(source_preference)}
    fallback = len(order)
    return sorted(candidates, key=lambda candidate: order.get(candidate.source_kind, fallback))


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


def _validate_cache(path: Path, meta: BaiduFileMeta) -> CacheValidation:
    local_size = path.stat().st_size
    remote_size = meta.size
    remote_md5 = meta.md5.strip() if meta.md5 else None
    remote_md5_usable = _is_standard_md5(remote_md5)

    if remote_size is not None and local_size != remote_size:
        return CacheValidation(
            matched=False,
            strength="miss",
            reason="size_mismatch",
            local_size=local_size,
            remote_size=remote_size,
            remote_md5=remote_md5,
            remote_md5_usable=remote_md5_usable,
        )

    if remote_md5_usable and remote_md5 is not None:
        local_md5 = _md5(path)
        if local_md5.lower() == remote_md5.lower():
            return CacheValidation(
                matched=True,
                strength="strong",
                reason="md5_match",
                local_size=local_size,
                remote_size=remote_size,
                remote_md5=remote_md5,
                remote_md5_usable=True,
                local_md5=local_md5,
                md5_check="passed",
            )
        return CacheValidation(
            matched=False,
            strength="miss",
            reason="md5_mismatch",
            local_size=local_size,
            remote_size=remote_size,
            remote_md5=remote_md5,
            remote_md5_usable=True,
            local_md5=local_md5,
            md5_check="failed",
        )

    if remote_size is None:
        return CacheValidation(
            matched=False,
            strength="miss",
            reason="no_verifiable_remote_metadata",
            local_size=local_size,
            remote_size=remote_size,
            remote_md5=remote_md5,
            remote_md5_usable=False,
        )

    zip_check = "skipped"
    if path.suffix.lower() == ".zip":
        if not zipfile.is_zipfile(path):
            return CacheValidation(
                matched=False,
                strength="miss",
                reason="zip_header_invalid",
                local_size=local_size,
                remote_size=remote_size,
                remote_md5=remote_md5,
                remote_md5_usable=False,
                zip_check="failed",
            )
        zip_check = "passed"

    return CacheValidation(
        matched=True,
        strength="weak",
        reason="remote_md5_unusable" if remote_md5 else "remote_md5_missing",
        local_size=local_size,
        remote_size=remote_size,
        remote_md5=remote_md5,
        remote_md5_usable=False,
        zip_check=zip_check,
    )


def _is_standard_md5(value: str | None) -> bool:
    return value is not None and len(value) == 32 and all(char in "0123456789abcdefABCDEF" for char in value)


def _log_cache_validation(remote_path: str, local_path: Path, validation: CacheValidation) -> None:
    message = "baidu cache hit" if validation.matched else "baidu cache stale"
    logger.info(
        (
            "%s remote_path=%s local_path=%s decision=%s reason=%s local_size=%s remote_size=%s "
            "remote_md5=%s remote_md5_usable=%s local_md5=%s md5_check=%s zip_check=%s"
        ),
        message,
        remote_path,
        local_path,
        validation.strength,
        validation.reason,
        validation.local_size,
        validation.remote_size,
        validation.remote_md5,
        validation.remote_md5_usable,
        validation.local_md5,
        validation.md5_check,
        validation.zip_check,
    )


def _discard_partial_download(path: Path) -> None:
    partial = path.with_name(f"{path.name}.part")
    if partial.exists():
        logger.info("baidu stale cache dropping partial download local_path=%s partial_path=%s", path, partial)
        partial.unlink()
