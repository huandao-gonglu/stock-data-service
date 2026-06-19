from __future__ import annotations

import datetime as dt
import logging
import time
import zipfile
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from collections.abc import Callable
from dataclasses import dataclass, field, replace
from typing import Any

import pandas as pd

from stock_data_service.market.symbol_normalizer import normalize_symbol
from stock_data_service.market.timeframe import Timeframe, expected_intraday_rows
from stock_data_service.market.zip_parser import ParseResult, ZipBarParser
from stock_data_service.storage.parquet_writer import ParquetBarWriter
from stock_data_service.storage.sync_metadata import SyncMetadata
from stock_data_service.sync.local_source import LocalRawFile

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class IngestFileResult:
    status: str
    committed: bool = False
    count: int = 1
    error_message: str | None = None
    archive_requested_count: int | None = None
    archive_present_count: int | None = None
    archive_missing_count: int | None = None

    @property
    def skipped(self) -> bool:
        return self.status in {"skipped", "symbol_missing"}


@dataclass
class IngestArchiveResult:
    committed_count: int = 0
    skipped_count: int = 0
    failed_count: int = 0
    status_counts: dict[str, int] = field(default_factory=dict)

    @property
    def processed_count(self) -> int:
        return sum(self.status_counts.values())

    def add(self, outcome: IngestFileResult) -> None:
        count = max(int(outcome.count), 0)
        if count == 0:
            return
        self.status_counts[outcome.status] = self.status_counts.get(outcome.status, 0) + count
        if outcome.committed:
            self.committed_count += count
        elif outcome.skipped:
            self.skipped_count += count
        else:
            self.failed_count += count


@dataclass(frozen=True)
class IngestSymbolTaskResult:
    symbol: str
    outcome: IngestFileResult
    file_ingest_row: dict[str, Any] | None = None
    symbol_records: tuple[dict[str, Any], ...] = ()
    coverage_rows: tuple[dict[str, Any], ...] = ()
    parse_seconds: float | None = None
    write_seconds: float = 0.0
    written_paths: tuple[str, ...] = ()


class Ingestor:
    def __init__(
        self,
        *,
        writer: ParquetBarWriter,
        metadata: SyncMetadata,
        parser: ZipBarParser | None = None,
        archive_workers: int = 2,
    ):
        self.writer = writer
        self.metadata = metadata
        self.parser = parser or ZipBarParser()
        self.archive_workers = max(int(archive_workers), 1)

    def ingest_file(
        self,
        raw_file: LocalRawFile,
        *,
        symbol: str,
        source_id: str = "local-raw",
        start: dt.datetime | None = None,
        end: dt.datetime | None = None,
    ) -> bool:
        return self.ingest_file_result(
            raw_file,
            symbol=symbol,
            source_id=source_id,
            start=start,
            end=end,
        ).committed

    def ingest_file_result(
        self,
        raw_file: LocalRawFile,
        *,
        symbol: str,
        source_id: str = "local-raw",
        start: dt.datetime | None = None,
        end: dt.datetime | None = None,
    ) -> IngestFileResult:
        normalized = normalize_symbol(symbol)
        logger.info(
            "ingest started source_id=%s remote_path=%s symbol=%s timeframe=%s",
            source_id,
            raw_file.remote_path,
            normalized,
            raw_file.timeframe.value,
        )
        self.metadata.start_file_ingest(
            source_id=source_id,
            remote_path=raw_file.remote_path,
            timeframe=raw_file.timeframe.value,
            symbol=normalized,
            content_hash=raw_file.content_hash,
        )

        result = self.parser.parse(
            raw_file.local_path.read_bytes(),
            normalized,
            start=start,
            end=end,
            source_path=raw_file.remote_path,
        )
        return self._record_parse_result(
            raw_file,
            symbol=normalized,
            source_id=source_id,
            result=result,
            log_details=True,
        )

    def ingest_archive_result(
        self,
        raw_file: LocalRawFile,
        *,
        symbols: list[str],
        source_id: str = "local-raw",
        start: dt.datetime | None = None,
        end: dt.datetime | None = None,
        cancel_check: Callable[[], None] | None = None,
        progress_callback: Callable[[str, IngestFileResult], None] | None = None,
    ) -> IngestArchiveResult:
        normalized_symbols = list(dict.fromkeys(normalize_symbol(symbol) for symbol in symbols))
        archive_result = IngestArchiveResult()
        logger.info(
            "archive ingest started source_id=%s remote_path=%s symbol_count=%s timeframe=%s",
            source_id,
            raw_file.remote_path,
            len(normalized_symbols),
            raw_file.timeframe.value,
        )
        index_started = time.perf_counter()
        try:
            member_index = self.metadata.get_archive_symbol_members(
                source_id=source_id,
                remote_path=raw_file.remote_path,
                content_hash=raw_file.content_hash,
            )
            index_source = "cache"
            if member_index is None:
                index_source = "zip"
                member_index = self.parser.symbol_member_index(raw_file.local_path)
                self.metadata.upsert_archive_symbol_members(
                    source_id=source_id,
                    remote_path=raw_file.remote_path,
                    content_hash=raw_file.content_hash,
                    members=member_index,
                )
        except zipfile.BadZipFile as exc:
            return self._mark_archive_status(
                raw_file,
                symbols=normalized_symbols,
                source_id=source_id,
                status="corrupted_zip",
                error_message=str(exc),
                progress_callback=progress_callback,
            )
        except Exception as exc:
            return self._mark_archive_status(
                raw_file,
                symbols=normalized_symbols,
                source_id=source_id,
                status="parse_failed",
                error_message=str(exc),
                progress_callback=progress_callback,
            )

        present_symbols = [symbol for symbol in normalized_symbols if symbol in member_index]
        missing_symbols = [symbol for symbol in normalized_symbols if symbol not in member_index]
        logger.info(
            "archive member index ready source_id=%s remote_path=%s source=%s requested=%s present=%s missing=%s seconds=%.3f",
            source_id,
            raw_file.remote_path,
            index_source,
            len(normalized_symbols),
            len(present_symbols),
            len(missing_symbols),
            time.perf_counter() - index_started,
        )

        def with_archive_counts(outcome: IngestFileResult) -> IngestFileResult:
            return replace(
                outcome,
                archive_requested_count=len(normalized_symbols),
                archive_present_count=len(present_symbols),
                archive_missing_count=len(missing_symbols),
            )

        if missing_symbols:
            mark_started = time.perf_counter()
            self.metadata.mark_file_ingest_status_many(
                source_id=source_id,
                remote_path=raw_file.remote_path,
                timeframe=raw_file.timeframe.value,
                symbols=missing_symbols,
                status="symbol_missing",
                error_message="symbol not found in archive",
                content_hash=raw_file.content_hash,
            )
            outcome = with_archive_counts(IngestFileResult(status="symbol_missing", count=len(missing_symbols)))
            archive_result.add(outcome)
            logger.info(
                "archive missing symbols marked source_id=%s remote_path=%s count=%s seconds=%.3f",
                source_id,
                raw_file.remote_path,
                len(missing_symbols),
                time.perf_counter() - mark_started,
            )
            if progress_callback is not None:
                progress_callback("", outcome)

        for task_result in self._ingest_present_symbols(
            raw_file,
            symbols=present_symbols,
            source_id=source_id,
            start=start,
            end=end,
            member_index=member_index,
            cancel_check=cancel_check,
            progress_callback=(
                (lambda symbol, outcome: progress_callback(symbol, with_archive_counts(outcome)))
                if progress_callback is not None
                else None
            ),
        ):
            archive_result.add(with_archive_counts(task_result.outcome))

        logger.info(
            "archive ingest finished source_id=%s remote_path=%s processed=%s committed=%s skipped=%s failed=%s statuses=%s",
            source_id,
            raw_file.remote_path,
            archive_result.processed_count,
            archive_result.committed_count,
            archive_result.skipped_count,
            archive_result.failed_count,
            archive_result.status_counts,
        )
        return archive_result

    def _ingest_present_symbols(
        self,
        raw_file: LocalRawFile,
        *,
        symbols: list[str],
        source_id: str,
        start: dt.datetime | None,
        end: dt.datetime | None,
        member_index: dict[str, str],
        cancel_check: Callable[[], None] | None,
        progress_callback: Callable[[str, IngestFileResult], None] | None,
    ) -> list[IngestSymbolTaskResult]:
        if not symbols:
            return []

        worker_count = min(self.archive_workers, len(symbols))
        started = time.perf_counter()
        if worker_count <= 1:
            task_results = self._ingest_present_symbols_sequential(
                raw_file,
                symbols=symbols,
                source_id=source_id,
                start=start,
                end=end,
                member_index=member_index,
                cancel_check=cancel_check,
                progress_callback=progress_callback,
            )
        else:
            task_results = self._ingest_present_symbols_parallel(
                raw_file,
                symbols=symbols,
                source_id=source_id,
                start=start,
                end=end,
                member_index=member_index,
                cancel_check=cancel_check,
                progress_callback=progress_callback,
                worker_count=worker_count,
            )

        if cancel_check is not None:
            cancel_check()
        metadata_started = time.perf_counter()
        self._record_task_metadata(task_results)
        metadata_seconds = time.perf_counter() - metadata_started
        for task_result in task_results:
            self._log_task_timing(raw_file, source_id, task_result, metadata_seconds)
        logger.info(
            "archive present symbols ingested source_id=%s remote_path=%s present=%s workers=%s seconds=%.3f metadata_seconds=%.3f",
            source_id,
            raw_file.remote_path,
            len(symbols),
            worker_count,
            time.perf_counter() - started,
            metadata_seconds,
        )
        return task_results

    def _ingest_present_symbols_sequential(
        self,
        raw_file: LocalRawFile,
        *,
        symbols: list[str],
        source_id: str,
        start: dt.datetime | None,
        end: dt.datetime | None,
        member_index: dict[str, str],
        cancel_check: Callable[[], None] | None,
        progress_callback: Callable[[str, IngestFileResult], None] | None,
    ) -> list[IngestSymbolTaskResult]:
        task_results: list[IngestSymbolTaskResult] = []
        for parsed_symbol, parse_result in self.parser.iter_parse_archive(
            raw_file.local_path,
            symbols,
            start=start,
            end=end,
            source_path=raw_file.remote_path,
            member_index=member_index,
        ):
            if cancel_check is not None:
                cancel_check()
            if progress_callback is not None:
                progress_callback(parsed_symbol, IngestFileResult(status="ingesting", count=0))
            try:
                task_result = self._task_result_from_parse_result(
                    raw_file,
                    symbol=parsed_symbol,
                    source_id=source_id,
                    result=parse_result,
                    writer=ParquetBarWriter(self.writer.parquet_root),
                )
            except Exception as exc:
                logger.exception(
                    "archive ingest exception source_id=%s remote_path=%s symbol=%s",
                    source_id,
                    raw_file.remote_path,
                    parsed_symbol,
                )
                task_result = self._failed_task_result(raw_file, source_id=source_id, symbol=parsed_symbol, error=exc)
            task_results.append(task_result)
            if progress_callback is not None:
                progress_callback(parsed_symbol, task_result.outcome)
        return task_results

    def _ingest_present_symbols_parallel(
        self,
        raw_file: LocalRawFile,
        *,
        symbols: list[str],
        source_id: str,
        start: dt.datetime | None,
        end: dt.datetime | None,
        member_index: dict[str, str],
        cancel_check: Callable[[], None] | None,
        progress_callback: Callable[[str, IngestFileResult], None] | None,
        worker_count: int,
    ) -> list[IngestSymbolTaskResult]:
        task_results: list[IngestSymbolTaskResult] = []
        symbol_iter = iter(symbols)
        futures: dict[Future[IngestSymbolTaskResult], str] = {}
        in_progress: set[str] = set()

        def submit_next(executor: ThreadPoolExecutor) -> bool:
            if cancel_check is not None:
                cancel_check()
            try:
                symbol = next(symbol_iter)
            except StopIteration:
                return False
            if symbol in in_progress:
                raise RuntimeError(f"duplicate in-flight archive symbol: {symbol}")
            in_progress.add(symbol)
            if progress_callback is not None:
                progress_callback(symbol, IngestFileResult(status="ingesting", count=0))
            future = executor.submit(
                self._ingest_archive_symbol_task,
                raw_file,
                symbol=symbol,
                source_id=source_id,
                start=start,
                end=end,
                member_name=member_index.get(symbol),
            )
            futures[future] = symbol
            return True

        with ThreadPoolExecutor(max_workers=worker_count, thread_name_prefix="archive-symbol") as executor:
            for _ in range(worker_count):
                if not submit_next(executor):
                    break
            while futures:
                if cancel_check is not None:
                    cancel_check()
                done, _ = wait(futures.keys(), timeout=0.2, return_when=FIRST_COMPLETED)
                if not done:
                    continue
                for future in done:
                    symbol = futures.pop(future)
                    in_progress.remove(symbol)
                    try:
                        task_result = future.result()
                    except Exception as exc:
                        logger.exception(
                            "archive ingest worker exception source_id=%s remote_path=%s symbol=%s",
                            source_id,
                            raw_file.remote_path,
                            symbol,
                        )
                        task_result = self._failed_task_result(raw_file, source_id=source_id, symbol=symbol, error=exc)
                    task_results.append(task_result)
                    if progress_callback is not None:
                        progress_callback(symbol, task_result.outcome)
                    submit_next(executor)
        return task_results

    def _ingest_archive_symbol_task(
        self,
        raw_file: LocalRawFile,
        *,
        symbol: str,
        source_id: str,
        start: dt.datetime | None,
        end: dt.datetime | None,
        member_name: str | None,
    ) -> IngestSymbolTaskResult:
        result = self.parser.parse_member(
            raw_file.local_path,
            symbol,
            member_name,
            start=start,
            end=end,
            source_path=raw_file.remote_path,
        )
        return self._task_result_from_parse_result(
            raw_file,
            symbol=symbol,
            source_id=source_id,
            result=result,
            writer=ParquetBarWriter(self.writer.parquet_root),
        )

    def _task_result_from_parse_result(
        self,
        raw_file: LocalRawFile,
        *,
        symbol: str,
        source_id: str,
        result: ParseResult,
        writer: ParquetBarWriter,
    ) -> IngestSymbolTaskResult:
        if result.status != "ok":
            outcome = IngestFileResult(status=result.status, error_message=result.error_message)
            return IngestSymbolTaskResult(
                symbol=symbol,
                outcome=outcome,
                file_ingest_row=_file_ingest_status_row(
                    raw_file,
                    source_id=source_id,
                    symbol=symbol,
                    status=result.status,
                    error_message=result.error_message,
                ),
                parse_seconds=result.parse_seconds,
            )

        df = result.dataframe
        if df.empty:
            error_message = "no rows in requested range"
            outcome = IngestFileResult(status="skipped", error_message=error_message)
            return IngestSymbolTaskResult(
                symbol=symbol,
                outcome=outcome,
                file_ingest_row=_file_ingest_status_row(
                    raw_file,
                    source_id=source_id,
                    symbol=symbol,
                    status="skipped",
                    error_message=error_message,
                ),
                parse_seconds=result.parse_seconds,
            )

        write_started = time.perf_counter()
        written_paths = writer.write_bars(df, raw_file.timeframe)
        write_seconds = time.perf_counter() - write_started
        parquet_path = str(written_paths[0]) if written_paths else None
        outcome = IngestFileResult(status="committed", committed=True)
        return IngestSymbolTaskResult(
            symbol=symbol,
            outcome=outcome,
            file_ingest_row=_file_ingest_commit_row(
                raw_file,
                source_id=source_id,
                symbol=symbol,
                df=df,
                parquet_path=parquet_path,
            ),
            symbol_records=tuple(_symbol_records(df)),
            coverage_rows=tuple(_coverage_rows(df, raw_file.timeframe)),
            parse_seconds=result.parse_seconds,
            write_seconds=write_seconds,
            written_paths=tuple(str(path) for path in written_paths),
        )

    def _failed_task_result(
        self,
        raw_file: LocalRawFile,
        *,
        source_id: str,
        symbol: str,
        error: Exception,
    ) -> IngestSymbolTaskResult:
        message = str(error)
        return IngestSymbolTaskResult(
            symbol=symbol,
            outcome=IngestFileResult(status="failed", error_message=message),
            file_ingest_row=_file_ingest_status_row(
                raw_file,
                source_id=source_id,
                symbol=symbol,
                status="failed",
                error_message=message,
            ),
        )

    def _record_task_metadata(self, task_results: list[IngestSymbolTaskResult]) -> None:
        self.metadata.record_ingest_metadata_many(
            symbol_records=[record for item in task_results for record in item.symbol_records],
            coverage_rows=[row for item in task_results for row in item.coverage_rows],
            file_ingest_rows=[
                item.file_ingest_row
                for item in task_results
                if item.file_ingest_row is not None
            ],
        )

    @staticmethod
    def _log_task_timing(
        raw_file: LocalRawFile,
        source_id: str,
        task_result: IngestSymbolTaskResult,
        metadata_seconds: float,
    ) -> None:
        outcome = task_result.outcome
        if outcome.committed:
            logger.info(
                "ingest symbol timing source_id=%s remote_path=%s symbol=%s status=committed rows=%s parse_seconds=%s write_seconds=%.3f metadata_seconds=%.3f parquet_paths=%s",
                source_id,
                raw_file.remote_path,
                task_result.symbol,
                task_result.file_ingest_row.get("row_count") if task_result.file_ingest_row else 0,
                _seconds_text(task_result.parse_seconds),
                task_result.write_seconds,
                metadata_seconds,
                len(task_result.written_paths),
            )
        else:
            logger.info(
                "ingest symbol timing source_id=%s remote_path=%s symbol=%s status=%s parse_seconds=%s metadata_seconds=%.3f error=%s",
                source_id,
                raw_file.remote_path,
                task_result.symbol,
                outcome.status,
                _seconds_text(task_result.parse_seconds),
                metadata_seconds,
                outcome.error_message,
            )

    def _mark_archive_status(
        self,
        raw_file: LocalRawFile,
        *,
        symbols: list[str],
        source_id: str,
        status: str,
        error_message: str | None,
        progress_callback: Callable[[str, IngestFileResult], None] | None,
    ) -> IngestArchiveResult:
        marked = self.metadata.mark_file_ingest_status_many(
            source_id=source_id,
            remote_path=raw_file.remote_path,
            timeframe=raw_file.timeframe.value,
            symbols=symbols,
            status=status,
            error_message=error_message,
            content_hash=raw_file.content_hash,
        )
        outcome = IngestFileResult(
            status=status,
            count=marked,
            archive_requested_count=len(symbols),
            archive_present_count=0,
            archive_missing_count=0,
        )
        archive_result = IngestArchiveResult()
        archive_result.add(outcome)
        if progress_callback is not None:
            progress_callback("", outcome)
        logger.warning(
            "archive ingest marked all symbols source_id=%s remote_path=%s status=%s count=%s error=%s",
            source_id,
            raw_file.remote_path,
            status,
            marked,
            error_message,
        )
        return archive_result

    def _record_parse_result(
        self,
        raw_file: LocalRawFile,
        *,
        symbol: str,
        source_id: str,
        result: ParseResult,
        log_details: bool,
    ) -> IngestFileResult:
        if result.status != "ok":
            metadata_started = time.perf_counter()
            if log_details:
                logger.warning(
                    "ingest parse status source_id=%s remote_path=%s symbol=%s status=%s error=%s",
                    source_id,
                    raw_file.remote_path,
                    symbol,
                    result.status,
                    result.error_message,
                )
            self.metadata.mark_file_ingest_status(
                source_id=source_id,
                remote_path=raw_file.remote_path,
                timeframe=raw_file.timeframe.value,
                symbol=symbol,
                status=result.status,
                error_message=result.error_message,
                content_hash=raw_file.content_hash,
            )
            logger.info(
                "ingest symbol timing source_id=%s remote_path=%s symbol=%s status=%s parse_seconds=%s metadata_seconds=%.3f",
                source_id,
                raw_file.remote_path,
                symbol,
                result.status,
                _seconds_text(result.parse_seconds),
                time.perf_counter() - metadata_started,
            )
            return IngestFileResult(status=result.status)

        df = result.dataframe
        if df.empty:
            metadata_started = time.perf_counter()
            if log_details:
                logger.info(
                    "ingest skipped empty rows source_id=%s remote_path=%s symbol=%s",
                    source_id,
                    raw_file.remote_path,
                    symbol,
                )
            self.metadata.mark_file_ingest_status(
                source_id=source_id,
                remote_path=raw_file.remote_path,
                timeframe=raw_file.timeframe.value,
                symbol=symbol,
                status="skipped",
                error_message="no rows in requested range",
                content_hash=raw_file.content_hash,
            )
            logger.info(
                "ingest symbol timing source_id=%s remote_path=%s symbol=%s status=skipped parse_seconds=%s metadata_seconds=%.3f",
                source_id,
                raw_file.remote_path,
                symbol,
                _seconds_text(result.parse_seconds),
                time.perf_counter() - metadata_started,
            )
            return IngestFileResult(status="skipped")

        write_started = time.perf_counter()
        written_paths = self.writer.write_bars(df, raw_file.timeframe)
        write_seconds = time.perf_counter() - write_started
        metadata_started = time.perf_counter()
        self._record_symbol(df)
        self._record_coverage(df, raw_file.timeframe)
        parquet_path = str(written_paths[0]) if written_paths else None
        self.metadata.commit_file_ingest(
            source_id=source_id,
            remote_path=raw_file.remote_path,
            timeframe=raw_file.timeframe.value,
            symbol=symbol,
            start_ts=_min_ts(df),
            end_ts=_exclusive_end(df, raw_file.timeframe),
            row_count=len(df),
            expected_row_count=expected_intraday_rows(raw_file.timeframe),
            content_hash=raw_file.content_hash,
            parquet_path=parquet_path,
        )
        metadata_seconds = time.perf_counter() - metadata_started
        if log_details:
            logger.info(
                "ingest committed source_id=%s remote_path=%s symbol=%s row_count=%s parquet_path=%s",
                source_id,
                raw_file.remote_path,
                symbol,
                len(df),
                parquet_path,
            )
        logger.info(
            "ingest symbol timing source_id=%s remote_path=%s symbol=%s status=committed rows=%s parse_seconds=%s write_seconds=%.3f metadata_seconds=%.3f parquet_paths=%s",
            source_id,
            raw_file.remote_path,
            symbol,
            len(df),
            _seconds_text(result.parse_seconds),
            write_seconds,
            metadata_seconds,
            len(written_paths),
        )
        return IngestFileResult(status="committed", committed=True)

    def _record_symbol(self, df: pd.DataFrame) -> None:
        for symbol, group in df.groupby("symbol"):
            code = str(group["code"].dropna().iloc[0]) if group["code"].notna().any() else symbol[2:]
            name = str(group["name"].dropna().iloc[0]) if group["name"].notna().any() else None
            self.metadata.upsert_symbol(symbol=symbol, code=code, name=name, exchange=symbol[:2])

    def _record_coverage(self, df: pd.DataFrame, timeframe: Timeframe) -> None:
        self.metadata.update_coverage_daily_many(_coverage_rows(df, timeframe))


def _file_ingest_status_row(
    raw_file: LocalRawFile,
    *,
    source_id: str,
    symbol: str,
    status: str,
    error_message: str | None,
) -> dict[str, Any]:
    return {
        "source_id": source_id,
        "remote_path": raw_file.remote_path,
        "timeframe": raw_file.timeframe.value,
        "symbol": symbol,
        "start_ts": None,
        "end_ts": None,
        "row_count": 0,
        "expected_row_count": None,
        "content_hash": raw_file.content_hash,
        "parquet_path": None,
        "status": status,
        "error_message": error_message,
    }


def _file_ingest_commit_row(
    raw_file: LocalRawFile,
    *,
    source_id: str,
    symbol: str,
    df: pd.DataFrame,
    parquet_path: str | None,
) -> dict[str, Any]:
    return {
        "source_id": source_id,
        "remote_path": raw_file.remote_path,
        "timeframe": raw_file.timeframe.value,
        "symbol": symbol,
        "start_ts": _min_ts(df),
        "end_ts": _exclusive_end(df, raw_file.timeframe),
        "row_count": len(df),
        "expected_row_count": expected_intraday_rows(raw_file.timeframe),
        "content_hash": raw_file.content_hash,
        "parquet_path": parquet_path,
        "status": "committed",
        "error_message": None,
    }


def _symbol_records(df: pd.DataFrame) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for symbol, group in df.groupby("symbol"):
        code = str(group["code"].dropna().iloc[0]) if group["code"].notna().any() else symbol[2:]
        name = str(group["name"].dropna().iloc[0]) if group["name"].notna().any() else None
        records.append(
            {
                "symbol": symbol,
                "code": code,
                "name": name,
                "exchange": symbol[:2],
                "listed_at": None,
                "delisted_at": None,
                "status": None,
                "source": "ingest",
            }
        )
    return records


def _coverage_rows(df: pd.DataFrame, timeframe: Timeframe) -> list[dict[str, Any]]:
    expected = expected_intraday_rows(timeframe)
    work = df.copy()
    work["ts"] = pd.to_datetime(work["ts"])
    work["trade_date"] = work["ts"].dt.date
    rows: list[dict[str, Any]] = []
    for (symbol, trade_date), group in work.groupby(["symbol", "trade_date"], sort=True):
        row_count = int(len(group))
        complete = row_count >= expected
        rows.append(
            {
                "symbol": symbol,
                "timeframe": timeframe.value,
                "trade_date": trade_date,
                "start_ts": group["ts"].min().to_pydatetime(),
                "end_ts": (group["ts"].max() + pd.Timedelta(minutes=timeframe.minute_span or 1)).to_pydatetime(),
                "row_count": row_count,
                "expected_row_count": expected,
                "is_complete": complete,
                "quality_flag": "ok" if complete else "partial",
            }
        )
    return rows


def _min_ts(df: pd.DataFrame) -> dt.datetime | None:
    if df.empty:
        return None
    return pd.to_datetime(df["ts"]).min().to_pydatetime()


def _exclusive_end(df: pd.DataFrame, timeframe: Timeframe) -> dt.datetime | None:
    if df.empty:
        return None
    minutes = timeframe.minute_span or 1
    return (pd.to_datetime(df["ts"]).max() + pd.Timedelta(minutes=minutes)).to_pydatetime()


def _seconds_text(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value:.3f}"
