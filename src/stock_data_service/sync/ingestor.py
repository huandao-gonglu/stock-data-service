from __future__ import annotations

import datetime as dt
import logging
import time
import zipfile
from collections.abc import Callable
from dataclasses import dataclass, field, replace

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


class Ingestor:
    def __init__(
        self,
        *,
        writer: ParquetBarWriter,
        metadata: SyncMetadata,
        parser: ZipBarParser | None = None,
    ):
        self.writer = writer
        self.metadata = metadata
        self.parser = parser or ZipBarParser()

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
        normalized_symbols = [normalize_symbol(symbol) for symbol in symbols]
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

        for parsed_symbol, parse_result in self.parser.iter_parse_archive(
            raw_file.local_path,
            present_symbols,
            start=start,
            end=end,
            source_path=raw_file.remote_path,
            member_index=member_index,
        ):
            if cancel_check is not None:
                cancel_check()
            if progress_callback is not None:
                progress_callback(parsed_symbol, with_archive_counts(IngestFileResult(status="ingesting", count=0)))
            try:
                self.metadata.start_file_ingest(
                    source_id=source_id,
                    remote_path=raw_file.remote_path,
                    timeframe=raw_file.timeframe.value,
                    symbol=parsed_symbol,
                    content_hash=raw_file.content_hash,
                )
                outcome = self._record_parse_result(
                    raw_file,
                    symbol=parsed_symbol,
                    source_id=source_id,
                    result=parse_result,
                    log_details=False,
                )
                outcome = with_archive_counts(outcome)
            except Exception as exc:
                logger.exception(
                    "archive ingest exception source_id=%s remote_path=%s symbol=%s",
                    source_id,
                    raw_file.remote_path,
                    parsed_symbol,
                )
                self.metadata.mark_file_ingest_status(
                    source_id=source_id,
                    remote_path=raw_file.remote_path,
                    timeframe=raw_file.timeframe.value,
                    symbol=parsed_symbol,
                    status="failed",
                    error_message=str(exc),
                    content_hash=raw_file.content_hash,
                )
                outcome = with_archive_counts(IngestFileResult(status="failed"))
            archive_result.add(outcome)
            if progress_callback is not None:
                progress_callback(parsed_symbol, outcome)

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
        expected = expected_intraday_rows(timeframe)
        work = df.copy()
        work["ts"] = pd.to_datetime(work["ts"])
        work["trade_date"] = work["ts"].dt.date
        rows: list[dict] = []
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
        self.metadata.update_coverage_daily_many(rows)


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
