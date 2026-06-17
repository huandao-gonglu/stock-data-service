from __future__ import annotations

import datetime as dt
import logging
from pathlib import Path

import pandas as pd

from stock_data_service.market.symbol_normalizer import normalize_symbol
from stock_data_service.market.timeframe import Timeframe, expected_intraday_rows
from stock_data_service.market.zip_parser import ZipBarParser
from stock_data_service.storage.parquet_writer import ParquetBarWriter
from stock_data_service.storage.sync_metadata import SyncMetadata
from stock_data_service.sync.local_source import LocalRawFile

logger = logging.getLogger(__name__)


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
        if result.status != "ok":
            logger.warning(
                "ingest parse status source_id=%s remote_path=%s symbol=%s status=%s error=%s",
                source_id,
                raw_file.remote_path,
                normalized,
                result.status,
                result.error_message,
            )
            self.metadata.mark_file_ingest_status(
                source_id=source_id,
                remote_path=raw_file.remote_path,
                timeframe=raw_file.timeframe.value,
                symbol=normalized,
                status=result.status,
                error_message=result.error_message,
                content_hash=raw_file.content_hash,
            )
            return False

        df = result.dataframe
        if df.empty:
            logger.info(
                "ingest skipped empty rows source_id=%s remote_path=%s symbol=%s",
                source_id,
                raw_file.remote_path,
                normalized,
            )
            self.metadata.mark_file_ingest_status(
                source_id=source_id,
                remote_path=raw_file.remote_path,
                timeframe=raw_file.timeframe.value,
                symbol=normalized,
                status="skipped",
                error_message="no rows in requested range",
                content_hash=raw_file.content_hash,
            )
            return False

        written_paths = self.writer.write_bars(df, raw_file.timeframe)
        self._record_symbol(df)
        self._record_coverage(df, raw_file.timeframe)
        parquet_path = str(written_paths[0]) if written_paths else None
        self.metadata.commit_file_ingest(
            source_id=source_id,
            remote_path=raw_file.remote_path,
            timeframe=raw_file.timeframe.value,
            symbol=normalized,
            start_ts=_min_ts(df),
            end_ts=_exclusive_end(df, raw_file.timeframe),
            row_count=len(df),
            expected_row_count=expected_intraday_rows(raw_file.timeframe),
            content_hash=raw_file.content_hash,
            parquet_path=parquet_path,
        )
        logger.info(
            "ingest committed source_id=%s remote_path=%s symbol=%s row_count=%s parquet_path=%s",
            source_id,
            raw_file.remote_path,
            normalized,
            len(df),
            parquet_path,
        )
        return True

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
        for (symbol, trade_date), group in work.groupby(["symbol", "trade_date"], sort=True):
            row_count = int(len(group))
            complete = row_count >= expected
            self.metadata.update_coverage_daily(
                symbol=symbol,
                timeframe=timeframe.value,
                trade_date=trade_date,
                start_ts=group["ts"].min().to_pydatetime(),
                end_ts=(group["ts"].max() + pd.Timedelta(minutes=timeframe.minute_span or 1)).to_pydatetime(),
                row_count=row_count,
                expected_row_count=expected,
                is_complete=complete,
                quality_flag="ok" if complete else "partial",
            )


def _min_ts(df: pd.DataFrame) -> dt.datetime | None:
    if df.empty:
        return None
    return pd.to_datetime(df["ts"]).min().to_pydatetime()


def _exclusive_end(df: pd.DataFrame, timeframe: Timeframe) -> dt.datetime | None:
    if df.empty:
        return None
    minutes = timeframe.minute_span or 1
    return (pd.to_datetime(df["ts"]).max() + pd.Timedelta(minutes=minutes)).to_pydatetime()
