from __future__ import annotations

import os
import logging
import tempfile
from pathlib import Path

import pandas as pd

from stock_data_service.market.normalizer import DAILY_COLUMNS, INTRADAY_COLUMNS, canonical_intraday
from stock_data_service.market.timeframe import Timeframe
from stock_data_service.sync.locks import partition_lock

logger = logging.getLogger(__name__)


class ParquetBarWriter:
    def __init__(self, parquet_root: str | Path):
        self.parquet_root = Path(parquet_root)

    def write_bars(self, dataframe: pd.DataFrame, timeframe: Timeframe) -> list[Path]:
        if dataframe.empty:
            return []
        if timeframe == Timeframe.D1:
            return self._write_daily(dataframe)
        return self._write_intraday(dataframe, timeframe)

    def partition_path(
        self,
        *,
        timeframe: Timeframe,
        symbol: str,
        year: int,
        month: int,
    ) -> Path:
        return (
            self.parquet_root
            / timeframe.dataset_name
            / f"symbol={symbol}"
            / f"year={year:04d}"
            / f"month={month:02d}"
            / "data.parquet"
        )

    def _write_intraday(self, dataframe: pd.DataFrame, timeframe: Timeframe) -> list[Path]:
        df = canonical_intraday(dataframe)
        df["ts"] = pd.to_datetime(df["ts"])
        df["year"] = df["ts"].dt.year
        df["month"] = df["ts"].dt.month
        written: list[Path] = []
        for (symbol, year, month), group in df.groupby(["symbol", "year", "month"], sort=True):
            path = self.partition_path(timeframe=timeframe, symbol=symbol, year=int(year), month=int(month))
            with partition_lock(timeframe.value, symbol, int(year), int(month)):
                merged = self._merge(path, group[INTRADAY_COLUMNS], subset=["symbol", "ts"], sort_col="ts")
                self._atomic_write(path, merged[INTRADAY_COLUMNS])
                logger.info(
                    "parquet partition written timeframe=%s symbol=%s year=%s month=%s rows=%s path=%s",
                    timeframe.value,
                    symbol,
                    int(year),
                    int(month),
                    len(merged),
                    path,
                )
            written.append(path)
        return written

    def _write_daily(self, dataframe: pd.DataFrame) -> list[Path]:
        df = dataframe.copy()
        for col in DAILY_COLUMNS:
            if col not in df.columns:
                df[col] = None
        df = df[DAILY_COLUMNS]
        df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
        ts = pd.to_datetime(df["trade_date"])
        df["year"] = ts.dt.year
        df["month"] = ts.dt.month
        written: list[Path] = []
        for (symbol, year, month), group in df.groupby(["symbol", "year", "month"], sort=True):
            path = self.partition_path(timeframe=Timeframe.D1, symbol=symbol, year=int(year), month=int(month))
            with partition_lock(Timeframe.D1.value, symbol, int(year), int(month)):
                merged = self._merge(path, group[DAILY_COLUMNS], subset=["symbol", "trade_date"], sort_col="trade_date")
                self._atomic_write(path, merged[DAILY_COLUMNS])
                logger.info(
                    "parquet partition written timeframe=%s symbol=%s year=%s month=%s rows=%s path=%s",
                    Timeframe.D1.value,
                    symbol,
                    int(year),
                    int(month),
                    len(merged),
                    path,
                )
            written.append(path)
        return written

    @staticmethod
    def _merge(path: Path, new_rows: pd.DataFrame, *, subset: list[str], sort_col: str) -> pd.DataFrame:
        if path.exists():
            old_rows = pd.read_parquet(path)
            combined = pd.concat([old_rows, new_rows], ignore_index=True)
        else:
            combined = new_rows.copy()
        return (
            combined.drop_duplicates(subset=subset, keep="last")
            .sort_values(sort_col)
            .reset_index(drop=True)
        )

    @staticmethod
    def _atomic_write(path: Path, dataframe: pd.DataFrame) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp_name = tempfile.mkstemp(prefix="data.", suffix=".parquet.tmp", dir=path.parent)
        os.close(fd)
        tmp_path = Path(tmp_name)
        try:
            dataframe.to_parquet(tmp_path, index=False)
            os.replace(tmp_path, path)
        finally:
            if tmp_path.exists():
                tmp_path.unlink()
