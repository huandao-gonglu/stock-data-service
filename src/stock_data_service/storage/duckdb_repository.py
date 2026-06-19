from __future__ import annotations

import datetime as dt
from pathlib import Path
from zoneinfo import ZoneInfo

import duckdb
import pandas as pd

from stock_data_service.market.calendar import SSETradingCalendar
from stock_data_service.market.timeframe import Timeframe

SHANGHAI = ZoneInfo("Asia/Shanghai")


class DuckDBRepository:
    def __init__(self, parquet_root: str | Path, metadata_db: str | Path):
        self.parquet_root = Path(parquet_root)
        self.metadata_db = Path(metadata_db)

    def query_bars(
        self,
        *,
        symbol: str,
        timeframe: Timeframe,
        start: dt.datetime,
        end: dt.datetime,
        limit: int = 5000,
        offset: int = 0,
    ) -> pd.DataFrame:
        files = self._candidate_files(symbol=symbol, timeframe=timeframe, start=start, end=end)
        if not files:
            return pd.DataFrame()

        with duckdb.connect() as con:
            con.from_parquet([str(path) for path in files]).create_view("bars")
            if timeframe == Timeframe.D1:
                sql = """
                    SELECT *
                    FROM bars
                    WHERE symbol = ? AND trade_date >= ? AND trade_date < ?
                    ORDER BY trade_date ASC
                    LIMIT ? OFFSET ?
                """
                params = [symbol, start.date(), end.date(), limit, offset]
            else:
                sql = """
                    SELECT *
                    FROM bars
                    WHERE symbol = ? AND ts >= ? AND ts < ?
                    ORDER BY ts ASC
                    LIMIT ? OFFSET ?
                """
                params = [symbol, start, end, limit, offset]
            return con.execute(sql, params).df()

    def coverage_summary(self, *, symbol: str, timeframe: Timeframe) -> dict:
        with duckdb.connect(str(self.metadata_db)) as con:
            rows = con.execute(
                """
                SELECT trade_date, start_ts, end_ts, row_count, expected_row_count, is_complete, quality_flag
                FROM coverage_daily
                WHERE symbol = ? AND timeframe = ?
                ORDER BY trade_date
                """,
                [symbol, timeframe.value],
            ).fetchall()
        if not rows:
            return {
                "symbol": symbol,
                "timeframe": timeframe.value,
                "start": None,
                "end": None,
                "complete": False,
                "complete_trade_dates": 0,
                "partial_trade_dates": 0,
                "missing_trade_dates": 0,
            }

        trade_dates = [row[0] for row in rows]
        present = set(trade_dates)
        calendar_days = SSETradingCalendar().get_trading_days(min(trade_dates), max(trade_dates))
        complete_count = sum(1 for row in rows if bool(row[5]))
        partial_count = sum(1 for row in rows if not bool(row[5]) or row[6] != "ok")
        missing_count = sum(1 for day in calendar_days if day not in present)
        start_ts = min((row[1] for row in rows if row[1] is not None), default=None)
        end_ts = max((row[2] for row in rows if row[2] is not None), default=None)
        return {
            "symbol": symbol,
            "timeframe": timeframe.value,
            "start": _format_ts(start_ts) if start_ts else None,
            "end": _format_ts(end_ts) if end_ts else None,
            "complete": missing_count == 0 and partial_count == 0,
            "complete_trade_dates": complete_count,
            "partial_trade_dates": partial_count,
            "missing_trade_dates": missing_count,
        }

    def coverage_gaps(
        self,
        *,
        symbol: str,
        timeframe: Timeframe,
        start: dt.date,
        end: dt.date,
    ) -> dict:
        with duckdb.connect(str(self.metadata_db)) as con:
            rows = con.execute(
                """
                SELECT trade_date, row_count, expected_row_count, is_complete, quality_flag
                FROM coverage_daily
                WHERE symbol = ? AND timeframe = ? AND trade_date >= ? AND trade_date <= ?
                ORDER BY trade_date
                """,
                [symbol, timeframe.value, start, end],
            ).fetchall()

        by_date = {row[0]: row for row in rows}
        missing: list[str] = []
        partial: list[dict] = []
        for day in SSETradingCalendar().get_trading_days(start, end):
            row = by_date.get(day)
            if row is None:
                missing.append(day.isoformat())
            elif not bool(row[3]) or row[4] != "ok":
                partial.append(
                    {
                        "date": day.isoformat(),
                        "expected_rows": int(row[2]),
                        "actual_rows": int(row[1]),
                        "quality_flag": row[4],
                    }
                )
        return {
            "symbol": symbol,
            "timeframe": timeframe.value,
            "start": start.isoformat(),
            "end": end.isoformat(),
            "complete": not missing and not partial,
            "missing_trade_dates": missing,
            "partial_dates": partial,
        }

    def _candidate_files(
        self,
        *,
        symbol: str,
        timeframe: Timeframe,
        start: dt.datetime,
        end: dt.datetime,
    ) -> list[Path]:
        months = _months_between(start.date(), (end - dt.timedelta(microseconds=1)).date())
        files: list[Path] = []
        for year, month in months:
            path = (
                self.parquet_root
                / timeframe.dataset_name
                / f"symbol={symbol}"
                / f"year={year:04d}"
                / f"month={month:02d}"
                / "data.parquet"
            )
            if path.exists():
                files.append(path)
        return files


def _months_between(start: dt.date, end: dt.date) -> list[tuple[int, int]]:
    first = dt.date(start.year, start.month, 1)
    last = dt.date(end.year, end.month, 1)
    current = first
    result = []
    while current <= last:
        result.append((current.year, current.month))
        if current.month == 12:
            current = dt.date(current.year + 1, 1, 1)
        else:
            current = dt.date(current.year, current.month + 1, 1)
    return result


def _format_ts(value: dt.datetime) -> str:
    return value.replace(tzinfo=SHANGHAI).isoformat()
