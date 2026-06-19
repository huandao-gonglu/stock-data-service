from __future__ import annotations

import datetime as dt

import pandas as pd

from stock_data_service.market.timeframe import Timeframe, expected_intraday_rows


INTRADAY_COLUMNS = [
    "symbol",
    "code",
    "name",
    "ts",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "amount",
    "change_pct",
    "amplitude",
    "source",
    "source_path",
    "ingested_at",
    "quality_flag",
]

INTRADAY_TEXT_COLUMNS = ["symbol", "code", "name", "source", "source_path", "quality_flag"]

DAILY_COLUMNS = [
    "symbol",
    "trade_date",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "amount",
    "source",
    "source_path",
    "ingested_at",
    "bar_count",
    "expected_bar_count",
    "is_complete",
    "quality_flag",
]

DAILY_TEXT_COLUMNS = ["symbol", "source", "source_path", "quality_flag"]


def canonical_intraday(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy()
    for col in INTRADAY_COLUMNS:
        if col not in result.columns:
            result[col] = None
    result = result[INTRADAY_COLUMNS]
    for col in INTRADAY_TEXT_COLUMNS:
        result[col] = _text_or_null(result[col])
    result["ts"] = pd.to_datetime(result["ts"])
    return result


def canonical_daily(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy()
    for col in DAILY_COLUMNS:
        if col not in result.columns:
            result[col] = None
    result = result[DAILY_COLUMNS]
    for col in DAILY_TEXT_COLUMNS:
        result[col] = _text_or_null(result[col])
    result["trade_date"] = pd.to_datetime(result["trade_date"]).dt.date
    return result


def synthesize_daily_from_intraday(
    df: pd.DataFrame,
    timeframe: Timeframe = Timeframe.M1,
) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=DAILY_COLUMNS)

    source = df.copy()
    source["ts"] = pd.to_datetime(source["ts"])
    source["trade_date"] = source["ts"].dt.date
    expected = expected_intraday_rows(timeframe)
    rows = []
    for (symbol, trade_date), group in source.groupby(["symbol", "trade_date"], sort=True):
        ordered = group.sort_values("ts")
        bar_count = int(len(ordered))
        complete = bar_count >= expected
        rows.append(
            {
                "symbol": symbol,
                "trade_date": trade_date,
                "open": ordered["open"].dropna().iloc[0] if ordered["open"].notna().any() else None,
                "high": ordered["high"].max(),
                "low": ordered["low"].min(),
                "close": ordered["close"].dropna().iloc[-1] if ordered["close"].notna().any() else None,
                "volume": ordered["volume"].sum(),
                "amount": ordered["amount"].sum(),
                "source": ordered["source"].dropna().iloc[0] if ordered["source"].notna().any() else None,
                "source_path": ordered["source_path"].dropna().iloc[0] if ordered["source_path"].notna().any() else None,
                "ingested_at": dt.datetime.now(dt.timezone.utc).replace(tzinfo=None),
                "bar_count": bar_count,
                "expected_bar_count": expected,
                "is_complete": bool(complete),
                "quality_flag": "ok" if complete else "partial",
            }
        )
    return pd.DataFrame(rows, columns=DAILY_COLUMNS)


def _text_or_null(series: pd.Series) -> pd.Series:
    return series.map(lambda value: None if pd.isna(value) else str(value))
