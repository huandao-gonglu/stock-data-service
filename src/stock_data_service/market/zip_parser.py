from __future__ import annotations

import datetime as dt
import io
import zipfile
from dataclasses import dataclass
from pathlib import Path
from collections.abc import Iterator
from typing import Literal
from zoneinfo import ZoneInfo

import pandas as pd

from stock_data_service.market.normalizer import INTRADAY_COLUMNS, canonical_intraday
from stock_data_service.market.symbol_normalizer import normalize_symbol

ParseStatus = Literal["ok", "symbol_missing", "parse_failed", "corrupted_zip"]
SHANGHAI = ZoneInfo("Asia/Shanghai")


@dataclass
class ParseResult:
    dataframe: pd.DataFrame
    status: ParseStatus
    error_message: str | None = None


class ZipBarParser:
    def parse(
        self,
        zip_bytes: bytes | io.BytesIO,
        symbol: str,
        *,
        start: dt.datetime | None = None,
        end: dt.datetime | None = None,
        source_path: str | None = None,
        source: str = "baidu_netdisk",
    ) -> ParseResult:
        normalized = normalize_symbol(symbol)
        code = normalized[2:]
        try:
            with zipfile.ZipFile(_as_bytes_io(zip_bytes), "r") as archive:
                member = self._find_member(archive.namelist(), normalized, code)
                if member is None:
                    return ParseResult(_empty(), "symbol_missing", f"{symbol} CSV not found")
                content = archive.read(member)
        except zipfile.BadZipFile as exc:
            return ParseResult(_empty(), "corrupted_zip", str(exc))
        except Exception as exc:
            return ParseResult(_empty(), "parse_failed", str(exc))

        try:
            parsed = self._parse_csv(
                content,
                normalized,
                start=_to_shanghai_naive(start),
                end=_to_shanghai_naive(end),
                source_path=source_path,
                source=source,
            )
        except Exception as exc:
            return ParseResult(_empty(), "parse_failed", str(exc))

        return ParseResult(parsed, "ok")

    def iter_parse_archive(
        self,
        zip_path: str | Path,
        symbols: list[str],
        *,
        start: dt.datetime | None = None,
        end: dt.datetime | None = None,
        source_path: str | None = None,
        source: str = "baidu_netdisk",
    ) -> Iterator[tuple[str, ParseResult]]:
        normalized_symbols = [normalize_symbol(symbol) for symbol in symbols]
        try:
            with zipfile.ZipFile(zip_path, "r") as archive:
                member_index = self._build_member_index(archive.namelist())
                for normalized in normalized_symbols:
                    member = member_index.get(normalized)
                    if member is None:
                        yield normalized, ParseResult(_empty(), "symbol_missing", f"{normalized} CSV not found")
                        continue
                    try:
                        content = archive.read(member)
                        parsed = self._parse_csv(
                            content,
                            normalized,
                            start=_to_shanghai_naive(start),
                            end=_to_shanghai_naive(end),
                            source_path=source_path,
                            source=source,
                        )
                    except zipfile.BadZipFile as exc:
                        yield normalized, ParseResult(_empty(), "corrupted_zip", str(exc))
                    except Exception as exc:
                        yield normalized, ParseResult(_empty(), "parse_failed", str(exc))
                    else:
                        yield normalized, ParseResult(parsed, "ok")
        except zipfile.BadZipFile as exc:
            for normalized in normalized_symbols:
                yield normalized, ParseResult(_empty(), "corrupted_zip", str(exc))
        except Exception as exc:
            for normalized in normalized_symbols:
                yield normalized, ParseResult(_empty(), "parse_failed", str(exc))

    @staticmethod
    def _find_member(names: list[str], normalized: str, code: str) -> str | None:
        candidates = {
            f"{normalized}.csv",
            f"{normalized.upper()}.csv",
            f"{code}.csv",
            f"{code}.{normalized[:2].upper()}.csv",
        }
        lowered_candidates = {item.lower() for item in candidates}
        for name in names:
            base = name.rsplit("/", 1)[-1]
            if base in candidates or base.lower() in lowered_candidates or _is_symbol_archive_member(base, normalized, code):
                return name
        return None

    @staticmethod
    def _build_member_index(names: list[str]) -> dict[str, str]:
        index: dict[str, str] = {}
        for name in names:
            base = name.rsplit("/", 1)[-1]
            for symbol in _symbols_for_member(base):
                index.setdefault(symbol, name)
        return index

    @staticmethod
    def _parse_csv(
        content: bytes,
        symbol: str,
        *,
        start: dt.datetime | None,
        end: dt.datetime | None,
        source_path: str | None,
        source: str,
    ) -> pd.DataFrame:
        df = _read_csv_with_fallback(content)
        df.columns = [str(col).strip() for col in df.columns]
        df = df.rename(columns=_COLUMN_MAP)
        df.columns = [str(col).strip().lower() for col in df.columns]

        if "date_time" in df.columns:
            ts = pd.to_datetime(df["date_time"], errors="raise", format="mixed")
        elif "date" in df.columns and "time" in df.columns:
            date_part = df["date"].astype(str).str.strip()
            time_part = df["time"].map(_normalize_time)
            ts = pd.to_datetime(date_part + " " + time_part, errors="raise", format="mixed")
        elif "date" in df.columns:
            ts = pd.to_datetime(df["date"], errors="raise", format="mixed")
        else:
            raise ValueError("CSV is missing timestamp columns")

        result = pd.DataFrame(index=df.index)
        result["symbol"] = symbol
        result["code"] = df.get("code", symbol[2:])
        result["name"] = df.get("name")
        result["ts"] = ts
        for column in ["open", "high", "low", "close", "volume", "amount", "change_pct", "amplitude"]:
            result[column] = pd.to_numeric(df[column], errors="coerce") if column in df.columns else None
        result["source"] = source
        result["source_path"] = source_path
        result["ingested_at"] = dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)
        result["quality_flag"] = "ok"
        result = canonical_intraday(result)
        result = result.dropna(subset=["ts"])
        if start is not None:
            result = result[result["ts"] >= start]
        if end is not None:
            result = result[result["ts"] < end]
        return result.sort_values("ts").reset_index(drop=True)


_COLUMN_MAP = {
    "时间": "date_time",
    "日期": "date",
    "day": "date",
    "time": "time",
    "代码": "code",
    "名称": "name",
    "开盘价": "open",
    "开盘": "open",
    "open": "open",
    "最高价": "high",
    "最高": "high",
    "high": "high",
    "最低价": "low",
    "最低": "low",
    "low": "low",
    "收盘价": "close",
    "收盘": "close",
    "close": "close",
    "成交量": "volume",
    "vol": "volume",
    "volume": "volume",
    "成交额": "amount",
    "amount": "amount",
    "涨幅": "change_pct",
    "振幅": "amplitude",
}


def _read_csv_with_fallback(content: bytes) -> pd.DataFrame:
    last_error: Exception | None = None
    for encoding in ("utf-8-sig", "utf-8", "gbk", "gb18030", "utf-16"):
        try:
            return pd.read_csv(io.BytesIO(content), encoding=encoding)
        except UnicodeDecodeError as exc:
            last_error = exc
        except Exception as exc:
            last_error = exc
            break
    if last_error is not None:
        raise last_error
    raise ValueError("unable to read CSV")


def _is_symbol_archive_member(base_name: str, normalized: str, code: str) -> bool:
    lower = base_name.lower()
    if not lower.endswith(".csv"):
        return False
    stem = lower[:-4]
    prefixes = (normalized.lower(), code.lower())
    return any(stem.startswith(prefix + separator) for prefix in prefixes for separator in ("_", "-", "."))


def _symbols_for_member(base_name: str) -> set[str]:
    lower = base_name.lower()
    if not lower.endswith(".csv"):
        return set()
    stem = lower[:-4]
    candidates = {stem}
    for separator in ("_", "-", "."):
        if separator in stem:
            candidates.add(stem.split(separator, 1)[0])

    symbols: set[str] = set()
    for candidate in candidates:
        try:
            symbols.add(normalize_symbol(candidate))
        except ValueError:
            continue
    return symbols


def _normalize_time(value: object) -> str:
    text = str(value).strip()
    if ":" in text:
        return text
    if "." in text:
        text = text.split(".", 1)[0]
    text = text.zfill(4)
    return f"{text[:2]}:{text[2:4]}"


def _to_shanghai_naive(value: dt.datetime | None) -> dt.datetime | None:
    if value is None:
        return None
    if value.tzinfo is not None:
        return value.astimezone(SHANGHAI).replace(tzinfo=None)
    return value


def _as_bytes_io(value: bytes | io.BytesIO) -> io.BytesIO:
    if isinstance(value, io.BytesIO):
        value.seek(0)
        return value
    return io.BytesIO(value)


def _empty() -> pd.DataFrame:
    return pd.DataFrame(columns=INTRADAY_COLUMNS)
