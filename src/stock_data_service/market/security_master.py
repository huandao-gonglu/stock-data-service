from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import Iterable


@dataclass(frozen=True)
class SecurityListing:
    symbol: str
    code: str
    name: str | None
    exchange: str
    listed_at: dt.date | None
    delisted_at: dt.date | None
    status: str
    source: str


class BaostockSecurityMasterClient:
    source = "baostock"

    def fetch_all(self) -> list[SecurityListing]:
        try:
            import baostock as bs
        except ImportError as exc:
            raise RuntimeError("baostock is not installed. Install project dependencies first.") from exc

        login = bs.login()
        if getattr(login, "error_code", "0") != "0":
            raise RuntimeError(f"baostock login failed: {getattr(login, 'error_msg', '')}")
        try:
            result = bs.query_stock_basic()
            if result.error_code != "0":
                raise RuntimeError(f"baostock query_stock_basic failed: {result.error_msg}")
            rows: list[dict[str, str]] = []
            while result.error_code == "0" and result.next():
                rows.append(dict(zip(result.fields, result.get_row_data(), strict=False)))
        finally:
            bs.logout()

        return list(_listings_from_baostock_rows(rows))


def _listings_from_baostock_rows(rows: Iterable[dict[str, str]]) -> Iterable[SecurityListing]:
    for row in rows:
        if row.get("type") and row.get("type") != "1":
            continue
        symbol = _normalize_baostock_code(row.get("code", ""))
        if symbol is None:
            continue
        exchange = symbol[:2]
        yield SecurityListing(
            symbol=symbol,
            code=symbol[2:],
            name=_blank_to_none(row.get("code_name")),
            exchange=exchange,
            listed_at=_parse_date(row.get("ipoDate")),
            delisted_at=_parse_date(row.get("outDate")),
            status="listed" if row.get("status") == "1" else "delisted",
            source=BaostockSecurityMasterClient.source,
        )


def _normalize_baostock_code(value: str) -> str | None:
    text = value.strip().lower()
    if "." not in text:
        return None
    exchange, code = text.split(".", 1)
    if exchange not in {"sh", "sz", "bj"} or not code.isdigit():
        return None
    return f"{exchange}{code}"


def _parse_date(value: str | None) -> dt.date | None:
    text = (value or "").strip()
    if not text:
        return None
    return dt.date.fromisoformat(text)


def _blank_to_none(value: str | None) -> str | None:
    text = (value or "").strip()
    return text or None
