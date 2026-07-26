from __future__ import annotations

import datetime as dt
import logging
import time
from dataclasses import dataclass
from typing import Iterable

logger = logging.getLogger(__name__)
MIN_TOTAL_SYMBOL_COUNT = 4000
MIN_LISTED_SYMBOL_COUNT = 3000
DEFAULT_FETCH_ATTEMPTS = 3


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
        errors: list[str] = []
        for attempt in range(1, DEFAULT_FETCH_ATTEMPTS + 1):
            try:
                listings = self._fetch_all_once()
                _validate_listing_set(listings)
                return listings
            except Exception as exc:
                errors.append(str(exc))
                if attempt >= DEFAULT_FETCH_ATTEMPTS:
                    break
                logger.warning(
                    "baostock security master fetch failed attempt=%s/%s error=%s",
                    attempt,
                    DEFAULT_FETCH_ATTEMPTS,
                    exc,
                )
                time.sleep(0.5 * attempt)

        last_error = errors[-1] if errors else "unknown error"
        raise RuntimeError(
            "baostock query_stock_basic failed to return a complete symbol list "
            f"after {DEFAULT_FETCH_ATTEMPTS} attempts: {last_error}"
        )

    def _fetch_all_once(self) -> list[SecurityListing]:
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


def _validate_listing_set(listings: list[SecurityListing]) -> None:
    listed = [listing for listing in listings if listing.status == "listed"]
    listed_exchanges = {listing.exchange for listing in listed}
    if (
        len(listings) < MIN_TOTAL_SYMBOL_COUNT
        or len(listed) < MIN_LISTED_SYMBOL_COUNT
        or not {"sh", "sz"}.issubset(listed_exchanges)
    ):
        raise RuntimeError(
            "baostock query_stock_basic returned incomplete symbol list: "
            f"total={len(listings)} listed={len(listed)} exchanges={sorted(listed_exchanges)}"
        )


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
