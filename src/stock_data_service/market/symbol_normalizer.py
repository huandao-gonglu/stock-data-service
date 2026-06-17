from __future__ import annotations

import re
from dataclasses import dataclass


class SymbolValidationError(ValueError):
    """400-ready validation error for API-facing symbol input."""


@dataclass(frozen=True)
class NormalizedSymbol:
    symbol: str
    exchange: str
    code: str
    asset_type: str = "stock"


_EXPLICIT_PREFIX_RE = re.compile(r"^(sh|sz|bj)(\d{6})$", re.IGNORECASE)
_SUFFIX_RE = re.compile(r"^(\d{6})[.](SH|SZ|BJ)$", re.IGNORECASE)
_BARE_RE = re.compile(r"^\d{6}$")
_AMBIGUOUS_INDEX_CODES = {
    "000016",
    "000300",
    "000905",
    "000852",
    "399001",
    "399006",
}


def normalize_symbol(value: str, asset_type: str = "stock") -> str:
    return parse_symbol(value, asset_type=asset_type).symbol


def parse_symbol(value: str, asset_type: str = "stock") -> NormalizedSymbol:
    raw = value.strip()
    if not raw:
        raise SymbolValidationError("symbol is required")

    match = _EXPLICIT_PREFIX_RE.match(raw)
    if match:
        exchange, code = match.group(1).lower(), match.group(2)
        return NormalizedSymbol(f"{exchange}{code}", exchange, code, asset_type)

    match = _SUFFIX_RE.match(raw)
    if match:
        code, exchange = match.group(1), match.group(2).lower()
        return NormalizedSymbol(f"{exchange}{code}", exchange, code, asset_type)

    if not _BARE_RE.match(raw):
        raise SymbolValidationError(f"invalid symbol format: {value}")

    if asset_type != "index" and raw in _AMBIGUOUS_INDEX_CODES:
        raise SymbolValidationError(
            f"bare code {raw} is ambiguous; provide exchange prefix or asset_type=index"
        )

    exchange = _exchange_for_bare_code(raw, asset_type=asset_type)
    return NormalizedSymbol(f"{exchange}{raw}", exchange, raw, asset_type)


def _exchange_for_bare_code(code: str, asset_type: str) -> str:
    if asset_type == "index":
        if code.startswith(("399", "159")):
            return "sz"
        return "sh"

    if code.startswith(("600", "601", "603", "605", "688")):
        return "sh"
    if code.startswith(("000", "001", "002", "003", "300", "301")):
        return "sz"
    if code.startswith(("430", "8", "920")):
        return "bj"

    raise SymbolValidationError(f"unknown A-share code prefix: {code[:3]}")


def symbol_code(symbol: str) -> str:
    normalized = normalize_symbol(symbol)
    return normalized[2:]
