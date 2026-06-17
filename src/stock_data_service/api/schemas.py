from __future__ import annotations

from pydantic import BaseModel


class HealthResponse(BaseModel):
    status: str


class BarRow(BaseModel):
    ts: str | None = None
    trade_date: str | None = None
    open: float | None = None
    high: float | None = None
    low: float | None = None
    close: float | None = None
    volume: float | None = None
    amount: float | None = None
    code: str | None = None
    name: str | None = None
    change_pct: float | None = None
    amplitude: float | None = None


class BarsResponse(BaseModel):
    symbol: str
    timeframe: str
    start: str
    end: str
    range_semantics: str
    rows: list[BarRow]
    next_cursor: str | None


class AuthStatusResponse(BaseModel):
    has_token: bool
    expires_at: str | None = None
    is_expiring: bool = False
    has_refresh_token: bool = False


class CoverageSummaryResponse(BaseModel):
    symbol: str
    timeframe: str
    start: str | None
    end: str | None
    complete: bool
    complete_trade_dates: int
    partial_trade_dates: int
    missing_trade_dates: int


class PartialCoverageDate(BaseModel):
    date: str
    expected_rows: int
    actual_rows: int
    quality_flag: str


class CoverageGapsResponse(BaseModel):
    symbol: str
    timeframe: str
    start: str
    end: str
    complete: bool
    missing_trade_dates: list[str]
    partial_dates: list[PartialCoverageDate]
