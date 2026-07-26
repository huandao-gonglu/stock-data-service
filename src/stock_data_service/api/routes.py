from __future__ import annotations

import datetime as dt
from typing import Annotated
from zoneinfo import ZoneInfo

import pandas as pd
from fastapi import APIRouter, Depends, Header, HTTPException, Query, Request

from stock_data_service.api.admin import create_admin_router
from stock_data_service.api.schemas import (
    AuthStatusResponse,
    BarsResponse,
    CoverageGapsResponse,
    CoverageSummaryResponse,
    HealthResponse,
)
from stock_data_service.auth.token_manager import TokenManager
from stock_data_service.config import Settings
from stock_data_service.market.symbol_normalizer import SymbolValidationError, normalize_symbol
from stock_data_service.market.timeframe import Timeframe
from stock_data_service.storage.duckdb_repository import DuckDBRepository

SHANGHAI = ZoneInfo("Asia/Shanghai")


def create_router(settings: Settings) -> APIRouter:
    router = APIRouter()

    def query_auth(
        x_api_key: Annotated[str | None, Header(alias="X-API-Key")] = None,
        authorization: Annotated[str | None, Header()] = None,
    ) -> None:
        if not settings.server_mode or not settings.data_api_key:
            return
        if _extract_key(x_api_key, authorization) != settings.data_api_key:
            raise HTTPException(status_code=401, detail="invalid or missing data API key")

    def admin_auth(
        x_api_key: Annotated[str | None, Header(alias="X-API-Key")] = None,
        authorization: Annotated[str | None, Header()] = None,
    ) -> None:
        if not settings.server_mode or not settings.admin_api_key:
            return
        if _extract_key(x_api_key, authorization) != settings.admin_api_key:
            raise HTTPException(status_code=401, detail="invalid or missing admin API key")

    def repo() -> DuckDBRepository:
        return DuckDBRepository(settings.parquet_root, settings.metadata_db)

    @router.get("/health", response_model=HealthResponse)
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @router.get("/auth/baidu/status", response_model=AuthStatusResponse, dependencies=[Depends(admin_auth)])
    def baidu_auth_status() -> dict:
        manager = TokenManager(
            token_file=settings.baidu_token_file,
            app_key=settings.baidu_app_key,
            app_secret=settings.baidu_app_secret,
        )
        return manager.status()

    @router.get("/bars", response_model=BarsResponse, dependencies=[Depends(query_auth)])
    def bars(
        request: Request,
        symbol: str = Query(...),
        timeframe: str = Query(...),
        start: str = Query(...),
        end: str = Query(...),
        limit: int = Query(5000, ge=1, le=10000),
        cursor: str | None = Query(None),
        repository: DuckDBRepository = Depends(repo),
    ) -> dict:
        normalized, frame, start_dt, end_dt = _parse_query(symbol, timeframe, start, end)
        _enforce_range(frame, start_dt, end_dt)
        offset = _parse_cursor(cursor)
        df = repository.query_bars(
            symbol=normalized,
            timeframe=frame,
            start=start_dt,
            end=end_dt,
            limit=limit + 1,
            offset=offset,
        )
        has_more = len(df) > limit
        df = df.head(limit)
        return {
            "symbol": normalized,
            "timeframe": frame.value,
            "start": _format_ts(start_dt),
            "end": _format_ts(end_dt),
            "range_semantics": "[start,end)",
            "rows": [_row_to_response(row, frame) for _, row in df.iterrows()],
            "next_cursor": str(offset + limit) if has_more else None,
        }

    @router.get("/coverage/summary", response_model=CoverageSummaryResponse, dependencies=[Depends(query_auth)])
    def coverage_summary(
        symbol: str = Query(...),
        timeframe: str = Query(...),
        repository: DuckDBRepository = Depends(repo),
    ) -> dict:
        normalized = _normalize_or_400(symbol)
        frame = _timeframe_or_400(timeframe)
        return repository.coverage_summary(symbol=normalized, timeframe=frame)

    @router.get("/coverage/gaps", response_model=CoverageGapsResponse, dependencies=[Depends(query_auth)])
    def coverage_gaps(
        symbol: str = Query(...),
        timeframe: str = Query(...),
        start: str = Query(...),
        end: str = Query(...),
        repository: DuckDBRepository = Depends(repo),
    ) -> dict:
        normalized = _normalize_or_400(symbol)
        frame = _timeframe_or_400(timeframe)
        try:
            start_date = dt.date.fromisoformat(start)
            end_date = dt.date.fromisoformat(end)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return repository.coverage_gaps(symbol=normalized, timeframe=frame, start=start_date, end=end_date)

    router.include_router(create_admin_router(settings, admin_auth))
    return router


def _extract_key(x_api_key: str | None, authorization: str | None) -> str | None:
    if x_api_key:
        return x_api_key
    if authorization and authorization.lower().startswith("bearer "):
        return authorization.split(" ", 1)[1]
    return None


def _parse_query(symbol: str, timeframe: str, start: str, end: str) -> tuple[str, Timeframe, dt.datetime, dt.datetime]:
    normalized = _normalize_or_400(symbol)
    frame = _timeframe_or_400(timeframe)
    try:
        start_dt = _parse_api_datetime(start)
        end_dt = _parse_api_datetime(end)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if start_dt >= end_dt:
        raise HTTPException(status_code=400, detail="start must be before end")
    return normalized, frame, start_dt, end_dt


def _normalize_or_400(symbol: str) -> str:
    try:
        return normalize_symbol(symbol)
    except SymbolValidationError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


def _timeframe_or_400(value: str) -> Timeframe:
    try:
        return Timeframe.parse(value)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


def _parse_api_datetime(value: str) -> dt.datetime:
    parsed = dt.datetime.fromisoformat(value)
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(SHANGHAI).replace(tzinfo=None)
    return parsed


def _format_ts(value: dt.datetime | pd.Timestamp) -> str:
    if isinstance(value, pd.Timestamp):
        value = value.to_pydatetime()
    return value.replace(tzinfo=SHANGHAI).isoformat()


def _row_to_response(row: pd.Series, timeframe: Timeframe) -> dict:
    base = {
        "open": _none_if_na(row.get("open")),
        "high": _none_if_na(row.get("high")),
        "low": _none_if_na(row.get("low")),
        "close": _none_if_na(row.get("close")),
        "volume": _none_if_na(row.get("volume")),
        "amount": _none_if_na(row.get("amount")),
        "code": _none_if_na(row.get("code")),
        "name": _none_if_na(row.get("name")),
        "change_pct": _none_if_na(row.get("change_pct")),
        "amplitude": _none_if_na(row.get("amplitude")),
    }
    if timeframe == Timeframe.D1:
        value = row.get("trade_date")
        if isinstance(value, pd.Timestamp):
            value = value.date()
        elif isinstance(value, dt.datetime):
            value = value.date()
        if hasattr(value, "isoformat"):
            base["trade_date"] = value.isoformat()
        else:
            base["trade_date"] = str(value)
    else:
        base["ts"] = _format_ts(row["ts"])
    return base


def _none_if_na(value):
    if pd.isna(value):
        return None
    return value


def _parse_cursor(cursor: str | None) -> int:
    if cursor is None:
        return 0
    try:
        value = int(cursor)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="cursor must be an integer offset") from exc
    if value < 0:
        raise HTTPException(status_code=400, detail="cursor must be non-negative")
    return value


def _enforce_range(timeframe: Timeframe, start: dt.datetime, end: dt.datetime) -> None:
    if (end.date() - start.date()).days + 1 > timeframe.max_range_days:
        raise HTTPException(
            status_code=400,
            detail=f"{timeframe.value} requests are limited to {timeframe.max_range_days} natural days",
        )
