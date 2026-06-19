from __future__ import annotations

import datetime as dt
import logging
from dataclasses import asdict
from typing import Any
from typing import Callable

from fastapi import APIRouter, Body, Depends, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, Response

from stock_data_service.admin_settings import AdminSettingsStore, validate_admin_sync_settings
from stock_data_service.baidu.pan_client import BaiduPanClient
from stock_data_service.auth.baidu_authorizer import BaiduAuthorizationError, BaiduOAuthStateStore, BaiduWebAuthorizer
from stock_data_service.auth.token_manager import TokenManager
from stock_data_service.config import Settings
from stock_data_service.market.calendar import SSETradingCalendar, natural_days
from stock_data_service.market.symbol_normalizer import SymbolValidationError, normalize_symbol
from stock_data_service.market.timeframe import Timeframe
from stock_data_service.storage.sync_metadata import SyncMetadata
from stock_data_service.sync.manager import ManagedFileSyncRequest, ManagedSyncRequest, SyncJobManager

logger = logging.getLogger(__name__)
BAIDU_DATA_ROOT = "/A股_分时数据"
MAX_CALENDAR_RANGE_DAYS = 366 * 80


def create_admin_router(settings: Settings, admin_auth_dependency: Callable) -> APIRouter:
    parent_router = APIRouter()
    callback_router = APIRouter()
    router = APIRouter(dependencies=[Depends(admin_auth_dependency)])
    oauth_states = BaiduOAuthStateStore()

    @callback_router.get("/callback", response_class=HTMLResponse, name="baidu_oauth_callback")
    def baidu_oauth_callback(
        request: Request,
        code: str | None = Query(default=None),
        state: str | None = Query(default=None),
        error: str | None = Query(default=None),
        error_description: str | None = Query(default=None),
    ) -> HTMLResponse:
        if error:
            message = error_description or error
            return HTMLResponse(_oauth_result_page(False, f"百度授权失败：{message}"), status_code=400)
        if not code or not state:
            return HTMLResponse(_oauth_result_page(False, "百度授权回调缺少 code 或 state"), status_code=400)
        try:
            _baidu_authorizer(request, settings, oauth_states).exchange_code(code=code, state=state)
        except Exception as exc:
            logger.exception("baidu oauth callback failed")
            return HTMLResponse(_oauth_result_page(False, str(exc)), status_code=400)
        logger.info("baidu oauth callback finished token_file=%s", settings.baidu_token_file)
        return HTMLResponse(_oauth_result_page(True, "百度授权成功"))

    @router.get("/admin", response_class=HTMLResponse)
    def admin_page() -> HTMLResponse:
        return HTMLResponse(_ADMIN_HTML)

    @router.get("/admin/calendar", response_class=HTMLResponse)
    def admin_calendar_page() -> HTMLResponse:
        return HTMLResponse(_ADMIN_CALENDAR_HTML)

    @router.get("/favicon.ico")
    def favicon() -> Response:
        return Response(status_code=204)

    @router.get("/admin/api/settings")
    def get_settings(request: Request) -> dict:
        store = AdminSettingsStore(settings)
        token_manager = TokenManager(
            token_file=settings.baidu_token_file,
            app_key=settings.baidu_app_key,
            app_secret=settings.baidu_app_secret,
        )
        return {
            "system": {
                "data_root": str(settings.data_root),
                "metadata_db": str(settings.metadata_db),
                "parquet_root": str(settings.parquet_root),
                "baidu_cache_dir": str(settings.baidu_cache_dir),
                "logs_dir": str(settings.logs_dir),
                "log_level": settings.log_level,
                "server_mode": settings.server_mode,
                "data_api_key_configured": bool(settings.data_api_key),
                "admin_api_key_configured": bool(settings.admin_api_key),
                "baidu_app_key_configured": bool(settings.baidu_app_key),
                "baidu_app_secret_configured": bool(settings.baidu_app_secret),
                "baidu_token_file": str(settings.baidu_token_file),
                "baidu_token_file_exists": settings.baidu_token_file.exists(),
                "baidu_redirect_uri": settings.baidu_redirect_uri,
                "baidu_effective_redirect_uri": _baidu_redirect_uri(request, settings),
                "baidu_scope": settings.baidu_scope,
            },
            "sync_defaults": asdict(store.load()),
            "baidu_auth": token_manager.status(),
        }

    @router.post("/admin/api/baidu/oauth/start")
    def start_baidu_oauth(request: Request) -> dict[str, str]:
        redirect_uri = _baidu_redirect_uri(request, settings)
        try:
            payload = _baidu_authorizer(request, settings, oauth_states).authorization_url(redirect_uri)
        except BaiduAuthorizationError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        logger.info("baidu oauth started redirect_uri=%s", redirect_uri)
        return payload

    @router.post("/admin/api/settings")
    def save_settings(payload: dict = Body(...)) -> dict:
        try:
            saved = AdminSettingsStore(settings).save(payload)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        logger.info(
            "admin settings saved source_id=%s timeframe=%s symbol_count=%s",
            saved.source_id,
            saved.timeframe,
            len(saved.symbols),
        )
        return {"sync_defaults": asdict(saved)}

    @router.get("/admin/api/sync/status")
    def sync_status(request: Request) -> dict:
        return {
            "manager": _sync_manager(request, settings).status(),
            "recent_jobs": _recent_sync_jobs(settings),
        }

    @router.get("/admin/api/symbols")
    def list_symbols(
        status: str = Query("listed"),
        limit: int = Query(10000, ge=1, le=100000),
    ) -> dict:
        rows = _symbol_rows(settings, status=status, limit=limit)
        return {
            "status": status,
            "count": len(rows),
            "symbols": [
                {
                    "symbol": row[0],
                    "code": row[1],
                    "name": row[2],
                    "exchange": row[3],
                    "listed_at": _iso(row[4]),
                    "delisted_at": _iso(row[5]),
                    "status": row[6],
                    "source": row[7],
                }
                for row in rows
            ],
        }

    @router.get("/admin/api/coverage/calendar")
    def coverage_calendar(
        symbol: str = Query(...),
        timeframe: str = Query(...),
        start: str = Query(...),
        end: str = Query(...),
    ) -> dict:
        normalized = _normalize_symbol_or_400(symbol)
        frame = _timeframe_or_400(timeframe)
        start_date = _date_or_400(start, "start")
        end_date = _date_or_400(end, "end")
        if start_date > end_date:
            raise HTTPException(status_code=400, detail="start must be before or equal to end")
        if (end_date - start_date).days > MAX_CALENDAR_RANGE_DAYS:
            raise HTTPException(status_code=400, detail="calendar range must be 80 years or less")
        metadata = SyncMetadata(settings.metadata_db)
        metadata.initialize()
        return _coverage_calendar(
            metadata=metadata,
            symbol=normalized,
            timeframe=frame,
            start=start_date,
            end=end_date,
        )

    @router.get("/admin/api/baidu/list")
    def list_baidu_files(
        request: Request,
        path: str = BAIDU_DATA_ROOT,
        source_id: str = "baidu-main",
        page: int = Query(1, ge=1),
        limit: int = Query(100, ge=1, le=500),
    ) -> dict:
        dir_path = _normalize_remote_dir(path)
        metadata = SyncMetadata(settings.metadata_db)
        metadata.initialize()
        try:
            entries, pagination = _list_baidu_dir(
                _baidu_client(request, settings),
                metadata,
                source_id,
                dir_path,
                page=page,
                limit=limit,
            )
        except Exception as exc:
            logger.exception("admin baidu list failed path=%s source_id=%s", dir_path, source_id)
            raise HTTPException(status_code=502, detail=str(exc)) from exc
        return {
            "path": dir_path,
            "parent": _parent_dir(dir_path),
            "source_id": source_id,
            "pagination": pagination,
            "entries": entries,
        }

    @router.post("/admin/api/sync/start")
    def start_sync(request: Request, payload: dict | None = Body(default=None)) -> dict:
        try:
            sync_settings = validate_admin_sync_settings(payload or asdict(AdminSettingsStore(settings).load()))
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        managed_request = ManagedSyncRequest(
            source_id=sync_settings.source_id,
            timeframe=sync_settings.timeframe,
            start=sync_settings.start,
            end=sync_settings.end,
            symbols=sync_settings.symbols,
        )
        try:
            job = _sync_manager(request, settings).start_baidu_sync(managed_request)
        except RuntimeError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        logger.info("admin sync start requested job_id=%s", job.id)
        return {"job": job.to_dict()}

    @router.post("/admin/api/sync/file")
    def start_file_sync(request: Request, payload: dict = Body(...)) -> dict:
        remote_path = _normalize_remote_file(payload.get("remote_path"))
        try:
            sync_settings = validate_admin_sync_settings(payload)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        managed_request = ManagedFileSyncRequest(
            source_id=sync_settings.source_id,
            timeframe=sync_settings.timeframe,
            start=sync_settings.start,
            end=sync_settings.end,
            symbols=sync_settings.symbols,
            remote_path=remote_path,
        )
        try:
            job = _sync_manager(request, settings).start_baidu_file_sync(managed_request)
        except RuntimeError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        logger.info("admin file sync start requested job_id=%s remote_path=%s", job.id, remote_path)
        return {"job": job.to_dict()}

    @router.post("/admin/api/sync/stop")
    def stop_sync(request: Request, payload: dict | None = Body(default=None)) -> dict:
        job_id = payload.get("job_id") if payload else None
        try:
            job = _sync_manager(request, settings).request_stop(job_id)
        except RuntimeError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        logger.info("admin sync stop requested job_id=%s", job.id)
        return {"job": job.to_dict()}

    parent_router.include_router(callback_router)
    parent_router.include_router(router)
    return parent_router


def _coverage_calendar(
    *,
    metadata: SyncMetadata,
    symbol: str,
    timeframe: Timeframe,
    start: dt.date,
    end: dt.date,
) -> dict:
    rows = metadata.fetchall(
        """
        SELECT trade_date, row_count, expected_row_count, is_complete, quality_flag
        FROM coverage_daily
        WHERE symbol = ? AND timeframe = ? AND trade_date >= ? AND trade_date <= ?
        ORDER BY trade_date
        """,
        [symbol, timeframe.value, start, end],
    )
    by_date = {row[0]: row for row in rows}
    trading_days = set(SSETradingCalendar().get_trading_days(start, end))
    days: list[dict] = []
    counts = {
        "has_data": 0,
        "missing": 0,
        "non_trading": 0,
        "trading": 0,
        "complete": 0,
        "partial": 0,
    }
    for day in natural_days(start, end):
        row = by_date.get(day)
        is_trading = day in trading_days
        has_data = row is not None and int(row[1] or 0) > 0
        if has_data:
            status = "has_data"
            counts["has_data"] += 1
            if is_trading:
                counts["trading"] += 1
            if bool(row[3]) and row[4] == "ok":
                counts["complete"] += 1
            else:
                counts["partial"] += 1
        elif not is_trading:
            status = "non_trading"
            counts["non_trading"] += 1
        else:
            status = "missing"
            counts["missing"] += 1
            counts["trading"] += 1
        days.append(
            {
                "date": day.isoformat(),
                "status": status,
                "is_trading_day": is_trading,
                "row_count": int(row[1]) if row else 0,
                "expected_row_count": int(row[2]) if row else None,
                "is_complete": bool(row[3]) if row else False,
                "quality_flag": row[4] if row else None,
            }
        )
    return {
        "symbol": symbol,
        "timeframe": timeframe.value,
        "start": start.isoformat(),
        "end": end.isoformat(),
        "counts": counts,
        "days": days,
    }


def _normalize_symbol_or_400(value: str) -> str:
    try:
        return normalize_symbol(value)
    except SymbolValidationError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


def _timeframe_or_400(value: str) -> Timeframe:
    try:
        return Timeframe.parse(value)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


def _date_or_400(value: str, field: str) -> dt.date:
    try:
        return dt.date.fromisoformat(value)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=f"invalid {field} date: {value}") from exc


def _sync_manager(request: Request, settings: Settings) -> SyncJobManager:
    manager = getattr(request.app.state, "sync_manager", None)
    if manager is None:
        manager = SyncJobManager(settings)
        request.app.state.sync_manager = manager
    return manager


def _baidu_client(request: Request, settings: Settings) -> BaiduPanClient:
    factory = getattr(request.app.state, "baidu_client_factory", None)
    if factory is not None:
        return factory(settings)
    token_manager = TokenManager(
        token_file=settings.baidu_token_file,
        app_key=settings.baidu_app_key,
        app_secret=settings.baidu_app_secret,
    )
    return BaiduPanClient(token_manager=token_manager, enable_cache=True, cache_dir=settings.baidu_cache_dir)


def _baidu_authorizer(request: Request, settings: Settings, state_store: BaiduOAuthStateStore) -> BaiduWebAuthorizer:
    session_factory = getattr(request.app.state, "baidu_oauth_session_factory", None)
    session = session_factory() if session_factory is not None else None
    return BaiduWebAuthorizer(
        app_key=settings.baidu_app_key,
        app_secret=settings.baidu_app_secret,
        token_file=str(settings.baidu_token_file),
        scope=settings.baidu_scope,
        state_store=state_store,
        session=session,
    )


def _baidu_redirect_uri(request: Request, settings: Settings) -> str:
    if settings.baidu_redirect_uri:
        return settings.baidu_redirect_uri
    return str(request.url_for("baidu_oauth_callback"))


def _oauth_result_page(ok: bool, message: str) -> str:
    status = "success" if ok else "error"
    title = "授权成功" if ok else "授权失败"
    return f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{_escape_html(title)}</title>
  <style>
    body {{
      margin: 0;
      min-height: 100vh;
      display: grid;
      place-items: center;
      background: #f6f7f8;
      color: #1f2933;
      font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }}
    main {{
      width: min(420px, calc(100vw - 32px));
      border: 1px solid #d8dde3;
      border-radius: 8px;
      background: #fff;
      padding: 24px;
    }}
    h1 {{ margin: 0 0 8px; font-size: 18px; letter-spacing: 0; }}
    p {{ margin: 0; color: #667085; line-height: 1.6; }}
  </style>
</head>
<body>
  <main>
    <h1>{_escape_html(title)}</h1>
    <p>{_escape_html(message)}</p>
  </main>
  <script>
    if (window.opener) {{
      window.opener.postMessage({{type: "baidu-oauth", status: "{status}"}}, window.location.origin);
    }}
    setTimeout(() => window.close(), {1200 if ok else 3000});
  </script>
</body>
</html>"""


def _escape_html(value: Any) -> str:
    return (
        str(value)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&#39;")
    )


def _list_baidu_dir(
    client: BaiduPanClient,
    metadata: SyncMetadata,
    source_id: str,
    dir_path: str,
    *,
    page: int,
    limit: int,
) -> tuple[list[dict], dict]:
    entries: list[dict] = []
    start = (page - 1) * limit
    fetch_limit = limit + 1
    payload = client.list_files(dir_path, start=start, limit=fetch_limit)
    items = payload.get("list", [])
    for item in items[:limit]:
        entry = _baidu_entry(item, metadata, source_id, dir_path)
        if entry:
            entries.append(entry)
    has_more = len(items) > limit or bool(payload.get("has_more"))
    total = _int_or_none(payload.get("total"))
    return entries, {
        "page": page,
        "limit": limit,
        "start": start,
        "returned_count": len(entries),
        "has_more": has_more,
        "prev_page": page - 1 if page > 1 else None,
        "next_page": page + 1 if has_more else None,
        "total": total,
    }


def _baidu_entry(item: dict, metadata: SyncMetadata, source_id: str, dir_path: str) -> dict | None:
    name = item.get("server_filename") or item.get("name")
    if not name:
        return None
    remote_path = item.get("path")
    if not remote_path:
        remote_path = f"{dir_path.rstrip('/')}/{name}" if dir_path != "/" else f"/{name}"
    remote_path = str(remote_path).replace("\\", "/")
    is_dir = item.get("isdir") in (1, "1", True)
    server_mtime = _server_mtime(item.get("server_mtime"))
    saved = None if is_dir else metadata.get_remote_file(source_id, remote_path)
    status, label, update_reasons = _sync_status_for_entry(
        is_dir=is_dir,
        size=item.get("size"),
        md5=item.get("md5"),
        server_mtime=server_mtime,
        saved=saved,
    )
    return {
        "name": str(name),
        "path": remote_path,
        "is_dir": is_dir,
        "size": item.get("size"),
        "md5": item.get("md5"),
        "server_mtime": _iso(server_mtime),
        "sync_status": status,
        "status_label": label,
        "update_reasons": update_reasons,
        "saved": _saved_remote_file(saved),
    }


def _sync_status_for_entry(
    *,
    is_dir: bool,
    size: int | None,
    md5: str | None,
    server_mtime: dt.datetime | None,
    saved: dict | None,
) -> tuple[str, str, list[str]]:
    if is_dir:
        return "directory", "目录", []
    if saved is None:
        return "not_synced", "未同步", []
    update_reasons = _metadata_changes(size=size, md5=md5, server_mtime=server_mtime, saved=saved)
    if update_reasons:
        return "update_available", "有更新", update_reasons
    if saved.get("status") == "failed":
        return "failed", "失败", []
    if saved.get("status") == "downloaded":
        return "synced", "已同步", []
    return str(saved.get("status") or "discovered"), "已发现", []


def _metadata_changes(*, size: int | None, md5: str | None, server_mtime: dt.datetime | None, saved: dict) -> list[str]:
    changes: list[str] = []
    if _changed_number(size, saved.get("size")):
        changes.append("size")
    if _changed_text(md5, saved.get("md5")):
        changes.append("md5")
    if _changed_datetime(server_mtime, saved.get("server_mtime")):
        changes.append("server_mtime")
    return changes


def _changed_number(remote: object, saved: object) -> bool:
    if remote is None:
        return False
    if saved is None:
        return True
    try:
        return int(remote) != int(saved)
    except (TypeError, ValueError):
        return str(remote) != str(saved)


def _changed_text(remote: object, saved: object) -> bool:
    if not remote:
        return False
    if not saved:
        return True
    return str(remote).lower() != str(saved).lower()


def _changed_datetime(remote: dt.datetime | None, saved: object) -> bool:
    if remote is None:
        return False
    if saved is None:
        return True
    if isinstance(saved, dt.datetime):
        return remote != saved.replace(tzinfo=None)
    return remote.isoformat() != str(saved)


def _saved_remote_file(saved: dict | None) -> dict | None:
    if saved is None:
        return None
    return {
        "status": saved.get("status"),
        "size": saved.get("size"),
        "md5": saved.get("md5"),
        "server_mtime": _iso(saved.get("server_mtime")),
        "downloaded_at": _iso(saved.get("downloaded_at")),
        "local_raw_path": saved.get("local_raw_path"),
        "error_message": saved.get("error_message"),
    }


def _server_mtime(value: object) -> dt.datetime | None:
    if value is None or value == "":
        return None
    if isinstance(value, dt.datetime):
        return value.replace(tzinfo=None)
    return dt.datetime.fromtimestamp(int(value), tz=dt.timezone.utc).replace(tzinfo=None)


def _int_or_none(value: object) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _normalize_remote_dir(path: object) -> str:
    text = str(path or BAIDU_DATA_ROOT).strip().replace("\\", "/")
    if not text:
        text = BAIDU_DATA_ROOT
    if not text.startswith("/"):
        text = "/" + text
    return text.rstrip("/") or "/"


def _normalize_remote_file(path: object) -> str:
    text = str(path or "").strip().replace("\\", "/")
    if not text:
        raise HTTPException(status_code=400, detail="remote_path is required")
    if not text.startswith("/"):
        text = "/" + text
    if text.endswith("/"):
        raise HTTPException(status_code=400, detail="remote_path must be a file path")
    return text


def _parent_dir(path: str) -> str | None:
    if path == "/":
        return None
    parent = path.rstrip("/").rsplit("/", 1)[0]
    return parent or "/"


def _recent_sync_jobs(settings: Settings) -> list[dict]:
    metadata = SyncMetadata(settings.metadata_db)
    metadata.initialize()
    rows = metadata.fetchall(
        """
        SELECT id, source_id, status, started_at, finished_at, scanned_count,
               downloaded_count, ingested_count, failed_count, error_message
        FROM sync_jobs
        ORDER BY started_at DESC
        LIMIT 20
        """
    )
    return [
        {
            "id": row[0],
            "source_id": row[1],
            "status": row[2],
            "started_at": _iso(row[3]),
            "finished_at": _iso(row[4]),
            "scanned_count": row[5],
            "downloaded_count": row[6],
            "ingested_count": row[7],
            "failed_count": row[8],
            "error_message": row[9],
        }
        for row in rows
    ]


def _symbol_rows(settings: Settings, *, status: str, limit: int) -> list:
    metadata = SyncMetadata(settings.metadata_db)
    metadata.initialize()
    return metadata.fetchall(
        """
        SELECT symbol, code, name, exchange, listed_at, delisted_at, status, source
        FROM symbols
        WHERE (? = 'all' OR status = ?)
        ORDER BY exchange, code
        LIMIT ?
        """,
        [status, status, limit],
    )


def _iso(value) -> str | None:
    return value.isoformat() if hasattr(value, "isoformat") else value


_ADMIN_HTML = """<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Stock Data Service 管理台</title>
  <style>
    :root {
      color-scheme: light;
      --bg: #f6f7f8;
      --panel: #ffffff;
      --ink: #1f2933;
      --muted: #667085;
      --line: #d8dde3;
      --accent: #16794c;
      --accent-ink: #ffffff;
      --danger: #b42318;
      --warn: #b54708;
      --ok: #067647;
      font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      background: var(--bg);
      color: var(--ink);
      font-size: 14px;
      line-height: 1.45;
    }
    header {
      border-bottom: 1px solid var(--line);
      background: var(--panel);
      padding: 14px 22px;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 16px;
      position: sticky;
      top: 0;
      z-index: 4;
    }
    h1 {
      margin: 0;
      font-size: 18px;
      font-weight: 650;
      letter-spacing: 0;
    }
    main {
      width: min(1320px, 100%);
      margin: 0 auto;
      padding: 18px 22px 28px;
      display: grid;
      gap: 16px;
    }
    .toolbar, section {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 8px;
    }
    .toolbar {
      padding: 12px;
      display: grid;
      grid-template-columns: minmax(180px, 1fr) repeat(4, auto);
      gap: 10px;
      align-items: end;
    }
    .grid {
      display: grid;
      grid-template-columns: minmax(340px, 460px) minmax(0, 1fr);
      gap: 16px;
    }
    section > h2 {
      margin: 0;
      padding: 13px 14px;
      border-bottom: 1px solid var(--line);
      font-size: 14px;
      font-weight: 650;
      letter-spacing: 0;
    }
    .section-body { padding: 14px; }
    label {
      display: grid;
      gap: 6px;
      color: var(--muted);
      font-size: 12px;
      font-weight: 560;
    }
    input, select, textarea {
      width: 100%;
      min-height: 36px;
      border: 1px solid var(--line);
      border-radius: 6px;
      padding: 8px 10px;
      color: var(--ink);
      background: #fff;
      font: inherit;
      letter-spacing: 0;
    }
    textarea {
      min-height: 74px;
      resize: vertical;
    }
    button, .nav-button {
      min-height: 36px;
      border: 1px solid var(--line);
      border-radius: 6px;
      padding: 0 12px;
      background: #fff;
      color: var(--ink);
      font: inherit;
      font-weight: 620;
      cursor: pointer;
      white-space: nowrap;
      display: inline-flex;
      align-items: center;
      justify-content: center;
      text-decoration: none;
    }
    button.primary {
      background: var(--accent);
      border-color: var(--accent);
      color: var(--accent-ink);
    }
    button.danger {
      border-color: #f2b8b5;
      color: var(--danger);
    }
    button:disabled {
      opacity: .55;
      cursor: not-allowed;
    }
    .form-grid {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 12px;
    }
    .form-grid .span-2 { grid-column: span 2; }
    .kv {
      display: grid;
      grid-template-columns: 170px minmax(0, 1fr);
      gap: 8px 12px;
      margin: 0;
    }
    .kv dt {
      color: var(--muted);
      font-weight: 560;
    }
    .kv dd {
      margin: 0;
      min-width: 0;
      overflow-wrap: anywhere;
      font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
      font-size: 12px;
    }
    .status-line {
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
      align-items: center;
      margin-bottom: 12px;
    }
    .progress-row {
      display: grid;
      grid-template-columns: minmax(0, 1fr) 48px;
      gap: 10px;
      align-items: center;
      margin: 4px 0 10px;
    }
    .progress {
      height: 10px;
      overflow: hidden;
      border: 1px solid var(--line);
      border-radius: 999px;
      background: #eef1f4;
    }
    .progress > div {
      width: 0%;
      height: 100%;
      background: var(--accent);
      transition: width .2s ease;
    }
    .pathbar {
      display: grid;
      grid-template-columns: minmax(240px, 1fr) repeat(2, auto);
      gap: 10px;
      align-items: end;
      margin-bottom: 10px;
    }
    .pager {
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
      align-items: end;
      margin-bottom: 10px;
    }
    .pager label {
      width: 120px;
    }
    .pill {
      display: inline-flex;
      align-items: center;
      min-height: 24px;
      padding: 0 8px;
      border-radius: 999px;
      border: 1px solid var(--line);
      color: var(--muted);
      background: #fff;
      font-size: 12px;
      font-weight: 620;
    }
    .pill.ok { color: var(--ok); border-color: #abefc6; }
    .pill.warn { color: var(--warn); border-color: #fedf89; }
    .pill.bad { color: var(--danger); border-color: #f2b8b5; }
    table {
      width: 100%;
      border-collapse: collapse;
      table-layout: fixed;
    }
    th, td {
      border-bottom: 1px solid var(--line);
      padding: 9px 8px;
      text-align: left;
      vertical-align: top;
      overflow-wrap: anywhere;
    }
    th {
      color: var(--muted);
      font-size: 12px;
      font-weight: 650;
      background: #fafbfc;
    }
    .muted { color: var(--muted); }
    .mono {
      font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
      font-size: 12px;
    }
    .actions {
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
    }
    .actions button {
      min-height: 30px;
      padding: 0 10px;
    }
    .message {
      min-height: 22px;
      color: var(--muted);
      font-size: 12px;
    }
    @media (max-width: 900px) {
      header, main { padding-left: 14px; padding-right: 14px; }
      .toolbar, .grid { grid-template-columns: 1fr; }
      .pathbar { grid-template-columns: 1fr; }
      .pager label { width: 100%; }
      .form-grid { grid-template-columns: 1fr; }
      .form-grid .span-2 { grid-column: auto; }
      .kv { grid-template-columns: 1fr; }
    }
  </style>
</head>
<body>
  <header>
    <h1>Stock Data Service 管理台</h1>
    <span id="topStatus" class="pill">读取中</span>
  </header>
  <main>
    <form class="toolbar" onsubmit="return false">
      <label>Admin Key
        <input id="adminKey" type="password" autocomplete="off" placeholder="server mode">
      </label>
      <button id="refreshBtn" type="button">↻ 刷新</button>
      <a class="nav-button" href="/admin/calendar">数据日历</a>
      <button id="startBtn" class="primary" type="button">▶ 同步指定股票</button>
      <button id="stopBtn" class="danger" type="button">■ 停止</button>
    </form>
    <div class="grid">
      <section>
        <h2>同步设置</h2>
        <div class="section-body">
          <div class="form-grid">
            <label>Source ID
              <input id="sourceId" value="baidu-main">
            </label>
            <label>周期
              <select id="timeframe">
                <option value="1m">1m</option>
                <option value="5m">5m</option>
                <option value="15m">15m</option>
                <option value="30m">30m</option>
                <option value="60m">60m</option>
              </select>
            </label>
            <label>开始日期
              <input id="startDate" type="date">
            </label>
            <label>结束日期
              <input id="endDate" type="date">
            </label>
            <label class="span-2">
              <span style="display:flex;align-items:center;justify-content:space-between;gap:10px;">
                <span>股票代码</span>
                <button id="fullSyncBtn" type="button">↓ 填入全市场股票</button>
              </span>
              <textarea id="symbols" spellcheck="false"></textarea>
            </label>
          </div>
          <div style="display:flex;gap:10px;margin-top:12px;">
            <button id="saveSettingsBtn" type="button">✓ 保存设置</button>
          </div>
          <p id="settingsMessage" class="message"></p>
        </div>
      </section>
      <section>
        <h2>运行状态</h2>
        <div class="section-body">
          <div class="status-line">
            <span id="activeStatus" class="pill">无运行任务</span>
            <span id="activeCounts" class="pill">0 / 0 / 0</span>
            <span id="downloadSpeed" class="pill">速度 0 B/s</span>
            <span id="etaDetail" class="pill">预计 -</span>
            <span id="ingestDetail" class="pill">入库 -</span>
          </div>
          <div class="progress-row">
            <div class="progress"><div id="progressBar"></div></div>
            <span id="progressText" class="mono">0%</span>
          </div>
          <p id="activeStage" class="message"></p>
          <div class="status-line">
            <span id="baiduAuthStatus" class="pill">百度授权读取中</span>
            <span id="baiduAuthExpires" class="pill">Token -</span>
            <button id="authorizeBaiduBtn" type="button">百度授权</button>
          </div>
          <p id="authMessage" class="message"></p>
          <dl id="systemInfo" class="kv"></dl>
        </div>
      </section>
    </div>
    <section>
      <h2>网盘数据目录</h2>
      <div class="section-body">
        <div class="pathbar">
          <label>目录
            <input id="netdiskPath" value="/A股_分时数据" spellcheck="false">
          </label>
          <button id="parentDirBtn" type="button">↑ 上级</button>
          <button id="loadNetdiskBtn" type="button">↻ 加载</button>
        </div>
        <div class="pager">
          <label>每页
            <select id="netdiskLimit">
              <option value="50">50</option>
              <option value="100" selected>100</option>
              <option value="200">200</option>
              <option value="500">500</option>
            </select>
          </label>
          <button id="prevPageBtn" type="button">← 上一页</button>
          <span id="netdiskPageInfo" class="pill">第 1 页</span>
          <button id="nextPageBtn" type="button">下一页 →</button>
        </div>
        <p id="netdiskMessage" class="message"></p>
      </div>
      <div class="section-body" style="padding:0;">
        <table>
          <thead>
            <tr>
              <th style="width:26%;">名称</th>
              <th style="width:8%;">类型</th>
              <th style="width:10%;">大小</th>
              <th style="width:16%;">更新时间</th>
              <th style="width:18%;">Hash</th>
              <th style="width:11%;">状态</th>
              <th style="width:11%;">操作</th>
            </tr>
          </thead>
          <tbody id="netdiskBody"></tbody>
        </table>
      </div>
    </section>
    <section>
      <h2>同步记录</h2>
      <div class="section-body" style="padding:0;">
        <table>
          <thead>
            <tr>
              <th style="width:22%;">任务</th>
              <th style="width:12%;">来源</th>
              <th style="width:12%;">状态</th>
              <th style="width:18%;">开始</th>
              <th style="width:18%;">结束</th>
              <th style="width:18%;">计数</th>
            </tr>
          </thead>
          <tbody id="jobsBody"></tbody>
        </table>
      </div>
    </section>
  </main>
  <script>
    const $ = (id) => document.getElementById(id);
    const apiKeyInput = $("adminKey");
    let activeJobId = null;
    let netdiskPage = 1;
    let netdiskHasMore = false;
    let baiduAuthPoll = null;
    let baiduHasToken = false;
    apiKeyInput.value = localStorage.getItem("stockDataAdminKey") || "";
    apiKeyInput.addEventListener("input", () => localStorage.setItem("stockDataAdminKey", apiKeyInput.value));
    $("netdiskLimit").value = localStorage.getItem("stockDataNetdiskLimit") || "100";

    function escapeHtml(value) {
      return String(value ?? "").replace(/[&<>"']/g, (char) => ({
        "&": "&amp;",
        "<": "&lt;",
        ">": "&gt;",
        '"': "&quot;",
        "'": "&#39;"
      }[char]));
    }
    function formatBytes(value) {
      if (value === null || value === undefined || value === "") return "";
      const bytes = Number(value);
      if (!Number.isFinite(bytes)) return String(value);
      const units = ["B", "KB", "MB", "GB", "TB"];
      let size = bytes;
      let unit = 0;
      while (size >= 1024 && unit < units.length - 1) {
        size /= 1024;
        unit += 1;
      }
      return `${size.toFixed(unit === 0 ? 0 : 1)} ${units[unit]}`;
    }
    function formatTime(value) {
      return value ? String(value).replace("T", " ") : "";
    }
    function shortHash(value) {
      const text = String(value || "");
      return text.length > 16 ? `${text.slice(0, 8)}…${text.slice(-6)}` : text;
    }
    function fileName(path) {
      const text = String(path || "");
      return text.split("/").filter(Boolean).pop() || "";
    }
    function countPair(done, total) {
      const safeDone = Number(done || 0);
      if (total === null || total === undefined || total === "") return String(safeDone);
      const safeTotal = Number(total);
      return Number.isFinite(safeTotal) && safeTotal > 0 ? `${safeDone}/${safeTotal}` : String(safeDone);
    }
    function formatRate(value) {
      return `${formatBytes(value)}/s`;
    }
    function formatDuration(seconds) {
      const total = Math.max(0, Math.round(Number(seconds || 0)));
      const days = Math.floor(total / 86400);
      const hours = Math.floor((total % 86400) / 3600);
      const minutes = Math.floor((total % 3600) / 60);
      const secs = total % 60;
      if (days > 0) return hours > 0 ? `${days}天${hours}小时` : `${days}天`;
      if (hours > 0) return minutes > 0 ? `${hours}小时${minutes}分钟` : `${hours}小时`;
      if (minutes > 0) return secs > 0 && minutes < 10 ? `${minutes}分钟${secs}秒` : `${minutes}分钟`;
      return `${secs}秒`;
    }
    function etaConfidenceLabel(value) {
      return ({
        warming_up: "估算中",
        stable: "稳定",
        volatile: "波动"
      }[value] || "");
    }
    function etaDisplay(active) {
      if (!active || active.eta_seconds === null || active.eta_seconds === undefined) {
        return "预计 估算中";
      }
      const confidence = etaConfidenceLabel(active.eta_confidence);
      const rate = Number(active.progress_rate_percent_per_min || 0);
      const rateText = rate > 0 ? ` · ${rate >= 10 ? rate.toFixed(0) : rate.toFixed(1)}%/分钟` : "";
      const confidenceText = confidence && confidence !== "稳定" ? ` · ${confidence}` : "";
      return `预计剩余 ${formatDuration(active.eta_seconds)}${confidenceText}${rateText}`;
    }
    function headers() {
      const key = apiKeyInput.value.trim();
      return key ? {"Content-Type": "application/json", "X-API-Key": key} : {"Content-Type": "application/json"};
    }
    async function api(path, options = {}) {
      const response = await fetch(path, {...options, headers: {...headers(), ...(options.headers || {})}});
      if (!response.ok) {
        let message = response.statusText;
        try { message = (await response.json()).detail || message; } catch {}
        throw new Error(message);
      }
      return await response.json();
    }
    function currentPayload() {
      return {
        source_id: $("sourceId").value.trim(),
        timeframe: $("timeframe").value,
        start: $("startDate").value,
        end: $("endDate").value,
        symbols: $("symbols").value.split(/[,\\n]/).map((item) => item.trim()).filter(Boolean)
      };
    }
    function setForm(sync) {
      $("sourceId").value = sync.source_id || "baidu-main";
      $("timeframe").value = sync.timeframe || "1m";
      $("startDate").value = sync.start || "";
      $("endDate").value = sync.end || "";
      $("symbols").value = (sync.symbols || []).join("\\n");
    }
    function statusClass(status) {
      if (["completed", "running"].includes(status)) return "pill ok";
      if (["queued", "stopping", "completed_with_errors", "stopped"].includes(status)) return "pill warn";
      if (["failed"].includes(status)) return "pill bad";
      return "pill";
    }
    function netdiskStatusClass(status) {
      if (status === "synced") return "pill ok";
      if (status === "update_available") return "pill warn";
      if (status === "failed") return "pill bad";
      return "pill";
    }
    function ingestStatusLabel(status) {
      return ({
        ingesting: "处理中",
        committed: "已提交",
        skipped: "跳过",
        symbol_missing: "归档未包含",
        parse_failed: "解析失败",
        corrupted_zip: "ZIP 损坏",
        failed: "失败"
      }[status] || status || "");
    }
    function renderBaiduAuth(settings) {
      const system = settings.system;
      const auth = settings.baidu_auth;
      const configured = !!(system.baidu_app_key_configured && system.baidu_app_secret_configured);
      baiduHasToken = !!auth.has_token;
      $("baiduAuthStatus").className = auth.has_token ? (auth.is_expiring ? "pill warn" : "pill ok") : "pill bad";
      $("baiduAuthStatus").textContent = auth.has_token ? (auth.is_expiring ? "百度授权即将过期" : "百度已授权") : "百度未授权";
      $("baiduAuthExpires").textContent = auth.expires_at ? `到期 ${formatTime(auth.expires_at)}` : "无到期时间";
      $("authorizeBaiduBtn").disabled = !configured;
      $("authorizeBaiduBtn").textContent = auth.has_token ? "重新授权" : "百度授权";
      $("loadNetdiskBtn").disabled = !baiduHasToken;
      $("parentDirBtn").disabled = !baiduHasToken;
      if (!configured) {
        $("authMessage").textContent = "未配置 BAIDU_APP_KEY / BAIDU_APP_SECRET";
      } else if ($("authMessage").textContent === "未配置 BAIDU_APP_KEY / BAIDU_APP_SECRET") {
        $("authMessage").textContent = "";
      }
    }
    function renderSystem(settings) {
      const system = settings.system;
      const auth = settings.baidu_auth;
      renderBaiduAuth(settings);
      const rows = [
        ["Data Root", system.data_root],
        ["Parquet", system.parquet_root],
        ["Meta DB", system.metadata_db],
        ["Raw Cache", system.baidu_cache_dir],
        ["Logs", system.logs_dir],
        ["Server Mode", system.server_mode ? "on" : "off"],
        ["Baidu App Key", system.baidu_app_key_configured ? "configured" : "missing"],
        ["Baidu App Secret", system.baidu_app_secret_configured ? "configured" : "missing"],
        ["Baidu Token", auth.has_token ? "has token" : "missing"],
        ["Refresh Token", auth.has_refresh_token ? "yes" : "no"],
        ["Token Expiring", auth.is_expiring ? "yes" : "no"],
        ["Baidu Scope", system.baidu_scope || ""],
        ["Redirect URI", system.baidu_effective_redirect_uri || system.baidu_redirect_uri || "auto"]
      ];
      $("systemInfo").innerHTML = rows.map(([k, v]) => `<dt>${k}</dt><dd>${v ?? ""}</dd>`).join("");
    }
    function renderStatus(data) {
      const active = data.manager.active_job;
      const previousActiveJobId = activeJobId;
      if (active) {
        activeJobId = active.id;
        $("activeStatus").className = statusClass(active.status);
        $("activeStatus").textContent = active.status;
        const archiveProgress = countPair(active.scanned_count, active.planned_download_count);
        const ingestProgress = countPair(active.ingest_processed_count, active.ingest_total_count);
        const currentArchiveRequested = active.current_archive_requested_count;
        const currentArchivePresent = active.current_archive_present_count ?? active.current_archive_ingest_total_count;
        const currentArchiveMissing = Number(active.current_archive_missing_count || 0);
        const currentArchiveIngestProgress = countPair(
          active.current_archive_ingest_processed_count,
          currentArchivePresent
        );
        const currentArchiveParts = [`实际 ${currentArchiveIngestProgress}`];
        if (currentArchiveRequested !== null && currentArchiveRequested !== undefined) {
          currentArchiveParts.push(`请求 ${currentArchiveRequested}`);
        }
        if (currentArchiveMissing > 0) {
          currentArchiveParts.push(`归档未包含 ${currentArchiveMissing}`);
        }
        const currentArchiveText = currentArchiveParts.join(" · ");
        $("activeCounts").textContent = `归档 ${archiveProgress} · 下载 ${active.downloaded_count} · 入库 ${active.ingested_count} (${ingestProgress}) · 失败 ${active.failed_count}`;
        const progress = Math.max(0, Math.min(Number(active.progress_percent || 0), 100));
        $("progressBar").style.width = `${progress}%`;
        $("progressText").textContent = `${progress}%`;
        $("activeStage").textContent = active.stage || "";
        const speed = Number(active.download_speed_bytes_per_sec || 0);
        const downloadedBytes = Number(active.downloaded_bytes || 0);
        const totalBytes = active.download_total_bytes === null || active.download_total_bytes === undefined ? null : Number(active.download_total_bytes);
        const totalText = totalBytes ? ` / ${formatBytes(totalBytes)}` : "";
        const nameText = active.current_download_path ? ` · ${fileName(active.current_download_path)}` : "";
        $("downloadSpeed").textContent = `速度 ${formatRate(speed)}${downloadedBytes ? ` · ${formatBytes(downloadedBytes)}${totalText}` : ""}${nameText}`;
        $("etaDetail").textContent = etaDisplay(active);
        const ingestSymbol = active.current_ingest_symbol || "";
        const ingestPath = active.current_ingest_path ? ` · ${fileName(active.current_ingest_path)}` : "";
        const ingestStatus = ingestStatusLabel(active.current_ingest_status);
        const ingestStatusText = ingestStatus ? ` · ${ingestStatus}` : "";
        $("ingestDetail").textContent = ingestSymbol
          ? `入库 ${ingestSymbol} · 当前归档 ${currentArchiveText} · 总 ${ingestProgress}${ingestStatusText}${ingestPath}`
          : `入库 当前归档 ${currentArchiveText} · 总 ${ingestProgress}${ingestStatusText}${ingestPath}`;
      } else {
        activeJobId = null;
        $("activeStatus").className = "pill";
        $("activeStatus").textContent = "无运行任务";
        $("activeCounts").textContent = "扫描 0 · 下载 0 · 入库 0 · 失败 0";
        $("downloadSpeed").textContent = "速度 0 B/s";
        $("etaDetail").textContent = "预计 -";
        $("ingestDetail").textContent = "入库 -";
        $("progressBar").style.width = "0%";
        $("progressText").textContent = "0%";
        $("activeStage").textContent = "";
        if (previousActiveJobId) {
          loadNetdisk().catch(() => {});
        }
      }
      $("stopBtn").disabled = !(active && ["queued", "running", "stopping"].includes(active.status));
      $("startBtn").disabled = !!(active && ["queued", "running", "stopping"].includes(active.status));
      $("fullSyncBtn").disabled = !!(active && ["queued", "running", "stopping"].includes(active.status));
      const rows = data.recent_jobs || [];
      $("jobsBody").innerHTML = rows.length ? rows.map((job) => `
        <tr>
          <td>${job.id}</td>
          <td>${job.source_id}</td>
          <td><span class="${statusClass(job.status)}">${job.status}</span></td>
          <td>${job.started_at || ""}</td>
          <td>${job.finished_at || ""}</td>
          <td>扫描 ${job.scanned_count ?? 0}<br>下载 ${job.downloaded_count ?? 0}<br>入库 ${job.ingested_count ?? 0}<br>失败 ${job.failed_count ?? 0}</td>
        </tr>`).join("") : `<tr><td colspan="6" class="muted">暂无记录</td></tr>`;
    }
    function parentPath(path) {
      const normalized = (path || "/A股_分时数据").replace(/\\/+$/, "");
      if (!normalized || normalized === "/") return "/";
      const index = normalized.lastIndexOf("/");
      return index <= 0 ? "/" : normalized.slice(0, index);
    }
    function renderNetdisk(data) {
      $("netdiskPath").value = data.path || "/A股_分时数据";
      const rows = data.entries || [];
      const pagination = data.pagination || {page: 1, limit: Number($("netdiskLimit").value || 100), has_more: false, returned_count: rows.length};
      netdiskPage = Number(pagination.page || 1);
      netdiskHasMore = !!pagination.has_more;
      $("netdiskLimit").value = String(pagination.limit || $("netdiskLimit").value || 100);
      const totalText = pagination.total !== null && pagination.total !== undefined ? ` · 总计 ${pagination.total}` : "";
      $("netdiskPageInfo").textContent = `第 ${netdiskPage} 页`;
      $("prevPageBtn").disabled = netdiskPage <= 1;
      $("nextPageBtn").disabled = !netdiskHasMore;
      $("netdiskMessage").textContent = rows.length ? `本页 ${rows.length} 项${totalText}${netdiskHasMore ? " · 还有下一页" : " · 已到末页"}` : "目录为空";
      $("netdiskBody").innerHTML = rows.length ? rows.map((entry) => {
        const action = entry.is_dir
          ? `<button type="button" data-action="open" data-path="${escapeHtml(entry.path)}">打开</button>`
          : `<button type="button" data-action="sync" data-path="${escapeHtml(entry.path)}">同步</button>`;
        const reasons = entry.update_reasons && entry.update_reasons.length ? `<br><span class="muted">${escapeHtml(entry.update_reasons.join(", "))}</span>` : "";
        return `
          <tr>
            <td class="mono">${escapeHtml(entry.name)}</td>
            <td>${entry.is_dir ? "目录" : "文件"}</td>
            <td>${entry.is_dir ? "" : escapeHtml(formatBytes(entry.size))}</td>
            <td>${escapeHtml(formatTime(entry.server_mtime))}</td>
            <td class="mono">${escapeHtml(shortHash(entry.md5))}</td>
            <td><span class="${netdiskStatusClass(entry.sync_status)}">${escapeHtml(entry.status_label || entry.sync_status)}</span>${reasons}</td>
            <td><div class="actions">${action}</div></td>
          </tr>`;
      }).join("") : `<tr><td colspan="7" class="muted">暂无文件</td></tr>`;
    }
    async function loadNetdisk(path = $("netdiskPath").value, page = netdiskPage) {
      if (!baiduHasToken) {
        $("netdiskMessage").textContent = "百度未授权";
        $("netdiskBody").innerHTML = `<tr><td colspan="7" class="muted">百度未授权</td></tr>`;
        $("prevPageBtn").disabled = true;
        $("nextPageBtn").disabled = true;
        return {entries: [], pagination: {page: 1, limit: Number($("netdiskLimit").value || 100), has_more: false}};
      }
      const sourceId = $("sourceId").value.trim() || "baidu-main";
      const targetPath = (path || "/A股_分时数据").trim();
      const limit = Number($("netdiskLimit").value || 100);
      $("netdiskMessage").textContent = "读取中";
      const data = await api(`/admin/api/baidu/list?path=${encodeURIComponent(targetPath)}&source_id=${encodeURIComponent(sourceId)}&page=${encodeURIComponent(page)}&limit=${encodeURIComponent(limit)}`, {method: "GET"});
      renderNetdisk(data);
      return data;
    }
    async function syncFile(remotePath) {
      $("netdiskMessage").textContent = "已提交同步任务";
      await api("/admin/api/sync/file", {method: "POST", body: JSON.stringify({...currentPayload(), remote_path: remotePath})});
      await refreshStatusOnly();
    }
    async function refreshAll() {
      try {
        const settings = await api("/admin/api/settings", {method: "GET"});
        setForm(settings.sync_defaults);
        renderSystem(settings);
        const status = await api("/admin/api/sync/status", {method: "GET"});
        renderStatus(status);
        try {
          await loadNetdisk();
        } catch (err) {
          $("netdiskMessage").textContent = err.message;
        }
        $("topStatus").className = "pill ok";
        $("topStatus").textContent = "已连接";
      } catch (err) {
        $("topStatus").className = "pill bad";
        $("topStatus").textContent = err.message;
      }
    }
    async function refreshStatusOnly() {
      try {
        renderStatus(await api("/admin/api/sync/status", {method: "GET"}));
      } catch {}
    }
    async function pollBaiduAuth(popup = null) {
      if (baiduAuthPoll) clearInterval(baiduAuthPoll);
      let attempts = 0;
      baiduAuthPoll = setInterval(async () => {
        attempts += 1;
        try {
          const settings = await api("/admin/api/settings", {method: "GET"});
          renderSystem(settings);
          if (settings.baidu_auth && settings.baidu_auth.has_token) {
            clearInterval(baiduAuthPoll);
            baiduAuthPoll = null;
            $("authMessage").textContent = "授权已更新";
            try { await loadNetdisk($("netdiskPath").value, 1); } catch {}
          } else if (attempts >= 150 || (popup && popup.closed)) {
            clearInterval(baiduAuthPoll);
            baiduAuthPoll = null;
            $("authMessage").textContent = attempts >= 150 ? "授权等待超时" : "授权窗口已关闭";
          }
        } catch {}
      }, 2000);
    }
    async function startBaiduAuthorization() {
      const button = $("authorizeBaiduBtn");
      button.disabled = true;
      $("authMessage").textContent = "正在打开百度授权窗口";
      try {
        const data = await api("/admin/api/baidu/oauth/start", {method: "POST", body: "{}"});
        const popup = window.open(data.authorize_url, "baiduOAuth", "width=760,height=780");
        if (!popup) {
          $("authMessage").innerHTML = `弹窗被阻止，<a href="${escapeHtml(data.authorize_url)}" target="_blank">打开授权页面</a>`;
          pollBaiduAuth();
          return;
        }
        $("authMessage").textContent = "等待百度授权完成";
        pollBaiduAuth(popup);
      } catch (err) {
        $("authMessage").textContent = err.message;
      } finally {
        button.disabled = false;
      }
    }
    window.addEventListener("message", async (event) => {
      if (event.origin !== window.location.origin) return;
      if (!event.data || event.data.type !== "baidu-oauth") return;
      if (baiduAuthPoll) {
        clearInterval(baiduAuthPoll);
        baiduAuthPoll = null;
      }
      $("authMessage").textContent = event.data.status === "success" ? "授权已完成" : "授权失败";
      await refreshAll();
    });
    $("refreshBtn").addEventListener("click", refreshAll);
    $("authorizeBaiduBtn").addEventListener("click", startBaiduAuthorization);
    $("loadNetdiskBtn").addEventListener("click", async () => {
      try {
        await loadNetdisk($("netdiskPath").value, 1);
      } catch (err) {
        $("netdiskMessage").textContent = err.message;
      }
    });
    $("parentDirBtn").addEventListener("click", async () => {
      try {
        await loadNetdisk(parentPath($("netdiskPath").value), 1);
      } catch (err) {
        $("netdiskMessage").textContent = err.message;
      }
    });
    $("prevPageBtn").addEventListener("click", async () => {
      if (netdiskPage <= 1) return;
      try {
        await loadNetdisk($("netdiskPath").value, netdiskPage - 1);
      } catch (err) {
        $("netdiskMessage").textContent = err.message;
      }
    });
    $("nextPageBtn").addEventListener("click", async () => {
      if (!netdiskHasMore) return;
      try {
        await loadNetdisk($("netdiskPath").value, netdiskPage + 1);
      } catch (err) {
        $("netdiskMessage").textContent = err.message;
      }
    });
    $("netdiskLimit").addEventListener("change", async () => {
      localStorage.setItem("stockDataNetdiskLimit", $("netdiskLimit").value);
      try {
        await loadNetdisk($("netdiskPath").value, 1);
      } catch (err) {
        $("netdiskMessage").textContent = err.message;
      }
    });
    $("netdiskBody").addEventListener("click", async (event) => {
      const button = event.target.closest("button[data-action]");
      if (!button) return;
      const path = button.dataset.path;
      try {
        if (button.dataset.action === "open") {
          await loadNetdisk(path, 1);
        } else if (button.dataset.action === "sync") {
          await syncFile(path);
        }
      } catch (err) {
        $("netdiskMessage").textContent = err.message;
      }
    });
    $("saveSettingsBtn").addEventListener("click", async () => {
      try {
        const result = await api("/admin/api/settings", {method: "POST", body: JSON.stringify(currentPayload())});
        setForm(result.sync_defaults);
        $("settingsMessage").textContent = "已保存";
      } catch (err) {
        $("settingsMessage").textContent = err.message;
      }
    });
    $("startBtn").addEventListener("click", async () => {
      try {
        await api("/admin/api/sync/start", {method: "POST", body: JSON.stringify(currentPayload())});
        await refreshStatusOnly();
      } catch (err) {
        $("settingsMessage").textContent = err.message;
      }
    });
    $("fullSyncBtn").addEventListener("click", async () => {
      try {
        $("settingsMessage").textContent = "Loading listed symbols...";
        const data = await api("/admin/api/symbols?status=listed&limit=100000", {method: "GET"});
        const symbols = (data.symbols || []).map((item) => item.symbol).filter(Boolean);
        if (!symbols.length) throw new Error("No listed symbols. Run refresh-symbols first.");
        $("symbols").value = symbols.join(String.fromCharCode(10));
        $("settingsMessage").textContent = `Filled ${symbols.length} listed symbols. Click sync selected symbols to start.`;
      } catch (err) {
        $("settingsMessage").textContent = err.message;
      }
    });
    $("stopBtn").addEventListener("click", async () => {
      try {
        await api("/admin/api/sync/stop", {method: "POST", body: "{}"});
        await refreshStatusOnly();
      } catch (err) {
        $("settingsMessage").textContent = err.message;
      }
    });
    refreshAll();
    setInterval(refreshStatusOnly, 3000);
  </script>
</body>
</html>
"""


_ADMIN_CALENDAR_HTML = """<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>数据日历 - Stock Data Service</title>
  <style>
    :root {
      color-scheme: light;
      --bg: #f6f7f8;
      --panel: #ffffff;
      --ink: #1f2933;
      --muted: #667085;
      --line: #d8dde3;
      --accent: #16794c;
      --accent-ink: #ffffff;
      --danger: #b42318;
      --ok: #067647;
      --calendar-bg: #101922;
      --calendar-data: #22c55e;
      --calendar-missing: rgba(34, 197, 94, .55);
      --calendar-non-trading: #101922;
      --calendar-zoom: .83;
      --day-size: calc(14px * var(--calendar-zoom));
      --calendar-gap: calc(5px * var(--calendar-zoom));
      --month-min-width: calc(168px * var(--calendar-zoom));
      font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      background: var(--bg);
      color: var(--ink);
      font-size: 14px;
      line-height: 1.45;
    }
    header {
      border-bottom: 1px solid var(--line);
      background: var(--panel);
      padding: 14px 22px;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 16px;
      position: sticky;
      top: 0;
      z-index: 4;
    }
    h1 {
      margin: 0;
      font-size: 18px;
      font-weight: 650;
      letter-spacing: 0;
    }
    main {
      width: 100%;
      margin: 0 auto;
      padding: 18px 22px 28px;
      display: grid;
      gap: 16px;
    }
    .toolbar, section {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 8px;
    }
    .toolbar {
      padding: 12px;
      display: grid;
      grid-template-columns: minmax(150px, 1fr) minmax(130px, 160px) minmax(90px, 110px) repeat(2, minmax(110px, 130px)) repeat(2, auto);
      gap: 10px;
      align-items: end;
    }
    label {
      display: grid;
      gap: 6px;
      color: var(--muted);
      font-size: 12px;
      font-weight: 560;
    }
    input, select {
      width: 100%;
      min-height: 36px;
      border: 1px solid var(--line);
      border-radius: 6px;
      padding: 8px 10px;
      color: var(--ink);
      background: #fff;
      font: inherit;
      letter-spacing: 0;
    }
    button, .nav-button {
      min-height: 36px;
      border: 1px solid var(--line);
      border-radius: 6px;
      padding: 0 12px;
      background: #fff;
      color: var(--ink);
      font: inherit;
      font-weight: 620;
      cursor: pointer;
      white-space: nowrap;
      display: inline-flex;
      align-items: center;
      justify-content: center;
      text-decoration: none;
    }
    button.primary {
      background: var(--accent);
      border-color: var(--accent);
      color: var(--accent-ink);
    }
    button:disabled {
      opacity: .55;
      cursor: not-allowed;
    }
    .pill {
      display: inline-flex;
      align-items: center;
      min-height: 24px;
      padding: 0 8px;
      border-radius: 999px;
      border: 1px solid var(--line);
      color: var(--muted);
      background: #fff;
      font-size: 12px;
      font-weight: 620;
    }
    .pill.ok { color: var(--ok); border-color: #abefc6; }
    .pill.bad { color: var(--danger); border-color: #f2b8b5; }
    .zoom-value {
      min-width: 56px;
      justify-content: center;
    }
    section > h2 {
      margin: 0;
      padding: 13px 14px;
      border-bottom: 1px solid var(--line);
      font-size: 14px;
      font-weight: 650;
      letter-spacing: 0;
    }
    .section-body { padding: 14px; }
    .status-row {
      display: grid;
      grid-template-columns: minmax(0, 1fr) 56px;
      gap: 10px;
      align-items: center;
      margin-bottom: 10px;
    }
    .progress {
      height: 10px;
      overflow: hidden;
      border: 1px solid var(--line);
      border-radius: 999px;
      background: #eef1f4;
    }
    .progress > div {
      width: 0%;
      height: 100%;
      background: var(--accent);
      transition: width .12s ease;
    }
    .message {
      min-height: 22px;
      margin: 0;
      color: var(--muted);
      font-size: 12px;
    }
    .calendar-stage-header {
      display: flex;
      justify-content: space-between;
      gap: 16px;
      align-items: center;
      margin: 0 0 14px;
      padding: 0 2px;
    }
    .legend {
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
      align-items: center;
      margin: 0;
      padding: 0;
    }
    .legend-item {
      display: inline-flex;
      gap: 6px;
      align-items: center;
      color: #b7c4d1;
      font-size: 12px;
      font-weight: 620;
    }
    .legend-swatch {
      width: 13px;
      height: 13px;
      border: 1px solid rgba(148, 163, 184, .34);
      border-radius: 3px;
      background: transparent;
    }
    .calendar-zoom-controls {
      display: inline-flex;
      flex: 0 0 auto;
      gap: 6px;
      align-items: center;
      justify-content: flex-end;
    }
    .calendar-zoom-controls button {
      width: 30px;
      min-width: 30px;
      min-height: 28px;
      padding: 0;
      border-color: rgba(148, 163, 184, .36);
      background: rgba(255, 255, 255, .04);
      color: #dce6ef;
    }
    .calendar-zoom-value {
      display: inline-flex;
      align-items: center;
      gap: 3px;
      min-width: 58px;
      min-height: 28px;
      padding: 0 8px;
      border: 1px solid rgba(148, 163, 184, .28);
      border-radius: 6px;
      background: rgba(255, 255, 255, .03);
      color: #dce6ef;
    }
    .calendar-zoom-value input {
      width: 34px;
      min-height: 24px;
      border: 0;
      padding: 0;
      background: transparent;
      color: #dce6ef;
      font: inherit;
      font-size: 12px;
      font-weight: 700;
      text-align: right;
      -moz-appearance: textfield;
    }
    .calendar-zoom-value input:focus {
      outline: none;
    }
    .calendar-zoom-value input::-webkit-outer-spin-button,
    .calendar-zoom-value input::-webkit-inner-spin-button {
      margin: 0;
      -webkit-appearance: none;
    }
    .calendar-zoom-unit {
      color: #aab7c4;
      font-size: 12px;
      font-weight: 700;
    }
    .calendar-stage {
      min-height: 100vh;
      padding: 14px 18px 18px;
      background: var(--calendar-bg);
      overflow: visible;
      overflow-anchor: none;
      scroll-margin-top: 72px;
    }
    .coverage-calendar {
      min-height: 100%;
      position: relative;
      transform-origin: top left;
      overflow-anchor: none;
    }
    .calendar-rows {
      display: grid;
      gap: calc(12px * var(--calendar-zoom));
      overflow-anchor: none;
    }
    .calendar-spacer {
      height: 0;
      overflow-anchor: none;
    }
    .calendar-year {
      padding: calc(10px * var(--calendar-zoom)) calc(12px * var(--calendar-zoom));
      border: 1px solid rgba(148, 163, 184, .2);
      border-radius: 6px;
      background: var(--calendar-bg);
      color: #e8eef4;
      overflow-anchor: none;
    }
    .calendar-year-header {
      display: flex;
      justify-content: space-between;
      gap: 12px;
      align-items: center;
      margin-bottom: calc(8px * var(--calendar-zoom));
    }
    .calendar-year-title {
      font-size: calc(18px * var(--calendar-zoom));
      font-weight: 720;
      letter-spacing: 0;
    }
    .calendar-year-summary {
      color: #9fb0bf;
      font-size: calc(12px * var(--calendar-zoom));
      font-weight: 620;
    }
    .calendar-months {
      display: grid;
      grid-template-columns: repeat(auto-fill, minmax(var(--month-min-width), 1fr));
      gap: calc(10px * var(--calendar-zoom));
    }
    .calendar-month {
      min-width: 0;
      padding: calc(6px * var(--calendar-zoom));
      border: 0;
      border-radius: 4px;
      background: transparent;
    }
    .calendar-month-title {
      margin-bottom: calc(7px * var(--calendar-zoom));
      text-align: center;
      color: #dce6ef;
      font-size: calc(14px * var(--calendar-zoom));
      font-weight: 720;
    }
    .calendar-weekdays,
    .calendar-days {
      display: grid;
      grid-template-columns: repeat(7, var(--day-size));
      justify-content: center;
      gap: var(--calendar-gap);
    }
    .calendar-weekday {
      color: #8fa0af;
      text-align: center;
      font-size: max(9px, calc(11px * var(--calendar-zoom)));
      font-weight: 680;
    }
    .calendar-days {
      margin-top: calc(7px * var(--calendar-zoom));
    }
    .calendar-day {
      width: var(--day-size);
      height: var(--day-size);
      border: 1px solid transparent;
      border-radius: 2px;
      background: var(--calendar-bg);
    }
    .calendar-day[data-status="has_data"] {
      border-color: var(--calendar-data);
    }
    .calendar-day[data-status="missing"] {
      border-color: var(--calendar-missing);
    }
    .calendar-day[data-status="non_trading"] {
      border-color: var(--calendar-bg);
    }
    .calendar-day.blank {
      visibility: hidden;
    }
    .calendar-empty {
      padding: 18px;
      border: 1px dashed #415160;
      border-radius: 8px;
      color: #9fb0bf;
      text-align: center;
    }
    body.calendar-mode-compact {
      --month-min-width: calc(132px * var(--calendar-zoom));
      --calendar-gap: calc(4px * var(--calendar-zoom));
    }
    body.calendar-mode-compact .calendar-year {
      padding: calc(8px * var(--calendar-zoom)) calc(10px * var(--calendar-zoom));
    }
    body.calendar-mode-compact .calendar-month {
      padding: calc(5px * var(--calendar-zoom));
    }
    body.calendar-mode-compact .calendar-year-header {
      margin-bottom: calc(8px * var(--calendar-zoom));
    }
    body.calendar-mode-dense {
      --day-size: max(5px, calc(14px * var(--calendar-zoom)));
      --calendar-gap: max(1px, calc(3px * var(--calendar-zoom)));
    }
    body.calendar-mode-dense .calendar-rows {
      gap: 10px;
    }
    body.calendar-mode-dense .calendar-year {
      display: grid;
      grid-template-columns: 96px minmax(0, 1fr);
      align-items: start;
      gap: 10px;
      padding: 6px 8px;
      border-color: rgba(148, 163, 184, .18);
      border-radius: 6px;
      background: var(--calendar-bg);
    }
    body.calendar-mode-dense .calendar-year-header {
      display: grid;
      gap: 2px;
      align-content: start;
      margin: 0;
    }
    body.calendar-mode-dense .calendar-year-title {
      font-size: 14px;
      line-height: 1.1;
    }
    body.calendar-mode-dense .calendar-year-summary {
      font-size: 10px;
      line-height: 1.35;
    }
    body.calendar-mode-dense .calendar-months {
      grid-template-columns: repeat(12, max-content);
      justify-content: start;
      align-items: start;
      gap: 8px;
      overflow: visible;
    }
    body.calendar-mode-dense .calendar-month {
      padding: 0;
      border: 0;
      border-radius: 0;
      background: transparent;
    }
    body.calendar-mode-dense .calendar-month-title {
      margin-bottom: 4px;
      color: #d7e1ea;
      text-align: left;
      font-size: 10px;
      line-height: 1;
    }
    body.calendar-mode-dense .calendar-weekdays {
      display: none;
    }
    body.calendar-mode-dense .calendar-days {
      justify-content: start;
      margin-top: 0;
    }
    body.calendar-mode-dense .calendar-day {
      border: 1px solid transparent;
      border-radius: 2px;
    }
    body.calendar-mode-dense .calendar-day[data-status="non_trading"] {
      border-color: var(--calendar-bg);
    }
    @media (max-width: 980px) {
      header, main { padding-left: 14px; padding-right: 14px; }
      .toolbar { grid-template-columns: 1fr 1fr; }
      .nav-button, button { width: 100%; }
    }
    @media (max-width: 640px) {
      .toolbar { grid-template-columns: 1fr; }
      .calendar-stage { padding: 10px; }
      .calendar-stage-header { align-items: flex-start; flex-direction: column; }
    }
  </style>
</head>
<body>
  <header>
    <h1>数据日历</h1>
    <span id="topStatus" class="pill">读取中</span>
  </header>
  <main>
    <form class="toolbar" onsubmit="return false">
      <label>Admin Key
        <input id="adminKey" type="password" autocomplete="off" placeholder="server mode">
      </label>
      <label>股票代码
        <input id="calendarSymbol" spellcheck="false" placeholder="sh600000">
      </label>
      <label>周期
        <select id="calendarTimeframe">
          <option value="1m">1m</option>
          <option value="5m">5m</option>
          <option value="15m">15m</option>
          <option value="30m">30m</option>
          <option value="60m">60m</option>
          <option value="1d">1d</option>
        </select>
      </label>
      <label>开始年
        <input id="calendarStartYear" type="number" min="1990" max="2100" step="1">
      </label>
      <label>结束年
        <input id="calendarEndYear" type="number" min="1990" max="2100" step="1">
      </label>
      <button id="loadCalendarBtn" class="primary" type="button">刷新</button>
      <a class="nav-button" href="/admin">管理台</a>
    </form>
    <section>
      <h2>覆盖情况</h2>
      <div class="section-body">
        <div class="status-row">
          <div class="progress"><div id="calendarProgressBar"></div></div>
          <span id="calendarProgressText" class="pill zoom-value">0%</span>
        </div>
        <p id="calendarMessage" class="message"></p>
      </div>
    </section>
    <div id="calendarStage" class="calendar-stage">
      <div class="calendar-stage-header">
        <div class="calendar-legend legend">
          <span class="legend-item"><span id="legendHasData" class="legend-swatch"></span>有数据</span>
          <span class="legend-item"><span id="legendMissing" class="legend-swatch"></span>交易日无数据</span>
          <span class="legend-item"><span id="legendNonTrading" class="legend-swatch"></span>非交易日</span>
          <span class="legend-item"><span id="legendBackground" class="legend-swatch"></span>背景</span>
        </div>
        <div class="calendar-zoom-controls" aria-label="缩放控制">
          <button id="zoomOutBtn" type="button">-</button>
          <span class="calendar-zoom-value">
            <input id="zoomInput" type="number" min="83" max="225" step="1" inputmode="numeric" aria-label="缩放百分比">
            <span class="calendar-zoom-unit">%</span>
          </span>
          <button id="zoomInBtn" type="button">+</button>
        </div>
      </div>
      <div id="coverageCalendar" class="coverage-calendar"></div>
    </div>
  </main>
  <script>
    const $ = (id) => document.getElementById(id);
    const apiKeyInput = $("adminKey");
    let renderToken = 0;
    let calendarZoom = Number(localStorage.getItem("stockCoverageCalendarZoom") || "0.83");
    let calendarView = null;
    let virtualFrame = null;
    const VIRTUAL_BUFFER_YEARS = 4;

    const CALENDAR_COLOR_BACKGROUND = "#101922";
    const CALENDAR_COLOR_HAS_DATA = "#22c55e";
    const CALENDAR_COLOR_MISSING_TRADING_DAY = "rgba(34, 197, 94, .55)";
    const CALENDAR_COLOR_NON_TRADING_DAY = CALENDAR_COLOR_BACKGROUND;
    const CALENDAR_STATUS_LABELS = {
      has_data: "有数据",
      missing: "交易日无数据",
      non_trading: "非交易日"
    };

    apiKeyInput.value = localStorage.getItem("stockDataAdminKey") || "";
    apiKeyInput.addEventListener("input", () => localStorage.setItem("stockDataAdminKey", apiKeyInput.value));

    function escapeHtml(value) {
      return String(value ?? "").replace(/[&<>"']/g, (char) => ({
        "&": "&amp;",
        "<": "&lt;",
        ">": "&gt;",
        '"': "&quot;",
        "'": "&#39;"
      }[char]));
    }
    function headers() {
      const key = apiKeyInput.value.trim();
      return key ? {"Content-Type": "application/json", "X-API-Key": key} : {"Content-Type": "application/json"};
    }
    async function api(path, options = {}) {
      const response = await fetch(path, {...options, headers: {...headers(), ...(options.headers || {})}});
      if (!response.ok) {
        let message = response.statusText;
        try { message = (await response.json()).detail || message; } catch {}
        throw new Error(message);
      }
      return await response.json();
    }
    function applyCalendarColors() {
      document.documentElement.style.setProperty("--calendar-bg", CALENDAR_COLOR_BACKGROUND);
      document.documentElement.style.setProperty("--calendar-data", CALENDAR_COLOR_HAS_DATA);
      document.documentElement.style.setProperty("--calendar-missing", CALENDAR_COLOR_MISSING_TRADING_DAY);
      document.documentElement.style.setProperty("--calendar-non-trading", CALENDAR_COLOR_NON_TRADING_DAY);
      $("legendHasData").style.backgroundColor = CALENDAR_COLOR_HAS_DATA;
      $("legendHasData").style.borderColor = CALENDAR_COLOR_HAS_DATA;
      $("legendMissing").style.backgroundColor = CALENDAR_COLOR_BACKGROUND;
      $("legendMissing").style.borderColor = CALENDAR_COLOR_MISSING_TRADING_DAY;
      $("legendNonTrading").style.backgroundColor = CALENDAR_COLOR_NON_TRADING_DAY;
      $("legendNonTrading").style.borderColor = CALENDAR_COLOR_BACKGROUND;
      $("legendBackground").style.backgroundColor = CALENDAR_COLOR_BACKGROUND;
    }
    function calendarColor(status) {
      if (status === "has_data") return CALENDAR_COLOR_HAS_DATA;
      if (status === "missing") return CALENDAR_COLOR_BACKGROUND;
      return CALENDAR_COLOR_NON_TRADING_DAY;
    }
    function clamp(value, min, max) {
      return Math.max(min, Math.min(max, value));
    }
    function setZoom(value) {
      calendarZoom = clamp(Number(value) || 0.83, 0.83, 2.25);
      document.documentElement.style.setProperty("--calendar-zoom", calendarZoom.toFixed(2));
      $("zoomInput").value = String(Math.round(calendarZoom * 100));
      localStorage.setItem("stockCoverageCalendarZoom", String(calendarZoom));
      setZoomMode(calendarZoom);
      if (calendarView) {
        calendarView.rowHeight = estimateYearHeight();
        scheduleVirtualRender(true);
      }
    }
    function applyZoomInput() {
      const percent = Number($("zoomInput").value);
      if (!Number.isFinite(percent)) {
        $("zoomInput").value = String(Math.round(calendarZoom * 100));
        return;
      }
      setZoom(percent / 100);
    }
    function zoomMode(value) {
      if (value < 0.65) return "dense";
      if (value < 0.9) return "compact";
      return "full";
    }
    function setZoomMode(value) {
      const mode = zoomMode(value);
      document.body.classList.toggle("calendar-mode-full", mode === "full");
      document.body.classList.toggle("calendar-mode-compact", mode === "compact");
      document.body.classList.toggle("calendar-mode-dense", mode === "dense");
    }
    function setProgress(percent, message) {
      const safe = clamp(Math.round(percent), 0, 100);
      $("calendarProgressBar").style.width = `${safe}%`;
      $("calendarProgressText").textContent = `${safe}%`;
      if (message !== undefined) {
        $("calendarMessage").textContent = message;
      }
    }
    function calendarRequest() {
      const currentYear = new Date().getFullYear();
      let startYear = Number($("calendarStartYear").value || currentYear);
      let endYear = Number($("calendarEndYear").value || startYear);
      if (!Number.isFinite(startYear)) startYear = currentYear;
      if (!Number.isFinite(endYear)) endYear = startYear;
      if (startYear > endYear) {
        const nextStart = endYear;
        endYear = startYear;
        startYear = nextStart;
      }
      $("calendarStartYear").value = startYear;
      $("calendarEndYear").value = endYear;
      const symbol = $("calendarSymbol").value.trim() || "sh600000";
      $("calendarSymbol").value = symbol;
      return {
        symbol,
        timeframe: $("calendarTimeframe").value || "1m",
        start: `${startYear}-01-01`,
        end: `${endYear}-12-31`
      };
    }
    function dayKey(year, month, day) {
      return `${year}-${String(month).padStart(2, "0")}-${String(day).padStart(2, "0")}`;
    }
    function daysInMonth(year, month) {
      return new Date(year, month, 0).getDate();
    }
    function mondayFirstWeekday(year, month, day) {
      return (new Date(year, month - 1, day).getDay() + 6) % 7;
    }
    function nextFrame() {
      return new Promise((resolve) => requestAnimationFrame(() => resolve()));
    }
    function groupDays(days) {
      const byDate = new Map();
      const byYear = new Map();
      for (const item of days) {
        byDate.set(item.date, item);
        const year = Number(item.date.slice(0, 4));
        if (!byYear.has(year)) byYear.set(year, []);
        byYear.get(year).push(item);
      }
      return {byDate, byYear};
    }
    function renderYear(year, byDate, byYear) {
      const yearDays = byYear.get(year) || [];
      const hasData = yearDays.filter((item) => item.status === "has_data").length;
      const missing = yearDays.filter((item) => item.status === "missing").length;
      const months = [];
      for (let month = 1; month <= 12; month += 1) {
        const blanks = Array.from({length: mondayFirstWeekday(year, month, 1)}, () => `<span class="calendar-day blank"></span>`).join("");
        const cells = [];
        for (let day = 1; day <= daysInMonth(year, month); day += 1) {
          const key = dayKey(year, month, day);
          const item = byDate.get(key) || {status: "non_trading", row_count: 0};
          const detail = item.row_count ? ` · ${item.row_count} 行` : "";
          const title = `${key} ${CALENDAR_STATUS_LABELS[item.status] || item.status}${detail}`;
          cells.push(`<span class="calendar-day" data-status="${escapeHtml(item.status)}" title="${escapeHtml(title)}" style="background-color:${calendarColor(item.status)}"></span>`);
        }
        months.push(`
          <div class="calendar-month">
            <div class="calendar-month-title">${month}月</div>
            <div class="calendar-weekdays">
              <span class="calendar-weekday">一</span>
              <span class="calendar-weekday">二</span>
              <span class="calendar-weekday">三</span>
              <span class="calendar-weekday">四</span>
              <span class="calendar-weekday">五</span>
              <span class="calendar-weekday">六</span>
              <span class="calendar-weekday">日</span>
            </div>
            <div class="calendar-days">${blanks}${cells.join("")}</div>
          </div>`);
      }
      return `
        <article class="calendar-year">
          <div class="calendar-year-header">
            <div class="calendar-year-title">${year} 年</div>
            <div class="calendar-year-summary">有数据 ${hasData} 天 · 缺失 ${missing} 天</div>
          </div>
          <div class="calendar-months">${months.join("")}</div>
        </article>`;
    }
    function createVirtualShell() {
      $("coverageCalendar").innerHTML = `
        <div id="calendarTopSpacer" class="calendar-spacer"></div>
        <div id="calendarRows" class="calendar-rows"></div>
        <div id="calendarBottomSpacer" class="calendar-spacer"></div>`;
    }
    function stageMetric(name, fallback) {
      const value = $("calendarStage")[name];
      return Number.isFinite(value) && value > 0 ? value : fallback;
    }
    function viewportHeight() {
      if (typeof window !== "undefined" && Number.isFinite(window.innerHeight)) return window.innerHeight;
      return stageMetric("clientHeight", 700);
    }
    function pageScrollY() {
      if (typeof window !== "undefined" && Number.isFinite(window.scrollY)) return window.scrollY;
      if (typeof document !== "undefined" && document.documentElement) return document.documentElement.scrollTop || 0;
      return 0;
    }
    function stagePageTop() {
      const stage = $("calendarStage");
      if (stage && typeof stage.getBoundingClientRect === "function") {
        const rect = stage.getBoundingClientRect();
        return (Number.isFinite(rect.top) ? rect.top : 0) + pageScrollY();
      }
      return 0;
    }
    function calendarScrollOffset() {
      return Math.max(0, pageScrollY() - stagePageTop());
    }
    function estimateYearHeight() {
      const width = Math.max(320, stageMetric("clientWidth", 1200) - 28);
      const mode = zoomMode(calendarZoom);
      if (mode === "dense") {
        return Math.max(52, Math.round(116 * calendarZoom));
      }
      const day = 14 * calendarZoom;
      const gap = (mode === "compact" ? 4 : 5) * calendarZoom;
      const monthMin = (mode === "compact" ? 132 : 168) * calendarZoom;
      const monthGap = 12 * calendarZoom;
      const columns = clamp(Math.floor((width + monthGap) / Math.max(1, monthMin + monthGap)), 1, 12);
      const monthRows = Math.ceil(12 / columns);
      const monthHeight = (mode === "compact" ? 16 : 20) * calendarZoom + (6 * day) + (7 * gap);
      const headerHeight = (mode === "compact" ? 28 : 38) * calendarZoom;
      return Math.max(90, Math.round(headerHeight + (monthRows * monthHeight) + ((monthRows - 1) * monthGap) + (28 * calendarZoom)));
    }
    function scheduleVirtualRender(force = false) {
      if (virtualFrame) return;
      virtualFrame = requestAnimationFrame(() => {
        virtualFrame = null;
        renderVirtualWindow(force);
      });
    }
    function renderVirtualWindow(force = false) {
      if (!calendarView) return;
      const years = calendarView.years;
      if (!years.length) return;
      const rowHeight = Math.max(1, calendarView.rowHeight || estimateYearHeight());
      const visibleCount = Math.ceil(viewportHeight() / rowHeight) + (VIRTUAL_BUFFER_YEARS * 2);
      const first = clamp(Math.floor(calendarScrollOffset() / rowHeight) - VIRTUAL_BUFFER_YEARS, 0, years.length - 1);
      const last = clamp(first + visibleCount - 1, first, years.length - 1);
      if (!force && first === calendarView.first && last === calendarView.last) return;

      calendarView.first = first;
      calendarView.last = last;
      $("calendarTopSpacer").style.height = `${Math.round(first * rowHeight)}px`;
      $("calendarBottomSpacer").style.height = `${Math.round((years.length - last - 1) * rowHeight)}px`;
      $("calendarRows").innerHTML = years
        .slice(first, last + 1)
        .map((year) => renderYear(year, calendarView.byDate, calendarView.byYear))
        .join("");
    }
    async function renderCoverageCalendar(data, token) {
      const days = data.days || [];
      const target = $("coverageCalendar");
      target.innerHTML = "";
      if (!days.length) {
        target.innerHTML = `<div class="calendar-empty">没有可显示的日期</div>`;
        setProgress(100, "");
        return;
      }
      const {byDate, byYear} = groupDays(days);
      const startYear = Number(data.start.slice(0, 4));
      const endYear = Number(data.end.slice(0, 4));
      const years = [];
      for (let year = startYear; year <= endYear; year += 1) {
        years.push(year);
      }
      setProgress(45, `准备 ${years.length} 年视图`);
      await nextFrame();
      if (token !== renderToken) return;
      createVirtualShell();
      calendarView = {
        data,
        byDate,
        byYear,
        years,
        rowHeight: estimateYearHeight(),
        first: -1,
        last: -1
      };
      const stage = $("calendarStage");
      if (stage && typeof stage.scrollIntoView === "function") {
        stage.scrollIntoView({block: "start"});
      }
      renderVirtualWindow(true);
      setProgress(85, `已渲染可见年份，滚动时继续加载`);
      await nextFrame();
      const counts = data.counts || {};
      setProgress(100, `${data.symbol} · ${data.timeframe} · 有数据 ${counts.has_data ?? 0} 天 · 交易日缺失 ${counts.missing ?? 0} 天`);
    }
    async function loadCoverageCalendar() {
      const token = ++renderToken;
      $("loadCalendarBtn").disabled = true;
      calendarView = null;
      virtualFrame = null;
      $("coverageCalendar").innerHTML = "";
      setProgress(3, "请求数据");
      try {
        const request = calendarRequest();
        const query = new URLSearchParams(request);
        const data = await api(`/admin/api/coverage/calendar?${query.toString()}`, {method: "GET"});
        if (token !== renderToken) return;
        setProgress(20, `收到 ${data.days.length} 天，开始渲染`);
        await renderCoverageCalendar(data, token);
        $("topStatus").className = "pill ok";
        $("topStatus").textContent = "已连接";
      } catch (err) {
        if (token === renderToken) {
          $("topStatus").className = "pill bad";
          $("topStatus").textContent = "加载失败";
          setProgress(0, err.message);
        }
      } finally {
        if (token === renderToken) {
          $("loadCalendarBtn").disabled = false;
        }
      }
    }
    async function loadDefaults() {
      try {
        const settings = await api("/admin/api/settings", {method: "GET"});
        const sync = settings.sync_defaults || {};
        const year = new Date().getFullYear();
        const startYear = Number(String(sync.start || "").slice(0, 4)) || year;
        const endYear = Number(String(sync.end || "").slice(0, 4)) || startYear;
        $("calendarSymbol").value = (sync.symbols || [])[0] || "sh600000";
        $("calendarTimeframe").value = sync.timeframe || "1m";
        $("calendarStartYear").value = startYear;
        $("calendarEndYear").value = endYear;
        await loadCoverageCalendar();
      } catch (err) {
        $("topStatus").className = "pill bad";
        $("topStatus").textContent = err.message;
        setProgress(0, err.message);
      }
    }
    $("loadCalendarBtn").addEventListener("click", loadCoverageCalendar);
    $("zoomOutBtn").addEventListener("click", () => setZoom(calendarZoom * 0.9));
    $("zoomInBtn").addEventListener("click", () => setZoom(calendarZoom * 1.1));
    $("zoomInput").addEventListener("change", applyZoomInput);
    $("zoomInput").addEventListener("keydown", (event) => {
      if (event.key === "Enter") {
        event.preventDefault();
        applyZoomInput();
        $("zoomInput").blur();
      }
      if (event.key === "Escape") {
        $("zoomInput").value = String(Math.round(calendarZoom * 100));
        $("zoomInput").blur();
      }
    });
    if (typeof window !== "undefined") {
      window.addEventListener("scroll", () => scheduleVirtualRender(false), {passive: true});
      window.addEventListener("resize", () => {
        if (!calendarView) return;
        calendarView.rowHeight = estimateYearHeight();
        calendarView.first = -1;
        calendarView.last = -1;
        scheduleVirtualRender(true);
      });
    }
    applyCalendarColors();
    setZoom(calendarZoom);
    loadDefaults();
  </script>
</body>
</html>
"""
