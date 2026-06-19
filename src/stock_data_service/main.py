from __future__ import annotations

import logging
import time

from fastapi import FastAPI

from stock_data_service.api.routes import create_router
from stock_data_service.config import Settings, ensure_runtime_dirs
from stock_data_service.logging_config import configure_logging
from stock_data_service.storage.sync_metadata import SyncMetadata
from stock_data_service.sync.manager import SyncJobManager

logger = logging.getLogger(__name__)


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or Settings.from_env()
    ensure_runtime_dirs(settings)
    log_path = configure_logging(settings)
    metadata = SyncMetadata(settings.metadata_db)
    metadata.initialize()
    recovered_jobs = metadata.mark_unfinished_sync_jobs_stopped()
    if recovered_jobs:
        logger.warning("recovered stale unfinished sync jobs count=%s", recovered_jobs)

    app = FastAPI(title="Stock Data Service", version="0.1.0")
    app.state.settings = settings
    app.state.log_path = log_path
    app.state.sync_manager = SyncJobManager(settings)

    @app.middleware("http")
    async def request_logging(request, call_next):
        started = time.perf_counter()
        try:
            response = await call_next(request)
        except Exception:
            elapsed_ms = (time.perf_counter() - started) * 1000
            logger.exception(
                "request failed method=%s path=%s elapsed_ms=%.2f",
                request.method,
                request.url.path,
                elapsed_ms,
            )
            raise

        elapsed_ms = (time.perf_counter() - started) * 1000
        logger.info(
            "request completed method=%s path=%s status_code=%s elapsed_ms=%.2f",
            request.method,
            request.url.path,
            response.status_code,
            elapsed_ms,
        )
        return response

    app.include_router(create_router(settings))
    logger.info("app created data_root=%s metadata_db=%s log_path=%s", settings.data_root, settings.metadata_db, log_path)
    return app


app = create_app()
