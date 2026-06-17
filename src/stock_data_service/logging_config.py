from __future__ import annotations

import datetime as dt
import logging
import re
import sys
from pathlib import Path
from typing import Callable

from stock_data_service.config import Settings

_MANAGED_ATTR = "_stock_data_service_managed"
_CURRENT_LOG_DIR_ATTR = "_stock_data_service_log_dir"


class DailyFileHandler(logging.Handler):
    """File handler that writes to logs named YYYY-MM-DD.log."""

    def __init__(
        self,
        log_dir: str | Path,
        *,
        date_provider: Callable[[], dt.date] | None = None,
        encoding: str = "utf-8",
    ):
        super().__init__()
        self.log_dir = Path(log_dir)
        self.date_provider = date_provider or dt.date.today
        self.encoding = encoding
        self._current_date: dt.date | None = None
        self._stream = None

    @property
    def current_log_path(self) -> Path:
        today = self.date_provider()
        return self.log_dir / f"{today.isoformat()}.log"

    def emit(self, record: logging.LogRecord) -> None:
        try:
            self._ensure_stream()
            msg = self.format(record)
            self._stream.write(msg + self.terminator)
            self.flush()
        except Exception:
            self.handleError(record)

    @property
    def terminator(self) -> str:
        return "\n"

    def flush(self) -> None:
        if self._stream and not self._stream.closed:
            self._stream.flush()

    def close(self) -> None:
        try:
            if self._stream and not self._stream.closed:
                self._stream.close()
        finally:
            self._stream = None
            super().close()

    def _ensure_stream(self) -> None:
        today = self.date_provider()
        if self._stream and self._current_date == today:
            return
        if self._stream and not self._stream.closed:
            self._stream.close()
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self._current_date = today
        self._stream = (self.log_dir / f"{today.isoformat()}.log").open("a", encoding=self.encoding)


class RedactingFormatter(logging.Formatter):
    _SECRET_PATTERNS = [
        re.compile(r"(?i)(access_token|refresh_token|app_secret|client_secret|api_key)(=|:)\s*([^,\s]+)"),
        re.compile(r"(?i)(Bearer)\s+([A-Za-z0-9._\-]+)"),
    ]

    def format(self, record: logging.LogRecord) -> str:
        text = super().format(record)
        for pattern in self._SECRET_PATTERNS:
            text = pattern.sub(_redact_match, text)
        return text


def configure_logging(settings: Settings) -> Path:
    log_dir = settings.logs_dir
    log_dir.mkdir(parents=True, exist_ok=True)
    level = getattr(logging, settings.log_level.upper(), logging.INFO)

    root_logger = logging.getLogger()
    current_dir = getattr(root_logger, _CURRENT_LOG_DIR_ATTR, None)
    if current_dir == str(log_dir):
        root_logger.setLevel(level)
        return log_dir / f"{dt.date.today().isoformat()}.log"

    for handler in list(root_logger.handlers):
        if getattr(handler, _MANAGED_ATTR, False):
            root_logger.removeHandler(handler)
            handler.close()

    formatter = RedactingFormatter(
        "%(asctime)s %(levelname)s [%(name)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    file_handler = DailyFileHandler(log_dir)
    file_handler.setFormatter(formatter)
    file_handler.setLevel(level)
    setattr(file_handler, _MANAGED_ATTR, True)

    console_handler = logging.StreamHandler(sys.stderr)
    console_handler.setFormatter(formatter)
    console_handler.setLevel(logging.WARNING)
    setattr(console_handler, _MANAGED_ATTR, True)

    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)
    root_logger.setLevel(level)
    setattr(root_logger, _CURRENT_LOG_DIR_ATTR, str(log_dir))

    logging.getLogger("stock_data_service").info("logging configured log_dir=%s level=%s", log_dir, settings.log_level)
    return file_handler.current_log_path


def _redact_match(match: re.Match) -> str:
    if match.group(1).lower() == "bearer":
        return f"{match.group(1)} <redacted>"
    return f"{match.group(1)}{match.group(2)}<redacted>"
