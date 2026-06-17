import datetime as dt
import logging

from stock_data_service.config import Settings, ensure_runtime_dirs
from stock_data_service.logging_config import DailyFileHandler, RedactingFormatter, configure_logging


class DateBox:
    def __init__(self, value: dt.date):
        self.value = value

    def today(self) -> dt.date:
        return self.value


def test_daily_file_handler_names_logs_by_year_month_day_and_rolls_over(tmp_path):
    dates = DateBox(dt.date(2026, 6, 15))
    logger = logging.getLogger("test.daily-file-handler")
    logger.handlers.clear()
    logger.propagate = False
    logger.setLevel(logging.INFO)
    handler = DailyFileHandler(tmp_path, date_provider=dates.today)
    handler.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(handler)

    logger.info("first")
    dates.value = dt.date(2026, 6, 16)
    logger.info("second")
    handler.close()

    assert (tmp_path / "2026-06-15.log").read_text(encoding="utf-8").strip() == "first"
    assert (tmp_path / "2026-06-16.log").read_text(encoding="utf-8").strip() == "second"


def test_configure_logging_writes_daily_file_and_redacts_secrets(tmp_path):
    settings = Settings(data_root=tmp_path / "data")
    ensure_runtime_dirs(settings)
    log_path = configure_logging(settings)

    logging.getLogger("stock_data_service.test").info(
        "credentials access_token=abc refresh_token:def Authorization=Bearer secret api_key=xyz"
    )
    for handler in logging.getLogger().handlers:
        handler.flush()

    text = log_path.read_text(encoding="utf-8")
    assert log_path.name == f"{dt.date.today().isoformat()}.log"
    assert "stock_data_service.test" in text
    assert "access_token=<redacted>" in text
    assert "refresh_token:<redacted>" in text
    assert "Bearer <redacted>" in text
    assert "api_key=<redacted>" in text
    assert "abc" not in text
    assert "secret" not in text


def test_redacting_formatter_handles_plain_messages():
    formatter = RedactingFormatter("%(message)s")
    record = logging.LogRecord("x", logging.INFO, __file__, 1, "hello", (), None)
    assert formatter.format(record) == "hello"
