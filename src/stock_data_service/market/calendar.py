from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field
from typing import Iterable, Protocol


class TradingCalendar(Protocol):
    def get_trading_days(self, start: dt.date, end: dt.date) -> list[dt.date]:
        ...

    def get_valid_range(self, start: dt.date, end: dt.date) -> tuple[dt.date | None, dt.date | None]:
        ...


@dataclass
class SimpleTradingCalendar:
    """Weekday-only trading calendar used by the service core and tests."""

    holidays: set[dt.date] = field(default_factory=set)

    def get_trading_days(self, start: dt.date, end: dt.date) -> list[dt.date]:
        if start > end:
            return []
        current = start
        days: list[dt.date] = []
        while current <= end:
            if current.weekday() < 5 and current not in self.holidays:
                days.append(current)
            current += dt.timedelta(days=1)
        return days

    def get_valid_range(self, start: dt.date, end: dt.date) -> tuple[dt.date | None, dt.date | None]:
        days = self.get_trading_days(start, end)
        if not days:
            return None, None
        return days[0], days[-1]


class SSETradingCalendar(SimpleTradingCalendar):
    """SSE calendar with a pandas-market-calendars fast path when installed."""

    def __init__(self) -> None:
        super().__init__()
        try:
            import pandas_market_calendars as mcal
        except Exception:
            self._calendar = None
        else:
            self._calendar = mcal.get_calendar("SSE")

    def get_trading_days(self, start: dt.date, end: dt.date) -> list[dt.date]:
        if self._calendar is None:
            return super().get_trading_days(start, end)
        if start > end:
            return []
        valid = self._calendar.valid_days(start_date=start, end_date=end)
        return [item.date() for item in valid]


def coerce_date(value: dt.date | dt.datetime | str) -> dt.date:
    if isinstance(value, dt.datetime):
        return value.date()
    if isinstance(value, dt.date):
        return value
    return dt.date.fromisoformat(value)


def natural_days(start: dt.date, end: dt.date) -> Iterable[dt.date]:
    current = start
    while current <= end:
        yield current
        current += dt.timedelta(days=1)
