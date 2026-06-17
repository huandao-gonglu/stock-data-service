from __future__ import annotations

from enum import StrEnum


class Timeframe(StrEnum):
    M1 = "1m"
    M5 = "5m"
    M15 = "15m"
    M30 = "30m"
    H1 = "60m"
    D1 = "1d"

    @property
    def dataset_name(self) -> str:
        return {
            Timeframe.M1: "bars_1m",
            Timeframe.M5: "bars_5m",
            Timeframe.M15: "bars_15m",
            Timeframe.M30: "bars_30m",
            Timeframe.H1: "bars_60m",
            Timeframe.D1: "bars_1d",
        }[self]

    @property
    def minute_span(self) -> int | None:
        return {
            Timeframe.M1: 1,
            Timeframe.M5: 5,
            Timeframe.M15: 15,
            Timeframe.M30: 30,
            Timeframe.H1: 60,
            Timeframe.D1: None,
        }[self]

    @property
    def max_range_days(self) -> int:
        return {
            Timeframe.M1: 31,
            Timeframe.M5: 180,
            Timeframe.M15: 365,
            Timeframe.M30: 365,
            Timeframe.H1: 365,
            Timeframe.D1: 3660,
        }[self]

    @classmethod
    def parse(cls, value: str) -> "Timeframe":
        normalized = value.strip().lower()
        aliases = {
            "m1": cls.M1,
            "1min": cls.M1,
            "m5": cls.M5,
            "5min": cls.M5,
            "m15": cls.M15,
            "15min": cls.M15,
            "m30": cls.M30,
            "30min": cls.M30,
            "h1": cls.H1,
            "1h": cls.H1,
            "60min": cls.H1,
            "d1": cls.D1,
            "day": cls.D1,
            "daily": cls.D1,
        }
        if normalized in cls._value2member_map_:
            return cls(normalized)
        if normalized in aliases:
            return aliases[normalized]
        raise ValueError(f"unsupported timeframe: {value}")


def expected_intraday_rows(timeframe: Timeframe) -> int:
    if timeframe == Timeframe.D1:
        return 1
    minutes = timeframe.minute_span
    if minutes is None:
        return 1
    return 240 // minutes
