from __future__ import annotations

import datetime as dt
from abc import ABC, abstractmethod
from dataclasses import dataclass, field

from stock_data_service.market.calendar import SimpleTradingCalendar, TradingCalendar
from stock_data_service.market.timeframe import Timeframe


@dataclass(frozen=True)
class RemoteFileCandidate:
    remote_path: str
    trade_date: dt.date
    source_kind: str


class RemotePathStrategy(ABC):
    @abstractmethod
    def candidates(self, timeframe: Timeframe, trade_date: dt.date) -> list[RemoteFileCandidate]:
        ...

    def candidates_for_range(
        self,
        timeframe: Timeframe,
        start: dt.date,
        end: dt.date,
    ) -> list[RemoteFileCandidate]:
        raise NotImplementedError


@dataclass
class BaiduStockKPathStrategy(RemotePathStrategy):
    data_dir: str = "/A股_分时数据"
    calendar: TradingCalendar = field(default_factory=SimpleTradingCalendar)
    supported_start: dt.date = dt.date(2000, 6, 9)
    supported_end: dt.date | None = None

    def candidates(self, timeframe: Timeframe, trade_date: dt.date) -> list[RemoteFileCandidate]:
        folder = self._folder(timeframe)
        suffix = self._suffix(timeframe)
        ymd = trade_date.strftime("%Y%m%d")
        month = trade_date.strftime("%Y-%m")
        year = trade_date.strftime("%Y")
        return [
            RemoteFileCandidate(
                f"{self.data_dir}/{folder}/{ymd}{suffix}",
                trade_date,
                "daily",
            ),
            RemoteFileCandidate(
                f"{self.data_dir}/{folder}_按月归档/{month}/{ymd}{suffix}",
                trade_date,
                "monthly",
            ),
            RemoteFileCandidate(
                f"{self.data_dir}/{folder}_按年汇总/{year}{suffix}",
                trade_date,
                "annual",
            ),
        ]

    def candidates_for_range(
        self,
        timeframe: Timeframe,
        start: dt.date,
        end: dt.date,
    ) -> list[RemoteFileCandidate]:
        start, end = self._clamp_range(start, end)
        days = self.calendar.get_trading_days(start, end)
        results: list[RemoteFileCandidate] = []
        for day in days:
            results.extend(self.candidates(timeframe, day))
        return results

    def _clamp_range(self, start: dt.date, end: dt.date) -> tuple[dt.date, dt.date]:
        max_end = self.supported_end or dt.date.today()
        return max(start, self.supported_start), min(end, max_end)

    @staticmethod
    def _folder(timeframe: Timeframe) -> str:
        return {
            Timeframe.M1: "1分钟",
            Timeframe.M5: "5分钟",
            Timeframe.M15: "15分钟",
            Timeframe.M30: "30分钟",
            Timeframe.H1: "60分钟",
            Timeframe.D1: "1分钟",
        }[timeframe]

    @staticmethod
    def _suffix(timeframe: Timeframe) -> str:
        return {
            Timeframe.M1: "_1min.zip",
            Timeframe.M5: "_5min.zip",
            Timeframe.M15: "_15min.zip",
            Timeframe.M30: "_30min.zip",
            Timeframe.H1: "_60min.zip",
            Timeframe.D1: "_1min.zip",
        }[timeframe]
