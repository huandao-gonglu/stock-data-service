from __future__ import annotations

import datetime as dt

from stock_data_service.baidu.pan_client import BaiduPanClient
from stock_data_service.market.path_strategy import BaiduStockKPathStrategy
from stock_data_service.market.timeframe import Timeframe


class BaiduNetdiskSource:
    def __init__(self, client: BaiduPanClient, path_strategy: BaiduStockKPathStrategy | None = None):
        self.client = client
        self.path_strategy = path_strategy or BaiduStockKPathStrategy()

    def fetch_zip_candidates(self, *, timeframe: Timeframe, trade_date: dt.date) -> dict[str, bytes | None]:
        results: dict[str, bytes | None] = {}
        for candidate in self.path_strategy.candidates(timeframe, trade_date):
            results[candidate.remote_path] = self.client.download_content_by_path(candidate.remote_path)
        return results
