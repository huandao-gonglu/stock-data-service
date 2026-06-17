import datetime as dt

from stock_data_service.market.calendar import SimpleTradingCalendar
from stock_data_service.market.path_strategy import BaiduStockKPathStrategy, RemotePathStrategy
from stock_data_service.market.timeframe import Timeframe


def test_remote_path_strategy_contract_exists():
    assert hasattr(RemotePathStrategy, "candidates")


def test_baidu_stockk_strategy_generates_current_daily_and_archive_paths():
    strategy = BaiduStockKPathStrategy()
    paths = [item.remote_path for item in strategy.candidates(Timeframe.M1, dt.date(2024, 12, 20))]
    assert "/A股_分时数据/1分钟/20241220_1min.zip" in paths
    assert "/A股_分时数据/1分钟_按月归档/2024-12/20241220_1min.zip" in paths
    assert "/A股_分时数据/1分钟_按年汇总/2024_1min.zip" in paths


def test_baidu_stockk_strategy_generates_timeframe_folders_and_suffixes():
    day = dt.date(2024, 12, 20)
    expected = {
        Timeframe.M5: "/A股_分时数据/5分钟/20241220_5min.zip",
        Timeframe.M15: "/A股_分时数据/15分钟/20241220_15min.zip",
        Timeframe.M30: "/A股_分时数据/30分钟/20241220_30min.zip",
        Timeframe.H1: "/A股_分时数据/60分钟/20241220_60min.zip",
    }
    strategy = BaiduStockKPathStrategy()
    for timeframe, path in expected.items():
        assert strategy.candidates(timeframe, day)[0].remote_path == path


def test_range_candidates_use_trading_days_and_clamp_supported_range():
    strategy = BaiduStockKPathStrategy(
        calendar=SimpleTradingCalendar(),
        supported_start=dt.date(2024, 12, 20),
        supported_end=dt.date(2024, 12, 23),
    )
    candidates = strategy.candidates_for_range(Timeframe.M1, dt.date(2024, 12, 19), dt.date(2024, 12, 24))
    trade_dates = {item.trade_date for item in candidates}
    assert trade_dates == {dt.date(2024, 12, 20), dt.date(2024, 12, 23)}
