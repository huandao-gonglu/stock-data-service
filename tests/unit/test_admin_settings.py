import pytest

from stock_data_service.admin_settings import validate_admin_sync_settings


def test_admin_sync_settings_rejects_daily_baidu_sync():
    with pytest.raises(ValueError, match="not available for Baidu sync"):
        validate_admin_sync_settings(
            {
                "source_id": "baidu-main",
                "timeframe": "1d",
                "start": "2024-12-20",
                "end": "2024-12-20",
                "symbols": ["sh600000"],
            }
        )


def test_admin_sync_settings_accepts_existing_baidu_timeframes():
    parsed = validate_admin_sync_settings(
        {
            "source_id": "baidu-main",
            "timeframe": "60m",
            "start": "2024-12-20",
            "end": "2024-12-20",
            "symbols": ["600000.SH"],
        }
    )

    assert parsed.timeframe == "60m"
    assert parsed.symbols == ["sh600000"]
