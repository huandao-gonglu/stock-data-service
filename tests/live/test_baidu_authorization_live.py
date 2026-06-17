import os

import pytest

from stock_data_service.auth.token_manager import TokenManager

pytestmark = pytest.mark.live


@pytest.mark.skipif(os.getenv("RUN_LIVE_BAIDU_TESTS") != "1", reason="live Baidu tests are opt-in")
def test_can_load_real_token_file():
    manager = TokenManager(
        os.environ["BAIDU_TOKEN_FILE"],
        app_key=os.getenv("BAIDU_APP_KEY"),
        app_secret=os.getenv("BAIDU_APP_SECRET"),
    )
    assert manager.status()["has_token"] is True
