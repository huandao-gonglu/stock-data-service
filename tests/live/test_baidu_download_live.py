import os

import pytest

from stock_data_service.auth.token_manager import TokenManager
from stock_data_service.baidu.pan_client import BaiduPanClient

pytestmark = pytest.mark.live


@pytest.mark.skipif(os.getenv("RUN_LIVE_BAIDU_TESTS") != "1", reason="live Baidu tests are opt-in")
def test_can_list_known_baidu_directory():
    manager = TokenManager(
        os.environ["BAIDU_TOKEN_FILE"],
        app_key=os.getenv("BAIDU_APP_KEY"),
        app_secret=os.getenv("BAIDU_APP_SECRET"),
    )
    client = BaiduPanClient(manager)
    payload = client.list_files("/A股_分时数据/1分钟", limit=1)
    assert "list" in payload
