import datetime as dt

from stock_data_service.market.security_master import _listings_from_baostock_rows


def test_builds_stock_listings_from_baostock_rows():
    rows = [
        {
            "code": "sh.600000",
            "code_name": "浦发银行",
            "ipoDate": "1999-11-10",
            "outDate": "",
            "type": "1",
            "status": "1",
        },
        {
            "code": "sh.000001",
            "code_name": "上证指数",
            "ipoDate": "",
            "outDate": "",
            "type": "2",
            "status": "1",
        },
        {
            "code": "sz.000003",
            "code_name": "退市股票",
            "ipoDate": "1991-01-01",
            "outDate": "2002-06-14",
            "type": "1",
            "status": "0",
        },
    ]

    listings = list(_listings_from_baostock_rows(rows))

    assert listings[0].symbol == "sh600000"
    assert listings[0].listed_at == dt.date(1999, 11, 10)
    assert listings[0].delisted_at is None
    assert listings[0].status == "listed"
    assert listings[1].symbol == "sz000003"
    assert listings[1].delisted_at == dt.date(2002, 6, 14)
    assert listings[1].status == "delisted"
