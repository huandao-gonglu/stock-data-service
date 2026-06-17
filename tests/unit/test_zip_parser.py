import datetime as dt
import io
import zipfile

from conftest import make_zip, sample_rows
from stock_data_service.market.normalizer import INTRADAY_COLUMNS
from stock_data_service.market.zip_parser import ZipBarParser


def test_extracts_symbol_csv_from_root_and_normalizes_columns():
    result = ZipBarParser().parse(make_zip(sample_rows()), "600000", source_path="/x.zip")
    assert result.status == "ok"
    assert list(result.dataframe["symbol"].unique()) == ["sh600000"]
    assert {"ts", "open", "high", "low", "close", "volume", "amount", "change_pct", "amplitude"}.issubset(
        result.dataframe.columns
    )
    assert result.dataframe.iloc[0]["source_path"] == "/x.zip"
    assert list(result.dataframe.columns) == INTRADAY_COLUMNS


def test_accepts_full_symbol_member_and_suffix_symbol_input_variants():
    payload = make_zip(sample_rows(), member="600000.SH.csv")
    assert ZipBarParser().parse(payload, "600000.SH").status == "ok"
    assert ZipBarParser().parse(payload, "sh600000").status == "ok"
    assert ZipBarParser().parse(payload, "600000").status == "ok"


def test_extracts_symbol_csv_from_nested_folder_and_gbk():
    payload = make_zip(sample_rows(), member="nested/600000.csv", encoding="gbk")
    result = ZipBarParser().parse(payload, "sh600000")
    assert result.status == "ok"
    assert len(result.dataframe) == 2


def test_accepts_old_columns_and_half_open_range():
    rows = [
        {"日期": "2024-12-20", "time": "930", "开盘": 1, "最高": 2, "最低": 1, "收盘": 2, "vol": 10},
        {"日期": "2024-12-20", "time": "931", "开盘": 2, "最高": 3, "最低": 2, "收盘": 3, "vol": 20},
    ]
    result = ZipBarParser().parse(
        make_zip(rows, member="600000.csv"),
        "600000",
        start=dt.datetime(2024, 12, 20, 9, 30),
        end=dt.datetime(2024, 12, 20, 9, 31),
    )
    assert result.status == "ok"
    assert len(result.dataframe) == 1


def test_missing_symbol_and_corrupted_zip_are_distinct():
    assert ZipBarParser().parse(make_zip(sample_rows(), member="000001.csv"), "600000").status == "symbol_missing"
    assert ZipBarParser().parse(b"not a zip", "600000").status == "corrupted_zip"


def test_parse_failed_is_not_empty_success():
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("600000.csv", "not,a,valid\n1,2")
    result = ZipBarParser().parse(buffer.getvalue(), "600000")
    assert result.status == "parse_failed"
