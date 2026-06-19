import datetime as dt
import io
import zipfile

import pandas as pd

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


def test_accepts_annual_archive_symbol_member_names():
    payload = make_zip(sample_rows(), member="sh600000_2002.csv")
    result = ZipBarParser().parse(payload, "sh600000")
    assert result.status == "ok"
    assert len(result.dataframe) == 2


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


def test_iter_parse_archive_builds_one_index_and_yields_symbol_results(tmp_path, monkeypatch):
    zip_path = tmp_path / "20241220_1min.zip"
    _write_multi_symbol_zip(zip_path)
    open_count = 0
    original_zip_file = zipfile.ZipFile

    class CountingZipFile(original_zip_file):
        def __init__(self, *args, **kwargs):
            nonlocal open_count
            open_count += 1
            super().__init__(*args, **kwargs)

    monkeypatch.setattr("stock_data_service.market.zip_parser.zipfile.ZipFile", CountingZipFile)

    results = list(
        ZipBarParser().iter_parse_archive(
            zip_path,
            ["sh600000", "sz000001", "sh600004"],
            source_path="/x.zip",
        )
    )

    assert open_count == 1
    assert [(symbol, result.status) for symbol, result in results] == [
        ("sh600000", "ok"),
        ("sz000001", "ok"),
        ("sh600004", "symbol_missing"),
    ]
    assert list(results[0][1].dataframe["symbol"].unique()) == ["sh600000"]
    assert list(results[1][1].dataframe["symbol"].unique()) == ["sz000001"]


def test_symbol_member_index_selects_present_and_missing_symbols(tmp_path):
    zip_path = tmp_path / "2000_1min.zip"
    _write_multi_symbol_zip(zip_path)

    parser = ZipBarParser()
    index = parser.symbol_member_index(zip_path)
    selection = parser.select_archive_symbols(zip_path, ["600000.SH", "000001.SZ", "sh600004"])

    assert index["sh600000"] == "sh600000.csv"
    assert index["sz000001"] == "sz000001.csv"
    assert selection.present_symbols == ["sh600000", "sz000001"]
    assert selection.missing_symbols == ["sh600004"]


def test_parse_member_reads_one_archive_member(tmp_path):
    zip_path = tmp_path / "2000_1min.zip"
    _write_multi_symbol_zip(zip_path)

    parser = ZipBarParser()
    result = parser.parse_member(zip_path, "sz000001", "sz000001.csv", source_path="/x.zip")

    assert result.status == "ok"
    assert list(result.dataframe["symbol"].unique()) == ["sz000001"]
    assert parser.parse_member(zip_path, "sh600004", None).status == "symbol_missing"


def test_numeric_text_fields_are_normalized_to_strings():
    rows = sample_rows(symbol=600000, name=600000)
    result = ZipBarParser().parse(make_zip(rows), "sh600000")

    assert result.status == "ok"
    assert result.dataframe.iloc[0]["code"] == "600000"
    assert result.dataframe.iloc[0]["name"] == "600000"


def test_iter_parse_archive_isolates_member_parse_failures(tmp_path):
    zip_path = tmp_path / "20241220_1min.zip"
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(
            "sh600000.csv",
            pd.DataFrame(sample_rows(symbol="sh600000")).to_csv(index=False).encode("utf-8-sig"),
        )
        archive.writestr("sz000001.csv", "not,a,valid\n1,2")

    results = list(ZipBarParser().iter_parse_archive(zip_path, ["sh600000", "sz000001"]))

    assert [(symbol, result.status) for symbol, result in results] == [
        ("sh600000", "ok"),
        ("sz000001", "parse_failed"),
    ]
    assert len(results[0][1].dataframe) == 2


def test_iter_parse_archive_applies_half_open_range_per_symbol(tmp_path):
    zip_path = tmp_path / "20241220_1min.zip"
    _write_multi_symbol_zip(zip_path)

    results = dict(
        ZipBarParser().iter_parse_archive(
            zip_path,
            ["sh600000", "sz000001"],
            start=dt.datetime(2024, 12, 20, 9, 30),
            end=dt.datetime(2024, 12, 20, 9, 31),
        )
    )

    assert results["sh600000"].status == "ok"
    assert results["sz000001"].status == "ok"
    assert len(results["sh600000"].dataframe) == 1
    assert len(results["sz000001"].dataframe) == 1
    assert results["sh600000"].dataframe.iloc[0]["ts"] == pd.Timestamp("2024-12-20 09:30:00")


def _write_multi_symbol_zip(path):
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as archive:
        for symbol in ["sh600000", "sz000001"]:
            payload = pd.DataFrame(sample_rows(symbol=symbol)).to_csv(index=False).encode("utf-8-sig")
            archive.writestr(f"{symbol}.csv", payload)
