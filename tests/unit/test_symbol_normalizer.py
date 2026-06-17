import pytest

from stock_data_service.market.symbol_normalizer import SymbolValidationError, normalize_symbol


def test_normalizes_full_symbols_and_suffixes():
    assert normalize_symbol("sh600000") == "sh600000"
    assert normalize_symbol("SH600000") == "sh600000"
    assert normalize_symbol("600000.SH") == "sh600000"


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("600000", "sh600000"),
        ("688001", "sh688001"),
        ("300750", "sz300750"),
        ("000001", "sz000001"),
        ("430047", "bj430047"),
        ("bj430047", "bj430047"),
        ("sz000001", "sz000001"),
    ],
)
def test_normalizes_bare_a_share_codes(raw, expected):
    assert normalize_symbol(raw) == expected


def test_rejects_ambiguous_index_like_bare_codes():
    with pytest.raises(SymbolValidationError):
        normalize_symbol("000300")
    assert normalize_symbol("000300", asset_type="index") == "sh000300"


def test_rejects_unknown_bare_prefix():
    with pytest.raises(SymbolValidationError):
        normalize_symbol("123456")
