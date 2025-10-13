from binance_api.utils import normalize_symbol_for_rest, normalize_symbol_for_ws


def test_normalize_symbol_for_rest():
    assert normalize_symbol_for_rest("BTC/EUR") == "BTCEUR"
    assert normalize_symbol_for_rest("adaeur") == "ADAEUR"


def test_normalize_symbol_for_ws():
    assert normalize_symbol_for_ws("BTC/EUR") == "btceur"
    assert normalize_symbol_for_ws("ADAEUR") == "adaeur"