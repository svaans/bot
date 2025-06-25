import pandas as pd
from core.utils.validacion import validar_dataframe

def test_validar_dataframe_ok():
    df = pd.DataFrame({"open": [1], "high": [2], "low": [0], "close": [1], "volume": [10]})
    assert validar_dataframe(df, ["open", "high", "low", "close", "volume"])

def test_validar_dataframe_faltante():
    df = pd.DataFrame({"open": [1], "high": [2], "close": [1]})
    assert not validar_dataframe(df, ["open", "high", "low", "close"])

def test_validar_dataframe_vacio():
    df = pd.DataFrame(columns=["open", "high", "low", "close"])
    assert not validar_dataframe(df, ["open", "high", "low", "close"])