import pandas as pd
import numpy as np

from indicadores import ema, rsi


def test_calcular_cruce_ema_fast_equivalente():
    np.random.seed(0)
    df = pd.DataFrame({"close": np.random.random(100)})
    esperado = ema.calcular_cruce_ema(df)
    resultado = ema.calcular_cruce_ema_fast(df)
    assert resultado == esperado


def test_calcular_rsi_fast_equivalente():
    np.random.seed(0)
    df = pd.DataFrame({"close": np.random.random(100)})
    esperado = rsi.calcular_rsi(df, serie_completa=True)
    resultado = rsi.calcular_rsi_fast(df, serie_completa=True)
    assert np.allclose(esperado, resultado, equal_nan=True)