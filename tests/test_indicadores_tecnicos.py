"""Pruebas de regresión de indicadores técnicos clave.

Se comparan los resultados de los indicadores implementados localmente contra
la librería ``ta`` para garantizar consistencia con referencias estándar.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest
from ta.momentum import RSIIndicator
from ta.trend import MACD

from indicadores.ichimoku import calcular_ichimoku_breakout
from indicadores.macd import calcular_macd
from indicadores.rsi import calcular_rsi, calcular_rsi_fast


def _sample_dataframe(longitud: int = 120) -> pd.DataFrame:
    rng = np.random.default_rng(42)
    precios = np.cumsum(rng.normal(loc=0.1, scale=1.0, size=longitud)) + 100
    altos = precios + rng.uniform(0.1, 1.0, size=longitud)
    bajos = precios - rng.uniform(0.1, 1.0, size=longitud)
    df = pd.DataFrame(
        {
            'close': precios,
            'high': np.maximum(altos, precios),
            'low': np.minimum(bajos, precios),
            'is_closed': True,
        }
    )
    return df


@pytest.mark.parametrize('short,long,signal', [(12, 26, 9), (5, 35, 5)])
def test_calcular_macd_coincide_con_ta(short: int, long: int, signal: int) -> None:
    df = _sample_dataframe(200)
    macd_val, signal_val, hist_val = calcular_macd(df, short=short, long=long, signal=signal)

    macd_ref = MACD(
        close=df['close'],
        window_fast=short,
        window_slow=long,
        window_sign=signal,
        fillna=False,
    )

    assert macd_val == pytest.approx(macd_ref.macd().iloc[-1])
    assert signal_val == pytest.approx(macd_ref.macd_signal().iloc[-1])
    assert hist_val == pytest.approx(macd_ref.macd_diff().iloc[-1])


@pytest.mark.parametrize('periodo', [7, 14])
def test_calcular_rsi_coincide_con_ta(periodo: int) -> None:
    df = _sample_dataframe(200)
    resultado = calcular_rsi(df, periodo=periodo, serie_completa=True)
    assert isinstance(resultado, pd.Series)

    rsi_ref = RSIIndicator(close=df['close'], window=periodo, fillna=False)
    serie_ref = rsi_ref.rsi()

    pd.testing.assert_series_equal(
        resultado.dropna(),
        serie_ref.dropna(),
        check_exact=False,
        rtol=1e-5,
        atol=1e-8,
    )


@pytest.mark.parametrize('periodo', [7, 14])
def test_calcular_rsi_fast_coincide_con_lento(periodo: int) -> None:
    df = _sample_dataframe(200)
    rsi_lento = calcular_rsi(df, periodo=periodo, serie_completa=True)
    rsi_fast = calcular_rsi_fast(df, periodo=periodo, serie_completa=True)

    pd.testing.assert_series_equal(
        rsi_fast.dropna(),
        rsi_lento.dropna(),
        check_exact=False,
        rtol=1e-5,
        atol=1e-8,
    )


def test_calcular_ichimoku_breakout_valores_estandar() -> None:
    df = _sample_dataframe(100)
    tenkan, kijun = calcular_ichimoku_breakout(df, tenkan_period=9, kijun_period=26)

    expected_tenkan = (df['high'].rolling(9).max() + df['low'].rolling(9).min()) / 2
    expected_kijun = (df['high'].rolling(26).max() + df['low'].rolling(26).min()) / 2

    assert tenkan == pytest.approx(expected_tenkan.iloc[-1])
    assert kijun == pytest.approx(expected_kijun.iloc[-1])


def test_indicadores_retornan_none_con_historial_insuficiente() -> None:
    df_corto = _sample_dataframe(10)
    assert calcular_macd(df_corto, short=5, long=9, signal=4) == (None, None, None)
    assert calcular_rsi(df_corto, periodo=14) is None
    assert calcular_rsi_fast(df_corto, periodo=14) is None
    assert calcular_ichimoku_breakout(df_corto, tenkan_period=9, kijun_period=26) == (None, None)