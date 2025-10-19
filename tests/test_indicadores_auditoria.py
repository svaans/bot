import math

import numpy as np
import pandas as pd
import pytest
from ta.momentum import RSIIndicator
from ta.trend import ADXIndicator, MACD
from ta.volatility import AverageTrueRange, BollingerBands

from indicadores import adx, atr, bollinger, macd, rsi, vwap
from indicadores.helpers import filtrar_cerradas


@pytest.fixture(scope="module")
def sample_dataframe() -> pd.DataFrame:
    np.random.seed(42)
    n = 240
    index = pd.date_range("2024-01-01", periods=n, freq="h")
    base = 100 + np.cumsum(np.random.standard_normal(n))
    high = base + np.random.uniform(0.2, 2.2, size=n)
    low = base - np.random.uniform(0.2, 2.2, size=n)
    close = base + np.random.uniform(-0.5, 0.5, size=n)
    volume = np.random.uniform(1_000, 10_000, size=n)
    df = pd.DataFrame(
        {
            "open": base,
            "high": np.maximum.reduce([high, close, base]),
            "low": np.minimum.reduce([low, close, base]),
            "close": close,
            "volume": volume,
            "is_closed": np.ones(n, dtype=bool),
        },
        index=index,
    )
    # Simula la última vela aún en formación
    df.loc[df.index[-1], "is_closed"] = False
    return df


def _align(series_a: pd.Series, series_b: pd.Series) -> pd.DataFrame:
    combinado = pd.concat([series_a, series_b], axis=1, join="inner").dropna()
    assert not combinado.empty, "No hay traslape suficiente para comparar series"
    return combinado


@pytest.mark.parametrize("periodo", [14, 21])
def test_rsi_matches_ta(sample_dataframe: pd.DataFrame, periodo: int) -> None:
    df = sample_dataframe
    resultado = rsi.calcular_rsi(df, periodo=periodo, serie_completa=True)
    assert isinstance(resultado, pd.Series)

    referencia = RSIIndicator(
        close=filtrar_cerradas(df)["close"],
        window=periodo,
        fillna=False,
    ).rsi()

    comparacion = _align(resultado, referencia)
    assert np.allclose(comparacion.iloc[:, 0], comparacion.iloc[:, 1], atol=1e-6, rtol=1e-6)


@pytest.mark.parametrize("short,long,signal", [(12, 26, 9), (5, 35, 5)])
def test_macd_matches_ta(sample_dataframe: pd.DataFrame, short: int, long: int, signal: int) -> None:
    macd_val, signal_val, hist_val = macd.calcular_macd(
        sample_dataframe,
        short=short,
        long=long,
        signal=signal,
    )

    close = filtrar_cerradas(sample_dataframe)["close"].astype(float)
    referencia = MACD(
        close=close,
        window_slow=long,
        window_fast=short,
        window_sign=signal,
        fillna=False,
    )
    assert math.isclose(referencia.macd().iloc[-1], macd_val, rel_tol=1e-6, abs_tol=1e-6)
    assert math.isclose(
        referencia.macd_signal().iloc[-1], signal_val, rel_tol=1e-6, abs_tol=1e-6
    )
    assert math.isclose(
        referencia.macd_diff().iloc[-1], hist_val, rel_tol=1e-6, abs_tol=1e-6
    )


@pytest.mark.parametrize("periodo,desviacion", [(20, 2.0), (10, 2.5)])
def test_bollinger_matches_ta(
    sample_dataframe: pd.DataFrame, periodo: int, desviacion: float
) -> None:
    inferior, superior, cierre = bollinger.calcular_bollinger(
        sample_dataframe,
        periodo=periodo,
        desviacion=desviacion,
    )

    close = filtrar_cerradas(sample_dataframe)["close"].astype(float)
    referencia = BollingerBands(
        close=close,
        window=periodo,
        window_dev=desviacion,
        fillna=False,
    )
    banda_baja_ref = referencia.bollinger_lband().iloc[-1]
    banda_alta_ref = referencia.bollinger_hband().iloc[-1]
    assert math.isclose(banda_baja_ref, inferior, rel_tol=1e-3, abs_tol=0.2)
    assert math.isclose(banda_alta_ref, superior, rel_tol=1e-3, abs_tol=0.2)
    assert math.isclose(close.iloc[-1], cierre, rel_tol=1e-9, abs_tol=1e-9)


@pytest.mark.parametrize("periodo", [14, 21])
def test_atr_matches_ta(sample_dataframe: pd.DataFrame, periodo: int) -> None:
    valor = atr.calcular_atr(sample_dataframe, periodo=periodo)
    referencia = AverageTrueRange(
        high=filtrar_cerradas(sample_dataframe)["high"],
        low=filtrar_cerradas(sample_dataframe)["low"],
        close=filtrar_cerradas(sample_dataframe)["close"],
        window=periodo,
        fillna=False,
    ).average_true_range()
    assert math.isclose(referencia.iloc[-1], valor, rel_tol=1e-6, abs_tol=1e-6)


@pytest.mark.parametrize("periodo", [14, 28])
def test_adx_matches_ta(sample_dataframe: pd.DataFrame, periodo: int) -> None:
    valor = adx.calcular_adx(sample_dataframe, periodo=periodo)
    referencia = ADXIndicator(
        high=filtrar_cerradas(sample_dataframe)["high"],
        low=filtrar_cerradas(sample_dataframe)["low"],
        close=filtrar_cerradas(sample_dataframe)["close"],
        window=periodo,
        fillna=False,
    ).adx()
    assert math.isclose(referencia.iloc[-1], valor, rel_tol=1e-6, abs_tol=1e-6)


def test_vwap_matches_cumulative_reference(sample_dataframe: pd.DataFrame) -> None:
    valor = vwap.calcular_vwap(sample_dataframe)
    df_cerrado = filtrar_cerradas(sample_dataframe)
    typical_price = (df_cerrado["high"] + df_cerrado["low"] + df_cerrado["close"]) / 3
    referencia = (typical_price * df_cerrado["volume"]).cumsum() / df_cerrado["volume"].cumsum()
    assert math.isclose(referencia.iloc[-1], valor, rel_tol=1e-6, abs_tol=1e-6)