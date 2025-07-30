import pandas as pd
import numpy as np
from core.strategies.evaluador_tecnico import evaluar_puntaje_tecnico, PESOS_DEFECTO
from core.adaptador_dinamico import calcular_umbral_adaptativo
from core.strategies.entry.validadores import (
    validar_rsi, validar_slope, validar_bollinger, validar_max_min, validar_spread
)


def _make_df(trend='flat', vol=0.01, n=60):
    if trend == 'up':
        base = np.linspace(100, 110, n)
    elif trend == 'down':
        base = np.linspace(110, 100, n)
    else:
        base = np.linspace(100, 100, n)
    noise = np.sin(np.linspace(0, 5, n)) * vol * 100
    close = base + noise
    return pd.DataFrame({
        'open': close - 0.5,
        'high': close + 0.5,
        'low': close - 1,
        'close': close,
        'volume': np.linspace(100, 120, n)
    })


def test_score_normalizado_calculo_correcto():
    df = _make_df('up')
    res = evaluar_puntaje_tecnico('AAA', df, df['close'].iloc[-1], df['low'].iloc[-1], df['high'].iloc[-1])
    score_max = sum(PESOS_DEFECTO.values())
    esperado = round(res['score_total'] / score_max, 2)
    assert res['score_normalizado'] == esperado


def test_umbral_adaptativo_responde_a_volatilidad_y_slope():
    estrategias = {'e1': True, 'e2': True, 'e3': True}
    pesos = {'e1': 9, 'e2': 9, 'e3': 9}

    df_base = _make_df('up', vol=0.001)
    df_volatil = _make_df('up', vol=1.0)
    df_bajista = _make_df('down', vol=0.001)

    umbral_base = calcular_umbral_adaptativo('AAA', df_base, estrategias, pesos)
    umbral_vol = calcular_umbral_adaptativo('AAA', df_volatil, estrategias, pesos)
    umbral_bajista = calcular_umbral_adaptativo('AAA', df_bajista, estrategias, pesos)

    assert umbral_vol > umbral_base
    assert umbral_bajista > umbral_base


def test_filtros_no_modifican_dataframe():
    df = _make_df('up')
    filtros = [validar_rsi, lambda d: validar_slope(d, 'alcista'),
               validar_bollinger, validar_max_min, validar_spread]
    for f in filtros:
        copia = df.copy(deep=True)
        resultado = f(copia)
        pd.testing.assert_frame_equal(copia, df)
        assert isinstance(bool(resultado), bool)