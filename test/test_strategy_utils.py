import pandas as pd
import numpy as np
from core.score_tecnico import calcular_score_tecnico
from core.scoring import PESOS_SCORE_TECNICO
from core.strategies.entry.validaciones_tecnicas import hay_contradicciones
from core.evaluacion_tecnica import evaluar_estrategias


def test_calcular_score_tecnico():
    df = pd.DataFrame({'close': np.linspace(1, 10, 10)})
    score = calcular_score_tecnico(df, rsi=60, momentum=0.01, slope=0.02,
        tendencia='alcista')
    esperado = (
        PESOS_SCORE_TECNICO['RSI']
        + PESOS_SCORE_TECNICO['Momentum']
        + PESOS_SCORE_TECNICO['Slope']
    )
    assert score == esperado


def test_hay_contradicciones():
    estrategias = {'rsi_alcista': True, 'rsi_bajista': True}
    assert hay_contradicciones(estrategias)
    estrategias = {'rsi_alcista': True, 'rsi_bajista': False}
    assert not hay_contradicciones(estrategias)


def test_evaluar_estrategias(monkeypatch):
    resultado = {'estrategias_activas': {'e1': True}, 'puntaje_total': 2.0,
        'diversidad': 1, 'sinergia': 0.1}

    def fake_eval(symbol, df, tendencia):
        return resultado
    monkeypatch.setattr(
        'estrategias_entrada.gestor_entradas.evaluar_estrategias', fake_eval)
    df = pd.DataFrame({'close': [1], 'high': [1], 'low': [1], 'volume': [1]})
    res = evaluar_estrategias('BTC/EUR', df, 'alcista')
    assert res == resultado
