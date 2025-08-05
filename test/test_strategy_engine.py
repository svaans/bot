import pandas as pd
from core.strategies.strategy_engine import StrategyEngine


def _setup_monkeypatch(monkeypatch, *, contradiccion=False, rsi=50, momentum=0.1):
    monkeypatch.setattr(
        "core.strategies.strategy_engine.evaluar_estrategias",
        lambda *a, **k: {
            "estrategias_activas": {"e1": True},
            "puntaje_total": 2.0,
            "diversidad": 2,
            "sinergia": 0.0,
        },
    )
    monkeypatch.setattr(
        "core.strategies.strategy_engine.hay_contradicciones",
        lambda estrategias: contradiccion,
    )
    monkeypatch.setattr(
        "core.strategies.strategy_engine.calcular_umbral_adaptativo",
        lambda *a, **k: 1.0,
    )
    monkeypatch.setattr(
        "core.strategies.strategy_engine.validar_volumen", lambda df: True
    )
    monkeypatch.setattr(
        "core.strategies.strategy_engine.validar_rsi",
        lambda df, direccion: True,
    )
    monkeypatch.setattr(
        "core.strategies.strategy_engine.validar_slope",
        lambda df, tendencia: True,
    )
    monkeypatch.setattr(
        "core.strategies.strategy_engine.validar_bollinger", lambda df: True
    )
    monkeypatch.setattr(
        "core.strategies.strategy_engine.calcular_rsi", lambda df: rsi
    )
    monkeypatch.setattr(
        "core.strategies.strategy_engine.calcular_slope", lambda df: 0.1
    )
    monkeypatch.setattr(
        "core.strategies.strategy_engine.calcular_momentum",
        lambda df: momentum,
    )
    monkeypatch.setattr(
        "core.strategies.strategy_engine.calcular_score_tecnico",
        lambda *a, **k: 2.0,
    )


def _make_df():
    return pd.DataFrame(
        {
            "close": [100] * 10,
            "high": [101] * 10,
            "low": [99] * 10,
            "volume": [1] * 10,
        }
    )


def test_contradicciones_respetan_config(monkeypatch):
    _setup_monkeypatch(monkeypatch, contradiccion=True)
    df = _make_df()
    cfg = {"contradicciones_bloquean_entrada": True}
    res = StrategyEngine.evaluar_entrada("BTC/EUR", df, tendencia="alcista", config=cfg)
    assert not res["permitido"] and res["motivo_rechazo"] == "contradiccion"
    cfg["contradicciones_bloquean_entrada"] = False
    res = StrategyEngine.evaluar_entrada("BTC/EUR", df, tendencia="alcista", config=cfg)
    assert res["permitido"]


def test_contradicciones_tecnicas_rsi(monkeypatch):
    _setup_monkeypatch(monkeypatch, rsi=80)
    df = _make_df()
    cfg = {"contradicciones_bloquean_entrada": True}
    res = StrategyEngine.evaluar_entrada("BTC/EUR", df, tendencia="alcista", config=cfg)
    assert not res["permitido"] and res["motivo_rechazo"] == "contradiccion"
    cfg["contradicciones_bloquean_entrada"] = False
    res = StrategyEngine.evaluar_entrada("BTC/EUR", df, tendencia="alcista", config=cfg)
    assert res["permitido"]
