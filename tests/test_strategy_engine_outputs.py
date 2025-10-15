"""Pruebas unitarias para validar la salida del StrategyEngine."""
from __future__ import annotations

from typing import Any, Mapping

import pandas as pd
import pytest

from core.strategies.strategy_engine import StrategyEngine


@pytest.mark.asyncio
async def test_strategy_engine_exposes_score_aliases() -> None:
    """El motor debe exponer ``score`` y metadatos compatibles con el pipeline."""

    df = pd.DataFrame(
        {
            "timestamp": [1, 2, 3, 4, 5],
            "open": [100.0, 101.0, 102.0, 103.0, 104.0],
            "high": [101.0, 102.0, 103.0, 104.0, 105.0],
            "low": [99.0, 100.0, 101.0, 102.0, 103.0],
            "close": [100.5, 101.5, 102.5, 103.5, 104.5],
            "volume": [10, 11, 12, 13, 14],
        }
    )

    async def fake_strategy_evaluator(
        symbol: str,
        frame: pd.DataFrame,
        tendencia: str,
    ) -> Mapping[str, Any]:
        assert symbol == "BTCUSDT"
        assert tendencia == "alcista"
        assert frame.equals(df)
        return {
            "estrategias_activas": {"estrategia_ema": True},
            "puntaje_total": 3.0,
            "diversidad": 1,
            "sinergia": 0.2,
        }

    engine = StrategyEngine(
        peso_provider=lambda symbol: {"estrategia_ema": 1.0},
        strategy_evaluator=fake_strategy_evaluator,
        tendencia_detector=lambda symbol, frame: ("alcista", {}),
        threshold_calculator=lambda symbol, frame, contexto: 1.5,
        technical_scorer=lambda *_args: (2.8, {"rsi": 1.0}),
        rsi_getter=lambda frame: 55.0,
        momentum_getter=lambda frame: 0.01,
        slope_getter=lambda frame: 0.02,
    )

    config = {
        "usar_score_tecnico": False,
        "umbral_score_tecnico": 2.0,
        "diversidad_minima": 1,
    }

    resultado = await engine.evaluar_entrada("BTCUSDT", df, config=config)

    assert resultado["permitido"] is True
    assert resultado["score"] == pytest.approx(resultado["score_tecnico"])
    assert resultado["side"] == "long"
    assert resultado["contradicciones"] is False
    assert resultado["score_tecnico_detalle"] == {"rsi": 1.0}


@pytest.mark.asyncio
async def test_strategy_engine_normaliza_tendencia_desde_estado() -> None:
    """Debe ignorar objetos ``EstadoSimbolo`` y recalcular la tendencia."""

    from core.trader import EstadoSimbolo

    df = pd.DataFrame(
        {
            "timestamp": [1, 2, 3, 4, 5],
            "open": [100.0, 101.0, 102.0, 103.0, 104.0],
            "high": [101.0, 102.0, 103.0, 104.0, 105.0],
            "low": [99.0, 100.0, 101.0, 102.0, 103.0],
            "close": [100.5, 101.5, 102.5, 103.5, 104.5],
            "volume": [10, 11, 12, 13, 14],
        }
    )

    async def fake_strategy_evaluator(
        symbol: str,
        frame: pd.DataFrame,
        tendencia: str,
    ) -> Mapping[str, Any]:
        assert symbol == "BTCUSDT"
        assert tendencia == "alcista"
        assert frame.equals(df)
        return {
            "estrategias_activas": {"estrategia_ema": True},
            "puntaje_total": 3.0,
            "diversidad": 1,
            "sinergia": 0.2,
        }

    engine = StrategyEngine(
        peso_provider=lambda symbol: {"estrategia_ema": 1.0},
        strategy_evaluator=fake_strategy_evaluator,
        tendencia_detector=lambda symbol, frame: ("alcista", {}),
        threshold_calculator=lambda symbol, frame, contexto: 1.5,
        technical_scorer=lambda *_args: (2.8, {"rsi": 1.0}),
        rsi_getter=lambda frame: 55.0,
        momentum_getter=lambda frame: 0.01,
        slope_getter=lambda frame: 0.02,
    )

    config = {
        "usar_score_tecnico": False,
        "umbral_score_tecnico": 2.0,
        "diversidad_minima": 1,
    }

    estado = EstadoSimbolo()

    resultado = await engine.evaluar_entrada("BTCUSDT", df, tendencia=estado, config=config)

    assert resultado["permitido"] is True
    assert resultado["side"] == "long"
