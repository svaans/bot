"""
Simulaciones de la compuerta de entrada del motor (StrategyEngine).

Sirven para validar en CI el mismo criterio que en producción cuando el bot
“no avanza” hacia órdenes o notificaciones: con frecuencia la evaluación sí
corre, pero ``permitido`` queda en ``False`` (p. ej. ``score_tecnico`` por
debajo de ``umbral_score_tecnico``). Esas variaciones se prueban aquí, no
mediante ejecución manual ni umbrales temporales en runtime.
"""

from __future__ import annotations

from typing import Any, Mapping

import pandas as pd
import pytest

from core.strategies.strategy_engine import StrategyEngine


def _minimal_ohlcv_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "timestamp": [1, 2, 3, 4, 5],
            "open": [100.0, 101.0, 102.0, 103.0, 104.0],
            "high": [101.0, 102.0, 103.0, 104.0, 105.0],
            "low": [99.0, 100.0, 101.0, 102.0, 103.0],
            "close": [100.5, 101.5, 102.5, 103.5, 104.5],
            "volume": [10, 11, 12, 13, 14],
        }
    )


async def _fake_strategies_ok(symbol: str, frame: pd.DataFrame, tendencia: str) -> Mapping[str, Any]:
    assert symbol == "BTC/EUR"
    assert tendencia == "alcista"
    return {
        "estrategias_activas": {"estrategia_ema": True},
        "puntaje_total": 5.0,
        "diversidad": 1,
        "sinergia": 0.0,
    }


@pytest.mark.asyncio
async def test_score_tecnico_below_umbral_blocks_even_with_high_score_total() -> None:
    """Réplica el caso típico de logs: score_total pasa pero score_tecnico no."""

    df = _minimal_ohlcv_df()
    engine = StrategyEngine(
        peso_provider=lambda _s: {"estrategia_ema": 1.0},
        strategy_evaluator=_fake_strategies_ok,
        tendencia_detector=lambda _s, _f: ("alcista", {}),
        threshold_calculator=lambda _s, _f, _c: 1.0,
        technical_scorer=lambda *_args: (1.0, {"unit": 1.0}),
        rsi_getter=lambda _f: 55.0,
        momentum_getter=lambda _f: 0.01,
        slope_getter=lambda _f: 0.02,
    )
    config = {
        "usar_score_tecnico": False,
        "umbral_score_tecnico": 2.0,
        "diversidad_minima": 1,
    }

    out = await engine.evaluar_entrada("BTC/EUR", df, config=config)

    assert out["permitido"] is False
    assert out["motivo_rechazo"] == "score_tecnico_bajo"
    assert out["score_tecnico"] == pytest.approx(1.0)
    assert out["umbral_score_tecnico"] == pytest.approx(2.0)
    assert out["score_total"] > out["umbral"]


@pytest.mark.asyncio
async def test_lowering_umbral_score_tecnico_only_in_test_allows_entry() -> None:
    """Misma simulación que arriba, pero el umbral técnico se relaja solo en el test."""

    df = _minimal_ohlcv_df()
    engine = StrategyEngine(
        peso_provider=lambda _s: {"estrategia_ema": 1.0},
        strategy_evaluator=_fake_strategies_ok,
        tendencia_detector=lambda _s, _f: ("alcista", {}),
        threshold_calculator=lambda _s, _f, _c: 1.0,
        technical_scorer=lambda *_args: (1.0, {"unit": 1.0}),
        rsi_getter=lambda _f: 55.0,
        momentum_getter=lambda _f: 0.01,
        slope_getter=lambda _f: 0.02,
    )
    config = {
        "usar_score_tecnico": False,
        "umbral_score_tecnico": 0.5,
        "diversidad_minima": 1,
    }

    out = await engine.evaluar_entrada("BTC/EUR", df, config=config)

    assert out["permitido"] is True
    assert out["motivo_rechazo"] is None
    assert out["score_tecnico"] == pytest.approx(1.0)


@pytest.mark.asyncio
async def test_conflicting_bull_and_bear_strategies_resolved_clears_contradiction_flag() -> None:
    """Tras ``_resolver_conflicto``, ``contradicciones`` puede quedar en False."""

    df = _minimal_ohlcv_df()

    async def fake_mixed(_symbol: str, _frame: pd.DataFrame, tendencia: str) -> Mapping[str, Any]:
        assert tendencia == "alcista"
        return {
            "estrategias_activas": {
                "estrategia_ema": True,
                "estrategia_sma_bajista": True,
            },
            "puntaje_total": 5.0,
            "diversidad": 2,
            "sinergia": 0.0,
        }

    engine = StrategyEngine(
        # Peso mayor a bajista para que gane SELL y ``contradiccion`` pase a False.
        peso_provider=lambda _s: {"estrategia_ema": 1.0, "estrategia_sma_bajista": 5.0},
        strategy_evaluator=fake_mixed,
        tendencia_detector=lambda _s, _f: ("alcista", {}),
        threshold_calculator=lambda _s, _f, _c: 1.0,
        technical_scorer=lambda *_args: (3.0, {"rsi": 1.0}),
        rsi_getter=lambda _f: 55.0,
        momentum_getter=lambda _f: 0.01,
        slope_getter=lambda _f: 0.02,
    )
    config = {
        "usar_score_tecnico": False,
        "umbral_score_tecnico": 2.0,
        "diversidad_minima": 1,
    }

    out = await engine.evaluar_entrada("BTC/EUR", df, config=config)

    assert out["contradicciones"] is False
    assert out["conflicto_resuelto"] is True
    assert out["resolucion_conflicto"] == "sell"
    assert out["permitido"] is True
