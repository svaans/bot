"""Tests del filtro macro BTC (core/strategies/filtro_macro.py).

Valida la función pura ``btc_en_tendencia`` y su integración como compuerta
en ``StrategyEngine._procesar_resultado_entrada`` (motivo ``macro_bajista``).
Tests de Fear & Greed en tests/test_fear_greed_filtro.py (sin dep. ta).
"""
from __future__ import annotations

from typing import Any, Mapping

import pandas as pd
import pytest

from core.strategies.filtro_macro import btc_en_tendencia
from core.strategies.strategy_engine import StrategyEngine


def _df_ohlcv(closes: list[float]) -> pd.DataFrame:
    serie = pd.Series(closes, dtype=float)
    return pd.DataFrame(
        {
            "timestamp": list(range(len(serie))),
            "open": serie,
            "high": serie * 1.01,
            "low": serie * 0.99,
            "close": serie,
            "volume": [10.0] * len(serie),
        }
    )


def _df_btc_bajista(n: int = 250) -> pd.DataFrame:
    # serie descendente: el cierre queda muy por debajo de la EMA200
    return _df_ohlcv([300.0 - i * 0.8 for i in range(n)])


def _df_btc_alcista(n: int = 250) -> pd.DataFrame:
    return _df_ohlcv([100.0 + i * 0.8 for i in range(n)])


# ----------------------------------------------------- función pura

def test_btc_en_tendencia_sin_datos_devuelve_none() -> None:
    assert btc_en_tendencia(None) is None
    assert btc_en_tendencia(_df_ohlcv([100.0] * 50)) is None  # < 200 velas
    assert btc_en_tendencia(pd.DataFrame({"open": [1.0] * 250})) is None


def test_btc_en_tendencia_detecta_direccion() -> None:
    assert btc_en_tendencia(_df_btc_alcista()) is True
    assert btc_en_tendencia(_df_btc_bajista()) is False


# ----------------------------------------------------- integración engine

def _engine_permisivo() -> StrategyEngine:
    async def fake_strategy_evaluator(
        symbol: str, frame: pd.DataFrame, tendencia: str
    ) -> Mapping[str, Any]:
        return {
            "estrategias_activas": {"estrategia_ema": True},
            "puntaje_total": 3.0,
            "diversidad": 1,
            "sinergia": 0.2,
        }

    return StrategyEngine(
        peso_provider=lambda _s: {"estrategia_ema": 1.0},
        strategy_evaluator=fake_strategy_evaluator,
        tendencia_detector=lambda _s, _f: ("alcista", {}),
        threshold_calculator=lambda _s, _f, _c: 1.5,
        technical_scorer=lambda *_args: (2.5, {}),
        rsi_getter=lambda _f: 55.0,
        momentum_getter=lambda _f: 0.01,
        slope_getter=lambda _f: 0.02,
    )


_BASE_CFG: dict[str, Any] = {
    "usar_score_tecnico": False,
    "umbral_score_tecnico": 2.0,
    "diversidad_minima": 1,
}


@pytest.mark.asyncio
async def test_filtro_desactivado_no_bloquea() -> None:
    engine = _engine_permisivo()
    r = await engine.evaluar_entrada(
        "BTC/EUR", _df_btc_bajista(), config={**_BASE_CFG}
    )
    assert r["permitido"] is True


@pytest.mark.asyncio
async def test_btc_bajista_bloquea_btc_y_alts() -> None:
    engine = _engine_permisivo()
    cfg = {**_BASE_CFG, "filtro_macro_btc_enabled": True}

    # BTC bajista: bloquea su propia entrada y deja cacheado el df macro
    r_btc = await engine.evaluar_entrada("BTC/EUR", _df_btc_bajista(), config=cfg)
    assert r_btc["permitido"] is False
    assert r_btc["motivo_rechazo"] == "macro_bajista"

    # la alt es alcista por sí misma, pero el macro manda
    r_alt = await engine.evaluar_entrada("ETH/EUR", _df_btc_alcista(), config=cfg)
    assert r_alt["permitido"] is False
    assert r_alt["motivo_rechazo"] == "macro_bajista"


@pytest.mark.asyncio
async def test_btc_alcista_permite_alts() -> None:
    engine = _engine_permisivo()
    cfg = {**_BASE_CFG, "filtro_macro_btc_enabled": True}

    r_btc = await engine.evaluar_entrada("BTC/EUR", _df_btc_alcista(), config=cfg)
    assert r_btc["permitido"] is True

    r_alt = await engine.evaluar_entrada("ETH/EUR", _df_btc_alcista(), config=cfg)
    assert r_alt["permitido"] is True


@pytest.mark.asyncio
async def test_sin_cache_btc_no_bloquea_alts() -> None:
    """Sin veredicto macro (nunca se vio un df de BTC) la alt no se bloquea."""
    engine = _engine_permisivo()
    cfg = {**_BASE_CFG, "filtro_macro_btc_enabled": True}
    r_alt = await engine.evaluar_entrada("ETH/EUR", _df_btc_alcista(), config=cfg)
    assert r_alt["permitido"] is True
