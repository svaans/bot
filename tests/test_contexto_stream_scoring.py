"""Verifica el cálculo de puntajes técnicos con datos simulados de Binance."""

from __future__ import annotations

import math
import statistics

import pytest

from core.contexto_externo import StreamContexto
from core.contexto_storage import PuntajeStore


@pytest.mark.asyncio
async def test_stream_contexto_calcula_puntaje_log_weight(tmp_path) -> None:
    """El puntaje inicial debe respetar la fórmula de ``weighted_return``."""

    store = PuntajeStore(path=tmp_path / "puntajes_ws.db", ttl_seconds=30)
    stream = StreamContexto(
        score_baseline_min=8,
        score_window=16,
        volume_baseline_min=3,
        volume_window=5,
        puntajes_store=store,
    )

    resultado = stream._calcular_puntaje_stream("BTCUSDT", 100.0, 101.0, 25.0)
    assert resultado is not None
    puntaje, metadata = resultado

    base = stream._obtener_volumen_base("BTCUSDT", 25.0)
    expected_return = math.log(101.0 / 100.0)
    expected_weight = math.log1p(25.0 / base)
    expected_score = expected_return * expected_weight

    assert puntaje == pytest.approx(expected_score, rel=1e-6)
    assert metadata["weighted_return"] == pytest.approx(expected_score, rel=1e-6)
    assert metadata["retorno_log"] == pytest.approx(expected_return, rel=1e-6)


@pytest.mark.asyncio
async def test_stream_contexto_normaliza_puntaje_con_historial(tmp_path) -> None:
    """Cuando hay historial suficiente se aplica z-score sobre el retorno ponderado."""

    store = PuntajeStore(path=tmp_path / "puntajes_historial.db", ttl_seconds=30)
    stream = StreamContexto(
        score_baseline_min=5,
        score_window=8,
        volume_baseline_min=3,
        volume_window=5,
        puntajes_store=store,
    )

    history = stream._score_history["ETHUSDT"]
    history.extend([0.01, -0.015, 0.02, 0.005, 0.012])
    history_snapshot = list(history)
    volumes = stream._volume_history["ETHUSDT"]
    volumes.extend([12.0, 11.5, 10.8, 12.2, 11.7])

    open_price = 100.0
    close_price = 100.6
    volume = 15.0

    base = stream._obtener_volumen_base("ETHUSDT", volume)
    weighted_return = math.log(close_price / open_price) * math.log1p(volume / base)
    mean = statistics.fmean(history_snapshot)
    stdev = statistics.pstdev(history_snapshot)
    expected_score = (weighted_return - mean) / stdev

    resultado = stream._calcular_puntaje_stream("ETHUSDT", open_price, close_price, volume)
    assert resultado is not None
    puntaje, metadata = resultado

    assert puntaje == pytest.approx(expected_score, rel=1e-6)
    assert metadata["weighted_return"] == pytest.approx(weighted_return, rel=1e-6)
    assert stream._score_history["ETHUSDT"][-1] == pytest.approx(
        weighted_return, rel=1e-9
    )