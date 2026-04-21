"""
Integración de alto nivel: el mismo cable que en producción (DataFeed → Trader → procesar_vela),
sin red ni StartupManager. Complementa los ~600 tests unitarios detectando fallos de composición.
"""

from __future__ import annotations

import time
from typing import Iterable

import pytest

from core.data_feed.datafeed import DataFeed
from core.procesar_vela import get_buffer_manager
from tests.log_sanitizer import parse_records
from tests.test_e2e_bot_flow import (
    TIMEFRAME,
    BotFlowHarness,
    _build_series,
    _enable_flow_logging,
    _find_events,
    wait_for,
)


@pytest.fixture(autouse=True)
def _clear_global_buffers() -> Iterable[None]:
    manager = get_buffer_manager()
    manager._estados.clear()
    manager._locks.clear()
    yield
    manager._estados.clear()
    manager._locks.clear()


@pytest.mark.asyncio(mode="strict")
@pytest.mark.flags(
    "orders.retry_persistencia.enabled",
    "orders.flush_periodico.enabled",
    "orders.limit.enabled",
    "metrics.extended.enabled",
)
async def test_flujo_integrado_multisimbolo_feed_trader_procesar_vela_ordenes(
    trader_factory, caplog
) -> None:
    """
    Dos símbolos en el mismo trader: cada vela cerrada debe recorrer el consumer,
    actualizar estado, invocar procesar_vela y crear orden vía el stub de orders.
    """
    _enable_flow_logging(caplog)
    caplog.clear()

    symbols = ("BTCUSDT", "ETHUSDT")
    trader = trader_factory(symbols=symbols)
    # Un feed por símbolo: reutilizar el mismo DataFeed cambia el handler del consumer y dispara handler.mismatch.
    feeds = {sym: DataFeed(TIMEFRAME) for sym in symbols}

    n_per = 4
    base_ts = int(time.time()) - 3600
    total_procesar = 0
    for sym in symbols:
        harness = BotFlowHarness(trader, feeds[sym], sym)
        candles = _build_series(base_ts, n_per, symbol=sym)
        total_procesar += await harness.run(candles)
        base_ts += n_per * 300

    await wait_for(lambda: len(trader.orders.created) == len(symbols) * n_per)

    assert total_procesar == len(symbols) * n_per
    assert len(trader.orders.created) == len(symbols) * n_per

    parsed = parse_records(caplog.get_records("call"))
    attempts = _find_events(parsed, "trader_modular", "procesar_vela.attempt")
    assert len(attempts) == len(symbols) * n_per

    for sym in symbols:
        assert feeds[sym]._stats[sym]["processed"] == n_per

    ticks = getattr(trader.supervisor, "ticks", [])
    assert len(ticks) == len(symbols) * n_per
    assert {t[0] for t in ticks} == set(symbols)

    for sym in symbols:
        assert sym in trader.estado
        assert len(trader.estado[sym].buffer) == n_per


@pytest.mark.asyncio(mode="strict")
@pytest.mark.flags(
    "orders.retry_persistencia.enabled",
    "metrics.extended.enabled",
)
async def test_flujo_integrado_warmup_luego_evaluacion_y_orden(trader_factory, caplog) -> None:
    """
    Warmup en el trader (sin procesar_vela) y warmup interno del pipeline: el buffer
    de ``procesar_vela`` solo crece cuando entra al handler, así que la primera orden
    útil aparece tras 2*min_need - 1 velas cerradas (ver ``pipeline_procesar.buf.append``).
    """
    _enable_flow_logging(caplog)
    caplog.clear()

    min_need = 4
    trader = trader_factory(symbols=["BTCUSDT"])
    trader.min_bars = min_need
    trader.config.min_bars = min_need
    trader.config.min_buffer_candles = min_need

    feed = DataFeed(TIMEFRAME)
    harness = BotFlowHarness(trader, feed, "BTCUSDT")

    total_velas = 2 * min_need - 1
    base_ts = int(time.time()) - 7200
    candles = _build_series(base_ts, total_velas)
    procesar_calls = await harness.run(candles)

    await wait_for(lambda: len(trader.orders.created) >= 1)

    assert procesar_calls == min_need
    assert len(trader.orders.created) == 1

    parsed = parse_records(caplog.get_records("call"))
    attempts = _find_events(parsed, "trader_modular", "procesar_vela.attempt")
    warmup_skips = _find_events(parsed, "datafeed", "consumer.skip")
    warmup_skips = [e for e in warmup_skips if e.get("reason") == "warmup"]

    assert len(attempts) == min_need
    assert len(warmup_skips) >= min_need - 1
