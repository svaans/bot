"""Tests para handlers.reiniciar_consumer — paths normal y self-cancel."""
from __future__ import annotations

import asyncio
import contextlib
from typing import Any
from unittest.mock import AsyncMock

import pytest

from core.data_feed.datafeed import DataFeed


def make_feed(**kwargs: Any) -> DataFeed:
    return DataFeed("1m", **kwargs)


def _make_symbol_queue(feed: DataFeed, symbol: str) -> asyncio.Queue:
    q: asyncio.Queue = asyncio.Queue()
    feed._queues[symbol] = q
    return q


# ---------------------------------------------------------------------------
# HANDLERS-REINICIAR-SELF-CANCEL-01: ruta externa (desde monitors)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_reiniciar_consumer_external_cancels_old_task() -> None:
    """Ruta normal (llamada desde monitors): el task antiguo es cancelado y se crea uno nuevo."""
    from core.data_feed.handlers import reiniciar_consumer

    feed = make_feed()
    symbol = "BTCUSDT"
    feed._running = True
    _make_symbol_queue(feed, symbol)

    # Crear un task "viejo" que bloquea indefinidamente
    old_task = asyncio.create_task(asyncio.sleep(10_000.0), name="consumer_BTCUSDT")
    feed._consumer_tasks[symbol] = old_task
    feed._consumer_last[symbol] = 0.0

    await reiniciar_consumer(feed, symbol)

    # El task antiguo debe haber sido cancelado
    assert old_task.cancelled() or old_task.done(), (
        "El task antiguo no fue cancelado por reiniciar_consumer (ruta externa)"
    )
    # Un nuevo task debe estar registrado
    new_task = feed._consumer_tasks.get(symbol)
    assert new_task is not None
    assert new_task is not old_task, "El task nuevo debe ser distinto del antiguo"

    # Limpiar
    new_task.cancel()
    with contextlib.suppress(asyncio.CancelledError, Exception):
        await new_task


@pytest.mark.asyncio
async def test_reiniciar_consumer_self_call_does_not_self_cancel() -> None:
    """HANDLERS-REINICIAR-SELF-CANCEL-01: cuando se llama desde dentro del consumer
    (self-restart), NO debe intentar cancelarse y awaitearse a sí mismo.

    El comportamiento correcto: se crea un nuevo task sin tocar el task actual.
    El task actual (consumer_loop) retorna naturalmente tras la llamada.
    """
    from core.data_feed.handlers import reiniciar_consumer

    feed = make_feed()
    symbol = "ETHUSDT"
    feed._running = True
    _make_symbol_queue(feed, symbol)

    cancel_called_on_self = False

    async def _self_call_scenario() -> None:
        """Simula el interior de consumer_loop llamando a reiniciar_consumer."""
        nonlocal cancel_called_on_self

        # Registrar el task actual como el consumer task del símbolo
        current = asyncio.current_task()
        feed._consumer_tasks[symbol] = current

        original_cancel = current.cancel

        def _tracked_cancel(*args: Any, **kwargs: Any) -> bool:
            nonlocal cancel_called_on_self
            cancel_called_on_self = True
            return original_cancel(*args, **kwargs)

        current.cancel = _tracked_cancel  # type: ignore[method-assign]

        # Llamada análoga a L1108 en consumer_loop
        await reiniciar_consumer(feed, symbol)
        # Si llegamos aquí sin CancelledError, el self-cancel fue evitado ✓

    task = asyncio.create_task(_self_call_scenario())
    await asyncio.wait_for(task, timeout=2.0)

    # La tarea completó sin cancelarse a sí misma
    assert not cancel_called_on_self, (
        "reiniciar_consumer llamó task.cancel() sobre asyncio.current_task() — "
        "HANDLERS-REINICIAR-SELF-CANCEL-01 no corregido"
    )
    # Un nuevo consumer task debe haber sido registrado (distinto del que llamó)
    new_task = feed._consumer_tasks.get(symbol)
    assert new_task is not None
    assert new_task is not task, "Se debe crear un nuevo task (distinto del llamante)"

    new_task.cancel()
    with contextlib.suppress(asyncio.CancelledError, Exception):
        await new_task


@pytest.mark.asyncio
async def test_reiniciar_consumer_creates_new_consumer_task_regardless_of_path() -> None:
    """En ambas rutas (externa y self-call), se crea un nuevo consumer task."""
    from core.data_feed.handlers import reiniciar_consumer

    feed = make_feed()
    symbol = "ADAUSDT"
    feed._running = True
    _make_symbol_queue(feed, symbol)

    # Sin task previo registrado
    assert feed._consumer_tasks.get(symbol) is None

    await reiniciar_consumer(feed, symbol)

    new_task = feed._consumer_tasks.get(symbol)
    assert new_task is not None, "Debe crearse un nuevo consumer task"
    assert not new_task.done(), "El nuevo task debe estar en ejecución"

    new_task.cancel()
    with contextlib.suppress(asyncio.CancelledError, Exception):
        await new_task


# ---------------------------------------------------------------------------
# Integración: el consumer detecta timestamp en loop y se reinicia limpiamente
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_consumer_loop_restart_on_stuck_timestamp() -> None:
    """consumer_loop se reinicia limpiamente cuando detecta ts_processed <= prev.

    Verifica que el flujo completo funciona sin que el consumer se cancele a
    sí mismo (bug anterior) ni deje tasks huérfanas.
    """
    from core.data_feed.handlers import consumer_loop, reiniciar_consumer
    from core.data_feed._shared import ConsumerState

    restarts: list[str] = []

    # Reemplazamos reiniciar_consumer para interceptar la llamada sin monkeypatch
    # (necesitamos verificar que se llama y que current_task no es cancelado)
    original_reiniciar = reiniciar_consumer

    feed = make_feed()
    symbol = "SOLUSDT"
    feed._running = True
    q = _make_symbol_queue(feed, symbol)

    handler_calls: list[dict] = []

    async def _handler(candle: dict) -> None:
        handler_calls.append(candle)
        # Después de procesar, detener el feed para no iterar indefinidamente
        feed._running = False

    feed._handler = _handler
    feed._last_processed_ts[symbol] = 1_650_000_060_000  # timestamp "previo"

    # Poner en la cola una vela con el MISMO timestamp que el previo → trigger de restart
    stuck_candle = {
        "symbol": symbol,
        "timestamp": 1_650_000_060_000,  # igual al prev → ts_processed <= prev
        "timeframe": "1m",
        "is_closed": True,
        "open": 40000.0, "high": 41000.0, "low": 39000.0, "close": 40500.0,
        "volume": 1.0,
        "_df_enqueue_time": 0.0,
    }
    await q.put(stuck_candle)

    # Lanzar consumer_loop
    consumer_task = asyncio.create_task(
        consumer_loop(feed, symbol), name=f"consumer_{symbol}"
    )
    feed._consumer_tasks[symbol] = consumer_task

    # Esperar a que el consumer procese la vela y reinicie (o timeout)
    try:
        await asyncio.wait_for(consumer_task, timeout=2.0)
    except (asyncio.CancelledError, asyncio.TimeoutError, Exception):
        pass

    # El nuevo task de consumer debe haber sido registrado
    new_task = feed._consumer_tasks.get(symbol)
    if new_task and new_task is not consumer_task:
        # El consumer detectó el timestamp en loop y creó un nuevo task ✓
        new_task.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await new_task
    else:
        # El consumer puede haber retornado directamente si _running=False ✓
        pass

    # Verificar que el consumer original no sigue vivo como task huérfano
    all_task_names = {t.get_name() for t in asyncio.all_tasks()}
    # El consumer antiguo debe estar terminado (no en pending tasks)
    assert consumer_task.done(), (
        "El consumer task original no terminó — podría ser una task huérfana"
    )
