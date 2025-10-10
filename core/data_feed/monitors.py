from __future__ import annotations

"""Monitores y tareas de mantenimiento del ``DataFeed``."""

import asyncio
import contextlib
import time
from typing import Callable

from core.metrics import CONSUMER_LAG_SECONDS, PRODUCER_LAG_SECONDS, QUEUE_SIZE

from ._shared import ConsumerState, log
from . import events


def _safe_set(metric: Callable[..., object], *labels: str, value: float) -> None:
    """Actualiza una métrica ignorando errores de registro."""

    try:
        metric(*labels).set(value)  # type: ignore[call-arg]
    except Exception:
        # En entornos de prueba las métricas pueden no estar inicializadas.
        pass


def _set_queue_size(symbol: str, size: int) -> None:
    _safe_set(QUEUE_SIZE.labels, symbol, value=float(size))


def _set_producer_lag(symbol: str, lag: float) -> None:
    _safe_set(PRODUCER_LAG_SECONDS.labels, symbol, value=lag)


def _set_consumer_lag(symbol: str, lag: float) -> None:
    _safe_set(CONSUMER_LAG_SECONDS.labels, symbol, value=lag)


async def monitor_inactividad(feed: "DataFeed") -> None:
    """Watchdog que reinicia streams ante inactividad prolongada."""

    try:
        while feed._running:
            await asyncio.sleep(feed.monitor_interval)
            ahora = time.monotonic()
            for symbol, last in list(feed._last_monotonic.items()):
                if last is None:
                    continue
                _set_producer_lag(symbol, max(0.0, ahora - last))
                queue = feed._queues.get(symbol)
                if queue is not None:
                    _set_queue_size(symbol, queue.qsize())
                if ahora - last > feed.tiempo_inactividad:
                    log.warning("%s: stream inactivo; reiniciando…", symbol)
                    events.emit_event(feed, "ws_inactividad_watchdog", {"symbol": symbol})
                    await reiniciar_stream(feed, symbol)
    except asyncio.CancelledError:
        return
    except Exception:
        log.exception("monitor_inactividad: error")


async def monitor_consumers(feed: "DataFeed", timeout: float | None = None) -> None:
    """Supervisa el avance de los consumidores y reinicia los que se estancan."""

    try:
        while feed._running:
            await asyncio.sleep(feed.monitor_interval)
            ahora = time.monotonic()
            current_timeout = timeout if timeout is not None else feed.consumer_timeout
            if current_timeout <= 0:
                continue
            for symbol, last in list(feed._consumer_last.items()):
                if last is not None:
                    _set_consumer_lag(symbol, max(0.0, ahora - last))
                tarea = feed._consumer_tasks.get(symbol)
                if tarea is None or tarea.done():
                    events.set_consumer_state(feed, symbol, ConsumerState.STALLED)
                    log.warning("%s: consumer detenido; reiniciando…", symbol)
                    events.emit_event(feed, "consumer_stalled", {"symbol": symbol})
                    await reiniciar_consumer(feed, symbol)
                    feed._consumer_last[symbol] = time.monotonic()
                    continue
                if ahora - last <= current_timeout:
                    continue
                queue = feed._queues.get(symbol)
                if queue is None:
                    continue
                _set_queue_size(symbol, queue.qsize())
                if queue.empty():
                    stream_task = feed._tasks.get(symbol)
                    stream_alive = stream_task is not None and not stream_task.done()
                    last_producer = feed._last_monotonic.get(symbol)
                    producer_idle = (
                        last_producer is None
                        or ahora - last_producer > max(feed.tiempo_inactividad, current_timeout)
                    )
                    if not stream_alive or producer_idle:
                        events.set_consumer_state(feed, symbol, ConsumerState.STALLED)
                        log.warning("%s: consumer sin datos; reiniciando stream…", symbol)
                        events.emit_event(feed, "consumer_idle_no_data", {"symbol": symbol})
                        await reiniciar_stream(feed, symbol)
                        feed._consumer_last[symbol] = time.monotonic()
                    continue
                events.set_consumer_state(feed, symbol, ConsumerState.STALLED)
                log.warning("%s: consumer sin progreso; reiniciando…", symbol)
                events.emit_event(feed, "consumer_no_progress", {"symbol": symbol})
                await reiniciar_consumer(feed, symbol)
                feed._consumer_last[symbol] = time.monotonic()
    except asyncio.CancelledError:
        return
    except Exception:
        log.exception("monitor_consumers: error")


async def reiniciar_stream(feed: "DataFeed", symbol: str) -> None:
    """Recrea la tarea del stream correspondiente al símbolo."""

    if feed._combined:
        task = next(iter(feed._tasks.values()), None)
        if task and not task.done():
            task.cancel()
            with contextlib.suppress(Exception):
                await asyncio.wait_for(task, timeout=feed.cancel_timeout)
        nueva = asyncio.create_task(feed._stream_combinado(feed._symbols), name="stream_combinado")
        for sym in feed._symbols:
            feed._tasks[sym] = nueva
    else:
        task = feed._tasks.get(symbol)
        if task and not task.done():
            task.cancel()
            with contextlib.suppress(Exception):
                await asyncio.wait_for(task, timeout=feed.cancel_timeout)
        feed._tasks[symbol] = asyncio.create_task(
            feed._stream_simple(symbol), name=f"stream_{symbol}"
        )


async def reiniciar_consumer(feed: "DataFeed", symbol: str) -> None:
    """Proxy hacia :func:`handlers.reiniciar_consumer` para evitar ciclos."""

    from .handlers import reiniciar_consumer as _reiniciar_consumer

    await _reiniciar_consumer(feed, symbol)


from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover
    from .datafeed import DataFeed
