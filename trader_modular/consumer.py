"""Consumidor asincrónico para streams WebSocket.

Este módulo separa la lectura del WebSocket del procesamiento de
mensajes utilizando una ``asyncio.Queue`` con tamaño máximo configurable.

Además mide la latencia entre la lectura y el procesamiento de eventos,
y expone métricas Prometheus para el ritmo de consumo.
"""

from __future__ import annotations

import asyncio
import time
from typing import Any, Awaitable, Callable

from core.utils.metrics_compat import Counter, Gauge

# Métricas -----------------------------------------------------------------

# Número total de eventos procesados. La tasa de eventos/segundo puede
# obtenerse consultando ``rate(consumer_rate[1m])`` en Prometheus.
CONSUMER_RATE: Counter = Counter(
    "consumer_rate", "Eventos procesados por el consumer", ["stream"]
)

# Latencia observada desde que el mensaje es leído hasta que es procesado.
QUEUE_LATENCY: Gauge = Gauge(
    "consumer_queue_latency_seconds",
    "Segundos transcurridos desde la lectura hasta el procesamiento",
    ["stream"],
)


# Funciones ----------------------------------------------------------------

async def websocket_reader(websocket: Any, queue: asyncio.Queue[tuple[float, Any]]) -> None:
    """Lee mensajes de ``websocket`` y los encola con marca temporal.

    ``websocket`` debe ser un objeto asincrónico iterable (``async for``).
    Cada mensaje se almacena como ``(timestamp, payload)``. Si la cola está
    llena, el mensaje se descarta silenciosamente para evitar bloqueos.
    """

    async for payload in websocket:
        item = (time.monotonic(), payload)
        try:
            queue.put_nowait(item)
        except asyncio.QueueFull:
            # Se descarta el mensaje para mantener el throughput.
            continue


async def queue_processor(
    queue: asyncio.Queue[tuple[float, Any]],
    handler: Callable[[Any], Awaitable[None]],
    *,
    stream: str = "default",
    timeout: float = 5.0,
) -> None:
    """Procesa eventos desde ``queue`` usando ``handler``.

    Utiliza ``asyncio.wait_for`` para detectar bloqueos en ``queue.get`` y
    permite registrar métricas de latencia. La función nunca termina a menos
    que se cancele la tarea que la ejecuta.

    Args:
        queue: Cola de la que se extraen los eventos.
        handler: Corutina que procesa cada evento.
        stream: Etiqueta para las métricas de Prometheus.
        timeout: Tiempo máximo en segundos esperando un nuevo evento.
    """

    while True:
        try:
            ts, payload = await asyncio.wait_for(queue.get(), timeout=timeout)
        except asyncio.TimeoutError:
            # No llegaron eventos en ``timeout`` segundos; se continúa para
            # permitir que la tarea de lectura siga viva.
            continue

        start = time.monotonic()
        try:
            await handler(payload)
        finally:
            queue.task_done()

        latency = start - ts
        QUEUE_LATENCY.labels(stream=stream).set(latency)
        CONSUMER_RATE.labels(stream=stream).inc()


async def start_consumer(
    websocket: Any,
    handler: Callable[[Any], Awaitable[None]],
    *,
    maxsize: int = 100,
    stream: str = "default",
    timeout: float = 5.0,
) -> tuple[asyncio.Task[Any], asyncio.Task[Any]]:
    """Inicia tareas de lectura y procesamiento para un ``websocket``.

    Devuelve una tupla con las tareas ``(reader_task, processor_task)`` para
    que el llamador pueda gestionarlas o cancelarlas según sea necesario.
    """

    queue: asyncio.Queue[tuple[float, Any]] = asyncio.Queue(maxsize=maxsize)
    reader = asyncio.create_task(websocket_reader(websocket, queue))
    processor = asyncio.create_task(
        queue_processor(queue, handler, stream=stream, timeout=timeout)
    )
    return reader, processor
