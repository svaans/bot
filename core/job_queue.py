from __future__ import annotations

import asyncio
import logging
import os
from dataclasses import dataclass, field
from typing import Any, Callable, Sequence
from time import monotonic

from core.registro_metrico import registro_metrico

from core.utils.utils import configurar_logger

log = configurar_logger("job_queue")

@dataclass
class Job:
    """Representa un trabajo de verificación de vela.

    Los jobs de *prioridad 0* corresponden a cierres o salidas críticas y se
    deben ejecutar antes que las nuevas entradas (*prioridad 1*).  El timestamp
    ``enqueued_at`` mantiene el orden FIFO dentro de la misma prioridad.
    """

    priority: int
    kind: str  # "entry" o "exit"
    symbol: str
    df: Any
    retries: int = 0
    enqueued_at: float = field(default_factory=monotonic)

async def enqueue_job(
    queue: asyncio.PriorityQueue,
    job: Job,
    *,
    drop_policy: str = "drop_oldest",
) -> None:
    """Intenta encolar ``job`` aplicando la política de descarte.

    Los jobs de prioridad 0 representan eventos críticos como cierres de
    posiciones, mientras que los de prioridad 1 son para nuevas entradas.
    """
    try:
        queue.put_nowait((job.priority, job.enqueued_at, job))
    except asyncio.QueueFull:
        if drop_policy == "drop_oldest":
            try:
                queue.get_nowait()
                queue.task_done()
                queue.put_nowait((job.priority, job.enqueued_at, job))
                log.warning("⏳ Cola llena. Descartando tarea más antigua")
            except asyncio.QueueFull:
                log.warning("⏳ Cola llena. Tarea descartada")
        else:
            log.warning("⏳ Cola llena. Tarea descartada")

async def worker(
    name: str,
    trader,
    queue: asyncio.PriorityQueue,
    *,
    timeout: float,
    drop_policy: str,
    max_retries: int = 3,
) -> None:
    """Consume trabajos de ``queue`` procesándolos con ``trader``.

    Se aplica un timeout al esperar por nuevos jobs para evitar bloqueos
    indefinidos en caso de que se detenga el flujo de velas.
    """
    log.info("🟢 Worker iniciado")
    while True:
        try:
            try:
                prio, ts, job = await asyncio.wait_for(queue.get(), timeout=60)
            except asyncio.TimeoutError:
                log.warning("⚠️ Worker bloqueado, sin recibir jobs en 60s")
                continue
        except asyncio.CancelledError:
            log.info("⚠️ Worker cancelado de forma segura")
            raise
        if job.kind == "dummy":
            log.info("🔹 Dummy job recibido")
            queue.task_done()
            if "PYTEST_CURRENT_TEST" not in os.environ:
                registro_metrico.registrar(
                    "job_done",
                    {"kind": job.kind, "symbol": job.symbol},
                )
            continue
        
        log.info(f"🚀 Ejecutando job con prioridad={prio} en timestamp={ts}")
        wait_ms = (monotonic() - job.enqueued_at) * 1000.0
        if "PYTEST_CURRENT_TEST" not in os.environ:
            registro_metrico.registrar(
                "job_wait",
                {"kind": job.kind, "symbol": job.symbol, "ms": wait_ms},
            )
        try:
            if job.kind == "exit":
                coro = trader._verificar_salidas(job.symbol, job.df)
            else:
                coro = trader.evaluar_condiciones_entrada(job.symbol, job.df)
            await asyncio.wait_for(coro, timeout)
            if job.kind == "entry":
                trader.actualizar_fraccion_kelly()
            log.info(f"✅ Job completado: {job}")
        except asyncio.TimeoutError:
            log.warning("⚠️ Timeout en trabajo")
            if job.retries < max_retries:
                job.retries += 1
                log.info("🔁 Reintento")
                await enqueue_job(queue, job, drop_policy=drop_policy)
        except asyncio.CancelledError:
            log.info("⚠️ Worker cancelado de forma segura")
            raise
        except Exception as exc:  # noqa: BLE001
            log.error("💥 Worker finalizado por error", exc_info=exc)
            if job.retries < max_retries:
                job.retries += 1
                log.info("🔁 Reintento")
                await enqueue_job(queue, job, drop_policy=drop_policy)
        finally:
            queue.task_done()
            if "PYTEST_CURRENT_TEST" not in os.environ:
                registro_metrico.registrar(
                    "job_done",
                    {"kind": job.kind, "symbol": job.symbol},
                )
    log.info("💥 Worker finalizado por error")

async def queue_watchdog(
    queue: asyncio.PriorityQueue,
    workers: Sequence[asyncio.Task],
    restart: Callable[[int], asyncio.Task],
    *,
    warn_threshold: int,
    interval: float = 5.0,
) -> None:
    """Vigila la cola, reinicia workers y encola dummy jobs si hay inactividad."""
    idx = len(workers)
    last_job = monotonic()
    prev_size = queue.qsize()
    while True:
        await asyncio.sleep(interval)
        size = queue.qsize()
        if size != prev_size:
            last_job = monotonic()
            prev_size = size
        elif size == 0 and monotonic() - last_job > 120:
            log.info("🌀 Encolando dummy job por inactividad")
            await enqueue_job(
                queue,
                Job(priority=10, kind="dummy", symbol="", df=None),
                drop_policy="drop_oldest",
            )
            last_job = monotonic()
            prev_size = queue.qsize()
        if size > warn_threshold:
            log.warning(f"⏳ Cola en espera ({size})")
        registro_metrico.registrar("job_queue", {"size": size})
        for i, w in enumerate(list(workers)):
            if w.done():
                exc = w.exception()
                if exc:
                    log.error("💥 Worker finalizado por error", exc_info=exc)
                else:
                    log.error("💥 Worker finalizado")
                workers[i] = restart(i)