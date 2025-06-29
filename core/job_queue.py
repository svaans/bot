from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Any, Callable, Sequence

from core.utils.utils import configurar_logger

log = configurar_logger("job_queue")

@dataclass
class Job:
    """Representa un trabajo de verificación de vela."""

    kind: str  # "entry" o "exit"
    symbol: str
    df: Any
    retries: int = 0

async def enqueue_job(
    queue: asyncio.Queue,
    job: Job,
    *,
    drop_policy: str = "drop_oldest",
) -> None:
    """Intenta encolar ``job`` aplicando la política de descarte."""
    try:
        queue.put_nowait(job)
    except asyncio.QueueFull:
        if drop_policy == "drop_oldest":
            try:
                queue.get_nowait()
                queue.task_done()
                queue.put_nowait(job)
                log.warning("⏳ Cola llena. Descartando tarea más antigua")
            except asyncio.QueueFull:
                log.warning("⏳ Cola llena. Tarea descartada")
        else:
            log.warning("⏳ Cola llena. Tarea descartada")

async def worker(
    name: str,
    trader,
    queue: asyncio.Queue,
    *,
    timeout: float,
    drop_policy: str,
    max_retries: int = 3,
) -> None:
    """Consume trabajos de ``queue`` procesándolos con ``trader``."""
    log.info("🟢 Worker iniciado")
    while True:
        try:
            job: Job = await queue.get()
        except asyncio.CancelledError:
            break
        try:
            if job.kind == "exit":
                coro = trader._verificar_salidas(job.symbol, job.df)
            else:
                coro = trader.evaluar_condiciones_entrada(job.symbol, job.df)
            await asyncio.wait_for(coro, timeout)
            if job.kind == "entry":
                trader.actualizar_fraccion_kelly()
            log.info("✅ Trabajo completado")
        except asyncio.TimeoutError:
            log.warning("⚠️ Timeout en trabajo")
            if job.retries < max_retries:
                job.retries += 1
                log.info("🔁 Reintento")
                await enqueue_job(queue, job, drop_policy=drop_policy)
        except Exception as exc:  # noqa: BLE001
            log.error("💥 Worker finalizado por error", exc_info=exc)
            if job.retries < max_retries:
                job.retries += 1
                log.info("🔁 Reintento")
                await enqueue_job(queue, job, drop_policy=drop_policy)
        finally:
            queue.task_done()
    log.info("💥 Worker finalizado por error")

async def queue_watchdog(
    queue: asyncio.Queue,
    workers: Sequence[asyncio.Task],
    restart: Callable[[int], asyncio.Task],
    *,
    warn_threshold: int,
    interval: float = 5.0,
) -> None:
    """Vigila la cola y reinicia workers terminados."""
    idx = len(workers)
    while True:
        await asyncio.sleep(interval)
        size = queue.qsize()
        if size > warn_threshold:
            log.warning(f"⏳ Cola en espera ({size})")
        for i, w in enumerate(list(workers)):
            if w.done():
                log.error("💥 Worker finalizado por error")
                workers[i] = restart(i)