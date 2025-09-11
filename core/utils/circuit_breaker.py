from __future__ import annotations
import asyncio
from collections import deque
from typing import Callable, Awaitable, Deque
from core.utils.logger import configurar_logger
from observabilidad import metrics as obs_metrics

log = configurar_logger('circuit_breaker')


class CircuitBreaker:
    """Registra reinicios y dispara un corte si supera el umbral."""

    def __init__(self, max_restarts: int, window_s: int) -> None:
        self.max_restarts = max_restarts
        self.window_s = window_s
        self.restart_times: Deque[float] = deque()

    def register_restart(self, now: float) -> bool:
        """Registra un reinicio y devuelve True si debe activarse el corte."""
        while self.restart_times and now - self.restart_times[0] > self.window_s:
            self.restart_times.popleft()
        self.restart_times.append(now)
        return len(self.restart_times) > self.max_restarts


def _set_feed_stopped() -> None:
    try:
        obs_metrics.FEED_STOPPED.set(1)
    except Exception:
        pass


async def heartbeat(
    task_factory: Callable[[], Awaitable[None]],
    breaker: CircuitBreaker,
    retry_delay: float = 5.0,
) -> None:
    """Ejecuta ``task_factory`` reiniciando al fallo hasta que el breaker se active."""
    while True:
        task = asyncio.create_task(task_factory())
        try:
            await task
            log.info('Tarea finalizada limpiamente')
            break
        except Exception as exc:  # pragma: no cover - log path
            log.exception('Tarea falló, reiniciando: %s', exc)
            loop = asyncio.get_event_loop()
            if breaker.register_restart(loop.time()):
                log.error('Circuit breaker activado; deteniendo feed')
                _set_feed_stopped()
                break
            await asyncio.sleep(retry_delay)