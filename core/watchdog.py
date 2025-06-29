from __future__ import annotations

import asyncio
from typing import Awaitable, Callable, Dict
from time import monotonic
from core.utils.logger import configurar_logger
from core.async_utils import dump_tasks_stacktraces

log = configurar_logger("watchdog")

Callback = Callable[[], Awaitable[None]] | Callable[[], None] | None

class Watchdog:
    """Monitoriza la actividad de tareas clave."""

    def __init__(self, timeout: int = 60) -> None:
        self.timeout = timeout
        self._last_ping: Dict[str, float] = {}
        self._callbacks: Dict[str, Callback] = {}
        self._timeouts: Dict[str, float] = {}

    def register(self, name: str, callback: Callback = None, *, timeout: float | None = None) -> None:
        """Registra una nueva tarea a vigilar."""
        try:
            loop = asyncio.get_running_loop()
            ts = loop.time()
        except RuntimeError:
            ts = monotonic()
        self._last_ping[name] = ts
        if callback:
            self._callbacks[name] = callback
        if timeout is not None:
            self._timeouts[name] = timeout

    def ping(self, name: str, *, timeout: float | None = None) -> None:
        """Actualiza la marca de tiempo de ``name``."""
        try:
            loop = asyncio.get_running_loop()
            ts = loop.time()
        except RuntimeError:
            ts = monotonic()
        self._last_ping[name] = ts
        if timeout is not None:
            self._timeouts[name] = timeout

    async def monitor(self) -> None:
        """Verifica periódicamente si las tareas siguen activas."""
        loop = asyncio.get_event_loop()
        while True:
            await asyncio.sleep(self.timeout / 2)
            now = loop.time()
            for name, ts in list(self._last_ping.items()):
                limit = self._timeouts.get(name, self.timeout)
                if now - ts > limit:
                    log.warning(
                        f"⚠️ Tarea '{name}' no responde desde {now - ts:.1f}s"
                    )
                    log.error("\n%s", dump_tasks_stacktraces())
                    cb = self._callbacks.get(name)
                    if cb:
                        try:
                            res = cb()
                            if asyncio.iscoroutine(res):
                                await res
                        except Exception as e:  # pragma: no cover - protección
                            log.error(
                                f"❌ Error ejecutando callback de '{name}': {e}"
                            )
                    self._last_ping[name] = now
