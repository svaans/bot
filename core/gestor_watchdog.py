"""
Gestor de Watchdog — envoltorio alrededor de SupervisorLite.

Objetivo
--------
- Centralizar la configuración de timeouts, reinicios y circuit breaker.
- Publicar eventos sencillos (dicts) vía on_event.
- Mantener código desacoplado del núcleo TraderLite.

API
---
class GestorWatchdog:
    - iniciar()
    - detener()
    - latido(nombre, causa=None)
    - registrar_tarea(nombre, factory, *, expected_interval=None)
    - reportar_datos(symbol, reinicio=False)
"""
from __future__ import annotations
import asyncio
from typing import Any, Awaitable, Callable, Optional

try:
    from supervisor import Supervisor # pragma: no cover
except Exception:  # pragma: no cover
    from supervisor import Supervisor


class GestorWatchdog:
    def __init__(self, *, on_event: Optional[Callable[[str, dict], None]] = None,
                 watchdog_timeout: int = 120, check_interval: int = 10) -> None:
        self.supervisor = Supervisor(on_event=on_event,
                                         watchdog_timeout=watchdog_timeout,
                                         watchdog_check_interval=check_interval)
        self._running = False

    def iniciar(self) -> None:
        if not self._running:
            self.supervisor.start_supervision()
            self._running = True

    async def detener(self) -> None:
        if self._running:
            await self.supervisor.shutdown()
            self._running = False

    # -------- latidos --------
    def latido(self, nombre: str, causa: str | None = None) -> None:
        self.supervisor.beat(nombre, causa)

    def reportar_datos(self, symbol: str, reinicio: bool = False) -> None:
        self.supervisor.tick_data(symbol, reinicio)

    # -------- tareas --------
    def registrar_tarea(
        self,
        nombre: str,
        factory: Callable[..., Awaitable[Any]],
        *,
        expected_interval: int | None = None,
    ) -> asyncio.Task:
        return self.supervisor.supervised_task(factory, name=nombre, expected_interval=expected_interval)

    def ajustar_intervalo(self, interval: int) -> None:
        self.supervisor.set_watchdog_interval(interval)
