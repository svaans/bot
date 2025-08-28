"""Módulo de supervisión del bot.

Este módulo provee la clase :class:`Supervisor`, que encapsula el estado
necesario para monitorizar la salud del proceso y reiniciar tareas
automáticamente. Un supervisor por defecto es expuesto para mantener
compatibilidad con el API anterior, pero el estado ya no se almacena en
variables globales, permitiendo instanciar supervisores adicionales si es
necesario.
"""

from __future__ import annotations
import asyncio
import logging
import traceback
import threading
from datetime import datetime, timezone
from typing import Awaitable, Callable, Dict


from config.config import INTERVALO_VELAS
from core.notificador import crear_notificador_desde_env
from core.utils.logger import configurar_logger
from core.utils.utils import intervalo_a_segundos


UTC = timezone.utc
log = configurar_logger("supervisor")

class Supervisor:
    """Vigila la salud del bot y gestiona reinicios de tareas."""

    def __init__(self) -> None:
        self.last_alive = datetime.now(UTC)
        self.last_function = "init"
        self.tasks: Dict[str, asyncio.Task] = {}
        self.task_heartbeat: Dict[str, datetime] = {}
        self.data_heartbeat: Dict[str, datetime] = {}
        self.TIMEOUT_SIN_DATOS = max(
            intervalo_a_segundos(INTERVALO_VELAS) * 5, 300
        )
        self.ALERTA_SIN_DATOS_INTERVALO = 300
        self.last_data_alert: Dict[str, datetime] = {}
        self.reinicios_inactividad: Dict[str, int] = {}
        self.notificador = crear_notificador_desde_env()
        self.data_feed_reconnector: Callable[[str], Awaitable[None]] | None = None
        self.main_loop: asyncio.AbstractEventLoop | None = None
        self._watchdog_interval_event = asyncio.Event()
        self._watchdog_interval = 10

    # ------------------------------------------------------------------
    # Métodos de utilidad
    # ------------------------------------------------------------------

    def exception_handler(self, loop: asyncio.AbstractEventLoop, context: dict) -> None:
        """Manejador de excepciones no controladas del loop principal."""

        exc = context.get("exception")
        if exc:
            if isinstance(exc, RecursionError):
                log.critical("Excepcion no controlada en loop: %s", exc)
            else:
                log.critical(
                    "Excepcion no controlada en loop: %s", exc, exc_info=exc
                )
        else:
            log.critical("Error en loop: %s", context.get("message"))

    def tick(self, name: str) -> None:
        """Actualiza la función y marca latido para ``name``."""

        self.last_function = name
        self.last_alive = datetime.now(UTC)
        self.task_heartbeat[name] = self.last_alive

    def tick_data(self, symbol: str, reinicio: bool = False) -> None:
        """Actualiza la marca de tiempo de la última vela recibida."""

        ahora = datetime.now(UTC)
        self.data_heartbeat[symbol] = ahora
        if reinicio:
            log.info("🔄 Reinicio exitoso del stream %s, esperando datos...", symbol)
        if symbol in self.last_data_alert:
            log.info("✅ %s retomó latidos de datos", symbol)
            self.last_data_alert.pop(symbol, None)
        log.debug("tick_data registrado para %s a las %s", symbol, ahora.isoformat())

    def registrar_reinicio_inactividad(self, symbol: str) -> None:
        """Incrementa el contador de reinicios por inactividad para ``symbol``."""

        self.reinicios_inactividad[symbol] = (
            self.reinicios_inactividad.get(symbol, 0) + 1
        )
        log.debug(
            "Reinicio por inactividad registrado para %s (total=%s)",
            symbol,
            self.reinicios_inactividad[symbol],
        )

    def registrar_reconexion_datafeed(
        self, cb: Callable[[str], Awaitable[None]]
    ) -> None:
        """Registra ``cb`` para reiniciar el DataFeed cuando falten datos."""

        self.data_feed_reconnector = cb

    # ------------------------------------------------------------------
    # Tareas de monitorización
    # ------------------------------------------------------------------

    async def heartbeat(self, interval: int = 60) -> None:
        """Emite latidos periódicos para el proceso principal."""

        while True:
            log.info("bot alive | last=%s", self.last_function)
            self.last_alive = datetime.now(UTC)
            await asyncio.sleep(interval)

    def set_watchdog_interval(self, interval: int) -> None:
        """Actualiza el intervalo de verificación del watchdog."""

        self._watchdog_interval = interval
        self._watchdog_interval_event.set()

    async def watchdog(self, timeout: int = 120, check_interval: int = 10) -> None:
        """Valida que el proceso siga activo e imprime trazas si se congela."""

        self._watchdog_interval = check_interval
        while True:
            delta = (datetime.now(UTC) - self.last_alive).total_seconds()
            if delta > timeout:
                log.critical(
                    "⚠️ BOT INACTIVO desde hace %.1f segundos. Ultima funcion: %s",
                    delta,
                    self.last_function,
                )
                for nombre, task in self.tasks.items():
                    try:
                        stack = "\n".join(traceback.format_stack(task.get_stack()))
                        log.critical("Stack de %s:\n%s", nombre, stack)
                    except Exception:
                        pass
            for sym, ts in self.data_heartbeat.items():
                sin_datos = (datetime.now(UTC) - ts).total_seconds()
                log.debug(
                    "Verificando datos de %s: %.1f segundos desde la última vela",
                    sym,
                    sin_datos,
                )
                if sin_datos > self.TIMEOUT_SIN_DATOS:
                    ahora = datetime.now(UTC)
                    ultima = self.last_data_alert.get(sym)
                    if not ultima or (
                        ahora - ultima
                    ).total_seconds() > self.ALERTA_SIN_DATOS_INTERVALO:
                        log.critical(
                            "⚠️ Sin datos de %s desde hace %.1f segundos",
                            sym,
                            sin_datos,
                        )
                        self.last_data_alert[sym] = ahora
                        try:
                            self.notificador.enviar(
                                f"⚠️ Sin datos de {sym} desde hace {sin_datos:.1f} segundos",
                                "CRITICAL",
                            )
                        except Exception:
                            pass
                        task = self.tasks.get(f"stream_{sym}")
                        if self.data_feed_reconnector:
                            try:
                                log.warning(
                                    "Solicitando reinicio de DataFeed para %s", sym
                                )
                                await self.data_feed_reconnector(sym)
                            except Exception as e:
                                log.error(
                                    "No se pudo solicitar reinicio de DataFeed para %s: %s",
                                    sym,
                                    e,
                                )
                        elif task:
                            try:
                                log.warning(
                                    "Cancelando stream %s desde watchdog", sym
                                )
                                task.cancel()
                                log.debug(
                                    "Stream %s cancelado; el monitor debería reiniciarlo",
                                    sym,
                                )
                            except Exception as e:
                                log.debug(
                                    "No se pudo cancelar stream %s: %s", sym, e
                                )

            
            try:
                await asyncio.wait_for(
                    self._watchdog_interval_event.wait(),
                    timeout=self._watchdog_interval,
                )
            except asyncio.TimeoutError:
                pass
            self._watchdog_interval_event.clear()


# ------------------------------------------------------------------
    # Gestión de tareas supervisadas
    # ------------------------------------------------------------------

    def start_supervision(self) -> None:
        """Configura el loop y lanza las tareas de monitorización."""

        loop = asyncio.get_running_loop()
        self.main_loop = loop
        loop.set_exception_handler(self.exception_handler)

        asyncio.create_task(self.heartbeat(), name="heartbeat")
        asyncio.create_task(self.watchdog(), name="watchdog")

    
        def thread_excepthook(args: threading.ExceptHookArgs) -> None:
            log.critical(
                "Excepcion en hilo %s: %s",
                args.thread.name,
                args.exc_value,
                exc_info=(args.exc_type, args.exc_value, args.exc_traceback),
            )
        threading.excepthook = thread_excepthook

    async def _restartable_runner(
        self,
        coro_factory: Callable[..., Awaitable],
        task_name: str,
        delay: int = 5,
        max_restarts: int | None = None,
    ) -> None:
        """Ejecuta ``coro_factory`` reiniciándolo ante fallos o finalización."""

        restarts = 0
        while True:
            self.tick(task_name)
            try:
                result = coro_factory()
                if asyncio.iscoroutine(result):
                    await result
                break  # fin normal
            except asyncio.CancelledError:
                log.info("Tarea %s cancelada", task_name)
                raise
            except Exception as e:  # pragma: no cover - log crítico
                log.error(
                    "⚠️ Error en %s: %r. Reiniciando en %ss",
                    task_name,
                    e,
                    delay,
                    exc_info=True,
                )
                if max_restarts is not None and restarts >= max_restarts:
                    log.error(
                        "❌ %s alcanzó el límite de reinicios (%s)",
                        task_name,
                        max_restarts,
                    )
                    break
                log.warning(
                    "⏹️ %s finalizó; reiniciando en %ss", task_name, delay
                )
                await asyncio.sleep(delay)
                restarts += 1
                continue

    def supervised_task(
        self,
        coro_factory: Callable[..., Awaitable],
        name: str | None = None,
        delay: int = 5,
        max_restarts: int | None = None,
    ) -> asyncio.Task:
        """Crea una tarea supervisada que se reinicia automáticamente."""

        task_name = name or getattr(coro_factory, "__name__", "task")
        task = asyncio.create_task(
            self._restartable_runner(coro_factory, task_name, delay, max_restarts),
            name=task_name,
        )
        self.tasks[task_name] = task
        return task


# ----------------------------------------------------------------------
#  API de compatibilidad
# ----------------------------------------------------------------------

_default_supervisor = Supervisor()

start_supervision = _default_supervisor.start_supervision
supervised_task = _default_supervisor.supervised_task
tick = _default_supervisor.tick
tick_data = _default_supervisor.tick_data
registrar_reinicio_inactividad = _default_supervisor.registrar_reinicio_inactividad
registrar_reconexion_datafeed = _default_supervisor.registrar_reconexion_datafeed
set_watchdog_interval = _default_supervisor.set_watchdog_interval

tasks = _default_supervisor.tasks
task_heartbeat = _default_supervisor.task_heartbeat
data_heartbeat = _default_supervisor.data_heartbeat
reinicios_inactividad = _default_supervisor.reinicios_inactividad


def get_last_alive() -> datetime:
    return _default_supervisor.last_alive


def __getattr__(name: str):  # pragma: no cover - acceso dinámico
    return getattr(_default_supervisor, name)

__all__ = [
    "Supervisor",
    "start_supervision",
    "supervised_task",
    "tick",
    "tick_data",
    "registrar_reinicio_inactividad",
    "registrar_reconexion_datafeed",
    "set_watchdog_interval",
    "tasks",
    "task_heartbeat",
    "data_heartbeat",
    "reinicios_inactividad",
    "get_last_alive",
]
