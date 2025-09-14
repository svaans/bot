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
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone
from typing import Awaitable, Callable, Deque, Dict

import os


from config.config import INTERVALO_VELAS, TIMEOUT_SIN_DATOS_FACTOR
from core.notification_manager import crear_notification_manager_desde_env
from core.utils.logger import configurar_logger
from core.utils.utils import intervalo_a_segundos
from core.metrics import registrar_watchdog_restart
from core.registro_metrico import registro_metrico
from core.utils.backoff import backoff_sleep, calcular_backoff
from observabilidad import metrics as obs_metrics


UTC = timezone.utc
log = configurar_logger("supervisor")

class Supervisor:
    """Vigila la salud del bot y gestiona reinicios de tareas."""

    def __init__(self) -> None:
        self.last_alive = datetime.now(UTC)
        self.last_function = "init"
        self.tasks: Dict[str, asyncio.Task] = {}
        self.task_factories: Dict[str, Callable[..., Awaitable]] = {}
        self.task_heartbeat: Dict[str, datetime] = {}
        self.task_intervals: Dict[str, Deque[float]] = defaultdict(lambda: deque(maxlen=100))
        self.task_expected_interval: Dict[str, int] = {}
        self.task_cause: Dict[str, str] = {}
        self.task_cooldown: Dict[str, datetime] = {}
        self.task_backoff: Dict[str, int] = defaultdict(int)
        self.task_restart_times: Dict[str, Deque[datetime]] = defaultdict(deque)
        self.data_heartbeat: Dict[str, datetime] = {}
        self.stop_event = asyncio.Event()
        self.reinicios_watchdog: Dict[str, int] = {}
        self.TIMEOUT_SIN_DATOS = max(
            intervalo_a_segundos(INTERVALO_VELAS) * TIMEOUT_SIN_DATOS_FACTOR, 300
        )
        self.ALERTA_SIN_DATOS_INTERVALO = 300
        self.last_data_alert: Dict[str, datetime] = {}
        self.reinicios_inactividad: Dict[str, int] = {}
        self.inactive_symbols: set[str] = set()
        self.notificador = crear_notification_manager_desde_env()
        self.data_feed_reconnector: Callable[[str], Awaitable[None]] | None = None
        self.main_loop: asyncio.AbstractEventLoop | None = None
        self._watchdog_interval_event = asyncio.Event()
        self._watchdog_interval = 10
        self.PING_RTT_SAMPLES = 5
        self.PING_RTT_THRESHOLD_MS = 500
        self.ping_rtts: Dict[str, Deque[float]] = defaultdict(
            lambda: deque(maxlen=self.PING_RTT_SAMPLES)
        )
        self.last_ping_alert: Dict[str, datetime] = {}
        self.reconexiones_ws: Dict[str, int] = {}
        self.reinicios_consumer: Dict[str, int] = {}
        self.inactividad_detectada: Dict[str, int] = {}
        self.cooldown_sec = float(os.getenv("SUPERVISOR_COOLDOWN_SEC", "15"))
        self.close_timeout = float(os.getenv("WS_CLOSE_TIMEOUT", "5"))

    # ------------------------------------------------------------------
    # Métodos de utilidad
    # ------------------------------------------------------------------

    def _now(self) -> datetime:
        return datetime.now(UTC)

    def exception_handler(self, loop: asyncio.AbstractEventLoop, context: dict) -> None:
        """Manejador de excepciones no controladas del loop principal."""

        exc = context.get("exception")
        if exc:
            if isinstance(exc, StopIteration):
                log.debug("StopIteration ignorado en loop")
                return
            if isinstance(exc, RecursionError):
                log.critical("Excepcion no controlada en loop: %s", exc)
            else:
                log.critical(
                    "Excepcion no controlada en loop: %s", exc, exc_info=exc
                )
        else:
            log.critical("Error en loop: %s", context.get("message"))

    def beat(self, name: str, cause: str | None = None) -> None:
        """Registra un latido para ``name`` y opcionalmente su ``cause``."""

        now = self._now()
        last = self.task_heartbeat.get(name)
        if last:
            interval = (now - last).total_seconds()
            self.task_intervals[name].append(interval)
            obs_metrics.HEARTBEAT_INTERVAL_MS.labels(name).set(interval * 1000)
        self.task_heartbeat[name] = now
        obs_metrics.HEARTBEAT_OK_TOTAL.labels(name).inc()
        if cause:
            self.task_cause[name] = cause

        self.last_function = name
        self.last_alive = now
        # reinicio exitoso resetea backoff
        if name in self.task_backoff:
            self.task_backoff[name] = 0

    # Alias retrocompatible
    def tick(self, name: str, cause: str | None = None) -> None:  # pragma: no cover - compat
        self.beat(name, cause)

    def tick_data(self, symbol: str, reinicio: bool = False) -> None:
        """Actualiza la marca de tiempo de la última vela recibida."""
        obs_metrics.registrar_tick_data(symbol, reinicio=reinicio)
        ahora = self._now()
        self.data_heartbeat[symbol] = ahora
        if reinicio:
            log.info("🔄 Reinicio exitoso del stream %s, esperando datos...", symbol)
        if symbol in self.last_data_alert:
            log.info("✅ %s retomó latidos de datos", symbol)
            self.last_data_alert.pop(symbol, None)
        if symbol in self.inactive_symbols:
            log.info("✅ %s reactivado tras ausencia de datos", symbol)
            self.inactive_symbols.discard(symbol)
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

    def registrar_ping(self, symbol: str, rtt_ms: float) -> None:
        """Almacena la latencia RTT de ``symbol`` para diagnóstico."""
        self.ping_rtts[symbol].append(rtt_ms)

    # ------------------------------------------------------------------
    # Tareas de monitorización
    # ------------------------------------------------------------------

    async def heartbeat(self, interval: int = 60) -> None:
        """Emite latidos periódicos para el proceso principal."""

        while True:
            log.info("bot alive | last=%s", self.last_function)
            self.last_alive = self._now()
            await asyncio.sleep(interval)

    def set_watchdog_interval(self, interval: int) -> None:
        """Actualiza el intervalo de verificación del watchdog."""

        self._watchdog_interval = interval
        self._watchdog_interval_event.set()

    async def _watchdog_once(self, timeout: int) -> None:
        now = self._now()
        delta = (now - self.last_alive).total_seconds()
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
        for task_name, ts in list(self.task_heartbeat.items()):
            cooldown = self.task_cooldown.get(task_name)
            if cooldown and now < cooldown:
                continue
            intervals = list(self.task_intervals.get(task_name, []))
            if intervals:
                idx = max(0, int(len(intervals) * 0.95) - 1)
                p95 = sorted(intervals)[idx]
                timeout_task = min(max(p95 * 3, 90), 300)
            else:
                timeout_task = timeout
            expected = self.task_expected_interval.get(task_name)
            if expected:
                timeout_task = max(timeout_task, expected * 3)
            timeout_task = max(timeout_task, timeout)
            delta_task = (now - ts).total_seconds()
            if delta_task > timeout_task:
                log.critical(
                    "⚠️ %s inactivo %.1fs (umbral %.1fs)",
                    task_name,
                    delta_task,
                    timeout_task,
                )
                self.inactividad_detectada[task_name] = (
                    self.inactividad_detectada.get(task_name, 0) + 1
                )
                registro_metrico.registrar(
                    "inactividad_detectada_total",
                    {"role": task_name, "count": self.inactividad_detectada[task_name]},
                )
                if task_name.startswith("stream_"):
                    sym = task_name.split("_", 1)[1]
                    if self.data_feed_reconnector:
                        log.warning(
                            "Inactividad detectada en %s; solicitando reinicio",
                            task_name,
                        )
                        await self.data_feed_reconnector(sym)
                    else:
                        log.warning(
                            "Inactividad detectada en %s; no hay reconector registrado",
                            task_name,
                        )
                else:
                    await self.restart_task(task_name)
        for sym, ts in self.data_heartbeat.items():
            sin_datos = (now - ts).total_seconds()
            log.debug(
                "Verificando datos de %s: %.1f segundos desde la última vela",
                sym,
                sin_datos,
            )
            if sin_datos > self.TIMEOUT_SIN_DATOS:
                if sym in self.inactive_symbols:
                    log.debug("%s marcado como inactivo; omitiendo alerta", sym)
                    continue
                ahora = now
                ultima = self.last_data_alert.get(sym)
                if not ultima or (
                    ahora - ultima
                ).total_seconds() > self.ALERTA_SIN_DATOS_INTERVALO:
                    log.critical(
                        "⚠️ Sin datos de %s desde hace %.1f segundos",
                        sym,
                        sin_datos,
                    )
                    role = f"data_{sym}"
                    self.inactividad_detectada[role] = (
                        self.inactividad_detectada.get(role, 0) + 1
                    )
                    registro_metrico.registrar(
                        "inactividad_detectada_total",
                        {"role": role, "count": self.inactividad_detectada[role]},
                    )
                    self.last_data_alert[sym] = ahora
                    try:
                        self.notificador.enviar(
                            f"⚠️ Sin datos de {sym} desde hace {sin_datos:.1f} segundos",
                            "CRITICAL",
                        )
                    except Exception:
                        pass
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
                    else:
                        log.debug(
                            "No hay reconector de DataFeed registrado para %s", sym
                        )
                    self.inactive_symbols.add(sym)

        for sym, rtts in list(self.ping_rtts.items()):
            if len(rtts) >= self.PING_RTT_SAMPLES:
                avg = sum(rtts) / len(rtts)
                if avg > self.PING_RTT_THRESHOLD_MS:
                    last_alert = self.last_ping_alert.get(sym)
                    if not last_alert or (
                        now - last_alert
                    ).total_seconds() > self.ALERTA_SIN_DATOS_INTERVALO:
                        log.warning(
                            "⚠️ RTT ping alto para %s: %.1f ms", sym, avg
                        )
                        self.last_ping_alert[sym] = now
                    if self.data_feed_reconnector:
                        try:
                            log.warning(
                                "Solicitando reinicio de DataFeed para %s por RTT alto",
                                sym,
                            )
                            await self.data_feed_reconnector(sym)
                        except Exception as e:
                            log.error(
                                "No se pudo solicitar reinicio de DataFeed para %s: %s",
                                sym,
                                e,
                            )
                    rtts.clear()

    async def watchdog(self, timeout: int = 120, check_interval: int = 10) -> None:
        """Valida que el proceso siga activo e imprime trazas si se congela."""

        self._watchdog_interval = check_interval
        while not self.stop_event.is_set():
            await self._watchdog_once(timeout)
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
        task = asyncio.current_task()
        limite = max_restarts if max_restarts is not None else 5
        try:
            while True:
                try:
                    result = coro_factory()
                    if asyncio.iscoroutine(result):
                        await result
                    # Latido tras finalizar correctamente
                    self.beat(task_name, "end")
                    break  # fin normal
                except asyncio.CancelledError:
                    # Aseguramos latido y limpieza ante cancelación
                    self.beat(task_name, "cancel")
                    log.info("Tarea %s cancelada", task_name)
                    raise
                except Exception as e:  # pragma: no cover - log crítico
                    self.beat(task_name, "error")
                    backoff = calcular_backoff(restarts, base=delay, max_seg=120)
                    log.error(
                        "⚠️ Error en %s: %r | intento=%s backoff=%.1fs",
                        task_name,
                        e,
                        restarts + 1,
                        backoff,
                        exc_info=True,
                    )
                    if restarts + 1 >= limite:
                        cooldown = calcular_backoff(restarts, base=delay * 2, max_seg=300)
                        log.error(
                            "🚫 %s alcanzó %s reinicios; enfriando %.1fs",
                            task_name,
                            limite,
                            cooldown,
                        )
                        await asyncio.sleep(cooldown)
                        restarts = 0
                        continue
                    log.warning(
                        "⏹️ %s finalizó; reintento en %.1fs", task_name, backoff
                    )
                    await asyncio.sleep(backoff)
                    restarts += 1
                    continue
        finally:
            # Limpia la referencia de la tarea si ya no es la vigente
            if self.tasks.get(task_name) is task:
                self.tasks.pop(task_name, None)
    async def restart_task(self, task_name: str) -> None:
        """Reinicia ``task_name`` aplicando backoff y registrando métricas."""

        task = self.tasks.get(task_name)
        if task:
            try:
                task.cancel()
                try:
                    await asyncio.wait_for(task, timeout=self.close_timeout)
                except asyncio.TimeoutError:
                    log.warning("⏱️ Timeout cancelando %s", task_name)
                except asyncio.CancelledError:
                    pass
                except Exception:
                    log.exception("Error esperando cancelación de %s", task_name)
            except Exception:
                pass
            finally:
                if self.tasks.get(task_name) is task:
                    self.tasks.pop(task_name, None)
        registrar_watchdog_restart(task_name)
        self.reinicios_watchdog[task_name] = (
            self.reinicios_watchdog.get(task_name, 0) + 1
        )
        factory = self.task_factories.get(task_name)
        if factory:
            self.supervised_task(factory, name=task_name)
        try:
            self.notificador.enviar(f"Reiniciando tarea {task_name}")
        except Exception:
            pass
        now = self._now()
        times = self.task_restart_times[task_name]
        times.append(now)
        limite = now - timedelta(minutes=10)
        while times and times[0] < limite:
            times.popleft()
        if len(times) > 3:
            log.warning(
                "⚠️ %s reiniciado %s veces en 10m", task_name, len(times)
            )
            try:
                self.notificador.enviar(
                    f"⚠️ {task_name} reiniciado {len(times)} veces en 10m",
                    "WARNING",
                )
            except Exception:
                pass
        attempt = self.task_backoff[task_name]
        async def _starter() -> None:
            await backoff_sleep(attempt, base=5, cap=120)
            factory = self.task_factories.get(task_name)
            if factory:
                self.supervised_task(factory, name=task_name)
            self.task_cooldown[task_name] = self._now() + timedelta(seconds=self.cooldown_sec)
        asyncio.create_task(_starter(), name=f"{task_name}_restart")
        if task_name.startswith("stream_"):
            sym = task_name.split("stream_")[1]
            obs_metrics.REINTENTOS_RECONEXION_TOTAL.labels(sym).inc()
            obs_metrics.STREAMS_ACTIVOS.labels(sym).set(0)
            self.reconexiones_ws[sym] = self.reconexiones_ws.get(sym, 0) + 1
            registro_metrico.registrar(
                "reconexiones_ws_total",
                {"symbol": sym, "count": self.reconexiones_ws[sym]},
            )
        elif task_name.startswith("consumer_"):
            sym = task_name.split("consumer_")[1]
            self.reinicios_consumer[sym] = self.reinicios_consumer.get(sym, 0) + 1
            registro_metrico.registrar(
                "reinicios_consumer_total",
                {"symbol": sym, "count": self.reinicios_consumer[sym]},
            )
        self.task_backoff[task_name] = attempt + 1

    def supervised_task(
        self,
        coro_factory: Callable[..., Awaitable],
        name: str | None = None,
        delay: int = 5,
        max_restarts: int | None = None,
        expected_interval: int | None = None,
    ) -> asyncio.Task:
        """Crea una tarea supervisada que se reinicia automáticamente."""

        task_name = name or getattr(coro_factory, "__name__", "task")
        self.task_factories[task_name] = coro_factory
        self.task_backoff[task_name] = 0
        if expected_interval is not None:
            self.task_expected_interval[task_name] = expected_interval
        task = asyncio.create_task(
            self._restartable_runner(coro_factory, task_name, delay, max_restarts),
            name=task_name,
        )
        self.tasks[task_name] = task
        return task
    
    async def shutdown(self) -> None:
        """Detiene todas las tareas supervisadas."""

        self.stop_event.set()
        for t in list(self.tasks.values()):
            t.cancel()
        if self.tasks:
            await asyncio.gather(*self.tasks.values(), return_exceptions=True)

# ----------------------------------------------------------------------
#  API de compatibilidad
# ----------------------------------------------------------------------

_default_supervisor = Supervisor()

start_supervision = _default_supervisor.start_supervision
supervised_task = _default_supervisor.supervised_task
beat = _default_supervisor.beat
tick = _default_supervisor.tick
tick_data = _default_supervisor.tick_data
registrar_reinicio_inactividad = _default_supervisor.registrar_reinicio_inactividad
registrar_reconexion_datafeed = _default_supervisor.registrar_reconexion_datafeed
registrar_ping = _default_supervisor.registrar_ping
set_watchdog_interval = _default_supervisor.set_watchdog_interval
shutdown = _default_supervisor.shutdown

async def stop_supervision() -> None:
    """Detiene todas las tareas supervisadas."""
    await _default_supervisor.shutdown()

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
    "beat",
    "tick",
    "tick_data",
    "registrar_reinicio_inactividad",
    "registrar_reconexion_datafeed",
    "registrar_ping",
    "set_watchdog_interval",
    "shutdown",
    "stop_supervision",
    "tasks",
    "task_heartbeat",
    "data_heartbeat",
    "reinicios_inactividad",
    "get_last_alive",
]
