"""
Supervisor Lite — versión simplificada y estable.

Objetivo: mantener la supervisión (heartbeat + watchdog + reinicios) con el mínimo
acoplamiento. Elimina dependencias a NotificationManager, registro_metrico,
obs/Prometheus y backoff externos. Expone una API clara y directa.

API principal:
- SupervisorLite(on_event: callable|None = None, *, cooldown_sec=15, close_timeout=5)
- start_supervision()
- supervised_task(coro_factory, name: str | None = None,
                  *, delay=5, max_restarts=None, expected_interval: int | None = None)
- beat(name: str, cause: str | None = None)
- tick(name: str, cause: str | None = None)  # alias
- tick_data(symbol: str, reinicio: bool = False)
- set_watchdog_interval(interval: int)
- restart_task(task_name: str)
- shutdown()

Compat:
Se mantienen alias a nivel de módulo si decides exportarlos igual que antes.
"""
from __future__ import annotations
import asyncio
import inspect
import traceback
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone
from typing import Awaitable, Callable, Deque, Dict, Optional

UTC = timezone.utc



class FactoryValidationError(TypeError):
    """Raised when a task factory registered in the ``Supervisor`` is invalid."""


class Supervisor:
    """Vigila la salud del proceso y reinicia tareas con backoff simple."""

    def __init__(
        self,
        on_event: Callable[[str, dict], None] | None = None,
        *,
        cooldown_sec: float = 15.0,
        close_timeout: float = 5.0,
        watchdog_timeout: int = 120,
        watchdog_check_interval: int = 10,
        validate_factories: bool = False,
        factory_validator: Callable[[Callable[..., Awaitable]], None] | None = None,
        circuit_breaker_failures: int = 5,
        circuit_breaker_window: float = 180.0,
        circuit_breaker_check: Callable[[str], bool] | None = None,
    ) -> None:
        self.on_event = on_event
        self.last_alive = datetime.now(UTC)
        self.last_function = "init"
        # Tasks y metadatos
        self.tasks: Dict[str, asyncio.Task] = {}
        self.task_factories: Dict[str, Callable[..., Awaitable]] = {}
        self.task_heartbeat: Dict[str, datetime] = {}
        self.task_intervals: Dict[str, Deque[float]] = defaultdict(lambda: deque(maxlen=100))
        self.task_expected_interval: Dict[str, int] = {}
        self.task_cooldown: Dict[str, datetime] = {}
        self.task_backoff: Dict[str, int] = defaultdict(int)
        self.task_restart_history: Dict[str, Deque[datetime]] = defaultdict(deque)
        self.task_circuit_open: Dict[str, datetime] = {}
        self.task_circuit_config: Dict[str, tuple[int, float]] = {}
        # Data
        self.data_heartbeat: Dict[str, datetime] = {}
        self.TIMEOUT_SIN_DATOS = 300  # por defecto; ajústalo en tu app si quieres
        # Pings WS
        self.last_ping: Dict[str, float] = {}
        # Estado
        self.stop_event = asyncio.Event()
        self._watchdog_interval_event = asyncio.Event()
        self._watchdog_interval = watchdog_check_interval
        self._watchdog_timeout = watchdog_timeout
        self.cooldown_sec = float(cooldown_sec)
        self.close_timeout = float(close_timeout)
        self._validate_factories = bool(validate_factories or factory_validator)
        self._factory_validator: Optional[Callable[[Callable[..., Awaitable]], None]] = (
            factory_validator if factory_validator is not None else None
        )
        if self._validate_factories and self._factory_validator is None:
            self._factory_validator = self._default_factory_validator
        self._circuit_default_failures = max(0, int(circuit_breaker_failures))
        self._circuit_default_window = max(1.0, float(circuit_breaker_window))
        self._circuit_reset_check = circuit_breaker_check

    # --------------------- utilidades ---------------------
    def _emit(self, evento: str, data: dict) -> None:
        if self.on_event:
            try:
                self.on_event(evento, data)
            except Exception:
                pass

    def _now(self) -> datetime:
        return datetime.now(UTC)
    
    def _get_circuit_config(self, task_name: str) -> tuple[int, float] | None:
        config = self.task_circuit_config.get(task_name)
        if config is None:
            if self._circuit_default_failures <= 0:
                return None
            config = (self._circuit_default_failures, self._circuit_default_window)
            self.task_circuit_config[task_name] = config
        failures, window = config
        if failures <= 0:
            return None
        return failures, max(1.0, float(window))

    def _register_restart_failure(self, task_name: str, *, timestamp: datetime | None = None) -> bool:
        config = self._get_circuit_config(task_name)
        if not config:
            return False
        threshold, window = config
        ts = timestamp or self._now()
        cutoff = ts - timedelta(seconds=window)
        history = self.task_restart_history[task_name]
        history.append(ts)
        while history and history[0] < cutoff:
            history.popleft()
        if len(history) >= threshold:
            if task_name not in self.task_circuit_open:
                self.task_circuit_open[task_name] = ts
                self._emit(
                    "task_circuit_open",
                    {
                        "name": task_name,
                        "threshold": threshold,
                        "window": window,
                        "failures": len(history),
                        "opened_at": ts.isoformat(),
                    },
                )
            return True
        return False

    def _is_circuit_blocking_restart(self, task_name: str) -> bool:
        if task_name not in self.task_circuit_open:
            return False
        if self._circuit_reset_check and self._circuit_reset_check(task_name):
            self.reset_task_circuit(task_name)
            return False
        return True

    def is_task_circuit_open(self, task_name: str) -> bool:
        """Indica si la tarea tiene el circuito abierto."""

        return task_name in self.task_circuit_open

    def reset_task_circuit(self, task_name: str) -> bool:
        """Cierra el circuito abierto y reinicia el historial de fallos."""

        opened_at = self.task_circuit_open.pop(task_name, None)
        was_open = opened_at is not None
        if was_open:
            self._emit(
                "task_circuit_closed",
                {"name": task_name, "opened_at": opened_at.isoformat() if hasattr(opened_at, "isoformat") else None},
            )
        self.task_restart_history.pop(task_name, None)
        self.task_backoff[task_name] = 0
        self.task_cooldown.pop(task_name, None)
        return was_open

    def exception_handler(self, loop: asyncio.AbstractEventLoop, context: dict) -> None:
        exc = context.get("exception")
        if exc:
            self._emit("loop_exception", {"exc": repr(exc)})
        else:
            self._emit("loop_error", {"message": context.get("message")})

    # ------------------- heartbeat/data -------------------
    def beat(self, name: str, cause: str | None = None) -> None:
        now = self._now()
        last = self.task_heartbeat.get(name)
        if last:
            self.task_intervals[name].append((now - last).total_seconds())
        self.task_heartbeat[name] = now
        self.last_function = name
        self.last_alive = now
        if name in self.task_backoff:
            self.task_backoff[name] = 0
        self._emit("beat", {"name": name, "cause": cause or ""})

    def tick(self, name: str, cause: str | None = None) -> None:
        self.beat(name, cause)

    def tick_data(self, symbol: str, reinicio: bool = False) -> None:
        self.data_heartbeat[symbol] = self._now()
        self._emit("tick_data", {"symbol": symbol, "reinicio": reinicio})

    def registrar_ping(self, symbol: str, rtt: float) -> None:
        """Registra latencias de ping para diagnósticos del WebSocket."""

        try:
            latencia = float(rtt)
        except (TypeError, ValueError):
            return
        if latencia < 0:
            return
        self.last_ping[symbol] = latencia
        self._emit("ws_ping", {"symbol": symbol, "latencia_ms": latencia})

    # ------------------- watchdog -------------------
    def set_watchdog_interval(self, interval: int) -> None:
        self._watchdog_interval = max(1, int(interval))
        self._watchdog_interval_event.set()

    async def _watchdog_once(self, timeout: int) -> None:
        now = self._now()
        # proceso
        if (now - self.last_alive).total_seconds() > timeout:
            self._emit("bot_inactivo", {"desde_s": (now - self.last_alive).total_seconds(), "ultima": self.last_function})
        # tareas
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
            if (now - ts).total_seconds() > timeout_task:
                await self.restart_task(task_name)
        # datos
        for sym, ts in list(self.data_heartbeat.items()):
            if (now - ts).total_seconds() > self.TIMEOUT_SIN_DATOS:
                self._emit("sin_datos", {"symbol": sym, "desde_s": (now - ts).total_seconds()})

    async def watchdog(self) -> None:
        while not self.stop_event.is_set():
            await self._watchdog_once(self._watchdog_timeout)
            try:
                await asyncio.wait_for(self._watchdog_interval_event.wait(), timeout=self._watchdog_interval)
            except asyncio.TimeoutError:
                pass
            self._watchdog_interval_event.clear()

    async def heartbeat(self, interval: int = 60) -> None:
        while not self.stop_event.is_set():
            self.last_alive = self._now()
            self._emit("heartbeat", {"last": self.last_function})
            await asyncio.sleep(interval)

    # --------------- gestión de tareas ----------------
    async def _restartable_runner(
        self,
        coro_factory: Callable[..., Awaitable],
        task_name: str,
        delay: int = 5,
        max_restarts: int | None = None,
    ) -> None:
        restarts = 0
        task = asyncio.current_task()
        limite = 5 if max_restarts is None else (float("inf") if max_restarts <= 0 else max_restarts)
        try:
            while True:
                try:
                    result = coro_factory()
                    if asyncio.iscoroutine(result):
                        await result
                    self.beat(task_name, "end")
                    break
                except asyncio.CancelledError:
                    self.beat(task_name, "cancel")
                    raise
                except Exception:
                    self.beat(task_name, "error")
                    intento = restarts + 1
                    backoff = min(120.0, (delay or 5) * (2 ** restarts))
                    self._emit("task_error", {"name": task_name, "restarts": intento, "backoff": backoff})
                    if self._register_restart_failure(task_name):
                        break
                    if intento >= (limite if limite != float("inf") else 1e9):
                        cooldown = min(300.0, (delay or 5) * 2 * (2 ** restarts))
                        self._emit("task_cooldown", {"name": task_name, "cooldown": cooldown})
                        await asyncio.sleep(cooldown)
                        restarts = 0
                        continue
                    await asyncio.sleep(backoff)
                    restarts = intento
                    continue
        finally:
            if self.tasks.get(task_name) is task:
                self.tasks.pop(task_name, None)

    async def restart_task(self, task_name: str) -> None:
        task = self.tasks.get(task_name)
        if task:
            try:
                task.cancel()
                try:
                    await asyncio.wait_for(task, timeout=self.close_timeout)
                except Exception:
                    pass
            finally:
                if self.tasks.get(task_name) is task:
                    self.tasks.pop(task_name, None)
        attempt = self.task_backoff[task_name]

        if self._is_circuit_blocking_restart(task_name):
            self._emit("task_restart_blocked", {"name": task_name, "reason": "circuit_open"})
            return

        async def _starter() -> None:
            backoff = min(120.0, 5.0 * (2 ** attempt))
            await asyncio.sleep(backoff)
            factory = self.task_factories.get(task_name)
            if factory:
                self.supervised_task(factory, name=task_name)
            self.task_cooldown[task_name] = self._now() + timedelta(seconds=self.cooldown_sec)
        asyncio.create_task(_starter(), name=f"{task_name}_restart")
        self.task_backoff[task_name] = attempt + 1
        self._emit("task_restart", {"name": task_name, "attempt": attempt + 1})

    def supervised_task(
        self,
        coro_factory: Callable[..., Awaitable],
        name: str | None = None,
        *,
        delay: int = 5,
        max_restarts: int | None = None,
        expected_interval: int | None = None,
        circuit_breaker_failures: int | None = None,
        circuit_breaker_window: float | None = None,
    ) -> asyncio.Task:
        """Register a coroutine factory that can be restarted on failures.

        The factory **must** be un-parameterised and return a fresh awaitable
        every time it is invoked. Enabling ``validate_factories`` (or providing
        a ``factory_validator``) activates static checks that reject factories
        with argumentos obligatorios, async generators o coroutines ya creados.

        Parameters
        ----------
        coro_factory:
            Callable que produce un nuevo ``Awaitable`` en cada invocación.
        name:
            Nombre opcional para identificar la tarea en los logs y métricas.
        delay:
            Retardo base del backoff exponencial tras errores consecutivos.
        max_restarts:
            Reinicios consecutivos antes de aplicar un enfriamiento prolongado.
        expected_interval:
            Intervalo esperado (en segundos) entre heartbeats/ticks de la tarea.

        circuit_breaker_failures:
            Número de fallas acumuladas dentro de la ventana que disparan la
            apertura del circuito. Usa ``0`` para desactivar el circuito en la
            tarea concreta.
        circuit_breaker_window:
            Ventana temporal (en segundos) empleada para contabilizar las
            fallas antes de abrir el circuito.

        Returns
        -------
        asyncio.Task
            La tarea supervisada que ejecutará el bucle reiniciable.

        Raises
        ------
        FactoryValidationError
            Si la factoría infringe las validaciones activas.
        """
        task_name = name or getattr(coro_factory, "__name__", "task")
        self._validate_factory(coro_factory, task_name)
        self.task_factories[task_name] = coro_factory
        self.task_backoff[task_name] = 0
        self.task_restart_history.pop(task_name, None)
        self.task_circuit_open.pop(task_name, None)
        failures_cfg = (
            self._circuit_default_failures
            if circuit_breaker_failures is None
            else max(0, int(circuit_breaker_failures))
        )
        if circuit_breaker_window is None:
            window_cfg = self._circuit_default_window
        else:
            window_cfg = max(1.0, float(circuit_breaker_window))
        self.task_circuit_config[task_name] = (failures_cfg, window_cfg)
        if expected_interval is not None:
            self.task_expected_interval[task_name] = expected_interval
        task = asyncio.create_task(
            self._restartable_runner(coro_factory, task_name, delay, max_restarts),
            name=task_name,
        )
        self.tasks[task_name] = task
        return task

    def start_supervision(self) -> None:
        loop = asyncio.get_running_loop()
        loop.set_exception_handler(self.exception_handler)
        asyncio.create_task(self.heartbeat(), name="heartbeat")
        asyncio.create_task(self.watchdog(), name="watchdog")
        self._emit("supervision_started", {})

    async def shutdown(self) -> None:
        self.stop_event.set()
        for t in list(self.tasks.values()):
            t.cancel()
        if self.tasks:
            await asyncio.gather(*self.tasks.values(), return_exceptions=True)
        self._emit("supervision_stopped", {})


    # ------------------- validación factorías -------------------

    def enable_factory_validation(
        self,
        *,
        validator: Callable[[Callable[..., Awaitable]], None] | None = None,
    ) -> None:
        """Activa o sustituye las validaciones de factorías en caliente.

        Si no se proporciona ``validator`` se reutiliza el validador por
        defecto, el cual garantiza que la factoría pueda ejecutarse sin
        argumentos requeridos. Esta operación es idempotente y puede ejecutarse
        en tiempo de ejecución antes de registrar nuevas tareas.
        """

        self._validate_factories = True
        self._factory_validator = validator or self._default_factory_validator

    def _validate_factory(
        self, coro_factory: Callable[..., Awaitable], task_name: str
    ) -> None:
        if asyncio.iscoroutine(coro_factory):
            raise FactoryValidationError(
                f"La factoría registrada para '{task_name}' es un coroutine ya creado; "
                "usa un callable que construya un nuevo coroutine en cada reinicio."
            )
        if inspect.isasyncgen(coro_factory):  # pragma: no cover - defensivo
            raise FactoryValidationError(
                f"La factoría registrada para '{task_name}' es un async generator ya creado; "
                "no es compatible con Supervisor."
            )
        if not callable(coro_factory):
            raise FactoryValidationError(
                f"La factoría registrada para '{task_name}' no es invocable (tipo: {type(coro_factory)!r})."
            )

        if self._validate_factories and self._factory_validator is not None:
            self._factory_validator(coro_factory)

    def _default_factory_validator(self, coro_factory: Callable[..., Awaitable]) -> None:
        if inspect.isasyncgenfunction(coro_factory):
            raise FactoryValidationError(
                "Las factorías supervisadas no pueden ser funciones generadoras asíncronas."
            )

        try:
            signature = inspect.signature(coro_factory)
        except (TypeError, ValueError):  # pragma: no cover - objetos opacos
            return

        required = [
            parameter.name
            for parameter in signature.parameters.values()
            if parameter.kind
            in (
                inspect.Parameter.POSITIONAL_ONLY,
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
                inspect.Parameter.KEYWORD_ONLY,
            )
            and parameter.default is inspect._empty
        ]
        if required:
            raise FactoryValidationError(
                "La factoría supervisada debe poder llamarse sin argumentos. "
                f"Faltan valores por defecto para: {', '.join(required)}. "
                "Usa un closure/lambda puro que capture los valores necesarios."
            )


# ------------- API de compatibilidad opcional -------------
# Exporta un supervisor por defecto para drop-in replacement.
_default_supervisor = Supervisor()

start_supervision = _default_supervisor.start_supervision
supervised_task = _default_supervisor.supervised_task
beat = _default_supervisor.beat
tick = _default_supervisor.tick
tick_data = _default_supervisor.tick_data
set_watchdog_interval = _default_supervisor.set_watchdog_interval
shutdown = _default_supervisor.shutdown
registrar_ping = _default_supervisor.registrar_ping
reset_task_circuit = _default_supervisor.reset_task_circuit
is_task_circuit_open = _default_supervisor.is_task_circuit_open


async def stop_supervision() -> None:
    """Detiene la supervisión por compatibilidad con el API anterior."""

    await _default_supervisor.shutdown()



