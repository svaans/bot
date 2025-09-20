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
import traceback
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone
from typing import Awaitable, Callable, Deque, Dict

UTC = timezone.utc


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
        # Data
        self.data_heartbeat: Dict[str, datetime] = {}
        self.TIMEOUT_SIN_DATOS = 300  # por defecto; ajústalo en tu app si quieres
        # Estado
        self.stop_event = asyncio.Event()
        self._watchdog_interval_event = asyncio.Event()
        self._watchdog_interval = watchdog_check_interval
        self._watchdog_timeout = watchdog_timeout
        self.cooldown_sec = float(cooldown_sec)
        self.close_timeout = float(close_timeout)

    # --------------------- utilidades ---------------------
    def _emit(self, evento: str, data: dict) -> None:
        if self.on_event:
            try:
                self.on_event(evento, data)
            except Exception:
                pass

    def _now(self) -> datetime:
        return datetime.now(UTC)

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
                    backoff = min(120.0, (delay or 5) * (2 ** restarts))
                    self._emit("task_error", {"name": task_name, "restarts": restarts + 1, "backoff": backoff})
                    if restarts + 1 >= (limite if limite != float("inf") else 1e9):
                        cooldown = min(300.0, (delay or 5) * 2 * (2 ** restarts))
                        self._emit("task_cooldown", {"name": task_name, "cooldown": cooldown})
                        await asyncio.sleep(cooldown)
                        restarts = 0
                        continue
                    await asyncio.sleep(backoff)
                    restarts += 1
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
    ) -> asyncio.Task:
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



