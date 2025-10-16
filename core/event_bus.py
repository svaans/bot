from __future__ import annotations
import asyncio
import contextlib
from collections import OrderedDict, defaultdict, deque
from dataclasses import dataclass
from functools import partial
from threading import Lock
from typing import Any, Awaitable, Callable, Deque, Dict, List, Tuple
from core.utils.logger import configurar_logger
from core.utils.metrics_compat import Counter, Gauge

log = configurar_logger('event_bus', modo_silencioso=True)

_PENDING_EMIT_COUNTER = Counter(
    'event_bus_pending_emits_total',
    'Eventos emitidos sin loop activo y encolados temporalmente',
    ('event_type',),
)
_PENDING_QUEUE_GAUGE = Gauge(
    'event_bus_pending_queue',
    'Número de eventos pendientes por falta de loop activo',
)


class EventBus:
    """Simple asynchronous event bus backed by lightweight asyncio tasks."""

    def __init__(self, *, max_cached_payloads: int = 256) -> None:
        self._listeners: Dict[str, List[Callable[[Any], Awaitable[None]]]] = defaultdict(list)
        self._waiters: Dict[str, List["EventBus._Waiter"]] = defaultdict(list)
        self._inflight: set[asyncio.Task] = set()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._closed = False
        if max_cached_payloads < 0:
            raise ValueError("max_cached_payloads debe ser mayor o igual a cero")
        self._max_cached_payloads = max_cached_payloads
        self._last_payload: "OrderedDict[str, Any | None]" = OrderedDict()
        self._pending: Deque[Tuple[str, Any | None]] = deque()
        self._lock = Lock()
        self._pending_alerted: set[str] = set()
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None
        if loop and loop.is_running():
            self._loop = loop

    @dataclass(slots=True)
    class _Waiter:
        event: asyncio.Event
        payload: Any | None = None

    def start(self) -> None:
        """Record the currently running loop so synchronous helpers can use it."""
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        if loop.is_running():
            self._loop = loop
            self._drain_pending_emits()

    def subscribe(self, event_type: str, callback: Callable[[Any], Awaitable[None]]) -> None:
        """Register ``callback`` to be invoked for ``event_type`` events."""
        self._listeners[event_type].append(callback)
        self.start()

    def emit(self, event_type: str, data: Any | None = None) -> None:
        """Emit ``data`` for ``event_type`` scheduling the publish coroutine."""
        if self._closed:
            return
        self._resolve_waiters(event_type, data)
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None
        if loop and loop.is_running():
            loop.create_task(self.publish(event_type, data))
            return
        target_loop: asyncio.AbstractEventLoop | None
        with self._lock:
            target_loop = self._loop
        if target_loop and not target_loop.is_closed():
            asyncio.run_coroutine_threadsafe(self.publish(event_type, data), target_loop)
            return
        with self._lock:
            self._pending.append((event_type, data))
            pending_size = len(self._pending)
            warn = False
            if event_type not in self._pending_alerted:
                self._pending_alerted.add(event_type)
                warn = True
        _PENDING_EMIT_COUNTER.labels(event_type=event_type).inc()
        _PENDING_QUEUE_GAUGE.set(float(pending_size))
        if warn:
            log.warning(
                "⚠️ EventBus.emit sin loop activo; evento '%s' encolado (pendientes=%d)",
                event_type,
                pending_size,
            )

    async def publish(self, event_type: str, data: Any | None) -> None:
        """Publish ``data`` for ``event_type``."""
        if self._closed:
            return
        self.start()
        self._resolve_waiters(event_type, data)
        callbacks = list(self._listeners.get(event_type, []))
        for cb in callbacks:
            self._schedule_callback(cb, event_type, data)
        # Yield control so scheduled callbacks can start executing.
        await asyncio.sleep(0)

    async def wait(self, event_type: str, timeout: float | None = None) -> Any | None:
        """Wait until ``event_type`` is emitted and return its payload.

        Parameters
        ----------
        event_type:
            Nombre del evento a esperar.
        timeout:
            Tiempo máximo en segundos antes de agotar la espera. ``None`` indica
            espera indefinida.
        """
        self.start()
        sentinel = object()
        cached = self._last_payload.pop(event_type, sentinel)
        if cached is not sentinel:
            # Evento emitido antes de la espera: devolver el payload cacheado sin
            # registrar waiters innecesarios.
            return cached
        waiter = EventBus._Waiter(event=asyncio.Event())
        waiters = self._waiters[event_type]
        waiters.append(waiter)

        # Segunda comprobación tras registrar el waiter para cubrir la carrera en
        # la que el evento llega entre la primera comprobación y el registro.
        cached = self._last_payload.pop(event_type, sentinel)
        if cached is not sentinel:
            waiters.remove(waiter)
            if not waiters:
                self._waiters.pop(event_type, None)
            return cached
        try:
            await asyncio.wait_for(waiter.event.wait(), timeout)
        except Exception:
            waiters = self._waiters.get(event_type)
            if waiters and waiter in waiters:
                waiters.remove(waiter)
                if not waiters:
                    self._waiters.pop(event_type, None)
            raise
        return waiter.payload

    def wait_sync(self, event_type: str, timeout: float | None = None) -> Any | None:
        """Block the current thread until ``event_type`` arrives or timeout expires.

        Parameters
        ----------
        event_type:
            Nombre del evento a esperar.
        timeout:
            Tiempo máximo de espera en segundos. ``None`` implica espera indefinida.

        Returns
        -------
        Any | None
            El *payload* recibido para ``event_type`` si llega a tiempo.

        Raises
        ------
        RuntimeError
            Si el bus no está asociado a ningún *event loop* activo.
        asyncio.TimeoutError
            Si se agota el tiempo de espera configurado.
        """

        if self._loop is None or self._loop.is_closed():
            raise RuntimeError("EventBus.wait_sync() requiere un loop activo")

        future = asyncio.run_coroutine_threadsafe(self.wait(event_type, timeout), self._loop)
        try:
            return future.result()
        except Exception:
            future.cancel()
            raise

    @staticmethod
    def _log_task_error(task: asyncio.Task, event_type: str) -> None:
        """Loggear cualquier excepción producida por ``task``."""
        if task.cancelled():
            return
        try:
            exc = task.exception()
        except Exception as err:  # pragma: no cover - seguridad adicional
            log.error(f"❌ Error revisando tarea de '{event_type}': {err}")
            return
        if exc:
            log.error(f"❌ Error en callback de '{event_type}': {exc}")

    async def close(self) -> None:
        self._closed = True
        tasks = list(self._inflight)
        self._inflight.clear()
        for task in tasks:
            if not task.done():
                task.cancel()
        for task in tasks:
            with contextlib.suppress(asyncio.CancelledError):
                await task
        for event_type, waiters in list(self._waiters.items()):
            for waiter in waiters:
                if not waiter.event.is_set():
                    waiter.payload = None
                    waiter.event.set()
            self._waiters.pop(event_type, None)
        self._last_payload.clear()
        with self._lock:
            self._pending.clear()
            self._pending_alerted.clear()
        _PENDING_QUEUE_GAUGE.set(0.0)
        self._loop = None

    def _drain_pending_emits(self) -> None:
        if self._loop is None or self._loop.is_closed():
            return
        pending: list[Tuple[str, Any | None]]
        with self._lock:
            if not self._pending:
                return
            pending = list(self._pending)
            self._pending.clear()
            self._pending_alerted.clear()
            _PENDING_QUEUE_GAUGE.set(float(len(self._pending)))
        loop = self._loop
        assert loop is not None
        for event_type, payload in pending:
            loop.call_soon_threadsafe(asyncio.create_task, self.publish(event_type, payload))

    def _resolve_waiters(self, event_type: str, data: Any | None) -> None:
        waiters = self._waiters.pop(event_type, [])
        for waiter in waiters:
            if not waiter.event.is_set():
                waiter.payload = data
                waiter.event.set()
        if waiters:
            self._last_payload.pop(event_type, None)
        else:
            self._cache_payload(event_type, data)

    def _schedule_callback(
        self,
        callback: Callable[[Any], Awaitable[None]],
        event_type: str,
        data: Any | None,
    ) -> None:
        try:
            task = asyncio.create_task(callback(data))
        except RuntimeError as exc:
            log.error(f"❌ Error despachando '{event_type}': {exc}")
            return
        self._inflight.add(task)
        task.add_done_callback(partial(self._cleanup_task, event_type=event_type))

    def _cleanup_task(self, task: asyncio.Task, event_type: str) -> None:
        self._inflight.discard(task)
        self._log_task_error(task, event_type)

    def _cache_payload(self, event_type: str, data: Any | None) -> None:
        if self._max_cached_payloads <= 0:
            self._last_payload.clear()
            return
        self._last_payload[event_type] = data
        self._last_payload.move_to_end(event_type)
        while len(self._last_payload) > self._max_cached_payloads:
            self._last_payload.popitem(last=False)
