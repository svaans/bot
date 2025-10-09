from __future__ import annotations
import asyncio
import contextlib
from collections import defaultdict
from dataclasses import dataclass
from functools import partial
from typing import Any, Awaitable, Callable, Dict, List
from core.utils.logger import configurar_logger

log = configurar_logger('event_bus', modo_silencioso=True)


class EventBus:
    """Simple asynchronous event bus backed by lightweight asyncio tasks."""

    def __init__(self) -> None:
        self._listeners: Dict[str, List[Callable[[Any], Awaitable[None]]]] = defaultdict(list)
        self._waiters: Dict[str, List["EventBus._Waiter"]] = defaultdict(list)
        self._inflight: set[asyncio.Task] = set()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._closed = False
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
        if self._loop and not self._loop.is_closed():
            asyncio.run_coroutine_threadsafe(self.publish(event_type, data), self._loop)

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
        waiter = EventBus._Waiter(event=asyncio.Event())
        self._waiters[event_type].append(waiter)
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
        self._loop = None

    def _resolve_waiters(self, event_type: str, data: Any | None) -> None:
        waiters = self._waiters.pop(event_type, [])
        for waiter in waiters:
            if not waiter.event.is_set():
                waiter.payload = data
                waiter.event.set()

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
