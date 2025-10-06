from __future__ import annotations
import asyncio
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Dict, List
from core.utils.logger import configurar_logger

log = configurar_logger('event_bus', modo_silencioso=True)


class EventBus:
    """Simple asynchronous event bus based on :class:`asyncio.Queue`."""

    def __init__(self) -> None:
        self._listeners: Dict[str, List[Callable[[Any], Awaitable[None]]]] = defaultdict(list)
        self._queue: asyncio.Queue[tuple[str, Any]] = asyncio.Queue()
        self._waiters: Dict[str, List["EventBus._Waiter"]] = defaultdict(list)
        self._task: asyncio.Task | None = None
        self._closed = False
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None
        if loop and loop.is_running():
            self._task = loop.create_task(self._dispatcher())

    @dataclass(slots=True)
    class _Waiter:
        event: asyncio.Event
        payload: Any | None = None

    def start(self) -> None:
        """Ensure the dispatcher task is running under the current loop."""
        if self._task is not None:
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        if loop.is_running():
            self._task = loop.create_task(self._dispatcher())

    def subscribe(self, event_type: str, callback: Callable[[Any], Awaitable[None]]) -> None:
        """Register ``callback`` to be invoked for ``event_type`` events."""
        self._listeners[event_type].append(callback)
        self.start()

    def emit(self, event_type: str, data: Any | None = None) -> None:
        """Emit ``data`` for ``event_type`` scheduling the publish coroutine."""
        self._resolve_waiters(event_type, data)
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        loop.create_task(self.publish(event_type, data))

    async def publish(self, event_type: str, data: Any | None) -> None:
        """Publish ``data`` for ``event_type``."""
        self.start()
        await self._queue.put((event_type, data))

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

    async def _dispatcher(self) -> None:
        while not self._closed:
            event_type, data = await self._queue.get()
            self._resolve_waiters(event_type, data)
            for cb in list(self._listeners.get(event_type, [])):
                try:
                    task = asyncio.create_task(cb(data))
                    task.add_done_callback(
                        lambda t, evt=event_type: t.exception()
                        and log.error(
                            f"❌ Error en callback de '{evt}': {t.exception()}"
                        )
                    )
                except Exception as e:
                    log.error(f"❌ Error despachando '{event_type}': {e}")

    async def close(self) -> None:
        self._closed = True
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        for event_type, waiters in list(self._waiters.items()):
            for waiter in waiters:
                if not waiter.event.is_set():
                    waiter.payload = None
                    waiter.event.set()
            self._waiters.pop(event_type, None)

    def _resolve_waiters(self, event_type: str, data: Any | None) -> None:
        waiters = self._waiters.pop(event_type, [])
        for waiter in waiters:
            if not waiter.event.is_set():
                waiter.payload = data
                waiter.event.set()
