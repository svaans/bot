from __future__ import annotations
import asyncio
from collections import defaultdict
from typing import Any, Awaitable, Callable, Dict, List
from core.utils.logger import configurar_logger

log = configurar_logger('event_bus', modo_silencioso=True)


class EventBus:
    """Simple asynchronous event bus based on :class:`asyncio.Queue`."""

    def __init__(self) -> None:
        self._listeners: Dict[str, List[Callable[[Any], Awaitable[None]]]] = defaultdict(list)
        self._queue: asyncio.Queue[tuple[str, Any]] = asyncio.Queue()
        self._waiters: Dict[str, List[asyncio.Future[Any]]] = defaultdict(list)
        self._task: asyncio.Task | None = None
        self._closed = False
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None
        if loop and loop.is_running():
            self._task = loop.create_task(self._dispatcher())

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
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        loop.create_task(self.publish(event_type, data))

    async def publish(self, event_type: str, data: Any | None) -> None:
        """Publish ``data`` for ``event_type``."""
        self.start()
        await self._queue.put((event_type, data))

    async def wait(self, event_type: str) -> Any | None:
        """Wait until ``event_type`` is emitted and return its payload."""
        self.start()
        loop = asyncio.get_running_loop()
        fut: asyncio.Future[Any] = loop.create_future()
        self._waiters[event_type].append(fut)
        return await fut

    async def _dispatcher(self) -> None:
        while not self._closed:
            event_type, data = await self._queue.get()
            waiters = self._waiters.pop(event_type, [])
            for waiter in waiters:
                if not waiter.done():
                    waiter.set_result(data)
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
        for waiters in self._waiters.values():
            for waiter in waiters:
                if not waiter.done():
                    waiter.cancel()
        self._waiters.clear()
