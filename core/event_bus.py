from __future__ import annotations
import asyncio
from collections import defaultdict
from typing import Any, Awaitable, Callable, Dict, List


class EventBus:
    """Simple asynchronous event bus based on :class:`asyncio.Queue`."""

    def __init__(self) -> None:
        self._listeners: Dict[str, List[Callable[[Any], Awaitable[None]]]] = defaultdict(list)
        self._queue: asyncio.Queue[tuple[str, Any]] = asyncio.Queue()
        self._task: asyncio.Task | None = asyncio.create_task(self._dispatcher())
        self._closed = False

    def subscribe(self, event_type: str, callback: Callable[[Any], Awaitable[None]]) -> None:
        """Register ``callback`` to be invoked for ``event_type`` events."""
        self._listeners[event_type].append(callback)

    async def publish(self, event_type: str, data: Any) -> None:
        """Publish ``data`` for ``event_type``."""
        await self._queue.put((event_type, data))

    async def _dispatcher(self) -> None:
        while not self._closed:
            event_type, data = await self._queue.get()
            for cb in list(self._listeners.get(event_type, [])):
                try:
                    asyncio.create_task(cb(data))
                except Exception:
                    pass

    async def close(self) -> None:
        self._closed = True
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass