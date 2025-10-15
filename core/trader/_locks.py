"""Synchronization primitives used across trader components."""

from __future__ import annotations

import asyncio
import weakref
from typing import Optional


class AsyncSymbolLock:
    """Reentrant asyncio lock bound to the owning task.

    ``asyncio.Lock`` instances are not reentrant by default. The trading
    pipeline, however, may call nested helpers that need to coordinate access
    to shared per-symbol state (buffers, cooldown windows, dynamic configs).
    ``AsyncSymbolLock`` wraps an ``asyncio.Lock`` and tracks the task holding it,
    allowing the same task to acquire it multiple times without deadlocking.
    """

    __slots__ = ("_lock", "_owner", "_depth")

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._owner: Optional[weakref.ReferenceType[asyncio.Task[object]]] = None
        self._depth: int = 0

    async def acquire(self) -> None:
        """Acquire the lock, waiting if necessary."""

        task = asyncio.current_task()
        if task is None:  # pragma: no cover - defensive, asyncio invariant
            raise RuntimeError("AsyncSymbolLock requires a running asyncio task")

        owner = self._owner() if self._owner is not None else None
        if owner is task:
            self._depth += 1
            return

        await self._lock.acquire()
        self._owner = weakref.ref(task)
        self._depth = 1

    async def release(self) -> None:
        """Release the lock held by the current task."""

        task = asyncio.current_task()
        if task is None:  # pragma: no cover - defensive, asyncio invariant
            raise RuntimeError("AsyncSymbolLock requires a running asyncio task")

        owner = self._owner() if self._owner is not None else None
        if owner is not task:
            raise RuntimeError("AsyncSymbolLock can only be released by its owner")

        self._depth -= 1
        if self._depth == 0:
            self._owner = None
            self._lock.release()

    def locked(self) -> bool:
        """Return ``True`` when the underlying lock is engaged."""

        return self._lock.locked()

    async def __aenter__(self) -> "AsyncSymbolLock":
        await self.acquire()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.release()