"""Compat patches executed at interpreter startup.

Provides a lightweight asyncio.TaskGroup fallback for Python 3.10 runtimes
so that the codebase and tests relying on TaskGroup keep working.
"""

from __future__ import annotations

import asyncio
from typing import Any, Awaitable, Optional


if not hasattr(asyncio, "TaskGroup"):
    class _CompatTaskGroup:  # pragma: no cover - exercised via tests
        """Simple asyncio.TaskGroup backport for Python < 3.11."""

        def __init__(self) -> None:
            self._tasks: set[asyncio.Task[Any]] = set()
            self._errors: list[BaseException] = []
            self._closing = False

        async def __aenter__(self) -> "_CompatTaskGroup":
            return self

        async def __aexit__(
            self,
            exc_type: Optional[type[BaseException]],
            exc: Optional[BaseException],
            tb: Any,
        ) -> bool:
            self._closing = True

            if exc is not None:
                self._cancel_all()
                await asyncio.gather(*self._tasks, return_exceptions=True)
                return False

            await self._wait_for_completion()

            if self._errors:
                raise self._errors[0]

            return False

        def create_task(
            self,
            coro: Awaitable[Any],
            *,
            name: Optional[str] = None,
        ) -> asyncio.Task[Any]:
            if self._closing:
                raise RuntimeError("TaskGroup is closing; cannot create new tasks")

            task = asyncio.create_task(coro, name=name)
            self._tasks.add(task)
            task.add_done_callback(self._on_task_done)
            return task

        def _on_task_done(self, task: asyncio.Task[Any]) -> None:
            self._tasks.discard(task)
            if task.cancelled():
                return

            try:
                task.result()
            except Exception as error:  # noqa: BLE001 - we must capture any exception
                self._errors.append(error)

        def _cancel_all(self) -> None:
            for task in list(self._tasks):
                task.cancel()

        async def _wait_for_completion(self) -> None:
            pending: set[asyncio.Task[Any]] = set(self._tasks)

            while pending:
                done, still_pending = await asyncio.wait(
                    pending,
                    return_when=asyncio.FIRST_EXCEPTION,
                )

                for task in done:
                    if task.cancelled():
                        continue

                    try:
                        task.result()
                    except Exception as error:  # noqa: BLE001
                        self._errors.append(error)
                        for other in still_pending:
                            other.cancel()
                        await asyncio.gather(
                            *still_pending,
                            return_exceptions=True,
                        )
                        self._tasks.difference_update(still_pending)
                        return

                pending = still_pending

            self._tasks.clear()

    asyncio.TaskGroup = _CompatTaskGroup  # type: ignore[attr-defined]
