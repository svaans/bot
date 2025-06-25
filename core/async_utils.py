"""Utilidades asincrónicas para gestión de tareas."""

from __future__ import annotations

import asyncio
from typing import Set


class TaskManager:
    """Gestiona un conjunto de tareas asincrónicas."""

    def __init__(self) -> None:
        self._tasks: Set[asyncio.Task] = set()

    def add_task(self, task: asyncio.Task) -> None:
        """Registra ``task`` para su seguimiento."""
        self._tasks.add(task)
        task.add_done_callback(lambda t: self._tasks.discard(t))

    async def cancel_all(self) -> None:
        """Cancela todas las tareas registradas y espera su finalización."""
        for task in list(self._tasks):
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()

    async def wait_all(self) -> None:
        """Espera a que todas las tareas finalicen."""
        await asyncio.gather(*self._tasks, return_exceptions=True)


__all__ = ["TaskManager"]