"""Utilidades asincrónicas para gestión de tareas."""

from __future__ import annotations

import asyncio
import functools
import logging
import traceback
from typing import Iterable, Set


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


def log_exceptions_async(func):
    """Decorador para registrar excepciones de corrutinas."""

    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # pragma: no cover - logging defensivo
            logging.getLogger(func.__module__).exception(
                f"Unhandled exception in {func.__name__}", exc_info=exc
            )
            raise

    return wrapper


def dump_tasks_stacktraces(tasks: Iterable[asyncio.Task] | None = None) -> str:
    """Devuelve un volcado de las pilas de ``tasks``."""
    if tasks is None:
        tasks = asyncio.all_tasks()
    lines: list[str] = []
    for t in tasks:
        lines.append(f"Task {t.get_name()} state={t._state}")
        stack = t.get_stack()
        if not stack:
            lines.append("  <no stack>\n")
            continue
        for frame in stack:
            lines.extend(traceback.format_list(traceback.extract_stack(frame)))
    return "".join(lines)


__all__ = ["TaskManager", "log_exceptions_async", "dump_tasks_stacktraces"]
