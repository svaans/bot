"""Utilidades de diagnóstico para tareas del ``DataFeed``."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Iterable


@dataclass(frozen=True)
class TaskSnapshot:
    """Resumen inmutable del estado actual de una ``asyncio.Task``."""

    name: str
    pending: bool
    done: bool
    cancelled: bool

    @classmethod
    def from_task(cls, task: asyncio.Task) -> "TaskSnapshot":
        """Crea un ``TaskSnapshot`` a partir de la tarea proporcionada."""

        return cls(
            name=task.get_name(),
            pending=not task.done(),
            done=task.done(),
            cancelled=task.cancelled(),
        )


def _resolve_loop(loop: asyncio.AbstractEventLoop | None) -> asyncio.AbstractEventLoop:
    if loop is not None:
        return loop
    try:
        return asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.get_event_loop()


def _find_first_by_name(tasks: Iterable[asyncio.Task[object]], names: set[str]) -> asyncio.Task[object] | None:
    for task in tasks:
        if task.get_name() in names:
            return task
    return None


@dataclass(frozen=True)
class ConsumerStreamSnapshot:
    """Estado agregado de las tareas de consumidor y stream."""

    consumer: TaskSnapshot | None
    stream: TaskSnapshot | None
    pending: dict[str, bool] = field(default_factory=dict)


def snapshot_consumer_stream(
    symbol: str,
    *,
    loop: asyncio.AbstractEventLoop | None = None,
) -> ConsumerStreamSnapshot:
    """Devuelve un snapshot del estado de las tareas ``consumer`` y ``stream`` para ``symbol``.

    Permite comprobar desde un ``Supervisor`` o un REPL que las tareas críticas del
    *data feed* siguen activas. Si únicamente queda vivo el consumidor, suele indicar
    que el productor se detuvo.
    """

    event_loop = _resolve_loop(loop)
    tasks = asyncio.all_tasks(event_loop)
    consumer_task = _find_first_by_name(tasks, {f"consumer_{symbol}"})
    stream_task = _find_first_by_name(tasks, {f"stream_{symbol}", "stream_combinado"})

    pending = {
        task.get_name(): not task.done()
        for task in tasks
        if task.get_name().startswith(("consumer_", "stream_"))
    }

    return ConsumerStreamSnapshot(
        consumer=TaskSnapshot.from_task(consumer_task) if consumer_task else None,
        stream=TaskSnapshot.from_task(stream_task) if stream_task else None,
        pending=pending,
    )


__all__ = ["TaskSnapshot", "ConsumerStreamSnapshot", "snapshot_consumer_stream"]
