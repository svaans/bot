from __future__ import annotations
import threading
import logging
import asyncio
import os
import time
import traceback
from datetime import datetime
from typing import Awaitable, Callable, Dict

from core.utils.logger import configurar_logger

log = configurar_logger('supervisor')

last_alive = datetime.utcnow()
last_function = 'init'
tasks: Dict[str, asyncio.Task] = {}
task_heartbeat: Dict[str, datetime] = {}


def tick(name: str) -> None:
    """Actualiza la función y marca latido para ``name``."""
    global last_alive, last_function
    last_function = name
    last_alive = datetime.utcnow()
    task_heartbeat[name] = last_alive


def heartbeat(interval: int = 60) -> None:
    """Emite latidos periódicos para el proceso principal."""
    global last_alive
    while True:
        log.info("bot alive | last=%s", last_function)
        last_alive = datetime.utcnow()
        time.sleep(interval)


def watchdog(timeout: int = 120, check_interval: int = 10) -> None:
    """Valida que el proceso siga activo e imprime trazas si se congela."""
    while True:
        delta = (datetime.utcnow() - last_alive).total_seconds()
        if delta > timeout:
            log.critical(
                "\u26a0\ufe0f BOT INACTIVO desde hace %.1f segundos. Ultima funcion: %s",
                delta,
                last_function,
            )
            for nombre, task in tasks.items():
                try:
                    stack = "\n".join(traceback.format_stack(task.get_stack()))
                    log.critical("Stack de %s:\n%s", nombre, stack)
                except Exception:
                    pass
        time.sleep(check_interval)


def start_supervision() -> None:
    """Lanza hilos de heartbeat y watchdog y configura manejo de excepciones."""
    threading.Thread(target=heartbeat, daemon=True).start()
    threading.Thread(target=watchdog, daemon=True).start()

    def exception_handler(loop: asyncio.AbstractEventLoop, context: dict) -> None:
        exc = context.get("exception")
        if exc:
            log.critical("Excepcion no controlada en loop: %s", exc, exc_info=exc)
        else:
            log.critical("Error en loop: %s", context.get("message"))

    loop = asyncio.get_event_loop()
    loop.set_exception_handler(exception_handler)

    def thread_excepthook(args: threading.ExceptHookArgs) -> None:
        log.critical(
            "Excepcion en hilo %s: %s",
            args.thread.name,
            args.exc_value,
            exc_info=(args.exc_type, args.exc_value, args.exc_traceback),
        )

    threading.excepthook = thread_excepthook


def supervised_task(coro_factory: Callable[..., Awaitable], name: str | None = None) -> asyncio.Task:
    """Crea una tarea supervisada que registra excepciones y actividad."""

    task_name = name or getattr(coro_factory, "__name__", "task")

    async def runner() -> None:
        tick(task_name)
        try:
            result = coro_factory()
            if asyncio.iscoroutine(result):
                await result
        except Exception as e:  # pragma: no cover - log crítico
            log.critical("Error fatal en %s: %r", task_name, e, exc_info=True)
            raise

    task = asyncio.create_task(runner(), name=task_name)
    tasks[task_name] = task
    return task

__all__ = ["start_supervision", "supervised_task", "tick", "tasks", "task_heartbeat"]