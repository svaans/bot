from __future__ import annotations
import threading
import logging
import asyncio
import os
import time
from datetime import datetime
from functools import wraps
from typing import Awaitable, Callable

from core.utils.logger import configurar_logger

log = configurar_logger('supervisor')

last_alive = datetime.utcnow()
last_function = 'init'


def heartbeat(interval: int = 60) -> None:
    global last_alive
    while True:
        log.info("bot alive | last=%s", last_function)
        last_alive = datetime.utcnow()
        time.sleep(interval)


def watchdog(timeout: int = 120, check_interval: int = 10) -> None:
    while True:
        delta = (datetime.utcnow() - last_alive).total_seconds()
        if delta > timeout:
            log.critical(
                "\u26a0\ufe0f BOT INACTIVO desde hace %.1f segundos."
                " Ultima funcion: %s", delta, last_function
            )
        time.sleep(check_interval)


def start_supervision() -> None:
    """Lanza hilos de heartbeat y watchdog."""
    threading.Thread(target=heartbeat, daemon=True).start()
    threading.Thread(target=watchdog, daemon=True).start()

    def exception_handler(loop: asyncio.AbstractEventLoop, context: dict) -> None:
        exc = context.get("exception")
        if exc:
            log.critical(
                "Excepcion no controlada en loop: %s", exc, exc_info=exc
            )
        else:
            log.critical("Error en loop: %s", context.get("message"))

    loop = asyncio.get_event_loop()
    loop.set_exception_handler(exception_handler)

    def thread_excepthook(args: threading.ExceptHookArgs) -> None:
        log.critical(
            "Excepcion en hilo %s: %s", args.thread.name, args.exc_value,
            exc_info=(args.exc_type, args.exc_value, args.exc_traceback)
        )

    threading.excepthook = thread_excepthook


def supervised_task(coro_func: Callable[..., Awaitable]):
    """Envoltura para registrar excepciones y ultima funcion."""

    @wraps(coro_func)
    async def wrapper(*args, **kwargs):
        global last_function, last_alive
        last_function = coro_func.__name__
        last_alive = datetime.utcnow()
        try:
            resultado = coro_func(*args, **kwargs)
            if asyncio.iscoroutine(resultado):
                return await resultado
            return resultado
        except Exception as e:
            log.critical(
                "Error fatal en %s: %r", coro_func.__name__, e, exc_info=True
            )
            raise

    return wrapper