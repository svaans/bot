"""Regression tests para fixes en core/supervisor.py.

Bug-IDs cubiertos:
- SUPER-RESTART-CANCEL-LEAK-01: except Exception no atrapaba CancelledError en restart_task(),
  lo que podía terminar el watchdog silenciosamente.
"""
from __future__ import annotations

import asyncio
from typing import Any

import pytest

from core.supervisor import Supervisor


# ---------------------------------------------------------------------------
# SUPER-RESTART-CANCEL-LEAK-01
# restart_task() debe absorber CancelledError de una tarea ya cancelada.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_restart_task_absorbs_cancelled_error_from_cancelled_task() -> None:
    """SUPER-RESTART-CANCEL-LEAK-01: restart_task() no debe propagar CancelledError
    cuando la tarea que se cancela lanza CancelledError al ser esperada.

    Antes del fix, `except Exception: pass` dejaba escapar CancelledError
    (BaseException en Python 3.8+), lo que terminaba el watchdog silenciosamente.
    """
    sup = Supervisor()

    # Tarea que duerme indefinidamente — el cancel() la terminará con CancelledError
    async def _long_task() -> None:
        await asyncio.sleep(9999)

    task_name = "test_long_task"
    sup.supervised_task(_long_task, name=task_name)
    await asyncio.sleep(0)  # permite que la tarea arranque

    # restart_task NO debe propagar CancelledError al caller
    try:
        await sup.restart_task(task_name)
    except asyncio.CancelledError:
        pytest.fail(
            "restart_task() propagó CancelledError. "
            "SUPER-RESTART-CANCEL-LEAK-01 no corregido."
        )
    except BaseException as exc:
        pytest.fail(f"restart_task() propagó excepción inesperada: {exc!r}")

    # Cleanup: cancelar la tarea de restart que pudo haberse creado
    for t in asyncio.all_tasks():
        if t.get_name().endswith("_restart") or t.get_name() == task_name:
            t.cancel()
            try:
                await t
            except (asyncio.CancelledError, Exception):
                pass


@pytest.mark.asyncio
async def test_watchdog_survives_restart_of_cancelled_task() -> None:
    """SUPER-RESTART-CANCEL-LEAK-01: el watchdog no debe morir al reiniciar
    una tarea que fue cancelada externamente antes de que el watchdog actúe.

    Simula el escenario completo: tarea cancelada externamente → watchdog
    detecta heartbeat vencido → llama restart_task → watchdog debe sobrevivir.
    """
    sup = Supervisor(watchdog_timeout=1, watchdog_check_interval=1)

    async def _placeholder() -> None:
        await asyncio.sleep(9999)

    task_name = "test_watchdog_task"
    sup.supervised_task(_placeholder, name=task_name)
    await asyncio.sleep(0)

    # Cancelar la tarea directamente (simula cancelación externa)
    t = sup.tasks.get(task_name)
    if t and not t.done():
        t.cancel()
        try:
            await t
        except (asyncio.CancelledError, Exception):
            pass

    # Forzar un tick vencido para que el watchdog detecte la tarea inactiva
    from datetime import datetime, timezone, timedelta
    sup.task_heartbeat[task_name] = datetime.now(timezone.utc) - timedelta(seconds=500)
    sup.tasks[task_name] = asyncio.current_task()  # type: ignore[assignment]
    # Reemplazar con una tarea cancellable real (ya finalizada)
    done_task: asyncio.Task[None] = asyncio.ensure_future(asyncio.sleep(0))
    await done_task  # completarla
    sup.tasks[task_name] = done_task

    # _watchdog_once no debe propagar CancelledError
    try:
        await sup._watchdog_once(1)
    except asyncio.CancelledError:
        pytest.fail(
            "_watchdog_once() propagó CancelledError. "
            "SUPER-RESTART-CANCEL-LEAK-01 no corregido."
        )

    # Cleanup
    for t in asyncio.all_tasks():
        if t.get_name().endswith("_restart") or t.get_name() == task_name:
            t.cancel()
            try:
                await t
            except (asyncio.CancelledError, Exception):
                pass


@pytest.mark.asyncio
async def test_restart_task_without_existing_task_does_not_raise() -> None:
    """restart_task() con nombre de tarea desconocido no lanza excepción."""
    sup = Supervisor()

    try:
        await sup.restart_task("nonexistent_task")
    except Exception as exc:
        pytest.fail(f"restart_task() con tarea inexistente lanzó: {exc!r}")

    # Cleanup
    for t in asyncio.all_tasks():
        if "nonexistent_task" in t.get_name():
            t.cancel()
            try:
                await t
            except (asyncio.CancelledError, Exception):
                pass


@pytest.mark.asyncio
async def test_restart_task_after_timeout_does_not_propagate() -> None:
    """SUPER-RESTART-CANCEL-LEAK-01: CancelledError de wait_for timeout tampoco escapa."""
    sup = Supervisor(close_timeout=0.01)  # timeout muy corto

    async def _slow_cancel() -> None:
        try:
            await asyncio.sleep(9999)
        except asyncio.CancelledError:
            # Tarda en responder al cancel
            await asyncio.sleep(0.5)
            raise

    task_name = "slow_cancel_task"
    sup.supervised_task(_slow_cancel, name=task_name)
    await asyncio.sleep(0)

    try:
        await sup.restart_task(task_name)
    except asyncio.CancelledError:
        pytest.fail(
            "restart_task() propagó CancelledError con timeout corto. "
            "SUPER-RESTART-CANCEL-LEAK-01 no corregido."
        )

    # Cleanup
    for t in asyncio.all_tasks():
        if task_name in t.get_name() or t.get_name().endswith("_restart"):
            t.cancel()
            try:
                await t
            except (asyncio.CancelledError, Exception):
                pass
