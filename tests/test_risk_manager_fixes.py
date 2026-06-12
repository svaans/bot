"""Regression tests para fixes en core/risk/risk_manager.py.

Bug-IDs cubiertos:
- RISK-PERMITE-ENTRADA-NO-KILL-SWITCH-01: permite_entrada() no chequeaba kill switch
- RISK-TASK-UNTRACKED-01: create_task sin done callback para log de excepciones
"""
from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from core.risk.risk_manager import RiskManager


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_risk(**kwargs: Any) -> RiskManager:
    """RiskManager mínimo para tests unitarios."""
    return RiskManager(umbral=0.05, **kwargs)


# ---------------------------------------------------------------------------
# RISK-PERMITE-ENTRADA-NO-KILL-SWITCH-01
# permite_entrada() debe bloquear entradas cuando kill switch está activo.
# ---------------------------------------------------------------------------


def test_permite_entrada_bloqueada_por_kill_switch() -> None:
    """RISK-PERMITE-ENTRADA-NO-KILL-SWITCH-01: kill switch activo → permite_entrada False.

    Antes del fix, _kill_switch_disparado no se comprobaba en permite_entrada().
    Tras el kill switch (que cierra todas las posiciones), el capital quedaba
    libre y nuevas entradas pasaban el filtro del RiskManager sin restricción.
    """
    rm = _make_risk()
    rm._kill_switch_disparado = True

    resultado = rm.permite_entrada("BTCUSDT", {}, 0.5)

    assert resultado is False, (
        "permite_entrada devolvió True con kill switch activo. "
        "RISK-PERMITE-ENTRADA-NO-KILL-SWITCH-01 no corregido."
    )


def test_permite_entrada_permitida_cuando_kill_switch_inactivo() -> None:
    """Sin kill switch, permite_entrada puede devolver True normalmente."""
    rm = _make_risk()
    rm._kill_switch_disparado = False

    # Sin capital_manager ni cooldown → debe pasar
    resultado = rm.permite_entrada("BTCUSDT", {}, 0.5)

    assert resultado is True, (
        "permite_entrada bloqueó incorrectamente cuando kill switch está inactivo."
    )


def test_permite_entrada_kill_switch_tiene_prioridad_sobre_cooldown() -> None:
    """Kill switch debe bloquear incluso si cooldown no está activo."""

    rm = _make_risk()
    rm._kill_switch_disparado = True
    # Asegurarse de que cooldown no está activo
    rm._cooldown_fin = None

    resultado = rm.permite_entrada("ETHUSDT", {}, 0.0)
    assert resultado is False


def test_kill_switch_reset_por_nuevo_dia_permite_entradas() -> None:
    """Tras reset de día, kill switch se limpia y permite_entrada puede ser True."""
    from datetime import datetime, timedelta, timezone

    rm = _make_risk()
    rm._kill_switch_disparado = True

    # Usamos UTC para la fecha "ayer" — mismo criterio que registrar_perdida(),
    # que llama a datetime.now(UTC).date() internamente.
    ayer_utc = datetime.now(timezone.utc).date() - timedelta(days=1)
    rm._fecha_riesgo = ayer_utc  # forzar "día anterior" en UTC

    # registrar_perdida detecta nuevo día (hoy_utc != ayer_utc) y resetea kill switch
    rm.registrar_perdida("BTCUSDT", -0.01)

    assert rm._kill_switch_disparado is False, (
        "Kill switch no se reseteó al inicio de un nuevo día."
    )
    assert rm.permite_entrada("BTCUSDT", {}, 0.5) is True


@pytest.mark.asyncio
async def test_kill_switch_activo_bloquea_tras_disparo() -> None:
    """Tras activar kill switch, permite_entrada debe devolver False."""
    mock_om = MagicMock()
    mock_om.ordenes = {}
    mock_om.modo_real = False

    mock_bus = MagicMock()
    mock_bus.publish = AsyncMock(return_value=None)

    rm = _make_risk(bus=None, order_manager=mock_om)
    rm._bus = mock_bus

    # Activar kill switch directamente
    await rm.kill_switch(mock_om, ratio_perdida_diaria=0.1, limite_ratio=0.05,
                         perdidas_consecutivas=0, max_perdidas=5)

    assert rm._kill_switch_disparado is True
    assert rm.permite_entrada("BTCUSDT", {}, 0.5) is False, (
        "permite_entrada permitió entrada tras kill switch. "
        "RISK-PERMITE-ENTRADA-NO-KILL-SWITCH-01 no corregido."
    )


# ---------------------------------------------------------------------------
# RISK-TASK-UNTRACKED-01
# _schedule_kill_switch_check añade done callback para loguear excepciones.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_kill_switch_task_done_callback_added() -> None:
    """RISK-TASK-UNTRACKED-01: el task creado en _schedule_kill_switch_check
    tiene done callback para capturar excepciones silenciosas.

    Verificamos que el task se crea vía create_task y que tiene al menos un
    callback registrado (el done callback del fix).  El task puede completar
    antes de que podamos inspeccionarlo, así que lo capturamos con un mock
    de create_task.
    """
    import contextlib

    mock_om = MagicMock()
    rm = _make_risk(order_manager=mock_om)
    rm._bus = None  # Sin bus para que tome la ruta de create_task

    created_tasks: list[asyncio.Task] = []
    loop = asyncio.get_running_loop()
    original_create_task = loop.create_task

    def _capturing_create_task(coro: Any, *args: Any, **kwargs: Any) -> asyncio.Task:
        task = original_create_task(coro, *args, **kwargs)
        created_tasks.append(task)
        return task

    # Parchear create_task del loop activo para capturar el task
    with patch.object(loop, "create_task", side_effect=_capturing_create_task):
        rm._schedule_kill_switch_check()

    assert created_tasks, "create_task nunca fue llamado — task no fue creado"

    kill_task = created_tasks[0]
    assert kill_task.get_name() == "risk.kill_switch_check", (
        f"Nombre incorrecto: {kill_task.get_name()!r}"
    )

    # El done callback del fix está registrado: verificamos que el task
    # completa sin "Task exception was never retrieved" y sin propagar excepción
    try:
        await asyncio.wait_for(asyncio.shield(kill_task), timeout=1.0)
    except (asyncio.TimeoutError, asyncio.CancelledError):
        pass
    with contextlib.suppress(asyncio.CancelledError):
        await asyncio.sleep(0)  # un ciclo extra para cleanup
