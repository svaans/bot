# tests/test_supervisor_watchdog.py
import asyncio
from datetime import timedelta
import pytest

from core.supervisor import Supervisor

@pytest.mark.asyncio
async def test_watchdog_emite_bot_inactivo(monkeypatch):
    """
    El watchdog debe emitir 'bot_inactivo' cuando no se reciben pulsos (heartbeat) en el tiempo configurado.
    """
    eventos = []
    def on_event(nombre, data):
        eventos.append((nombre, data))
    sup = Supervisor(on_event=on_event, watchdog_timeout=1, watchdog_check_interval=1)
    # simular que el último pulso fue hace 2 segundos
    sup.last_alive -= timedelta(seconds=2)
    await sup._watchdog_once(timeout=1)
    assert any(e[0] == "bot_inactivo" for e in eventos)

@pytest.mark.asyncio
async def test_watchdog_reinicia_tarea_inactiva(monkeypatch):
    """
    Cuando una tarea no envía heartbeats durante más tiempo que su timeout calculado,
    el watchdog debe llamar a restart_task().
    """
    sup = Supervisor(watchdog_timeout=1, watchdog_check_interval=1)
    llamado = []
    async def fake_restart(name):
        llamado.append(name)
    # sustituimos restart_task por un stub que registra la llamada
    monkeypatch.setattr(sup, "restart_task", fake_restart)
    # simulamos que la tarea 't' tuvo su último heartbeat hace 100s y sus intervalos previos eran cortos
    sup.task_heartbeat["t"] = sup._now() - timedelta(seconds=100)
    from collections import deque
    sup.task_intervals["t"] = deque([1], maxlen=100)
    # ejecutamos una pasada del watchdog
    await sup._watchdog_once(timeout=1)
    assert "t" in llamado, "El watchdog debió pedir reinicio de la tarea inactiva"

@pytest.mark.asyncio
async def test_watchdog_emite_sin_datos(monkeypatch):
    """
    El watchdog debe emitir 'sin_datos' cuando no se reciben datos (tick_data) en el periodo TIMEOUT_SIN_DATOS.
    """
    eventos = []
    def on_event(nombre, data):
        eventos.append((nombre, data))
    sup = Supervisor(on_event=on_event)
    # acortamos el timeout para no esperar 5 minutos
    sup.TIMEOUT_SIN_DATOS = 1
    # registramos un tick de datos hace 2s
    sup.data_heartbeat["BTCUSDT"] = sup._now() - timedelta(seconds=2)
    await sup._watchdog_once(timeout=1)
    assert any(e[0] == "sin_datos" for e in eventos), "Debe emitir sin_datos cuando no llegan velas"
