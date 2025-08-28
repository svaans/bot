import asyncio
from datetime import datetime, timedelta, timezone

import pytest

from core.supervisor import Supervisor

UTC = timezone.utc


@pytest.mark.asyncio
async def test_watchdog_reinicia_tarea_sin_latido(monkeypatch):
    sup = Supervisor()
    sup.MAX_REINICIOS_WATCHDOG = 2
    mensajes: list[str] = []
    monkeypatch.setattr(sup.notificador, "enviar", lambda m, t='INFO': mensajes.append(m))

    async def tarea():
        await asyncio.sleep(10)

    sup.supervised_task(tarea, "demo")
    sup.task_heartbeat["demo"] = datetime.now(UTC) - timedelta(seconds=10)

    wd = asyncio.create_task(sup.watchdog(timeout=5, check_interval=0.05))
    await asyncio.sleep(0.2)
    sup.stop_event.set()
    await asyncio.wait_for(wd, 1)

    assert sup.reinicios_watchdog.get("demo", 0) == 1
    assert any("Reiniciando tarea demo" in m for m in mensajes)
    await sup.shutdown()