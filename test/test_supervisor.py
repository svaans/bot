import asyncio
import pytest
from core.supervisor import supervised_task, tick

@pytest.mark.asyncio
async def test_tareas_supervisadas_reinician_automatico():
    llamadas = []

    async def tarea():
        llamadas.append('run')
        if len(llamadas) == 1:
            raise RuntimeError('boom')
        await asyncio.sleep(0.05)
        tick('dummy')

    task = supervised_task(tarea, 'dummy', delay=0.01)
    await asyncio.sleep(0.2)
    task.cancel()
    await asyncio.sleep(0.05)
    assert len(llamadas) >= 2