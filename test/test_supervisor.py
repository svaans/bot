import asyncio
import pytest
from core.supervisor import supervised_task, tasks, task_heartbeat, tick

@pytest.mark.asyncio
async def test_tareas_supervisadas_no_se_quedan_congeladas():
    llamadas = []

    async def tarea():
        llamadas.append('run')
        if len(llamadas) == 1:
            raise ConnectionError('boom')
        await asyncio.sleep(0.05)
        tick('dummy')

    # crear tarea supervisada
    task = supervised_task(tarea, 'dummy')

    # pequeña vigilancia manual
    await asyncio.sleep(0.1)
    assert task.done()
    # reiniciar manualmente para simular _vigilancia_tareas
    supervised_task(tarea, 'dummy')
    await asyncio.sleep(0.1)
    assert len(llamadas) >= 2