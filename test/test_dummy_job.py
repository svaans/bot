import asyncio
import pytest

from core.job_queue import Job, enqueue_job, worker

class DummyTrader:
    def __init__(self):
        self.called = False
    async def _verificar_salidas(self, symbol, df):
        self.called = True
    async def evaluar_condiciones_entrada(self, symbol, df):
        self.called = True
    def actualizar_fraccion_kelly(self):
        pass

@pytest.mark.asyncio
async def test_dummy_job_ignored():
    trader = DummyTrader()
    queue = asyncio.PriorityQueue(maxsize=10)
    await enqueue_job(queue, Job(priority=10, kind="dummy", symbol="", df=None))
    task = asyncio.create_task(worker("w", trader, queue, timeout=1, drop_policy="drop_oldest"))
    await queue.join()
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    assert not trader.called