import asyncio
import contextlib

from core.contexto_externo import StreamContexto


def test_stream_contexto_reinicia(monkeypatch):
    llamadas = []

    async def fake_stream(self, symbol, handler):
        llamadas.append('run')
        await asyncio.sleep(0.01)

    async def fake_monitor(self):
        await asyncio.sleep(0.005)
        task = self._tasks.get('BTCUSDT')
        if task and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            self._tasks['BTCUSDT'] = asyncio.create_task(
                self._stream('BTCUSDT', self._handler_actual)
            )
        await asyncio.sleep(0.01)

    monkeypatch.setattr(StreamContexto, '_stream', fake_stream)
    monkeypatch.setattr(StreamContexto, '_monitor_inactividad', fake_monitor)

    sc = StreamContexto()

    async def handler(symbol, puntaje):
        pass

    async def run():
        task = asyncio.create_task(sc.escuchar(['BTCUSDT'], handler))
        await asyncio.sleep(0.05)
        await sc.detener()
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    asyncio.run(run())
    assert llamadas.count('run') >= 2