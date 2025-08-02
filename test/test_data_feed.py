import asyncio
import pytest
from core.data_feed import DataFeed


def test_data_feed_streams_candles(monkeypatch):
    received = []

    async def fake_listen(symbol, interval, handler, *args, **kwargs):
        await handler({'symbol': symbol, 'timestamp': 1})
    monkeypatch.setattr('core.data_feed.escuchar_velas', fake_listen)
    feed = DataFeed('1m')

    async def handler(candle):
        received.append(candle)
    asyncio.run(feed.escuchar(['BTC/EUR'], handler))
    assert received == [{'symbol': 'BTC/EUR', 'timestamp': 1}]
    assert 'BTC/EUR' in feed._tasks
    assert feed._tasks['BTC/EUR'].done()

def test_stream_restarts_on_cancel(monkeypatch):
    calls = []

    async def fake_listen(symbol, interval, handler, *args, **kwargs):
        calls.append(symbol)
        await asyncio.sleep(0.01)

    monkeypatch.setattr('core.data_feed.escuchar_velas', fake_listen)

    async def fake_monitor(self, symbol):
        await asyncio.sleep(0.005)
        task = self._tasks.get(symbol)
        if task and not task.done():
            task.cancel()

    monkeypatch.setattr(DataFeed, '_monitor_activity', fake_monitor)

    async def fast_escuchar(self, symbols, handler):
        for sym in symbols:
            if sym in self._tasks:
                continue
            self._tasks[sym] = asyncio.create_task(self.stream(sym, handler))
        while self._tasks:
            tareas_actuales = list(self._tasks.items())
            resultados = await asyncio.gather(
                *[t for _, t in tareas_actuales], return_exceptions=True
            )
            reiniciar = {}
            for (sym, task), resultado in zip(tareas_actuales, resultados):
                if isinstance(resultado, asyncio.CancelledError):
                    reiniciar[sym] = asyncio.create_task(self.stream(sym, handler))
                    continue
                if isinstance(resultado, Exception):
                    reiniciar[sym] = asyncio.create_task(self.stream(sym, handler))
            if reiniciar:
                self._tasks.update(reiniciar)
            else:
                break

    monkeypatch.setattr(DataFeed, 'escuchar', fast_escuchar)

    feed = DataFeed('1m')

    async def run():
        try:
            await asyncio.wait_for(
                feed.escuchar(['BTC/EUR'], lambda c: None), timeout=0.05
            )
        except asyncio.TimeoutError:
            pass

    asyncio.run(run())

    assert len(calls) >= 2



def test_cliente_pasado_a_escuchar_velas(monkeypatch):
    recibido = []

    async def fake_listen(symbol, interval, handler, *args, **kwargs):
        recibido.append(kwargs.get('cliente'))
        raise asyncio.CancelledError

    monkeypatch.setattr('core.data_feed.escuchar_velas', fake_listen)
    feed = DataFeed('1m')

    async def handler(candle):
        pass
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(feed.escuchar(['BTC/EUR'], handler, cliente='dummy'))
    assert recibido == ['dummy']