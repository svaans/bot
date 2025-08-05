import asyncio
import contextlib
import pytest
from core.data_feed import DataFeed


def test_data_feed_streams_candles(monkeypatch):
    received = []

    async def fake_listen(symbol, interval, handler, *args, **kwargs):
        await handler({'symbol': symbol, 'timestamp': 1})
    monkeypatch.setattr('core.data_feed.escuchar_velas', fake_listen)

    combined_called = False

    async def fake_listen_combinado(*args, **kwargs):
        nonlocal combined_called
        combined_called = True

    monkeypatch.setattr('core.data_feed.escuchar_velas_combinado', fake_listen_combinado)

    feed = DataFeed('1m')

    async def handler(candle):
        received.append(candle)

    async def run():
        task = asyncio.create_task(feed.escuchar(['BTC/EUR'], handler))
        await asyncio.sleep(0.01)
        await feed.detener()
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    asyncio.run(run())
    assert received == [{'symbol': 'BTC/EUR', 'timestamp': 1}]
    assert not combined_called


def test_data_feed_streams_candles_combinado(monkeypatch):
    recibidos = []
    handlers_capturados = {}

    async def fake_listen(symbols, interval, handlers, *args, **kwargs):
        handlers_capturados.update(handlers)
        for s in symbols:
            await handlers[s]({'symbol': s, 'timestamp': 1})

    monkeypatch.setattr('core.data_feed.escuchar_velas_combinado', fake_listen)

    async def fake_individual(*args, **kwargs):
        raise AssertionError('escuchar_velas no debe ser llamado en modo combinado')

    monkeypatch.setattr('core.data_feed.escuchar_velas', fake_individual)
    feed = DataFeed('1m', usar_stream_combinado=True)

    async def handler(candle):
        recibidos.append(candle)

    async def run():
        task = asyncio.create_task(
            feed.escuchar(['BTC/EUR', 'ETH/EUR'], handler)
        )
        await asyncio.sleep(0.01)
        await feed.detener()
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    asyncio.run(run())
    assert recibidos == [
        {'symbol': 'BTC/EUR', 'timestamp': 1},
        {'symbol': 'ETH/EUR', 'timestamp': 1},
    ]
    assert sorted(handlers_capturados.keys()) == ['BTC/EUR', 'ETH/EUR']
    assert handlers_capturados['BTC/EUR'] is not handlers_capturados['ETH/EUR']

def test_stream_restarts_on_cancel(monkeypatch):
    calls = []

    async def fake_listen(symbol, interval, handler, *args, **kwargs):
        calls.append(symbol)
        await asyncio.sleep(0.01)

    monkeypatch.setattr('core.data_feed.escuchar_velas', fake_listen)

    combined_called = False

    async def fake_listen_combinado(*args, **kwargs):
        nonlocal combined_called
        combined_called = True

    monkeypatch.setattr('core.data_feed.escuchar_velas_combinado', fake_listen_combinado)

    async def fake_monitor(self):
        await asyncio.sleep(0.005)
        task = self._tasks.get('BTC/EUR')
        if task and not task.done():
            task.cancel()

            await asyncio.gather(task, return_exceptions=True)
            self._tasks['BTC/EUR'] = asyncio.create_task(
                self.stream('BTC/EUR', self._handler_actual)
            )
        await asyncio.sleep(0.01)

    monkeypatch.setattr(DataFeed, '_monitor_global_inactividad', fake_monitor)

    feed = DataFeed('1m')

    async def run():
        task = asyncio.create_task(feed.escuchar(['BTC/EUR'], lambda c: None))
        await asyncio.sleep(0.05)
        await feed.detener()
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    asyncio.run(run())

    assert len(calls) >= 2
    assert not combined_called



def test_cliente_pasado_a_escuchar_velas(monkeypatch):
    recibido = []

    async def fake_listen(symbol, interval, handler, *args, **kwargs):
        recibido.append(kwargs.get('cliente'))
        raise asyncio.CancelledError

    monkeypatch.setattr('core.data_feed.escuchar_velas', fake_listen)

    combined_called = False

    async def fake_listen_combinado(*args, **kwargs):
        nonlocal combined_called
        combined_called = True

    monkeypatch.setattr('core.data_feed.escuchar_velas_combinado', fake_listen_combinado)
    feed = DataFeed('1m')

    async def handler(candle):
        pass
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(feed.escuchar(['BTC/EUR'], handler, cliente='dummy'))
    assert recibido == ['dummy']
    assert not combined_called


def test_global_reconnect_on_combined_cancel(monkeypatch):
    calls = []

    async def fake_stream_combinado(self, symbols, handler):
        calls.append('run')
        await asyncio.sleep(0.01)
        while True:
            await asyncio.sleep(1)

    monkeypatch.setattr(DataFeed, '_stream_combinado', fake_stream_combinado)

    async def fake_monitor(self):
        task = self._tasks.get('combined')
        if task and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            self._tasks['combined'] = asyncio.create_task(
                self._stream_combinado(self._symbols, self._handler_actual)
            )

    monkeypatch.setattr(DataFeed, '_monitor_global_inactividad', fake_monitor)

    feed = DataFeed('1m', usar_stream_combinado=True)

    async def handler(candle):
        pass

    async def run():
        task = asyncio.create_task(
            feed.escuchar(['BTC/EUR', 'ETH/EUR'], handler)
        )
        await asyncio.sleep(0.05)
        await feed._monitor_global_inactividad()
        await asyncio.sleep(0.05)
        await feed.detener()
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    asyncio.run(run())

    assert calls.count('run') >= 2


def test_inactivity_intervals_parameter():
    feed = DataFeed('5m', inactivity_intervals=2)
    assert feed.tiempo_inactividad == 600

def test_handler_timeout(monkeypatch):
    cancelado = False

    async def fake_listen(symbol, interval, handler, *args, **kwargs):
        await handler({'symbol': symbol, 'timestamp': 1})

    monkeypatch.setattr('core.data_feed.escuchar_velas', fake_listen)

    feed = DataFeed('1m', handler_timeout=0.01)

    async def handler(candle):
        nonlocal cancelado
        try:
            await asyncio.sleep(0.02)
        except asyncio.CancelledError:
            cancelado = True
            raise

    async def run():
        task = asyncio.create_task(feed.escuchar(['BTC/EUR'], handler))
        await asyncio.sleep(0.05)
        await feed.detener()
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    asyncio.run(run())
    assert cancelado


def test_monitor_interval_minimum():
    feed = DataFeed('1m', monitor_interval=0)
    assert feed.monitor_interval == 1


def test_activos_after_relaunch(monkeypatch):
    runs = []

    async def fake_listen(symbol, interval, handler, *args, **kwargs):
        runs.append(symbol)
        await asyncio.sleep(0.01)

    monkeypatch.setattr('core.data_feed.escuchar_velas', fake_listen)

    async def fake_monitor(self):
        await asyncio.sleep(0.005)
        task = self._tasks.get('BTC/EUR')
        if task and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            self._tasks['BTC/EUR'] = asyncio.create_task(
                self.stream('BTC/EUR', self._handler_actual)
            )
        await asyncio.sleep(0.01)

    monkeypatch.setattr(DataFeed, '_monitor_global_inactividad', fake_monitor)

    feed = DataFeed('1m')

    async def run():
        task = asyncio.create_task(
            feed.escuchar(['BTC/EUR', 'ETH/EUR'], lambda c: None)
        )
        await asyncio.sleep(0.05)
        activos = feed.activos
        await feed.detener()
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
        return activos

    activos = asyncio.run(run())
    assert sorted(activos) == ['BTC/EUR', 'ETH/EUR']
    assert runs.count('BTC/EUR') >= 2
