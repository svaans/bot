import asyncio
import pytest
from core.data_feed import DataFeed


def test_data_feed_streams_candles(monkeypatch):
    received = []

    async def fake_listen(symbol, interval, handler):
        await handler({'symbol': symbol, 'timestamp': 1})
    monkeypatch.setattr('core.data_feed.escuchar_velas', fake_listen)
    feed = DataFeed('1m')

    async def handler(candle):
        received.append(candle)
    asyncio.run(feed.escuchar(['BTC/EUR'], handler))
    assert received == [{'symbol': 'BTC/EUR', 'timestamp': 1}]
    assert 'BTC/EUR' in feed._tasks
    assert feed._tasks['BTC/EUR'].done()
