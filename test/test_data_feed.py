import asyncio
import pytest

from core.data_feed import DataFeed
from core import candle_pb2, candle_pb2_grpc


def test_data_feed_streams_candles(monkeypatch):
    received = []

    async def fake_stream(self, symbol, handler):
        await handler({"symbol": symbol, "timestamp": 1})

    monkeypatch.setattr(DataFeed, "stream", fake_stream)

    feed = DataFeed("1m")

    async def handler(candle):
        received.append(candle)

    asyncio.run(feed.escuchar(["BTC/EUR"], handler))

    assert received == [{"symbol": "BTC/EUR", "timestamp": 1}]
    assert "BTC/EUR" not in feed._tasks


def test_stream_reraises_unexpected(monkeypatch):
    class FakeStub:
        def Subscribe(self, req):
            raise TypeError("boom")

        monkeypatch.setattr(candle_pb2_grpc, "CandleServiceStub", lambda ch: FakeStub())

    feed = DataFeed("1m")

    async def run():
        await feed.stream("AAA", lambda d: None)

    with pytest.raises(TypeError):
        asyncio.run(run())