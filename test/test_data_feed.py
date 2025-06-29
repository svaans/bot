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


def test_stream_records_metrics_and_timeout(monkeypatch):
    received = []
    called = {}

    class FakeStream:
        def __init__(self):
            self.sent = False

        def __aiter__(self):
            return self

        async def __anext__(self):
            if not self.sent:
                self.sent = True
                return candle_pb2.Candle(
                    symbol="AAA",
                    timestamp=1,
                    open=1,
                    high=1,
                    low=1,
                    close=1,
                    volume=1,
                )
            await asyncio.sleep(3600)

    class FakeStub:
        def __init__(self, channel):
            pass

        def Subscribe(self, req, timeout=None):
            called["timeout"] = timeout
            return FakeStream()

    monkeypatch.setattr(candle_pb2_grpc, "CandleServiceStub", lambda ch: FakeStub(ch))

    feed = DataFeed("1m", timeout=0.5)

    async def handler(candle):
        received.append(candle)

    async def run():
        task = asyncio.create_task(feed.stream("AAA", handler))
        await asyncio.sleep(0.01)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    asyncio.run(run())

    assert received and received[0]["symbol"] == "AAA"
    assert called["timeout"] == 0.5
    metrics = feed.metricas("AAA")
    assert metrics["cuenta"] == 1
    assert metrics["ultimo"] is not None