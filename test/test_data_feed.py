import asyncio
import pytest

from core.data_feed import DataFeed


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
    async def fake_open_connection(host, port):
        class R:
            async def readline(self):
                return b"{}"

        class W:
            def write(self, d):
                pass

            async def drain(self):
                pass

            def close(self):
                pass

            async def wait_closed(self):
                pass

        return R(), W()

    def bad_json(_):
        raise TypeError("boom")

    monkeypatch.setattr(asyncio, "open_connection", fake_open_connection)
    monkeypatch.setattr("core.data_feed.json.loads", bad_json)

    feed = DataFeed("1m")

    async def run():
        await feed.stream("AAA", lambda d: None)

    with pytest.raises(TypeError):
        asyncio.run(run())