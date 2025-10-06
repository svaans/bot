import asyncio
import logging
from collections.abc import Mapping
from types import SimpleNamespace

import pytest

from data_feed.lite import DataFeed as RealDataFeed

from core.trader.trader_lite import TraderLite


@pytest.mark.asyncio
async def test_data_feed_supervised_task_creates_and_runs(caplog, monkeypatch):
    caplog.set_level(logging.DEBUG)
    caplog.set_level(logging.DEBUG, logger="backfill_service")

    monkeypatch.setattr("core.trader_modular.DataFeed", RealDataFeed, raising=False)
    monkeypatch.setenv("BACKFILL_MIN_NEEDED", "30")
    monkeypatch.setenv("BACKFILL_WARMUP_EXTRA", "0")
    monkeypatch.setenv("BACKFILL_MODE", "A")
    monkeypatch.setenv("BACKFILL_ENABLED", "true")

    # --- STUB WS: escuchar_velas devuelve un async-iterable con velas cerradas ---
    produced: list[dict] = []

    async def _fake_escuchar_velas(*args, **kwargs) -> None:
        symbol = kwargs.get("symbol") or (args[0] if args else "BTC/EUR")
        intervalo = kwargs.get("intervalo") or (args[1] if len(args) > 1 else "1m")
        ultimo_ts = kwargs.get("ultimo_timestamp") or 0
        step_ms = 60_000
        if isinstance(intervalo, str) and intervalo.endswith("m"):
            try:
                step_ms = int(float(intervalo[:-1]) * 60_000)
            except ValueError:  # pragma: no cover - defensivo
                step_ms = 60_000
        start = ((int(ultimo_ts) // step_ms) + 1) * step_ms if ultimo_ts else step_ms

        handler = kwargs.get("handler") if "handler" in kwargs else args[2]

        for idx in range(3):
            ts = start + idx * step_ms
            candle = {
                "symbol": symbol,
                "intervalo": intervalo,
                "interval": intervalo,
                "timeframe": intervalo,
                "open_time": ts,
                "close_time": ts,
                "timestamp": ts,
                "open": 10.0,
                "high": 10.1,
                "low": 9.9,
                "close": 10.05,
                "volume": 1.0,
                "is_closed": True,
            }
            produced.append({"symbol": candle["symbol"], "timestamp": candle["timestamp"]})
            maybe = handler(candle)
            if asyncio.iscoroutine(maybe):
                await maybe
            await asyncio.sleep(0)

    async def _fake_escuchar_velas_combinado(*args, **kwargs) -> None:
        symbols = kwargs.get("symbols") or (args[0] if args else ["BTC/EUR"])
        intervalo = kwargs.get("intervalo") or (args[1] if len(args) > 1 else "1m")
        handler = kwargs.get("handler") if "handler" in kwargs else args[2]

        for index, symbol in enumerate(symbols):
            ts = (index + 1) * 60_000
            candle = {
                "symbol": symbol,
                "intervalo": intervalo,
                "interval": intervalo,
                "timeframe": intervalo,
                "open_time": ts,
                "close_time": ts,
                "timestamp": ts,
                "open": 20.0,
                "high": 20.1,
                "low": 19.9,
                "close": 20.05,
                "volume": 2.0,
                "is_closed": True,
            }
            produced.append({"symbol": candle["symbol"], "timestamp": candle["timestamp"]})

            if isinstance(handler, Mapping):
                target = handler.get(symbol)
                if target is None:
                    continue
                maybe = target(candle)
            else:
                maybe = handler(symbol, candle)

            if asyncio.iscoroutine(maybe):
                await maybe
            await asyncio.sleep(0)

    import binance_api
    import core.data_feed as core_data_feed

    monkeypatch.setattr(
        binance_api.websocket, "escuchar_velas", _fake_escuchar_velas, raising=True
    )
    monkeypatch.setattr(
        binance_api.websocket,
        "escuchar_velas_combinado",
        _fake_escuchar_velas_combinado,
        raising=True,
    )
    monkeypatch.setattr(
        "core.data_feed.escuchar_velas", _fake_escuchar_velas, raising=True
    )
    monkeypatch.setattr(
        "core.data_feed.escuchar_velas_combinado",
        _fake_escuchar_velas_combinado,
        raising=True,
    )
    original_describe_handler = core_data_feed.DataFeed._describe_handler

    def _describe_handler_sin_module(self, handler):
        info = original_describe_handler(self, handler)
        info.pop("module", None)
        return info

    monkeypatch.setattr(
        core_data_feed.DataFeed,
        "_describe_handler",
        _describe_handler_sin_module,
        raising=False,
    )

    config = SimpleNamespace(symbols=["BTC/EUR"], intervalo_velas="1m", modo_real=False)
    received: list[dict] = []
    recorded_messages: list[str] = []

    class _ListHandler(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:  # pragma: no cover - simple collector
            recorded_messages.append(record.getMessage())

    async def handler(candle: dict) -> None:
        received.append(candle)

    caplog.set_level(logging.DEBUG, logger="trader")
    caplog.set_level(logging.DEBUG, logger="trader_modular")

    trader = TraderLite(config, candle_handler=handler)
    trader.start()

    handler_collector = _ListHandler()
    handler_collector.setLevel(logging.DEBUG)
    datafeed_logger = logging.getLogger("datafeed")
    backfill_logger = logging.getLogger("backfill_service")
    datafeed_logger.addHandler(handler_collector)
    backfill_logger.addHandler(handler_collector)

    try:
        data_feed_task: asyncio.Task | None = None
        for _ in range(20):
            await asyncio.sleep(0.1)
            data_feed_task = trader.supervisor.tasks.get("data_feed")
            if data_feed_task is not None:
                break

        assert data_feed_task is not None, "Supervisor no registró la tarea del feed"
        assert isinstance(data_feed_task, asyncio.Task)
        assert not data_feed_task.done()

        await trader.start_backfill()

        for _ in range(40):
            if received:
                break
            await asyncio.sleep(0.2)
        assert produced, "El stub de WS no produjo velas"
        assert received, "El handler no recibió velas desde DataFeed"


    finally:
        await trader.stop()
        datafeed_logger.removeHandler(handler_collector)
        backfill_logger.removeHandler(handler_collector)

    assert any("suscrito" in msg for msg in recorded_messages)
    assert any("recv candle" in msg for msg in recorded_messages)
    assert any("page_fetched" in msg for msg in recorded_messages)
