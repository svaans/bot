import asyncio
import logging
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

    config = SimpleNamespace(symbols=["BTCUSDT"], intervalo_velas="1m", modo_real=False)
    received: list[dict] = []
    recorded_messages: list[str] = []

    class _ListHandler(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:  # pragma: no cover - simple collector
            recorded_messages.append(record.getMessage())

    async def handler(candle: dict) -> None:
        received.append(candle)

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

        for _ in range(30):
            if received:
                break
            await asyncio.sleep(0.2)
        assert received, "El handler no recibió velas desde DataFeed"

        # Forza un backfill ligero para verificar logs del servicio
        await trader.start_backfill()

    finally:
        await trader.stop()
        datafeed_logger.removeHandler(handler_collector)
        backfill_logger.removeHandler(handler_collector)

    assert any("suscrito" in msg for msg in recorded_messages)
    assert any("recv candle" in msg for msg in recorded_messages)
    assert any("page_fetched" in msg for msg in recorded_messages)
