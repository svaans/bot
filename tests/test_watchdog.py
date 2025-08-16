import time
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

import binance_api.websocket as ws


@pytest.mark.asyncio
async def test_watchdog_cierra_ws_si_inactivo(monkeypatch):
    symbol = 'BTC/USDT'
    last_message = {symbol: time.monotonic() - 11}
    ws_mock = SimpleNamespace(close=AsyncMock())

    async def fake_sleep(_):
        pass

    monkeypatch.setattr(ws.asyncio, 'sleep', fake_sleep)
    monkeypatch.setattr(ws, 'tick', lambda *args, **kwargs: None)
    monkeypatch.setattr(ws, 'tick_data', lambda *args, **kwargs: None)

    with pytest.raises(ws.InactividadTimeoutError):
        await ws._watchdog(ws_mock, symbol, last_message, 10)

    ws_mock.close.assert_awaited_once()