import pytest

import binance_api.websocket as ws
from core import supervisor


def test_registrar_reconexion_y_tasa(monkeypatch):
    ws._reconnect_history.clear()
    monkeypatch.setattr(ws, 'RECONNECT_THRESHOLD', 2)
    messages = {}

    def fake_warning(msg: str) -> None:
        messages['msg'] = msg

    monkeypatch.setattr(ws.log, 'warning', fake_warning)
    ws._registrar_reconexion()
    ws._registrar_reconexion()
    ws._registrar_reconexion()
    assert 'Tasa de reconexión alta' in messages.get('msg', '')
    assert ws.obtener_tasa_reconexion() == 3


@pytest.mark.asyncio
async def test_registrar_reconexion_datafeed_callback():
    called = {}

    async def fake_cb(symbol: str) -> None:
        called['symbol'] = symbol

    supervisor.registrar_reconexion_datafeed(fake_cb)
    assert supervisor.data_feed_reconnector is fake_cb
    await supervisor.data_feed_reconnector('BTC/USDT')
    assert called['symbol'] == 'BTC/USDT'