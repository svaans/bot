import pytest
from unittest.mock import MagicMock
from datetime import datetime, timezone
from ccxt.base.errors import NetworkError

from core import monitor_estado_bot
from core.data_feed import DataFeed


def test_monitorear_estado_bot_logs_exception(monkeypatch):
    fake_log = MagicMock()
    monkeypatch.setattr(monitor_estado_bot, 'log', fake_log)

    def fake_balance():
        raise ValueError('boom')

    with pytest.raises(ValueError):
        monitor_estado_bot.monitorear_estado_bot(get_balance=fake_balance)

    fake_log.exception.assert_called_once()


@pytest.mark.asyncio
async def test_backfill_handles_network_error(monkeypatch):
    monkeypatch.setattr('core.data_feed.registrar_reconexion_datafeed', lambda cb: None)
    df = DataFeed('1m')
    df._cliente = object()
    now = int(datetime.now(timezone.utc).timestamp() * 1000)
    df._last_close_ts['BTC/USDT'] = now - df.intervalo_segundos * 2000

    async def fake_fetch(*args, **kwargs):
        raise NetworkError('net')

    fake_log = MagicMock()
    monkeypatch.setattr('core.data_feed.fetch_ohlcv_async', fake_fetch)
    monkeypatch.setattr('core.data_feed.log', fake_log)

    await df._backfill_candles('BTC/USDT')

    fake_log.warning.assert_called_once()


@pytest.mark.asyncio
async def test_backfill_logs_unexpected_exception(monkeypatch):
    monkeypatch.setattr('core.data_feed.registrar_reconexion_datafeed', lambda cb: None)
    df = DataFeed('1m')
    df._cliente = object()
    now = int(datetime.now(timezone.utc).timestamp() * 1000)
    df._last_close_ts['BTC/USDT'] = now - df.intervalo_segundos * 2000

    async def fake_fetch(*args, **kwargs):
        raise ValueError('boom')

    fake_log = MagicMock()
    monkeypatch.setattr('core.data_feed.fetch_ohlcv_async', fake_fetch)
    monkeypatch.setattr('core.data_feed.log', fake_log)

    await df._backfill_candles('BTC/USDT')

    fake_log.exception.assert_called_once()


@pytest.mark.asyncio
async def test_backfill_since_salta_duplicado(monkeypatch):
    monkeypatch.setattr('core.data_feed.registrar_reconexion_datafeed', lambda cb: None)
    df = DataFeed('1m')
    df._cliente = object()
    now = int(datetime.now(timezone.utc).timestamp() * 1000)
    last_ts = now - df.intervalo_segundos * 2000
    df._last_close_ts['BTC/USDT'] = last_ts
    llamado = {}

    async def fake_fetch(cliente, symbol, tf, since, limit):
        llamado['since'] = since
        return []

    monkeypatch.setattr('core.data_feed.fetch_ohlcv_async', fake_fetch)

    await df._backfill_candles('BTC/USDT')

    assert llamado['since'] == last_ts + 1
