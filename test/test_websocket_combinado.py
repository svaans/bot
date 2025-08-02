import asyncio
import json

import pytest

from binance_api import websocket as ws_mod


class FakeWebSocket:
    def __init__(self):
        self.messages = [
            json.dumps(
                {
                    'stream': 'btcusdt@kline_1m',
                    'data': {
                        'e': 'kline',
                        'k': {
                            't': 1,
                            'o': '1',
                            'h': '2',
                            'l': '0',
                            'c': '1.5',
                            'v': '10',
                            'x': True,
                        },
                    },
                }
            ),
            json.dumps(
                {
                    'stream': 'ethusdt@kline_1m',
                    'data': {
                        'e': 'kline',
                        'k': {
                            't': 2,
                            'o': '1',
                            'h': '2',
                            'l': '0',
                            'c': '1.5',
                            'v': '10',
                            'x': True,
                        },
                    },
                }
            ),
        ]
        self.index = 0

    async def recv(self):
        if self.index < len(self.messages):
            msg = self.messages[self.index]
            self.index += 1
            return msg
        await asyncio.sleep(3600)

    async def close(self):
        pass

    async def wait_closed(self):
        pass


async def fake_watchdog(*args, **kwargs):
    while True:
        await asyncio.sleep(3600)


async def fake_keepalive(*args, **kwargs):
    while True:
        await asyncio.sleep(3600)


def test_escuchar_velas_combinado(monkeypatch):
    received = {'BTC/USDT': [], 'ETH/USDT': []}

    async def handler_btc(candle):
        received['BTC/USDT'].append(candle)

    async def handler_eth(candle):
        received['ETH/USDT'].append(candle)

    async def fake_connect(*args, **kwargs):
        return FakeWebSocket()

    monkeypatch.setattr(ws_mod, 'websockets', type('W', (), {'connect': fake_connect}))
    monkeypatch.setattr(ws_mod, '_watchdog', fake_watchdog)
    monkeypatch.setattr(ws_mod, '_keepalive', fake_keepalive)
    monkeypatch.setattr(ws_mod, '_habilitar_tcp_keepalive', lambda ws: None)

    async def run():
        task = asyncio.create_task(
            ws_mod.escuchar_velas_combinado(
                ['BTC/USDT', 'ETH/USDT'],
                '1m',
                {
                    'BTC/USDT': handler_btc,
                    'ETH/USDT': handler_eth,
                },
                tiempo_maximo=1000,
                mensaje_timeout=None,
            )
        )
        await asyncio.sleep(0.1)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    asyncio.run(run())

    assert received['BTC/USDT'][0]['symbol'] == 'BTC/USDT'
    assert received['ETH/USDT'][0]['symbol'] == 'ETH/USDT'