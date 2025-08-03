import asyncio
import json

import pytest

from binance_api import websocket as ws_mod


class FakeWebSocket:
    def __init__(self):
        self.messages = [
            json.dumps(
                {
                    'e': 'kline',
                    'k': {
                        't': 0,
                        'o': '1',
                        'h': '1',
                        'l': '1',
                        'c': '1',
                        'v': '5',
                        'x': True,
                    },
                }
            ),
            json.dumps(
                {
                    'e': 'kline',
                    'k': {
                        't': 120000,
                        'o': '2',
                        'h': '2',
                        'l': '2',
                        'c': '2',
                        'v': '10',
                        'x': True,
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


def test_gap_fill(monkeypatch):
    received = []

    async def handler(candle):
        received.append(candle)

    async def fake_connect(*args, **kwargs):
        return FakeWebSocket()

    monkeypatch.setattr(ws_mod, 'websockets', type('W', (), {'connect': fake_connect}))
    monkeypatch.setattr(ws_mod, '_watchdog', fake_watchdog)
    monkeypatch.setattr(ws_mod, '_keepalive', fake_keepalive)
    monkeypatch.setattr(ws_mod, '_habilitar_tcp_keepalive', lambda ws: None)

    async def run():
        task = asyncio.create_task(
            ws_mod.escuchar_velas(
                'BTC/USDT',
                '1m',
                handler,
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

    assert len(received) == 3
    assert received[1]['timestamp'] == 60000
    assert received[1]['volume'] == 0.0
    assert received[1]['open'] == received[0]['close']