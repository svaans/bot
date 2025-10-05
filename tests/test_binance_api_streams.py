"""Tests for simulated Binance client and websocket streams."""
from __future__ import annotations

import asyncio
from typing import Dict

import pytest

from binance_api.cliente import (
    BinanceClient,
    crear_cliente,
    fetch_balance_async,
    fetch_ohlcv_async,
    fetch_ticker_async,
)
from binance_api import websocket as ws


@pytest.mark.asyncio
async def test_fetch_ohlcv_async_produces_deterministic_candles() -> None:
    """``fetch_ohlcv_async`` should return a stable deterministic sequence."""

    client = crear_cliente(api_key="key", api_secret="secret", testnet=True)
    candles = await fetch_ohlcv_async(client, "BTCUSDT", "1m", limit=3, since=0)

    assert candles == [
        [0.0, 99.9, 100.2, 99.8, 100.0, 50.0],
        [60000.0, 100.0, 100.3, 99.9, 100.1, 51.5],
        [120000.0, 100.1, 100.4, 100.0, 100.2, 53.0],
    ]


@pytest.mark.asyncio
async def test_fetch_balance_and_ticker_are_consistent() -> None:
    """Balance and ticker helpers return predictable structures."""

    client = BinanceClient(api_key="demo", api_secret="demo", testnet=False)
    balance = await fetch_balance_async(client)
    ticker = await fetch_ticker_async(client, "ETHUSDT")

    assert balance == {
        "total": {"USDT": 1000.0, "BUSD": 0.0},
        "free": {"USDT": 1000.0, "BUSD": 0.0},
    }
    # ``fetch_ticker_async`` hashes the symbol to build the offset.
    assert ticker == {"last": pytest.approx(100.0 + (abs(hash("ETHUSDT")) % 1000) / 100.0)}


@pytest.mark.asyncio
async def test_escuchar_velas_emits_candles(monkeypatch: pytest.MonkeyPatch) -> None:
    """The websocket stream should deliver structured candle payloads."""

    def deterministic_uniform(a: float, b: float) -> float:
        return (a + b) / 2.0

    monkeypatch.setattr(ws.random, "uniform", deterministic_uniform)

    captured: list[Dict[str, float | int | str | bool]] = []
    received = asyncio.Event()

    async def handler(candle: Dict[str, float | int | str | bool]) -> None:
        captured.append(candle)
        received.set()

    task = asyncio.create_task(
        ws.escuchar_velas(
            "BTCUSDT",
            "1m",
            handler,
            {},
            timeout_inactividad=5.0,
            _heartbeat=1.0,
            ultimo_timestamp=120000,
            ultimo_cierre=101.0,
        )
    )

    await asyncio.wait_for(received.wait(), timeout=2.0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert len(captured) == 1
    candle = captured[0]
    assert candle["symbol"] == "BTCUSDT"
    assert candle["intervalo"] == "1m"
    assert candle["open_time"] == 180000
    assert candle["close_time"] == 180000
    assert candle["open"] == pytest.approx(101.0)
    assert candle["close"] == pytest.approx(101.0)
    assert candle["high"] == pytest.approx(101.05)
    assert candle["low"] == pytest.approx(100.95)
    assert candle["volume"] == pytest.approx(30.0)
    assert candle["is_closed"] is True


@pytest.mark.asyncio
async def test_escuchar_velas_combinado_dispatches_to_handlers(monkeypatch: pytest.MonkeyPatch) -> None:
    """Multiple symbol stream should forward messages to each handler."""

    def deterministic_uniform(a: float, b: float) -> float:
        return (a + b) / 2.0

    monkeypatch.setattr(ws.random, "uniform", deterministic_uniform)

    results: Dict[str, Dict[str, float | int | str | bool]] = {}
    received = asyncio.Event()

    async def btc_handler(candle: Dict[str, float | int | str | bool]) -> None:
        results["BTCUSDT"] = candle
        if len(results) == 2:
            received.set()

    async def eth_handler(candle: Dict[str, float | int | str | bool]) -> None:
        results["ETHUSDT"] = candle
        if len(results) == 2:
            received.set()

    task = asyncio.create_task(
        ws.escuchar_velas_combinado(
            ["BTCUSDT", "ETHUSDT"],
            "1m",
            {"BTCUSDT": btc_handler, "ETHUSDT": eth_handler},
            {},
            timeout_inactividad=5.0,
            _heartbeat=1.0,
            ultimo_timestamp=60000,
            ultimo_cierre=99.5,
        )
    )

    await asyncio.wait_for(received.wait(), timeout=2.0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert set(results) == {"BTCUSDT", "ETHUSDT"}
    for symbol, candle in results.items():
        assert candle["symbol"] == symbol
        assert candle["intervalo"] == "1m"
        assert candle["open_time"] == 120000
        assert candle["close_time"] == 120000
        assert candle["is_closed"] is True
        assert candle["volume"] == pytest.approx(30.0)