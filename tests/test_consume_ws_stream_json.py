"""Frames WebSocket malformados no deben tumbar el bucle de recepción."""

from __future__ import annotations

import asyncio
import contextlib
import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from binance_api import websocket as ws_mod


@pytest.mark.asyncio
async def test_consume_ws_stream_skips_invalid_json_then_calls_handler() -> None:
    calls: list[dict] = []

    async def handler(data: dict) -> None:
        calls.append(data)

    recv_n = 0

    async def recv() -> bytes | str:
        nonlocal recv_n
        recv_n += 1
        if recv_n == 1:
            return b"not valid json {{{"
        if recv_n == 2:
            return json.dumps({"a": 1})
        await asyncio.Event().wait()

    mock_ws = MagicMock()
    mock_ws.recv = recv

    cm = AsyncMock()
    cm.__aenter__.return_value = mock_ws
    cm.__aexit__.return_value = None

    with patch.object(ws_mod.websockets, "connect", return_value=cm):
        task = asyncio.create_task(
            ws_mod._consume_ws_stream(
                "wss://example.test/stream",
                handler,
                timeout_inactividad=60.0,
                mensaje_timeout=30.0,
            )
        )
        for _ in range(50):
            if calls:
                break
            await asyncio.sleep(0.01)
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    assert calls == [{"a": 1}]
