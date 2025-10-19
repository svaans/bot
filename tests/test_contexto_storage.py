from __future__ import annotations

from typing import Any

import pytest

import core.contexto_externo as contexto
from core.contexto_externo import StreamContexto, obtener_puntaje_contexto
from core.contexto_storage import PuntajeStore


@pytest.mark.asyncio
async def test_puntaje_store_persists_and_expires(tmp_path: Any) -> None:
    current = {"value": 1_000_000.0}

    def _now() -> float:
        return current["value"]

    store = PuntajeStore(path=tmp_path / "puntajes.db", ttl_seconds=10, time_provider=_now)
    await store.set("BTCUSDT", 1.23, {"source": "test"})
    snapshot = await store.load_all()
    assert "BTCUSDT" in snapshot
    assert snapshot["BTCUSDT"].value == pytest.approx(1.23)

    current["value"] += 15
    await store.purge_expired()
    snapshot_after = await store.load_all()
    assert "BTCUSDT" not in snapshot_after


@pytest.mark.asyncio
async def test_stream_contexto_aplica_rest_snapshot(tmp_path: Any) -> None:
    current = {"value": 2_000_000.0}

    def _now() -> float:
        return current["value"]

    store = PuntajeStore(path=tmp_path / "puntajes_stream.db", ttl_seconds=120, time_provider=_now)
    stream = StreamContexto(rest_poll_interval=0, puntajes_store=store, puntajes_ttl=120)
    updates: list[tuple[str, float]] = []

    async def handler(symbol: str, puntaje: float) -> None:
        updates.append((symbol, puntaje))

    stream._handler_actual = handler  # type: ignore[attr-defined]
    stream._running = True

    backup_puntajes = dict(contexto._PUNTAJES)
    backup_datos = {k: dict(v) for k, v in contexto._DATOS_EXTERNOS.items()}
    try:
        snapshot = {
            "open_time": 1_700_000_000_000,
            "close_time": 1_700_000_300_000,
            "event_time": 1_700_000_300_000,
            "open": 100.0,
            "close": 102.0,
            "high": 103.0,
            "low": 99.5,
            "volume": 25.0,
            "timeframe": "5m",
        }
        await stream._apply_rest_snapshot("BTCUSDT", snapshot)
        assert obtener_puntaje_contexto("BTCUSDT") == pytest.approx(((102.0 - 100.0) / 100.0) * 25.0)
        stored = store.load_all_sync()
        assert "BTCUSDT" in stored
        assert updates and updates[0][0] == "BTCUSDT"
    finally:
        contexto._PUNTAJES.clear()
        contexto._PUNTAJES.update(backup_puntajes)
        contexto._DATOS_EXTERNOS.clear()
        contexto._DATOS_EXTERNOS.update(backup_datos)
        stream._running = False
        await stream.detener()