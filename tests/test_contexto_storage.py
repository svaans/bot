from __future__ import annotations

from typing import Any, Iterator

import pytest

import math

import core.contexto_externo as contexto
from core.contexto_externo import StreamContexto, obtener_puntaje_contexto
from core.contexto_storage import PuntajeStore
from observability.metrics import (
    CONTEXT_LAST_UPDATE_SECONDS,
    CONTEXT_PARSING_ERRORS_TOTAL,
    CONTEXT_SCORE_DISTRIBUTION,
    CONTEXT_UPDATE_LATENCY_SECONDS,
    CONTEXT_VOLUME_EXTREME_TOTAL,
)


@pytest.fixture(autouse=True)
def reset_context_metrics() -> Iterator[None]:
    metrics = [
        CONTEXT_LAST_UPDATE_SECONDS,
        CONTEXT_SCORE_DISTRIBUTION,
        CONTEXT_UPDATE_LATENCY_SECONDS,
        CONTEXT_PARSING_ERRORS_TOTAL,
        CONTEXT_VOLUME_EXTREME_TOTAL,
    ]
    backups: list[tuple[dict, float | None, list | None]] = []
    for metric in metrics:
        children = metric._children  # type: ignore[attr-defined]
        value = getattr(metric, "_value", None)
        observations = getattr(metric, "_observations", None)
        backups.append((children, value, observations))
        metric._children = {}  # type: ignore[attr-defined]
        if hasattr(metric, "_value"):
            metric._value = 0.0  # type: ignore[attr-defined]
        if hasattr(metric, "_observations"):
            metric._observations = []  # type: ignore[attr-defined]
    try:
        yield
    finally:
        for metric, (children, value, observations) in zip(metrics, backups):
            metric._children = children  # type: ignore[attr-defined]
            if value is not None and hasattr(metric, "_value"):
                metric._value = value  # type: ignore[attr-defined]
            if observations is not None and hasattr(metric, "_observations"):
                metric._observations = observations  # type: ignore[attr-defined]


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
        expected = math.log(snapshot["close"] / snapshot["open"]) * math.log1p(1.0)
        assert obtener_puntaje_contexto("BTCUSDT") == pytest.approx(expected)
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


@pytest.mark.asyncio
async def test_stream_contexto_actualiza_metricas(tmp_path: Any) -> None:
    store = PuntajeStore(path=tmp_path / "puntajes_metricas.db", ttl_seconds=120)
    stream = StreamContexto(rest_poll_interval=0, puntajes_store=store, puntajes_ttl=120)
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
        gauge = CONTEXT_LAST_UPDATE_SECONDS.labels("BTCUSDT")
        assert gauge._value == pytest.approx(1_700_000_300.0)  # type: ignore[attr-defined]
        hist = CONTEXT_SCORE_DISTRIBUTION.labels("BTCUSDT")
        assert hist._observations  # type: ignore[attr-defined]
        expected = math.log(snapshot["close"] / snapshot["open"]) * math.log1p(1.0)
        assert hist._observations[-1] == pytest.approx(expected)  # type: ignore[attr-defined]
        latency = CONTEXT_UPDATE_LATENCY_SECONDS.labels("BTCUSDT", "rest")
        assert latency._observations  # type: ignore[attr-defined]
        assert latency._observations[-1] >= 0  # type: ignore[attr-defined]
    finally:
        contexto._PUNTAJES.clear()
        contexto._PUNTAJES.update(backup_puntajes)
        contexto._DATOS_EXTERNOS.clear()
        contexto._DATOS_EXTERNOS.update(backup_datos)
        stream._running = False
        await stream.detener()


def test_stream_contexto_registra_error_parseo() -> None:
    stream = StreamContexto(rest_poll_interval=0)
    stream._record_parsing_error("ETHUSDT", stage="unit_test", exc=ValueError("boom"))
    counter = CONTEXT_PARSING_ERRORS_TOTAL.labels("ETHUSDT", "unit_test")
    assert counter._value == pytest.approx(1.0)  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_stream_contexto_detecta_volumen_extremo(tmp_path: Any) -> None:
    store = PuntajeStore(path=tmp_path / "puntajes_volumen.db", ttl_seconds=120)
    stream = StreamContexto(
        rest_poll_interval=0,
        puntajes_store=store,
        puntajes_ttl=120,
        volume_window=5,
        volume_baseline_min=3,
        volume_extreme_multiplier=2.0,
    )
    stream._running = True
    backup_puntajes = dict(contexto._PUNTAJES)
    backup_datos = {k: dict(v) for k, v in contexto._DATOS_EXTERNOS.items()}
    try:
        base_snapshot = {
            "open_time": 1_700_000_000_000,
            "close_time": 1_700_000_300_000,
            "event_time": 1_700_000_300_000,
            "open": 100.0,
            "close": 100.5,
            "high": 101.0,
            "low": 99.5,
            "timeframe": "5m",
        }
        for idx, volume in enumerate((10.0, 11.0, 9.0), start=1):
            snapshot = dict(base_snapshot)
            snapshot["volume"] = volume
            snapshot["open_time"] += idx * 60000
            snapshot["close_time"] += idx * 60000
            snapshot["event_time"] += idx * 60000
            await stream._apply_rest_snapshot("ETHUSDT", snapshot)

        extreme_snapshot = dict(base_snapshot)
        extreme_snapshot.update(
            {
                "volume": 35.0,
                "open_time": base_snapshot["open_time"] + 4 * 60000,
                "close_time": base_snapshot["close_time"] + 4 * 60000,
                "event_time": base_snapshot["event_time"] + 4 * 60000,
            }
        )
        await stream._apply_rest_snapshot("ETHUSDT", extreme_snapshot)
        counter = CONTEXT_VOLUME_EXTREME_TOTAL.labels("ETHUSDT", "rest")
        assert counter._value == pytest.approx(1.0)  # type: ignore[attr-defined]
    finally:
        contexto._PUNTAJES.clear()
        contexto._PUNTAJES.update(backup_puntajes)
        contexto._DATOS_EXTERNOS.clear()
        contexto._DATOS_EXTERNOS.update(backup_datos)
        stream._running = False
        await stream.detener()