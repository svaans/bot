from __future__ import annotations

import json
import queue
import sqlite3
import time
from pathlib import Path

import pytest

from estado.evaluaciones_repo import EvaluacionRepository, PersistenceQueueFullError


def _read_all(path: Path) -> list[sqlite3.Row]:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    try:
        return list(conn.execute("SELECT * FROM evaluaciones"))
    finally:
        conn.close()


def test_repository_persists_payload(tmp_path: Path) -> None:
    db_path = tmp_path / "evaluaciones.db"
    repo = EvaluacionRepository(path=db_path)
    try:
        repo.save_evaluacion(
            symbol="BTCUSDT",
            timeframe="5m",
            side="long",
            score=87.5,
            persistencia_ok=True,
            meta={"persistencia_ok": True, "gate": "entry"},
        )
        repo.flush(timeout=2.0)
    finally:
        repo.close()

    rows = _read_all(db_path)
    assert len(rows) == 1
    row = rows[0]
    assert row["symbol"] == "BTCUSDT"
    assert row["timeframe"] == "5m"
    assert row["side"] == "long"
    assert row["score"] == pytest.approx(87.5)
    assert row["persistencia_ok"] == 1
    meta = json.loads(row["meta"])
    assert meta["gate"] == "entry"
    payload = json.loads(row["payload"])
    assert payload["symbol"] == "BTCUSDT"


def test_repository_stats_reflect_inserts(tmp_path: Path) -> None:
    repo = EvaluacionRepository(path=tmp_path / "stats.db")
    try:
        for idx in range(3):
            repo.save_evaluacion(symbol="ETHUSDT", timeframe="1h", side="long", score=idx)
        repo.flush(timeout=2.0)
        stats = repo.stats()
    finally:
        repo.close()

    assert stats.inserted == 3
    assert stats.pending == 0
    assert stats.dropped == 0
    assert stats.path.exists()


def test_repository_queue_full_raises(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    repo = EvaluacionRepository(path=tmp_path / "queue_full.db", max_queue=1)

    def _raise_full() -> None:
        raise queue.Full

    try:
        monkeypatch.setattr(repo._queue, "put_nowait", lambda _record: _raise_full())  # type: ignore[attr-defined]
        with pytest.raises(PersistenceQueueFullError):
            repo.save_evaluacion(symbol="SOLUSDT", timeframe="15m", side="short", score=1.0)
    finally:
        repo.close()


def test_repository_flush_timeout(tmp_path: Path) -> None:
    repo = EvaluacionRepository(path=tmp_path / "flush.db", max_queue=1)
    try:
        repo._stop_event.set()  # type: ignore[attr-defined]
        repo.save_evaluacion(symbol="BNBUSDT", timeframe="1m", side="long", score=1.0)
        start = time.monotonic()
        with pytest.raises(TimeoutError):
            repo.flush(timeout=0.1)
        elapsed = time.monotonic() - start
    finally:
        try:
            while True:
                repo._queue.get_nowait()  # type: ignore[attr-defined]
                repo._queue.task_done()  # type: ignore[attr-defined]
        except queue.Empty:
            pass
        repo._stop_event.clear()  # type: ignore[attr-defined]
        repo.close()

    assert elapsed >= 0.1