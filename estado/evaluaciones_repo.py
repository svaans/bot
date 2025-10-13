"""Repositorio ligero para auditar evaluaciones del gate de entradas."""
from __future__ import annotations

import json
import queue
import sqlite3
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from core.utils.log_utils import safe_extra
from core.utils.utils import configurar_logger

__all__ = ["EvaluacionRepository", "EvaluacionRepositoryStats", "PersistenceQueueFullError"]


log = configurar_logger("evaluaciones_repo", modo_silencioso=True)

_STOP = object()


class PersistenceQueueFullError(RuntimeError):
    """Señala que la cola de persistencia alcanzó su capacidad máxima."""


@dataclass(slots=True)
class EvaluacionRepositoryStats:
    """Estadísticas básicas del repositorio."""

    path: Path
    pending: int
    inserted: int
    dropped: int


class EvaluacionRepository:
    """Persistencia asíncrona de evaluaciones en SQLite con hilo dedicado."""

    def __init__(
        self,
        path: str | Path | None = None,
        *,
        max_queue: int = 4096,
        journal_mode: str = "WAL",
    ) -> None:
        base_path = Path(path) if path is not None else Path("estado/evaluaciones.db")
        self.path = base_path.expanduser().resolve()
        self.path.parent.mkdir(parents=True, exist_ok=True)

        self._queue: "queue.Queue[dict[str, Any] | object]" = queue.Queue(max_queue)
        self._stop_event = threading.Event()
        self._worker: threading.Thread | None = None
        self._closed = False
        self._lock = threading.Lock()
        self._inserted = 0
        self._dropped = 0
        self._journal_mode = journal_mode

        self._start_worker()

    # -------------------------- API pública --------------------------
    def save_evaluacion(self, **payload: Any) -> None:
        """Encola una evaluación para persistirla en segundo plano."""

        if self._closed:
            raise RuntimeError("EvaluacionRepository cerrado")

        record = self._normalize_payload(payload)
        try:
            self._queue.put_nowait(record)
        except queue.Full as exc:  # pragma: no cover - depende de backpressure real
            self._dropped += 1
            log.warning(
                "evaluacion_repo.queue_full",
                extra=safe_extra({"path": str(self.path), "max_queue": self._queue.maxsize}),
            )
            raise PersistenceQueueFullError("Cola de persistencia saturada") from exc

    def flush(self, timeout: float | None = None) -> None:
        """Bloquea hasta que la cola quede vacía o se alcance ``timeout``."""

        if timeout is None:
            self._queue.join()
            return

        deadline = time.monotonic() + max(0.0, timeout)
        while self._queue.unfinished_tasks:  # type: ignore[attr-defined]
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("Timeout esperando flush del repositorio")
            time.sleep(min(0.05, remaining))

    def close(self, *, timeout: float | None = 5.0) -> None:
        """Detiene el hilo y asegura que los datos pendientes sean escritos."""

        with self._lock:
            if self._closed:
                return
            self._closed = True

        try:
            self.flush(timeout)
        except TimeoutError:
            log.warning(
                "evaluacion_repo.flush_timeout",
                extra=safe_extra({"path": str(self.path)}),
            )

        self._stop_event.set()
        self._queue.put(_STOP)
        worker = self._worker
        if worker and worker.is_alive():
            worker.join(timeout)

    def stats(self) -> EvaluacionRepositoryStats:
        with self._lock:
            inserted = self._inserted
            dropped = self._dropped
        pending = self._queue.qsize()
        return EvaluacionRepositoryStats(
            path=self.path,
            pending=pending,
            inserted=inserted,
            dropped=dropped,
        )

    # ----------------------- internals -----------------------
    def _start_worker(self) -> None:
        thread = threading.Thread(target=self._worker_loop, name="EvaluacionRepoWriter", daemon=True)
        self._worker = thread
        thread.start()

    def _worker_loop(self) -> None:
        try:
            conn = sqlite3.connect(
                str(self.path),
                isolation_level=None,
                check_same_thread=False,
                detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES,
            )
        except Exception:
            log.exception(
                "evaluacion_repo.connection_error",
                extra=safe_extra({"path": str(self.path)}),
            )
            self._stop_event.set()
            return

        try:
            if self._journal_mode:
                try:
                    conn.execute(f"PRAGMA journal_mode={self._journal_mode}")
                except sqlite3.DatabaseError:
                    log.debug(
                        "evaluacion_repo.journal_mode_failed",
                        extra=safe_extra({"mode": self._journal_mode}),
                    )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS evaluaciones (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_at REAL NOT NULL,
                    symbol TEXT,
                    timeframe TEXT,
                    side TEXT,
                    score REAL,
                    persistencia_ok INTEGER,
                    meta TEXT,
                    payload TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_evaluaciones_symbol_tf
                ON evaluaciones(symbol, timeframe, created_at DESC)
                """
            )

            while not self._stop_event.is_set():
                try:
                    item = self._queue.get(timeout=0.5)
                except queue.Empty:
                    continue

                if item is _STOP:
                    self._queue.task_done()
                    break

                try:
                    self._write_record(conn, item)
                except Exception:
                    with self._lock:
                        self._dropped += 1
                    log.exception(
                        "evaluacion_repo.write_error",
                        extra=safe_extra({"path": str(self.path)}),
                    )
                finally:
                    self._queue.task_done()
        finally:
            conn.close()

    def _write_record(self, conn: sqlite3.Connection, record: Mapping[str, Any]) -> None:
        conn.execute(
            """
            INSERT INTO evaluaciones (
                created_at,
                symbol,
                timeframe,
                side,
                score,
                persistencia_ok,
                meta,
                payload
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                record["created_at"],
                record.get("symbol"),
                record.get("timeframe"),
                record.get("side"),
                record.get("score"),
                1 if record.get("persistencia_ok") else 0,
                record.get("meta"),
                record["payload"],
            ),
        )
        with self._lock:
            self._inserted += 1

    def _normalize_payload(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        symbol = str(payload.get("symbol", "") or "").upper() or None
        timeframe = payload.get("timeframe")
        if timeframe is not None:
            timeframe = str(timeframe)
        side = payload.get("side")
        if side is not None:
            side = str(side)
        score_val = payload.get("score")
        score = None
        if score_val is not None:
            try:
                score = float(score_val)
            except (TypeError, ValueError):
                score = None
        meta_obj = payload.get("meta")
        meta_json = None
        if isinstance(meta_obj, Mapping):
            try:
                meta_json = json.dumps(meta_obj, ensure_ascii=False)
            except (TypeError, ValueError):
                meta_json = None
        persist_ok = payload.get("persistencia_ok")
        if persist_ok is None and isinstance(meta_obj, Mapping):
            persist_ok = meta_obj.get("persistencia_ok")
        persist_ok = bool(persist_ok) if persist_ok is not None else None

        record = {
            "created_at": time.time(),
            "symbol": symbol,
            "timeframe": timeframe,
            "side": side,
            "score": score,
            "persistencia_ok": persist_ok,
            "meta": meta_json,
            "payload": json.dumps(payload, ensure_ascii=False, default=str),
        }
        return record

    def __del__(self) -> None:  # pragma: no cover - destructor defensivo
        try:
            self.close(timeout=1.0)
        except Exception:
            pass