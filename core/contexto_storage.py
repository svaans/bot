from __future__ import annotations

"""Persistencia ligera para puntajes de contexto con TTL configurable."""

import asyncio
import json
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict

__all__ = ["PuntajeSnapshot", "PuntajeStore", "cargar_puntajes_persistidos"]


@dataclass(slots=True)
class PuntajeSnapshot:
    """Representa un puntaje almacenado junto con su metadato y vencimiento."""

    value: float
    metadata: Dict[str, Any]
    expires_at: float


class PuntajeStore:
    """Persistencia basada en SQLite para los puntajes de contexto."""

    def __init__(
        self,
        *,
        path: str | Path | None = None,
        ttl_seconds: int = 600,
        time_provider: Callable[[], float] | None = None,
    ) -> None:
        self.path = Path(path or Path("estado") / "puntajes_contexto.db")
        self.ttl_seconds = max(1, int(ttl_seconds))
        self._time = time_provider or time.time
        self._lock = asyncio.Lock()
        self._ensure_schema()

    # ──────────────────────────── API ASÍNCRONA ────────────────────────────
    async def set(self, symbol: str, value: float, metadata: Dict[str, Any] | None = None) -> None:
        payload = metadata or {}
        payload.setdefault("symbol", symbol)
        payload.setdefault("updated_at", int(self._time() * 1000))
        expires_at = self._time() + self.ttl_seconds
        record = (symbol.upper(), float(value), expires_at, json.dumps(payload, ensure_ascii=False))
        async with self._lock:
            await asyncio.to_thread(self._write_record, record)

    async def load_all(self) -> Dict[str, PuntajeSnapshot]:
        now = self._time()
        async with self._lock:
            rows = await asyncio.to_thread(self._read_records, now)
        return rows

    async def purge_expired(self) -> None:
        now = self._time()
        async with self._lock:
            await asyncio.to_thread(self._purge_records, now)

    async def close(self) -> None:  # pragma: no cover - compat futura
        """Hook para compatibilidad: no se mantiene conexión abierta."""
        await asyncio.sleep(0)

    # ────────────────────────────── API SINCRÓNICA ─────────────────────────────
    def load_all_sync(self) -> Dict[str, PuntajeSnapshot]:
        """Carga instantánea de puntajes (por ejemplo, para procesos secundarios)."""

        now = self._time()
        return self._read_records(now)

    # ───────────────────────────── MÉTODOS INTERNOS ────────────────────────────
    def _ensure_schema(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(self.path) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS puntajes (
                    symbol TEXT PRIMARY KEY,
                    value REAL NOT NULL,
                    expires_at REAL NOT NULL,
                    payload TEXT
                )
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_puntajes_expiry
                ON puntajes(expires_at)
                """
            )
            conn.commit()

    def _write_record(self, record: tuple[str, float, float, str]) -> None:
        with sqlite3.connect(self.path) as conn:
            conn.execute(
                """
                INSERT INTO puntajes(symbol, value, expires_at, payload)
                VALUES(?, ?, ?, ?)
                ON CONFLICT(symbol)
                DO UPDATE SET value=excluded.value, expires_at=excluded.expires_at, payload=excluded.payload
                """,
                record,
            )
            conn.commit()

    def _read_records(self, reference_ts: float) -> Dict[str, PuntajeSnapshot]:
        cleaned: Dict[str, PuntajeSnapshot] = {}
        with sqlite3.connect(self.path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute(
                "SELECT symbol, value, expires_at, payload FROM puntajes WHERE expires_at > ?",
                (reference_ts,),
            )
            for row in cursor:
                symbol = str(row["symbol"])
                value = float(row["value"])
                expires_at = float(row["expires_at"])
                raw_payload = row["payload"]
                metadata: Dict[str, Any]
                try:
                    metadata = json.loads(raw_payload) if raw_payload else {}
                except json.JSONDecodeError:
                    metadata = {}
                cleaned[symbol] = PuntajeSnapshot(value=value, metadata=metadata, expires_at=expires_at)
        return cleaned

    def _purge_records(self, reference_ts: float) -> None:
        with sqlite3.connect(self.path) as conn:
            conn.execute("DELETE FROM puntajes WHERE expires_at <= ?", (reference_ts,))
            conn.commit()


def cargar_puntajes_persistidos(store: PuntajeStore | None = None) -> Dict[str, PuntajeSnapshot]:
    """Helper conveniente para recuperar puntajes sin crear instancias manualmente."""

    target = store or PuntajeStore()
    return target.load_all_sync()