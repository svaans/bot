"""Ejecución del estudio de timeframes desde el dashboard.

Lanza ``backtesting.backtest_rapido.estudio_timeframes_data`` en un hilo (no
bloquea el event loop ni al bot) y expone el estado/resultados para que el
frontend los consulte por polling. Solo se permite una ejecución a la vez.
"""
from __future__ import annotations

import asyncio
import logging
import time
from threading import Lock
from typing import Any, Dict, List

log = logging.getLogger("dashboard.study")

# Universo por defecto (formato ticker Binance, sin barra).
_SYMBOLS_DEFAULT = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "AVAXUSDT"]
_DAYS_PERMITIDOS = {180, 365, 730}

_lock = Lock()
_estado: Dict[str, Any] = {
    "status": "idle",          # idle | running | done | error
    "progress": 0.0,           # 0..1
    "mensaje": "",
    "params": {},
    "resultados": [],          # list[dict] del estudio
    "error": None,
    "ts_inicio": None,
    "ts_fin": None,
}
_task: asyncio.Task | None = None


def estado_estudio() -> Dict[str, Any]:
    """Devuelve una copia del estado actual (JSON-serializable)."""
    with _lock:
        return dict(_estado)


def _set(**kwargs: Any) -> None:
    with _lock:
        _estado.update(kwargs)


def _progress_cb(fraccion: float, mensaje: str) -> None:
    _set(progress=round(max(0.0, min(1.0, fraccion)), 3), mensaje=mensaje)


def _ejecutar(symbols: List[str], days: int, capital: float) -> List[dict]:
    # Import perezoso: backtesting arrastra dependencias pesadas.
    from backtesting.backtest_rapido import estudio_timeframes_data  # noqa: PLC0415

    return estudio_timeframes_data(symbols, days, capital, progress_cb=_progress_cb)


async def _runner(symbols: List[str], days: int, capital: float) -> None:
    try:
        resultados = await asyncio.to_thread(_ejecutar, symbols, days, capital)
        _set(
            status="done",
            progress=1.0,
            mensaje="Completado",
            resultados=resultados,
            ts_fin=time.time(),
        )
    except Exception as exc:  # noqa: BLE001 — se reporta al frontend
        log.warning("Estudio de timeframes falló: %s", exc, exc_info=True)
        _set(
            status="error",
            error=str(exc),
            mensaje="Error al ejecutar el estudio",
            ts_fin=time.time(),
        )


def lanzar_estudio(days: int = 365, capital: float = 1000.0) -> Dict[str, Any]:
    """Lanza el estudio si no hay uno en curso. Devuelve el estado resultante."""
    global _task
    with _lock:
        if _estado["status"] == "running":
            return dict(_estado)
        if days not in _DAYS_PERMITIDOS:
            days = 365
        _estado.update({
            "status": "running",
            "progress": 0.0,
            "mensaje": "Iniciando…",
            "params": {"symbols": _SYMBOLS_DEFAULT, "days": days, "capital": capital},
            "resultados": [],
            "error": None,
            "ts_inicio": time.time(),
            "ts_fin": None,
        })
    _task = asyncio.create_task(_runner(_SYMBOLS_DEFAULT, days, capital))
    return estado_estudio()
