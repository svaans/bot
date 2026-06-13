"""Trabajos de análisis lanzables desde el dashboard (estudio + simulación).

Cada trabajo corre en un hilo (``asyncio.to_thread`` → no bloquea el event loop
ni al bot), admite una sola ejecución simultánea y expone su estado/resultado
para consulta por polling desde el frontend.
"""
from __future__ import annotations

import asyncio
import logging
import time
from threading import Lock
from typing import Any, Callable, Dict, List

log = logging.getLogger("dashboard.study")

# Universo por defecto (formato ticker Binance, sin barra).
_SYMBOLS_DEFAULT = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "AVAXUSDT"]
_DAYS_PERMITIDOS = {180, 365, 730}


class _Job:
    """Estado + ejecución en segundo plano de un análisis."""

    def __init__(self) -> None:
        self._lock = Lock()
        self._task: asyncio.Task | None = None
        self._estado: Dict[str, Any] = {
            "status": "idle",      # idle | running | done | error
            "progress": 0.0,
            "mensaje": "",
            "params": {},
            "resultado": None,
            "error": None,
            "ts_inicio": None,
            "ts_fin": None,
        }

    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            return dict(self._estado)

    def _set(self, **kwargs: Any) -> None:
        with self._lock:
            self._estado.update(kwargs)

    def progress(self, fraccion: float, mensaje: str) -> None:
        self._set(progress=round(max(0.0, min(1.0, fraccion)), 3), mensaje=mensaje)

    def lanzar(self, fn: Callable[[], Any], params: Dict[str, Any]) -> Dict[str, Any]:
        with self._lock:
            if self._estado["status"] == "running":
                return dict(self._estado)
            self._estado.update({
                "status": "running", "progress": 0.0, "mensaje": "Iniciando…",
                "params": params, "resultado": None, "error": None,
                "ts_inicio": time.time(), "ts_fin": None,
            })
        self._task = asyncio.create_task(self._run(fn))
        return self.snapshot()

    async def _run(self, fn: Callable[[], Any]) -> None:
        try:
            resultado = await asyncio.to_thread(fn)
            self._set(status="done", progress=1.0, mensaje="Completado",
                      resultado=resultado, ts_fin=time.time())
        except Exception as exc:  # noqa: BLE001 — se reporta al frontend
            log.warning("Trabajo de dashboard falló: %s", exc, exc_info=True)
            self._set(status="error", error=str(exc),
                      mensaje="Error al ejecutar", ts_fin=time.time())


def _days_validos(days: int) -> int:
    return days if days in _DAYS_PERMITIDOS else 365


# ── Estudio de timeframes ──────────────────────────────────────────────────
_estudio = _Job()


def lanzar_estudio(days: int = 365, capital: float = 1000.0) -> Dict[str, Any]:
    days = _days_validos(days)

    def _fn() -> List[dict]:
        from backtesting.backtest_rapido import estudio_timeframes_data  # noqa: PLC0415
        return estudio_timeframes_data(_SYMBOLS_DEFAULT, days, capital, progress_cb=_estudio.progress)

    return _estudio.lanzar(_fn, {"symbols": _SYMBOLS_DEFAULT, "days": days, "capital": capital})


def estado_estudio() -> Dict[str, Any]:
    return _estudio.snapshot()


# ── Simulación de inversión ────────────────────────────────────────────────
_simulacion = _Job()


def lanzar_simulacion(days: int = 365, capital: float = 1000.0) -> Dict[str, Any]:
    days = _days_validos(days)

    def _fn() -> dict:
        from backtesting.backtest_rapido import simulacion_inversion_data  # noqa: PLC0415
        return simulacion_inversion_data(
            _SYMBOLS_DEFAULT, days, capital, tf="1d", progress_cb=_simulacion.progress,
        )

    return _simulacion.lanzar(_fn, {"symbols": _SYMBOLS_DEFAULT, "days": days, "capital": capital})


def estado_simulacion() -> Dict[str, Any]:
    return _simulacion.snapshot()
