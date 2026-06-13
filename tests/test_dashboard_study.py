"""Tests del estudio de timeframes consumible por el dashboard.

Cubre:
1. estudio_timeframes_data: salida estructurada, JSON-serializable y ordenada
   por PF de test descendente (sin inf/nan).
2. dashboard.study: flujo lanzar→running→done y rechazo de ejecución
   concurrente.
"""
from __future__ import annotations

import asyncio
import json

import pytest


def _fake_klines(symbol: str, interval: str, days: int):
    """Velas deterministas con deriva suave (suficientes para train/test)."""
    out = []
    t = 1_700_000_000_000
    price = 100.0
    for i in range(600):
        price *= 1 + (0.001 if i % 7 else -0.002)
        out.append([t + i * 60_000, price, price * 1.01, price * 0.99, price * 1.0005, 1000.0])
    return out


def test_estudio_timeframes_data_estructura(monkeypatch: pytest.MonkeyPatch) -> None:
    import backtesting.backtest_rapido as bt

    monkeypatch.setattr(bt, "descargar_klines", _fake_klines)
    filas = bt.estudio_timeframes_data(["BTCUSDT", "ETHUSDT"], 365, 1000.0)

    # 4 timeframes x 3 umbrales x 2 trailing x 2 tendencia = 48 combinaciones
    assert len(filas) == 48
    json.dumps(filas)  # JSON-serializable (sin inf/nan)

    fila = filas[0]
    assert set(fila) == {"tf", "umbral", "trailing", "tendencia", "train", "test"}
    for fase in ("train", "test"):
        assert set(fila[fase]) == {"ret", "pf", "trades"}
        assert fila[fase]["pf"] is None or isinstance(fila[fase]["pf"], (int, float))

    # Ordenado por PF de test descendente (None=inf primero).
    def _clave(f):
        return float("inf") if f["test"]["pf"] is None else f["test"]["pf"]

    claves = [_clave(f) for f in filas]
    assert claves == sorted(claves, reverse=True)


def test_estudio_timeframes_data_progress_cb(monkeypatch: pytest.MonkeyPatch) -> None:
    import backtesting.backtest_rapido as bt

    monkeypatch.setattr(bt, "descargar_klines", _fake_klines)
    eventos: list[tuple[float, str]] = []
    bt.estudio_timeframes_data(["BTCUSDT"], 365, 1000.0, progress_cb=lambda f, m: eventos.append((f, m)))
    assert eventos and eventos[-1][0] == 1.0


def test_dashboard_study_flujo(monkeypatch: pytest.MonkeyPatch) -> None:
    import backtesting.backtest_rapido as bt
    import dashboard.study as study

    resultado_fake = [{
        "tf": "1d", "umbral": 3.0, "trailing": True, "tendencia": False,
        "train": {"ret": 12.3, "pf": 1.8, "trades": 40},
        "test": {"ret": 6.1, "pf": 1.4, "trades": 18},
    }]

    def fake_data(symbols, days, capital, progress_cb=None):
        if progress_cb:
            progress_cb(1.0, "Completado")
        return resultado_fake

    monkeypatch.setattr(bt, "estudio_timeframes_data", fake_data)
    # Estado limpio
    study._estado.update({"status": "idle", "resultados": [], "error": None})

    async def run() -> None:
        estado = study.lanzar_estudio(days=365)
        assert estado["status"] == "running"
        # Segundo lanzamiento mientras corre: no arranca otro
        assert study.lanzar_estudio(days=730)["status"] == "running"
        # Esperar a que termine
        for _ in range(50):
            await asyncio.sleep(0.05)
            if study.estado_estudio()["status"] != "running":
                break
        final = study.estado_estudio()
        assert final["status"] == "done"
        assert final["resultados"] == resultado_fake
        assert final["progress"] == 1.0

    asyncio.run(run())


def test_dashboard_study_error(monkeypatch: pytest.MonkeyPatch) -> None:
    import backtesting.backtest_rapido as bt
    import dashboard.study as study

    def fake_data_falla(symbols, days, capital, progress_cb=None):
        raise RuntimeError("binance bloqueado")

    monkeypatch.setattr(bt, "estudio_timeframes_data", fake_data_falla)
    study._estado.update({"status": "idle", "resultados": [], "error": None})

    async def run() -> None:
        study.lanzar_estudio(days=365)
        for _ in range(50):
            await asyncio.sleep(0.05)
            if study.estado_estudio()["status"] != "running":
                break
        final = study.estado_estudio()
        assert final["status"] == "error"
        assert "binance" in (final["error"] or "").lower()

    asyncio.run(run())
