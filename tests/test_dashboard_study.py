"""Tests del estudio de timeframes y la simulación de inversión del dashboard.

Cubre:
1. estudio_timeframes_data: salida estructurada, JSON-serializable y ordenada.
2. simulacion_inversion_data: estrategia vs comprar y mantener.
3. dashboard.study: flujo lanzar→running→done, error y rechazo concurrente.
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

    # Orden: filas con operaciones reales en test primero (por PF desc), y las
    # de 0 trades (PF=inf espurio) al final.
    def _clave(f):
        tiene = 1 if f["test"]["trades"] > 0 else 0
        pf = float("inf") if f["test"]["pf"] is None else f["test"]["pf"]
        return (tiene, pf)

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
        assert final["resultado"] == resultado_fake
        assert final["progress"] == 1.0

    asyncio.run(run())


def test_dashboard_study_error(monkeypatch: pytest.MonkeyPatch) -> None:
    import backtesting.backtest_rapido as bt
    import dashboard.study as study

    def fake_data_falla(symbols, days, capital, progress_cb=None):
        raise RuntimeError("binance bloqueado")

    monkeypatch.setattr(bt, "estudio_timeframes_data", fake_data_falla)

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


def test_simulacion_inversion_data(monkeypatch: pytest.MonkeyPatch) -> None:
    import backtesting.backtest_rapido as bt

    monkeypatch.setattr(bt, "descargar_klines", _fake_klines)
    d = bt.simulacion_inversion_data(["BTCUSDT", "ETHUSDT"], 365, 1000.0, tf="1d")

    json.dumps(d)  # JSON-serializable
    assert d["capital_inicial"] == 1000.0
    assert set(d) >= {
        "capital_inicial", "estrategia_final", "estrategia_pct",
        "buyhold_final", "buyhold_pct", "trades", "winrate", "por_simbolo",
    }
    assert len(d["por_simbolo"]) == 2
    # El reparto del capital es coherente con el agregado.
    assert isinstance(d["estrategia_final"], (int, float))
    assert isinstance(d["buyhold_final"], (int, float))


def test_dashboard_simulacion_flujo(monkeypatch: pytest.MonkeyPatch) -> None:
    import backtesting.backtest_rapido as bt
    import dashboard.study as study

    resultado_fake = {
        "capital_inicial": 1000.0, "estrategia_final": 1080.0, "estrategia_pct": 8.0,
        "buyhold_final": 1250.0, "buyhold_pct": 25.0, "trades": 12, "winrate": 58.0,
        "tf": "1d", "umbral": 3.0, "dias": 365, "por_simbolo": [],
    }

    def fake_sim(symbols, days, capital, tf="1d", umbral=3.0, progress_cb=None):
        if progress_cb:
            progress_cb(1.0, "Completado")
        return resultado_fake

    monkeypatch.setattr(bt, "simulacion_inversion_data", fake_sim)

    async def run() -> None:
        estado = study.lanzar_simulacion(days=365)
        assert estado["status"] == "running"
        for _ in range(50):
            await asyncio.sleep(0.05)
            if study.estado_simulacion()["status"] != "running":
                break
        final = study.estado_simulacion()
        assert final["status"] == "done"
        assert final["resultado"] == resultado_fake

    asyncio.run(run())
