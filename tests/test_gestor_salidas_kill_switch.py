"""Prioridad de kill switch y flag ``kill_switch`` en resultados."""

from __future__ import annotations

import pandas as pd
import pytest

from core.strategies.exit import gestor_salidas as gs


@pytest.mark.asyncio
async def test_kill_switch_en_config_devuelve_flag(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(gs, "cargar_estrategias_salida", lambda: [])
    df = pd.DataFrame({"close": [100.0], "high": [101.0], "low": [99.0], "volume": [1.0]})
    orden = {"symbol": "BTC/USDT", "timestamp": "2020-01-01T00:00:00+00:00"}
    r = await gs.evaluar_salidas(orden, df, config={"kill_switch": True})
    assert r.get("cerrar") is True
    assert r.get("kill_switch") is True
    assert "kill" in str(r.get("razon", "")).lower()


@pytest.mark.asyncio
async def test_stop_loss_gana_sobre_cierre_por_tiempo(monkeypatch: pytest.MonkeyPatch) -> None:
    """Prioridad numérica mayor vence cuando compiten dos acciones prioritarias."""

    def fake_estrategias():
        async def sl(*_a, **_k):
            return {"cerrar": True, "evento": "stop loss tocado", "razon": "SL"}

        return [sl]

    monkeypatch.setattr(gs, "cargar_estrategias_salida", fake_estrategias)
    df = pd.DataFrame({"close": [100.0], "high": [101.0], "low": [99.0], "volume": [1.0]})
    orden = {
        "symbol": "BTC/USDT",
        "timestamp": "2020-01-01T00:00:00+00:00",
        "precio_entrada": 100.0,
        "direccion": "long",
    }
    r = await gs.evaluar_salidas(orden, df, config={"t_max": 1})
    assert r.get("cerrar") is True
    assert r.get("kill_switch") is not True
    texto = f"{r.get('razon', '')} {r.get('evento', '')}".lower()
    assert "stop" in texto or "sl" in texto
