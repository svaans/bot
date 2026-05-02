"""Resolución de ramas JSON por símbolo (USDT vs EUR legacy) y regresión."""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from config.configuracion import ConfigurationService
from config.symbol_config_resolve import resolve_symbol_branch
from core.strategies.pesos import GestorPesos


def test_resolve_exact_wins_over_base_candidates() -> None:
    data = {"BTC/EUR": {"x": 1}, "BTC/USDT": {"x": 2}}
    k, v, res = resolve_symbol_branch("BTC/USDT", data)
    assert res == "exact"
    assert k == "BTC/USDT"
    assert v == {"x": 2}


def test_resolve_base_alias_single_quote_variant() -> None:
    data = {"BTC/EUR": {"sl_ratio": 9.9}}
    k, v, res = resolve_symbol_branch("BTC/USDT", data)
    assert res == "base_alias"
    assert k == "BTC/EUR"
    assert v == {"sl_ratio": 9.9}


def test_resolve_ambiguous_multiple_same_base() -> None:
    data = {"BTC/EUR": {"sl_ratio": 1.0}, "BTC/BUSD": {"sl_ratio": 2.0}}
    k, v, res = resolve_symbol_branch("BTC/USDT", data)
    assert res == "ambiguous"
    assert k is None and v is None


def test_configuration_service_load_usdt_uses_eur_branch(tmp_path: Path) -> None:
    ruta = tmp_path / "opt.json"
    ruta.write_text(
        json.dumps({"BTC/EUR": {"sl_ratio": 7.7, "tp_ratio": 2.5}}),
        encoding="utf-8",
    )
    svc = ConfigurationService(ruta=str(ruta))
    cfg = svc.load("BTC/USDT")
    assert cfg["sl_ratio"] == pytest.approx(7.7)
    assert cfg["tp_ratio"] == pytest.approx(2.5)


def test_configuration_service_exact_symbol_sin_alias(tmp_path: Path) -> None:
    ruta = tmp_path / "opt.json"
    ruta.write_text(
        json.dumps({"BTC/USDT": {"sl_ratio": 3.3}, "BTC/EUR": {"sl_ratio": 9.9}}),
        encoding="utf-8",
    )
    svc = ConfigurationService(ruta=str(ruta))
    cfg = svc.load("BTC/USDT")
    assert cfg["sl_ratio"] == pytest.approx(3.3)


def test_gestor_pesos_usdt_alias_a_eur(tmp_path: Path) -> None:
    path = tmp_path / "estrategias_pesos.json"
    path.write_text(json.dumps({"BTC/EUR": {"rsi": 12.0, "tendencia": 20.0}}), encoding="utf-8")
    g = GestorPesos.from_file(path, total=100.0, piso=1.0)
    w = g.obtener_pesos_symbol("BTC/USDT")
    assert w.get("rsi") == pytest.approx(12.0)
    assert g.obtener_peso("rsi", "BTC/USDT") == pytest.approx(12.0)


@pytest.mark.asyncio
async def test_evaluador_pesos_tecnicos_usdt_alias(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    import core.strategies.evaluador_tecnico as ev

    p = tmp_path / "pesos_tecnicos.json"
    p.write_text(json.dumps({"BTC/EUR": {"rsi": 0.42}}), encoding="utf-8")
    monkeypatch.setattr(ev, "RUTA_PESOS", str(p))
    ev._pesos_cache = None
    try:
        w = await ev.cargar_pesos_tecnicos("BTC/USDT")
        assert w["rsi"] == pytest.approx(0.42)
    finally:
        ev._pesos_cache = None


def _ohlcv_umbral(n: int = 50) -> pd.DataFrame:
    rng = np.random.default_rng(3)
    close = 100.0 + np.cumsum(rng.normal(0, 0.2, n))
    high = close + 0.5
    low = close - 0.5
    open_ = np.roll(close, 1)
    open_[0] = close[0]
    vol = rng.uniform(800, 1200, n)
    return pd.DataFrame({"open": open_, "high": high, "low": low, "close": close, "volume": vol})


def test_adaptador_umbral_usdt_usa_rama_eur_via_resolve(monkeypatch: pytest.MonkeyPatch) -> None:
    import core.adaptador_umbral as au

    monkeypatch.setattr(au, "_ensure_umbral_estado_cargado", lambda: None)
    monkeypatch.setattr(au, "_maybe_persist_umbral_estado", lambda: None)
    monkeypatch.setattr(
        au,
        "_cargar_config",
        lambda: {"BTC/EUR": {"peso_minimo_total": 2.0, "factor_umbral": 2.0}},
    )
    u = au.calcular_umbral_adaptativo("BTC/USDT", _ohlcv_umbral(50), None)
    assert isinstance(u, float) and u > 3.0
