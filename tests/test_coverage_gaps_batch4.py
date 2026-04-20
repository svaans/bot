"""Cobertura adicional: tendencia, bootstrap, umbrales, TP/SL dinámico, feeds, reporting, capital."""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from core.data import bootstrap as bs


def _ohlcv(n: int = 70, seed: int = 11) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    ts = np.arange(n, dtype=np.int64) * 60_000
    close = 100.0 + np.cumsum(rng.normal(0, 0.2, n))
    high = close + 0.5
    low = close - 0.5
    open_ = np.roll(close, 1)
    open_[0] = close[0]
    vol = rng.uniform(400, 900, n)
    return pd.DataFrame(
        {
            "timestamp": ts,
            "open": open_,
            "high": high,
            "low": low,
            "close": close,
            "volume": vol,
        }
    )


@pytest.fixture(autouse=True)
def _reset_bootstrap_y_tendencia() -> None:
    bs.reset_state()
    import core.strategies.tendencia as t

    t._CACHE_METRICAS.clear()
    t._ESTADO_TENDENCIA.clear()
    yield
    bs.reset_state()
    t._CACHE_METRICAS.clear()
    t._ESTADO_TENDENCIA.clear()


def test_bootstrap_progress_y_cola() -> None:
    bs.update_progress("ABC", 10, target=100)
    assert bs.get_progress("ABC") == pytest.approx(0.1, rel=0, abs=0.02)
    bs.enqueue_fetch("ABC")
    assert "ABC" in bs.pending_symbols()
    bs.reset_state()
    assert bs.get_progress("ABC") == 0.0


def test_tendencia_hash_y_detectar_corta() -> None:
    import core.strategies.tendencia as t

    assert t._hash_dataframe(pd.DataFrame()) == (0, 0, 0.0)
    assert t._almost_equal(1.0, 1.0000001) is True
    tend, _ = t.detectar_tendencia("X", pd.DataFrame({"close": [1.0, 2.0]}))
    assert tend == "lateral"


def test_tendencia_detectar_con_datos() -> None:
    import core.strategies.tendencia as t

    df = _ohlcv(45)
    tend, estr = t.detectar_tendencia("BTC/EUR", df, registrar=True)
    assert tend in ("alcista", "bajista", "lateral")
    assert isinstance(estr, dict)


def test_tendencia_obtener_y_parametros_persistencia() -> None:
    import core.strategies.tendencia as t

    df = _ohlcv(50)
    out = t.obtener_tendencia("ETH/EUR", df)
    assert out in ("alcista", "bajista", "lateral")
    assert t.obtener_parametros_persistencia("lateral", 0.01) == (0.6, 3)
    assert t.obtener_parametros_persistencia("alcista", 0.015)[0] < 0.6


@pytest.mark.asyncio
async def test_tendencia_senales_repetidas_buffer_corto() -> None:
    import core.strategies.tendencia as t

    n = await t.señales_repetidas([], {"a": 1.0}, "lateral", 0.01, ventanas=3)
    assert n == 0


def test_adaptador_umbral_calcular(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    import core.adaptador_umbral as au

    monkeypatch.setattr(au, "_ensure_umbral_estado_cargado", lambda: None)
    monkeypatch.setattr(au, "_maybe_persist_umbral_estado", lambda: None)
    monkeypatch.setattr(
        au,
        "_cargar_config",
        lambda: {"UMBRAL_T": {"peso_minimo_total": 0.55, "factor_umbral": 1.05}},
    )
    u = au.calcular_umbral_adaptativo("UMBRAL_T", _ohlcv(50), None)
    assert isinstance(u, float) and u > 0


def test_adaptador_dinamico_tp_sl_rama_sin_high_low(monkeypatch: pytest.MonkeyPatch) -> None:
    import core.adaptador_dinamico as ad

    df = pd.DataFrame({"close": [100.0, 101.0]})
    res = ad.calcular_tp_sl_adaptativos("S", df, {"sl_ratio": 1.0, "tp_ratio": 2.0}, precio_actual=100.0)
    assert res.sl < res.tp
    assert hasattr(res, "sl_clamped")


def test_adaptador_dinamico_tp_sl_completo(monkeypatch: pytest.MonkeyPatch) -> None:
    import core.adaptador_dinamico as ad

    monkeypatch.setattr(ad, "detectar_regimen", lambda _df: "lateral")
    df = _ohlcv(40)
    res = ad.calcular_tp_sl_adaptativos(
        "BTC/EUR",
        df,
        {
            "sl_ratio": 1.2,
            "tp_ratio": 2.0,
            "tick_size": 0.01,
            "min_distance_pct": 0.001,
            "min_distance_ticks": 1,
            "modo_capital_bajo": True,
        },
        capital_actual=100.0,
        precio_actual=float(df["close"].iloc[-1]),
    )
    assert not np.isnan(res.sl) and res.tp > res.sl


def test_adaptador_dinamico_recargar_configs(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import core.adaptador_dinamico as ad

    p = tmp_path / "cfg.json"
    p.write_text('{"X": {"sl_ratio": 1}}', encoding="utf-8")
    monkeypatch.setattr(ad, "RUTA_CONFIGS_OPTIMAS", str(p))
    ad.recargar_configs_optimas()
    assert "X" in ad.CONFIGS_OPTIMAS


@pytest.mark.asyncio
async def test_external_feeds_es_futuros_en_instancia() -> None:
    from core.data.external_feeds import ExternalFeeds

    ef = ExternalFeeds()
    ef._futures_symbols = {"SOLUSDT"}
    assert await ef.es_futuros("SOLUSDT") is True


def test_reporter_diario_en_tmp(tmp_path: Path) -> None:
    from core.reporting import ReporterDiario

    r = ReporterDiario(carpeta=str(tmp_path / "rd"), max_operaciones=50)
    assert Path(r.carpeta).is_dir()


def test_gestor_capital_piramidacion() -> None:
    from core import gestor_capital as gc

    orden = SimpleNamespace(
        precio_entrada=100.0,
        direccion="long",
        cantidad=1.0,
        adds_realizadas=0,
    )
    add = gc.aplicar_piramidacion(orden, 101.5, {"paso": 0.01, "max_adds": 3, "riesgo_max": 0.5})
    assert add is None or isinstance(add, float)


def test_adaptador_dinamico_extraer_tick_size() -> None:
    import core.adaptador_dinamico as ad

    assert ad._extraer_tick_size({"tick_size": 0.01}) == 0.01
    assert ad._extraer_tick_size({"filters": {"tickSize": "0.02"}}) == 0.02
