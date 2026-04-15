"""Cobertura de evaluador técnico, modo operativo, watchdog, aprendizaje y alias."""

from __future__ import annotations

import asyncio
import importlib
import json
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import numpy as np
import pandas as pd
import pytest


def _ohlcv(n: int = 65) -> pd.DataFrame:
    rng = np.random.default_rng(7)
    close = 100.0 + np.cumsum(rng.normal(0, 0.2, n))
    high = close + 0.5
    low = close - 0.5
    open_ = np.roll(close, 1)
    open_[0] = close[0]
    vol = rng.uniform(800, 1200, n)
    return pd.DataFrame(
        {"open": open_, "high": high, "low": low, "close": close, "volume": vol}
    )


@pytest.fixture
def evaluador_reset(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    import core.strategies.evaluador_tecnico as ev

    p = tmp_path / "pesos_tecnicos.json"
    p.write_text('{"BTC/EUR": {"rsi": 0.5}}', encoding="utf-8")
    monkeypatch.setattr(ev, "RUTA_PESOS", str(p))
    ev._pesos_cache = None
    ev._backfill_tasks.clear()
    yield ev
    ev._pesos_cache = None
    ev._backfill_tasks.clear()


@pytest.mark.asyncio
async def test_evaluador_cargar_pesos(evaluador_reset) -> None:
    ev = evaluador_reset
    w = await ev.cargar_pesos_tecnicos("BTC/EUR")
    assert w["rsi"] == 0.5


@pytest.mark.asyncio
async def test_evaluador_puntaje_con_datos(evaluador_reset) -> None:
    ev = evaluador_reset
    df = _ohlcv(65)
    out = await ev.evaluar_puntaje_tecnico(
        "BTC/EUR", df, float(df["close"].iloc[-1]), 90.0, 115.0, window_size=60
    )
    assert "score_total" in out and "detalles" in out


@pytest.mark.asyncio
async def test_evaluador_datos_insuficientes_dispara_backfill(
    evaluador_reset, monkeypatch: pytest.MonkeyPatch
) -> None:
    ev = evaluador_reset
    ev._backfill_tasks.clear()
    called: list[int] = []

    async def fake_backfill(sym: str, n: int) -> None:
        called.append(n)

    monkeypatch.setattr("core.strategies.evaluador_tecnico.backfill", fake_backfill)
    out = await ev.evaluar_puntaje_tecnico(
        "SYM_BACKFILL_TEST", _ohlcv(5), 1.0, 0.5, 2.0, window_size=60
    )
    assert out["score_total"] == 0.0
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    assert called


@pytest.mark.asyncio
async def test_evaluador_actualizar_pesos(evaluador_reset, monkeypatch: pytest.MonkeyPatch) -> None:
    ev = evaluador_reset
    monkeypatch.setattr(ev, "log_pesos_tecnicos_persistidos", lambda *a, **k: None)

    await ev.actualizar_pesos_tecnicos("BTC/EUR", {"rsi": 1.0}, retorno=0.1, factor=0.05)
    data = json.loads(Path(ev.RUTA_PESOS).read_text(encoding="utf-8"))
    assert "BTC/EUR" in data


def test_modo_operativo_desde_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("MODO_OPERATIVO", raising=False)
    monkeypatch.delenv("BOT_MODE", raising=False)
    monkeypatch.setenv("MODO_REAL", "false")
    import core.modo as modo

    importlib.reload(modo)
    assert modo.MODO_REAL is False


def test_trader_simulado_fuerza_paper(monkeypatch: pytest.MonkeyPatch) -> None:
    import core.trader_simulado as ts

    class OC:
        modo_real = True

    def fake_replace(_cfg, **kwargs):
        assert kwargs.get("modo_real") is False

        class NC:
            modo_real = False

        return NC()

    monkeypatch.setattr(ts, "replace", fake_replace)
    monkeypatch.setattr(ts.Trader, "__init__", lambda self, cfg: setattr(self, "config", cfg))
    t = ts.TraderSimulado(OC())
    assert t.config.modo_real is False


def test_risk_manager_alias_import() -> None:
    import core.risk_manager as rm

    assert hasattr(rm, "RiskManager") or hasattr(rm, "riesgo_superado")


@pytest.mark.asyncio
async def test_async_learning_emit() -> None:
    from core import async_learning_manager as alm

    seen: list[str] = []

    def on_e(evt: str, data: dict) -> None:
        seen.append(evt)

    alm._emit(on_e, "x", {"k": 1})
    assert seen == ["x"]


@pytest.mark.asyncio
async def test_gestor_aprendizaje_emit() -> None:
    from core import gestor_aprendizaje as ga

    ga._emit(None, "a", {})
    out: list[str] = []

    def cb(e: str, _d: dict) -> None:
        out.append(e)

    ga._emit(cb, "b", {})
    assert out == ["b"]


def test_gestor_watchdog_latido(monkeypatch: pytest.MonkeyPatch) -> None:
    from core import gestor_watchdog as gw

    beats: list[tuple[str, str | None]] = []

    class Sup:
        def __init__(self, **_kwargs) -> None:
            pass

        def start_supervision(self) -> None:
            return None

        async def shutdown(self) -> None:
            return None

        def beat(self, nombre: str, causa: str | None = None, **kwargs) -> None:
            beats.append((nombre, causa))

        def tick_data(self, *_a, **_k) -> None:
            return None

        def supervised_task(self, *_a, **_k):
            return mock.Mock()

        def set_task_expected_interval(self, *_a, **_k) -> None:
            return None

        def set_watchdog_interval(self, *_a, **_k) -> None:
            return None

    monkeypatch.setattr(gw, "Supervisor", Sup)
    g = gw.GestorWatchdog()
    g.iniciar()
    g.latido("t", "c")
    assert beats == [("t", "c")]


def test_validador_ordenes_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    from core.risk import validador_ordenes as vo

    class FakeCli:
        def public_get_exchangeinfo(self, _params):
            raise RuntimeError("no network")

    monkeypatch.setattr(vo, "crear_cliente", lambda: FakeCli())
    sl, tp = vo.validar_orden("BTCUSDT", 100.0, 95.0, 105.0)
    assert isinstance(sl, float) and isinstance(tp, float)


def test_learning_reset_pesos_sin_base(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    import learning.reset_pesos as rp

    base = tmp_path / "estrategias_pesos_base.json"
    base.write_text('{"BTC/EUR": {"a": 1}}', encoding="utf-8")
    ctrl = tmp_path / "ctrl.txt"
    monkeypatch.setattr(rp, "RUTA_BASE", base)
    monkeypatch.setattr(rp, "RUTA_CONTROL", ctrl)
    monkeypatch.setattr(rp, "persist_entry_weights", lambda *a, **k: None)
    monkeypatch.setattr(
        rp,
        "gestor_pesos",
        SimpleNamespace(ruta=tmp_path / "live.json"),
    )
    rp.resetear_pesos_diarios_si_corresponde()
    assert ctrl.read_text(encoding="utf-8").strip()


def test_monitor_estado_sin_db(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    import core.monitor_estado_bot as mon

    monkeypatch.setattr(mon, "ORDENES_DB_PATH", str(tmp_path / "no.db"))
    monkeypatch.setattr(mon, "real_orders", SimpleNamespace(cargar_ordenes=lambda: {}))
    r = mon.obtener_orden_abierta()
    assert r is None or isinstance(r, (dict, type(None)))


def test_build_parser_orders_cli() -> None:
    from core.orders.cli import build_parser

    p = build_parser()
    assert p.parse_args(["reconcile-trades", "--json"])


@pytest.mark.asyncio
async def test_external_feeds_es_futuros_cached(monkeypatch: pytest.MonkeyPatch) -> None:
    from core.data import external_feeds as ef

    ef._futures_symbols_global = {"BTCUSDT"}
    try:
        assert await ef.es_futuros("BTCUSDT", session=None) is True
        assert await ef.es_futuros("MISSING", session=None) is False
    finally:
        ef._futures_symbols_global = None


def test_aprendizaje_en_linea_simbolo_invalido() -> None:
    import learning.aprendizaje_en_linea as ael

    ael.registrar_resultado_trade("??/invalid", {}, 1.0)


def test_reset_configuracion_diaria(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import learning.reset_configuracion as rc

    base = tmp_path / "configuraciones_base.json"
    base.write_text('{"k": 0}', encoding="utf-8")
    actual = tmp_path / "configuraciones_optimas.json"
    actual.write_text('{"k": 99}', encoding="utf-8")
    ctl = tmp_path / "reset_config_fecha.txt"
    monkeypatch.setattr(rc, "RUTA_BASE", base)
    monkeypatch.setattr(rc, "RUTA_ACTUAL", actual)
    monkeypatch.setattr(rc, "RUTA_CONTROL", ctl)
    monkeypatch.setattr(rc, "_CFG", tmp_path)
    rc.resetear_configuracion_diaria_si_corresponde()
    assert json.loads(actual.read_text(encoding="utf-8")) == {"k": 0}
