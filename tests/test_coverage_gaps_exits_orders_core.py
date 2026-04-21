"""Cobertura de módulos de salida, órdenes auxiliares y utilidades relacionadas."""

from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
import numpy as np
import pandas as pd
import pytest

UTC = timezone.utc


def _ohlcv(n: int = 65, seed: int = 42) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    close = 100.0 + np.cumsum(rng.normal(0, 0.3, n))
    high = close + rng.uniform(0.1, 0.8, n)
    low = close - rng.uniform(0.1, 0.8, n)
    open_ = np.roll(close, 1)
    open_[0] = close[0]
    vol = rng.uniform(500, 2000, n)
    return pd.DataFrame(
        {"open": open_, "high": high, "low": low, "close": close, "volume": vol}
    )


def test_salida_utils_resultado_salida() -> None:
    from core.strategies.exit.salida_utils import resultado_salida

    r = resultado_salida("E", True, "x", extra=1)
    assert r["cerrar"] and r["evento"] == "E" and r["extra"] == 1


def test_filtro_salidas_validar_necesidad_de_salida() -> None:
    from core.strategies.exit.filtro_salidas import validar_necesidad_de_salida

    df = _ohlcv(50)
    assert validar_necesidad_de_salida(df, {"symbol": "T", "direccion": "short"}, {}) is True


def test_analisis_salidas_patron_tecnico_fuerte() -> None:
    from core.strategies.exit import analisis_salidas as ans

    assert ans.patron_tecnico_fuerte(pd.DataFrame({"close": [1.0]})) is False


def test_salida_por_rsi_insuficiente() -> None:
    from core.strategies.exit.salida_por_rsi import salida_por_rsi

    r = salida_por_rsi(pd.DataFrame({"close": range(5)}))
    assert r["cerrar"] is False


def test_salida_por_tendencia_sin_tendencia() -> None:
    from core.strategies.exit.salida_por_tendencia import salida_por_tendencia

    r = salida_por_tendencia({"symbol": "BTC/EUR"}, _ohlcv(40))
    assert r["cerrar"] is False


def test_salida_por_macd_insuficiente() -> None:
    from core.strategies.exit.salida_por_macd import salida_por_macd

    r = salida_por_macd({"symbol": "BTC/EUR"}, _ohlcv(10))
    assert r["cerrar"] is False


def test_salida_tiempo_maximo() -> None:
    from core.strategies.exit.salida_tiempo_maximo import salida_tiempo_maximo

    viejo = (datetime.now(UTC) - timedelta(hours=10)).isoformat()
    r = salida_tiempo_maximo({"timestamp": viejo, "symbol": "X"}, _ohlcv(20))
    assert r["cerrar"] is True


def test_salida_break_even_sin_entrada() -> None:
    from core.strategies.exit.salida_break_even import salida_break_even

    r = salida_break_even({"symbol": "X"}, _ohlcv(30))
    assert r["cerrar"] is False


def test_stoploss_helpers() -> None:
    from core.strategies.exit import salida_stoploss as sl

    p = sl.stoploss_pct(100.0, 2.0, 0.01, "long")
    assert p < 100
    p2 = sl.stoploss_price(100.0, 1.0, 0.01, "long")
    assert p2 == pytest.approx(99.0, rel=0, abs=0.02)
    p3 = sl.stoploss_atr(100.0, 0.5, 2.0, 0.01, "long")
    assert p3 < 100


@pytest.mark.asyncio
async def test_verificar_salida_stoploss_datos_invalidos() -> None:
    from core.strategies.exit.salida_stoploss import verificar_salida_stoploss

    r = await verificar_salida_stoploss({}, None)
    assert r.get("cerrar") is False


@pytest.mark.asyncio
async def test_salida_stoploss_sin_evaluacion(monkeypatch: pytest.MonkeyPatch) -> None:
    from core.strategies.exit import salida_stoploss as sl

    async def no_eval(*_a, **_k):
        return None

    monkeypatch.setattr(sl, "evaluar_estrategias", no_eval)
    df = _ohlcv(25)
    orden = {
        "symbol": "BTC/EUR",
        "precio_entrada": 100.0,
        "stop_loss": 200.0,
        "direccion": "long",
    }
    r = await sl.salida_stoploss(orden, df)
    assert r.get("cerrar") is True


def test_salida_trailing_stop_insuficiente() -> None:
    from core.strategies.exit.salida_trailing_stop import salida_trailing_stop

    r = salida_trailing_stop({"precio_entrada": 100.0}, pd.DataFrame({"close": [1.0]}))
    assert r["cerrar"] is False


def test_verificar_trailing_stop_sin_atr(monkeypatch: pytest.MonkeyPatch) -> None:
    from core.strategies.exit.salida_trailing_stop import verificar_trailing_stop

    monkeypatch.setattr(
        "core.strategies.exit.salida_trailing_stop.get_atr", lambda *_a, **_k: None
    )
    info = {"precio_entrada": 100.0, "symbol": "BTC/EUR", "direccion": "long"}
    ok, msg = verificar_trailing_stop(info, 99.0, _ohlcv(30))
    assert ok is False and "ATR" in msg


def test_salida_stoploss_atr_corto() -> None:
    from core.strategies.exit.salida_stoploss_atr import salida_stoploss_atr

    r = salida_stoploss_atr({"direccion": "long"}, _ohlcv(10))
    assert r["cerrar"] is False


def test_salida_takeprofit_atr_corto() -> None:
    from core.strategies.exit.salida_takeprofit_atr import salida_takeprofit_atr

    r = salida_takeprofit_atr({"direccion": "long"}, _ohlcv(10))
    assert r["cerrar"] is False


def test_salidas_inteligentes_verificar_take_profit() -> None:
    from core.strategies.exit.salidas_inteligentes import verificar_take_profit

    orden = {"direccion": "long", "precio_entrada": 50.0, "take_profit": 200.0, "symbol": "T"}
    df = _ohlcv(25)
    r = verificar_take_profit(orden, df)
    assert r["cerrar"] is False


@pytest.mark.asyncio
async def test_evaluar_salida_inteligente_volumen_bajo(monkeypatch: pytest.MonkeyPatch) -> None:
    from core.strategies.exit import salidas_inteligentes as si

    cfg = {"volumen_minimo_salida": 1e9, "max_spread_ratio": 1.0}

    def fake_load(_sym: str):
        return cfg

    monkeypatch.setattr(si, "load_exit_config", fake_load)
    df = _ohlcv(40)
    df["volume"] = 1.0
    out = await si.evaluar_salida_inteligente({"symbol": "T"}, df)
    assert out.get("cerrar") is False
    assert "Volumen" in str(out.get("motivo_final", ""))


@pytest.mark.asyncio
async def test_verificar_salidas_sin_orden(monkeypatch: pytest.MonkeyPatch) -> None:
    from core.strategies.exit import verificar_salidas as vs

    monkeypatch.setattr(vs, "is_flag_enabled", lambda *_a, **_k: False)

    class Orders:
        def obtener(self, _sym: str):
            return None

    trader = SimpleNamespace(orders=Orders(), config_por_simbolo={})
    await vs.verificar_salidas(trader, "BTC/EUR", _ohlcv(30))


def test_ordenes_reales_stub() -> None:
    import core.ordenes_reales as or_

    assert or_.ejecutar_orden_market()["status"] == "FILLED"
    assert or_.obtener_todas_las_ordenes() == {}


def test_orders_place_order_dedup() -> None:
    import core.orders.orders as ordmod

    ordmod._SENT_CIDS.clear()
    ordmod._SEEN_KEYS.clear()
    t0 = 1_000_000.0
    r1 = ordmod.place_order("BTC/EUR", "buy", 1, "v1", now=t0)
    r2 = ordmod.place_order("BTC/EUR", "buy", 1, "v1", now=t0 + 1)
    assert r1["status"] == "SENT"
    assert r2["status"] == "SKIP_DUPLICATE"


def test_orders_cli_reconcile(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    from core.orders import cli as orders_cli

    monkeypatch.setattr(
        orders_cli,
        "reconciliar_trades_binance",
        lambda *a, **k: [{"symbol": "BTC", "reason": "test", "amount": 1, "local_amount": 1, "side": "buy"}],
    )
    rc = orders_cli.main(["reconcile-trades", "--symbols", "BTC"])
    assert rc == 0
    out = capsys.readouterr().out.lower()
    assert "divergencias" in out or "btc" in out


def test_orders_validators_remainder_mock_client(monkeypatch: pytest.MonkeyPatch) -> None:
    from core.orders import validators as v

    class FakeMarket:
        def __init__(self) -> None:
            self.limits = {
                "amount": {"min": 0.001},
                "cost": {"min": 10.0},
            }
            self.precision = {"amount": 3}

    class FakeClient:
        def load_markets(self):
            return {"BTC/USDT": FakeMarket()}

    monkeypatch.setattr(v, "obtener_cliente", lambda: FakeClient())
    assert v.remainder_executable("BTC/USDT", 50000.0, 0.01) is True


def test_risk_validators_validate_levels_long() -> None:
    from core.risk.validators import LevelValidationError, validate_levels

    e, sl, tp = validate_levels("long", 100.0, 95.0, 105.0, 0.001, 0.01, 0.0)
    assert e == 100.0 and sl < e < tp


def test_risk_validators_invalid_side() -> None:
    from core.risk.validators import LevelValidationError, validate_levels

    with pytest.raises(LevelValidationError):
        validate_levels("invalid", 1.0, 0.5, 2.0, 0.01, 0.0, 0.0)


def test_order_manager_exchange_sides_by_direction() -> None:
    from core.orders import order_manager as om

    assert om._exchange_side_open_position("long") == "buy"
    assert om._exchange_side_open_position("short") == "sell"
    assert om._exchange_side_reduce_position("long") == "sell"
    assert om._exchange_side_reduce_position("short") == "buy"


def test_verificar_trailing_stop_short_ratio_activation() -> None:
    """Short: ``max_price`` guarda el mínimo favorable; activación con entrada/ratio."""
    from core.strategies.exit.salida_trailing_stop import verificar_trailing_stop

    df = _ohlcv(35)
    entrada = 100.0
    ratio = 1.015
    trigger = entrada / ratio
    info = {
        "precio_entrada": entrada,
        "max_price": trigger - 0.5,
        "direccion": "short",
        "symbol": "BTC/EUR",
    }
    cerrar, msg = verificar_trailing_stop(
        dict(info),
        float(df["close"].iloc[-1]),
        df,
        config={"trailing_start_ratio": ratio},
    )
    assert isinstance(cerrar, bool)
    assert isinstance(msg, str)


def test_reajuste_tp_sl_obtener_archivo() -> None:
    from core.data import reajuste_tp_sl as r

    p = r.obtener_archivo("BTC/EUR")
    assert "BTC_EUR" in p.replace("\\", "/")


def test_adaptador_persistencia_calcular() -> None:
    from core.data import adaptador_persistencia as ap

    v = ap.calcular_persistencia_minima("S", _ohlcv(25), "alcista", base_minimo=1.0)
    assert isinstance(v, float) and v >= 0.5


def test_external_feeds_normalizers() -> None:
    from core.data import external_feeds as ef

    f = ef.normalizar_funding_rate({"symbol": "X", "fundingRate": "0.01", "fundingTime": 1})
    assert f["type"] == "funding_rate" and f["value"] == 0.01
    oi = ef.normalizar_open_interest({"symbol": "Y", "openInterest": "123"})
    assert oi["type"] == "open_interest"
    n = ef.normalizar_noticia({"symbol": "Z", "title": "t"})
    assert n["type"] == "news"


def test_gestor_capital_kelly() -> None:
    from core import gestor_capital as gc

    assert gc.calcular_fraccion_kelly(0.6, 1.5) > 0
    assert gc.calcular_fraccion_kelly(0.6, 0.0) == 0.0


def test_gestor_capital_redistribuir() -> None:
    from core import gestor_capital as gc

    out = gc.redistribuir_capital(
        1000.0,
        {"A": {"volatilidad": 0.02}, "B": {"volatilidad": 0.04}},
        {},
        {"riesgo_base": 0.02, "riesgo_max": 0.05, "capital_min": 1.0},
    )
    assert set(out.keys()) == {"A", "B"}


def test_gestor_warmup_validar_contiguo_y_emit() -> None:
    from core import gestor_warmup as gw

    events: list[tuple[str, dict]] = []

    def on_e(evt: str, data: dict) -> None:
        events.append((evt, data))

    gw._emit(on_e, "t", {"a": 1})
    assert events == [("t", {"a": 1})]
    base = 60_000
    velas = [
        {"timestamp": base, "close": 1.0},
        {"timestamp": base + base, "close": 1.1},
    ]
    assert gw._validar_contiguo(velas, base) is True
    assert gw._validar_contiguo([velas[0]], base) is True


@pytest.mark.asyncio
async def test_gestor_warmup_symbols_vacio() -> None:
    from core import gestor_warmup as gw

    out = await gw.warmup_symbols([], "5m", cliente=None, min_bars=5)
    assert out == {}


def test_metricas_tracker_tmp(tmp_path: Path) -> None:
    from core import metricas_semanales as ms

    p = tmp_path / "m.json"
    t = ms.MetricasTracker(archivo=str(p))
    t.registrar_sl_evitado()
    assert t.data["sl_evitas"] >= 1


def test_analisis_previo_salida_permitir_corto_df() -> None:
    from core.strategies.exit import analisis_previo_salida as aps

    assert aps.permitir_cierre_tecnico("S", pd.DataFrame({"close": [1.0]}), 1.0, {}) is True


def test_analisis_previo_entrada_datos_cortos() -> None:
    from core.strategies.entry import analisis_previo as ap

    assert ap.validar_condiciones_tecnicas_extra("S", pd.DataFrame({"close": range(10)}), 1.0, 0.5, 2.0) is False


def test_ajustador_pesos_softmax(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from core.strategies import ajustador_pesos as ap

    monkeypatch.chdir(tmp_path)
    r = ap.ajustar_pesos_por_desempeno({"BTC": {"a": 1.0, "b": 2.0}}, "out/pesos.json")
    assert "BTC" in r


def test_ajustador_pesos_softmax_collapse(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from core.strategies import ajustador_pesos as ap

    monkeypatch.chdir(tmp_path)
    r = ap.ajustar_pesos_por_desempeno({"X": {"a": -1.0, "b": -2.0}}, "out/p2.json")
    assert r == {}


def test_diag_auditoria_helpers() -> None:
    from core.diag import auditoria_adaptadores as aa

    assert aa._float_delta(3.0, 5.0) == 2.0
    df = aa._generar_dataset_sintetico("BTC/EUR", barras=32)
    assert len(df) == 32 and "close" in df.columns


def test_adaptador_configuracion_dinamica_validadores() -> None:
    from core import adaptador_configuracion_dinamica as acd

    assert acd._validar_dataframe(pd.DataFrame({"close": [1]})) is False
    assert acd._validar_coherencia_tp_sl(1.0, 2.0) is True
    assert acd._validar_coherencia_tp_sl(2.0, 1.0) is False


@pytest.mark.skipif(sys.platform != "win32", reason="solo Windows")
def test_binance_time_sync_non_admin_returns_false(monkeypatch: pytest.MonkeyPatch) -> None:
    from core.utils import binance_time_sync as bts

    class Resp:
        def read(self) -> bytes:
            return b'{"serverTime": 1700000000000}'

        def __enter__(self) -> Resp:
            return self

        def __exit__(self, *a) -> None:
            return None

    monkeypatch.setattr(bts.urllib.request, "urlopen", lambda *a, **k: Resp())
    assert bts.try_sync_windows_system_time_from_binance(timeout=1.0) is False


@pytest.mark.skipif(sys.platform == "win32", reason="no aplica en Windows")
def test_binance_time_sync_skips_non_windows() -> None:
    from core.utils import binance_time_sync as bts

    assert bts.try_sync_windows_system_time_from_binance() is False


def test_binance_time_sync_normalize_mode() -> None:
    from core.utils import binance_time_sync as bts

    assert bts._normalize_mode(None) == "auto"
    assert bts._normalize_mode("") == "auto"
    assert bts._normalize_mode("true") == "auto"
    assert bts._normalize_mode("on") == "auto"
    assert bts._normalize_mode("AUTO") == "auto"
    assert bts._normalize_mode("off") == "off"
    assert bts._normalize_mode("FALSE") == "off"
    assert bts._normalize_mode("no") == "off"
    assert bts._normalize_mode("elevate") == "elevate"
    assert bts._normalize_mode("UAC") == "elevate"
    assert bts._normalize_mode("runas") == "elevate"


def test_binance_time_sync_auto_off_short_circuits(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from core.utils import binance_time_sync as bts

    called = {"n": 0}

    def _boom(*_a, **_k) -> bool:
        called["n"] += 1
        return True

    monkeypatch.setattr(bts, "try_sync_windows_system_time_from_binance", _boom)
    monkeypatch.setattr(bts, "try_sync_via_w32tm_elevated", _boom)
    monkeypatch.setattr(bts, "_try_sync_unix", _boom)

    outcome = bts.auto_sync_system_clock_from_binance(mode="off")

    assert outcome["synced"] is False
    assert outcome["method"] == "skipped"
    assert outcome["mode"] == "off"
    assert called["n"] == 0


@pytest.mark.skipif(sys.platform != "win32", reason="solo Windows")
def test_binance_time_sync_auto_non_admin_reports_requires_admin(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from core.utils import binance_time_sync as bts

    monkeypatch.setattr(bts, "is_windows_admin", lambda: False)
    monkeypatch.setattr(
        bts,
        "try_sync_windows_system_time_from_binance",
        lambda *a, **k: False,
    )

    outcome = bts.auto_sync_system_clock_from_binance(mode="auto")

    assert outcome["synced"] is False
    assert outcome["method"] == "SetSystemTime:requires_admin"
    assert outcome["mode"] == "auto"


@pytest.mark.skipif(sys.platform != "win32", reason="solo Windows")
def test_binance_time_sync_auto_admin_calls_setsystemtime(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from core.utils import binance_time_sync as bts

    monkeypatch.setattr(bts, "is_windows_admin", lambda: True)
    monkeypatch.setattr(
        bts,
        "try_sync_windows_system_time_from_binance",
        lambda *a, **k: True,
    )

    outcome = bts.auto_sync_system_clock_from_binance(mode="auto")

    assert outcome["synced"] is True
    assert outcome["method"] == "SetSystemTime"
    assert outcome["elevated"] is True


def test_pid_lock_creates_and_releases_file(tmp_path: Path) -> None:
    from core.utils import pid_lock as pl

    target = tmp_path / "bot.pid"
    lock = pl.acquire_pid_lock(target)
    assert target.exists()
    payload = target.read_text(encoding="utf-8")
    assert str(lock.pid) in payload

    pl.release_pid_lock(lock)
    assert not target.exists()


def test_pid_lock_blocks_when_other_python_alive(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import pytest as _pytest

    from core.utils import pid_lock as pl

    target = tmp_path / "bot.pid"
    target.write_text('{"pid": 999999, "started_at": 0}', encoding="utf-8")

    monkeypatch.setattr(pl, "_process_is_alive", lambda pid: True)
    monkeypatch.setattr(pl, "_process_is_python", lambda pid: True)
    monkeypatch.delenv("BOT_PID_LOCK_FORCE", raising=False)

    with _pytest.raises(pl.PidLockError):
        pl.acquire_pid_lock(target)


def test_pid_lock_force_override_steals_lock(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from core.utils import pid_lock as pl

    target = tmp_path / "bot.pid"
    target.write_text('{"pid": 999999, "started_at": 0}', encoding="utf-8")

    monkeypatch.setattr(pl, "_process_is_alive", lambda pid: True)
    monkeypatch.setattr(pl, "_process_is_python", lambda pid: True)
    monkeypatch.setenv("BOT_PID_LOCK_FORCE", "true")

    lock = pl.acquire_pid_lock(target)
    assert lock.pid == os.getpid()
    pl.release_pid_lock(lock)


def test_pid_lock_stale_pid_takes_over(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from core.utils import pid_lock as pl

    target = tmp_path / "bot.pid"
    target.write_text('{"pid": 999999, "started_at": 0}', encoding="utf-8")

    monkeypatch.setattr(pl, "_process_is_alive", lambda pid: False)

    lock = pl.acquire_pid_lock(target)
    assert lock.pid == os.getpid()
    pl.release_pid_lock(lock)


def test_pid_lock_non_python_pid_takes_over(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from core.utils import pid_lock as pl

    target = tmp_path / "bot.pid"
    target.write_text('{"pid": 999999, "started_at": 0}', encoding="utf-8")

    monkeypatch.setattr(pl, "_process_is_alive", lambda pid: True)
    monkeypatch.setattr(pl, "_process_is_python", lambda pid: False)

    lock = pl.acquire_pid_lock(target)
    assert lock.pid == os.getpid()
    pl.release_pid_lock(lock)


def test_learning_trade_results_reexport() -> None:
    from learning.trade_results_manager import registrar_resultado_trade

    assert callable(registrar_resultado_trade)


def test_learning_aprendizaje_continuo_cargar_feedback(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import learning.aprendizaje_continuo as ac

    p = tmp_path / "fb.json"
    p.write_text('{"BTC/EUR": {"pesos": {}}}', encoding="utf-8")
    monkeypatch.setattr(ac, "FEEDBACK_PATH", str(p))
    assert ac._cargar_feedback("BTC/EUR") == {"pesos": {}}
    monkeypatch.setattr(ac, "FEEDBACK_PATH", str(tmp_path / "missing.json"))
    assert ac._cargar_feedback("X") == {}


def test_analisis_pesos_backtest_csv(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from core.strategies.entry import analisis_pesos as ap

    csv_p = tmp_path / "o.csv"
    rows = "\n".join(
        'cerrada,BTC/EUR,100,101,"{""rsi"": true}"' for _ in range(4)
    )
    csv_p.write_text(
        "status,symbol,precio_entrada,precio_salida,estrategias_activas\n" + rows + "\n",
        encoding="utf-8",
    )
    out_j = tmp_path / "pesos.json"
    monkeypatch.setattr(ap, "persist_entry_weights", lambda *a, **k: None)
    monkeypatch.setattr(
        ap,
        "gestor_pesos",
        SimpleNamespace(ruta=out_j, obtener_pesos_symbol=lambda _s: {"rsi": 1.0}),
    )
    r = ap.calcular_pesos_desde_backtest(str(csv_p), str(out_j))
    assert isinstance(r, dict)


def test_reajuste_tp_sl_con_archivo(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from core.data import reajuste_tp_sl as r

    sym = "ZZ/TEST"
    monkeypatch.setattr(r, "RUTA_RESULTADOS", str(tmp_path))
    fp = Path(r.obtener_archivo(sym))
    fp.parent.mkdir(parents=True, exist_ok=True)
    ahora = datetime.now(UTC)
    fp.write_text(
        "precio_entrada,precio_cierre,fecha_cierre\n"
        f"1.0,1.1,{ahora.isoformat()}\n",
        encoding="utf-8",
    )
    out = r.calcular_promedios_sl_tp(sym, dias=7)
    assert out is not None and len(out) == 2
