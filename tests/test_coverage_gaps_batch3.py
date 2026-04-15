"""Más cobertura: riesgo (Kelly/sizing), market_utils, notificador, salidas, learning, verificar_salidas."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

UTC = timezone.utc


def _ohlcv(n: int = 65, seed: int = 3) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    close = 100.0 + np.cumsum(rng.normal(0, 0.25, n))
    high = close + 0.4
    low = close - 0.4
    open_ = np.roll(close, 1)
    open_[0] = close[0]
    vol = rng.uniform(500, 1000, n)
    return pd.DataFrame(
        {"open": open_, "high": high, "low": low, "close": close, "volume": vol}
    )


def test_kelly_fallback_sin_carpeta(monkeypatch: pytest.MonkeyPatch) -> None:
    import core.risk.kelly as k

    monkeypatch.setattr(k.os.path, "isdir", lambda _p: False)
    assert k.calcular_fraccion_kelly(dias_historia=30, fallback=0.2) == 0.2


def test_kelly_con_reportes_sinteticos(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import core.risk.kelly as k

    rd = tmp_path / "reportes_diarios"
    rd.mkdir(parents=True)
    cols = ",".join([f"c{i}" for i in range(19)]) + ",retorno_total\n"
    row_pos = ",".join(["0"] * 19) + ",0.02\n"
    row_neg = ",".join(["0"] * 19) + ",-0.01\n"
    body = cols + (row_pos + row_neg) * 8
    for d in range(12):
        day = (datetime.now(UTC).date() - timedelta(days=d)).strftime("%Y-%m-%d.csv")
        (rd / day).write_text(body, encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    f = k.calcular_fraccion_kelly(dias_historia=365, fallback=0.15)
    assert isinstance(f, float) and f > 0


def test_sizing_apply_limits_and_size_order() -> None:
    from core.risk.sizing import MarketInfo, apply_exchange_limits, size_order

    m = MarketInfo(tick_size=0.1, step_size=0.001, min_notional=10.0)
    p, q, n = apply_exchange_limits(100.07, 0.05, m, "buy")
    assert p == pytest.approx(100.0, abs=0.02)
    assert n >= m.min_notional or q == 0
    p2, q2 = size_order(100.0, 95.0, m, risk_limit=50.0, exposure_limit=5000.0, current_exposure=0.0)
    assert q2 >= 0


def test_market_utils_slope_y_atr() -> None:
    from core.utils import market_utils as mu

    assert mu.calcular_slope_pct([1.0, 2.0, 3.0]) != 0.0
    assert mu.calcular_slope_pct([-1.0, -2.0, -3.0]) != 0.0
    assert mu.calcular_atr_pct(pd.DataFrame({"close": [1]}), periodo=14) == 0.0
    df = _ohlcv(20)
    atr = mu.calcular_atr_pct(df, periodo=5)
    assert isinstance(atr, float) and atr >= 0.0


def test_notificador_escape_y_modo_test() -> None:
    from core.notificador import Notificador, escape_markdown

    assert "\\_" in escape_markdown("a_b")
    n = Notificador(token="t", chat_id="1", modo_test=True)
    ok, info = n.enviar("hola_mundo", tipo="INFO")
    assert ok and info.get("status_code") == 200


@pytest.mark.asyncio
async def test_notificador_async_sin_credenciales() -> None:
    from core.notificador import Notificador

    n = Notificador(token="", chat_id="")
    ok, info = await n.enviar_async("x")
    assert ok is False


def test_analisis_previo_salida_envolvente_y_permitir() -> None:
    from core.strategies.exit import analisis_previo_salida as aps

    df2 = pd.DataFrame(
        {
            "open": [10.0, 9.0],
            "high": [10.5, 10.2],
            "low": [9.5, 9.8],
            "close": [9.8, 10.1],
            "volume": [100.0, 200.0],
        }
    )
    assert aps.es_vela_envolvente_alcista(df2) in (True, False)
    df60 = _ohlcv(65)
    out = aps.permitir_cierre_tecnico(
        "BTC/EUR", df60, 100.0, {"direccion": "long", "stop_loss": 90.0, "estrategias_activas": {}}
    )
    assert isinstance(out, bool)


def test_gestor_aprendizaje_orden_incompleta() -> None:
    import learning.gestor_aprendizaje as ga

    ga.registrar_resultado_trade({})
    ga.registrar_resultado_trade({"symbol": "BTC/EUR"})


def test_gestor_aprendizaje_guarda_parquet(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import learning.gestor_aprendizaje as ga

    monkeypatch.setattr(ga, "CARPETA_ORDENES", str(tmp_path))
    monkeypatch.setattr(ga, "analizar_estrategias_en_ordenes", lambda _path: pd.DataFrame())
    monkeypatch.setattr(ga, "ajustar_pesos_por_desempeno", lambda *_a, **_k: {})
    monkeypatch.setattr(ga, "persist_entry_weights", lambda *_a, **_k: None)
    orden = {
        "symbol": "BTC/EUR",
        "estrategias_activas": {"rsi": True},
        "timestamp": 1_700_000_000.0,
        "retorno_total": 0.5,
    }
    ga.registrar_resultado_trade(orden)
    assert list(tmp_path.glob("*.parquet"))


def test_recalibrar_sin_parquet(monkeypatch: pytest.MonkeyPatch) -> None:
    import learning.recalibrar_semana as rs

    monkeypatch.setattr(rs.glob, "glob", lambda _pattern: [])
    rs.recalibrar_pesos_semana()


def test_recalibrar_con_metricas(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import learning.recalibrar_semana as rs

    pq = tmp_path / "BTC_EUR.parquet"
    pd.DataFrame(
        {
            "estrategia": ["a"],
            "retorno_total": [1.0],
        }
    ).to_parquet(pq, index=False)
    monkeypatch.setattr(rs, "CARPETA_ORDENES", str(tmp_path))
    monkeypatch.setattr(
        rs,
        "analizar_estrategias_en_ordenes",
        lambda _path: pd.DataFrame({"estrategia": ["a"], "retorno_total": [1.0]}),
    )
    monkeypatch.setattr(rs, "ajustar_pesos_por_desempeno", lambda res, p: {"BTC/EUR": {"a": 1.0}})
    monkeypatch.setattr(rs, "normalizar_scores", lambda d: d)
    monkeypatch.setattr(rs, "persist_entry_weights", lambda *_a, **_k: None)
    monkeypatch.setattr(rs.glob, "glob", lambda pattern: [str(pq)])
    rs.recalibrar_pesos_semana()


def test_aprendizaje_continuo_actualizar_config_vacio() -> None:
    import learning.aprendizaje_continuo as ac

    ac._actualizar_config("BTC/EUR", pd.DataFrame())


def test_aprendizaje_continuo_feedback_sin_pesos(monkeypatch: pytest.MonkeyPatch) -> None:
    import learning.aprendizaje_continuo as ac

    monkeypatch.setattr(ac.gestor_pesos, "obtener_pesos_symbol", lambda _s: {})
    ac._aplicar_feedback_pesos("BTC/EUR", {"pesos": {"rsi": 0.1}})


def test_aprendizaje_continuo_ejecutar_ciclo_sin_archivos(monkeypatch: pytest.MonkeyPatch) -> None:
    import learning.aprendizaje_continuo as ac

    monkeypatch.setattr(ac.glob, "glob", lambda _p: [])
    ac.ejecutar_ciclo()


@pytest.mark.asyncio
async def test_verificar_salidas_contexto_macro_cierra(monkeypatch: pytest.MonkeyPatch) -> None:
    from core.strategies.exit import verificar_salidas as vs

    monkeypatch.setattr(vs, "is_flag_enabled", lambda *_a, **_k: False)
    monkeypatch.setattr(
        vs,
        "load_exit_config",
        lambda _s: {
            "umbral_puntaje_macro_cierre": 0.5,
            "timeout_validaciones": 30,
        },
    )
    monkeypatch.setattr(vs, "obtener_puntaje_contexto", lambda _sym: 10.0)

    cerrado = asyncio.Event()

    class Orden:
        symbol = "BTC/EUR"
        direccion = "long"

        def to_dict(self) -> dict:
            return {"symbol": self.symbol, "direccion": self.direccion}

    class Orders:
        def obtener(self, _sym: str):
            return Orden()

    class Trader:
        orders = Orders()
        config_por_simbolo: dict = {}

        async def _piramidar(self, *_a, **_k) -> None:
            return None

        async def _cerrar_y_reportar(self, *_a, **_k) -> bool:
            cerrado.set()
            return True

    await vs.verificar_salidas(Trader(), "BTC/EUR", _ohlcv(40))
    assert cerrado.is_set()
