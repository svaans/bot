"""Cobertura: helpers de real_orders, auditoría, balance, feeds HTTP mock, análisis previo salida."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any

import numpy as np
import pandas as pd
import pytest

from core.orders import real_orders as ro


def test_real_orders_coerce_y_extractores() -> None:
    assert ro._coerce_open_orders(None) == []
    assert ro._coerce_open_orders(({"a": 1},)) == [{"a": 1}]
    m = {"info": {"clientOrderId": "x1"}}
    assert ro._extraer_valor(m, ("clientOrderId",)) == "x1"
    assert ro._extraer_float({"price": "1.5"}, ("price",)) == 1.5
    assert ro._extraer_float({}, ("x",)) == 0.0
    assert ro._coincide_operation_id({"clientOrderId": "abc"}, "abc") is True
    assert ro._coincide_operation_id({}, "abc") is False


def test_real_orders_validar_orden() -> None:
    d = ro._validar_datos_orden({"symbol": "BTC/EUR", "precio_entrada": "1", "cantidad": 2})
    assert d["symbol"] == "BTC/EUR" and d["precio_entrada"] == 1.0
    with pytest.raises(ValueError):
        ro._validar_datos_orden({"symbol": "", "precio_entrada": 1, "cantidad": 1})


def test_real_orders_venta_fallida_flags() -> None:
    ro.limpiar_venta_fallida("SYM_TEST")
    assert ro.venta_fallida("SYM_TEST") is False
    ro.registrar_venta_fallida("SYM_TEST")
    assert ro.venta_fallida("SYM_TEST") is True
    ro.limpiar_venta_fallida("SYM_TEST")


def test_real_orders_auditar_desde_cerradas() -> None:
    orden_cerrada = {
        "clientOrderId": "op-99",
        "filled": 0.5,
        "remaining": 0.5,
        "average": 100.0,
    }

    class Cli:
        def fetch_closed_orders(self, symbol: str, limit: int = 10) -> list[dict]:
            return [orden_cerrada]

    out = ro._auditar_operacion_post_error(Cli(), "BTC/USDT", "op-99", 1.0)
    assert out is not None
    assert out["source"] == "closed_orders"
    assert out["ejecutado"] == 0.5


def test_real_orders_auditar_abiertas(monkeypatch: pytest.MonkeyPatch) -> None:
    orden_abierta = {
        "clientOrderId": "op-88",
        "filled": 0.0,
        "amount": 1.0,
        "price": 50.0,
    }

    class Cli:
        def fetch_closed_orders(self, *_a, **_k) -> list:
            return []

    monkeypatch.setattr(
        ro,
        "consultar_ordenes_abiertas",
        lambda _sym: [orden_abierta],
    )
    out = ro._auditar_operacion_post_error(Cli(), "ETH/USDT", "op-88", 1.0)
    assert out is not None
    assert out["source"] == "open_orders_pending"


def test_real_orders_acumular_metricas() -> None:
    ro._METRICAS_OPERACION.clear()
    resp: dict[str, Any] = {
        "symbol": "BTC/USDT",
        "trades": [
            {
                "side": "buy",
                "price": 100.0,
                "amount": 0.1,
                "fee": {"cost": 0.01, "currency": "BTC", "rate": 0.001},
            }
        ],
    }
    m = ro._acumular_metricas("oid-1", resp)
    assert "fee" in m and "pnl" in m


def test_real_orders_chunked() -> None:
    assert list(ro._chunked([1, 2, 3, 4], 2)) == [[1, 2], [3, 4]]


def test_real_orders_esperar_balance(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(ro.time, "sleep", lambda *_a, **_k: None)

    class Cli:
        def fetch_balance(self) -> dict[str, Any]:
            return {"free": {"BTC": 2.5}}

    got = ro.esperar_balance(Cli(), "BTC/USDT", 1.0, max_intentos=2, delay=0.0)
    assert got >= 1.0


@pytest.mark.asyncio
async def test_external_feeds_funding_404_marca_missing() -> None:
    from core.data import external_feeds as ef

    class Resp:
        status = 404

        async def json(self) -> list:
            return []

        async def __aenter__(self) -> Resp:
            return self

        async def __aexit__(self, *_a: Any) -> bool:
            return False

    class Sess:
        def get(self, _url: str) -> Resp:
            return Resp()

    feeds = ef.ExternalFeeds(session=Sess())
    feeds._funding_permanent_missing.discard("BTCUSDT")
    data = await feeds.funding_rate_rest("BTCUSDT")
    assert data is None
    assert "BTCUSDT" in feeds._funding_permanent_missing


@pytest.mark.asyncio
async def test_external_feeds_funding_ok_cached() -> None:
    from core.data import external_feeds as ef
    import time as time_mod

    payload = {"fundingRate": "0.0001", "fundingTime": 1}

    class Resp:
        status = 200

        async def json(self) -> list:
            return [payload]

        async def __aenter__(self) -> Resp:
            return self

        async def __aexit__(self, *_a: Any) -> bool:
            return False

    class Sess:
        def get(self, _url: str) -> Resp:
            return Resp()

    feeds = ef.ExternalFeeds(session=Sess())
    feeds._funding_cache.clear()
    d1 = await feeds.funding_rate_rest("ZZUSDT")
    assert d1 is not None
    feeds._funding_cache["ZZUSDT"] = (time_mod.time() + 3600.0, {"cached": True})
    d2 = await feeds.funding_rate_rest("ZZUSDT")
    assert d2 == {"cached": True}


def test_analisis_previo_salida_precio_soporte_corta() -> None:
    from core.strategies.exit import analisis_previo_salida as aps

    assert aps.precio_cerca_de_soporte(pd.DataFrame({"close": [1.0]}), 1.0) is False


def test_analisis_previo_salida_evaluar_cierre_evita(monkeypatch: pytest.MonkeyPatch) -> None:
    from core.strategies.exit import analisis_previo_salida as aps

    n = 28
    close = np.linspace(100.0, 108.0, n)
    vol = np.array([100.0] * (n - 1) + [280.0])
    df = pd.DataFrame(
        {
            "open": close - 0.05,
            "high": close + 0.2,
            "low": close - 0.2,
            "close": close,
            "volume": vol,
        }
    )
    monkeypatch.setattr(aps, "get_rsi", lambda _df: 62.0)
    monkeypatch.setattr(aps, "get_slope", lambda _df: 0.02)
    monkeypatch.setattr(aps, "detectar_tendencia", lambda _s, _df: ("alcista", {}))
    orden = {
        "direccion": "long",
        "tendencia": "alcista",
        "sl_evitar_info": [],
        "max_evitar_sl": 3,
    }
    assert aps.evaluar_condiciones_de_cierre_anticipado("BTC/EUR", df, orden, 1.0, {}) is False


@pytest.mark.asyncio
async def test_verificar_salidas_timeout_gather_principal(monkeypatch: pytest.MonkeyPatch) -> None:
    from core.strategies.exit import verificar_salidas as vs

    monkeypatch.setattr(vs, "is_flag_enabled", lambda *_a, **_k: False)
    monkeypatch.setattr(vs, "load_exit_config", lambda _s: {"timeout_validaciones": 0.05})

    async def hang(*_a: Any, **_k: Any) -> bool:
        await asyncio.sleep(10.0)
        return False

    monkeypatch.setattr(vs, "_chequear_contexto_macro", hang)
    monkeypatch.setattr(vs, "_manejar_stop_loss", hang)
    monkeypatch.setattr(vs, "_procesar_take_profit", hang)

    class Orden:
        symbol = "BTC/EUR"
        direccion = "long"

        def to_dict(self) -> dict:
            return {"symbol": self.symbol}

    class Orders:
        def obtener(self, _sym: str) -> Orden:
            return Orden()

    class Trader:
        orders = Orders()
        config_por_simbolo: dict = {}

        async def _piramidar(self, *_a, **_k) -> None:
            return None

    df = pd.DataFrame(
        {
            "close": np.linspace(100, 101, 35),
            "high": np.linspace(101, 102, 35),
            "low": np.linspace(99, 100, 35),
            "volume": np.ones(35) * 500,
        }
    )
    await vs.verificar_salidas(Trader(), "BTC/EUR", df)
