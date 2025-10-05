from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Generator
from typing import Any, Dict

import pytest

from core import procesar_vela as procesar_vela_mod
from core.procesar_vela import procesar_vela


@dataclass
class DummyConfig:
    symbols: list[str]
    intervalo_velas: str = "1m"
    trader_fastpath_enabled: bool = False
    trader_fastpath_threshold: int = 400
    trader_fastpath_skip_entries: bool = False


class DummyMetric:
    def labels(self, *_args: Any, **_kwargs: Any) -> "DummyMetric":
        return self

    def inc(self, *_args: Any, **_kwargs: Any) -> None:
        return None

    def set(self, *_args: Any, **_kwargs: Any) -> None:
        return None

    def observe(self, *_args: Any, **_kwargs: Any) -> None:
        return None


class DummyOrders:
    def __init__(self) -> None:
        self.created: list[Dict[str, Any]] = []
        self.requested: list[str] = []

    def obtener(self, symbol: str) -> None:
        self.requested.append(symbol)
        return None

    async def crear(self, **payload: Any) -> None:
        # Simula creación satisfactoria almacenando los datos exactos
        await asyncio.sleep(0)
        self.created.append(dict(payload))


class DummyTrader:
    def __init__(self, *, side: str, generar_propuesta: bool = True) -> None:
        self.config = DummyConfig(symbols=["BTCUSDT"])
        self.orders = DummyOrders()
        self.estado = {"BTCUSDT": {"trend": "up"}}
        self.notifications: list[tuple[str, str]] = []
        self.side = side
        self.generar_propuesta = generar_propuesta
        self.evaluaciones: list[tuple[str, Any, Any]] = []

    async def evaluar_condiciones_de_entrada(self, symbol: str, df, estado_symbol: Any):  # type: ignore[override]
        self.evaluaciones.append((symbol, df.copy(), estado_symbol))
        if not self.generar_propuesta:
            return None

        ultimo = float(df.iloc[-1]["close"])
        return {
            "symbol": symbol,
            "side": self.side,
            "precio_entrada": ultimo,
            "stop_loss": ultimo * 0.99,
            "take_profit": ultimo * 1.02,
            "score": 3.5,
            "meta": {"fuente": "unit-test"},
        }

    def enqueue_notification(self, mensaje: str, nivel: str = "INFO") -> None:
        self.notifications.append((mensaje, nivel))


@pytest.fixture(autouse=True)

def reset_buffers() -> Generator[Any, None, None]:
    procesar_vela_mod._buffers._estados.clear()
    procesar_vela_mod._buffers._locks.clear()

    metric = DummyMetric()
    originals = {
        name: getattr(procesar_vela_mod, name)
        for name in (
            "ENTRADAS_CANDIDATAS",
            "ENTRADAS_ABIERTAS",
            "ENTRADAS_RECHAZADAS_V2",
            "CANDLES_IGNORADAS",
            "HANDLER_EXCEPTIONS",
            "EVAL_LATENCY",
            "BUFFER_SIZE_V2",
            "WARMUP_RESTANTE",
            "LAST_BAR_AGE",
        )
    }
    for name in originals:
        setattr(procesar_vela_mod, name, metric)

    yield

    procesar_vela_mod._buffers._estados.clear()
    procesar_vela_mod._buffers._locks.clear()
    for name, value in originals.items():
        setattr(procesar_vela_mod, name, value)


def _build_candle(close: float) -> dict[str, Any]:
    return {
        "symbol": "BTCUSDT",
        "timestamp": 1_700_000_000,
        "open": close * 0.99,
        "high": close * 1.01,
        "low": close * 0.98,
        "close": close,
        "volume": 123.45,
    }


@pytest.mark.parametrize("side", ["long", "short"])
@pytest.mark.asyncio
async def test_procesar_vela_abre_operacion_para_oportunidad(side: str) -> None:
    trader = DummyTrader(side=side)
    candle = _build_candle(27_500.0)

    await procesar_vela(trader, candle)

    assert trader.orders.created, "La oportunidad detectada debe abrir una operación"
    created = trader.orders.created[0]

    assert created["symbol"] == "BTCUSDT"
    assert created["side"] == side
    assert created["precio"] == pytest.approx(candle["close"])
    assert created["meta"]["score"] == pytest.approx(3.5)
    assert created["meta"]["fuente"] == "unit-test"
    assert trader.notifications == [
        (f"Abrir {side} BTCUSDT @ {candle['close']:.6f}", "INFO")
    ]


@pytest.mark.asyncio
async def test_procesar_vela_omite_operacion_si_no_hay_oportunidad() -> None:
    trader = DummyTrader(side="long", generar_propuesta=False)
    candle = _build_candle(18_400.0)

    await procesar_vela(trader, candle)

    assert trader.orders.created == []
    # Debe seguir registrando la evaluación, demostrando que revisó la oportunidad
    assert len(trader.evaluaciones) == 1
    symbol, df, estado = trader.evaluaciones[0]
    assert symbol == "BTCUSDT"
    assert estado == {"trend": "up"}
    assert float(df.iloc[-1]["close"]) == pytest.approx(candle["close"])
    assert trader.notifications == []