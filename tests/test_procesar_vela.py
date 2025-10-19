from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from math import isclose
from typing import Any, Dict, Generator

import pytest

from core import procesar_vela as procesar_vela_mod
from core.procesar_vela import procesar_vela
from core.operational_mode import OperationalMode
from indicadores.atr import calcular_atr
from indicadores.momentum import calcular_momentum
from indicadores.rsi import calcular_rsi


@dataclass
class DummyConfig:
    symbols: list[str]
    intervalo_velas: str = "1m"
    modo_real: bool = False
    modo_operativo: OperationalMode = OperationalMode.PAPER_TRADING
    trader_fastpath_enabled: bool = False
    trader_fastpath_threshold: int = 400
    trader_fastpath_resume_threshold: int = 350
    trader_fastpath_skip_entries: bool = False
    indicadores_incremental_enabled: bool = False


class DummyMetric:
    def labels(self, *_args: Any, **_kwargs: Any) -> "DummyMetric":
        return self

    def inc(self, *_args: Any, **_kwargs: Any) -> None:
        return None

    def set(self, *_args: Any, **_kwargs: Any) -> None:
        return None

    def observe(self, *_args: Any, **_kwargs: Any) -> None:
        return None


class RecordingMetric(DummyMetric):
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []
        self.inc_calls = 0

    def labels(self, *_args: Any, **labels: Any) -> "RecordingMetric":  # type: ignore[override]
        self.calls.append(labels)
        return self

    def inc(self, *_args: Any, **_kwargs: Any) -> None:  # type: ignore[override]
        self.inc_calls += 1


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


class FlakyOrders(DummyOrders):
    def __init__(self, failures_before_success: int) -> None:
        super().__init__()
        self.failures_before_success = failures_before_success
        self.trace_ids: list[str | None] = []

    async def crear(self, **payload: Any) -> None:  # type: ignore[override]
        self.trace_ids.append(payload.get("meta", {}).get("trace_id"))
        if self.failures_before_success > 0:
            self.failures_before_success -= 1
            raise RuntimeError("transient_failure")
        await super().crear(**payload)


class AlwaysFailOrders(DummyOrders):
    def __init__(self) -> None:
        super().__init__()
        self.attempts = 0
        self.trace_ids: list[str | None] = []

    async def crear(self, **payload: Any) -> None:  # type: ignore[override]
        self.attempts += 1
        self.trace_ids.append(payload.get("meta", {}).get("trace_id"))
        raise RuntimeError("exchange_down")


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


class WarmupTrader:
    def __init__(self) -> None:
        self.config = DummyConfig(symbols=["BTCUSDT"])
        self.orders = DummyOrders()
        self.estado = {"BTCUSDT": {}}
        self._last_eval_skip_reason: str | None = None
        self._last_eval_skip_details: Dict[str, Any] | None = None

    def is_symbol_ready(self, symbol: str, timeframe: str | None = None) -> bool:
        return True

    async def evaluar_condiciones_de_entrada(self, symbol: str, df, estado_symbol: Any):
        self._last_eval_skip_reason = "warmup"
        self._last_eval_skip_details = {
            "buffer_len": len(df),
            "min_needed": 5,
            "timeframe": getattr(df, "tf", None),
            "score": -1.0,
        }
        return None
    

class RecordingBus:
    def __init__(self) -> None:
        self.emitted: list[tuple[str, Dict[str, Any]]] = []

    def emit(self, name: str, payload: Dict[str, Any]) -> None:
        self.emitted.append((name, dict(payload)))


@pytest.fixture(autouse=True)

def reset_buffers() -> Generator[Any, None, None]:
    procesar_vela_mod._buffers._estados.clear()
    procesar_vela_mod._buffers._locks.clear()
    procesar_vela_mod._ORDER_CIRCUITS.clear()

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
            "PARSE_LATENCY",
            "GATING_LATENCY",
            "STRATEGY_LATENCY",
            "BUFFER_SIZE_V2",
            "WARMUP_RESTANTE",
            "LAST_BAR_AGE",
            "SPREAD_GUARD_MISSING",
        )
    }
    for name in originals:
        setattr(procesar_vela_mod, name, metric)

    yield

    procesar_vela_mod._buffers._estados.clear()
    procesar_vela_mod._buffers._locks.clear()
    procesar_vela_mod._ORDER_CIRCUITS.clear()
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


@pytest.mark.asyncio
@pytest.mark.parametrize("timeframe", ["1m", "5m"])
async def test_incomplete_candle_is_skipped_before_evaluation(timeframe: str) -> None:
    trader = DummyTrader(side="long", generar_propuesta=False)
    trader.config.intervalo_velas = timeframe
    trader.config.min_bars = 1
    trader.min_bars = 1  # type: ignore[attr-defined]

    incomplete = _build_candle(101.0)
    incomplete.update(
        {
            "timeframe": timeframe,
            "is_closed": False,
            "is_final": False,
            "final": False,
        }
    )

    await procesar_vela(trader, incomplete)

    assert trader.evaluaciones == []


@pytest.mark.asyncio
async def test_strategy_stage_latency_event_is_emitted() -> None:
    trader = DummyTrader(side="long")
    trader.config.min_bars = 1
    trader.min_bars = 1  # type: ignore[attr-defined]
    trader.bus = RecordingBus()

    await procesar_vela(trader, _build_candle(102.0))

    stages = [payload["stage"] for name, payload in trader.bus.emitted if name == "procesar_vela.latency"]
    assert "strategy" in stages


def test_buffer_manager_clear_specific_timeframe() -> None:
    manager = procesar_vela_mod.get_buffer_manager()
    candle_1m = {**_build_candle(27_000.0), "timeframe": "1m"}
    candle_5m = {**_build_candle(27_500.0), "timeframe": "5m"}

    manager.append("BTCUSDT", candle_1m)
    manager.append("BTCUSDT", candle_5m)

    assert manager.size("BTCUSDT", "1m") == 1
    assert manager.size("BTCUSDT", "5m") == 1

    manager.clear("BTCUSDT", "1m")

    assert manager.size("BTCUSDT", "1m") == 0
    assert manager.size("BTCUSDT", "5m") == 1


def test_buffer_manager_clear_all_drop_state() -> None:
    manager = procesar_vela_mod.get_buffer_manager()
    candle_1m = {**_build_candle(28_000.0), "timeframe": "1m"}
    candle_15m = {**_build_candle(28_500.0), "timeframe": "15m"}

    manager.append("ETHUSDT", candle_1m)
    manager.append("ETHUSDT", candle_15m)

    # Sanity check before limpiar
    assert manager.size("ETHUSDT", "1m") == 1
    assert manager.size("ETHUSDT", "15m") == 1

    manager.clear("ETHUSDT", drop_state=True)

    # El estado completo debe desaparecer
    assert procesar_vela_mod._buffers._estados.get("ETHUSDT") is None
    # Reutilizar la API no debería fallar tras limpiar
    manager.append("ETHUSDT", candle_1m)
    assert manager.size("ETHUSDT", "1m") == 1


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
    assert isinstance(created["meta"].get("trace_id"), str)
    assert len(created["meta"]["trace_id"]) >= 8
    assert trader.notifications == [
        (f"Abrir {side} BTCUSDT @ {candle['close']:.6f}", "INFO")
    ]


@pytest.mark.asyncio
async def test_procesar_vela_registra_metricas_spread_guard_missing() -> None:
    trader = DummyTrader(side="long", generar_propuesta=False)
    trader.spread_guard = object()
    candle = _build_candle(30_000.0)

    metric = RecordingMetric()
    original_metric = procesar_vela_mod.SPREAD_GUARD_MISSING
    procesar_vela_mod.SPREAD_GUARD_MISSING = metric

    logger = logging.getLogger("procesar_vela")
    records: list[logging.LogRecord] = []

    class _ListHandler(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            records.append(record)

    handler = _ListHandler()
    logger.addHandler(handler)

    try:
        await procesar_vela(trader, candle)
    finally:
        procesar_vela_mod.SPREAD_GUARD_MISSING = original_metric
        logger.removeHandler(handler)

    assert metric.calls == [{"symbol": "BTCUSDT", "timeframe": "1m"}]
    assert metric.inc_calls == 1
    assert any(record.getMessage() == "spread_guard_missing" for record in records)


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


@pytest.mark.asyncio
async def test_procesar_vela_marca_skip_reason_de_trader() -> None:
    trader = WarmupTrader()
    candle = _build_candle(19_000.0)

    await procesar_vela(trader, candle)

    assert candle.get("_df_skip_reason") == "warmup"
    details = candle.get("_df_skip_details")
    assert isinstance(details, dict)
    assert details.get("buffer_len", 0) >= 1
    assert details.get("min_needed") == 5
    assert details.get("gate") == "warmup"
    assert details.get("score") == pytest.approx(-1.0)
    trace_id = candle.get("_df_trace_id")
    assert isinstance(trace_id, str) and len(trace_id) >= 8


@pytest.mark.asyncio
async def test_procesar_vela_reintenta_apertura_preservando_trace_id() -> None:
    trader = DummyTrader(side="long")
    flaky = FlakyOrders(failures_before_success=2)
    trader.orders = flaky
    candle = _build_candle(21_000.0)

    await procesar_vela(trader, candle)

    assert len(trader.orders.created) == 1
    assert len(flaky.trace_ids) == procesar_vela_mod._ORDER_CREATION_MAX_ATTEMPTS
    assert all(isinstance(t, str) and len(t) >= 8 for t in flaky.trace_ids)
    assert len(set(flaky.trace_ids)) == 1
    meta = trader.orders.created[0]["meta"]
    assert meta.get("trace_id") == flaky.trace_ids[0]


@pytest.mark.asyncio
async def test_procesar_vela_circuit_breaker_abre_y_evita_reintentos() -> None:
    trader = DummyTrader(side="long")
    always_fail = AlwaysFailOrders()
    trader.orders = always_fail
    candle = _build_candle(19_500.0)

    await procesar_vela(trader, candle)

    assert candle.get("_df_skip_reason") == "orders_circuit_open"
    details = candle.get("_df_skip_details") or {}
    assert details.get("retry_after", 0.0) > 0.0
    assert details.get("failures", 0) >= procesar_vela_mod._ORDER_CIRCUIT_MAX_FAILURES
    assert always_fail.attempts == procesar_vela_mod._ORDER_CREATION_MAX_ATTEMPTS
    assert always_fail.trace_ids
    assert len(set(t for t in always_fail.trace_ids if t)) <= 1

    candle2 = _build_candle(19_510.0)
    await procesar_vela(trader, candle2)

    assert always_fail.attempts == procesar_vela_mod._ORDER_CREATION_MAX_ATTEMPTS
    assert candle2.get("_df_skip_reason") == "orders_circuit_open"


@pytest.mark.asyncio
async def test_incremental_indicators_sincronizados_con_batch() -> None:
    trader = DummyTrader(side="long", generar_propuesta=False)
    trader.config.indicadores_incremental_enabled = True
    trader.estado["BTCUSDT"] = {}

    base_ts = 1_700_000_000_000
    for idx in range(20):
        close = 100.0 + idx
        candle = {
            "symbol": "BTCUSDT",
            "timestamp": base_ts + idx * 60_000,
            "open": close - 0.5,
            "high": close + 1.0,
            "low": close - 1.0,
            "close": close,
            "volume": 1_000.0 + idx,
            "is_closed": True,
        }
        await procesar_vela(trader, candle)

    assert trader.evaluaciones
    symbol, df_final, _ = trader.evaluaciones[-1]
    assert symbol == "BTCUSDT"

    estado_symbol = trader.estado["BTCUSDT"]
    caches = estado_symbol.get("indicadores_cache")
    assert isinstance(caches, dict)

    rsi_val = caches.get("rsi", {}).get("valor")
    momentum_val = caches.get("momentum", {}).get("valor")
    atr_val = caches.get("atr", {}).get("valor")

    assert rsi_val is not None
    assert momentum_val is not None
    assert atr_val is not None

    rsi_batch = calcular_rsi(df_final, 14)
    atr_batch = calcular_atr(df_final, 14)
    momentum_batch = calcular_momentum(df_final, 10)

    assert rsi_batch is not None
    assert atr_batch is not None

    assert isclose(float(rsi_val), float(rsi_batch), rel_tol=1e-7, abs_tol=1e-7)
    assert isclose(float(momentum_val), float(momentum_batch), rel_tol=1e-7, abs_tol=1e-7)
    assert isclose(float(atr_val), float(atr_batch), rel_tol=1e-7, abs_tol=1e-7)

    cache_obj = df_final.attrs.get("_indicators_cache")
    assert cache_obj is not None
