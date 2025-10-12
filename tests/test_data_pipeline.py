import asyncio
import contextlib
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any, Dict, List, Tuple

import pandas as pd
import pytest

import sys
import importlib.util

if "indicators" not in sys.modules:
    indicators_pkg = ModuleType("indicators")
    indicators_pkg.__path__ = []  # type: ignore[attr-defined]
    sys.modules["indicators"] = indicators_pkg
    for nombre in ("ema", "rsi"):
        stub = ModuleType(f"indicators.{nombre}")
        sys.modules[f"indicators.{nombre}"] = stub

from core import procesar_vela as procesar_vela_mod
from core.data_feed import DataFeed
from core.data_feed import handlers as df_handlers
from core.procesar_vela import procesar_vela as procesar_vela_handler
from core.trader.trader_lite import TraderLite
from tests.factories import DummyConfig, DummySupervisor


def _load_verificar_entradas():
    file_path = Path(__file__).resolve().parents[1] / "core/strategies/entry/verificar_entradas.py"
    spec = importlib.util.spec_from_file_location("verificar_entradas_unit", file_path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(module)  # type: ignore[arg-type]
    return module


verificar_mod = _load_verificar_entradas()
verificar_func = verificar_mod.verificar_entrada


@pytest.mark.asyncio
async def test_datafeed_consumer_executes_registered_handler() -> None:
    symbol = "BTCUSDT"
    processed: List[dict] = []

    async def handler(candle: dict) -> None:
        processed.append(candle)

    feed = DataFeed("1m", handler_timeout=1.0)
    feed._symbols = [symbol]
    feed._queues[symbol] = asyncio.Queue()
    feed._handler = handler
    feed._running = True

    consumer = asyncio.create_task(feed._consumer(symbol))

    candle = {"symbol": symbol, "timestamp": 1_700_000_000}
    await feed._queues[symbol].put(candle)

    async def _wait_for_processing() -> None:
        for _ in range(100):
            if processed:
                return
            await asyncio.sleep(0.01)
        raise AssertionError("handler no fue invocado por el consumer")

    await _wait_for_processing()

    assert processed[0]["timestamp"] == candle["timestamp"]
    assert feed._stats[symbol]["processed"] == 1

    feed._running = False
    consumer.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await consumer

@pytest.mark.asyncio
async def test_datafeed_consumer_skip_expected_downgrades_log_and_counts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    symbol = "BTCUSDT"

    class DummyCounter:
        def __init__(self) -> None:
            self.calls: List[tuple[str, ...]] = []

        def labels(self, *values: str, **kwargs: str) -> "DummyCounter":
            if kwargs:
                ordered_kwargs = tuple(f"{key}={value}" for key, value in sorted(kwargs.items()))
                self.calls.append(("labels", *values, *ordered_kwargs))
            else:
                self.calls.append(("labels", *values))
            return self

        def inc(self, amount: float = 1.0) -> None:
            self.calls.append(("inc", str(amount)))

    dummy_counter = DummyCounter()
    monkeypatch.setattr(df_handlers, "CONSUMER_SKIPPED_EXPECTED_TOTAL", dummy_counter)

    class DummyLog:
        def __init__(self) -> None:
            self.calls: List[tuple[str, str, dict[str, Any]]] = []

        def _record(self, level: str, message: str, *args: Any, **kwargs: Any) -> None:
            extra = kwargs.get("extra")
            if isinstance(extra, dict):
                payload = dict(extra)
            else:
                payload = {"extra": extra}
            self.calls.append((level, message, payload))

        def debug(self, message: str, *args: Any, **kwargs: Any) -> None:
            self._record("debug", message, *args, **kwargs)

        def info(self, message: str, *args: Any, **kwargs: Any) -> None:
            self._record("info", message, *args, **kwargs)

        def warning(self, message: str, *args: Any, **kwargs: Any) -> None:
            self._record("warning", message, *args, **kwargs)

        def error(self, message: str, *args: Any, **kwargs: Any) -> None:
            self._record("error", message, *args, **kwargs)

        def exception(self, message: str, *args: Any, **kwargs: Any) -> None:
            self._record("exception", message, *args, **kwargs)

    dummy_log = DummyLog()
    monkeypatch.setattr(df_handlers, "log", dummy_log)

    async def handler(candle: dict) -> None:
        candle["timeframe"] = "1m"
        candle["_df_skip_reason"] = "no_signal"

    feed = DataFeed("1m", handler_timeout=1.0)
    feed._symbols = [symbol]
    feed._queues[symbol] = asyncio.Queue()
    feed._handler = handler
    feed._running = True

    consumer = asyncio.create_task(feed._consumer(symbol))

    candle = {"symbol": symbol, "timestamp": 1_700_000_000}
    await feed._queues[symbol].put(candle)

    for _ in range(100):
        if feed._stats[symbol]["skipped_expected"]:
            break
        await asyncio.sleep(0.01)
    else:
        raise AssertionError("Skip esperado no registrado")

    skip_calls = [call for call in dummy_log.calls if call[1] == "consumer.skip"]
    assert skip_calls, "Se esperaba un log de consumer.skip"
    assert skip_calls[-1][0] == "debug"

    assert feed._stats[symbol]["skipped_expected"] == 1
    assert any(call[0] == "inc" for call in dummy_counter.calls)

    feed._running = False
    consumer.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await consumer


@pytest.mark.asyncio
async def test_datafeed_consumer_skip_non_expected_logs_info(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    symbol = "BTCUSDT"

    class DummyLog:
        def __init__(self) -> None:
            self.calls: List[tuple[str, str, dict[str, Any]]] = []

        def _record(self, level: str, message: str, *args: Any, **kwargs: Any) -> None:
            extra = kwargs.get("extra")
            if isinstance(extra, dict):
                payload = dict(extra)
            else:
                payload = {"extra": extra}
            self.calls.append((level, message, payload))

        def debug(self, message: str, *args: Any, **kwargs: Any) -> None:
            self._record("debug", message, *args, **kwargs)

        def info(self, message: str, *args: Any, **kwargs: Any) -> None:
            self._record("info", message, *args, **kwargs)

        def warning(self, message: str, *args: Any, **kwargs: Any) -> None:
            self._record("warning", message, *args, **kwargs)

        def error(self, message: str, *args: Any, **kwargs: Any) -> None:
            self._record("error", message, *args, **kwargs)

        def exception(self, message: str, *args: Any, **kwargs: Any) -> None:
            self._record("exception", message, *args, **kwargs)

    dummy_log = DummyLog()
    monkeypatch.setattr(df_handlers, "log", dummy_log)

    async def handler(candle: dict) -> None:
        candle["_df_skip_reason"] = "warmup"

    feed = DataFeed("1m", handler_timeout=1.0)
    feed._symbols = [symbol]
    feed._queues[symbol] = asyncio.Queue()
    feed._handler = handler
    feed._running = True

    consumer = asyncio.create_task(feed._consumer(symbol))

    candle = {"symbol": symbol, "timestamp": 1_700_000_001}
    await feed._queues[symbol].put(candle)

    for _ in range(100):
        if feed._stats[symbol]["skipped"]:
            break
        await asyncio.sleep(0.01)
    else:
        raise AssertionError("Skip no esperado no registrado")

    skip_calls = [call for call in dummy_log.calls if call[1] == "consumer.skip"]
    assert skip_calls, "Se esperaba log consumer.skip"
    assert skip_calls[-1][0] == "info"

    feed._running = False
    consumer.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await consumer


@pytest.mark.asyncio
async def test_datafeed_backfill_latency_event(monkeypatch: pytest.MonkeyPatch) -> None:
    from core.data_feed import datafeed as datafeed_mod

    symbol = "ETHUSDT"
    feed = DataFeed("1m", handler_timeout=1.0)
    feed._backfill_latency_threshold = 0.0

    async def fake_backfill(_feed: DataFeed, _symbol: str) -> None:
        await asyncio.sleep(0)

    emitted: List[Tuple[str, Dict[str, Any]]] = []

    monkeypatch.setattr(datafeed_mod.backfill_module, "do_backfill", fake_backfill)
    monkeypatch.setattr(
        datafeed_mod.events_module,
        "emit_bus_signal",
        lambda df, evt, payload: emitted.append((evt, payload)),
    )

    await feed._do_backfill(symbol)

    assert emitted, "Debe emitirse un evento backfill.latency"
    event, payload = emitted[0]
    assert event == "backfill.latency"
    assert payload["symbol"] == symbol
    assert payload["elapsed_ms"] == feed._stats[symbol]["backfill_last_elapsed_ms"]
    assert "stats" in payload and isinstance(payload["stats"], dict)


@pytest.mark.asyncio
async def test_datafeed_consumer_registra_skip_reason() -> None:
    symbol = "BTCUSDT"

    async def handler(candle: dict) -> None:
        candle["_df_skip_reason"] = "warmup"
        candle["_df_skip_details"] = {"buffer_len": 5}

    feed = DataFeed("1m", handler_timeout=1.0)
    feed._symbols = [symbol]
    feed._queues[symbol] = asyncio.Queue()
    feed._handler = handler
    feed._running = True

    consumer = asyncio.create_task(feed._consumer(symbol))

    candle = {"symbol": symbol, "timestamp": 1_700_000_000}
    await feed._queues[symbol].put(candle)

    for _ in range(100):
        if feed._stats[symbol]["skipped"]:
            break
        await asyncio.sleep(0.01)
    else:
        raise AssertionError("El consumer no registró el skip")

    assert feed._stats[symbol]["processed"] == 0
    assert feed._stats[symbol]["skipped"] == 1
    assert "_df_skip_reason" not in candle

    feed._running = False
    consumer.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await consumer
        
@pytest.mark.asyncio
async def test_trader_procesar_vela_puentea_al_handler() -> None:
    supervisor = DummySupervisor()
    config = DummyConfig()

    async def dummy_handler(_: TraderLite, vela: dict) -> None:
        return None

    trader = TraderLite(config, candle_handler=dummy_handler, supervisor=supervisor)

    capturados: List[dict] = []

    async def fake_invoker(vela: dict) -> None:
        capturados.append(vela)

    trader._handler_invoker = fake_invoker  # type: ignore[assignment]

    candle = {"symbol": "BTCUSDT", "timestamp": 1_700_000_000, "close": 42.0}
    assert trader._update_estado_con_candle(candle) is True
    await trader._procesar_vela(candle)

    assert capturados == [candle]


@pytest.mark.asyncio
async def test_verificar_entradas_emite_motivo_si_engine_falta() -> None:
    eventos: List[Tuple[str, Dict[str, Any]]] = []

    def on_event(evt: str, data: Dict[str, Any]) -> None:
        eventos.append((evt, data))

    config = SimpleNamespace(
        timeout_evaluar_condiciones=1,
        timeout_evaluar_condiciones_por_symbol={},
        usar_score_tecnico=False,
        min_dist_pct=0.001,
        min_dist_pct_overrides={},
        contradicciones_bloquean_entrada=False,
    )

    class TraderStub:
        def __init__(self) -> None:
            self.engine = None
            self.persistencia = None
            self.config = config

        def _puede_evaluar_entradas(self, _symbol: str) -> bool:
            return True

    df = pd.DataFrame(
        [
            {"timestamp": 1_700_000_000, "open": 10.0, "high": 11.0, "low": 9.5, "close": 10.5, "volume": 100.0}
        ]
    )

    resultado = await verificar_func(
        TraderStub(),
        "BTCUSDT",
        df,
        estado={},
        on_event=on_event,
    )

    assert resultado is None
    assert ("entry_skip", {"symbol": "BTCUSDT", "reason": "engine_missing"}) in eventos


@pytest.mark.asyncio
async def test_procesar_vela_no_falla_si_metricas_explotan(monkeypatch: pytest.MonkeyPatch) -> None:
    class ExplodingMetric:
        def labels(self, *_args: Any, **_kwargs: Any) -> "ExplodingMetric":
            raise TypeError("boom labels")

        def inc(self, *_args: Any, **_kwargs: Any) -> None:
            raise TypeError("boom inc")

        def set(self, *_args: Any, **_kwargs: Any) -> None:
            raise TypeError("boom set")

    class ExplodingHistogram(ExplodingMetric):
        def observe(self, *_args: Any, **_kwargs: Any) -> None:
            raise RuntimeError("boom observe")

    class TraderStub:
        def __init__(self) -> None:
            self.config = SimpleNamespace(trader_fastpath_enabled=False)
            self.estado = {"BTCUSDT": {}}
            self.notifications: List[Tuple[str, str]] = []

            class _Orders:
                def __init__(self) -> None:
                    self.created: List[Dict[str, Any]] = []

                def obtener(self, _symbol: str) -> None:
                    return None

                def crear(self, **payload: Any) -> None:
                    self.created.append(dict(payload))

            self.orders = _Orders()

        def is_symbol_ready(self, *_args: Any, **_kwargs: Any) -> bool:
            return True

        async def evaluar_condiciones_de_entrada(self, symbol: str, df: pd.DataFrame, _estado: Any) -> Dict[str, Any]:
            return {
                "symbol": symbol,
                "side": "long",
                "precio_entrada": float(df.iloc[-1]["close"]),
                "stop_loss": float(df.iloc[-1]["close"]) * 0.99,
                "take_profit": float(df.iloc[-1]["close"]) * 1.02,
                "score": 3.0,
            }

        def enqueue_notification(self, mensaje: str, nivel: str = "INFO") -> None:
            self.notifications.append((mensaje, nivel))

    trader = TraderStub()

    invalid_candle = {"timestamp": 1}

    candle = {
        "symbol": "BTCUSDT",
        "timestamp": 1_700_000_000,
        "open": 10.0,
        "high": 10.5,
        "low": 9.5,
        "close": 10.2,
        "volume": 15.0,
    }

    procesar_vela_mod._buffers._estados.clear()
    procesar_vela_mod._buffers._locks.clear()

    monkeypatch.setattr("core.procesar_vela.CANDLES_IGNORADAS", ExplodingMetric())
    monkeypatch.setattr("core.procesar_vela.ENTRADAS_CANDIDATAS", ExplodingMetric())
    monkeypatch.setattr("core.procesar_vela.ENTRADAS_ABIERTAS", ExplodingMetric())
    monkeypatch.setattr("core.procesar_vela.WARMUP_RESTANTE", ExplodingMetric())
    monkeypatch.setattr("core.procesar_vela.LAST_BAR_AGE", ExplodingMetric())
    monkeypatch.setattr("core.procesar_vela.HANDLER_EXCEPTIONS", ExplodingMetric())
    monkeypatch.setattr("core.procesar_vela.EVAL_LATENCY", ExplodingHistogram())
    monkeypatch.setattr("core.procesar_vela.PARSE_LATENCY", ExplodingHistogram())
    monkeypatch.setattr("core.procesar_vela.GATING_LATENCY", ExplodingHistogram())
    monkeypatch.setattr("core.procesar_vela.STRATEGY_LATENCY", ExplodingHistogram())

    await procesar_vela_handler(trader, invalid_candle)
    await procesar_vela_handler(trader, candle)

    assert trader.orders.created, "El flujo de apertura no debe interrumpirse por métricas"
