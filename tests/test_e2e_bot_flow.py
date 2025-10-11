from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from typing import Any, Callable, Iterable, List, Sequence

import pytest

from core.data_feed.datafeed import DataFeed
from core.metrics import ENTRADAS_RECHAZADAS_V2
from core.procesar_vela import get_buffer_manager
from tests.fake_feed import mk_candle_in_range
from tests.log_sanitizer import parse_records

Symbol = str
TIMEFRAME = "5m"


def _enable_flow_logging(caplog: pytest.LogCaptureFixture, *, metrics_level: int = logging.WARNING) -> None:
    handler = caplog.handler
    for name, level in (
        ("datafeed", logging.DEBUG),
        ("trader_modular", logging.DEBUG),
        ("procesar_vela", logging.DEBUG),
        ("metrics", metrics_level),
    ):
        logger = logging.getLogger(name)
        logger.setLevel(level)
        if handler not in logger.handlers:
            logger.addHandler(handler)


async def wait_for(condition: Callable[[], bool], *, timeout: float = 1.0, interval: float = 0.01) -> None:
    """Espera asincrónicamente a que ``condition`` sea verdadera."""

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if condition():
            return
        await asyncio.sleep(interval)
    raise AssertionError("Timeout esperando a que la condición se cumpla")


@pytest.fixture(autouse=True)
def _clear_global_buffers() -> Iterable[None]:
    manager = get_buffer_manager()
    manager._estados.clear()
    manager._locks.clear()
    yield
    manager._estados.clear()
    manager._locks.clear()


def _align_to_interval(timestamp: int, interval_secs: int) -> int:
    if interval_secs <= 0:
        return timestamp
    return (timestamp // interval_secs) * interval_secs


def _to_millis(candle: dict[str, Any]) -> dict[str, Any]:
    for key in ("open_time", "close_time", "timestamp", "event_time"):
        if key in candle and candle[key] is not None:
            candle[key] = int(candle[key]) * 1000
    return candle


def _build_series(
    base_ts: int,
    count: int,
    *,
    symbol: Symbol = "BTCUSDT",
    interval_secs: int = 300,
) -> List[dict[str, Any]]:
    aligned_base = _align_to_interval(base_ts, interval_secs)
    candles: List[dict[str, Any]] = []
    for i in range(count):
        raw = mk_candle_in_range(
            aligned_base + i * interval_secs,
            interval_secs=interval_secs,
            symbol=symbol,
            timeframe=TIMEFRAME,
        ).to_dict()
        candles.append(_to_millis(raw))
    return candles


def _find_events(parsed: List[dict[str, Any]], logger: str, message: str) -> List[dict[str, Any]]:
    events: List[dict[str, Any]] = []
    seen: set[tuple[Any, ...]] = set()
    for entry in parsed:
        if entry.get("logger") != logger or entry.get("message") != message:
            continue
        key = (
            entry.get("timestamp"),
            entry.get("bar_open_ts"),
            entry.get("event_ts"),
            entry.get("reason"),
            entry.get("buffer_len"),
        )
        if key in seen:
            continue
        seen.add(key)
        events.append(entry)
    return events


def _format_skip_entries(entries: Iterable[dict[str, Any]]) -> str:
    compact = []
    for entry in entries:
        compact.append(
            {
                "reason": entry.get("reason"),
                "symbol": entry.get("symbol"),
                "timeframe": entry.get("timeframe"),
                "buffer_len": entry.get("buffer_len"),
                "min_needed": entry.get("min_needed"),
                "details": entry.get("details"),
            }
        )
    return str(compact)


class BotFlowHarness:
    """Orquesta un DataFeed real y un Trader para los tests de integración."""

    def __init__(self, trader: Any, feed: DataFeed, symbol: Symbol) -> None:
        self.trader = trader
        self.feed = feed
        self.symbol = symbol.upper()
        self._procesar_calls = 0

    async def _handler(self, candle: dict) -> None:
        should_process = self.trader._update_estado_con_candle(candle)
        if should_process:
            self._procesar_calls += 1
            await self.trader._procesar_vela(candle)

    async def run(self, candles: Sequence[dict[str, Any]], *, timeout: float = 1.0) -> int:
        symbol = self.symbol
        queue = asyncio.Queue(maxsize=self.feed.queue_max)
        self.feed._queues[symbol] = queue
        self.feed._symbols = [symbol]
        self.feed._handler = self._handler
        self.feed._running = True
        consumer_task = asyncio.create_task(self.feed._consumer(symbol))
        try:
            for candle in candles:
                payload = dict(candle)
                payload.setdefault("symbol", symbol)
                await self.feed._handle_candle(symbol, payload)
            await asyncio.wait_for(queue.join(), timeout)
        finally:
            self.feed._running = False
            consumer_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await consumer_task
            self.feed._queues.pop(symbol, None)
            self.feed._handler = None
            self.feed._symbols = []
        return self._procesar_calls


@pytest.mark.asyncio(mode="strict")
async def test_flow_happy_path_emits_attempts_and_metrics(trader_factory, caplog) -> None:
    _enable_flow_logging(caplog)
    caplog.clear()

    trader = trader_factory(symbols=["BTCUSDT"])
    feed = DataFeed(TIMEFRAME)
    harness = BotFlowHarness(trader, feed, "BTCUSDT")

    base_ts = int(time.time()) - 900
    candles = _build_series(base_ts, 3)

    await harness.run(candles)
    await wait_for(lambda: len(trader.orders.created) == len(candles))

    records = caplog.get_records("call")
    parsed = parse_records(records)
    attempts = _find_events(parsed, "trader_modular", "procesar_vela.attempt")
    skips = _find_events(parsed, "datafeed", "consumer.skip")

    assert len(attempts) == len(candles), (
        f"Esperaba {len(candles)} intentos de procesar_vela, obtuve {len(attempts)}. "
        f"Skips detectados: {_format_skip_entries(skips)}"
    )
    for entry in attempts:
        assert entry.get("event") == "procesar_vela.attempt"
        assert entry.get("reason") == "go_evaluate"
        for field in (
            "symbol",
            "timeframe",
            "bar_open_ts",
            "event_ts",
            "elapsed_secs",
            "interval_secs",
            "buffer_len",
            "min_needed",
        ):
            assert field in entry, f"Campo {field} ausente en log de intento: {entry}"
            assert entry[field] is not None, f"Campo {field} sin valor en log de intento: {entry}"

    assert not skips, f"No debería haber skips en escenario feliz: {_format_skip_entries(skips)}"
    assert len(trader.orders.created) == len(candles)
    assert feed._stats[harness.symbol]["processed"] == len(candles)


@pytest.mark.asyncio(mode="strict")
async def test_flow_bar_in_future_skips_with_reason(trader_factory, caplog) -> None:
    _enable_flow_logging(caplog)
    caplog.clear()

    trader = trader_factory(symbols=["BTCUSDT"])
    feed = DataFeed(TIMEFRAME)
    harness = BotFlowHarness(trader, feed, "BTCUSDT")

    future_base = _align_to_interval(int(time.time()) + 1200, 300)
    candle = _build_series(future_base, 1)[0]
    open_ms = candle["open_time"]
    # Simula evento recibido antes de la apertura esperada.
    candle["event_time"] = open_ms - 60_000
    candle["close_time"] = open_ms - 60_000
    await harness.run([candle])

    records = caplog.get_records("call")
    parsed = parse_records(records)
    attempts = _find_events(parsed, "trader_modular", "procesar_vela.attempt")
    skips = _find_events(parsed, "datafeed", "consumer.skip")

    assert not attempts, f"No debería evaluarse vela futura. Intentos: {attempts}"
    assert skips, "Se esperaba un log consumer.skip con razón bar_in_future"
    last = skips[-1]
    assert last.get("reason") == "bar_in_future", f"Skip incorrecto: {_format_skip_entries(skips)}"
    assert last.get("event") == "consumer.skip"
    assert last.get("details"), "Los skips deben incluir detalles diagnósticos"


@pytest.mark.asyncio(mode="strict")
async def test_flow_out_of_range_skips_with_reason(trader_factory, caplog) -> None:
    _enable_flow_logging(caplog)
    caplog.clear()

    trader = trader_factory(symbols=["BTCUSDT"])
    feed = DataFeed(TIMEFRAME)
    harness = BotFlowHarness(trader, feed, "BTCUSDT")

    past_base = _align_to_interval(int(time.time()) - 7200, 300)
    candle = _build_series(past_base, 1)[0]
    open_ms = candle["open_time"]
    candle["event_time"] = open_ms - 600_000
    candle["close_time"] = open_ms - 600_000
    await harness.run([candle])

    parsed = parse_records(caplog.get_records("call"))
    attempts = _find_events(parsed, "trader_modular", "procesar_vela.attempt")
    skips = _find_events(parsed, "datafeed", "consumer.skip")

    assert not attempts, f"No se esperaba evaluación para vela fuera de rango: {attempts}"
    assert skips, "Debe registrarse consumer.skip"
    reason = skips[-1].get("reason")
    assert reason == "bar_ts_out_of_range", f"Razón incorrecta ({reason}). Logs: {_format_skip_entries(skips)}"
    assert skips[-1].get("event") == "consumer.skip"


@pytest.mark.asyncio(mode="strict")
async def test_flow_warmup_not_met_reports_reason(trader_factory, caplog) -> None:
    _enable_flow_logging(caplog)
    caplog.clear()

    trader = trader_factory(symbols=["BTCUSDT"])
    trader.min_bars = 4
    trader.config.min_bars = 4
    trader.config.min_buffer_candles = 4
    feed = DataFeed(TIMEFRAME)
    harness = BotFlowHarness(trader, feed, "BTCUSDT")

    base_ts = int(time.time()) - 900
    candles = _build_series(base_ts, 3)
    await harness.run(candles)

    parsed = parse_records(caplog.get_records("call"))
    attempts = _find_events(parsed, "trader_modular", "procesar_vela.attempt")
    skips = _find_events(parsed, "datafeed", "consumer.skip")

    assert not attempts, f"No debe evaluarse durante warmup. Intentos: {attempts}"
    assert skips, "Warmup debería registrarse como skip"
    last = skips[-1]
    assert last.get("reason") == "warmup", f"Razón inesperada: {_format_skip_entries(skips)}"
    assert last.get("buffer_len") == len(candles)
    assert last.get("min_needed") == 4


@pytest.mark.asyncio(mode="strict")
async def test_flow_pipeline_missing_propagates_reason(trader_factory, caplog) -> None:
    _enable_flow_logging(caplog)
    caplog.clear()

    trader = trader_factory(symbols=["BTCUSDT"])
    trader._verificar_entrada = None
    trader._verificar_entrada_provider = None
    feed = DataFeed(TIMEFRAME)
    harness = BotFlowHarness(trader, feed, "BTCUSDT")

    base_ts = int(time.time()) - 600
    candles = _build_series(base_ts, 1)
    await harness.run(candles)

    parsed = parse_records(caplog.get_records("call"))
    attempts = _find_events(parsed, "trader_modular", "procesar_vela.attempt")
    skips = _find_events(parsed, "datafeed", "consumer.skip")

    assert len(attempts) == 1, f"Debe existir un intento de evaluación: {attempts}"
    skip_reasons = [entry.get("reason") for entry in skips]
    assert "pipeline_missing" in skip_reasons, f"pipeline_missing ausente en logs: {_format_skip_entries(skips)}"
    pipeline_log = next(entry for entry in skips if entry.get("reason") == "pipeline_missing")
    details = pipeline_log.get("details", {})
    assert details.get("provider") is None
    assert details.get("pipeline_present") is False
    assert details.get("pipeline_callable") is False
    assert details.get("pipeline_required") is False
    if "pipeline_error_type" in details:
        assert isinstance(details["pipeline_error_type"], str)
        assert isinstance(details.get("pipeline_error_message"), str)


@pytest.mark.asyncio(mode="strict")
async def test_flow_pipeline_missing_exposes_component_error(trader_factory, caplog) -> None:
    _enable_flow_logging(caplog)
    caplog.clear()

    trader = trader_factory(symbols=["BTCUSDT"])
    trader._verificar_entrada = None
    trader._verificar_entrada_provider = None
    trader._component_errors["verificar_entrada"] = RuntimeError("pipeline exploded")
    feed = DataFeed(TIMEFRAME)
    harness = BotFlowHarness(trader, feed, "BTCUSDT")

    base_ts = int(time.time()) - 600
    candles = _build_series(base_ts, 1)
    await harness.run(candles)

    parsed = parse_records(caplog.get_records("call"))
    skips = _find_events(parsed, "datafeed", "consumer.skip")

    pipeline_log = next(entry for entry in skips if entry.get("reason") == "pipeline_missing")
    details = pipeline_log.get("details", {})
    assert details.get("pipeline_error_type") == "RuntimeError"
    assert "pipeline exploded" in details.get("pipeline_error_message", "")


@pytest.mark.asyncio(mode="strict")
async def test_flow_fastpath_guard_emits_metric(trader_factory, caplog) -> None:
    _enable_flow_logging(caplog)
    caplog.clear()

    trader = trader_factory(symbols=["BTCUSDT"], fastpath=True)
    feed = DataFeed(TIMEFRAME)
    harness = BotFlowHarness(trader, feed, "BTCUSDT")

    metric = ENTRADAS_RECHAZADAS_V2.labels(
        symbol="BTCUSDT",
        timeframe=TIMEFRAME,
        reason="fastpath_skip_entries",
    )
    baseline = metric._value

    base_ts = int(time.time()) - 900
    candles = _build_series(base_ts, 3)
    await harness.run(candles)

    parsed = parse_records(caplog.get_records("call"))
    skips = _find_events(parsed, "datafeed", "consumer.skip")
    fastpath_logs = [entry for entry in skips if entry.get("reason") == "fastpath_skip_entries"]

    assert fastpath_logs, f"No se registró fastpath_skip_entries: {_format_skip_entries(skips)}"
    await wait_for(lambda: metric._value >= baseline + len(fastpath_logs))
    for entry in fastpath_logs:
        assert entry.get("buffer_len") >= trader.config.trader_fastpath_threshold


@pytest.mark.asyncio(mode="strict")
async def test_flow_backpressure_does_not_block_attempt(trader_factory, caplog) -> None:
    _enable_flow_logging(caplog)
    caplog.clear()

    trader = trader_factory(symbols=["BTCUSDT"])
    feed = DataFeed(TIMEFRAME, queue_max=1, queue_policy="block")
    harness = BotFlowHarness(trader, feed, "BTCUSDT")

    base_ts = int(time.time()) - 900
    candles = _build_series(base_ts, 3)
    await harness.run(candles, timeout=2.0)

    parsed = parse_records(caplog.get_records("call"))
    attempts = _find_events(parsed, "trader_modular", "procesar_vela.attempt")

    assert len(attempts) == len(candles), (
        f"Backpressure no debe impedir evaluaciones. Intentos: {attempts}"
    )
    assert feed._stats[harness.symbol]["received"] == len(candles)


@pytest.mark.asyncio(mode="strict")
async def test_flow_reconnect_continues_after_reset(trader_factory, caplog) -> None:
    _enable_flow_logging(caplog)
    caplog.clear()

    trader = trader_factory(symbols=["BTCUSDT"])
    feed = DataFeed(TIMEFRAME, max_reconnect_attempts=2, max_reconnect_time=30)

    base_ts = int(time.time()) - 900
    first_batch = _build_series(base_ts, 2)
    harness = BotFlowHarness(trader, feed, "BTCUSDT")
    await harness.run(first_batch)

    parsed_first = parse_records(caplog.get_records("call"))
    attempts_first = _find_events(parsed_first, "trader_modular", "procesar_vela.attempt")
    assert len(attempts_first) == len(first_batch)

    feed._register_reconnect_attempt("BTCUSDT", "error")
    assert feed._reconnect_attempts["BTCUSDT"] == 1
    feed._reset_reconnect_tracking("BTCUSDT")
    assert "BTCUSDT" not in feed._reconnect_attempts

    caplog.clear()
    second_batch = _build_series(base_ts + 2 * 300, 2)
    harness = BotFlowHarness(trader, feed, "BTCUSDT")
    await harness.run(second_batch)

    parsed_second = parse_records(caplog.get_records("call"))
    attempts_second = _find_events(parsed_second, "trader_modular", "procesar_vela.attempt")
    assert len(attempts_second) == len(second_batch), (
        f"Tras reconectar deben seguir los intentos. Logs: {attempts_second}"
  )
