from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Iterable, Sequence

import pytest

import core.procesar_vela as procesar_vela_mod
from tests.fake_feed import mk_candle_in_range
from tests.log_sanitizer import has_event, parse_records
from tests.spies import spy_trader


def _enable_caplog(caplog: pytest.LogCaptureFixture, *, metrics_level: int = logging.WARNING) -> None:
    handler = caplog.handler
    for name, level in (
        ("datafeed", logging.DEBUG),
        ("procesar_vela", logging.DEBUG),
        ("trader_modular", logging.DEBUG),
        ("metrics", metrics_level),
    ):
        logger = logging.getLogger(name)
        logger.setLevel(level)
        if handler not in logger.handlers:
            logger.addHandler(handler)


@pytest.fixture(autouse=True)
def _reset_buffers() -> Iterable[None]:
    procesar_vela_mod._buffers._estados.clear()
    procesar_vela_mod._buffers._locks.clear()
    yield
    procesar_vela_mod._buffers._estados.clear()
    procesar_vela_mod._buffers._locks.clear()


def _build_handler(trader: Any):
    async def _handler(candle: dict) -> None:
        if trader._update_estado_con_candle(candle):
            await trader._procesar_vela(candle)

    return _handler


async def _run_consumer(
    feed: Any,
    symbol: str,
    candles: Sequence[dict],
    handler: Any,
    *,
    timeout: float = 1.0,
) -> None:
    queue = asyncio.Queue()
    feed._queues[symbol] = queue
    feed._symbols = [symbol]
    feed._handler = handler
    feed._running = True

    try:
        async with asyncio.TaskGroup() as tg:
            consumer_task = tg.create_task(feed._consumer(symbol))
            for candle in candles:
                await queue.put(dict(candle))
            await asyncio.wait_for(queue.join(), timeout=timeout)
            feed._running = False
            consumer_task.cancel()
    except asyncio.CancelledError:
        pass


@pytest.mark.asyncio(mode="strict")
async def test_consume_y_procesa_una_vela_cerrada(
    trader_factory,
    datafeed_instance,
    caplog,
    monkeypatch,
) -> None:
    trader = trader_factory(symbols=["BTCUSDT"])
    handler = _build_handler(trader)
    spy = spy_trader(monkeypatch, trader)

    _enable_caplog(caplog)
    caplog.clear()

    base_ts = int(time.time()) - 600
    candle = mk_candle_in_range(base_ts, symbol="BTCUSDT", timeframe="5m").to_dict()
    await _run_consumer(datafeed_instance, "BTCUSDT", [candle], handler)

    records = caplog.get_records("call")
    parsed = parse_records(records)
    enter_idx = next(
        (i for i, entry in enumerate(parsed) if entry["logger"] == "datafeed" and entry["message"] == "consumer.enter"),
        None,
    )
    exit_idx = next(
        (i for i, entry in enumerate(parsed) if entry["logger"] == "datafeed" and entry["message"] == "consumer.exit"),
        None,
    )
    assert enter_idx is not None, f"No se encontró consumer.enter. Logs: {parsed}"
    assert exit_idx is not None, f"No se encontró consumer.exit. Logs: {parsed}"
    assert enter_idx < exit_idx, "consumer.exit debe ocurrir después de consumer.enter"

    assert has_event(records, "datafeed", "consumer.enter", symbol="BTCUSDT")
    assert has_event(records, "datafeed", "consumer.exit", symbol="BTCUSDT")
    assert not has_event(records, "datafeed", "consumer.skip", symbol="BTCUSDT")
    assert has_event(records, "procesar_vela", "pre-eval", symbol="BTCUSDT")
    assert has_event(records, "trader_modular", "Entrada candidata generada")
    assert spy.calls >= 1, "Trader._procesar_vela debe invocarse al menos una vez"


@pytest.mark.asyncio(mode="strict")
async def test_registra_hitos_de_evaluacion(
    trader_factory,
    datafeed_instance,
    caplog,
) -> None:
    trader = trader_factory(symbols=["BTCUSDT"])
    handler = _build_handler(trader)

    _enable_caplog(caplog)
    caplog.clear()

    base_ts = int(time.time()) - 600
    candle = mk_candle_in_range(base_ts, symbol="BTCUSDT", timeframe="5m").to_dict()
    await _run_consumer(datafeed_instance, "BTCUSDT", [candle], handler)

    records = caplog.get_records("call")
    parsed = parse_records(records)

    def _find_event(message: str) -> int | None:
        for idx, entry in enumerate(parsed):
            if entry.get("logger") != "procesar_vela":
                continue
            if entry.get("message") != message:
                continue
            if entry.get("symbol") != "BTCUSDT":
                continue
            return idx
        return None

    pre_idx = _find_event("pre-eval")
    go_idx = _find_event("go_evaluate")
    entrada_idx = _find_event("entrada_verificada")
    salida_idx = _find_event("salida_verificada")

    assert pre_idx is not None, f"Falta pre-eval en logs: {parsed}"
    assert go_idx is not None, f"Falta go_evaluate en logs: {parsed}"

    final_idx = entrada_idx if entrada_idx is not None else salida_idx
    assert final_idx is not None, f"Falta entrada/salida_verificada en logs: {parsed}"

    assert pre_idx < go_idx < final_idx, (
        "La secuencia de logs debe ser pre-eval → go_evaluate → entrada/salida",
        parsed,
    )
    assert not has_event(records, "datafeed", "consumer.skip", symbol="BTCUSDT")


@pytest.mark.asyncio(mode="strict")
async def test_no_marca_bar_in_future_con_timestamp_valido(
    trader_factory,
    datafeed_instance,
    caplog,
) -> None:
    trader = trader_factory(symbols=["BTCUSDT"])
    handler = _build_handler(trader)

    _enable_caplog(caplog)
    caplog.clear()

    base_ts = int(time.time()) - 600
    candle = mk_candle_in_range(base_ts, symbol="BTCUSDT", timeframe="5m").to_dict()
    await _run_consumer(datafeed_instance, "BTCUSDT", [candle], handler)

    records = caplog.get_records("call")
    assert not has_event(
        records, "trader_modular", "update_estado.skip", reason="bar_in_future"
    ), "No debe descartarse por bar_in_future"
    assert not has_event(
        records, "trader_modular", "update_estado.skip", reason="bar_ts_out_of_range"
    ), "No debe descartarse por bar_ts_out_of_range"


@pytest.mark.asyncio(mode="strict")
async def test_fastpath_registra_skip_reason_visible(
    trader_factory,
    datafeed_instance,
    caplog,
) -> None:
    trader = trader_factory(symbols=["BTCUSDT"], fastpath=True)
    handler = _build_handler(trader)

    _enable_caplog(caplog)
    caplog.clear()

    base_ts = int(time.time()) - 900
    candles = [
        mk_candle_in_range(base_ts + i * 300, symbol="BTCUSDT", timeframe="5m").to_dict()
        for i in range(3)
    ]
    await _run_consumer(datafeed_instance, "BTCUSDT", candles, handler)

    records = caplog.get_records("call")
    assert has_event(records, "datafeed", "consumer.skip", reason="fastpath_skip_entries"), (
        "No se registró consumer.skip con motivo fastpath_skip_entries",
        parse_records(records),
    )


@pytest.mark.asyncio(mode="strict")
async def test_no_pipeline_missing_con_pipeline_configurado(
    trader_factory,
    datafeed_instance,
    caplog,
) -> None:
    trader = trader_factory(symbols=["BTCUSDT"])
    handler = _build_handler(trader)

    _enable_caplog(caplog)
    caplog.clear()

    base_ts = int(time.time()) - 600
    candle = mk_candle_in_range(base_ts, symbol="BTCUSDT", timeframe="5m").to_dict()
    await _run_consumer(datafeed_instance, "BTCUSDT", [candle], handler)

    records = caplog.get_records("call")
    assert not has_event(records, "datafeed", "consumer.skip", reason="pipeline_missing"), (
        "Se detectó pipeline_missing pese a tener pipeline configurado",
        parse_records(records),
    )


class _StrictMetric:
    def __init__(self, name: str, allowed: Sequence[str]) -> None:
        self.name = name
        self.allowed = set(allowed)

    def labels(self, **labels: Any) -> "_StrictMetric":
        invalid = set(labels) - self.allowed
        if invalid:
            raise TypeError(f"{self.name}: invalid labels {sorted(invalid)}")
        return self

    def inc(self, *args: Any, **kwargs: Any) -> None:  # pragma: no cover - noop
        return None

    def set(self, *args: Any, **kwargs: Any) -> None:  # pragma: no cover - noop
        return None

    def observe(self, *args: Any, **kwargs: Any) -> None:  # pragma: no cover - noop
        return None


@pytest.mark.asyncio(mode="strict")
async def test_metricas_no_rompen_por_labels(
    trader_factory,
    datafeed_instance,
    caplog,
    monkeypatch,
) -> None:
    trader = trader_factory(symbols=["BTCUSDT"])
    handler = _build_handler(trader)

    replacements = {
        "ENTRADAS_RECHAZADAS_V2": _StrictMetric("ENTRADAS_RECHAZADAS_V2", ("symbol", "timeframe", "reason")),
        "ENTRADAS_CANDIDATAS": _StrictMetric("ENTRADAS_CANDIDATAS", ("symbol", "side")),
        "ENTRADAS_ABIERTAS": _StrictMetric("ENTRADAS_ABIERTAS", ("symbol", "side")),
        "CANDLES_IGNORADAS": _StrictMetric("CANDLES_IGNORADAS", ("reason",)),
        "WARMUP_RESTANTE": _StrictMetric("WARMUP_RESTANTE", ("symbol", "timeframe")),
        "LAST_BAR_AGE": _StrictMetric("LAST_BAR_AGE", ("symbol", "timeframe")),
        "BUFFER_SIZE_V2": _StrictMetric("BUFFER_SIZE_V2", ("timeframe",)),
    }

    for name, metric in replacements.items():
        monkeypatch.setattr(procesar_vela_mod, name, metric)

    _enable_caplog(caplog)
    caplog.set_level(logging.WARNING, logger="metrics")
    caplog.clear()

    base_ts = int(time.time()) - 600
    candle = mk_candle_in_range(base_ts, symbol="BTCUSDT", timeframe="5m").to_dict()
    await _run_consumer(datafeed_instance, "BTCUSDT", [candle], handler)

    records = caplog.get_records("call")
    assert not has_event(records, "metrics", "metrics_label_mismatch"), "Las labels deben ser válidas"


@pytest.mark.parametrize(
    "symbol",
    ["BTC/EUR", "ETH/EUR", "SOL/EUR", "ADA/EUR", "BNB/EUR"],
)
@pytest.mark.asyncio(mode="strict")
async def test_procesa_multisimbolo_sin_skips(
    symbol: str,
    trader_factory,
    datafeed_instance,
    caplog,
    monkeypatch,
) -> None:
    trader = trader_factory(symbols=[symbol])
    handler = _build_handler(trader)
    spy = spy_trader(monkeypatch, trader)

    _enable_caplog(caplog)
    caplog.clear()

    base_ts = int(time.time()) - 600
    candle = mk_candle_in_range(base_ts, symbol=symbol, timeframe="5m").to_dict()
    await _run_consumer(datafeed_instance, symbol.upper(), [candle], handler)

    records = caplog.get_records("call")
    assert records, f"No se capturaron logs para {symbol}"
    assert not has_event(records, "datafeed", "consumer.skip", symbol=symbol.upper())
    assert has_event(records, "procesar_vela", "pre-eval", symbol=symbol.upper())
    assert spy.calls >= 1
