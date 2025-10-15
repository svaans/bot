"""Pruebas de unidad para la lógica de entrada del trader."""

from __future__ import annotations

import threading
import types
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable

import pandas as pd
import pytest

from core.utils.feature_flags import reset_flag_cache
from core.trader_modular import Trader, TraderComponentFactories

from .factories import DummyConfig, DummySupervisor

UTC = timezone.utc


def _build_trader(**config_overrides: Any) -> Trader:
    """Crea una instancia de :class:`Trader` lista para tests unitarios."""

    config = DummyConfig(**config_overrides)

    async def _handler(_: dict) -> None:
        return None

    return Trader(config, candle_handler=_handler, supervisor=DummySupervisor())


def _df_from_timestamps(timestamps: Iterable[int], tf: str = "1m") -> pd.DataFrame:
    """Construye un DataFrame con columna ``timestamp`` y atributo ``tf``."""

    df = pd.DataFrame(
        [{"timestamp": ts, "close": float(idx)} for idx, ts in enumerate(timestamps, start=1)]
    )
    df.attrs["tf"] = tf
    return df


def _set_last_candle(
    trader: Trader,
    *,
    open_ts: int,
    close_ts: int,
    event_ts: int | None = None,
    symbol: str = "BTCUSDT",
) -> None:
    """Inicializa el estado interno con una vela sintética."""

    estado = trader.estado[symbol]
    estado.buffer.clear()
    estado.buffer.append(
        {
            "symbol": symbol,
            "timestamp": close_ts,
            "open": 1.0,
            "high": 1.0,
            "low": 1.0,
            "close": 1.0,
            "volume": 1.0,
            "open_time": open_ts,
            "close_time": close_ts,
            "event_time": event_ts if event_ts is not None else close_ts,
        }
    )


class _StubOrders:
    def __init__(self, existing: Any | None = None, abriendo: Iterable[str] | None = None) -> None:
        self._existing = existing
        self.abriendo = set(abriendo or [])

    def obtener(self, symbol: str) -> Any | None:
        return self._existing


class _StubCapitalManager:
    def __init__(self, *, libre: bool = True, asignado: bool = True) -> None:
        self._libre = libre
        self._asignado = asignado

    def hay_capital_libre(self) -> bool:
        return self._libre

    def tiene_capital(self, symbol: str) -> bool:
        return self._asignado


class _StubRiskManager:
    def __init__(self, allow: bool) -> None:
        self.allow = allow
        self.calls: list[tuple[str, dict[str, float], float]] = []
        self.correlaciones = {"BTCUSDT": {}}

    def permite_entrada(
        self,
        symbol: str,
        correlaciones: dict[str, float],
        diversidad_minima: float,
    ) -> bool:
        self.calls.append((symbol, correlaciones, diversidad_minima))
        return self.allow


def test_puede_evaluar_entradas_false_when_stop_event_set() -> None:
    trader = _build_trader()
    trader._stop_event.set()

    assert trader._puede_evaluar_entradas("BTCUSDT") is False


def test_puede_evaluar_entradas_false_with_existing_order() -> None:
    trader = _build_trader()
    trader.orders = _StubOrders(existing={"id": 1})

    assert trader._puede_evaluar_entradas("BTCUSDT") is False


def test_puede_evaluar_entradas_false_when_opening_order() -> None:
    trader = _build_trader()
    trader.orders = _StubOrders(existing=None, abriendo={"BTCUSDT"})

    assert trader._puede_evaluar_entradas("BTCUSDT") is False


def test_puede_evaluar_entradas_respects_datetime_cooldown() -> None:
    trader = _build_trader()
    cooldown_until = datetime.now(UTC) + timedelta(minutes=5)
    trader._entrada_cooldowns["BTCUSDT"] = cooldown_until

    assert trader._puede_evaluar_entradas("BTCUSDT") is False


def test_puede_evaluar_entradas_respects_timestamp_cooldown() -> None:
    trader = _build_trader()
    trader._entrada_cooldowns["BTCUSDT"] = datetime.now(UTC).timestamp() + 60

    assert trader._puede_evaluar_entradas("BTCUSDT") is False


def test_puede_evaluar_entradas_checks_capital_libre() -> None:
    trader = _build_trader()
    trader.capital_manager = _StubCapitalManager(libre=False, asignado=True)

    assert trader._puede_evaluar_entradas("BTCUSDT") is False


def test_puede_evaluar_entradas_checks_capital_asignado() -> None:
    trader = _build_trader()
    trader.capital_manager = _StubCapitalManager(libre=True, asignado=False)

    assert trader._puede_evaluar_entradas("BTCUSDT") is False


def test_puede_evaluar_entradas_consulta_gestor_riesgo() -> None:
    trader = _build_trader(diversidad_minima=0.2)
    risk = _StubRiskManager(allow=False)
    trader.risk = risk

    assert trader._puede_evaluar_entradas("BTCUSDT") is False
    assert risk.calls == [("BTCUSDT", {}, 0.2)]


def test_puede_evaluar_entradas_true_when_all_conditions_clear() -> None:
    trader = _build_trader()
    trader.capital_manager = _StubCapitalManager(libre=True, asignado=True)
    trader.risk = _StubRiskManager(allow=True)

    assert trader._puede_evaluar_entradas("BTCUSDT") is True


@pytest.mark.asyncio
async def test_evaluar_condiciones_returns_none_when_strategies_disabled() -> None:
    trader = _build_trader()
    trader.estrategias_habilitadas = False
    df = _df_from_timestamps([100])

    resultado = await trader.evaluar_condiciones_de_entrada(
        "BTCUSDT", df, trader.estado["BTCUSDT"]
    )

    assert resultado is None


@pytest.mark.asyncio
async def test_evaluar_condiciones_returns_none_on_invalid_dataframe() -> None:
    trader = _build_trader()
    trader.estrategias_habilitadas = True
    empty_df = pd.DataFrame()

    resultado = await trader.evaluar_condiciones_de_entrada(
        "BTCUSDT", empty_df, trader.estado["BTCUSDT"]
    )

    assert resultado is None


@pytest.mark.asyncio
async def test_evaluar_condiciones_respects_min_bars(monkeypatch: pytest.MonkeyPatch) -> None:
    trader = _build_trader()
    trader.estrategias_habilitadas = True
    monkeypatch.setattr(trader, "_resolve_min_bars_requirement", lambda: 5)
    df = _df_from_timestamps([100, 200, 300])

    resultado = await trader.evaluar_condiciones_de_entrada(
        "BTCUSDT", df, trader.estado["BTCUSDT"]
    )

    assert resultado is None


@pytest.mark.asyncio
async def test_evaluar_condiciones_waits_for_bar_close(monkeypatch: pytest.MonkeyPatch) -> None:
    trader = _build_trader()
    trader.estrategias_habilitadas = True
    monkeypatch.setattr(trader, "_resolve_min_bars_requirement", lambda: 1)
    df = _df_from_timestamps([120])
    _set_last_candle(trader, open_ts=60, close_ts=120, event_ts=115)

    resultado = await trader.evaluar_condiciones_de_entrada(
        "BTCUSDT", df, trader.estado["BTCUSDT"]
    )

    assert resultado is None
    assert trader._last_eval_skip_details is not None
    assert trader._last_eval_skip_details.get("elapsed_secs") == pytest.approx(55.0)
    assert trader._last_eval_skip_details.get("remaining_secs") == pytest.approx(5.0)


@pytest.mark.asyncio
async def test_evaluar_condiciones_detects_future_bar(monkeypatch: pytest.MonkeyPatch) -> None:
    trader = _build_trader()
    trader.estrategias_habilitadas = True
    monkeypatch.setattr(trader, "_resolve_min_bars_requirement", lambda: 1)
    df = _df_from_timestamps([240])
    _set_last_candle(trader, open_ts=180, close_ts=240, event_ts=170)

    resultado = await trader.evaluar_condiciones_de_entrada(
        "BTCUSDT", df, trader.estado["BTCUSDT"]
    )

    assert resultado is None
    assert trader._last_eval_skip_reason == "bar_in_future"
    assert trader._last_eval_skip_details is not None
    assert trader._last_eval_skip_details.get("elapsed_secs") == pytest.approx(-10.0)


@pytest.mark.asyncio
async def test_evaluar_condiciones_detects_out_of_range_bar(monkeypatch: pytest.MonkeyPatch) -> None:
    trader = _build_trader()
    trader.estrategias_habilitadas = True
    monkeypatch.setattr(trader, "_resolve_min_bars_requirement", lambda: 1)
    df = _df_from_timestamps([1260])
    _set_last_candle(trader, open_ts=1200, close_ts=1260, event_ts=300)

    resultado = await trader.evaluar_condiciones_de_entrada(
        "BTCUSDT", df, trader.estado["BTCUSDT"]
    )

    assert resultado is None
    assert trader._last_eval_skip_reason == "bar_ts_out_of_range"
    assert trader._last_eval_skip_details is not None
    assert trader._last_eval_skip_details.get("elapsed_secs") == pytest.approx(-900.0)


@pytest.mark.asyncio
async def test_evaluar_condiciones_skips_duplicate_bars(monkeypatch: pytest.MonkeyPatch) -> None:
    trader = _build_trader()
    trader.estrategias_habilitadas = True
    monkeypatch.setattr(trader, "_resolve_min_bars_requirement", lambda: 1)
    df = _df_from_timestamps([100])
    trader._last_evaluated_bar[("BTCUSDT", "1m")] = 100

    resultado = await trader.evaluar_condiciones_de_entrada(
        "BTCUSDT", df, trader.estado["BTCUSDT"]
    )

    assert resultado is None


@pytest.mark.asyncio
async def test_evaluar_condiciones_returns_pipeline_result(monkeypatch: pytest.MonkeyPatch) -> None:
    trader = _build_trader()
    trader.estrategias_habilitadas = True
    monkeypatch.setattr(trader, "_resolve_min_bars_requirement", lambda: 1)
    monkeypatch.setattr(trader, "_should_evaluate", lambda *args, **kwargs: True)
    df = _df_from_timestamps([100, 200])

    async def _fake_verificar_entrada(
        self: Trader,
        symbol: str,
        dataframe: pd.DataFrame,
        estado: Any,
        *,
        on_event: Any | None = None,
    ) -> dict[str, Any]:
        return {"symbol": symbol, "count": len(dataframe)}

    trader.verificar_entrada = types.MethodType(_fake_verificar_entrada, trader)

    resultado = await trader.evaluar_condiciones_de_entrada(
        "BTCUSDT", df, trader.estado["BTCUSDT"]
    )

    assert resultado == {"symbol": "BTCUSDT", "count": 2}


@pytest.mark.asyncio
async def test_verificar_entrada_prefers_pipeline_function() -> None:
    trader = _build_trader()
    llamado: dict[str, Any] = {}
    df = _df_from_timestamps([100])

    async def _pipeline(
        self: Trader,
        symbol: str,
        dataframe: pd.DataFrame,
        estado: Any,
        *,
        on_event: Any | None = None,
    ) -> dict[str, Any]:
        llamado["args"] = (symbol, dataframe, estado, on_event)
        return {"pipeline": True}

    trader._verificar_entrada = _pipeline

    resultado = await trader.verificar_entrada(
        "BTCUSDT", df, trader.estado["BTCUSDT"], on_event=lambda *_: None
    )

    assert resultado == {"pipeline": True}
    assert trader._verificar_entrada_provider == "pipeline"
    assert llamado["args"][0] == "BTCUSDT"
    assert callable(llamado["args"][3])


@pytest.mark.asyncio
async def test_verificar_entrada_uses_strategy_engine_fallback() -> None:
    trader = _build_trader()
    trader._verificar_entrada = None
    df = _df_from_timestamps([100, 200])
    capturas: dict[str, Any] = {}

    class DummyEngine:
        def verificar_entrada(
            self,
            symbol: str,
            dataframe: pd.DataFrame,
            estado: Any,
            *,
            on_event: Any | None = None,
        ) -> dict[str, Any]:
            capturas["args"] = (symbol, dataframe, estado, on_event)
            return {"engine": True}

    trader.engine = DummyEngine()

    resultado = await trader.verificar_entrada("BTCUSDT", df, trader.estado["BTCUSDT"])

    assert resultado == {"engine": True}
    assert trader._verificar_entrada_provider == "engine.verificar_entrada"
    assert capturas["args"][0] == "BTCUSDT"


@pytest.mark.asyncio
async def test_evaluar_condiciones_offloads_large_buffers() -> None:
    config = DummyConfig(
        trader_eval_offload=True,
        trader_eval_offload_min_df=2,
    )

    def fake_verificar(*args: Any, **kwargs: Any) -> dict[str, Any]:
        symbol = kwargs.get("symbol")
        if symbol is None and len(args) >= 2:
            symbol = args[1]
        thread_calls.append(threading.current_thread().name)
        return {"symbol": symbol or "UNKNOWN"}

    thread_calls: list[str] = []
    factories = TraderComponentFactories(verificar_entrada=fake_verificar)

    async def handler(_: dict) -> None:
        return None

    trader = Trader(
        config,
        candle_handler=handler,
        supervisor=DummySupervisor(),
        component_factories=factories,
    )
    trader.habilitar_estrategias()

    timestamps = [60_000, 120_000, 180_000]
    df = _df_from_timestamps(timestamps)
    estado = trader.estado["BTCUSDT"]
    _set_last_candle(
        trader,
        symbol="BTCUSDT",
        open_ts=timestamps[-2],
        close_ts=timestamps[-1],
        event_ts=timestamps[-1],
    )

    main_thread = threading.current_thread().name
    resultado = await trader.evaluar_condiciones_de_entrada("BTCUSDT", df, estado)

    assert resultado == {"symbol": "BTCUSDT"}
    assert thread_calls
    assert any(name != main_thread for name in thread_calls)
    stats = trader.get_eval_offload_stats()
    assert stats == {"enabled": True, "threshold": 2, "count": 1}


def test_historial_cierres_enforces_retention(monkeypatch: pytest.MonkeyPatch) -> None:
    trader = _build_trader(
        historial_cierres_max_per_symbol=2,
        historial_cierres_ttl_min=1,
    )
    hist = trader.historial_cierres["BTCUSDT"]

    clock = {"value": 1_000_000.0}

    def fake_time() -> float:
        return clock["value"]

    monkeypatch.setattr("core.trader.trader.time.time", fake_time)

    hist["one"] = {"id": 1}
    clock["value"] += 50
    hist["two"] = {"id": 2}
    clock["value"] += 30
    hist["three"] = {"id": 3}

    keys = list(hist)
    assert "one" not in keys
    assert len(hist) == 2
    stats = trader.get_historial_cierres_stats()
    assert stats["max_por_simbolo"] == 2
    assert stats["ttl_segundos"] == 60
    assert stats["por_simbolo"]["BTCUSDT"] == 2
    assert stats["total"] == 2


def test_purge_historial_cierres_respects_symbol_limit(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TRADER_PURGE_HISTORIAL_ENABLED", "true")
    reset_flag_cache()
    trader = _build_trader(historial_cierres_max_per_symbol=5)

    hist = trader.historial_cierres["BTCUSDT"]
    hist._max_entries = 10  # type: ignore[attr-defined]
    for idx, key in enumerate(["one", "two", "three", "four"], start=1):
        hist[key] = {"id": idx}

    other = trader.historial_cierres["ETHUSDT"]
    other["keep"] = {"id": 99}

    hist._max_entries = 2  # type: ignore[attr-defined]
    trader.purge_historial_cierres(symbol="BTCUSDT")

    assert list(hist.keys()) == ["three", "four"]
    assert list(other.keys()) == ["keep"]
    reset_flag_cache()


def test_historial_cierres_prunes_idle_symbols(monkeypatch: pytest.MonkeyPatch) -> None:
    trader = _build_trader(
        historial_cierres_max_per_symbol=5,
        historial_cierres_ttl_min=1,
    )

    btc_hist = trader.historial_cierres["BTCUSDT"]
    eth_hist = trader.historial_cierres["ETHUSDT"]

    clock = {"time": 1_000_000.0, "monotonic": 50_000.0}

    def fake_time() -> float:
        return clock["time"]

    def fake_monotonic() -> float:
        return clock["monotonic"]

    monkeypatch.setattr("core.trader.trader.time.time", fake_time)
    monkeypatch.setattr("core.trader.trader.time.monotonic", fake_monotonic)

    btc_hist["old"] = {"id": 1}

    clock["time"] += 120
    clock["monotonic"] += 120

    eth_hist["keep"] = {"id": 2}

    btc_data = trader.historial_cierres._stores["BTCUSDT"]._data  # type: ignore[attr-defined]
    assert "old" not in btc_data
