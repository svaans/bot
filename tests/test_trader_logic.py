"""Pruebas de unidad para la lógica de entrada del trader."""

from __future__ import annotations

import types
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable

import pandas as pd
import pytest

from core.trader_modular import Trader

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
    df = _df_from_timestamps([90])
    monkeypatch.setattr("core.trader.trader.time.time", lambda: 100.0)

    resultado = await trader.evaluar_condiciones_de_entrada(
        "BTCUSDT", df, trader.estado["BTCUSDT"]
    )

    assert resultado is None


@pytest.mark.asyncio
async def test_evaluar_condiciones_detects_future_bar(monkeypatch: pytest.MonkeyPatch) -> None:
    trader = _build_trader()
    trader.estrategias_habilitadas = True
    monkeypatch.setattr(trader, "_resolve_min_bars_requirement", lambda: 1)
    df = _df_from_timestamps([200])
    monkeypatch.setattr("core.trader.trader.time.time", lambda: 100.0)

    resultado = await trader.evaluar_condiciones_de_entrada(
        "BTCUSDT", df, trader.estado["BTCUSDT"]
    )

    assert resultado is None
    assert trader._last_eval_skip_reason == "bar_in_future"
    assert trader._last_eval_skip_details is not None
    assert trader._last_eval_skip_details.get("elapsed_secs") == pytest.approx(-100.0)


@pytest.mark.asyncio
async def test_evaluar_condiciones_detects_out_of_range_bar(monkeypatch: pytest.MonkeyPatch) -> None:
    trader = _build_trader()
    trader.estrategias_habilitadas = True
    monkeypatch.setattr(trader, "_resolve_min_bars_requirement", lambda: 1)
    df = _df_from_timestamps([1000])
    monkeypatch.setattr("core.trader.trader.time.time", lambda: 100.0)

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
    monkeypatch.setattr("core.trader.trader.time.time", lambda: 1000.0)

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
    monkeypatch.setattr("core.trader.trader.time.time", lambda: 500.0)
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
