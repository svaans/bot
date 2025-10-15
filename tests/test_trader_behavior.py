from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Any, Callable

import pandas as pd
import pytest

from core.trader_modular import Trader, TraderLite
from core.funding_rate import FundingResult

from .factories import DummyConfig, DummySupervisor


UTC = timezone.utc


@pytest.fixture
def trader_lite(monkeypatch: pytest.MonkeyPatch) -> TraderLite:
    """Instancia un ``TraderLite`` listo para pruebas unitarias."""

    supervisor = DummySupervisor()
    config = DummyConfig()

    async def handler(_: dict) -> None:
        return None

    trader = TraderLite(config, candle_handler=handler, supervisor=supervisor)

    # Forzar atributos opcionales a None para evitar dependencias externas.
    monkeypatch.setattr(trader, "orders", None, raising=False)
    monkeypatch.setattr(trader, "risk", None, raising=False)
    monkeypatch.setattr(trader, "capital_manager", None, raising=False)
    monkeypatch.setattr(trader, "_entrada_cooldowns", {}, raising=False)
    monkeypatch.setattr(trader, "estrategias_habilitadas", False, raising=False)

    return trader


def test_resolve_data_feed_kwargs_prefers_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    """Los valores inválidos en config deben caer a los definidos en entorno."""

    supervisor = DummySupervisor()
    config = DummyConfig(
        handler_timeout="invalid",
        inactivity_intervals="bad",
        df_queue_default_limit="oops",
        df_queue_policy=None,
        monitor_interval="bad",
    )

    monkeypatch.setenv("DF_HANDLER_TIMEOUT_SEC", "4.5")
    monkeypatch.setenv("DF_INACTIVITY_INTERVALS", "21")
    monkeypatch.setenv("DF_QUEUE_MAX", "4321")
    monkeypatch.setenv("DF_QUEUE_POLICY", "DROP_NEWEST")
    monkeypatch.setenv("DF_MONITOR_INTERVAL", "3.75")

    async def handler(_: dict) -> None:
        return None

    trader = TraderLite(config, candle_handler=handler, supervisor=supervisor)

    assert trader.feed.handler_timeout == pytest.approx(4.5)
    assert trader.feed.inactivity_intervals == 21
    assert trader.feed.queue_max == 4321
    assert trader.feed.queue_policy == "drop_newest"
    assert trader.feed.monitor_interval == pytest.approx(3.75)


def test_resolve_backpressure_defaults_true(monkeypatch: pytest.MonkeyPatch) -> None:
    supervisor = DummySupervisor()
    config = DummyConfig(df_backpressure=None)

    monkeypatch.delenv("DF_BACKPRESSURE", raising=False)

    async def handler(_: dict) -> None:
        return None

    trader = TraderLite(config, candle_handler=handler, supervisor=supervisor)

    assert trader.feed.backpressure is True


def test_create_spread_guard_returns_none_when_constructor_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FailingGuard:
        def __init__(self, *args: Any, **kwargs: Any) -> None:  # pragma: no cover - stub
            raise RuntimeError("boom")

    supervisor = DummySupervisor()
    config = DummyConfig(
        max_spread_ratio=0.0,
        spread_dynamic=False,
    )

    async def handler(_: dict) -> None:
        return None

    trader = TraderLite(config, candle_handler=handler, supervisor=supervisor)

    trader.config.max_spread_ratio = 0.001
    trader.config.spread_dynamic = True
    trader.config.spread_guard_window = 25
    trader.config.spread_guard_hysteresis = 0.1
    trader.config.spread_guard_max_limit = 0.003

    trader._SpreadGuard = FailingGuard

    guard = TraderLite._create_spread_guard(trader)

    assert guard is None


@pytest.mark.asyncio
async def test_precargar_historico_invoca_feed(stub_data_feed: list[Any], trader_lite: TraderLite) -> None:
    await Trader._precargar_historico(trader_lite, velas=150)

    feed = stub_data_feed[0]
    assert feed.precargar_calls == [
        {"symbols": ("BTCUSDT",), "cliente": None, "minimo": 150}
    ]


def test_ajustar_capital_diario_actualiza_fecha(trader_lite: TraderLite) -> None:
    nueva_fecha = datetime(2024, 1, 15, tzinfo=UTC).date()
    Trader.ajustar_capital_diario(trader_lite, fecha=nueva_fecha)
    assert trader_lite.fecha_actual == nueva_fecha


def test_solicitar_parada_setea_evento(trader_lite: TraderLite) -> None:
    assert trader_lite._stop_event.is_set() is False
    Trader.solicitar_parada(trader_lite)
    assert trader_lite._stop_event.is_set() is True


def test_puede_evaluar_entradas_rechaza_por_stop(trader_lite: TraderLite) -> None:
    trader_lite._stop_event.set()
    resultado = Trader._puede_evaluar_entradas(trader_lite, "BTCUSDT")
    assert resultado is False


def test_puede_evaluar_entradas_rechaza_por_orden_existente(trader_lite: TraderLite) -> None:
    class DummyOrders:
        def __init__(self) -> None:
            self.abriendo: set[str] = set()

        def obtener(self, symbol: str) -> object:
            return object()

    trader_lite.orders = DummyOrders()
    resultado = Trader._puede_evaluar_entradas(trader_lite, "BTCUSDT")
    assert resultado is False


def test_puede_evaluar_entradas_rechaza_por_apertura_en_curso(trader_lite: TraderLite) -> None:
    class DummyOrders:
        def __init__(self) -> None:
            self.abriendo = {"BTCUSDT"}

        def obtener(self, symbol: str) -> None:
            return None

    trader_lite.orders = DummyOrders()
    resultado = Trader._puede_evaluar_entradas(trader_lite, "BTCUSDT")
    assert resultado is False


def test_puede_evaluar_entradas_rechaza_por_cooldown(trader_lite: TraderLite) -> None:
    future = datetime.now(UTC) + timedelta(minutes=5)
    trader_lite._entrada_cooldowns = {"BTCUSDT": future}
    resultado = Trader._puede_evaluar_entradas(trader_lite, "BTCUSDT")
    assert resultado is False


def test_puede_evaluar_entradas_rechaza_por_capital(trader_lite: TraderLite) -> None:
    class DummyCapital:
        def hay_capital_libre(self) -> bool:
            return False

        def tiene_capital(self, symbol: str) -> bool:
            return False

    trader_lite.capital_manager = DummyCapital()
    resultado = Trader._puede_evaluar_entradas(trader_lite, "BTCUSDT")
    assert resultado is False


def test_puede_evaluar_entradas_rechaza_por_riesgo(trader_lite: TraderLite) -> None:
    class DummyRisk:
        correlaciones = {"BTCUSDT": {"ETHUSDT": 0.8}}

        def permite_entrada(self, symbol: str, correlaciones: dict, diversidad: float) -> bool:
            assert correlaciones == {"ETHUSDT": 0.8}
            assert diversidad == 0.0
            return False

    trader_lite.risk = DummyRisk()
    resultado = Trader._puede_evaluar_entradas(trader_lite, "BTCUSDT")
    assert resultado is False


def test_trader_real_mode_inicia_sincronizacion_de_ordenes() -> None:
    """El arranque debe disparar la sincronización inicial incluso en modo real."""

    class StubOrderManager:
        def __init__(self, modo_real: bool, bus: Any | None = None) -> None:
            self.modo_real = modo_real
            self.start_sync_calls = 0
            self.called_without_loop = 0

        def start_sync(self, intervalo: int | None = None) -> None:
            try:
                asyncio.get_running_loop()
            except RuntimeError:
                self.called_without_loop += 1
                return
            self.start_sync_calls += 1

    config = DummyConfig(modo_real=True)
    supervisor = DummySupervisor()

    async def handler(_: dict) -> None:
        return None

    trader = Trader(config, candle_handler=handler, supervisor=supervisor)

    ordenes_stub = StubOrderManager(modo_real=True)
    ordenes_stub.start_sync()
    trader.orders = ordenes_stub

    async def arrancar_y_detener() -> None:
        trader.start()
        await asyncio.sleep(0)
        await trader.stop()

    asyncio.run(arrancar_y_detener())

    assert ordenes_stub.called_without_loop == 1
    assert ordenes_stub.start_sync_calls == 1


@pytest.mark.asyncio
async def test_verificar_entrada_prefiere_pipeline(trader_lite: TraderLite) -> None:
    async def pipeline(
        _: Trader, symbol: str, df: pd.DataFrame, estado: Any, **extra: Any
    ) -> dict:
        return {"symbol": symbol, "estado": estado}

    trader_lite._verificar_entrada = pipeline  # type: ignore[attr-defined]

    resultado = await Trader.verificar_entrada(
        trader_lite,
        "BTCUSDT",
        pd.DataFrame({"close": [1, 2, 3]}),
        estado={"tendencia": "alcista"},
    )

    assert resultado == {"symbol": "BTCUSDT", "estado": {"tendencia": "alcista"}}
    assert trader_lite._verificar_entrada_provider == "pipeline"


@pytest.mark.asyncio
async def test_verificar_entrada_recurre_a_engine(trader_lite: TraderLite) -> None:
    class DummyEngine:
        async def verificar_entrada(
            self,
            symbol: str,
            df: pd.DataFrame,
            estado: Any,
            *,
            on_event: Callable[[str, dict], None] | None = None,
        ) -> dict[str, Any]:
            if on_event:
                on_event("engine", {"symbol": symbol})
            return {"symbol": symbol, "estado": estado, "provider": "engine"}

    eventos: list[tuple[str, dict]] = []

    def on_event(evt: str, data: dict) -> None:
        eventos.append((evt, data))

    trader_lite.engine = DummyEngine()
    trader_lite.on_event = on_event

    resultado = await Trader.verificar_entrada(
        trader_lite,
        "BTCUSDT",
        pd.DataFrame({"close": [1, 2]}),
        estado={"rsi": 55},
        on_event=on_event,
    )

    assert resultado == {"symbol": "BTCUSDT", "estado": {"rsi": 55}, "provider": "engine"}
    assert trader_lite._verificar_entrada_provider == "engine.verificar_entrada"


    @pytest.mark.asyncio
async def test_verificar_entrada_aplica_decorador_funding(
    trader_lite: TraderLite, monkeypatch: pytest.MonkeyPatch
) -> None:
    trader_lite.config.funding_enabled = True
    trader_lite.config.umbral_score_tecnico = 0.0
    trader_lite.config.funding_warning_threshold = 0.0001
    trader_lite.config.funding_score_penalty_enabled = True
    trader_lite.config.funding_score_penalty = 0.2

    class DummyEngine:
        async def evaluar_condiciones_de_entrada(
            self,
            symbol: str,
            df: pd.DataFrame,
            estado: Any,
            *,
            on_event: Callable[[str, dict], None] | None = None,
        ) -> dict[str, Any]:
            if on_event:
                on_event("engine", {"symbol": symbol})
            return {
                "symbol": symbol,
                "side": "long",
                "score": 1.0,
                "historial": {},
                "pesos": {},
            }

    trader_lite.engine = DummyEngine()

    funding_result = FundingResult(
        symbol_spot="BTCUSDT",
        mapped_symbol="BTCUSDT",
        segment="usdtm",
        available=True,
        rate=0.001,
        fetched_at=datetime.now(UTC),
        reason=None,
        source="test",
    )

    async def fake_cached(_trader: TraderLite, _symbol: str) -> FundingResult:
        return funding_result

    outcomes: list[str] = []

    monkeypatch.setattr(
        "core.strategies.entry.verificar_entradas._obtener_funding_cached",
        fake_cached,
    )
    monkeypatch.setattr(
        "core.strategies.entry.verificar_entradas.registrar_funding_signal_decoration",
        lambda _symbol, _side, outcome: outcomes.append(outcome),
    )

    eventos: list[tuple[str, dict]] = []

    def on_event(evt: str, data: dict) -> None:
        eventos.append((evt, data))

    df = pd.DataFrame(
        {
            "timestamp": [1000, 2000, 3000],
            "open": [1.0, 1.0, 1.0],
            "high": [1.0, 1.1, 1.2],
            "low": [0.9, 0.95, 0.98],
            "close": [1.0, 1.05, 1.1],
            "volume": [10, 11, 12],
        }
    )
    df.tf = "1m"  # type: ignore[attr-defined]

    resultado = await Trader.verificar_entrada(
        trader_lite,
        "BTCUSDT",
        df,
        estado={},
        on_event=on_event,
    )

    assert resultado is not None
    assert resultado["score"] == pytest.approx(0.8)
    funding_meta = resultado["meta"].get("funding", {})
    assert funding_meta.get("warning") is True
    assert funding_meta.get("direction") == "pay"
    assert outcomes and outcomes[-1] == "penalized"
    assert eventos[0] == ("engine", {"symbol": "BTCUSDT"})


@pytest.mark.asyncio
async def test_evaluar_condiciones_de_entrada_valida(trader_lite: TraderLite) -> None:
    trader_lite.estrategias_habilitadas = True

    async def pipeline(
        symbol: str,
        df: pd.DataFrame,
        estado: Any,
        *,
        on_event: Callable[[str, dict], None] | None = None,
    ) -> dict[str, Any]:
        if on_event:
            on_event("pipeline", {"symbol": symbol})
        return {"symbol": symbol, "estado": estado, "tf": getattr(df, "tf", None)}

    trader_lite.verificar_entrada = pipeline  # type: ignore[assignment]

    eventos: list[tuple[str, dict]] = []

    def on_event(evt: str, data: dict) -> None:
        eventos.append((evt, data))

    trader_lite.on_event = on_event

    df = pd.DataFrame({"close": [1.0, 1.1, 1.2]})
    df.tf = "1m"  # type: ignore[attr-defined]

    resultado = await Trader.evaluar_condiciones_de_entrada(
        trader_lite,
        "BTCUSDT",
        df,
        estado={"trend": "up"},
    )

    assert resultado == {"symbol": "BTCUSDT", "estado": {"trend": "up"}, "tf": "1m"}
    assert eventos[0][0] == "pipeline"


@pytest.mark.asyncio
async def test_evaluar_condiciones_de_entrada_con_df_invalido(trader_lite: TraderLite) -> None:
    trader_lite.estrategias_habilitadas = True
    trader_lite.verificar_entrada = lambda *args, **kwargs: None  # type: ignore[assignment]

    resultado = await Trader.evaluar_condiciones_de_entrada(
        trader_lite,
        "BTCUSDT",
        pd.DataFrame(),
        estado={},
    )

    assert resultado is None


@pytest.mark.asyncio
async def test_evaluar_condiciones_de_entrada_con_estrategias_desactivadas(
    trader_lite: TraderLite,
) -> None:
    trader_lite.estrategias_habilitadas = False

    resultado = await Trader.evaluar_condiciones_de_entrada(
        trader_lite,
        "BTCUSDT",
        pd.DataFrame({"close": [1, 2, 3]}),
        estado={"trend": "down"},
    )

    assert resultado is None


@pytest.mark.asyncio
async def test_evaluar_condiciones_de_entrada_propagates_skip_details(
    trader_lite: TraderLite,
) -> None:
    trader_lite.estrategias_habilitadas = True

    async def pipeline(
        symbol: str,
        df: pd.DataFrame,
        estado: Any,
        *,
        on_event: Callable[[str, dict], None] | None = None,
    ) -> None:
        if on_event:
            on_event(
                "entry_skip",
                {"symbol": symbol, "reason": "score_bajo", "score": 1.5, "umbral": 2.0},
            )
        return None

    trader_lite.verificar_entrada = pipeline  # type: ignore[assignment]

    resultado = await Trader.evaluar_condiciones_de_entrada(
        trader_lite,
        "BTCUSDT",
        pd.DataFrame({"close": [1.0, 2.0, 3.0]}),
        estado={},
    )

    assert resultado is None
    assert trader_lite._last_eval_skip_reason == "score_bajo"
    details = trader_lite._last_eval_skip_details
    assert isinstance(details, dict)
    assert details.get("score") == pytest.approx(1.5)
    assert details.get("umbral") == pytest.approx(2.0)
