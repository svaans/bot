from __future__ import annotations

from datetime import datetime, timedelta, timezone

UTC = timezone.utc
from types import SimpleNamespace
from typing import Any, Generator
from unittest.mock import ANY

import pytest

import core.risk.risk_manager as risk_module
from core.risk.risk_manager import RiskManager
from core.capital_manager import CapitalManager
from core.utils.feature_flags import reset_flag_cache


class DummyCapitalManager:
    def __init__(self, capital: dict[str, float], *, libre: bool = True) -> None:
        self.capital_por_simbolo = capital
        self._libre = libre
        self.fraccion_kelly = 0.1
        self.aplicados: list[float] = []

    def hay_capital_libre(self) -> bool:
        return self._libre

    def exposure_disponible(self, symbol: str | None = None) -> float:
        if symbol:
            return float(self.capital_por_simbolo.get(symbol, 0.0))
        return float(sum(self.capital_por_simbolo.values()))
    
    def aplicar_multiplicador_kelly(self, factor: float) -> float:
        self.aplicados.append(factor)
        self.fraccion_kelly = 0.1 * factor
        return self.fraccion_kelly


@pytest.fixture(autouse=True)
def patch_dependencies(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_actualizar(symbol: str, perdida: float) -> None:
        return None

    monkeypatch.setattr("core.risk.risk_manager.actualizar_perdida", fake_actualizar)

    def fake_alerta(_: float, __: float) -> tuple[bool, float]:
        return (False, 0.0)

    monkeypatch.setattr("core.risk.risk_manager.evaluar_alerta_capital", fake_alerta)

    dummy_reporter = SimpleNamespace(ultimas_operaciones={})
    monkeypatch.setattr("core.risk.risk_manager.reporter_diario", dummy_reporter)
    dummy_registro = SimpleNamespace(registrar=lambda *args, **kwargs: None)
    monkeypatch.setattr("core.risk.risk_manager.registro_metrico", dummy_registro)


@pytest.fixture(autouse=True)
def _reset_flags() -> Generator[None, None, None]:
    try:
        yield
    finally:
        reset_flag_cache()


@pytest.fixture(autouse=True)
def reset_gauges() -> Generator[None, None, None]:
    risk_module.RIESGO_CONSUMIDO_GAUGE.set(0.0)
    risk_module.COOLDOWN_ACTIVO_GAUGE.set(0.0)
    risk_module.CAPITAL_ALERTA_GAUGE.set(0.0)
    try:
        yield
    finally:
        risk_module.RIESGO_CONSUMIDO_GAUGE.set(0.0)
        risk_module.COOLDOWN_ACTIVO_GAUGE.set(0.0)
        risk_module.CAPITAL_ALERTA_GAUGE.set(0.0)


def test_registrar_perdida_activa_cooldown() -> None:
    capital = DummyCapitalManager({"BTCUSDT": 100.0})
    manager = RiskManager(0.05, capital_manager=capital, cooldown_pct=0.1, cooldown_duracion=60)

    manager.registrar_perdida("BTCUSDT", -20.0)

    assert pytest.approx(manager.riesgo_diario, rel=1e-9) == 20.0
    assert manager.cooldown_activo is True

def test_registrar_perdida_publica_evento_cooldown() -> None:
    class DummyBus:
        def __init__(self) -> None:
            self.events: list[tuple[str, Any]] = []

        def subscribe(self, *_: Any, **__: Any) -> None:
            return None

        def emit(self, event_type: str, data: Any | None = None) -> None:
            self.events.append((event_type, data))

    bus = DummyBus()
    capital = DummyCapitalManager({"BTCUSDT": 100.0})
    manager = RiskManager(0.05, bus=bus, capital_manager=capital, cooldown_pct=0.1, cooldown_duracion=30)

    manager.registrar_perdida("BTCUSDT", -20.0)

    assert bus.events
    evento, payload = bus.events[0]
    assert evento == "risk.cooldown_activated"
    assert payload["symbol"] == "BTCUSDT"


def test_alerta_capital_se_activa_y_se_libera(monkeypatch: pytest.MonkeyPatch) -> None:
    class DummyBus:
        def __init__(self) -> None:
            self.events: list[tuple[str, Any | None]] = []

        def subscribe(self, *_: Any, **__: Any) -> None:
            return None

        def emit(self, event_type: str, data: Any | None = None) -> None:
            self.events.append((event_type, data))

    bus = DummyBus()
    capital = DummyCapitalManager({"BTCUSDT": 100.0})
    manager = RiskManager(
        0.05,
        bus=bus,
        capital_manager=capital,
        alerta_capital_pct=0.8,
    )

    def alerta_activa(_: float, __: float) -> tuple[bool, float]:
        return (True, 0.9)

    monkeypatch.setattr(risk_module, "evaluar_alerta_capital", alerta_activa)

    manager.registrar_perdida("BTCUSDT", -20.0)

    assert manager.alerta_capital_activa is True
    assert pytest.approx(manager.ratio_alerta_capital, rel=1e-9) == 0.9
    assert bus.events[-1][0] == "risk.capital_alert"
    assert pytest.approx(risk_module.CAPITAL_ALERTA_GAUGE._value, rel=1e-9) == 1.0

    def alerta_inactiva(_: float, __: float) -> tuple[bool, float]:
        return (False, 0.2)

    monkeypatch.setattr(risk_module, "evaluar_alerta_capital", alerta_inactiva)

    manager._evaluar_alerta_capital(100.0)

    assert manager.alerta_capital_activa is False
    assert pytest.approx(risk_module.CAPITAL_ALERTA_GAUGE._value, rel=1e-9) == 0.0
    assert any(evento == "risk.capital_alert_cleared" for evento, _ in bus.events)


def test_registrar_perdida_actualiza_metricas_y_resetea_dia(monkeypatch: pytest.MonkeyPatch) -> None:
    capital = DummyCapitalManager({"BTCUSDT": 100.0})
    manager = RiskManager(0.05, capital_manager=capital)

    llamadas: list[tuple[tuple[Any, ...], dict[str, Any]]] = []
    dummy_registro = SimpleNamespace(
        registrar=lambda *args, **kwargs: llamadas.append((args, kwargs))
    )
    monkeypatch.setattr("core.risk.risk_manager.registro_metrico", dummy_registro)

    real_datetime = risk_module.datetime
    primera_fecha = real_datetime(2024, 1, 1, 12, 0, tzinfo=UTC)

    class FixedDateTime(real_datetime):
        current = primera_fecha

        @classmethod
        def now(cls, tz=None):  # type: ignore[override]
            if tz is None:
                return cls.current.replace(tzinfo=None)
            return cls.current.astimezone(tz)

    monkeypatch.setattr(risk_module, "datetime", FixedDateTime)

    manager.registrar_perdida("BTCUSDT", -10.0)

    assert pytest.approx(manager.riesgo_diario, rel=1e-9) == 10.0
    assert pytest.approx(risk_module.RIESGO_CONSUMIDO_GAUGE._value, rel=1e-9) == 10.0
    assert llamadas and llamadas[0][0][0] == "risk_drawdown"

    FixedDateTime.current = primera_fecha + timedelta(days=1)

    manager.registrar_perdida("BTCUSDT", -5.0)

    assert pytest.approx(manager.riesgo_diario, rel=1e-9) == 5.0
    assert pytest.approx(risk_module.RIESGO_CONSUMIDO_GAUGE._value, rel=1e-9) == 5.0
    assert len(llamadas) == 2


def test_permite_entrada_verifica_condiciones() -> None:
    capital = DummyCapitalManager({"BTCUSDT": 100.0}, libre=False)
    manager = RiskManager(0.05, capital_manager=capital)
    manager._cooldown_fin = datetime.now(UTC) + timedelta(seconds=10)

    assert manager.permite_entrada("ETHUSDT", {}, 0.5) is False

    manager._cooldown_fin = None
    assert manager.permite_entrada("ETHUSDT", {}, 0.5) is False

    capital._libre = True
    manager.abrir_posicion("BTCUSDT")
    manager.abrir_posicion("ETHUSDT")
    correlaciones = {"BTCUSDT": 0.9}
    assert manager.permite_entrada("ETHUSDT", correlaciones, 0.5) is False

    correlaciones = {"BTCUSDT": 0.1}
    assert manager.permite_entrada("ETHUSDT", correlaciones, 0.5) is True

def test_correlaciones_expiran(monkeypatch: pytest.MonkeyPatch) -> None:
    capital = DummyCapitalManager({"BTCUSDT": 100.0, "ETHUSDT": 100.0})
    manager = RiskManager(0.05, capital_manager=capital, correlacion_ttl=1)
    manager.abrir_posicion("BTCUSDT")
    manager.abrir_posicion("ETHUSDT")
    manager.registrar_correlaciones("BTCUSDT", {"ETHUSDT": 0.8})

    assert pytest.approx(manager.correlacion_media("BTCUSDT", {}), rel=1e-9) == 0.8

    real_datetime = risk_module.datetime

    class FutureDateTime(datetime):
        @classmethod
        def now(cls, tz=None):  # type: ignore[override]
            return real_datetime.now(tz) + timedelta(seconds=5)

    monkeypatch.setattr(risk_module, "datetime", FutureDateTime)

    manager._limpiar_correlaciones_expiradas()
    assert manager.correlacion_media("BTCUSDT", {}) == 0.0
    assert not manager.correlaciones


def test_correlacion_media_con_valores_extremos() -> None:
    capital = DummyCapitalManager({"BTCUSDT": 100.0, "ETHUSDT": 100.0, "SOLUSDT": 100.0})
    manager = RiskManager(0.05, capital_manager=capital)
    manager.abrir_posicion("BTCUSDT")
    manager.abrir_posicion("ETHUSDT")
    manager.abrir_posicion("SOLUSDT")

    manager.registrar_correlaciones("BTCUSDT", {"ETHUSDT": 0.9})

    media = manager.correlacion_media(
        "ETHUSDT", {"BTCUSDT": -0.9, "SOLUSDT": -0.8}
    )

    assert pytest.approx(media, rel=1e-9) == pytest.approx((0.9 + 0.8) / 2, rel=1e-9)


def test_ajustar_umbral_aplica_reglas() -> None:
    manager = RiskManager(0.04)

    manager.ajustar_umbral({"ganancia_semana": 0.1})
    assert manager.umbral > 0.04

    manager.ajustar_umbral({"drawdown": -0.1})
    assert manager.umbral <= 0.5

    manager.ajustar_umbral(
        {
            "winrate": 0.7,
            "capital_actual": 120.0,
            "capital_inicial": 100.0,
        }
    )
    assert manager.umbral <= 0.06

    manager.ajustar_umbral({"volatilidad_market": 3.0, "volatilidad_media": 1.0})
    manager.ajustar_umbral({"exposicion_actual": 0.6})
    manager.ajustar_umbral({"correlacion_media": 0.9})
    assert manager.umbral >= 0.01


def test_factor_volatilidad_reduce_exceso() -> None:
    manager = RiskManager(0.05)
    assert manager.factor_volatilidad(1.0, 1.0) == 1.0
    factor = manager.factor_volatilidad(5.0, 1.0, umbral=2.0)
    assert 0.25 <= factor <= 1.0


def test_multiplicador_kelly_media_suavizada(monkeypatch: pytest.MonkeyPatch) -> None:
    dummy_reporter = SimpleNamespace(
        ultimas_operaciones={
            "BTC": [{"retorno_total": 0.05}, {"retorno_total": 0.1}],
            "ETH": [{"retorno_total": -0.02}],
        }
    )
    monkeypatch.setattr("core.risk.risk_manager.reporter_diario", dummy_reporter)
    capital = DummyCapitalManager({"BTC": 100.0, "ETH": 100.0})
    manager = RiskManager(0.05, capital_manager=capital)

    factor1 = manager.multiplicador_kelly(n_trades=3)
    assert 0.5 <= factor1 <= 1.5

    dummy_reporter.ultimas_operaciones["BTC"].append({"retorno_total": 0.2})
    factor2 = manager.multiplicador_kelly(n_trades=3)
    assert factor1 <= factor2 <= 1.5
    assert capital.aplicados  # Se aplicó al menos un factor
    assert pytest.approx(capital.aplicados[-1], rel=1e-9) == factor2


def test_capital_guard_exposure(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("RISK_CAPITAL_MANAGER_ENABLED", "true")
    reset_flag_cache()

    class ExposureCapital:
        def __init__(self) -> None:
            self.capital_por_simbolo = {"BTCUSDT": 0.0}

        def hay_capital_libre(self) -> bool:
            return True

        def exposure_disponible(self, symbol=None):  # type: ignore[override]
            if symbol is None or symbol == "BTCUSDT":
                return self.capital_por_simbolo.get("BTCUSDT", 0.0)
            return 0.0

    capital = ExposureCapital()
    manager = RiskManager(0.05, capital_manager=capital)

    assert manager.permite_entrada("BTCUSDT", {}, 0.5) is False

    capital.capital_por_simbolo["BTCUSDT"] = 50.0
    assert manager.permite_entrada("BTCUSDT", {}, 0.5) is True


def test_sincronizar_exposure_actualiza_capital() -> None:
    config = SimpleNamespace(
        symbols=["BTCUSDT"],
        risk_capital_total=0.0,
        risk_capital_default_per_symbol=0.0,
        risk_capital_per_symbol={"BTCUSDT": 400.0},
        min_order_eur=10.0,
        risk_kelly_base=0.1,
    )
    capital = CapitalManager(config)
    manager = RiskManager(0.05, capital_manager=capital)

    manager.sincronizar_exposure("BTCUSDT", 120.0)
    assert capital.exposure_disponible("BTCUSDT") == pytest.approx(280.0)

    manager.sincronizar_exposure("BTCUSDT", 0.0)
    asignado = capital.exposure_asignada("BTCUSDT")
    assert capital.exposure_disponible("BTCUSDT") == pytest.approx(asignado)


@pytest.mark.asyncio
async def test_kill_switch_cierra_posiciones_y_notifica() -> None:
    class DummyBus:
        def __init__(self) -> None:
            self.events: list[tuple[str, Any]] = []

        def subscribe(self, *_: Any, **__: Any) -> None:
            return None

        async def publish(self, event_type: str, data: Any) -> None:
            self.events.append((event_type, data))

    class DummyOrder:
        def __init__(self, precio: float) -> None:
            self.precio_entrada = precio

    class DummyOrderManager:
        def __init__(self) -> None:
            self.ordenes = {"BTCUSDT": DummyOrder(10.0)}
            self.closed: list[tuple[str, float, str]] = []

        async def cerrar_async(self, symbol: str, precio: float, motivo: str) -> None:
            self.closed.append((symbol, precio, motivo))

    bus = DummyBus()
    manager = RiskManager(0.05, bus=bus)

    orders = DummyOrderManager()
    triggered = await manager.kill_switch(orders, -0.1, -0.05, 3, 2)

    assert triggered is True
    assert orders.closed == [("BTCUSDT", 10.0, "Kill Switch")]
    assert bus.events and bus.events[0][0] == "notify"


def test_cooldown_emite_alerta_y_se_libera(monkeypatch: pytest.MonkeyPatch) -> None:
    class DummyBus:
        def __init__(self) -> None:
            self.events: list[tuple[str, Any]] = []

        def subscribe(self, *_: Any, **__: Any) -> None:
            return None

        def emit(self, event_type: str, data: Any | None = None) -> None:
            self.events.append((event_type, data))

        def start(self) -> None:
            return None

    capital = DummyCapitalManager({"BTCUSDT": 100.0})
    bus = DummyBus()
    manager = RiskManager(0.05, bus=bus, capital_manager=capital, cooldown_pct=0.1, cooldown_duracion=120)

    real_datetime = risk_module.datetime
    base = real_datetime(2024, 1, 1, 12, 0, tzinfo=UTC)

    class ControlledDateTime(real_datetime):
        current = base

        @classmethod
        def now(cls, tz=None):  # type: ignore[override]
            if tz is None:
                return cls.current.replace(tzinfo=None)
            return cls.current.astimezone(tz)

        @classmethod
        def advance(cls, **kwargs: Any) -> None:
            cls.current = cls.current + timedelta(**kwargs)

    monkeypatch.setattr(risk_module, "datetime", ControlledDateTime)

    manager.registrar_perdida("BTCUSDT", -20.0)

    assert bus.events == [("risk.cooldown_activated", ANY)]
    assert manager.cooldown_activo is True
    assert pytest.approx(risk_module.COOLDOWN_ACTIVO_GAUGE._value, rel=1e-9) == 1.0

    manager.registrar_perdida("BTCUSDT", -1.0)
    assert len(bus.events) == 1
    assert pytest.approx(risk_module.COOLDOWN_ACTIVO_GAUGE._value, rel=1e-9) == 1.0

    ControlledDateTime.advance(seconds=180)

    assert manager.cooldown_activo is False
    assert pytest.approx(risk_module.COOLDOWN_ACTIVO_GAUGE._value, rel=1e-9) == 0.0
