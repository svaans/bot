from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable

import pytest

from core.auditoria import AuditEvent, AuditResult
from core.gestor_ordenes import GestorOrdenes


class DummyBus:
    """Bus asíncrono que completa el ``future`` con respuestas predefinidas."""

    def __init__(self, responses: Iterable[Any] | None = None) -> None:
        self._responses = list(responses or [])
        self.published: list[tuple[str, dict[str, Any]]] = []

    async def publish(self, evento: str, payload: dict[str, Any]) -> None:
        copia = {k: v for k, v in payload.items() if k != "future"}
        self.published.append((evento, copia))
    
    async def request(
        self,
        evento: str,
        payload: dict[str, Any],
        *,
        timeout: float | None = None,
    ) -> dict[str, Any]:
        del timeout
        data = dict(payload)
        data.setdefault("ack", None)
        data.setdefault("error", None)
        self.published.append((evento, data))
        respuesta = self._responses.pop(0) if self._responses else {"ack": True}
        if isinstance(respuesta, dict):
            result = dict(respuesta)
            result["ack"] = bool(result.get("ack", True))
            result.setdefault("error", None)
            return result
        ack_value = bool(respuesta)
        return {"ack": ack_value, "error": None if ack_value else "nack"}


class PayloadDict(dict):
    """Dict especializado que omite ``symbol`` al expandirse con ``**``."""

    def keys(self):  # type: ignore[override]
        return (k for k in super().keys() if k != "symbol")

    def items(self):  # type: ignore[override]
        return ((k, v) for k, v in super().items() if k != "symbol")

    def __iter__(self):  # type: ignore[override]
        return (k for k in super().keys() if k != "symbol")


@dataclass
class DummyOrder:
    symbol: str
    precio_entrada: float | None
    cantidad: float
    direccion: str
    extras: dict[str, Any]
    cerrando: bool = False

    def to_dict(self) -> PayloadDict:
        data = PayloadDict(
            {
                "precio_entrada": self.precio_entrada,
                "cantidad": self.cantidad,
                "direccion": self.direccion,
            }
        )
        if self.extras:
            data["extras"] = self.extras
        return data


class DummyOrders:
    def __init__(self) -> None:
        self.created: list[DummyOrder] = []
        self.deleted: list[str] = []
        self.store: dict[str, DummyOrder] = {}

    def crear(
        self,
        symbol: str,
        precio: float,
        cantidad: float,
        direccion: str,
        extras: dict[str, Any] | None = None,
    ) -> DummyOrder:
        orden = DummyOrder(symbol, precio, cantidad, direccion, extras or {})
        self.store[symbol] = orden
        self.created.append(orden)
        return orden

    def obtener(self, symbol: str) -> DummyOrder | None:
        return self.store.get(symbol)

    def eliminar(self, symbol: str) -> None:
        self.store.pop(symbol, None)
        self.deleted.append(symbol)


class DummyReporter:
    def __init__(self) -> None:
        self.records: list[dict[str, Any]] = []

    def registrar_operacion(self, info: dict[str, Any]) -> None:
        record = dict(info)
        if "symbol" not in record and hasattr(info, "get"):
            symbol = info.get("symbol")
            if symbol is not None:
                record["symbol"] = symbol
        self.records.append(record)


class DummyAuditor:
    def __init__(self) -> None:
        self.records: list[dict[str, Any]] = []

    def registrar(self, **kwargs: Any) -> None:
        self.records.append(dict(kwargs))


@pytest.mark.asyncio
async def test_abrir_operacion_exitosa_emite_evento() -> None:
    bus = DummyBus([True])
    orders = DummyOrders()
    eventos: list[tuple[str, dict[str, Any]]] = []

    def registrar_evento(evt: str, data: dict[str, Any]) -> None:
        eventos.append((evt, data))

    gestor = GestorOrdenes(bus, orders, timeout_bus=5.0, on_event=registrar_evento)

    ok = await gestor.abrir_operacion("BTCUSDT", 10_000.0, 0.25, "long", extras={"source": "test"})

    assert ok is True
    assert len(orders.created) == 1
    assert orders.obtener("BTCUSDT") is orders.created[0]
    assert bus.published == [
        (
            "abrir_orden",
            {
                "symbol": "BTCUSDT",
                "precio": 10_000.0,
                "cantidad": 0.25,
                "direccion": "long",
                "ack": None,
                "error": None,
            },
        )
    ]
    assert eventos[0][0] == "orden_abierta"
    assert eventos[0][1]["symbol"] == "BTCUSDT"


@pytest.mark.asyncio
async def test_abrir_operacion_rechaza_cantidad_no_valida() -> None:
    gestor = GestorOrdenes(DummyBus(), DummyOrders(), timeout_bus=5.0)

    ok = await gestor.abrir_operacion("BTCUSDT", 10_000.0, 0.0, "long")

    assert ok is False


@pytest.mark.asyncio
async def test_abrir_operacion_rechaza_precio_no_valido() -> None:
    gestor = GestorOrdenes(DummyBus(), DummyOrders(), timeout_bus=5.0)

    ok = await gestor.abrir_operacion("BTCUSDT", 0.0, 0.5, "long")

    assert ok is False


@pytest.mark.asyncio
async def test_abrir_operacion_revierte_si_bus_rechaza() -> None:
    bus = DummyBus([False])
    orders = DummyOrders()

    gestor = GestorOrdenes(bus, orders, timeout_bus=5.0)
    ok = await gestor.abrir_operacion("BTCUSDT", 10_000.0, 0.25, "short")

    assert ok is False
    assert orders.deleted == ["BTCUSDT"]


@pytest.mark.asyncio
async def test_cerrar_operacion_exitosa_registra_reporte() -> None:
    bus = DummyBus([True])
    orders = DummyOrders()
    reporter = DummyReporter()
    eventos: list[tuple[str, dict[str, Any]]] = []

    def registrar_evento(evt: str, data: dict[str, Any]) -> None:
        eventos.append((evt, data))

    orden = orders.crear("BTCUSDT", 9_500.0, 0.3, "long", {})
    gestor = GestorOrdenes(
        bus,
        orders,
        reporter=reporter,
        timeout_bus=5.0,
        on_event=registrar_evento,
    )

    auditoria: list[dict[str, Any]] = []

    async def auditar_stub(**kwargs: Any) -> None:
        auditoria.append(dict(kwargs))

    gestor._auditar = auditar_stub  # type: ignore[attr-defined]

    ok = await gestor.cerrar_operacion("BTCUSDT", 10_200.0, "take_profit")

    assert ok is True
    assert bus.published[-1][0] == "cerrar_orden"
    assert reporter.records[0]["symbol"] == "BTCUSDT"
    assert auditoria[0]["evento"] == AuditEvent.EXIT
    assert auditoria[0]["resultado"] == AuditResult.SUCCESS
    assert eventos[-1][0] == "orden_cerrada"
    assert orden.cerrando is True


@pytest.mark.asyncio
async def test_cerrar_operacion_falla_sin_orden() -> None:
    gestor = GestorOrdenes(DummyBus(), DummyOrders(), timeout_bus=5.0)

    ok = await gestor.cerrar_operacion("ETHUSDT", 1_800.0, "stop_loss")

    assert ok is False


@pytest.mark.asyncio
async def test_cerrar_operacion_falla_sin_precio_disponible() -> None:
    bus = DummyBus()
    orders = DummyOrders()
    orders.store["BTCUSDT"] = DummyOrder("BTCUSDT", None, 0.5, "long", {})

    gestor = GestorOrdenes(bus, orders, timeout_bus=5.0)

    async def precio_stub(*args: Any, **kwargs: Any) -> float | None:
        return None

    gestor._ticker_precio = precio_stub  # type: ignore[attr-defined]

    async def auditar_stub(**kwargs: Any) -> None:
        raise AssertionError("_auditar no debería ser llamado")

    gestor._auditar = auditar_stub  # type: ignore[attr-defined]

    ok = await gestor.cerrar_operacion("BTCUSDT", None, "manual")

    assert ok is False
    assert bus.published == []


@pytest.mark.asyncio
async def test_cerrar_operacion_falla_si_bus_rechaza() -> None:
    bus = DummyBus([False])
    orders = DummyOrders()
    reporter = DummyReporter()
    orders.crear("BTCUSDT", 9_900.0, 0.4, "long", {})

    gestor = GestorOrdenes(bus, orders, reporter=reporter, timeout_bus=5.0)
    ok = await gestor.cerrar_operacion("BTCUSDT", 9_800.0, "stop_loss")

    assert ok is False
    assert not reporter.records


@pytest.mark.asyncio
async def test_cerrar_operacion_no_duplica_si_en_proceso() -> None:
    bus = DummyBus()
    orders = DummyOrders()
    orden = DummyOrder("BTCUSDT", 9_900.0, 0.4, "long", {})
    orden.cerrando = True
    orders.store["BTCUSDT"] = orden

    gestor = GestorOrdenes(bus, orders, timeout_bus=5.0)
    ok = await gestor.cerrar_operacion("BTCUSDT", 9_950.0, "manual")

    assert ok is False
    assert bus.published == []


@pytest.mark.asyncio
async def test_cerrar_parcial_exitosa_registra_eventos() -> None:
    bus = DummyBus([True])
    orders = DummyOrders()
    reporter = DummyReporter()
    eventos: list[tuple[str, dict[str, Any]]] = []

    def registrar_evento(evt: str, data: dict[str, Any]) -> None:
        eventos.append((evt, data))

    orders.crear("BTCUSDT", 9_700.0, 0.5, "long", {})
    gestor = GestorOrdenes(
        bus,
        orders,
        reporter=reporter,
        timeout_bus=5.0,
        on_event=registrar_evento,
    )

    auditoria: list[dict[str, Any]] = []

    async def auditar_stub(**kwargs: Any) -> None:
        auditoria.append(dict(kwargs))

    gestor._auditar = auditar_stub  # type: ignore[attr-defined]

    ok = await gestor.cerrar_parcial("BTCUSDT", 0.2, 9_950.0, "rebalance")

    assert ok is True
    assert bus.published[-1][0] == "cerrar_parcial"
    assert reporter.records[0]["cantidad_cerrada"] == pytest.approx(0.2)
    assert auditoria[0]["evento"] == AuditEvent.PARTIAL_EXIT
    assert auditoria[0]["resultado"] == AuditResult.PARTIAL
    assert eventos[-1][0] == "orden_cierre_parcial"


@pytest.mark.asyncio
async def test_cerrar_parcial_rechaza_cantidad_invalida() -> None:
    bus = DummyBus()
    orders = DummyOrders()
    orders.crear("BTCUSDT", 9_700.0, 0.5, "long", {})

    gestor = GestorOrdenes(bus, orders, timeout_bus=5.0)
    ok = await gestor.cerrar_parcial("BTCUSDT", 0.0, 9_800.0, "rebalance")

    assert ok is False
    assert bus.published == []


@pytest.mark.asyncio
async def test_cerrar_parcial_falla_si_bus_rechaza() -> None:
    bus = DummyBus([False])
    orders = DummyOrders()
    reporter = DummyReporter()
    orders.crear("BTCUSDT", 9_700.0, 0.5, "long", {})

    gestor = GestorOrdenes(bus, orders, reporter=reporter, timeout_bus=5.0)
    ok = await gestor.cerrar_parcial("BTCUSDT", 0.1, 9_650.0, "stop_loss")

    assert ok is False
    assert not reporter.records
    assert bus.published[-1][0] == "cerrar_parcial"