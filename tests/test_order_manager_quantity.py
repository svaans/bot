import pytest
from types import SimpleNamespace

from core.event_bus import EventBus
from core.capital_manager import CapitalManager
from core.orders.order_manager import OrderManager


class _DummyCapitalManager:
    def __init__(self, cantidad: float) -> None:
        self._cantidad = cantidad
        self.calls: list[tuple[str, float, float, float | None]] = []

    async def calcular_cantidad_async(
        self,
        symbol: str,
        precio: float,
        *,
        exposicion_total: float = 0.0,
        stop_loss: float | None = None,
    ) -> tuple[float, float]:
        self.calls.append((symbol, precio, exposicion_total, stop_loss))
        return precio, self._cantidad


@pytest.mark.asyncio
async def test_crear_fallback_capital_manager() -> None:
    manager = OrderManager(modo_real=False, bus=None)
    dummy = _DummyCapitalManager(0.25)
    manager.capital_manager = dummy

    ok = await manager.crear(
        symbol="BTC/USDT",
        side="buy",
        precio=20_000.0,
        sl=19_000.0,
        tp=21_000.0,
        meta={},
    )

    assert ok is True
    assert dummy.calls == [("BTC/USDT", 20_000.0, 0.0, 19_000.0)]
    orden = manager.obtener("BTC/USDT")
    assert orden is not None
    assert orden.cantidad_abierta == pytest.approx(0.25)


@pytest.mark.asyncio
async def test_crear_fallback_event_bus() -> None:
    bus = EventBus()
    manager = OrderManager(modo_real=False, bus=bus)

    async def _resolver_cantidad(payload: dict) -> None:
        EventBus.respond(
            payload,
            ack=True,
            cantidad=0.33,
            precio=payload.get("precio", 0.0),
        )

    bus.subscribe("calcular_cantidad", _resolver_cantidad)

    ok = await manager.crear(
        symbol="ETH/USDT",
        side="buy",
        precio=1_500.0,
        sl=1_450.0,
        tp=1_650.0,
        meta={},
    )

    assert ok is True
    orden = manager.obtener("ETH/USDT")
    assert orden is not None
    assert orden.cantidad_abierta == pytest.approx(0.33)


@pytest.mark.asyncio
async def test_capital_manager_actualizado_con_fills() -> None:
    config = SimpleNamespace(
        symbols=["BTC/USDT"],
        risk_capital_total=0.0,
        risk_capital_default_per_symbol=0.0,
        risk_capital_per_symbol={"BTC/USDT": 500.0},
        min_order_eur=10.0,
        risk_kelly_base=0.1,
    )
    capital = CapitalManager(config)
    manager = OrderManager(modo_real=False, bus=None)
    manager.capital_manager = capital

    precio_entrada = 20_000.0
    cantidad = 0.01

    ok = await manager.abrir_async(
        symbol="BTC/USDT",
        precio=precio_entrada,
        sl=19_000.0,
        tp=21_000.0,
        estrategias={},
        tendencia="alcista",
        direccion="long",
        cantidad=cantidad,
        puntaje=0.0,
        umbral=0.0,
        score_tecnico=0.0,
    )

    assert ok is True
    asignado = capital.exposure_asignada("BTC/USDT")
    comprometido = precio_entrada * cantidad
    assert capital.exposure_disponible("BTC/USDT") == pytest.approx(
        asignado - comprometido, rel=1e-9
    )

    cerrado = await manager.cerrar_async("BTC/USDT", precio=20_500.0, motivo="manual")
    assert cerrado is True
    assert capital.exposure_disponible("BTC/USDT") == pytest.approx(asignado, rel=1e-9)
