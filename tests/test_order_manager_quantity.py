import pytest

from core.event_bus import EventBus
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
