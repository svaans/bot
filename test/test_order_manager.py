import pytest
from core.orders.order_manager import OrderManager


@pytest.mark.asyncio
async def test_open_close_paper():
    om = OrderManager(modo_real=False)
    await om.abrir_async('BTC/EUR', 100, 90, 120, {'s1': 1}, 'bullish')
    assert 'BTC/EUR' in om.ordenes
    await om.cerrar_async('BTC/EUR', 110, 'take')
    assert 'BTC/EUR' not in om.ordenes


@pytest.mark.asyncio
async def test_open_close_real(monkeypatch):
    pytest.skip('Prueba de modo real deshabilitada por falta de entorno')
