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
async def test_historial_limit():
    om = OrderManager(modo_real=False, max_historial=2)
    for i in range(4):
        await om.abrir_async('BTC/EUR', 100 + i, 90, 120, {'s1': 1}, 'bullish')
        await om.cerrar_async('BTC/EUR', 110 + i, 'take')
    assert len(om.historial['BTC/EUR']) == 2


@pytest.mark.asyncio
async def test_agregar_parcial_actualiza_precio_piramide():
    om = OrderManager(modo_real=False)
    await om.abrir_async('BTC/EUR', 100, 90, 120, {'s1': 1}, 'bullish', cantidad=1, fracciones=2)
    orden = om.obtener('BTC/EUR')
    assert orden.precio_ultima_piramide == 100
    await om.agregar_parcial_async('BTC/EUR', 106, 0.5)
    assert orden.precio_ultima_piramide == 106
    
@pytest.mark.asyncio
async def test_open_close_real(monkeypatch):
    pytest.skip('Prueba de modo real deshabilitada por falta de entorno')
