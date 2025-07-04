import pytest
from unittest.mock import Mock
from core.orders.order_manager import OrderManager


def test_open_close_paper():
    om = OrderManager(modo_real=False)
    om.abrir('BTC/EUR', 100, 90, 120, {'s1': 1}, 'bullish')
    assert 'BTC/EUR' in om.ordenes
    om.cerrar('BTC/EUR', 110, 'take')
    assert 'BTC/EUR' not in om.ordenes


def test_open_close_real(monkeypatch):
    mock_exec = Mock()
    mock_registrar = Mock()
    mock_eliminar = Mock()
    monkeypatch.setattr('core.ordenes_reales.ejecutar_orden_market', mock_exec)
    monkeypatch.setattr('core.ordenes_reales.registrar_orden', mock_registrar)
    monkeypatch.setattr('core.ordenes_reales.eliminar_orden', mock_eliminar)
    risk = Mock()
    om = OrderManager(modo_real=True, risk=risk)
    om.abrir('BTC/EUR', 100, 90, 120, {'s1': 1}, 'bullish', cantidad=1)
    mock_exec.assert_called_once()
    mock_registrar.assert_called_once()
    assert 'BTC/EUR' in om.ordenes
    om.cerrar('BTC/EUR', 90, 'stop')
    mock_eliminar.assert_called_once_with('BTC/EUR')
    assert risk.registrar_perdida.called
    assert 'BTC/EUR' not in om.ordenes
