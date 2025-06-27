import pytest
from unittest.mock import Mock
import asyncio

from core.orders.order_service import OrderServiceSimulado, OrderServiceReal


def test_open_close_paper():
    om = OrderServiceSimulado()
    om.abrir("BTC/EUR", 100, 90, 120, {"s1": 1}, "bullish")
    assert "BTC/EUR" in om.ordenes

    om.cerrar("BTC/EUR", 110, "take")
    assert "BTC/EUR" not in om.ordenes


def test_open_close_real(monkeypatch):
    mock_exec = Mock()
    mock_registrar = Mock()
    mock_eliminar = Mock()
    monkeypatch.setattr("core.orders.real_orders.ejecutar_orden_market", mock_exec)
    monkeypatch.setattr("core.orders.real_orders.registrar_orden", mock_registrar)
    monkeypatch.setattr("core.orders.real_orders.eliminar_orden", mock_eliminar)
    risk = Mock()
    om = OrderServiceReal(risk=risk)
    om.abrir("BTC/EUR", 100, 90, 120, {"s1": 1}, "bullish", cantidad=1)

    mock_exec.assert_called_once()
    mock_registrar.assert_called_once()
    assert "BTC/EUR" in om.ordenes

    om.cerrar("BTC/EUR", 90, "stop")
    mock_eliminar.assert_called_once_with("BTC/EUR")
    assert risk.registrar_perdida.called
    assert "BTC/EUR" not in om.ordenes


def test_abrir_async_reraises(monkeypatch):
    monkeypatch.setattr("core.orders.real_orders.ejecutar_orden_market", lambda *a, **k: 1.0)
    monkeypatch.setattr("core.orders.real_orders.registrar_orden", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("fail")))
    om = OrderServiceReal()

    with pytest.raises(RuntimeError):
        asyncio.run(om.abrir_async("AAA/USDT", 1.0, 0.8, 1.2, {"e": 1}, "bullish", cantidad=1.0))