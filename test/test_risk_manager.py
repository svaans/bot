import pytest
from unittest.mock import Mock

from core.risk.risk_manager import RiskManager


def test_riesgo_superado(monkeypatch):
    called = {}

    def fake_check(umbral, capital):
        called['args'] = (umbral, capital)
        return True

    monkeypatch.setattr("core.risk_manager._riesgo_superado", fake_check)
    rm = RiskManager(0.2)
    assert rm.riesgo_superado(1000) is True
    assert called['args'] == (0.2, 1000)


def test_registrar_perdida(monkeypatch):
    mock_update = Mock()
    monkeypatch.setattr("core.risk_manager.actualizar_perdida", mock_update)
    rm = RiskManager(0.3)
    rm.registrar_perdida("BTC/EUR", -0.1)
    mock_update.assert_called_once_with("BTC/EUR", -0.1)