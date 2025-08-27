import pytest

from core.risk.risk_manager import RiskManager


class DummyCapitalManager:
    """Versión mínima del capital manager para pruebas."""

    def __init__(self, capital_por_simbolo=None):
        self.capital_por_simbolo = capital_por_simbolo or {}

    def hay_capital_libre(self) -> bool:
        return True


def test_registrar_perdida_dispara_cooldown_y_registra_riesgo():
    cm = DummyCapitalManager({'BTC/USDT': 100.0})
    rm = RiskManager(umbral=0.1, capital_manager=cm, cooldown_pct=0.1, cooldown_duracion=5)
    rm.registrar_perdida('BTC/USDT', -20.0)
    assert rm.riesgo_consumido == pytest.approx(20.0)
    assert rm.cooldown_activo


def test_permite_entrada_considera_correlacion_media():
    cm = DummyCapitalManager({'BTC/USDT': 1000.0})
    rm = RiskManager(umbral=0.1, capital_manager=cm)
    rm.abrir_posicion('BTC/USDT')
    assert not rm.permite_entrada('ETH/USDT', {'BTC/USDT': 0.9}, diversidad_minima=0.5)
    assert rm.permite_entrada('ETH/USDT', {'BTC/USDT': 0.1}, diversidad_minima=0.5)