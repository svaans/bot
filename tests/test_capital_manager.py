# tests/test_capital_manager.py
"""
Tests for the :class:`core.capital_manager.CapitalManager` class.

These tests focus on the synchronous parts of ``CapitalManager``
that can be executed without connecting to Binance or requiring
external dependencies.  The asynchronous methods are deliberately
ignored because they depend on the trading infrastructure.
"""

from __future__ import annotations

import unittest
import types
import sys


def _install_stubs() -> None:
    """Register stubs for external packages referenced in capital_manager.

    CapitalManager imports a wide range of modules at import time,
    including ``dotenv``, ``colorama``, ``prometheus_client``,
    ``binance_api`` and core modules like ``core.risk``.  To test the
    synchronous behaviour in isolation we provide lightweight stubs
    with the necessary attributes so that the module can import
    successfully.  Only the attributes actually touched by
    ``CapitalManager`` are defined.
    """
    # Stub helper to register modules
    def stub(name: str) -> types.ModuleType:
        mod = sys.modules.get(name)
        if mod is None:
            mod = types.ModuleType(name)
            sys.modules[name] = mod
        return mod

    # Minimal stubs for logger
    logger_mod = stub('core.utils.logger')
    if not hasattr(logger_mod, 'configurar_logger'):
        logger_mod.configurar_logger = lambda *args, **kwargs: types.SimpleNamespace(
            info=lambda *a, **k: None,
            warning=lambda *a, **k: None,
            error=lambda *a, **k: None,
            critical=lambda *a, **k: None,
            debug=lambda *a, **k: None,
        )

    # Stub risk module with dummy MarketInfo and size_order
    risk_mod = stub('core.risk')
    if not hasattr(risk_mod, 'MarketInfo'):
        class MarketInfo:
            def __init__(self, tick_size: float, step_size: float, min_notional: float) -> None:
                self.tick_size = tick_size
                self.step_size = step_size
                self.min_notional = min_notional
        risk_mod.MarketInfo = MarketInfo
    if not hasattr(risk_mod, 'size_order'):
        def size_order(**kwargs):
            # Always return price and a fixed quantity to avoid hitting risk logic
            price = kwargs.get('price', 0.0)
            return price, 1.0
        risk_mod.size_order = size_order
    # Dummy RiskManager class with minimal attributes
    if not hasattr(risk_mod, 'RiskManager'):
        class RiskManager:
            def __init__(self, umbral: float = 0.1) -> None:
                self.umbral = umbral
                self.posiciones_abiertas: list[str] = []
        risk_mod.RiskManager = RiskManager

    # Stub other modules imported by capital_manager
    stub('config.config_manager')
    stub('binance_api.cliente').fetch_balance_async = lambda *args, **kwargs: {'total': {}}  # type: ignore
    stub('binance_api.filters').get_symbol_filters = lambda symbol, client: {}
    stub('core.contexto_externo').obtener_puntaje_contexto = lambda symbol: 0.0

    # Optional third-party stubs that might appear
    stub('dotenv').load_dotenv = lambda *args, **kwargs: None  # type: ignore
    stub('colorlog')
    stub('colorama')


class DummyConfig:
    """Simple configuration object for the CapitalManager.

    Only the attributes referenced in the constructor or methods are
    provided.  Additional attributes may be set via keyword
    arguments.
    """
    def __init__(self, **kwargs) -> None:
        self.symbols = kwargs.get('symbols', ['BTC/USDT', 'ETH/USDT'])
        self.modo_real = kwargs.get('modo_real', False)
        self.modo_capital_bajo = kwargs.get('modo_capital_bajo', False)
        self.riesgo_maximo_diario = kwargs.get('riesgo_maximo_diario', 0.1)
        self.capital_currency = kwargs.get('capital_currency', None)
        self.umbral_puntaje_macro = kwargs.get('umbral_puntaje_macro', 6)


class CapitalManagerTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        _install_stubs()
        from core.capital_manager import CapitalManager  # type: ignore
        cls.CapitalManager = CapitalManager
        # Provide dummy risk manager instance
        cls.risk = sys.modules['core.risk'].RiskManager(umbral=0.1)

    def test_detectar_divisa_principal(self) -> None:
        detect = self.CapitalManager._detectar_divisa_principal
        self.assertEqual(detect(['BTC/USDT', 'ETH/USDT']), 'USDT')
        self.assertEqual(detect(['ADA/EUR', 'BNB/EUR', 'BTC/USDT']), 'EUR')
        self.assertEqual(detect(['BTCUSDT', 'ADAUSDT']), 'EUR')  # no '/' implies EUR fallback

    def test_initial_capital_assignment(self) -> None:
        config = DummyConfig(symbols=['BTC/USDT', 'ETH/USDT', 'BNB/USDT'])
        cm = self.CapitalManager(config, cliente=None, risk=self.risk, fraccion_kelly=0.1)
        # 1000 total / 3 symbols, min 20 per symbol
        expected = max(1000.0 / 3, 20.0)
        self.assertEqual(cm.capital_por_simbolo['BTC/USDT'], expected)
        # capital currency should be detected as 'USDT'
        self.assertEqual(cm.capital_currency, 'USDT')

    def test_tiene_capital_and_es_moneda_base(self) -> None:
        config = DummyConfig(symbols=['BTC/USDT'])
        cm = self.CapitalManager(config, cliente=None, risk=self.risk, fraccion_kelly=0.1)
        self.assertTrue(cm.tiene_capital('BTC/USDT'))
        cm.capital_por_simbolo['BTC/USDT'] = 0.0
        self.assertFalse(cm.tiene_capital('BTC/USDT'))
        # es_moneda_base
        self.assertTrue(cm.es_moneda_base('BTC/USDT'))
        self.assertFalse(cm.es_moneda_base('ADA/EUR'))

    def test_capital_libre_and_hay_capital_libre(self) -> None:
        config = DummyConfig(symbols=['BTC/USDT', 'ETH/USDT'])
        cm = self.CapitalManager(config, cliente=None, risk=self.risk, fraccion_kelly=0.1)
        # initially no positions, capital libre is total
        total_capital = sum(cm.capital_por_simbolo.values())
        self.assertAlmostEqual(cm.capital_libre(), total_capital)
        self.assertTrue(cm.hay_capital_libre())
        # simulate open position reduces capital libre
        self.risk.posiciones_abiertas = ['BTC/USDT']
        free_after = cm.capital_libre()
        self.assertLess(free_after, total_capital)
        self.assertTrue(cm.hay_capital_libre())
        # set both capitals to zero -> no capital libre
        cm.capital_por_simbolo = {s: 0.0 for s in cm.capital_por_simbolo}
        self.assertFalse(cm.hay_capital_libre())

    def test_actualizar_capital(self) -> None:
        config = DummyConfig(symbols=['BTC/USDT'])
        cm = self.CapitalManager(config, cliente=None, risk=self.risk, fraccion_kelly=0.1)
        initial = cm.capital_por_simbolo['BTC/USDT']
        final_cap = cm.actualizar_capital('BTC/USDT', 0.1)  # 10% gain
        self.assertAlmostEqual(final_cap, initial * 1.1)
        self.assertAlmostEqual(cm.capital_por_simbolo['BTC/USDT'], initial * 1.1)


if __name__ == '__main__':
    unittest.main()
