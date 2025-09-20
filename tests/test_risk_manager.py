"""
Tests for ``core.risk.risk_manager.RiskManager``.

These tests exercise key methods of the RiskManager class in isolation.
External modules are stubbed out to avoid pulling in unavailable
dependencies.  The goal is to validate the logic around correlation
computations, entry permissions, volatility factor calculations and
threshold adjustments.
"""

import sys
import types
import unittest
from datetime import datetime, timedelta


def _install_stubs() -> None:
    """Install stubs for external dependencies required by risk modules."""
    stubs: dict[str, object] = {
        'dotenv': types.SimpleNamespace(load_dotenv=lambda *args, **kwargs: None),
        'colorlog': types.SimpleNamespace(),
        'colorama': types.SimpleNamespace(Back=None, Fore=None, Style=None, init=lambda *args, **kwargs: None),
        'ccxt': types.SimpleNamespace(binance=lambda *args, **kwargs: types.SimpleNamespace(set_sandbox_mode=lambda *a, **kw: None)),
        'filelock': types.SimpleNamespace(FileLock=lambda *args, **kwargs: types.SimpleNamespace(__enter__=lambda self: None, __exit__=lambda self, exc_type, exc_val, exc_tb: False)),
        'prometheus_client': types.SimpleNamespace(
            start_http_server=lambda *args, **kwargs: None,
            start_wsgi_server=lambda *args, **kwargs: None,
            CollectorRegistry=object,
            Counter=lambda *args, **kwargs: types.SimpleNamespace(),
            Gauge=lambda *args, **kwargs: types.SimpleNamespace(),
            Summary=lambda *args, **kwargs: types.SimpleNamespace(),
            Histogram=lambda *args, **kwargs: types.SimpleNamespace(),
        ),
        'psutil': types.SimpleNamespace(
            cpu_percent=lambda *args, **kwargs: 0.0,
            virtual_memory=lambda *args, **kwargs: types.SimpleNamespace(percent=0.0),
            Process=lambda *args, **kwargs: types.SimpleNamespace(
                cpu_percent=lambda self, interval=None: 0.0,
                memory_info=lambda self: types.SimpleNamespace(rss=0),
            ),
        ),
        'aiomonitor': types.SimpleNamespace(Monitor=lambda loop: types.SimpleNamespace(__enter__=lambda self: None, __exit__=lambda self, exc_type, exc_val, exc_tb: False)),
        'numpy': types.ModuleType('numpy'),
        'pandas': types.ModuleType('pandas'),
        'scipy': types.ModuleType('scipy'),
        'matplotlib': types.ModuleType('matplotlib'),
        'matplotlib.pyplot': types.ModuleType('matplotlib.pyplot'),
        'ta': types.ModuleType('ta'),
        'ta.trend': types.SimpleNamespace(ADXIndicator=lambda *args, **kwargs: None),
        # Provide minimal stub for ta.momentum used by adaptador_persistencia
        'ta.momentum': types.SimpleNamespace(RSIIndicator=lambda *args, **kwargs: None),
        'binance_api.websocket': types.SimpleNamespace(
            escuchar_velas=lambda *args, **kwargs: None,
            escuchar_velas_combinado=lambda *args, **kwargs: None,
            InactividadTimeoutError=Exception,
        ),
        'binance_api.cliente': types.SimpleNamespace(
            fetch_ohlcv_async=lambda *args, **kwargs: None,
            obtener_cliente=lambda *args, **kwargs: types.SimpleNamespace(load_markets=lambda: {}),
            crear_cliente=lambda *args, **kwargs: types.SimpleNamespace(load_markets=lambda: {}),
        ),
        'config.config': types.SimpleNamespace(BACKFILL_MAX_CANDLES=100, INTERVALO_VELAS='1m'),
    }
    for name, module in stubs.items():
        if name not in sys.modules:
            sys.modules[name] = module
        else:
            existing = sys.modules[name]
            for attr in dir(module):
                if not hasattr(existing, attr):
                    setattr(existing, attr, getattr(module, attr))


class DummyCapitalManager:
    """Simple stub for CapitalManager used in tests."""
    def __init__(self, hay_capital: bool = True) -> None:
        self.capital_por_simbolo: dict[str, float] = {}
        self._hay_capital = hay_capital

    def hay_capital_libre(self) -> bool:
        return self._hay_capital


class RiskManagerTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        _install_stubs()
        from core.risk.risk_manager import RiskManager  # type: ignore
        cls.RiskManager = RiskManager

    def test_correlacion_media(self) -> None:
        rm = self.RiskManager(umbral=0.1)
        # Open two positions
        rm.abrir_posicion('BTCUSDT')
        rm.abrir_posicion('ETHUSDT')
        # Provide correlations for both open positions
        correlaciones = {'BTCUSDT': 0.5, 'ETHUSDT': 0.3}
        media = rm.correlacion_media('BNBUSDT', correlaciones)
        self.assertAlmostEqual(media, (0.5 + 0.3) / 2)

    def test_permite_entrada_ok(self) -> None:
        # No cooldown, capital available, low correlation
        rm = self.RiskManager(umbral=0.1, capital_manager=DummyCapitalManager(hay_capital=True))
        rm.abrir_posicion('BTCUSDT')
        correlaciones = {'BTCUSDT': 0.1}
        permitido = rm.permite_entrada('ETHUSDT', correlaciones, diversidad_minima=0.5)
        self.assertTrue(permitido)

    def test_permite_entrada_cooldown(self) -> None:
        rm = self.RiskManager(umbral=0.1, capital_manager=DummyCapitalManager(hay_capital=True))
        # Simulate cooldown by setting a future end time
        rm._cooldown_fin = datetime.utcnow() + timedelta(seconds=60)
        rm.abrir_posicion('BTCUSDT')
        permitido = rm.permite_entrada('ETHUSDT', {'BTCUSDT': 0.1}, diversidad_minima=0.5)
        self.assertFalse(permitido)

    def test_permite_entrada_sin_capital(self) -> None:
        rm = self.RiskManager(umbral=0.1, capital_manager=DummyCapitalManager(hay_capital=False))
        rm.abrir_posicion('BTCUSDT')
        permitido = rm.permite_entrada('ETHUSDT', {'BTCUSDT': 0.1}, diversidad_minima=0.5)
        self.assertFalse(permitido)

    def test_factor_volatilidad(self) -> None:
        rm = self.RiskManager(umbral=0.1)
        # Excessive volatility → factor reduced to minimum of 0.25
        factor = rm.factor_volatilidad(volatilidad_actual=100.0, volatilidad_media=10.0, umbral=2.0)
        self.assertAlmostEqual(factor, 0.25)
        # Mild volatility → factor remains 1.0
        factor2 = rm.factor_volatilidad(volatilidad_actual=20.0, volatilidad_media=10.0, umbral=2.0)
        self.assertAlmostEqual(factor2, 1.0)
        # Invalid inputs → defaults to 1.0
        factor3 = rm.factor_volatilidad(volatilidad_actual=-5, volatilidad_media=10)
        self.assertEqual(factor3, 1.0)

    def test_ajustar_umbral(self) -> None:
        rm = self.RiskManager(umbral=0.1)
        # Positive weekly gain should increase threshold slightly
        rm.ajustar_umbral({'ganancia_semana': 0.1})
        self.assertGreater(rm.umbral, 0.1)
        umbral_after_gain = rm.umbral
        # Large drawdown should reduce threshold
        rm.ajustar_umbral({'drawdown': -0.2})
        self.assertLess(rm.umbral, umbral_after_gain)
        # High winrate with increasing capital should boost threshold but cap at 0.06
        rm.umbral = 0.05
        rm.ajustar_umbral({'winrate': 0.7, 'capital_actual': 110, 'capital_inicial': 100})
        self.assertAlmostEqual(rm.umbral, min(0.06, round(0.05 * 1.2, 4)))


if __name__ == '__main__':
    unittest.main()