"""
Tests for a subset of helper functions in ``core.procesar_vela``.

The full candle processing logic involves I/O, shared state and
asynchronous operations.  Here we focus on deterministic helper
functions that can be exercised in isolation: spread calculation and
simple spread gating.  External dependencies are stubbed out where
necessary.
"""

import sys
import types
import unittest


def _install_stubs() -> None:
    """Install minimal stubs for third‑party modules used by procesar_vela."""
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
        # Observability metrics stubbed to avoid Prometheus imports
        'observability': types.SimpleNamespace(metrics=types.SimpleNamespace(_get_metric=lambda *args, **kwargs: None)),
        'observability.metrics': types.SimpleNamespace(
            _get_metric=lambda *args, **kwargs: None,
            EVALUAR_ENTRADA_LATENCY_MS=None,
            EVALUAR_ENTRADA_TIMEOUTS=None,
        ),
    }
    for name, module in stubs.items():
        if name not in sys.modules:
            sys.modules[name] = module
        else:
            existing = sys.modules[name]
            for attr in dir(module):
                if not hasattr(existing, attr):
                    setattr(existing, attr, getattr(module, attr))


class DummyTrader:
    """Simplified trader object for testing spread gate logic."""
    def __init__(self, spread_guard=None, max_spread_ratio=0.0) -> None:
        self.spread_guard = spread_guard
        self.config = types.SimpleNamespace(max_spread_ratio=max_spread_ratio)


class ProcesarVelaUtilsTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        _install_stubs()
        import core.procesar_vela as pv  # type: ignore
        # Bind helper functions as static methods to avoid implicit 'self'
        cls._approximate_spread = staticmethod(pv._approximate_spread)  # type: ignore
        cls._spread_gate = staticmethod(pv._spread_gate)  # type: ignore

    def test_approximate_spread(self) -> None:
        snapshot = {'high': 110.0, 'low': 90.0, 'close': 100.0}
        ratio = self._approximate_spread(snapshot)
        self.assertAlmostEqual(ratio, 0.2)
        # If close is zero or missing, ratio should be zero
        self.assertEqual(self._approximate_spread({'high': 10, 'low': 5, 'close': 0}), 0.0)
        self.assertEqual(self._approximate_spread({}), 0.0)

    def test_spread_gate_default(self) -> None:
        # Without spread_guard and limit zero -> allowed always
        trader = DummyTrader(spread_guard=None, max_spread_ratio=0.0)
        permitted, ratio, limit = self._spread_gate(trader, 'BTCUSDT', {'high': 110, 'low': 90, 'close': 100})
        self.assertTrue(permitted)
        self.assertAlmostEqual(ratio, 0.2)
        self.assertEqual(limit, 0.0)
        # With a non-zero limit, exceeding ratio should be disallowed
        trader2 = DummyTrader(spread_guard=None, max_spread_ratio=0.1)
        permitted2, ratio2, limit2 = self._spread_gate(trader2, 'ETHUSDT', {'high': 110, 'low': 90, 'close': 100})
        self.assertFalse(permitted2)
        self.assertAlmostEqual(limit2, 0.1)


if __name__ == '__main__':
    unittest.main()