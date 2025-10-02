"""
Tests for the Binance remainder validator in :mod:`core.orders.validators_binance`.

The ``remainder_executable`` function consults exchange metadata to
determine if a residual order amount can be executed.  To avoid
network calls in tests, this suite uses ``unittest.mock`` to patch
``binance_api.cliente.obtener_cliente`` and supply fake market data.
"""
import asyncio
import unittest
import sys
import os
import types
from unittest.mock import patch

# Stub third‑party modules so that validators import cleanly.  These are
# required by :mod:`core.utils.logger` and :mod:`binance_api.cliente`.
sys.modules.setdefault(
    'dotenv', types.SimpleNamespace(load_dotenv=lambda *args, **kwargs: None)
)
sys.modules.setdefault('colorlog', types.SimpleNamespace())
sys.modules.setdefault('colorama', types.SimpleNamespace(Back=None, Fore=None, Style=None, init=lambda: None))
sys.modules.setdefault('ccxt', types.SimpleNamespace(binance=lambda *args, **kwargs: None))
# Stub filelock for risk modules (not used in this test but prevents import errors)
sys.modules.setdefault(
    'filelock',
    types.SimpleNamespace(
        FileLock=lambda *args, **kwargs: types.SimpleNamespace(
            __enter__=lambda self: None,
            __exit__=lambda self, exc_type, exc_val, exc_tb: False,
        )
    ),
)
# Ensure observability metrics exposes a real asyncio.Event so awaited calls succeed.
metrics_module = sys.modules.setdefault(
    'observability.metrics',
    types.SimpleNamespace(start_background_worker=lambda *args, **kwargs: None),
)
metrics_module.background_worker_stop_event = asyncio.Event()

# Add repository root to sys.path so core modules can be imported
TEST_DIR = os.path.dirname(__file__)
REPO_ROOT = os.path.abspath(os.path.join(TEST_DIR, os.pardir))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from core.orders.validators_binance import remainder_executable


class DummyClient:
    """Simple stub simulating the ``ccxt`` exchange client interface."""

    def __init__(self, markets):
        self._markets = markets

    def load_markets(self):
        # Return the markets dict exactly as provided
        return self._markets


class RemainderExecutableTestCase(unittest.TestCase):
    """Unit tests for ``remainder_executable``."""

    def setUp(self):
        # Provide a fresh asyncio.Event for the background worker stop hook so
        # any awaited ``wait()`` calls interact with a real event object instead
        # of a ``MagicMock``.  This prevents RuntimeWarning messages under
        # ``pytest-asyncio`` strict mode.
        metrics_module.background_worker_stop_event = asyncio.Event()
        # Market metadata for our fake symbol
        self.market_data = {
            "BTC/USDT": {
                "limits": {
                    "amount": {"min": 0.001},
                    "cost": {"min": 10.0},
                },
                "precision": {"amount": 3},
            }
        }

    def _patch_client(self):
        """Patch the local reference to ``obtener_cliente`` used in validators.

        The :mod:`core.orders.validators_binance` module imports
        ``obtener_cliente`` directly from ``binance_api.cliente`` at import
        time.  Patching ``binance_api.cliente.obtener_cliente`` alone would
        not update this local reference, so we patch the symbol inside
        ``core.orders.validators_binance`` instead.
        """
        return patch(
            "core.orders.validators_binance.obtener_cliente",
            return_value=DummyClient(self.market_data),
        )

    def test_remainder_too_small(self):
        """If quantity < min_qty then it is not executable."""
        with self._patch_client():
            result = remainder_executable("BTC/USDT", price=50000.0, quantity=0.0005)
            self.assertFalse(result)

    def test_remainder_below_notional(self):
        """If notional < min_notional then it is not executable."""
        with self._patch_client():
            result = remainder_executable("BTC/USDT", price=4000.0, quantity=0.001)
            # 4000 * 0.001 = 4 < 10 => false
            self.assertFalse(result)

    def test_remainder_not_multiple_of_step(self):
        """If quantity is not a multiple of step_size (based on precision) then it's invalid."""
        with self._patch_client():
            # precision=3 => step_size=0.001; 0.0015 % 0.001 != 0
            result = remainder_executable("BTC/USDT", price=60000.0, quantity=0.0015)
            self.assertFalse(result)

    def test_remainder_valid(self):
        """A quantity that meets all criteria should return True."""
        with self._patch_client():
            result = remainder_executable("BTC/USDT", price=60000.0, quantity=0.002)
            # 0.002 >= 0.001; price*quantity=120 >= 10; and 0.002 % 0.001 == 0
            self.assertTrue(result)


if __name__ == "__main__":
    unittest.main()