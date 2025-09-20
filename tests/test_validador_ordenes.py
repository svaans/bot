"""
Tests for :mod:`core.risk.validador_ordenes`.

The ``validar_orden`` function uses Binance exchange information to
validate stop-loss and take-profit levels.  It computes a minimum
distance between the entry price and the stop loss based on either
``tick_size`` or ``percent_price`` and then uses ``ajustar_tick_size`` to
quantize levels.  To avoid external requests in tests, the exchange
client is replaced with a dummy implementation.
"""

import unittest
import sys
import os
import types
from unittest.mock import patch

TEST_DIR = os.path.dirname(__file__)
REPO_ROOT = os.path.abspath(os.path.join(TEST_DIR, os.pardir))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

# Stub external modules used by the bot so imports succeed in a minimal environment.
sys.modules.setdefault(
    'dotenv', types.SimpleNamespace(load_dotenv=lambda *args, **kwargs: None)
)
sys.modules.setdefault('colorlog', types.SimpleNamespace())
sys.modules.setdefault('colorama', types.SimpleNamespace(Back=None, Fore=None, Style=None, init=lambda: None))
sys.modules.setdefault('ccxt', types.SimpleNamespace(binance=lambda *args, **kwargs: None))
# Stub filelock to avoid missing dependency for risk modules
sys.modules.setdefault(
    'filelock',
    types.SimpleNamespace(
        FileLock=lambda *args, **kwargs: types.SimpleNamespace(
            __enter__=lambda self: None,
            __exit__=lambda self, exc_type, exc_val, exc_tb: False,
        )
    ),
)

from core.risk.validador_ordenes import validar_orden


class DummyClient:
    """Stub client that returns a precomputed exchangeInfo payload."""

    def __init__(self, payload):
        self._payload = payload

    def public_get_exchangeinfo(self, params):
        # ignore params and return the stored payload
        return self._payload


class ValidarOrdenTestCase(unittest.TestCase):
    def _patch_client(self, tick_size: float, multiplier_up: float, multiplier_down: float):
        """Create a patch for ``binance_api.cliente.crear_cliente`` that returns a dummy client.

        The payload includes a single symbol with ``PRICE_FILTER`` and ``PERCENT_PRICE``.
        ``tick_size`` must be passed as a float and will be converted to string in the payload.
        ``multiplier_up`` and ``multiplier_down`` control the percent_price calculation.
        """
        payload = {
            "symbols": [
                {
                    "filters": [
                        {"filterType": "PRICE_FILTER", "tickSize": str(tick_size)},
                        {
                            "filterType": "PERCENT_PRICE",
                            "multiplierUp": str(multiplier_up),
                            "multiplierDown": str(multiplier_down),
                        },
                    ]
                }
            ]
        }
        # Patch the reference used inside core.risk.validador_ordenes.  The module
        # imports ``crear_cliente`` directly, so patching it in
        # ``binance_api.cliente`` would not affect the local name.
        return patch(
            "core.risk.validador_ordenes.crear_cliente",
            return_value=DummyClient(payload),
        )

    def test_min_distance_enforced(self):
        """SL too close to entry should raise a ValueError."""
        # tick_size=0.5, percent_price=(1.05-1)=0.05 -> min_distance = max(0.5*2, 0.05*100) = 5
        with self._patch_client(tick_size=0.5, multiplier_up=1.05, multiplier_down=0.95):
            with self.assertRaises(ValueError):
                validar_orden(
                    symbol="BTC/USDT",
                    entry=100.0,
                    sl=98.0,
                    tp=105.0,
                    min_ticks=2,
                )

    def test_valid_levels_return_quantized(self):
        """A valid SL/TP returns values quantized to tick_size."""
        with self._patch_client(tick_size=0.5, multiplier_up=1.02, multiplier_down=0.98):
            sl, tp = validar_orden(
                symbol="BTC/USDT",
                entry=100.0,
                sl=90.3,  # quantized down to 90.0 for long (below entry)
                tp=110.7,  # quantized up to 111.0 for long (above entry)
                min_ticks=2,
            )
            # With tick_size=0.5 the SL will be rounded down to nearest multiple
            self.assertEqual(sl, 90.0)
            self.assertEqual(tp, 111.0)


if __name__ == "__main__":
    unittest.main()