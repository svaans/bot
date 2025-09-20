"""
Tests for the level validation routines in :mod:`core.risk.level_validators`.

These tests exercise the public API of ``validate_levels`` and ensure
that it enforces ordering and distance rules correctly for both long
and short positions.  Since ``validate_levels`` does not depend on
external services, it can be tested directly.
"""

import unittest
import types
import sys
import os

# Ensure the ``core`` package from the repository root is on the path
TEST_DIR = os.path.dirname(__file__)
REPO_ROOT = os.path.abspath(os.path.join(TEST_DIR, os.pardir))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

# Stub out optional third‑party modules so that imports succeed.  The bot's
# logger depends on python‑dotenv and colorlog at import time; by registering
# dummy modules here, we avoid ModuleNotFoundError when importing the core
# packages.  Likewise ``ccxt`` is required by binance_api.cliente, so we stub
# it out, although it is not used in these tests.
sys.modules.setdefault(
    'dotenv', types.SimpleNamespace(load_dotenv=lambda *args, **kwargs: None)
)
sys.modules.setdefault('colorlog', types.SimpleNamespace())
sys.modules.setdefault('colorama', types.SimpleNamespace(Back=None, Fore=None, Style=None, init=lambda: None))
sys.modules.setdefault('ccxt', types.SimpleNamespace(binance=lambda *args, **kwargs: None))
# ``filelock`` is used by the risk modules; stub it to avoid import errors.
sys.modules.setdefault(
    'filelock',
    types.SimpleNamespace(
        FileLock=lambda *args, **kwargs: types.SimpleNamespace(
            __enter__=lambda self: None,
            __exit__=lambda self, exc_type, exc_val, exc_tb: False,
        )
    ),
)

from core.risk.level_validators import validate_levels, LevelValidationError


class ValidateLevelsTestCase(unittest.TestCase):
    """Unit tests for ``validate_levels``."""

    def test_valid_long_levels(self):
        """A basic long position with wide SL/TP should be accepted."""
        entry, sl, tp = validate_levels(
            side="long",
            entry=100.0,
            sl=90.0,
            tp=110.0,
            min_dist_pct=0.02,
            tick_size=0.1,
            step_size=0.1,
        )
        # Returned values should match the quantized inputs
        self.assertEqual(entry, 100.0)
        self.assertEqual(sl, 90.0)
        self.assertEqual(tp, 110.0)

    def test_valid_short_levels(self):
        """A short position flips the ordering of SL/TP."""
        entry, sl, tp = validate_levels(
            side="short",
            entry=100.0,
            sl=110.0,
            tp=90.0,
            min_dist_pct=0.02,
            tick_size=0.1,
            step_size=0.1,
        )
        self.assertEqual(entry, 100.0)
        # For shorts, ``sl`` above entry is quantized up and ``tp`` below entry down
        self.assertEqual(sl, 110.0)
        self.assertEqual(tp, 90.0)

    def test_invalid_direction(self):
        """An unknown side should raise an error."""
        with self.assertRaises(LevelValidationError) as ctx:
            validate_levels(
                side="buy",
                entry=100.0,
                sl=90.0,
                tp=110.0,
                min_dist_pct=0.02,
                tick_size=0.1,
                step_size=0.1,
            )
        self.assertEqual(ctx.exception.reason, "invalid_side")

    def test_min_distance_violation(self):
        """An SL/TP too close to entry should trigger the min_distance rule."""
        # For a 1% minimum distance on a long, 100 -> min_distance = max(1, 0.0)
        with self.assertRaises(LevelValidationError) as ctx:
            validate_levels(
                side="long",
                entry=100.0,
                sl=99.2,  # 0.8 away from entry < 1.0 minimum
                tp=101.0,
                min_dist_pct=0.01,
                tick_size=0.1,
                step_size=0.1,
            )
        self.assertEqual(ctx.exception.reason, "min_distance")


if __name__ == "__main__":
    unittest.main()