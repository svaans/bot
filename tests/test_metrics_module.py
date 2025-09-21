# tests/test_metrics_module.py
"""
Tests for the metrics module (:mod:`core.metrics`).

These tests verify that the exposed functions update internal
dictionaries and interact with the ``registro_metrico`` and
``alert_manager`` stubs appropriately.  Since the real module
relies on Prometheus counters and gauges, these objects are
stubbed to capture calls to ``inc`` and ``set`` methods without
requiring external packages.
"""

from __future__ import annotations

import unittest
import types
import sys


def _install_metrics_stubs() -> None:
    """Install stubs for Prometheus and alert manager used in metrics.py.

    The metrics module imports many heavy dependencies.  To avoid
    pulling them in, we provide minimal stubs that record calls to
    ``inc`` and ``set``.  We also stub ``registro_metrico.registrar``
    to count how many times it is invoked.
    """
    # Helper for module registration
    def stub(name: str) -> types.ModuleType:
        mod = sys.modules.get(name)
        if mod is None:
            mod = types.ModuleType(name)
            sys.modules[name] = mod
        return mod

    # Logger stub
    logger_mod = stub('core.utils.logger')
    if not hasattr(logger_mod, 'configurar_logger'):
        logger_mod.configurar_logger = lambda *args, **kwargs: types.SimpleNamespace(
            info=lambda *a, **k: None,
            warning=lambda *a, **k: None,
            error=lambda *a, **k: None,
            critical=lambda *a, **k: None,
        )

    # Stub registro_metrico
    registro = stub('core.registro_metrico')
    calls: list[tuple[str, dict]] = []
    def registrar(metric_name: str, labels: dict) -> None:
        calls.append((metric_name, labels))
    registro.registrar = registrar  # type: ignore
    registro.reset_calls = lambda: calls.clear()  # helper for tests
    registro.calls = calls

    # Stub alert manager
    alert_mod = stub('core.alertas')
    alert_manager = types.SimpleNamespace()
    # simple counters per key-symbol combination
    alert_counts: dict[tuple[str, str], int] = {}
    def record(key: str, symbol: str, count: int = 1) -> float:
        alert_counts[(key, symbol)] = alert_counts.get((key, symbol), 0) + count
        # return rate per second (count over 60 seconds) arbitrarily
        return alert_counts[(key, symbol)] / 60.0
    def should_alert(key: str, symbol: str) -> bool:
        # only alert when count exceeds 1 for test simplicity
        return alert_counts.get((key, symbol), 0) > 1
    alert_manager.record = record
    alert_manager.should_alert = should_alert
    alert_mod.alert_manager = alert_manager  # type: ignore

    # Prometheus stub classes
    class DummyMetric:
        def __init__(self) -> None:
            self.calls: list[tuple[str, tuple, dict]] = []
        def labels(self, **kwargs):
            return self
        def inc(self, *args, **kwargs):
            self.calls.append(('inc', args, kwargs))
        def set(self, *args, **kwargs):
            self.calls.append(('set', args, kwargs))
    class DummyHistogram(DummyMetric):
        def observe(self, *args, **kwargs):
            self.calls.append(('observe', args, kwargs))

    prometheus = stub('prometheus_client')
    prometheus.Counter = lambda *args, **kwargs: DummyMetric()
    prometheus.Gauge = lambda *args, **kwargs: DummyMetric()
    prometheus.Histogram = lambda *args, **kwargs: DummyHistogram()
    prometheus.start_wsgi_server = lambda *args, **kwargs: (None, None)

    # Provide environment variable used for UMBRAL_VELAS_RECHAZADAS
    import os
    os.environ.setdefault('UMBRAL_VELAS_RECHAZADAS', '5')


class MetricsModuleTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        _install_metrics_stubs()
        import core.metrics as metrics  # type: ignore
        cls.metrics = metrics

    def setUp(self) -> None:
        # Reset internal counters and stub call logs before each test
        self.metrics._decisions.clear()
        self.metrics._orders.clear()
        self.metrics._correlacion_btc.clear()
        self.metrics._velas_total.clear()
        self.metrics._velas_rechazadas.clear()
        self.metrics._buy_rejected_insufficient_funds = 0
        # reset registro_metrico calls
        sys.modules['core.registro_metrico'].reset_calls()

    def test_registrar_decision(self) -> None:
        self.metrics.registrar_decision('BTCUSDT', 'buy')
        self.assertEqual(self.metrics._decisions['BTCUSDT']['buy'], 1)
        self.metrics.registrar_decision('BTCUSDT', 'buy')
        self.assertEqual(self.metrics._decisions['BTCUSDT']['buy'], 2)
        # Ensure registro_metrico.registrar called twice
        calls = sys.modules['core.registro_metrico'].calls
        self.assertEqual(len(calls), 2)
        self.assertEqual(calls[0][0], 'decision')

    def test_registrar_orden(self) -> None:
        self.metrics.registrar_orden('closed')
        self.metrics.registrar_orden('closed')
        self.assertEqual(self.metrics._orders['closed'], 2)

    def test_registrar_buy_rejected_insufficient_funds(self) -> None:
        self.metrics.registrar_buy_rejected_insufficient_funds()
        self.metrics.registrar_buy_rejected_insufficient_funds()
        self.assertEqual(self.metrics._buy_rejected_insufficient_funds, 2)

    def test_registrar_correlacion_btc(self) -> None:
        self.metrics.registrar_correlacion_btc('BTCUSDT', 0.5)
        self.assertAlmostEqual(self.metrics._correlacion_btc['BTCUSDT'], 0.5)

    def test_registrar_vela_recibida_and_rechazada(self) -> None:
        # recibir una vela
        self.metrics.registrar_vela_recibida('BTCUSDT')
        self.assertEqual(self.metrics._velas_total['BTCUSDT'], 1)
        # recibir otra y luego rechazar una
        self.metrics.registrar_vela_recibida('BTCUSDT')
        self.metrics.registrar_vela_rechazada('BTCUSDT', 'duplicate')
        # totals should reflect increments
        self.assertEqual(self.metrics._velas_total['BTCUSDT'], 2)
        self.assertEqual(self.metrics._velas_rechazadas['BTCUSDT']['duplicate'], 1)
        # porcentaje > 0 due to one rejection of two
        pct = self.metrics._velas_rechazadas['BTCUSDT']['duplicate'] / self.metrics._velas_total['BTCUSDT'] * 100
        self.assertGreaterEqual(pct, 50.0)

    def test_registrar_feed_funding_and_open_interest_missing(self) -> None:
        # feed funding missing
        self.metrics.registrar_feed_funding_missing('BTCUSDT', 'no_funding')
        # feed open interest missing
        self.metrics.registrar_feed_open_interest_missing('BTCUSDT', 'no_open')
        # registro_metrico.registrar should have two calls
        calls = sys.modules['core.registro_metrico'].calls
        # order isn't guaranteed due to stub but length check
        self.assertEqual(len(calls), 2)
        reasons = {labels['reason'] for _, labels in calls}
        self.assertEqual(reasons, {'no_funding', 'no_open'})

    def test_registrar_watchdog_restart(self) -> None:
        self.metrics.registrar_watchdog_restart('contexto')
        # second call triggers alert due to stub conditions (record count > 1)
        self.metrics.registrar_watchdog_restart('contexto')
        # registro_metrico should have two entries with task 'contexto'
        calls = sys.modules['core.registro_metrico'].calls
        self.assertEqual(len(calls), 2)
        self.assertTrue(all(labels.get('task') == 'contexto' for _, labels in calls))

    def test_registrar_warmup_progress_and_weight(self) -> None:
        # The gauge set methods are stubbed; here we just ensure no exception is raised
        try:
            self.metrics.registrar_warmup_progress('BTCUSDT', 0.5)
            self.metrics.registrar_binance_weight(42)
        except Exception as exc:
            self.fail(f"Gauge operations raised an exception: {exc}")


if __name__ == '__main__':
    unittest.main()
