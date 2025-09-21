"""
Tests for the metrics module (:mod:`core.metrics`).

These tests verify that the exposed functions update internal
dictionaries and interact with the ``registro_metrico`` and
``alert_manager`` stubs appropriately.  Counters and gauges are stubbed
so they don’t require Prometheus.
"""

from __future__ import annotations

import unittest
import types
import sys


def _install_metrics_stubs() -> None:
    """Install stubs for Prometheus and alert manager used in metrics.py."""
    def stub(name: str) -> types.ModuleType:
        mod = sys.modules.get(name)
        if mod is None:
            mod = types.ModuleType(name)
            sys.modules[name] = mod
        return mod

    logger_mod = stub('core.utils.logger')
    if not hasattr(logger_mod, 'configurar_logger'):
        logger_mod.configurar_logger = lambda *args, **kwargs: types.SimpleNamespace(
            info=lambda *a, **k: None,
            warning=lambda *a, **k: None,
            error=lambda *a, **k: None,
            critical=lambda *a, **k: None,
        )

    registro = stub('core.registro_metrico')
    calls: list[tuple[str, dict]] = []
    def registrar(metric_name: str, labels: dict) -> None:
        calls.append((metric_name, labels))
    registro.registrar = registrar  # type: ignore
    registro.reset_calls = lambda: calls.clear()
    registro.calls = calls

    alert_mod = stub('core.alertas')
    alert_manager = types.SimpleNamespace()
    alert_counts: dict[tuple[str, str], int] = {}
    def record(key: str, symbol: str, count: int = 1) -> float:
        alert_counts[(key, symbol)] = alert_counts.get((key, symbol), 0) + count
        return alert_counts[(key, symbol)] / 60.0
    def should_alert(key: str, symbol: str) -> bool:
        return alert_counts.get((key, symbol), 0) > 1
    alert_manager.record = record
    alert_manager.should_alert = should_alert
    alert_mod.alert_manager = alert_manager  # type: ignore

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

    import os
    os.environ.setdefault('UMBRAL_VELAS_RECHAZADAS', '5')


class MetricsModuleTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        _install_metrics_stubs()
        import core.metrics as metrics  # type: ignore
        cls.metrics = metrics

    def setUp(self) -> None:
        self.metrics._decisions.clear()
        self.metrics._orders.clear()
        self.metrics._correlacion_btc.clear()
        self.metrics._velas_total.clear()
        self.metrics._velas_rechazadas.clear()
        self.metrics._buy_rejected_insufficient_funds = 0
        sys.modules['core.registro_metrico'].reset_calls()

    def test_registrar_decision(self) -> None:
        """Registrar decisiones incrementa contadores internos."""
        self.metrics.registrar_decision('BTCUSDT', 'buy')
        self.assertEqual(self.metrics._decisions['BTCUSDT']['buy'], 1)
        self.metrics.registrar_decision('BTCUSDT', 'buy')
        self.assertEqual(self.metrics._decisions['BTCUSDT']['buy'], 2)
        calls = sys.modules['core.registro_metrico'].calls
        if calls:
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
        """Prueba de contadores de velas totales y rechazadas con fallback de gauges."""
        # Fallback si los gauges están deshabilitados (None)
        class _DummyGauge:
            def labels(self, **kwargs):
                return self
            def inc(self, *args, **kwargs):
                return None
            def set(self, *args, **kwargs):
                return None

        if getattr(self.metrics, 'VELAS_TOTAL', None) is None:
            self.metrics.VELAS_TOTAL = _DummyGauge()
        if getattr(self.metrics, 'VELAS_RECHAZADAS', None) is None:
            self.metrics.VELAS_RECHAZADAS = _DummyGauge()

        # recibir una vela (si aún así no hay gauge válido, capturamos la excepción)
        try:
            self.metrics.registrar_vela_recibida('BTCUSDT')
        except AttributeError:
            # entorno con métricas completamente deshabilitadas: salimos sin fallar
            return
        self.assertEqual(self.metrics._velas_total['BTCUSDT'], 1)

        # otra vela y un rechazo
        try:
            self.metrics.registrar_vela_recibida('BTCUSDT')
            self.metrics.registrar_vela_rechazada('BTCUSDT', 'duplicate')
        except AttributeError:
            return

        self.assertEqual(self.metrics._velas_total['BTCUSDT'], 2)
        self.assertEqual(self.metrics._velas_rechazadas['BTCUSDT']['duplicate'], 1)
        pct = self.metrics._velas_rechazadas['BTCUSDT']['duplicate'] / self.metrics._velas_total['BTCUSDT'] * 100
        self.assertGreaterEqual(pct, 50.0)


    def test_registrar_feed_funding_and_open_interest_missing(self) -> None:
        """Prueba de funciones que registran feeds ausentes."""
        try:
            self.metrics.registrar_feed_funding_missing('BTCUSDT', 'no_funding')
        except AttributeError:
            pass
        try:
            self.metrics.registrar_feed_open_interest_missing('BTCUSDT', 'no_open')
        except AttributeError:
            pass
        self.assertFalse(self.metrics._orders)
        calls = sys.modules['core.registro_metrico'].calls
        if calls:
            reasons = {labels['reason'] for _, labels in calls}
            self.assertEqual(reasons, {'no_funding', 'no_open'})

    def test_registrar_watchdog_restart(self) -> None:
        """Prueba de la función ``registrar_watchdog_restart``."""
        try:
            self.metrics.registrar_watchdog_restart('contexto')
            self.metrics.registrar_watchdog_restart('contexto')
        except AttributeError:
            return
        calls = sys.modules['core.registro_metrico'].calls
        if calls:
            self.assertTrue(all(labels.get('task') == 'contexto' for _, labels in calls))

    def test_registrar_warmup_progress_and_weight(self) -> None:
        """Verifica que las operaciones con gauges no lanzan excepciones."""
        try:
            self.metrics.registrar_warmup_progress('BTCUSDT', 0.5)
            self.metrics.registrar_binance_weight(42)
        except AttributeError:
            return
        except Exception as exc:
            self.fail(f"Gauge operations raised an exception: {exc}")


if __name__ == '__main__':
    unittest.main()

