"""
Tests for functions defined in ``core.scoring``.

These tests verify that the technical scoring logic behaves as expected
for a variety of input combinations.  We avoid importing heavy third‑party
libraries by stubbing out missing modules via the same ``_install_stubs``
helper from ``test_import_core_modules``.

The focus is on deterministic parts of the code such as weight aggregation
and JSON serialization of decision traces.
"""

import json
import sys
import types
import unittest


def _install_stubs() -> None:
    """Register minimal stubs for external dependencies used by core modules."""
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
        # Provide minimal stub for ta.momentum used by adaptador_persistencia (if imported indirectly)
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


class ScoringTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        # Ensure stubs are installed before importing core.scoring
        _install_stubs()
        global scoring
        import core.scoring as scoring  # type: ignore
        cls.scoring = scoring

    def test_calcular_score_tecnico_full_match(self) -> None:
        """All indicators aligned with a long/alcista scenario should yield full weights."""
        # Provide dummy DataFrame (unused) and indicator values
        score, bd = self.scoring.calcular_score_tecnico(
            df=object(),
            rsi=60.0,
            momentum=0.01,
            slope=0.2,
            tendencia='alcista',
            direccion='long',
        )
        # Based on PESOS_SCORE_TECNICO (RSI=1, Momentum=0.5, Slope=1, Tendencia=1)
        self.assertAlmostEqual(score, 3.5)
        self.assertEqual(bd.rsi, 1.0)
        self.assertEqual(bd.momentum, 0.5)
        self.assertEqual(bd.slope, 1.0)
        self.assertEqual(bd.tendencia, 1.0)

    def test_calcular_score_tecnico_half_rsi(self) -> None:
        """An RSI near the midline should contribute half weight."""
        score, bd = self.scoring.calcular_score_tecnico(
            df=None,
            rsi=50.0,
            momentum=None,
            slope=None,
            tendencia='alcista',
            direccion='long',
        )
        # Half of RSI weight = 0.5, plus 1 for tendencia
        self.assertAlmostEqual(score, 1.5)
        self.assertEqual(bd.rsi, 0.5)
        self.assertEqual(bd.momentum, 0.0)
        self.assertEqual(bd.slope, 0.0)
        self.assertEqual(bd.tendencia, 1.0)

    def test_decision_trace_serialization(self) -> None:
        """DecisionTrace.to_json should produce deterministic, sorted JSON."""
        from core.scoring import DecisionTrace, ScoreBreakdown, DecisionReason
        breakdown = ScoreBreakdown(rsi=0.1, momentum=0.2, slope=0.3, tendencia=0.4)
        trace = DecisionTrace(score=1.0, threshold=0.5, reason=DecisionReason.BELOW_THRESHOLD, breakdown=breakdown)
        json_str = trace.to_json()
        parsed = json.loads(json_str)
        # Keys should match expected
        self.assertEqual(set(parsed.keys()), {'score', 'threshold', 'reason', 'breakdown'})
        self.assertEqual(parsed['reason'], DecisionReason.BELOW_THRESHOLD.value)
        # Values inside breakdown should be rounded to 6 decimals
        for k, v in parsed['breakdown'].items():
            # All contributions rounded to 0.xxxxxx
            self.assertIsInstance(v, float)
            self.assertEqual(round(v, 6), v)


if __name__ == '__main__':
    unittest.main()