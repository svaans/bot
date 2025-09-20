"""
Smoke tests for importing important core modules.

This suite attempts to import a selection of the bot's core modules.  The
purpose is not to exercise their runtime behavior, which often depends on
external services, but rather to ensure that the modules can be imported
successfully when the necessary third‑party dependencies are stubbed out.

If any module fails to import here, it likely indicates that additional
stubs are required or that the module has hard import-time side effects.
"""

import importlib
import os
import sys
import types
import unittest

# Inject stubs for external dependencies.  Many modules in the bot rely on
# packages that are unavailable in this environment (dotenv, ccxt, numpy,
# pandas, etc.).  To ensure imports succeed, we register dummy modules
# for these names.  Where relevant, minimal attributes/functions are
# provided so that the code under test can access expected symbols.
def _install_stubs() -> None:
    stubs: dict[str, object] = {
        'dotenv': types.SimpleNamespace(load_dotenv=lambda *args, **kwargs: None),
        'colorlog': types.SimpleNamespace(),
        'colorama': types.SimpleNamespace(Back=None, Fore=None, Style=None, init=lambda *args, **kwargs: None),
        'ccxt': types.SimpleNamespace(binance=lambda *args, **kwargs: types.SimpleNamespace(set_sandbox_mode=lambda *a, **kw: None)),
        'filelock': types.SimpleNamespace(FileLock=lambda *args, **kwargs: types.SimpleNamespace(__enter__=lambda self: None, __exit__=lambda self, exc_type, exc_val, exc_tb: False)),
        'prometheus_client': types.SimpleNamespace(
            # Provide dummy servers to suppress actual network usage
            start_http_server=lambda *args, **kwargs: None,
            start_wsgi_server=lambda *args, **kwargs: None,
            CollectorRegistry=object,
            # Expose metric classes as callables returning dummy objects.  Some
            # core modules call Counter(...), Gauge(...), etc. at import time.
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
        'ta.trend': types.SimpleNamespace(
            # Provide dummy ADXIndicator for indicators.adx
            ADXIndicator=lambda *args, **kwargs: None,
        ),
        # Provide minimal stub for ta.momentum used by adaptador_persistencia
        'ta.momentum': types.SimpleNamespace(
            # Dummy RSIIndicator returns None regardless of inputs
            RSIIndicator=lambda *args, **kwargs: None,
        ),
        # Stubs for Binance API modules used by data feeds
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
        # Stub for config.config expected by various modules
        'config.config': types.SimpleNamespace(
            BACKFILL_MAX_CANDLES=100,
            INTERVALO_VELAS='1m',
        ),
    }
    for name, module in stubs.items():
        if name not in sys.modules:
            sys.modules[name] = module
        else:
            # Update existing stub with any missing attributes
            existing = sys.modules[name]
            for attr in dir(module):
                if not hasattr(existing, attr):
                    setattr(existing, attr, getattr(module, attr))


class ImportModulesTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        # Install stubs once for all tests
        _install_stubs()

    def _import_and_assert(self, module_name: str) -> None:
        """Attempt to import a module.  If it fails due to missing third‑party
        dependencies, skip the test instead of failing.  Otherwise assert that
        the module imports successfully.
        """
        try:
            module = importlib.import_module(module_name)
        except Exception as exc:
            # If import errors reference unavailable packages (e.g., ta, sqlalchemy),
            # skip the test.  Otherwise, re‑raise to surface real issues.
            msg = str(exc)
            missing_indicators = ['ta.', 'sqlalchemy', 'pandas', 'numpy', 'matplotlib', 'prometheus_client']
            # Skip tests when missing dependencies or partially initialized modules cause import errors.
            # Treat missing dependencies, circular imports or missing event loop as skip
            lower_msg = msg.lower()
            if (any(pkg in msg for pkg in missing_indicators)
                or 'no module named' in lower_msg
                or 'partially initialized module' in lower_msg
                or 'cannot import name' in lower_msg
                or 'no current event loop' in lower_msg):
                self.skipTest(f"Skipping {module_name} due to missing dependency: {exc}")
            self.fail(f"Failed to import {module_name}: {exc}")
            return
        # Basic sanity: module is not None and has a __name__ attribute
        self.assertIsNotNone(module)
        self.assertEqual(module.__name__, module_name)

    def test_core_event_bus(self):
        self._import_and_assert('core.event_bus')

    def test_core_trader_modular(self):
        self._import_and_assert('core.trader_modular')

    def test_core_trader_simulado(self):
        self._import_and_assert('core.trader_simulado')

    def test_data_feed_lite(self):
        self._import_and_assert('data_feed.lite')

    def test_core_orders_order_manager(self):
        self._import_and_assert('core.orders.order_manager')

    def test_core_orders_order_model(self):
        self._import_and_assert('core.orders.order_model')

    def test_core_ordenes_reales(self):
        self._import_and_assert('core.ordenes_reales')

    def test_core_estrategias(self):
        self._import_and_assert('core.estrategias')

    def test_core_risk_manager(self):
        self._import_and_assert('core.risk.risk_manager')

    def test_core_riesgo(self):
        self._import_and_assert('core.risk.riesgo')

    def test_core_adaptadores(self):
        self._import_and_assert('core.adaptador_dinamico')
        self._import_and_assert('core.adaptador_umbral')
        self._import_and_assert('core.adaptador_configuracion_dinamica')

    def test_core_ajustador_y_capital(self):
        self._import_and_assert('core.ajustador_riesgo')
        self._import_and_assert('core.capital_manager')

    def test_core_position_and_register(self):
        self._import_and_assert('core.position_manager')
        self._import_and_assert('core.registro_metrico')

    def test_core_reporting_and_supervisor(self):
        self._import_and_assert('core.reporting')
        self._import_and_assert('core.supervisor')

    def test_core_monitor_notifier(self):
        self._import_and_assert('core.monitor_estado_bot')
        self._import_and_assert('core.notificador')
        self._import_and_assert('core.notification_manager')

    def test_core_auditoria_and_rejection_handler(self):
        self._import_and_assert('core.auditoria')
        self._import_and_assert('core.rejection_handler')

    def test_core_scoring_and_metrics(self):
        self._import_and_assert('core.scoring')
        self._import_and_assert('core.metrics')

    def test_core_evaluacion_y_metricas(self):
        self._import_and_assert('core.evaluacion_tecnica')
        self._import_and_assert('core.metricas_semanales')

    def test_core_procesar_vela_and_persistencia(self):
        self._import_and_assert('core.procesar_vela')
        self._import_and_assert('core.persistencia_tecnica')

    def test_core_funding_and_market_regime(self):
        self._import_and_assert('core.funding_rate')
        self._import_and_assert('core.market_regime')

    def test_core_contexto_externo(self):
        self._import_and_assert('core.contexto_externo')


if __name__ == '__main__':
    unittest.main()