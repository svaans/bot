# tests/test_contexto_externo.py
"""
Tests for the core.contexto_externo module.

These tests cover the synchronous helper functions that do not
require network connectivity.  WebSocket streaming is not tested
because it depends on live connections to Binance.  Instead, we
verify that storing and retrieving context scores and external
data behaves as expected, and that the ``StreamContexto`` helper
updates its internal store correctly.
"""

from __future__ import annotations

import unittest
import types
import sys


def _install_context_stubs() -> None:
    """Install stubs so that ``contexto_externo`` can be imported.

    The module imports ``websockets`` which is not available in
    this environment.  We stub it out with a dummy module to
    prevent import errors.  Also, stub other dependencies.
    """
    def stub(name: str) -> types.ModuleType:
        mod = sys.modules.get(name)
        if mod is None:
            mod = types.ModuleType(name)
            sys.modules[name] = mod
        return mod

    # websockets stub with connect returning a context manager
    ws_mod = stub('websockets')
    if not hasattr(ws_mod, 'connect'):
        class DummyWS:
            async def __aenter__(self): return self
            async def __aexit__(self, exc_type, exc, tb): return False
            async def __aiter__(self):
                return self
            async def __anext__(self):
                raise StopAsyncIteration
        async def connect(*args, **kwargs) -> DummyWS:
            return DummyWS()
        ws_mod.connect = connect  # type: ignore

    # Stubs for logger and backoff
    stub('core.utils.logger').configurar_logger = lambda *args, **kwargs: types.SimpleNamespace(
        info=lambda *a, **k: None,
        warning=lambda *a, **k: None,
        error=lambda *a, **k: None,
        critical=lambda *a, **k: None,
        debug=lambda *a, **k: None,
    )
    stub('core.utils.backoff').calcular_backoff = lambda *args, **kwargs: 1.0
    # Minimal registro_metrico
    stub('core.registro_metrico').registro_metrico = types.SimpleNamespace(registrar=lambda *args, **kwargs: None)
    # Stub supervisor tick
    stub('core.supervisor').tick = lambda *args, **kwargs: None


class ContextoExternoTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        _install_context_stubs()
        import core.contexto_externo as ctx  # type: ignore
        cls.ctx = ctx

    def setUp(self) -> None:
        # reset internal storage before each test
        self.ctx._PUNTAJES.clear()
        self.ctx._DATOS_EXTERNOS.clear()

    def test_obtener_puntaje_contexto_and_todos(self) -> None:
        self.assertEqual(self.ctx.obtener_puntaje_contexto('BTCUSDT'), 0.0)
        self.ctx._PUNTAJES['BTCUSDT'] = 1.5
        self.assertAlmostEqual(self.ctx.obtener_puntaje_contexto('BTCUSDT'), 1.5)
        # store non-float should return 0
        self.ctx._PUNTAJES['ETHUSDT'] = 'invalid'
        self.assertEqual(self.ctx.obtener_puntaje_contexto('ETHUSDT'), 0.0)
        all_scores = self.ctx.obtener_todos_puntajes()
        self.assertEqual(all_scores, {'BTCUSDT': 1.5, 'ETHUSDT': 'invalid'})

    def test_obtener_y_actualizar_datos_externos(self) -> None:
        self.assertEqual(self.ctx.obtener_datos_externos('BTCUSDT'), {})
        self.ctx._DATOS_EXTERNOS['BTCUSDT'] = {'foo': 'bar'}
        self.assertEqual(self.ctx.obtener_datos_externos('BTCUSDT'), {'foo': 'bar'})
        # update via StreamContexto.actualizar_datos_externos
        stream = self.ctx.StreamContexto()
        stream.actualizar_datos_externos('BTCUSDT', {'bar': 'baz'})
        # both keys should be present
        self.assertEqual(self.ctx.obtener_datos_externos('BTCUSDT'), {'foo': 'bar', 'bar': 'baz'})


if __name__ == '__main__':
    unittest.main()
