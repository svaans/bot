"""
Tests for the core.contexto_externo module.

These tests cover synchronous helper functions that do not require
network connectivity.  If the module does not expose the internal
storage dictionaries, the tests are skipped.
"""

from __future__ import annotations

import unittest
import types
import sys


def _install_context_stubs() -> None:
    """Install stubs so that ``contexto_externo`` can be imported."""
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

    stub('core.utils.logger').configurar_logger = lambda *args, **kwargs: types.SimpleNamespace(
        info=lambda *a, **k: None,
        warning=lambda *a, **k: None,
        error=lambda *a, **k: None,
        critical=lambda *a, **k: None,
        debug=lambda *a, **k: None,
    )
    stub('core.utils.backoff').calcular_backoff = lambda *args, **kwargs: 1.0
    stub('core.registro_metrico').registro_metrico = types.SimpleNamespace(registrar=lambda *args, **kwargs: None)
    stub('core.supervisor').tick = lambda *args, **kwargs: None


class ContextoExternoTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        _install_context_stubs()
        import core.contexto_externo as ctx  # type: ignore
        if not hasattr(ctx, '_PUNTAJES') or not hasattr(ctx, '_DATOS_EXTERNOS'):
            raise unittest.SkipTest('contexto_externo does not expose internal storage')
        cls.ctx = ctx

    def setUp(self) -> None:
        if not hasattr(self.ctx, '_PUNTAJES') or not hasattr(self.ctx, '_DATOS_EXTERNOS'):
            self.skipTest('contexto_externo does not expose internal storage')
        self.ctx._PUNTAJES.clear()
        self.ctx._DATOS_EXTERNOS.clear()

    def test_obtener_puntaje_contexto_and_todos(self) -> None:
        self.assertEqual(self.ctx.obtener_puntaje_contexto('BTCUSDT'), 0.0)
        self.ctx._PUNTAJES['BTCUSDT'] = 1.5
        self.assertAlmostEqual(self.ctx.obtener_puntaje_contexto('BTCUSDT'), 1.5)
        self.ctx._PUNTAJES['ETHUSDT'] = 'invalid'
        self.assertEqual(self.ctx.obtener_puntaje_contexto('ETHUSDT'), 0.0)
        all_scores = self.ctx.obtener_todos_puntajes()
        self.assertEqual(all_scores, {'BTCUSDT': 1.5, 'ETHUSDT': 'invalid'})

    def test_obtener_y_actualizar_datos_externos(self) -> None:
        self.assertEqual(self.ctx.obtener_datos_externos('BTCUSDT'), {})
        self.ctx._DATOS_EXTERNOS['BTCUSDT'] = {'foo': 'bar'}
        self.assertEqual(self.ctx.obtener_datos_externos('BTCUSDT'), {'foo': 'bar'})
        stream = self.ctx.StreamContexto()
        stream.actualizar_datos_externos('BTCUSDT', {'bar': 'baz'})
        self.assertEqual(self.ctx.obtener_datos_externos('BTCUSDT'), {'foo': 'bar', 'bar': 'baz'})


if __name__ == '__main__':
    unittest.main()

