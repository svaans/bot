"""
Tests for ``core.estrategias`` helper functions.

This module verifies that strategies are correctly retrieved and filtered
according to trend and trade direction, and that the synergy metric is
computed accurately.  Since the functions are pure and do not depend
on external services, the tests are straightforward.
"""

import sys
import types
import unittest


def _install_stubs() -> None:
    """Install minimal stubs for third‑party modules referenced indirectly."""
    stubs: dict[str, object] = {
        'dotenv': types.SimpleNamespace(load_dotenv=lambda *args, **kwargs: None),
        'colorlog': types.SimpleNamespace(),
        'colorama': types.SimpleNamespace(Back=None, Fore=None, Style=None, init=lambda *args, **kwargs: None),
        'ccxt': types.SimpleNamespace(binance=lambda *args, **kwargs: types.SimpleNamespace(set_sandbox_mode=lambda *a, **kw: None)),
        'filelock': types.SimpleNamespace(FileLock=lambda *args, **kwargs: types.SimpleNamespace(__enter__=lambda self: None, __exit__=lambda self, exc_type, exc_val, exc_tb: False)),
    }
    for name, module in stubs.items():
        if name not in sys.modules:
            sys.modules[name] = module
        else:
            existing = sys.modules[name]
            for attr in dir(module):
                if not hasattr(existing, attr):
                    setattr(existing, attr, getattr(module, attr))


class EstrategiasTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        _install_stubs()
        import core.estrategias as estr  # type: ignore
        cls.estr = estr

    def test_obtener_estrategias_por_tendencia(self) -> None:
        alcistas = self.estr.obtener_estrategias_por_tendencia('alcista')
        self.assertIsInstance(alcistas, list)
        self.assertIn('ascending_triangle', alcistas)
        # Unknown trend returns empty list
        self.assertEqual(self.estr.obtener_estrategias_por_tendencia('desconocida'), [])

    def test_filtrar_por_direccion(self) -> None:
        estrategias = {
            'ascending_triangle': True,   # alcista
            'descending_triangle': True,  # bajista
            'head_and_shoulders': True,   # lateral
        }
        coherentes_long, incoherentes_long = self.estr.filtrar_por_direccion(estrategias, 'long')
        # For long trades, bajista strategies are incoherent
        self.assertIn('descending_triangle', incoherentes_long)
        self.assertNotIn('ascending_triangle', incoherentes_long)
        self.assertEqual(coherentes_long['ascending_triangle'], True)
        # For short trades, alcista strategies are incoherent
        coherentes_short, incoherentes_short = self.estr.filtrar_por_direccion(estrategias, 'short')
        self.assertIn('ascending_triangle', incoherentes_short)
        self.assertNotIn('descending_triangle', incoherentes_short)

    def test_calcular_sinergia(self) -> None:
        estrategias_activas = {
            'ascending_triangle': True,   # ideal alcista
            'descending_triangle': True,  # ideal bajista
            'head_and_shoulders': False,  # lateral but inactive
        }
        # For trend 'alcista', only ascending_triangle counts
        frac_alcista = self.estr.calcular_sinergia(estrategias_activas, 'alcista')
        self.assertAlmostEqual(frac_alcista, 0.5)
        # For trend 'bajista', only descending_triangle counts
        frac_bajista = self.estr.calcular_sinergia(estrategias_activas, 'bajista')
        self.assertAlmostEqual(frac_bajista, 0.5)


if __name__ == '__main__':
    unittest.main()