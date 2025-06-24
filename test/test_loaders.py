import unittest

from core.strategies.entry.loader import cargar_estrategias
from core.strategies.exit.loader_salidas import cargar_estrategias_salida

class TestStrategyLoaders(unittest.TestCase):
    def test_cargar_estrategias(self):
        estrategias = cargar_estrategias()
        self.assertIsInstance(estrategias, dict)
        self.assertTrue(estrategias, "No se cargaron estrategias de entrada")
        for nombre, func in estrategias.items():
            self.assertTrue(callable(func), f"{nombre} no es callable")

    def test_cargar_estrategias_salida(self):
        estrategias = cargar_estrategias_salida()
        self.assertIsInstance(estrategias, list)
        self.assertTrue(estrategias, "No se cargaron estrategias de salida")
        for func in estrategias:
            self.assertTrue(callable(func), f"{func} no es callable")

if __name__ == '__main__':
    unittest.main()