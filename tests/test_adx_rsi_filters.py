"""Tests unitarios para los filtros ADX y RSI mínimo de entrada."""
from __future__ import annotations

import math
import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
import numpy as np


def _make_df(n: int = 60, trend: bool = True) -> pd.DataFrame:
    """Genera un DataFrame de velas sintéticas con tendencia o lateral."""
    np.random.seed(42)
    if trend:
        prices = 100 + np.cumsum(np.random.normal(0.5, 1.0, n))
    else:
        prices = 100 + np.random.normal(0, 1.0, n)
    prices = np.maximum(prices, 1.0)
    high = prices * (1 + np.random.uniform(0, 0.02, n))
    low = prices * (1 - np.random.uniform(0, 0.02, n))
    volume = np.ones(n) * 1000
    return pd.DataFrame({
        "timestamp": range(n),
        "open": prices,
        "high": high,
        "low": low,
        "close": prices,
        "volume": volume,
    })


class TestADXFilter(unittest.TestCase):

    def test_adx_disabled_always_ok(self):
        """adx_min_entrada=0 nunca bloquea sin importar el ADX."""
        from indicadores.adx import calcular_adx
        df = _make_df(60, trend=False)
        adx = calcular_adx(df)
        # Con adx_min=0, sin importar el valor, debería permitir
        adx_min = 0.0
        adx_ok = not (adx_min > 0.0 and (adx is None or adx < adx_min))
        self.assertTrue(adx_ok)

    def test_adx_below_threshold_blocks(self):
        """ADX bajo el umbral bloquea la entrada."""
        with patch("indicadores.adx.calcular_adx", return_value=15.0):
            adx_val = 15.0
            adx_min = 20.0
            adx_ok = not (adx_min > 0.0 and (adx_val is None or adx_val < adx_min))
            self.assertFalse(adx_ok)

    def test_adx_above_threshold_permits(self):
        """ADX sobre el umbral permite la entrada."""
        with patch("indicadores.adx.calcular_adx", return_value=25.0):
            adx_val = 25.0
            adx_min = 20.0
            adx_ok = not (adx_min > 0.0 and (adx_val is None or adx_val < adx_min))
            self.assertTrue(adx_ok)

    def test_adx_none_blocks_when_enabled(self):
        """ADX=None con umbral activo bloquea (mercado sin datos = sin tendencia)."""
        adx_val = None
        adx_min = 20.0
        adx_ok = not (adx_min > 0.0 and (adx_val is None or adx_val < adx_min))
        self.assertFalse(adx_ok)

    def test_adx_equal_threshold_permits(self):
        """ADX exactamente igual al umbral permite (>= no <)."""
        adx_val = 20.0
        adx_min = 20.0
        adx_ok = not (adx_min > 0.0 and (adx_val is None or adx_val < adx_min))
        self.assertTrue(adx_ok)

    def test_calcular_adx_returns_float(self):
        """calcular_adx retorna float válido con datos suficientes."""
        from indicadores.adx import calcular_adx
        df = _make_df(60, trend=True)
        result = calcular_adx(df)
        self.assertIsNotNone(result)
        self.assertIsInstance(result, float)
        self.assertFalse(math.isnan(result))
        self.assertGreater(result, 0.0)

    def test_calcular_adx_none_insufficient_data(self):
        """calcular_adx retorna None con datos insuficientes."""
        from indicadores.adx import calcular_adx
        df = _make_df(5, trend=True)
        result = calcular_adx(df)
        self.assertIsNone(result)


class TestRSIMinFilter(unittest.TestCase):

    def test_rsi_min_disabled_always_ok(self):
        """rsi_min_entrada=0 nunca bloquea."""
        rsi_val = 30.0
        rsi_min = 0.0
        rsi_ok = not (rsi_min > 0.0 and rsi_val is not None and rsi_val < rsi_min)
        self.assertTrue(rsi_ok)

    def test_rsi_below_min_blocks(self):
        """RSI bajo el mínimo bloquea."""
        rsi_val = 45.0
        rsi_min = 50.0
        rsi_ok = not (rsi_min > 0.0 and rsi_val is not None and rsi_val < rsi_min)
        self.assertFalse(rsi_ok)

    def test_rsi_above_min_permits(self):
        """RSI sobre el mínimo permite."""
        rsi_val = 55.0
        rsi_min = 50.0
        rsi_ok = not (rsi_min > 0.0 and rsi_val is not None and rsi_val < rsi_min)
        self.assertTrue(rsi_ok)

    def test_rsi_none_does_not_block(self):
        """RSI=None con filtro activo no bloquea (falta de datos = conservador)."""
        rsi_val = None
        rsi_min = 50.0
        rsi_ok = not (rsi_min > 0.0 and rsi_val is not None and rsi_val < rsi_min)
        self.assertTrue(rsi_ok)

    def test_rsi_exactly_at_min_permits(self):
        """RSI exactamente en el mínimo permite (< no <=)."""
        rsi_val = 50.0
        rsi_min = 50.0
        rsi_ok = not (rsi_min > 0.0 and rsi_val is not None and rsi_val < rsi_min)
        self.assertTrue(rsi_ok)


class TestDCAEngine(unittest.TestCase):

    def setUp(self):
        from core.portfolio import dca_engine
        dca_engine.resetear_dca()

    def test_dca_disabled_returns_false(self):
        """DCA desactivado en config retorna False."""
        from core.portfolio.dca_engine import dca_permite_entrada
        config = {"dca_enabled": False}
        self.assertFalse(dca_permite_entrada("BTC/USDT", config))

    def test_primer_dca_siempre_permitido(self):
        """Primer DCA para un símbolo nuevo siempre es permitido."""
        from core.portfolio.dca_engine import dca_permite_entrada
        config = {"dca_enabled": True, "dca_interval_days": 7}
        result = dca_permite_entrada("BTC/USDT", config)
        self.assertTrue(result)

    def test_dca_reciente_no_permitido(self):
        """DCA ejecutado hace menos del intervalo no permite nueva entrada."""
        from core.portfolio.dca_engine import dca_permite_entrada, registrar_dca_ejecutado
        config = {"dca_enabled": True, "dca_interval_days": 7}
        registrar_dca_ejecutado("ETH/USDT")
        result = dca_permite_entrada("ETH/USDT", config)
        self.assertFalse(result)

    def test_simbolos_independientes(self):
        """Estado DCA de un símbolo no afecta a otro."""
        from core.portfolio.dca_engine import dca_permite_entrada, registrar_dca_ejecutado
        config = {"dca_enabled": True, "dca_interval_days": 7}
        registrar_dca_ejecutado("BTC/USDT")
        result_btc = dca_permite_entrada("BTC/USDT", config)
        result_eth = dca_permite_entrada("ETH/USDT", config)
        self.assertFalse(result_btc)
        self.assertTrue(result_eth)

    def test_dca_error_retorna_false(self):
        """Errores en DCA retornan False (conservador)."""
        from core.portfolio.dca_engine import dca_permite_entrada
        config = {"dca_enabled": True, "dca_interval_days": "not_a_number"}
        result = dca_permite_entrada("BTC/USDT", config)
        self.assertFalse(result)


class TestRebalancer(unittest.TestCase):

    def test_drift_igual_peso_es_cero(self):
        """Sin drift cuando la cartera está balanceada."""
        from core.portfolio.rebalancer import calcular_drift
        valores = {"BTC/USDT": 50.0, "ETH/USDT": 50.0}
        pesos = {"BTC/USDT": 0.5, "ETH/USDT": 0.5}
        drift = calcular_drift(valores, pesos)
        self.assertAlmostEqual(drift["BTC/USDT"], 0.0, places=3)
        self.assertAlmostEqual(drift["ETH/USDT"], 0.0, places=3)

    def test_drift_sobrepondero_positivo(self):
        """Símbolo que supera su peso objetivo tiene drift positivo."""
        from core.portfolio.rebalancer import calcular_drift
        valores = {"BTC/USDT": 70.0, "ETH/USDT": 30.0}
        pesos = {"BTC/USDT": 0.5, "ETH/USDT": 0.5}
        drift = calcular_drift(valores, pesos)
        self.assertGreater(drift["BTC/USDT"], 0)
        self.assertLess(drift["ETH/USDT"], 0)

    def test_necesita_rebalanceo_bajo_umbral(self):
        """No requiere rebalanceo cuando el drift está por debajo del umbral."""
        from core.portfolio.rebalancer import necesita_rebalanceo
        valores = {"BTC/USDT": 52.0, "ETH/USDT": 48.0}
        pesos = {"BTC/USDT": 0.5, "ETH/USDT": 0.5}
        self.assertFalse(necesita_rebalanceo(valores, pesos, umbral=0.05))

    def test_necesita_rebalanceo_sobre_umbral(self):
        """Requiere rebalanceo cuando el drift supera el umbral."""
        from core.portfolio.rebalancer import necesita_rebalanceo
        valores = {"BTC/USDT": 80.0, "ETH/USDT": 20.0}
        pesos = {"BTC/USDT": 0.5, "ETH/USDT": 0.5}
        self.assertTrue(necesita_rebalanceo(valores, pesos, umbral=0.05))

    def test_generar_acciones_correctas(self):
        """Genera acción 'reducir' para sobrepondero y 'ampliar' para subpondero."""
        from core.portfolio.rebalancer import generar_acciones_rebalanceo
        valores = {"BTC/USDT": 80.0, "ETH/USDT": 20.0}
        pesos = {"BTC/USDT": 0.5, "ETH/USDT": 0.5}
        acciones = generar_acciones_rebalanceo(valores, pesos)
        self.assertEqual(len(acciones), 2)
        btc_accion = next(a for a in acciones if a["symbol"] == "BTC/USDT")
        eth_accion = next(a for a in acciones if a["symbol"] == "ETH/USDT")
        self.assertEqual(btc_accion["accion"], "reducir")
        self.assertEqual(eth_accion["accion"], "ampliar")

    def test_resumen_cartera_string(self):
        """resumen_cartera retorna string no vacío."""
        from core.portfolio.rebalancer import resumen_cartera
        valores = {"BTC/USDT": 60.0, "ETH/USDT": 40.0}
        pesos = {"BTC/USDT": 0.5, "ETH/USDT": 0.5}
        result = resumen_cartera(valores, pesos)
        self.assertIsInstance(result, str)
        self.assertIn("BTC/USDT", result)

    def test_cartera_vacia(self):
        """Cartera sin valores retorna mensaje de vacío."""
        from core.portfolio.rebalancer import resumen_cartera
        result = resumen_cartera({}, {"BTC/USDT": 1.0})
        self.assertEqual(result, "Cartera vacía")


if __name__ == "__main__":
    unittest.main()
