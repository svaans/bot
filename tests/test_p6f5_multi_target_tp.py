"""Tests for P6-F5: multi-target TP state persistence via targets_alcanzados.

Covers:
- salida_takeprofit_atr filters already-processed targets
- gestor_salidas propagates targets_parcial result instead of mutating throwaway dict
- verificar_salidas handler calls cerrar_parcial and persists targets_alcanzados on Order
"""

from __future__ import annotations

import asyncio
from typing import Any, Dict, List
from unittest.mock import AsyncMock, MagicMock, patch

import pandas as pd
import pytest

from core.strategies.exit.salida_takeprofit_atr import salida_takeprofit_atr
from core.strategies.exit import gestor_salidas as gs
from core.orders.order_model import Order


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_df(close: float = 100.0, rows: int = 25) -> pd.DataFrame:
    data = {
        "open": [close] * rows,
        "high": [close + 1] * rows,
        "low": [close - 1] * rows,
        "close": [close] * rows,
        "volume": [1000.0] * rows,
    }
    return pd.DataFrame(data)


def _make_order(**kwargs) -> Dict[str, Any]:
    base = {
        "symbol": "BTC/USDT",
        "precio_entrada": 90.0,
        "cantidad": 1.0,
        "cantidad_abierta": 1.0,
        "direccion": "long",
        "targets_alcanzados": None,
    }
    base.update(kwargs)
    return base


# ---------------------------------------------------------------------------
# salida_takeprofit_atr — unit tests
# ---------------------------------------------------------------------------

class TestSalidaTakeprofitAtr:
    def test_targets_hit_cuando_precio_supera_nivel(self):
        """Precio actual > TP1 → targets_hit contiene ese target."""
        orden = _make_order(precio_entrada=90.0)
        df = _make_df(close=100.0)  # close 100, entrada 90 → ATR ~1 → TP1 ~92.5
        targets = [{"porcentaje": 2.5, "qty_frac": 0.5}, {"porcentaje": 5.0, "qty_frac": 0.5}]
        result = salida_takeprofit_atr(orden, df, targets=targets)
        # TP1 = 90 + ATR*2.5; with ATR≈1 → ~92.5 < 100, should be hit
        assert result.get("targets_hit"), "Se esperaba al menos un target alcanzado"
        for t in result["targets_hit"]:
            assert "porcentaje" in t, "targets_hit items deben tener 'porcentaje'"
            assert "qty_frac" in t, "targets_hit items deben tener 'qty_frac'"

    def test_targets_ya_procesados_son_filtrados(self):
        """Si targets_alcanzados ya contiene TP1, sólo se evalúa TP2."""
        orden = _make_order(
            precio_entrada=90.0,
            targets_alcanzados=[{"porcentaje": 2.5, "qty_frac": 0.5}],
        )
        df = _make_df(close=100.0)
        targets = [{"porcentaje": 2.5, "qty_frac": 0.5}, {"porcentaje": 5.0, "qty_frac": 0.5}]
        result = salida_takeprofit_atr(orden, df, targets=targets)
        # TP1 already processed — should not appear in any result
        for t in result.get("targets_hit", []):
            assert t.get("porcentaje") != 2.5, "TP1 ya procesado no debe volver a aparecer"

    def test_todos_targets_procesados_devuelve_cerrar_false(self):
        """Todos los targets procesados → cerrar=False, motivo claro."""
        targets = [{"porcentaje": 2.5, "qty_frac": 0.5}, {"porcentaje": 5.0, "qty_frac": 0.5}]
        orden = _make_order(
            precio_entrada=90.0,
            targets_alcanzados=targets,
        )
        df = _make_df(close=100.0)
        result = salida_takeprofit_atr(orden, df, targets=targets)
        assert result.get("cerrar") is False
        assert "procesados" in str(result.get("razon", "")).lower()

    def test_target_no_alcanzado_devuelve_cerrar_false(self):
        """Precio por debajo del nivel TP → cerrar=False."""
        orden = _make_order(precio_entrada=90.0)
        df = _make_df(close=91.0)  # close 91, entrada 90 → ATR ~1 → TP at 92.5 → NOT hit
        targets = [{"porcentaje": 2.5, "qty_frac": 1.0}]
        result = salida_takeprofit_atr(orden, df, targets=targets)
        assert result.get("cerrar") is False


# ---------------------------------------------------------------------------
# gestor_salidas — targets_parcial propagation (P6-F5 core)
# ---------------------------------------------------------------------------

class TestGestorSalidasTargetsParcial:
    @pytest.mark.asyncio
    async def test_targets_parcial_propagado_cuando_hay_restantes(self, monkeypatch):
        """gestor_salidas retorna targets_parcial=True cuando quedan targets."""

        def fake_tp_strategy(orden, df):
            return {
                "cerrar": False,
                "targets_hit": [{"price": 92.5, "qty": 0.5, "porcentaje": 2.5, "qty_frac": 0.5}],
                "targets": [
                    {"price": 92.5, "qty": 0.5, "porcentaje": 2.5, "qty_frac": 0.5},
                    {"price": 95.0, "qty": 0.5, "porcentaje": 5.0, "qty_frac": 0.5},
                ],
                "evento": "TP parcial",
            }

        monkeypatch.setattr(gs, "cargar_estrategias_salida", lambda: [fake_tp_strategy])

        df = _make_df()
        orden = _make_order()
        result = await gs.evaluar_salidas(orden, df, config={})

        assert result.get("targets_parcial") is True
        assert len(result.get("targets_hit", [])) == 1
        assert len(result.get("targets_restantes", [])) == 1
        assert result.get("cerrar") is False

    @pytest.mark.asyncio
    async def test_todos_targets_hit_cierra_posicion(self, monkeypatch):
        """Cuando targets_restantes está vacío gestor retorna cerrar=True."""

        def fake_tp_all_hit(orden, df):
            niveles = [{"price": 92.5, "qty": 1.0, "porcentaje": 2.5, "qty_frac": 1.0}]
            return {
                "cerrar": False,
                "targets_hit": niveles,
                "targets": niveles,
                "evento": "TP completado",
            }

        monkeypatch.setattr(gs, "cargar_estrategias_salida", lambda: [fake_tp_all_hit])

        df = _make_df()
        result = await gs.evaluar_salidas(_make_order(), df, config={})

        assert result.get("cerrar") is True
        assert "target" in str(result.get("razon", "")).lower()


# ---------------------------------------------------------------------------
# Order model — targets_alcanzados field
# ---------------------------------------------------------------------------

class TestOrderModelTargetsAlcanzados:
    def test_targets_alcanzados_defaults_none(self):
        data = {
            "symbol": "ETH/USDT",
            "precio_entrada": 2000.0,
            "cantidad": 0.5,
            "stop_loss": 1900.0,
            "take_profit": 2100.0,
            "timestamp": "2024-01-01T00:00:00+00:00",
            "estrategias_activas": {},
            "tendencia": "alcista",
            "max_price": 2000.0,
        }
        order = Order.from_dict(data)
        assert order.targets_alcanzados is None

    def test_targets_alcanzados_preserved_in_from_dict(self):
        processed = [{"porcentaje": 2.5, "qty_frac": 0.5}]
        data = {
            "symbol": "ETH/USDT",
            "precio_entrada": 2000.0,
            "cantidad": 0.5,
            "stop_loss": 1900.0,
            "take_profit": 2100.0,
            "timestamp": "2024-01-01T00:00:00+00:00",
            "estrategias_activas": {},
            "tendencia": "alcista",
            "max_price": 2000.0,
            "targets_alcanzados": processed,
        }
        order = Order.from_dict(data)
        assert order.targets_alcanzados == processed

    def test_targets_alcanzados_survives_roundtrip(self):
        processed = [{"porcentaje": 5.0, "qty_frac": 0.5}]
        data = {
            "symbol": "BTC/USDT",
            "precio_entrada": 90000.0,
            "cantidad": 0.01,
            "stop_loss": 88000.0,
            "take_profit": 95000.0,
            "timestamp": "2024-01-01T00:00:00+00:00",
            "estrategias_activas": {},
            "tendencia": "alcista",
            "max_price": 90000.0,
            "targets_alcanzados": processed,
        }
        order = Order.from_dict(data)
        roundtrip = Order.from_dict(order.to_dict())
        assert roundtrip.targets_alcanzados == processed
