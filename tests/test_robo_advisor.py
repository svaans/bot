"""Tests para los módulos de robo-advisor: DCA engine y Portfolio Rebalancer."""
from __future__ import annotations

import json
import tempfile
from datetime import datetime, timezone, timedelta
from pathlib import Path
from unittest.mock import patch

import pytest


# ── DCA Engine tests ─────────────────────────────────────────────────────────

@pytest.fixture(autouse=True)
def tmp_dca_state(tmp_path):
    state_file = tmp_path / "dca_state.json"
    with patch("core.portfolio.dca_engine._STATE_PATH", state_file):
        yield state_file


@pytest.fixture()
def tmp_target(tmp_path):
    target_file = tmp_path / "portfolio_target.json"
    with patch("core.portfolio.rebalancer._TARGET_PATH", target_file):
        yield target_file


def _config(dca_enabled=True, interval=7):
    class C:
        def get(self, k, d=None):
            return {"dca_enabled": dca_enabled, "dca_interval_days": interval}.get(k, d)
        dca_enabled = True
        dca_interval_days = interval
    c = C()
    c.dca_enabled = dca_enabled
    c.dca_interval_days = interval
    return c


def test_dca_primer_vez_permite():
    from core.portfolio.dca_engine import dca_permite_entrada
    assert dca_permite_entrada("BTC/USDT", _config()) is True


def test_dca_desactivado_bloquea():
    from core.portfolio.dca_engine import dca_permite_entrada
    assert dca_permite_entrada("BTC/USDT", _config(dca_enabled=False)) is False


def test_dca_reciente_bloquea():
    from core.portfolio.dca_engine import dca_permite_entrada, registrar_dca_ejecutado
    registrar_dca_ejecutado("BTC/USDT")
    assert dca_permite_entrada("BTC/USDT", _config(interval=7)) is False


def test_dca_expirado_permite():
    from core.portfolio.dca_engine import dca_permite_entrada, _STATE_PATH
    # Inyectar fecha hace 8 días
    hace_8_dias = (datetime.now(timezone.utc) - timedelta(days=8)).isoformat()
    _STATE_PATH.write_text(json.dumps({"BTC/USDT": hace_8_dias}), encoding="utf-8")
    assert dca_permite_entrada("BTC/USDT", _config(interval=7)) is True


def test_dca_registrar_y_verificar():
    from core.portfolio.dca_engine import registrar_dca_ejecutado, dias_hasta_proximo_dca
    registrar_dca_ejecutado("ETH/USDT")
    assert dias_hasta_proximo_dca("ETH/USDT", interval_days=7) > 0


def test_dca_simbolos_independientes():
    from core.portfolio.dca_engine import dca_permite_entrada, registrar_dca_ejecutado
    registrar_dca_ejecutado("BTC/USDT")
    # ETH no fue DCA'd → debe permitir
    assert dca_permite_entrada("ETH/USDT", _config()) is True


# ── Rebalancer tests ──────────────────────────────────────────────────────────

def test_calcular_drift_igual_peso():
    from core.portfolio.rebalancer import calcular_drift
    valores = {"BTC/USDT": 500.0, "ETH/USDT": 500.0}
    pesos = {"BTC/USDT": 0.5, "ETH/USDT": 0.5}
    drift = calcular_drift(valores, pesos)
    assert abs(drift["BTC/USDT"]) < 0.001
    assert abs(drift["ETH/USDT"]) < 0.001


def test_calcular_drift_sobrepondero():
    from core.portfolio.rebalancer import calcular_drift
    valores = {"BTC/USDT": 700.0, "ETH/USDT": 300.0}
    pesos = {"BTC/USDT": 0.5, "ETH/USDT": 0.5}
    drift = calcular_drift(valores, pesos)
    assert drift["BTC/USDT"] > 0   # BTC sobrepondero
    assert drift["ETH/USDT"] < 0   # ETH subpondero


def test_necesita_rebalanceo_bajo_umbral():
    from core.portfolio.rebalancer import necesita_rebalanceo
    valores = {"BTC/USDT": 510.0, "ETH/USDT": 490.0}
    pesos = {"BTC/USDT": 0.5, "ETH/USDT": 0.5}
    assert necesita_rebalanceo(valores, pesos, umbral=0.05) is False


def test_necesita_rebalanceo_sobre_umbral():
    from core.portfolio.rebalancer import necesita_rebalanceo
    valores = {"BTC/USDT": 700.0, "ETH/USDT": 300.0}
    pesos = {"BTC/USDT": 0.5, "ETH/USDT": 0.5}
    assert necesita_rebalanceo(valores, pesos, umbral=0.05) is True


def test_generar_acciones_rebalanceo():
    from core.portfolio.rebalancer import generar_acciones_rebalanceo
    valores = {"BTC/USDT": 700.0, "ETH/USDT": 300.0}
    pesos = {"BTC/USDT": 0.5, "ETH/USDT": 0.5}
    acciones = generar_acciones_rebalanceo(valores, pesos, umbral=0.05)
    assert len(acciones) == 2
    btc_accion = next(a for a in acciones if a["symbol"] == "BTC/USDT")
    eth_accion = next(a for a in acciones if a["symbol"] == "ETH/USDT")
    assert btc_accion["accion"] == "reducir"
    assert eth_accion["accion"] == "ampliar"


def test_generar_acciones_sin_drift():
    from core.portfolio.rebalancer import generar_acciones_rebalanceo
    valores = {"BTC/USDT": 500.0, "ETH/USDT": 500.0}
    pesos = {"BTC/USDT": 0.5, "ETH/USDT": 0.5}
    acciones = generar_acciones_rebalanceo(valores, pesos, umbral=0.05)
    assert acciones == []


def test_guardar_cargar_pesos_objetivo(tmp_target):
    from core.portfolio.rebalancer import guardar_pesos_objetivo, cargar_pesos_objetivo
    pesos = {"BTC/USDT": 0.30, "ETH/USDT": 0.25, "SOL/USDT": 0.20,
             "XRP/USDT": 0.15, "AVAX/USDT": 0.10}
    guardar_pesos_objetivo(pesos)
    cargados = cargar_pesos_objetivo()
    assert abs(cargados["BTC/USDT"] - 0.30) < 0.001


def test_resumen_cartera():
    from core.portfolio.rebalancer import resumen_cartera
    valores = {"BTC/USDT": 600.0, "ETH/USDT": 400.0}
    pesos = {"BTC/USDT": 0.5, "ETH/USDT": 0.5}
    resumen = resumen_cartera(valores, pesos)
    assert "BTC/USDT" in resumen
    assert "ETH/USDT" in resumen
