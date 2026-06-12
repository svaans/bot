"""Tests para core/risk/per_symbol_guard.py"""
from __future__ import annotations

import json
from unittest.mock import patch

import pytest


# Redirigir el path de estado a un directorio temporal
@pytest.fixture(autouse=True)
def tmp_state(tmp_path):
    state_file = tmp_path / "per_symbol_losses.json"
    with patch("core.risk.per_symbol_guard._STATE_PATH", state_file):
        yield state_file


def test_factor_reduccion_sin_perdidas():
    from core.risk.per_symbol_guard import factor_reduccion_simbolo
    assert factor_reduccion_simbolo("BTC/USDT", umbral=2) == 1.0


def test_factor_reduccion_bajo_umbral():
    from core.risk.per_symbol_guard import factor_reduccion_simbolo, registrar_resultado
    registrar_resultado("BTC/USDT", ganador=False)
    assert factor_reduccion_simbolo("BTC/USDT", umbral=2) == 1.0


def test_factor_reduccion_sobre_umbral():
    from core.risk.per_symbol_guard import factor_reduccion_simbolo, registrar_resultado
    registrar_resultado("BTC/USDT", ganador=False)
    registrar_resultado("BTC/USDT", ganador=False)
    assert factor_reduccion_simbolo("BTC/USDT", umbral=2, factor=0.5) == 0.5


def test_reset_tras_ganador():
    from core.risk.per_symbol_guard import (
        factor_reduccion_simbolo,
        obtener_racha,
        registrar_resultado,
    )
    registrar_resultado("ETH/USDT", ganador=False)
    registrar_resultado("ETH/USDT", ganador=False)
    assert obtener_racha("ETH/USDT") == 2
    registrar_resultado("ETH/USDT", ganador=True)
    assert obtener_racha("ETH/USDT") == 0
    assert factor_reduccion_simbolo("ETH/USDT", umbral=2) == 1.0


def test_simbolos_independientes():
    from core.risk.per_symbol_guard import factor_reduccion_simbolo, registrar_resultado
    registrar_resultado("SOL/USDT", ganador=False)
    registrar_resultado("SOL/USDT", ganador=False)
    # BTC/USDT sin pérdidas: no debe reducirse
    assert factor_reduccion_simbolo("BTC/USDT", umbral=2) == 1.0
    # SOL/USDT con 2 pérdidas: debe reducirse
    assert factor_reduccion_simbolo("SOL/USDT", umbral=2, factor=0.5) == 0.5


def test_persistencia_entre_llamadas(tmp_state):
    from core.risk.per_symbol_guard import obtener_racha, registrar_resultado
    registrar_resultado("XRP/USDT", ganador=False)
    registrar_resultado("XRP/USDT", ganador=False)
    # El archivo debe haberse escrito
    assert tmp_state.exists()
    data = json.loads(tmp_state.read_text())
    assert data.get("XRP/USDT", 0) == 2
    # Segunda lectura (simula restart del proceso)
    assert obtener_racha("XRP/USDT") == 2


def test_resetear_simbolo():
    from core.risk.per_symbol_guard import obtener_racha, registrar_resultado, resetear_simbolo
    registrar_resultado("AVAX/USDT", ganador=False)
    registrar_resultado("AVAX/USDT", ganador=False)
    resetear_simbolo("AVAX/USDT")
    assert obtener_racha("AVAX/USDT") == 0
