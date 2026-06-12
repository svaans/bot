"""Tests para los módulos de robo-advisor: DCA engine y Portfolio Rebalancer."""
from __future__ import annotations

import json
from datetime import datetime, timezone, timedelta
from unittest.mock import patch

import pytest


# ── DCA Engine tests ─────────────────────────────────────────────────────────

@pytest.fixture(autouse=True)
def tmp_dca_state(tmp_path):
    state_file = tmp_path / "dca_state.json"
    with patch("core.portfolio.dca_engine._STATE_PATH", state_file):
        yield state_file




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















