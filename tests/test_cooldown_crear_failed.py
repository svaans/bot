"""Cooldown tras rechazo lógico de OrderManager.crear (crear_failed)."""

from __future__ import annotations

import types
from datetime import datetime, timezone

from core.procesar_vela import _resolve_entrada_cooldown_tras_crear_failed_sec
from tests.test_trader_logic import _build_trader

UTC = timezone.utc


def test_resolve_entrada_cooldown_override_por_simbolo() -> None:
    trader = types.SimpleNamespace(
        config=types.SimpleNamespace(
            entrada_cooldown_tras_crear_failed_sec=10.0,
            entrada_cooldown_tras_crear_failed_por_symbol={"BTC/EUR": 99.0},
        )
    )
    assert _resolve_entrada_cooldown_tras_crear_failed_sec(trader, "btc/eur") == 99.0
    assert _resolve_entrada_cooldown_tras_crear_failed_sec(trader, "ETH/EUR") == 10.0


def test_resolve_entrada_cooldown_sin_config() -> None:
    assert _resolve_entrada_cooldown_tras_crear_failed_sec(types.SimpleNamespace(), "X") == 0.0


def test_registrar_cooldown_entrada_escribe_ventana_futura() -> None:
    trader = _build_trader()
    trader.registrar_cooldown_entrada("ABC/XYZ", 120.0)
    assert "ABC/XYZ" in trader._entrada_cooldowns
    assert trader._entrada_cooldowns["ABC/XYZ"] > datetime.now(UTC)


def test_registrar_cooldown_entrada_bloquea_puede_evaluar_sin_capital_ni_riesgo() -> None:
    trader = _build_trader()
    trader.capital_manager = None
    trader.risk = None
    assert trader._puede_evaluar_entradas("BTCUSDT") is True
    trader.registrar_cooldown_entrada("BTCUSDT", 3600.0)
    assert trader._puede_evaluar_entradas("BTCUSDT") is False
