"""Funciones de indicadores con soporte incremental por símbolo."""

from __future__ import annotations

from indicators.incremental import (
    actualizar_rsi_incremental,
    actualizar_momentum_incremental,
    actualizar_atr_incremental,
)


def get_rsi(estado, periodo: int = 14):
    """Devuelve el RSI actualizando solo con la última vela."""

    return actualizar_rsi_incremental(estado, periodo)


def get_momentum(estado, periodo: int = 10):
    """Devuelve el *momentum* actualizando solo con la última vela."""

    return actualizar_momentum_incremental(estado, periodo)


def get_atr(estado, periodo: int = 14):
    """Devuelve el ATR actualizando solo con la última vela."""

    return actualizar_atr_incremental(estado, periodo)