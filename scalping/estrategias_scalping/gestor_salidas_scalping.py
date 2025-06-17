"""Gestor de salidas para operaciones de scalping."""

from __future__ import annotations

import pandas as pd
from datetime import datetime, timedelta
from dataclasses import dataclass

from utils_scalping.indicadores import atr
from config_scalping import CONFIGS


@dataclass
class EstadoOperacion:
    symbol: str
    precio_entrada: float
    timestamp: datetime
    max_price: float


def evaluar_salida(df: pd.DataFrame, estado: EstadoOperacion) -> tuple[bool, str]:
    config = CONFIGS.get(estado.symbol)
    if not config:
        return False, "sin_config"

    precio_actual = df["close"].iloc[-1]
    duracion = datetime.utcnow() - estado.timestamp
    atr_actual = atr(df).iloc[-1]
    if atr_actual and config.stop_loss_pct <= 1:
        sl = estado.precio_entrada * (1 - config.stop_loss_pct / 100)
    else:
        sl = estado.precio_entrada - atr_actual
    tp = estado.precio_entrada * (1 + config.take_profit_pct / 100)

    if precio_actual <= sl:
        return True, "stop_loss"
    if precio_actual >= tp:
        return True, "take_profit"

    if precio_actual > estado.max_price:
        estado.max_price = precio_actual
    trailing = estado.max_price * (1 - config.buffer_trailing_pct / 100)
    if precio_actual <= trailing:
        return True, "trailing_stop"

    if duracion >= timedelta(minutes=config.max_duracion_min):
        return True, "timeout"

    return False, "continuar"