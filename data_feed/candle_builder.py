"""Utilidades para construir velas y recuperar históricos."""
from __future__ import annotations

import pandas as pd
from typing import List, Dict

from binance_api.cliente import obtener_cliente, fetch_ohlcv_async
from config.config import INTERVALO_VELAS, BACKFILL_MAX_CANDLES
from core.utils.utils import configurar_logger

log = configurar_logger("candle_builder")


async def backfill(symbol: str, n: int) -> List[Dict[str, float]]:
    """Recupera las últimas ``n`` velas cerradas de ``symbol`` vía REST.

    Las velas se retornan como una lista de diccionarios con las claves
    ``timestamp``, ``open``, ``high``, ``low``, ``close`` y ``volume``.
    """
    limite = max(1, min(n, BACKFILL_MAX_CANDLES))
    cliente = obtener_cliente()
    try:
        ohlcv = await fetch_ohlcv_async(
            cliente, symbol, timeframe=INTERVALO_VELAS, limit=limite
        )
    except Exception as e:  # pragma: no cover - logging de error
        log.warning(f"[{symbol}] Error solicitando backfill: {e}")
        return []
    columnas = ["timestamp", "open", "high", "low", "close", "volume"]
    df = pd.DataFrame(ohlcv, columns=columnas)
    df["timestamp"] = df["timestamp"].astype(int)
    df[["open", "high", "low", "close", "volume"]] = df[
        ["open", "high", "low", "close", "volume"]
    ].astype(float)
    df["is_closed"] = True
    return df.to_dict(orient="records")