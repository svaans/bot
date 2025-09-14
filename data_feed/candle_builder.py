"""Utilidades para construir velas y recuperar históricos."""
from __future__ import annotations

import asyncio
from collections import deque
from typing import Deque, Dict, List

import pandas as pd

from binance_api.cliente import obtener_cliente, fetch_ohlcv_async
from config.config import INTERVALO_VELAS, BACKFILL_MAX_CANDLES
from core.utils.utils import configurar_logger
from data_feed.persistencia_velas import PersistenciaVelas

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
    df = df.sort_values("timestamp").drop_duplicates(subset=["timestamp"])
    df["is_closed"] = True
    return df.to_dict(orient="records")


class CandleBuilder:
    """Mantiene una ventana de velas con persistencia.

    La clase permite restaurar velas desde almacenamiento persistente al iniciar o
    tras una reconexión antes de procesar nuevos ticks.
    """

    def __init__(
        self,
        symbol: str,
        ventana: int,
        persistencia: PersistenciaVelas | None = None,
    ) -> None:
        self.symbol = symbol
        self.ventana = ventana
        self.persistencia = persistencia or PersistenciaVelas()
        self.buffer: Deque[Dict[str, float]] = deque(maxlen=ventana)

    async def restaurar(self) -> None:
        """Rellena el buffer con las velas persistidas."""
        velas = await asyncio.to_thread(
            self.persistencia.cargar_ultimas, self.symbol, self.ventana
        )
        for vela in velas:
            self.buffer.append(vela)

    async def agregar_candle(self, candle: Dict[str, float]) -> None:
        """Agrega una vela cerrada y la persiste."""
        self.buffer.append(candle)
        await asyncio.to_thread(
            self.persistencia.guardar_candle, self.symbol, candle, self.ventana
        )

    def ventana_actual(self) -> List[Dict[str, float]]:
        """Obtiene la ventana actual de velas."""
        return list(self.buffer)