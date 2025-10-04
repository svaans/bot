"""Cliente asíncrono mínimo para simular respuestas de Binance."""
from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional

__all__ = [
    "BinanceClient",
    "crear_cliente",
    "obtener_cliente",
    "fetch_ohlcv_async",
    "fetch_balance_async",
    "fetch_ticker_async",
]

UTC = timezone.utc


@dataclass(slots=True)
class BinanceClient:
    """Representa un cliente Binance simulado.

    Parameters
    ----------
    api_key:
        Clave pública para autenticación (opcional en modo simulado).
    api_secret:
        Clave privada para autenticación (opcional en modo simulado).
    testnet:
        Indica si la conexión se realiza contra el entorno de pruebas.
    """

    api_key: str | None = None
    api_secret: str | None = None
    testnet: bool = False

    def as_dict(self) -> Dict[str, Any]:
        """Serializa el cliente a un diccionario sencillo."""

        return {
            "api_key": self.api_key or "",  # Evita ``None`` en logs
            "testnet": self.testnet,
        }


def crear_cliente(api_key: str | None = None, api_secret: str | None = None, *, testnet: bool | None = None) -> BinanceClient:
    """Crea un :class:`BinanceClient` simulado.

    En entornos de desarrollo devolvemos un cliente en memoria que cumple con la
    interfaz esperada por el resto del bot.
    """

    return BinanceClient(api_key=api_key, api_secret=api_secret, testnet=bool(testnet))


def obtener_cliente(*, api_key: str | None = None, api_secret: str | None = None, testnet: bool | None = None) -> BinanceClient:
    """Obtiene (o crea) un cliente simulado.

    Se mantiene para compatibilidad con módulos que llamaban a esta función.
    """

    return crear_cliente(api_key, api_secret, testnet=testnet)


_INTERVAL_MAP = {
    "1m": timedelta(minutes=1),
    "3m": timedelta(minutes=3),
    "5m": timedelta(minutes=5),
    "15m": timedelta(minutes=15),
    "30m": timedelta(minutes=30),
    "1h": timedelta(hours=1),
    "2h": timedelta(hours=2),
    "4h": timedelta(hours=4),
    "6h": timedelta(hours=6),
    "8h": timedelta(hours=8),
    "12h": timedelta(hours=12),
    "1d": timedelta(days=1),
}


def _intervalo_ms(intervalo: str) -> int:
    delta = _INTERVAL_MAP.get(intervalo)
    if not delta:
        raise ValueError(f"Intervalo no soportado en modo simulado: {intervalo}")
    return int(delta.total_seconds() * 1000)


async def fetch_ohlcv_async(
    cliente: BinanceClient | None,
    symbol: str,
    intervalo: str,
    *,
    since: int | None = None,
    limit: int = 500,
) -> List[List[float]]:
    """Genera una lista determinista de velas OHLCV.

    La data se produce en memoria de forma reproducible para facilitar tests.
    """

    await asyncio.sleep(0)  # garantiza punto de conmutación
    limit = max(1, min(int(limit or 500), 1500))
    interval_ms = _intervalo_ms(intervalo)
    if since is None:
        now_ms = int(datetime.now(UTC).timestamp() * 1000)
        since = now_ms - limit * interval_ms
    else:
        since = int(since)

    candles: List[List[float]] = []
    base_price = 100.0
    for i in range(limit):
        open_ts = since + i * interval_ms
        close_price = base_price + (i % 10) * 0.1
        high_price = close_price + 0.2
        low_price = close_price - 0.2
        volume = 50.0 + (i % 5) * 1.5
        candles.append([
            float(open_ts),
            round(close_price - 0.1, 6),
            round(high_price, 6),
            round(low_price, 6),
            round(close_price, 6),
            round(volume, 6),
        ])
    return candles


async def fetch_balance_async(cliente: BinanceClient | None) -> Dict[str, Dict[str, float]]:
    """Devuelve un balance simulado organizado por moneda."""

    await asyncio.sleep(0)
    return {
        "total": {
            "USDT": 1000.0,
            "BUSD": 0.0,
        },
        "free": {
            "USDT": 1000.0,
            "BUSD": 0.0,
        },
    }


async def fetch_ticker_async(cliente: BinanceClient | None, symbol: str) -> Dict[str, float]:
    """Ticker simple usado en pruebas del gestor de órdenes."""

    await asyncio.sleep(0)
    base_price = 100.0
    offset = abs(hash(symbol)) % 1000 / 100.0
    return {"last": base_price + offset}