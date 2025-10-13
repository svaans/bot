"""Cliente asíncrono para interactuar con la API spot de Binance.

El módulo expone una interfaz única que puede operar en dos modos:

* **simulado**: genera datos deterministas en memoria para tests y entornos
  sin conexión.
* **real**: utiliza la API REST oficial de Binance (entorno live o testnet)
  usando ``aiohttp`` y firma HMAC para endpoints privados.

El modo por defecto es el simulado para conservar compatibilidad con la suite
de tests existente. Para activar el modo real se puede:

* Pasar ``simulated=False`` a :func:`crear_cliente`.
* Establecer la variable de entorno ``BINANCE_SIMULATED=0``.

Ambos modos comparten la misma firma pública, por lo que el resto del bot
permanece desacoplado del origen de los datos.
"""
from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Mapping, MutableMapping
from urllib.parse import urlencode

import aiohttp

__all__ = [
    "BinanceClient",
    "BinanceAPIError",
    "crear_cliente",
    "obtener_cliente",
    "fetch_ohlcv_async",
    "fetch_balance_async",
    "fetch_ticker_async",
]

logger = logging.getLogger(__name__)
UTC = timezone.utc

# ──────────────────────────────── CONSTANTES ────────────────────────────────
_SPOT_BASE_URL = "https://api.binance.com"
_SPOT_TESTNET_BASE_URL = "https://testnet.binance.vision"
_DEFAULT_TIMEOUT = aiohttp.ClientTimeout(total=15)


class BinanceAPIError(RuntimeError):
    """Error genérico asociado a respuestas inválidas de Binance."""


@dataclass(slots=True)
class BinanceClient:
    """Cliente ligero reutilizable para la API spot de Binance.

    Parameters
    ----------
    api_key:
        Clave pública para firmar peticiones privadas. Opcional si solo se
        consumen endpoints públicos.
    api_secret:
        Clave privada utilizada en la firma HMAC SHA256 de las peticiones
        autenticadas.
    testnet:
        Si ``True`` las peticiones apuntan al entorno de pruebas oficial.
    simulated:
        Controla si el cliente opera en modo offline. Por defecto toma el valor
        de la variable de entorno ``BINANCE_SIMULATED`` (``"1"`` si no existe).
    request_timeout:
        ``aiohttp.ClientTimeout`` configurable para ajustar límites de tiempo
        de las peticiones REST.
    """

    api_key: str | None = None
    api_secret: str | None = None
    testnet: bool = False
    simulated: bool | None = None
    request_timeout: aiohttp.ClientTimeout = _DEFAULT_TIMEOUT
    _session: aiohttp.ClientSession | None = field(default=None, init=False, repr=False)
    _session_lock: asyncio.Lock | None = field(default=None, init=False, repr=False)

    def __post_init__(self) -> None:  # pragma: no cover - rama trivial
        if self.simulated is None:
            env_value = os.getenv("BINANCE_SIMULATED", "1").strip().lower()
            self.simulated = env_value not in {"0", "false", "no"}

    # ───────────────────────────── PUBLIC API ─────────────────────────────

    def as_dict(self) -> Dict[str, Any]:
        """Serializa los datos del cliente para logging estructurado."""

        return {
            "api_key": bool(self.api_key),
            "testnet": self.testnet,
            "simulated": bool(self.simulated),
        }


    @property
    def rest_base_url(self) -> str:
        """Devuelve la URL base correspondiente al entorno configurado."""

        return _SPOT_TESTNET_BASE_URL if self.testnet else _SPOT_BASE_URL

    async def close(self) -> None:
        """Cierra la sesión HTTP reutilizada por el cliente."""

        if self._session is not None and not self._session.closed:
            await self._session.close()
        self._session = None

    # ────────────────────────── MÉTODOS INTERNOS ─────────────────────────
    async def _ensure_session(self) -> aiohttp.ClientSession:
        if self._session is not None and not self._session.closed:
            return self._session
        if self._session_lock is None:
            self._session_lock = asyncio.Lock()
        async with self._session_lock:
            if self._session is not None and not self._session.closed:
                return self._session
            self._session = aiohttp.ClientSession(timeout=self.request_timeout)
            return self._session

    async def _request(
        self,
        method: str,
        path: str,
        *,
        params: Mapping[str, Any] | None = None,
        signed: bool = False,
    ) -> Any:
        session = await self._ensure_session()
        params = dict(params or {})
        headers: MutableMapping[str, str] = {}
        if signed:
            if not self.api_key or not self.api_secret:
                raise BinanceAPIError("Se requieren credenciales para peticiones firmadas")
            params.setdefault("timestamp", int(time.time() * 1000))
            query = urlencode(sorted((k, v) for k, v in params.items()))
            signature = hmac.new(
                self.api_secret.encode(), query.encode(), hashlib.sha256
            ).hexdigest()
            params["signature"] = signature
            headers["X-MBX-APIKEY"] = self.api_key
        elif self.api_key:
            headers["X-MBX-APIKEY"] = self.api_key

        url = f"{self.rest_base_url}{path}"
        try:
            async with session.request(method, url, params=params, headers=headers) as resp:
                text = await resp.text()
                if resp.status != 200:
                    logger.error(
                        "binance_rest_error",
                        extra={
                            "event": "binance_rest_error",
                            "status": resp.status,
                            "url": path,
                            "body": text,
                        },
                    )
                    raise BinanceAPIError(f"Respuesta {resp.status} desde Binance: {text[:200]}")
                if text:
                    return json.loads(text)
                return None
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # pragma: no cover - errores de red
            logger.error(
                "binance_rest_exception",
                extra={"event": "binance_rest_exception", "error": repr(exc), "url": path},
            )
            raise


def crear_cliente(
    api_key: str | None = None,
    api_secret: str | None = None,
    *,
    testnet: bool | None = None,
    simulated: bool | None = None,
    request_timeout: aiohttp.ClientTimeout | None = None,
) -> BinanceClient:
    """Crea y devuelve un :class:`BinanceClient` configurado."""

    return BinanceClient(
        api_key=api_key,
        api_secret=api_secret,
        testnet=bool(testnet),
        simulated=simulated,
        request_timeout=request_timeout or _DEFAULT_TIMEOUT,
    )


def obtener_cliente(
    *,
    api_key: str | None = None,
    api_secret: str | None = None,
    testnet: bool | None = None,
    simulated: bool | None = None,
    request_timeout: aiohttp.ClientTimeout | None = None,
) -> BinanceClient:
    """Alias de :func:`crear_cliente` mantenido por compatibilidad."""

    return crear_cliente(
        api_key=api_key,
        api_secret=api_secret,
        testnet=testnet,
        simulated=simulated,
        request_timeout=request_timeout,
    )


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
        raise ValueError(f"Intervalo no soportado: {intervalo}")
    return int(delta.total_seconds() * 1000)


async def fetch_ohlcv_async(
    cliente: BinanceClient | None,
    symbol: str,
    intervalo: str,
    *,
    since: int | None = None,
    limit: int = 500,
) -> List[List[float]]:
    """Obtiene velas OHLCV ya sea simuladas o desde Binance."""

    if cliente is None or getattr(cliente, "simulated", True):
        return await _fetch_ohlcv_simulado(symbol, intervalo, since=since, limit=limit)
    return await _fetch_ohlcv_real(cliente, symbol, intervalo, since=since, limit=limit)


async def fetch_balance_async(cliente: BinanceClient | None) -> Dict[str, Dict[str, float]]:
    """Recupera el balance del usuario o un stub determinista."""

    if cliente is None or getattr(cliente, "simulated", True):
        return await _fetch_balance_simulado()
    return await _fetch_balance_real(cliente)


async def fetch_ticker_async(cliente: BinanceClient | None, symbol: str) -> Dict[str, float]:
    """Obtiene el último precio negociado para ``symbol``."""

    if cliente is None or getattr(cliente, "simulated", True):
        return await _fetch_ticker_simulado(symbol)
    return await _fetch_ticker_real(cliente, symbol)

# ────────────────────────────── MODO SIMULADO ─────────────────────────────
async def _fetch_ohlcv_simulado(
    symbol: str,
    intervalo: str,
    *,
    since: int | None,
    limit: int,
) -> List[List[float]]:
    del symbol  # se mantiene para compatibilidad de firma
    await asyncio.sleep(0)
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
        candles.append(
            [
                float(open_ts),
                round(close_price - 0.1, 6),
                round(high_price, 6),
                round(low_price, 6),
                round(close_price, 6),
                round(volume, 6),
            ]
        )
    return candles


async def _fetch_balance_simulado() -> Dict[str, Dict[str, float]]:

    await asyncio.sleep(0)
    return {
        "total": {"USDT": 1000.0, "BUSD": 0.0},
        "free": {"USDT": 1000.0, "BUSD": 0.0},
    }


async def _fetch_ticker_simulado(symbol: str) -> Dict[str, float]:

    await asyncio.sleep(0)
    base_price = 100.0
    offset = abs(hash(symbol)) % 1000 / 100.0
    return {"last": base_price + offset}


# ──────────────────────────────── MODO REAL ───────────────────────────────
async def _fetch_ohlcv_real(
    cliente: BinanceClient,
    symbol: str,
    intervalo: str,
    *,
    since: int | None,
    limit: int,
) -> List[List[float]]:
    params: Dict[str, Any] = {
        "symbol": symbol.upper(),
        "interval": intervalo,
        "limit": min(max(limit or 500, 1), 1000),
    }
    if since is not None:
        params["startTime"] = int(since)
    data = await cliente._request("GET", "/api/v3/klines", params=params)
    candles: List[List[float]] = []
    for entry in data:
        candles.append(
            [
                float(entry[0]),
                float(entry[1]),
                float(entry[2]),
                float(entry[3]),
                float(entry[4]),
                float(entry[5]),
            ]
        )
    return candles


async def _fetch_balance_real(cliente: BinanceClient) -> Dict[str, Dict[str, float]]:
    data = await cliente._request("GET", "/api/v3/account", signed=True)
    total: Dict[str, float] = {}
    free: Dict[str, float] = {}
    for balance in data.get("balances", []):
        asset = balance.get("asset")
        if not asset:
            continue
        free_amt = float(balance.get("free", 0.0))
        locked_amt = float(balance.get("locked", 0.0))
        total[asset] = round(free_amt + locked_amt, 8)
        free[asset] = round(free_amt, 8)
    return {"total": total, "free": free}


async def _fetch_ticker_real(cliente: BinanceClient, symbol: str) -> Dict[str, float]:
    data = await cliente._request(
        "GET",
        "/api/v3/ticker/price",
        params={"symbol": symbol.upper()},
    )
    price = float(data.get("price", 0.0))
    return {"last": price}


# ──────────────────────────────── HELPERS ────────────────────────────────
async def close_client_session(cliente: BinanceClient | None) -> None:
    """Cierra el ``ClientSession`` asociado si está activo."""

    if isinstance(cliente, BinanceClient):
        await cliente.close()


# Alias para compatibilidad retro
fetch_ohlcv = fetch_ohlcv_async
fetch_balance = fetch_balance_async
fetch_ticker = fetch_ticker_async
__all__.extend(["fetch_ohlcv", "fetch_balance", "fetch_ticker", "close_client_session"])
