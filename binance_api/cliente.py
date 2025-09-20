"""
Binance Client Lite — cliente mínimo y estable sobre ccxt

Objetivo
--------
- Ofrecer llamadas **asíncronas** a ccxt con reintentos sencillos y sin dependencias
  cruzadas (métricas, circuit breakers globales, user-streams, etc.).
- Mantener la interfaz que ya usa tu bot: `crear_cliente(...)` y helpers `*_async`.

Qué incluye
-----------
- crear_cliente(config: Optional[Config]) -> exchange  (ccxt.binance)
- BinanceClient (envoltorio async opcional)
- Funciones async: fetch_ohlcv_async, fetch_ticker_async, fetch_order_book_async,
  fetch_balance_async, create_order_async, create_market_buy/sell_order_async,
  fetch_open_orders_async, load_markets_async
- filtrar_simbolos_activos(symbols, config) -> (activos, inactivos)

Qué **no** incluye (para simplificar)
-------------------------------------
- Circuit breaker global por endpoint, tracking de weight/métricas, ni user data stream.
- Si necesitas user stream más adelante, lo añadimos aquí como módulo aparte.
"""
from __future__ import annotations
import asyncio
import functools
import os
import random
import time
from typing import Any, Callable, Optional, Tuple

import ccxt  # type: ignore

try:
    from core.utils.utils import configurar_logger
except Exception:  # pragma: no cover
    import logging
    def configurar_logger(name: str):
        logging.basicConfig(level=logging.INFO)
        return logging.getLogger(name)

try:
    from config.config_manager import Config  # type: ignore
except Exception:  # pragma: no cover
    class Config:  # fallback mínimo
        modo_real: bool = True


log = configurar_logger("binance_client_lite")


# ----------------- Backoff sencillo (sin CB global) -----------------
class BinanceCallError(Exception):
    pass


def _retry_sync(fn: Callable[[], Any], *, max_attempts: int = 5) -> Any:
    """Ejecuta `fn` con reintentos exponenciales para 5xx/429/-1003/-1021.
    No incluye métricas ni circuit breaker, sólo backoff con jitter.
    """
    attempt = 1
    delay = 0.5
    jitter = 0.2
    time_synced = False
    while True:
        try:
            return fn()
        except Exception as exc:  # pragma: no cover - dependiente de red
            # Intentar detectar códigos conocidos
            code = getattr(exc, "code", None)
            status = getattr(exc, "http_status", None) or getattr(exc, "status", None)
            # Desfase horario -1021: probar a ajustar y reintentar una vez
            if code == -1021 and not time_synced:
                exchange = getattr(fn, "__self__", None)
                if exchange and hasattr(exchange, "load_time_difference"):
                    try:
                        exchange.load_time_difference()
                        time_synced = True
                        attempt += 1
                        continue
                    except Exception:
                        pass
            # Filtros: no reintentar en errores de parámetros conocidos
            if code in (-1013, -1100, -1102, -1130):
                raise
            # Rate limit / 5xx → reintentar
            retriable = (code in (-1003,) or (isinstance(status, int) and (status == 429 or 500 <= status < 600)))
            if not retriable or attempt >= max_attempts:
                raise
            sleep_for = delay + random.random() * jitter
            log.debug(f"binance retry {attempt}/{max_attempts} in {sleep_for:.2f}s")
            time.sleep(sleep_for)  # sync; se ejecuta en executor
            attempt += 1
            delay = min(delay * 2, 5.0)


# ----------------- Cliente y helpers async -----------------
class BinanceClient:
    """Wrapper asíncrono mínimo sobre un exchange ccxt.binance."""

    def __init__(self, config: Optional[Config] = None) -> None:
        self.exchange = crear_cliente(config)
        self.authenticated = bool(getattr(self.exchange, "apiKey", None) and getattr(self.exchange, "secret", None))
        self.modo_real = getattr(self.exchange, "_modo_real", True)

    async def execute(self, func: Callable[..., Any], *args, **kwargs) -> Any:
        loop = asyncio.get_running_loop()
        parcial = functools.partial(func, *args, **kwargs)
        # Reintentos dentro del executor
        caller = functools.partial(_retry_sync, lambda: parcial())
        return await loop.run_in_executor(None, caller)

    # Métodos conveniencia (compat con tu código existente)
    async def fetch_balance(self, *args, **kwargs):
        return await self.execute(self.exchange.fetch_balance, *args, **kwargs)

    async def create_order(self, *args, **kwargs):
        return await self.execute(self.exchange.create_order, *args, **kwargs)

    async def create_market_buy_order(self, *args, **kwargs):
        return await self.execute(self.exchange.create_market_buy_order, *args, **kwargs)

    async def create_market_sell_order(self, *args, **kwargs):
        return await self.execute(self.exchange.create_market_sell_order, *args, **kwargs)

    async def fetch_open_orders(self, *args, **kwargs):
        return await self.execute(self.exchange.fetch_open_orders, *args, **kwargs)

    async def fetch_ticker(self, *args, **kwargs):
        return await self.execute(self.exchange.fetch_ticker, *args, **kwargs)

    async def fetch_order_book(self, *args, **kwargs):
        return await self.execute(self.exchange.fetch_order_book, *args, **kwargs)

    async def load_markets(self, *args, **kwargs):
        return await self.execute(self.exchange.load_markets, *args, **kwargs)

    async def fetch_ohlcv(self, *args, **kwargs):
        return await self.execute(self.exchange.fetch_ohlcv, *args, **kwargs)


def crear_cliente(config: Optional[Config] = None):
    modo_real = True if config is None else getattr(config, "modo_real", True)
    api_key = os.getenv("BINANCE_API_KEY")
    api_secret = os.getenv("BINANCE_SECRET") or os.getenv("BINANCE_API_SECRET")
    base = os.getenv("BINANCE_BASE")
    testnet = os.getenv("BINANCE_TESTNET", "false").lower() == "true"
    default_type = os.getenv("BINANCE_DEFAULT_TYPE", "spot")

    exchange = ccxt.binance(
        {
            "apiKey": api_key,
            "secret": api_secret,
            "enableRateLimit": True,
            "options": {
                "defaultType": default_type,
                "adjustForTimeDifference": True,
                "warnOnFetchOpenOrdersWithoutSymbol": False,
            },
        }
    )
    if base:
        try:
            exchange.urls["api"] = base
        except Exception:
            pass
    exchange.set_sandbox_mode(testnet or not modo_real)
    try:  # no crítico
        exchange.load_time_difference()
    except Exception:
        pass
    exchange._modo_real = modo_real

    # Envolver métodos principales con retry sync (sigue siendo síncrono aquí)
    def _wrap(name: str) -> None:
        original = getattr(exchange, name, None)
        if not callable(original):
            return
        def caller(*args, **kwargs):
            return _retry_sync(lambda: original(*args, **kwargs))
        setattr(exchange, name, caller)

    for nombre in (
        "fetch_balance",
        "create_order",
        "create_market_buy_order",
        "create_market_sell_order",
        "fetch_open_orders",
        "fetch_ticker",
        "load_markets",
        "fetch_ohlcv",
        "fetch_order_book",
        "fetch_my_trades",
        "cancel_order",
        "fetch_order",
    ):
        _wrap(nombre)

    return exchange


_cliente_cache: Any | None = None


def obtener_cliente(config: Optional[Config] = None):
    """Devuelve una instancia reutilizable del cliente de Binance."""

    global _cliente_cache
    if _cliente_cache is None:
        _cliente_cache = crear_cliente(config)
    return _cliente_cache


# --------- Helpers asíncronos directos (aceptan BinanceClient o ccxt) ---------
async def _exec_async(cliente, func_name: str, *args, **kwargs):
    if isinstance(cliente, BinanceClient):
        method = getattr(cliente, func_name)
        return await method(*args, **kwargs)
    loop = asyncio.get_running_loop()
    func = getattr(cliente, func_name)
    caller = functools.partial(_retry_sync, lambda: func(*args, **kwargs))
    return await loop.run_in_executor(None, caller)


async def fetch_balance_async(cliente, *args, **kwargs):
    # Si no hay claves, retorna balance ficticio
    if not getattr(cliente, "apiKey", None) and not isinstance(cliente, BinanceClient):
        return {"total": {"EUR": 1000.0}, "free": {"EUR": 1000.0}}
    return await _exec_async(cliente, "fetch_balance", *args, **kwargs)


async def create_order_async(cliente, *args, **kwargs):
    return await _exec_async(cliente, "create_order", *args, **kwargs)


async def create_market_buy_order_async(cliente, *args, **kwargs):
    return await _exec_async(cliente, "create_market_buy_order", *args, **kwargs)


async def create_market_sell_order_async(cliente, *args, **kwargs):
    return await _exec_async(cliente, "create_market_sell_order", *args, **kwargs)


async def fetch_open_orders_async(cliente, *args, **kwargs):
    return await _exec_async(cliente, "fetch_open_orders", *args, **kwargs)


async def fetch_ticker_async(cliente, *args, **kwargs):
    return await _exec_async(cliente, "fetch_ticker", *args, **kwargs)


async def fetch_order_book_async(cliente, *args, **kwargs):
    return await _exec_async(cliente, "fetch_order_book", *args, **kwargs)


async def load_markets_async(cliente, *args, **kwargs):
    return await _exec_async(cliente, "load_markets", *args, **kwargs)


async def fetch_ohlcv_async(cliente, *args, **kwargs):
    return await _exec_async(cliente, "fetch_ohlcv", *args, **kwargs)


def filtrar_simbolos_activos(symbols: list[str], config: Optional[Config] = None) -> Tuple[list[str], list[str]]:
    """Devuelve (activos, inactivos). Si falla la consulta, asume todos activos."""
    try:
        cliente = crear_cliente(config)
        mercados = cliente.load_markets()
    except Exception:
        return symbols, []
    activos: list[str] = []
    inactivos: list[str] = []
    for sym in symbols:
        info = mercados.get(sym)
        if not info or not info.get("active", True):
            inactivos.append(sym)
        else:
            activos.append(sym)
    return activos, inactivos


