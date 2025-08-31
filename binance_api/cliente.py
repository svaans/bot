import os
import asyncio
import functools
import json
import random
import re
import time
from typing import Any, Callable

import ccxt
from config.config_manager import Config
from core.utils.utils import configurar_logger

log = configurar_logger('binance_client')


AUTH_WARNING_EMITTED = False

# Estado del circuit breaker por endpoint
_CIRCUIT_BREAKERS: dict[str, dict[str, float]] = {}


class BinanceError(Exception):
    """Excepción propia con código y sugerencia."""

    def __init__(self, code: int, reason: str, suggestion: str) -> None:
        super().__init__(f"{code}: {reason} - {suggestion}")
        self.code = code
        self.reason = reason
        self.suggestion = suggestion

def _extract_code(exc: Exception) -> int | None:
    code = getattr(exc, "code", None)
    if isinstance(code, int):
        return code
    try:
        data = json.loads(str(exc))
        return int(data.get("code"))
    except Exception:
        match = re.search(r'"code":(-?\d+)', str(exc))
        if match:
            return int(match.group(1))
    return None


def binance_call(
    fn: Callable[[], Any], *, signed: bool = False, endpoint: str | None = None, symbol: str | None = None
) -> Any:
    endpoint = endpoint or getattr(fn, "__name__", "unknown")
    state = _CIRCUIT_BREAKERS.setdefault(endpoint, {"fails": 0, "until": 0.0})
    now = time.time()
    if state["until"] > now:
        exc = RuntimeError("Circuit breaker activo")
        exc.endpoint = endpoint
        exc.symbol = symbol
        exc.signed = signed
        exc.attempts = 0
        log.error(f"Circuit breaker activo para {endpoint}")
        raise exc

    max_attempts = 5
    base = 0.5
    jitter = 0.1
    time_synced = False
    attempt = 1
    while attempt <= max_attempts:
        try:
            result = fn()
            state["fails"] = 0
            state["until"] = 0.0
            return result
        except Exception as exc:
            code = _extract_code(exc)
            status = getattr(exc, "http_status", None)
            is_5xx = isinstance(status, int) and 500 <= status < 600
            if is_5xx:
                state["fails"] += 1
                if state["fails"] > 3:
                    state["until"] = time.time() + 30
            else:
                state["fails"] = 0
            _CIRCUIT_BREAKERS[endpoint] = state

            final = False
            if code == -2015:
                final = True
            elif state["fails"] > 3 or attempt >= max_attempts:
                final = True

            if code == -1021 and not time_synced:
                exchange = getattr(fn, "__self__", None)
                if exchange and hasattr(exchange, "load_time_difference"):
                    try:
                        exchange.load_time_difference()
                    except Exception:
                        pass
                time_synced = True
                attempt += 1
                continue

            exc.endpoint = endpoint
            exc.symbol = symbol
            exc.signed = signed
            exc.attempts = attempt
            if final:
                log.error(f"Error final en {endpoint}: {exc}")
                raise
            log.debug(f"Intento {attempt} fallido en {endpoint}: {exc}")
            espera = base * (2 ** (attempt - 1)) + random.random() * jitter
            time.sleep(espera)
            attempt += 1


def auth_guard(default: Any = None):
    """Bloquea llamadas privadas en modo simulado o sin auth."""

    def decorator(func):
        @functools.wraps(func)
        async def wrapper(self, *args, **kwargs):
            global AUTH_WARNING_EMITTED
            if not self.modo_real or not self.authenticated:
                if not AUTH_WARNING_EMITTED:
                    log.warning(
                        "🔒 AuthGuard: llamadas privadas bloqueadas en modo SIMULADO"
                    )
                    AUTH_WARNING_EMITTED = True
                if isinstance(default, Exception):
                    raise default
                if callable(default):
                    return default()
                return default
            return await func(self, *args, **kwargs)

        return wrapper

    return decorator


class BinanceClient:
    """Wrapper asíncrono mínimo sobre el cliente Binance."""

    def __init__(self, config: Config | None = None) -> None:
        self.exchange = crear_cliente(config)
        self.authenticated = bool(
            getattr(self.exchange, "apiKey", None) and getattr(self.exchange, "secret", None)
        )
        self.modo_real = getattr(self.exchange, "_modo_real", True)

    async def execute(self, func: Callable[..., Any], *args, **kwargs) -> Any:
        loop = asyncio.get_running_loop()
        parcial = functools.partial(func, *args, **kwargs)
        return await loop.run_in_executor(None, parcial)

    @auth_guard(lambda: {'total': {'EUR': 1000.0}, 'free': {'EUR': 1000.0}})
    async def fetch_balance(self, *args, **kwargs):
        return await self.execute(self.exchange.fetch_balance, *args, **kwargs)

    @auth_guard(PermissionError('Auth required'))
    async def create_order(self, *args, **kwargs):
        return await self.execute(self.exchange.create_order, *args, **kwargs)

    @auth_guard(PermissionError('Auth required'))
    async def create_market_buy_order(self, *args, **kwargs):
        return await self.execute(self.exchange.create_market_buy_order, *args, **kwargs)

    @auth_guard(PermissionError('Auth required'))
    async def create_market_sell_order(self, *args, **kwargs):
        return await self.execute(self.exchange.create_market_sell_order, *args, **kwargs)

    @auth_guard(lambda: [])
    async def fetch_open_orders(self, *args, **kwargs):
        return await self.execute(self.exchange.fetch_open_orders, *args, **kwargs)

    async def fetch_ticker(self, *args, **kwargs):
        return await self.execute(self.exchange.fetch_ticker, *args, **kwargs)

    async def load_markets(self, *args, **kwargs):
        return await self.execute(self.exchange.load_markets, *args, **kwargs)

    async def fetch_ohlcv(self, *args, **kwargs):
        return await self.execute(self.exchange.fetch_ohlcv, *args, **kwargs)


def crear_cliente(config: Config | None = None):
    modo_real = True
    if config is not None:
        modo_real = getattr(config, "modo_real", True)
    else:
        modo_real = os.getenv("MODO_REAL", "true").lower() == "true"

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
        exchange.urls["api"] = base
    exchange.set_sandbox_mode(testnet or not modo_real)
    try:
        exchange.load_time_difference()
    except Exception:
        pass
    exchange._modo_real = modo_real

    def _wrap(name: str, signed: bool) -> None:
        original = getattr(exchange, name, None)
        if not callable(original):
            return

        def caller(*args, **kwargs):
            symbol = kwargs.get('symbol')
            if symbol is None and args:
                first = args[0]
                if isinstance(first, str):
                    symbol = first
            return binance_call(lambda: original(*args, **kwargs), signed=signed, endpoint=name, symbol=symbol)

        setattr(exchange, name, caller)

    wrappers = {
        'fetch_balance': True,
        'create_order': True,
        'create_market_buy_order': True,
        'create_market_sell_order': True,
        'fetch_open_orders': True,
        'fetch_ticker': False,
        'load_markets': False,
        'fetch_ohlcv': False,
    }
    for nombre, firmado in wrappers.items():
        _wrap(nombre, firmado)
        
    return exchange


obtener_cliente = crear_cliente


def filtrar_simbolos_activos(symbols: list[str], config: Config | None = None) -> tuple[list[str], list[str]]:
    """Devuelve dos listas con símbolos activos e inactivos en Binance.

    Si ocurre cualquier error al consultar los mercados, se asume que todos
    los símbolos son válidos para no interrumpir la ejecución.
    """
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


async def fetch_balance_async(cliente, *args, **kwargs):
    """
    Obtiene el balance de forma asíncrona.
    Si ``cliente`` es una instancia de ``BinanceClient`` se usan sus
    reintentos. Si no hay claves API (modo simulado), devuelve un balance
    ficticio.
    """
    if isinstance(cliente, BinanceClient):
        return await cliente.fetch_balance(*args, **kwargs)
    if not getattr(cliente, 'apiKey', None) or not getattr(cliente, 'secret', None):
        return {'total': {'EUR': 1000.0}, 'free': {'EUR': 1000.0}}
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, cliente.fetch_balance, *args, **kwargs)


async def create_order_async(cliente, *args, **kwargs):
    """Versión asíncrona de ``create_order`` con reintentos."""
    if isinstance(cliente, BinanceClient):
        return await cliente.create_order(*args, **kwargs)
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, cliente.create_order, *args, **kwargs)


async def create_market_buy_order_async(cliente, *args, **kwargs):
    if isinstance(cliente, BinanceClient):
        return await cliente.create_market_buy_order(*args, **kwargs)
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, cliente.create_market_buy_order, *args, **kwargs)


async def create_market_sell_order_async(cliente, *args, **kwargs):
    if isinstance(cliente, BinanceClient):
        return await cliente.create_market_sell_order(*args, **kwargs)
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, cliente.create_market_sell_order, *args, **kwargs)


async def fetch_open_orders_async(cliente, *args, **kwargs):
    if isinstance(cliente, BinanceClient):
        return await cliente.fetch_open_orders(*args, **kwargs)
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, cliente.fetch_open_orders, *args, **kwargs)


async def fetch_ticker_async(cliente, *args, **kwargs):
    if isinstance(cliente, BinanceClient):
        return await cliente.fetch_ticker(*args, **kwargs)
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, cliente.fetch_ticker, *args, **kwargs)


async def load_markets_async(cliente, *args, **kwargs):
    if isinstance(cliente, BinanceClient):
        return await cliente.load_markets(*args, **kwargs)
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, cliente.load_markets, *args, **kwargs)


async def fetch_ohlcv_async(cliente, *args, **kwargs):
    """Versión asíncrona de `fetch_ohlcv` con reintentos."""
    if isinstance(cliente, BinanceClient):
        return await cliente.fetch_ohlcv(*args, **kwargs)
    loop = asyncio.get_running_loop()
    func = functools.partial(cliente.fetch_ohlcv, *args, **kwargs)
    return await loop.run_in_executor(None, func)
