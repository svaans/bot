import os
import asyncio
import functools
import json
import os
import random
import re
from typing import Any, Callable

import ccxt
from prometheus_client import Gauge
from config.config_manager import Config
from core.utils.utils import configurar_logger

log = configurar_logger('binance_client')

DEGRADED_GAUGE = Gauge('degraded_mode', 'Estado del modo degradado', ['state'])
DEGRADED_GAUGE.labels(state='on').set(0)
DEGRADED_GAUGE.labels(state='off').set(1)

AUTH_WARNING_EMITTED = False


class BinanceError(Exception):
    """Excepción propia con código y sugerencia."""

    def __init__(self, code: int, reason: str, suggestion: str) -> None:
        super().__init__(f"{code}: {reason} - {suggestion}")
        self.code = code
        self.reason = reason
        self.suggestion = suggestion


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
    """Wrapper del cliente Binance con reintentos y modo degradado."""

    def __init__(
        self,
        config: Config | None = None,
        *,
        max_retries: int = 5,
        backoff_base: float = 0.5,
        jitter: float = 0.1,
        fail_threshold: int = 5,
    ) -> None:
        self.exchange = crear_cliente(config)
        self.authenticated = bool(getattr(self.exchange, "apiKey", None) and getattr(self.exchange, "secret", None))
        self.modo_real = getattr(self.exchange, "_modo_real", True)
        self.max_retries = max_retries
        self.backoff_base = backoff_base
        self.jitter = jitter
        self.fail_threshold = fail_threshold
        self._consecutive_failures = 0
        self.degraded = False

    def _update_metric(self) -> None:
        if self.degraded:
            DEGRADED_GAUGE.labels(state='on').set(1)
            DEGRADED_GAUGE.labels(state='off').set(0)
        else:
            DEGRADED_GAUGE.labels(state='on').set(0)
            DEGRADED_GAUGE.labels(state='off').set(1)

    def _record_success(self) -> None:
        if self.degraded:
            self.degraded = False
            log.info('Modo degradado desactivado')
            self._update_metric()
        self._consecutive_failures = 0

    def _record_failure(self) -> None:
        self._consecutive_failures += 1
        if self._consecutive_failures >= self.fail_threshold and not self.degraded:
            self.degraded = True
            log.warning('Modo degradado activado')
            self._update_metric()

    @staticmethod
    def _retryable(exc: Exception) -> bool:
        if isinstance(exc, (ccxt.NetworkError, ccxt.RequestTimeout, asyncio.TimeoutError)):
            return True
        status = getattr(exc, 'http_status', None)
        return isinstance(status, int) and 500 <= status < 600

    def _extract_code(self, exc: Exception) -> int | None:
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

    def _map_error(self, exc: Exception) -> Exception:
        code = self._extract_code(exc)
        mapping = {
            -2015: ("invalid_api_key", "Verifica API key, IP o permisos"),
            -1021: ("timestamp", "Sincroniza reloj"),
            -2014: ("bad_api_key_format", "Revisa el formato de las claves"),
            -2011: ("unknown_order", "Revisa el id de la orden"),
        }
        if isinstance(exc, ccxt.DDoSProtection) or getattr(exc, "http_status", None) == 429:
            return BinanceError(429, "rate_limit", "Reduce la frecuencia de llamadas")
        if code in mapping:
            reason, suggestion = mapping[code]
            return BinanceError(code, reason, suggestion)
        return exc

    async def execute(self, func: Callable[..., Any], *args, **kwargs) -> Any:
        loop = asyncio.get_running_loop()
        time_synced = False
        for intento in range(1, self.max_retries + 1):
            try:
                if asyncio.iscoroutinefunction(func):
                    resultado = await func(*args, **kwargs)
                else:
                    parcial = functools.partial(func, *args, **kwargs)
                    resultado = await loop.run_in_executor(None, parcial)
                self._record_success()
                return resultado
            except Exception as exc:  # pragma: no cover - guard para errores no previstos
                code = self._extract_code(exc)
                if code == -1021 and not time_synced:
                    try:
                        await loop.run_in_executor(None, self.exchange.load_time_difference)
                    except Exception:
                        pass
                    time_synced = True
                    continue
                if not self._retryable(exc):
                    self._record_failure()
                    raise self._map_error(exc)
                self._record_failure()
                if intento >= self.max_retries:
                    raise self._map_error(exc)
                espera = self.backoff_base * (2 ** (intento - 1))
                espera += random.random() * self.jitter
                await asyncio.sleep(espera)

    @auth_guard(lambda: {'total': {'EUR': 1000.0}, 'free': {'EUR': 1000.0}})
    async def fetch_balance(self, *args, **kwargs):
        return await self.execute(self.exchange.fetch_balance, *args, **kwargs)

    @auth_guard(PermissionError('Auth required'))
    async def create_order(self, *args, **kwargs):
        if self.degraded:
            raise RuntimeError('Modo degradado activo: creación de órdenes suspendida')
        return await self.execute(self.exchange.create_order, *args, **kwargs)

    @auth_guard(PermissionError('Auth required'))
    async def create_market_buy_order(self, *args, **kwargs):
        if self.degraded:
            raise RuntimeError('Modo degradado activo: creación de órdenes suspendida')
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
