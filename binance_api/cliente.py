import os
import asyncio
import functools
import random
from typing import Any, Callable

import ccxt
from prometheus_client import Gauge
from config.config_manager import Config, ConfigManager
from core.utils.utils import configurar_logger

log = configurar_logger('binance_client')

DEGRADED_GAUGE = Gauge('degraded_mode', 'Estado del modo degradado', ['state'])
DEGRADED_GAUGE.labels(state='on').set(0)
DEGRADED_GAUGE.labels(state='off').set(1)


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

    async def execute(self, func: Callable[..., Any], *args, **kwargs) -> Any:
        loop = asyncio.get_running_loop()
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
                if not self._retryable(exc):
                    self._record_failure()
                    raise
                self._record_failure()
                if intento >= self.max_retries:
                    raise
                espera = self.backoff_base * (2 ** (intento - 1))
                espera += random.random() * self.jitter
                await asyncio.sleep(espera)

    async def fetch_balance(self, *args, **kwargs):
        if not getattr(self.exchange, 'apiKey', None) or not getattr(self.exchange, 'secret', None):
            return {'total': {'EUR': 1000.0}, 'free': {'EUR': 1000.0}}
        return await self.execute(self.exchange.fetch_balance, *args, **kwargs)

    async def create_order(self, *args, **kwargs):
        if self.degraded:
            raise RuntimeError('Modo degradado activo: creación de órdenes suspendida')
        return await self.execute(self.exchange.create_order, *args, **kwargs)

    async def create_market_buy_order(self, *args, **kwargs):
        if self.degraded:
            raise RuntimeError('Modo degradado activo: creación de órdenes suspendida')
        return await self.execute(self.exchange.create_market_buy_order, *args, **kwargs)

    async def create_market_sell_order(self, *args, **kwargs):
        return await self.execute(self.exchange.create_market_sell_order, *args, **kwargs)

    async def fetch_open_orders(self, *args, **kwargs):
        return await self.execute(self.exchange.fetch_open_orders, *args, **kwargs)

    async def fetch_ticker(self, *args, **kwargs):
        return await self.execute(self.exchange.fetch_ticker, *args, **kwargs)

    async def load_markets(self, *args, **kwargs):
        return await self.execute(self.exchange.load_markets, *args, **kwargs)

    async def fetch_ohlcv(self, *args, **kwargs):
        return await self.execute(self.exchange.fetch_ohlcv, *args, **kwargs)


def crear_cliente(config: (Config | None)=None):
    if config is None:
        config = ConfigManager.load_from_env()
    exchange = ccxt.binance({'apiKey': config.api_key, 'secret': config.
        api_secret, 'enableRateLimit': True, 'options': {'defaultType':
        'spot', 'adjustForTimeDifference': True,
        'warnOnFetchOpenOrdersWithoutSymbol': False}})
    exchange.set_sandbox_mode(not config.modo_real)
    try:
        exchange.load_time_difference()
    except Exception:
        pass
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
