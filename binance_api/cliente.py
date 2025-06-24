import os
import asyncio
import ccxt
import functools
from config.config_manager import Config, ConfigManager

def crear_cliente(config: Config | None = None):
    if config is None:
        config = ConfigManager.load_from_env()
    exchange = ccxt.binance({
        'apiKey': config.api_key,
        'secret': config.api_secret,
        'enableRateLimit': True,
        'options': {
            'defaultType': 'spot',
            'adjustForTimeDifference': True,
            'warnOnFetchOpenOrdersWithoutSymbol': False,
        }
    })
    exchange.set_sandbox_mode(not config.modo_real)
    try:
        exchange.load_time_difference()
    except Exception:
        # If the time difference cannot be loaded, continue without failing.
        pass
    return exchange

obtener_cliente = crear_cliente

async def fetch_balance_async(cliente, *args, **kwargs):
    """
    Obtiene el balance de forma asíncrona.
    Si no hay claves API (modo simulado), devuelve un balance ficticio.
    """
    if not getattr(cliente, "apiKey", None) or not getattr(cliente, "secret", None):
        return {
            "total": {"EUR": 1000.0},
            "free": {"EUR": 1000.0}
        }
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, cliente.fetch_balance, *args, **kwargs)



async def create_order_async(cliente, *args, **kwargs):
    """Versión asíncrona de ``create_order``."""
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, cliente.create_order, *args, **kwargs)


async def create_market_buy_order_async(cliente, *args, **kwargs):
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, cliente.create_market_buy_order, *args, **kwargs)


async def create_market_sell_order_async(cliente, *args, **kwargs):
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, cliente.create_market_sell_order, *args, **kwargs)


async def fetch_open_orders_async(cliente, *args, **kwargs):
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, cliente.fetch_open_orders, *args, **kwargs)


async def fetch_ticker_async(cliente, *args, **kwargs):
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, cliente.fetch_ticker, *args, **kwargs)


async def load_markets_async(cliente, *args, **kwargs):
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, cliente.load_markets, *args, **kwargs)

async def fetch_ohlcv_async(cliente, *args, **kwargs):
    """Versión asíncrona de `fetch_ohlcv`."""
    loop = asyncio.get_running_loop()
    func = functools.partial(cliente.fetch_ohlcv, *args, **kwargs)
    return await loop.run_in_executor(None, func)
