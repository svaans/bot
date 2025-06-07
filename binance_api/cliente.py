import os
import ccxt
from core.config_manager import Config, ConfigManager

def crear_cliente(config: Config | None = None):
    if config is None:
        config = ConfigManager.load_from_env()
    exchange = ccxt.binance({
        'apiKey': config.api_key,
        'secret': config.api_secret,
        'enableRateLimit': True,
        'options': {
            'defaultType': 'spot',
            'adjustForTimeDifference': True 
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
