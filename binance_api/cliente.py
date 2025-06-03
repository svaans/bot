import os
import ccxt
from config.config import API_KEY, API_SECRET, MODO_REAL

def crear_cliente():
    exchange = ccxt.binance({
        'apiKey': API_KEY,
        'secret': API_SECRET,
        'enableRateLimit': True,
        'options': {
            'defaultType': 'spot',
            'adjustForTimeDifference': True 
        }
    })
    exchange.set_sandbox_mode(not MODO_REAL)
    return exchange

obtener_cliente = crear_cliente
