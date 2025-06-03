# config/config.py

import os
from dotenv import load_dotenv

# Cargar variables desde .env
load_dotenv(dotenv_path="config/claves.env")

# API keys
API_KEY = os.getenv("BINANCE_API_KEY")
API_SECRET = os.getenv("BINANCE_API_SECRET")

# Configuración general del bot
MODO_REAL = os.getenv("MODO_REAL", "False").lower() == "true"
INTERVALO_VELAS = "1m"
SYMBOLS = ["BTC/EUR", "ETH/EUR", "ADA/EUR"]

# Parámetros del bot agresivo
UMBRAL_RIESGO_DIARIO = float(os.getenv("UMBRAL_RIESGO_DIARIO", 0.03))  # 3%
MODO_VERBOSE = True