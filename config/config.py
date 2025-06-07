# config/config.py
"""Configuración de alto nivel cargada desde ``ConfigManager``."""

from core.config_manager import ConfigManager


# API keys
cfg = ConfigManager.load_from_env()

# Configuración general del bot
API_KEY = cfg.api_key
API_SECRET = cfg.api_secret
MODO_REAL = cfg.modo_real
INTERVALO_VELAS = cfg.intervalo_velas
SYMBOLS = cfg.symbols
UMBRAL_RIESGO_DIARIO = cfg.umbral_riesgo_diario
MIN_ORDER_EUR = cfg.min_order_eur
PERSISTENCIA_MINIMA = cfg.persistencia_minima
PESO_EXTRA_PERSISTENCIA = cfg.peso_extra_persistencia
TELEGRAM_TOKEN = cfg.telegram_token
TELEGRAM_CHAT_ID = cfg.telegram_chat_id