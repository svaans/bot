# config/config.py
"""Configuración de alto nivel cargada desde ``ConfigManager``."""

from core.config_manager import ConfigManager
import os

# API keys
cfg = ConfigManager.load_from_env()

# Configuración general del bot
API_KEY = cfg.api_key
API_SECRET = cfg.api_secret
MODO_REAL = cfg.modo_real
INTERVALO_VELAS = cfg.intervalo_velas
SYMBOLS = cfg.symbols
