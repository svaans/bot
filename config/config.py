# config/config.py
"""Configuración de alto nivel cargada desde ``ConfigManager``."""

from config.config_manager import ConfigManager


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
MIN_ORDER_SYMBOL = cfg.min_order_symbol
PERSISTENCIA_MINIMA = cfg.persistencia_minima
PESO_EXTRA_PERSISTENCIA = cfg.peso_extra_persistencia
MODO_CAPITAL_BAJO = cfg.modo_capital_bajo
TELEGRAM_TOKEN = cfg.telegram_token
TELEGRAM_CHAT_ID = cfg.telegram_chat_id
UMBRAL_SCORE_TECNICO = cfg.umbral_score_tecnico
USAR_SCORE_TECNICO = cfg.usar_score_tecnico
CONTRADICCIONES_BLOQUEAN_ENTRADA = cfg.contradicciones_bloquean_entrada
REGISTRO_TECNICO_CSV = cfg.registro_tecnico_csv
MAX_CONCURRENT_ENTRADAS = cfg.max_concurrent_entradas
MAX_CONCURRENT_SALIDAS = cfg.max_concurrent_salidas
MAX_CONCURRENT_TASKS = cfg.max_concurrent_tasks
CANDLE_PROCESS_INTERVAL = cfg.candle_process_interval
WATCHDOG_TIMEOUT = cfg.watchdog_timeout