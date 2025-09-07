from dataclasses import dataclass
from typing import List
import os
from dotenv import load_dotenv
from pathlib import Path
from config.development import DevelopmentConfig
from config.production import ProductionConfig
from core.utils.utils import configurar_logger
log = configurar_logger('config_manager')


def _cargar_float(clave, valor_defecto):
    try:
        return float(os.getenv(clave, valor_defecto))
    except ValueError:
        log.warning(
            f'⚠️ Valor inválido para {clave}. Usando valor por defecto {valor_defecto}'
            )
        return float(valor_defecto)


def _cargar_int(clave, valor_defecto):
    try:
        return int(os.getenv(clave, valor_defecto))
    except ValueError:
        log.warning(
            f'⚠️ Valor inválido para {clave}. Usando valor por defecto {valor_defecto}'
            )
        return int(valor_defecto)


@dataclass(frozen=True)
class Config:
    """Configuración inmutable cargada desde el entorno."""
    api_key: str
    api_secret: str
    modo_real: bool
    intervalo_velas: str
    symbols: List[str]
    umbral_riesgo_diario: float
    min_order_eur: float
    diversidad_minima: int = 2
    capital_currency: str | None = None
    persistencia_minima: int = 1
    peso_extra_persistencia: float = 0.5
    modo_capital_bajo: bool = False
    telegram_token: str | None = None
    telegram_chat_id: str | None = None
    umbral_score_tecnico: float = 2.0
    usar_score_tecnico: bool = True
    contradicciones_bloquean_entrada: bool = True
    registro_tecnico_csv: str = 'logs/rechazos_tecnico.csv'
    umbral_confirmacion_micro: float = 0.6
    umbral_confirmacion_macro: float = 0.6
    fracciones_piramide: int = 1
    reserva_piramide: float = 0.0
    umbral_piramide: float = 0.005
    max_perdidas_diarias: int = 6
    volumen_min_relativo: float = 1.0
    max_spread_ratio: float = 0.003
    timeout_verificar_salidas: int = 20
    timeout_evaluar_condiciones: int = 15
    timeout_cerrar_operacion: int = 20
    timeout_abrir_operacion: int = 20
    timeout_bus_eventos: int = 10
    max_timeouts_salidas: int = 3
    heartbeat_interval: int = 60
    monitor_interval: int = 5
    max_stream_restarts: int = 5
    inactivity_intervals: int = 5
    handler_timeout: int = 5
    frecuencia_tendencia: int = 1
    frecuencia_correlaciones: int = 300
    umbral_alerta_cpu: float = 85.0
    umbral_alerta_mem: float = 90.0
    ciclos_alerta_recursos: int = 5
    frecuencia_recursos: int = 60
    timeout_sin_datos_factor: int = 6
    backfill_max_candles: int = 1000


class ConfigManager:
    """Carga y proporciona acceso a la configuración del bot."""

    @staticmethod
    def load_from_env() ->Config:
        env_path = Path(__file__).resolve(
            ).parent.parent / 'config' / 'claves.env'
        load_dotenv(env_path)
        env_name = os.getenv('BOT_ENV', 'development').lower()
        log_dir = os.getenv('LOG_DIR', 'logs')
        defaults = DevelopmentConfig()
        if env_name == 'production':
            defaults = ProductionConfig()
        symbols_env = os.getenv('SYMBOLS', ','.join(defaults.symbols))
        symbols = [s.strip().upper() for s in symbols_env.split(',') if s.
            strip()]
        api_key = os.environ.get('BINANCE_API_KEY')
        api_secret = os.environ.get('BINANCE_API_SECRET')
        missing = []
        if not api_key:
            missing.append('BINANCE_API_KEY')
        if not api_secret:
            missing.append('BINANCE_API_SECRET')
        if not symbols:
            missing.append('SYMBOLS')
        if missing:
            datos = ', '.join(missing)
            log.error(f'❌ Faltan variables de entorno requeridas: {datos}')
            raise ValueError(f'Faltan datos de configuración: {datos}')
        capital_currency = os.getenv('CAPITAL_CURRENCY')
        return Config(
            api_key=api_key,
            api_secret=api_secret,
            modo_real=os.getenv('MODO_REAL', str(defaults.modo_real)).lower() == 'true',
            intervalo_velas=os.getenv('INTERVALO_VELAS', defaults.intervalo_velas),
            symbols=symbols,
            umbral_riesgo_diario=_cargar_float('UMBRAL_RIESGO_DIARIO', defaults.umbral_riesgo_diario),
            min_order_eur=_cargar_float('MIN_ORDER_EUR', defaults.min_order_eur),
            diversidad_minima=_cargar_int('DIVERSIDAD_MINIMA', defaults.diversidad_minima),
            capital_currency=capital_currency,
            persistencia_minima=_cargar_int('PERSISTENCIA_MINIMA', defaults.persistencia_minima),
            peso_extra_persistencia=_cargar_float('PESO_EXTRA_PERSISTENCIA', defaults.peso_extra_persistencia),
            modo_capital_bajo=os.getenv('MODO_CAPITAL_BAJO', str(defaults.modo_capital_bajo)).lower() == 'true',
            telegram_token=os.getenv('TELEGRAM_TOKEN', defaults.telegram_token),
            telegram_chat_id=os.getenv('TELEGRAM_CHAT_ID', defaults.telegram_chat_id),
            umbral_score_tecnico=_cargar_float('UMBRAL_SCORE_TECNICO', defaults.umbral_score_tecnico),
            usar_score_tecnico=os.getenv('USAR_SCORE_TECNICO', str(defaults.usar_score_tecnico)).lower() == 'true',
            contradicciones_bloquean_entrada=os.getenv('CONTRADICCIONES_BLOQUEAN_ENTRADA', str(defaults.contradicciones_bloquean_entrada)).lower() == 'true',
            registro_tecnico_csv=os.getenv('REGISTRO_TECNICO_CSV', os.path.join(log_dir, 'rechazos_tecnico.csv')),
            umbral_confirmacion_micro=_cargar_float('UMBRAL_CONFIRMACION_MICRO', getattr(defaults, 'umbral_confirmacion_micro', 0.6)),
            umbral_confirmacion_macro=_cargar_float('UMBRAL_CONFIRMACION_MACRO', getattr(defaults, 'umbral_confirmacion_macro', 0.6)),
            fracciones_piramide=int(os.getenv('FRACCIONES_PIRAMIDE', defaults.fracciones_piramide)),
            reserva_piramide=float(os.getenv('RESERVA_PIRAMIDE', defaults.reserva_piramide)),
            umbral_piramide=float(os.getenv('UMBRAL_PIRAMIDE', defaults.umbral_piramide)),
            max_perdidas_diarias=_cargar_int('MAX_PERDIDAS_DIARIAS', defaults.max_perdidas_diarias),
            volumen_min_relativo=_cargar_float('VOLUMEN_MIN_RELATIVO', defaults.volumen_min_relativo),
            max_spread_ratio=_cargar_float('MAX_SPREAD_RATIO', defaults.max_spread_ratio),
            timeout_verificar_salidas=_cargar_int('TIMEOUT_VERIFICAR_SALIDAS', defaults.timeout_verificar_salidas),
            timeout_evaluar_condiciones=_cargar_int('TIMEOUT_EVALUAR_CONDICIONES', defaults.timeout_evaluar_condiciones),
            timeout_cerrar_operacion=_cargar_int('TIMEOUT_CERRAR_OPERACION', defaults.timeout_cerrar_operacion),
            timeout_abrir_operacion=_cargar_int('TIMEOUT_ABRIR_OPERACION', defaults.timeout_abrir_operacion),
            timeout_bus_eventos=_cargar_int('TIMEOUT_BUS_EVENTOS', defaults.timeout_bus_eventos),
            max_timeouts_salidas=_cargar_int('MAX_TIMEOUTS_SALIDAS', defaults.max_timeouts_salidas),
            heartbeat_interval=_cargar_int('HEARTBEAT_INTERVAL', defaults.heartbeat_interval),
            monitor_interval=_cargar_int('MONITOR_INTERVAL', defaults.monitor_interval),
            inactivity_intervals=_cargar_int('INACTIVITY_INTERVALS', defaults.inactivity_intervals),
            max_stream_restarts=_cargar_int('MAX_STREAM_RESTARTS', defaults.max_stream_restarts),
            handler_timeout=_cargar_int('HANDLER_TIMEOUT', defaults.handler_timeout),
            frecuencia_tendencia=_cargar_int('FRECUENCIA_TENDENCIA', defaults.frecuencia_tendencia),
            frecuencia_correlaciones=_cargar_int('FRECUENCIA_CORRELACIONES', defaults.frecuencia_correlaciones),
            umbral_alerta_cpu=_cargar_float('UMBRAL_ALERTA_CPU', defaults.umbral_alerta_cpu),
            umbral_alerta_mem=_cargar_float('UMBRAL_ALERTA_MEM', defaults.umbral_alerta_mem),
            ciclos_alerta_recursos=_cargar_int('CICLOS_ALERTA_RECURSOS', defaults.ciclos_alerta_recursos),
            frecuencia_recursos=_cargar_int('FRECUENCIA_RECURSOS', defaults.frecuencia_recursos),
            timeout_sin_datos_factor=_cargar_int('TIMEOUT_SIN_DATOS_FACTOR', getattr(defaults, 'timeout_sin_datos_factor', 6)),
            backfill_max_candles=_cargar_int('BACKFILL_MAX_CANDLES', getattr(defaults, 'backfill_max_candles', 1000)),
        )
