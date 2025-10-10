from dataclasses import dataclass, field
from typing import Dict, List, Optional
import os
from dotenv import load_dotenv
from pathlib import Path

from config.development import DevelopmentConfig
from config.production import ProductionConfig
from core.utils.utils import configurar_logger
from core.utils.log_utils import safe_extra

log = configurar_logger('config_manager')


def _cargar_float(clave: str, valor_defecto) -> float:
    try:
        return float(os.getenv(clave, valor_defecto))
    except ValueError:
        log.warning(f'⚠️ Valor inválido para {clave}. Usando valor por defecto {valor_defecto}')
        return float(valor_defecto)


def _cargar_int(clave: str, valor_defecto) -> int:
    try:
        return int(os.getenv(clave, valor_defecto))
    except ValueError:
        log.warning(f'⚠️ Valor inválido para {clave}. Usando valor por defecto {valor_defecto}')
        return int(valor_defecto)


def _parse_int_mapping(clave: str) -> Dict[str, int]:
    """Parses mappings ``SIMBOLO:valor`` separados por coma en enteros."""
    resultado: Dict[str, int] = {}
    raw = os.getenv(clave, "")
    if not raw:
        return resultado
    for fragmento in raw.split(','):
        fragmento = fragmento.strip()
        if not fragmento:
            continue
        if ':' not in fragmento:
            log.warning(f'⚠️ Formato inválido en {clave}: {fragmento}')
            continue
        simbolo, valor = fragmento.split(':', 1)
        simbolo = simbolo.strip().upper()
        try:
            resultado[simbolo] = int(valor.strip())
        except ValueError:
            log.warning(f'⚠️ Valor inválido en {clave} para {simbolo}: {valor}')
    return resultado


def _parse_str_mapping(clave: str) -> Dict[str, str]:
    """Parses mappings ``SIMBOLO:valor`` separados por coma en strings."""
    resultado: Dict[str, str] = {}
    raw = os.getenv(clave, "")
    if not raw:
        return resultado
    for fragmento in raw.split(','):
        fragmento = fragmento.strip()
        if not fragmento:
            continue
        if ':' not in fragmento:
            log.warning(f'⚠️ Formato inválido en {clave}: {fragmento}')
            continue
        simbolo, valor = fragmento.split(':', 1)
        resultado[simbolo.strip().upper()] = valor.strip().lower()
    return resultado


def _parse_float_mapping(clave: str) -> Dict[str, float]:
    """Parses mappings ``SIMBOLO:valor`` en flotantes."""
    resultado: Dict[str, float] = {}
    raw = os.getenv(clave, "")
    if not raw:
        return resultado
    for fragmento in raw.split(','):
        fragmento = fragmento.strip()
        if not fragmento:
            continue
        if ':' not in fragmento:
            log.warning(f'⚠️ Formato inválido en {clave}: {fragmento}')
            continue
        simbolo, valor = fragmento.split(':', 1)
        simbolo = simbolo.strip().upper()
        try:
            resultado[simbolo] = float(valor.strip())
        except ValueError:
            log.warning(f'⚠️ Valor inválido en {clave} para {simbolo}: {valor}')
    return resultado


@dataclass(frozen=True)
class Config:
    """Configuración inmutable cargada desde el entorno."""
    api_key: Optional[str]
    api_secret: Optional[str]
    modo_real: bool
    intervalo_velas: str
    symbols: List[str]
    umbral_riesgo_diario: float
    min_order_eur: float
    diversidad_minima: int = 2
    capital_currency: Optional[str] = None
    persistencia_minima: int = 1
    peso_extra_persistencia: float = 0.5
    modo_capital_bajo: bool = False
    telegram_token: Optional[str] = None
    telegram_chat_id: Optional[str] = None
    umbral_score_tecnico: float = 2.0
    usar_score_tecnico: bool = True
    contradicciones_bloquean_entrada: bool = True
    registro_tecnico_csv: str = 'logs/rechazos_tecnico.csv'
    umbral_confirmacion_micro: float = 0.6
    umbral_confirmacion_macro: float = 0.6
    min_dist_pct: float = 0.0005
    min_dist_pct_overrides: Dict[str, float] = field(default_factory=dict)
    fracciones_piramide: int = 1
    reserva_piramide: float = 0.0
    umbral_piramide: float = 0.005
    max_perdidas_diarias: int = 6
    volumen_min_relativo: float = 1.0
    max_spread_ratio: float = 0.003
    spread_dynamic: bool = True
    timeout_verificar_salidas: int = 20
    timeout_evaluar_condiciones: int = 15
    timeout_cerrar_operacion: int = 20
    timeout_abrir_operacion: int = 20
    timeout_bus_eventos: int = 10
    max_timeouts_salidas: int = 3
    heartbeat_interval: int = 60
    monitor_interval: int = 5
    max_stream_restarts: int = 10
    inactivity_intervals: int = 10
    handler_timeout: float = 2.0
    ws_timeout: int = 30
    frecuencia_tendencia: int = 1
    frecuencia_correlaciones: int = 300
    umbral_alerta_cpu: float = 85.0
    umbral_alerta_mem: float = 90.0
    ciclos_alerta_recursos: int = 5
    frecuencia_recursos: int = 60
    timeout_sin_datos_factor: int = 6
    backfill_max_candles: int = 1000
    df_backpressure: bool = True
    df_backpressure_drop: bool = True
    df_queue_default_limit: int = 2000
    df_queue_limits: Dict[str, int] = field(default_factory=dict)
    df_queue_policy: str = "block"
    df_queue_policy_by_symbol: Dict[str, str] = field(default_factory=dict)
    df_queue_coalesce_ms: int = 0
    df_queue_high_watermark: float = 0.8
    df_queue_safety_policy: str = "drop_oldest"
    df_queue_alert_interval: float = 5.0
    df_metrics_log_interval: float = 5.0
    trader_metrics_log_interval: float = 5.0
    trader_fastpath_enabled: bool = True
    trader_fastpath_threshold: int = 350
    trader_fastpath_recovery: int = 200
    trader_fastpath_skip_notifications: bool = True
    trader_fastpath_skip_entries: bool = False
    trader_fastpath_skip_trend: bool = True
    timeout_evaluar_condiciones_por_symbol: Dict[str, int] = field(default_factory=dict)
    indicadores_normalize_default: bool = True
    indicadores_cache_max_entries: int = 128

class ConfigManager:
    """Carga y proporciona acceso a la configuración del bot."""

    def __init__(self, *, auto_load: bool = True) -> None:
        """Inicializa el administrador de configuración.

        Parameters
        ----------
        auto_load:
            Cuando es ``True`` (por defecto) carga inmediatamente la
            configuración desde las variables de entorno. Los tests
            unitarios inyectan este objeto sin preparar el entorno, por lo
            que es importante exponer siempre un atributo ``config`` con al
            menos los campos básicos utilizados por el resto del sistema.
        """

        self._config: Optional[Config] = None
        # ``config`` debe existir incluso si aún no se cargó nada para que
        # componentes antiguos que acceden directamente al atributo no
        # fallen con ``AttributeError``.
        self.config: Optional[Config] = None

        if auto_load:
            self.reload()

    @staticmethod
    def load_from_env() -> Config:
        # Cargar .env local si existe (no rompe si falta)
        env_path = Path(__file__).resolve().parent.parent / 'config' / 'claves.env'
        load_dotenv(env_path)

        env_name = os.getenv('BOT_ENV', 'development').lower()
        log_dir = os.getenv('LOG_DIR', 'logs')

        defaults = DevelopmentConfig()
        if env_name == 'production':
            defaults = ProductionConfig()

        # Símbolos
        symbols_env = os.getenv('SYMBOLS', ','.join(defaults.symbols))
        symbols = [s.strip().upper() for s in symbols_env.split(',') if s.strip()]
        if not symbols:
            log.error('❌ Faltan símbolos (SYMBOLS)')
            raise ValueError('Faltan datos de configuración: SYMBOLS')

        # Flags globales
        modo_real = os.getenv('MODO_REAL', str(defaults.modo_real)).lower() == 'true'

        # Claves API (solo si modo_real)
        api_key = os.environ.get('BINANCE_API_KEY')
        api_secret = os.environ.get('BINANCE_API_SECRET')
        if modo_real:
            missing = []
            if not api_key:
                missing.append('BINANCE_API_KEY')
            if not api_secret:
                missing.append('BINANCE_API_SECRET')
            if missing:
                datos = ', '.join(missing)
                log.error(f'❌ Faltan variables de entorno requeridas: {datos}')
                raise ValueError(f'Faltan datos de configuración: {datos}')

        # Capital
        capital_currency = os.getenv('CAPITAL_CURRENCY')

        # DataFeed queue tuning
        df_queue_limits = dict(getattr(defaults, 'df_queue_limits', {}))
        df_queue_limits.update(_parse_int_mapping('DF_QUEUE_LIMITS'))
        df_queue_policy_by_symbol = dict(getattr(defaults, 'df_queue_policy_by_symbol', {}))
        df_queue_policy_by_symbol.update(_parse_str_mapping('DF_QUEUE_POLICIES'))

        # Distancias
        min_dist_pct = _cargar_float('MIN_DIST_PCT', getattr(defaults, 'min_dist_pct', 0.0005))
        min_dist_pct_overrides = dict(getattr(defaults, 'min_dist_pct_overrides', {}))
        min_dist_pct_overrides.update(_parse_float_mapping('MIN_DIST_PCT_OVERRIDES'))

        # Queue defaults / policy
        df_queue_default_limit = _cargar_int('DF_QUEUE_DEFAULT_LIMIT', getattr(defaults, 'df_queue_default_limit', 2000))
        df_queue_policy = os.getenv('DF_QUEUE_POLICY', getattr(defaults, 'df_queue_policy', 'block')).lower()
        df_queue_coalesce_ms = _cargar_int('DF_QUEUE_COALESCE_MS', getattr(defaults, 'df_queue_coalesce_ms', 0))
        df_queue_high_watermark = _cargar_float('DF_QUEUE_HIGH_WATERMARK', getattr(defaults, 'df_queue_high_watermark', 0.8))
        df_queue_safety_policy = os.getenv('DF_QUEUE_SAFETY_POLICY', getattr(defaults, 'df_queue_safety_policy', 'drop_oldest')).lower()
        df_queue_alert_interval = _cargar_float('DF_QUEUE_ALERT_INTERVAL', getattr(defaults, 'df_queue_alert_interval', 5.0))
        df_metrics_log_interval = _cargar_float('DF_METRICS_LOG_INTERVAL', getattr(defaults, 'df_metrics_log_interval', 5.0))
        trader_metrics_log_interval = _cargar_float('TRADER_METRICS_LOG_INTERVAL', getattr(defaults, 'trader_metrics_log_interval', 5.0))

        # Fastpath
        trader_fastpath_enabled = os.getenv(
            'TRADER_FASTPATH_ENABLED',
            str(getattr(defaults, 'trader_fastpath_enabled', True)),
        ).lower() == 'true'
        trader_fastpath_threshold = _cargar_int('TRADER_FASTPATH_THRESHOLD', getattr(defaults, 'trader_fastpath_threshold', 350))
        trader_fastpath_recovery = _cargar_int('TRADER_FASTPATH_RECOVERY', getattr(defaults, 'trader_fastpath_recovery', 200))
        trader_fastpath_skip_notifications = os.getenv(
            'TRADER_FASTPATH_SKIP_NOTIFICATIONS',
            str(getattr(defaults, 'trader_fastpath_skip_notifications', True)),
        ).lower() == 'true'
        trader_fastpath_skip_entries = os.getenv(
            'TRADER_FASTPATH_SKIP_ENTRIES',
            str(getattr(defaults, 'trader_fastpath_skip_entries', True)),
        ).lower() == 'true'
        trader_fastpath_skip_trend = os.getenv(
            'TRADER_FASTPATH_SKIP_TREND',
            str(getattr(defaults, 'trader_fastpath_skip_trend', True)),
        ).lower() == 'true'

        # Timeouts por símbolo (nuevo: se toma de env si existe)
        timeout_evaluar_por_symbol = _parse_int_mapping('TIMEOUT_EVALUAR_CONDICIONES_POR_SYMBOL')


        indicadores_normalize_default = os.getenv(
            'INDICADORES_NORMALIZE_DEFAULT',
            str(getattr(defaults, 'indicadores_normalize_default', True)),
        ).strip().lower() not in {'false', '0', 'no', 'off'}
        indicadores_cache_max_entries = _cargar_int(
            'INDICADORES_CACHE_MAX_ENTRIES',
            getattr(defaults, 'indicadores_cache_max_entries', 128),
        )
        
        config = Config(
            api_key=api_key,
            api_secret=api_secret,
            modo_real=modo_real,
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
            min_dist_pct=min_dist_pct,
            min_dist_pct_overrides=min_dist_pct_overrides,
            fracciones_piramide=_cargar_int('FRACCIONES_PIRAMIDE', defaults.fracciones_piramide),
            reserva_piramide=_cargar_float('RESERVA_PIRAMIDE', defaults.reserva_piramide),
            umbral_piramide=_cargar_float('UMBRAL_PIRAMIDE', defaults.umbral_piramide),
            max_perdidas_diarias=_cargar_int('MAX_PERDIDAS_DIARIAS', defaults.max_perdidas_diarias),
            volumen_min_relativo=_cargar_float('VOLUMEN_MIN_RELATIVO', defaults.volumen_min_relativo),
            max_spread_ratio=_cargar_float('MAX_SPREAD_RATIO', defaults.max_spread_ratio),
            spread_dynamic=os.getenv('SPREAD_DYNAMIC', str(getattr(defaults, 'spread_dynamic', True))).lower() == 'true',
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
            handler_timeout=_cargar_float('HANDLER_TIMEOUT', defaults.handler_timeout),
            ws_timeout=_cargar_int('WS_TIMEOUT', getattr(defaults, 'ws_timeout', 30)),
            frecuencia_tendencia=_cargar_int('FRECUENCIA_TENDENCIA', defaults.frecuencia_tendencia),
            frecuencia_correlaciones=_cargar_int('FRECUENCIA_CORRELACIONES', defaults.frecuencia_correlaciones),
            umbral_alerta_cpu=_cargar_float('UMBRAL_ALERTA_CPU', defaults.umbral_alerta_cpu),
            umbral_alerta_mem=_cargar_float('UMBRAL_ALERTA_MEM', defaults.umbral_alerta_mem),
            ciclos_alerta_recursos=_cargar_int('CICLOS_ALERTA_RECURSOS', defaults.ciclos_alerta_recursos),
            frecuencia_recursos=_cargar_int('FRECUENCIA_RECURSOS', defaults.frecuencia_recursos),
            timeout_sin_datos_factor=_cargar_int('TIMEOUT_SIN_DATOS_FACTOR', getattr(defaults, 'timeout_sin_datos_factor', 6)),
            backfill_max_candles=_cargar_int('BACKFILL_MAX_CANDLES', getattr(defaults, 'backfill_max_candles', 1000)),
            df_backpressure=os.getenv('DF_BACKPRESSURE', 'true').lower() == 'true',
            df_backpressure_drop=os.getenv('DF_BACKPRESSURE_DROP', 'true').lower() == 'true',
            df_queue_default_limit=df_queue_default_limit,
            df_queue_limits=df_queue_limits,
            df_queue_policy=df_queue_policy,
            df_queue_policy_by_symbol=df_queue_policy_by_symbol,
            df_queue_coalesce_ms=df_queue_coalesce_ms,
            df_queue_high_watermark=df_queue_high_watermark,
            df_queue_safety_policy=df_queue_safety_policy,
            df_queue_alert_interval=df_queue_alert_interval,
            df_metrics_log_interval=df_metrics_log_interval,
            trader_metrics_log_interval=trader_metrics_log_interval,
            trader_fastpath_enabled=trader_fastpath_enabled,
            trader_fastpath_threshold=trader_fastpath_threshold,
            trader_fastpath_recovery=trader_fastpath_recovery,
            trader_fastpath_skip_notifications=trader_fastpath_skip_notifications,
            trader_fastpath_skip_entries=trader_fastpath_skip_entries,
            trader_fastpath_skip_trend=trader_fastpath_skip_trend,
            timeout_evaluar_condiciones_por_symbol=timeout_evaluar_por_symbol,
            indicadores_normalize_default=indicadores_normalize_default,
            indicadores_cache_max_entries=indicadores_cache_max_entries,
        )

        log.info(
            "Fastpath trader configurado",
            extra=safe_extra(
                {
                    "event": "config.fastpath",
                    "trader_fastpath_enabled": trader_fastpath_enabled,
                    "trader_fastpath_threshold": trader_fastpath_threshold,
                    "trader_fastpath_recovery": trader_fastpath_recovery,
                    "trader_fastpath_skip_notifications": trader_fastpath_skip_notifications,
                    "trader_fastpath_skip_entries": trader_fastpath_skip_entries,
                    "trader_fastpath_skip_trend": trader_fastpath_skip_trend,
                }
            ),
        )

        return config


    def reload(self) -> Config:
            """Recarga la configuración desde el entorno y la expone en ``config``."""
    
            cfg = self.load_from_env()
            self._config = cfg
            self.config = cfg
            return cfg
    
    
    def get_config(self) -> Config:
        """Devuelve la configuración actual, recargándola si es necesario."""
    
        if self._config is None:
            return self.reload()
        return self._config
