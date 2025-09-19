from dataclasses import dataclass, field
from typing import List, Dict


@dataclass(frozen=True)
class DevelopmentConfig:
    """Valores por defecto para el entorno de desarrollo."""
    modo_real: bool = False
    intervalo_velas: str = '5m'
    symbols: List[str] = field(default_factory=lambda : ['BTC/EUR',
        'ETH/EUR', 'ADA/EUR', 'SOL/EUR', 'BNB/EUR'])
    umbral_riesgo_diario: float = 0.03
    min_order_eur: float = 10.0
    persistencia_minima: int = 1
    peso_extra_persistencia: float = 0.5
    modo_capital_bajo: bool = False
    telegram_token: str | None = None
    telegram_chat_id: str | None = None
    umbral_score_tecnico: float = 2.0
    usar_score_tecnico: bool = True
    contradicciones_bloquean_entrada: bool = False
    registro_tecnico_csv: str = 'logs/rechazos_tecnico.csv'
    umbral_confirmacion_micro: float = 0.6
    umbral_confirmacion_macro: float = 0.6
    fracciones_piramide: int = 1
    reserva_piramide: float = 0.0
    umbral_piramide: float = 0.006
    max_perdidas_diarias: int = 6
    volumen_min_relativo: float = 1.0
    max_spread_ratio: float = 0.003  # valor base
    spread_dynamic: bool = True  # activar ajuste dinámico de spread
    diversidad_minima: int = 2
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
    frecuencia_tendencia: int = 3
    frecuencia_correlaciones: int = 300
    umbral_alerta_cpu: float = 85.0
    umbral_alerta_mem: float = 90.0
    ciclos_alerta_recursos: int = 5
    frecuencia_recursos: int = 60
    timeout_sin_datos_factor: int = 6
    backfill_max_candles: int = 1000
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
    trader_fastpath_skip_entries: bool = True
    trader_fastpath_skip_trend: bool = True
    timeout_evaluar_condiciones_por_symbol: Dict[str, int] = field(default_factory=dict)
