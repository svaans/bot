from dataclasses import dataclass, field
from typing import List, Dict

from core.operational_mode import OperationalMode


@dataclass(frozen=True)
class DevelopmentConfig:
    """Valores por defecto para el entorno de desarrollo."""
    modo_real: bool = False
    modo_operativo: OperationalMode = OperationalMode.PAPER_TRADING
    intervalo_velas: str = '5m'
    # Spot Binance (CCXT): mercados líquidos USDT; evita */EUR por defecto que en
    # binance.com suelen no existir o no coincidir con el feed WS usado en paper.
    symbols: List[str] = field(default_factory=lambda: ["BTC/USDT", "ETH/USDT"])
    umbral_riesgo_diario: float = 0.03
    risk_kill_switch_max_consecutive_losses: int = 5
    risk_alerta_capital_pct: float = 0.85
    min_order_eur: float = 10.0
    persistencia_minima: int = 1
    peso_extra_persistencia: float = 0.5
    modo_capital_bajo: bool = False
    telegram_token: str | None = None
    telegram_chat_id: str | None = None
    umbral_score_tecnico: float = 2.0
    usar_score_tecnico: bool = True
    umbral_score_overrides: Dict[str, float] = field(default_factory=dict)
    usar_score_overrides: Dict[str, bool] = field(default_factory=dict)
    persistencia_strict: bool = False
    persistencia_strict_overrides: Dict[str, bool] = field(default_factory=dict)
    contradicciones_bloquean_entrada: bool = False
    registro_tecnico_csv: str = 'logs/rechazos_tecnico.csv'
    umbral_confirmacion_micro: float = 0.6
    umbral_confirmacion_macro: float = 0.6
    min_dist_pct: float = 0.0005
    min_dist_pct_overrides: Dict[str, float] = field(default_factory=dict)
    fracciones_piramide: int = 1
    reserva_piramide: float = 0.0
    umbral_piramide: float = 0.006
    max_perdidas_diarias: int = 6
    volumen_min_relativo: float = 1.0
    max_spread_ratio: float = 0.003  # valor base
    spread_dynamic: bool = True  # activar ajuste dinámico de spread
    diversidad_minima: int = 2
    max_posiciones_cartera: int = 0
    max_posiciones_mismo_sentido: int = 0
    regimen_entrada_enabled: bool = False
    regimen_vol_atr_ratio_alto: float = 0.025
    regimen_vol_atr_ratio_bajo: float = 0.008
    regimen_atr_periodo: int = 14
    regimen_mult_umbral_alta: float = 1.0
    regimen_mult_umbral_media: float = 1.0
    regimen_mult_umbral_baja: float = 1.0
    regimen_mult_umbral_score_alta: float = 1.0
    regimen_mult_umbral_score_media: float = 1.0
    regimen_mult_umbral_score_baja: float = 1.0
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
    # Alineado con ``DataFeed`` (``DF_HANDLER_TIMEOUT_SEC`` default 180) y Fase 1:
    # el pipeline real supera con frecuencia 12s; 12s aquí anulaba el umbral largo vía Config.
    handler_timeout: float = 180.0
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
    df_queue_min_recommended: int = 16
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
    trader_fastpath_threshold: int = 800
    trader_fastpath_resume_threshold: int = 600
    trader_fastpath_recovery: int = 200
    trader_fastpath_skip_notifications: bool = True
    trader_fastpath_skip_entries: bool = False
    trader_fastpath_skip_trend: bool = True
    trader_buffer_isolated: bool = False
    timeout_evaluar_condiciones_por_symbol: Dict[str, int] = field(default_factory=dict)
    indicadores_normalize_default: bool = True
    indicadores_cache_max_entries: int = 128
    indicadores_incremental_enabled: bool = False
    orders_retry_persistencia_enabled: bool = False
    trader_purge_historial_enabled: bool = False
    metrics_extended_enabled: bool = False
    datafeed_debug_wrapper_enabled: bool = False
    orders_flush_periodico_enabled: bool = False
    orders_limit_enabled: bool = False
    orders_execution_policy: str = "market"
    orders_execution_policy_by_symbol: Dict[str, str] = field(default_factory=dict)
    orders_reconcile_enabled: bool = False
    funding_enabled: bool = False
    backfill_ventana_enabled: bool = False
    risk_capital_total: float = 0.0
    risk_capital_default_per_symbol: float = 0.0
    risk_capital_per_symbol: Dict[str, float] = field(default_factory=dict)
    risk_kelly_base: float = 0.1
    risk_capital_divergence_threshold: float = 0.1
    momentum_activation_threshold: float = 0.001
    momentum_threshold_overrides: Dict[str, float] = field(default_factory=dict)
    entrada_cooldown_tras_crear_failed_sec: float = 300.0
    entrada_cooldown_tras_crear_failed_por_symbol: Dict[str, float] = field(default_factory=dict)
    # Evita re-emitir la misma candidatura (mismo TF + vela + lado) en re-evaluaciones del mismo cierre.
    entrada_dedupe_por_vela: bool = True
