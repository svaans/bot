from dataclasses import dataclass, field
from typing import List


@dataclass(frozen=True)
class DevelopmentConfig:
    """Valores por defecto para el entorno de desarrollo."""
    modo_real: bool = False
    intervalo_velas: str = '1m'
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
    fracciones_piramide: int = 1
    reserva_piramide: float = 0.0
    umbral_piramide: float = 0.006
    max_perdidas_diarias: int = 6
    volumen_min_relativo: float = 1.0
    max_spread_ratio: float = 0.003
    diversidad_minima: int = 2
    timeout_verificar_salidas: int = 20
    timeout_evaluar_condiciones: int = 15
    timeout_cerrar_operacion: int = 20
    timeout_abrir_operacion: int = 20
    timeout_bus_eventos: int = 10
    max_timeouts_salidas: int = 3
    heartbeat_interval: int = 60
    monitor_interval: int = 5
    max_stream_restarts: int = 5
    inactivity_intervals: int = 3
    handler_timeout: int = 5
    frecuencia_tendencia: int = 3
    frecuencia_correlaciones: int = 300
    umbral_alerta_cpu: float = 85.0
    umbral_alerta_mem: float = 90.0
    ciclos_alerta_recursos: int = 5
    frecuencia_recursos: int = 60
