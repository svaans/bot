from dataclasses import dataclass, field
from typing import List

from core.operational_mode import OperationalMode

from .development import DevelopmentConfig


@dataclass(frozen=True)
class ProductionConfig(DevelopmentConfig):
    """Configuración base para producción."""
    modo_real: bool = True
    modo_operativo: OperationalMode = OperationalMode.REAL
    intervalo_velas: str = '1d'  # ver nota en DevelopmentConfig.intervalo_velas
    # BTC, ETH, SOL: PF test 1.48-3.97 en backtesting 5yr (estudio_simbolos).
    # XRP, AVAX: PF test 1.31-1.48 — reemplazan ADA (PF 0.57) y BNB (PF 1.02).
    symbols: List[str] = field(default_factory=lambda: [
        "BTC/USDT", "ETH/USDT", "SOL/USDT", "XRP/USDT", "AVAX/USDT"
    ])
    contradicciones_bloquean_entrada: bool = True
    entrada_cooldown_tras_crear_failed_sec: float = 0.0
    # Guardia adaptativa de volatilidad (calibración en DevelopmentConfig).
    regimen_entrada_enabled: bool = True
    # Filtro macro BTC: no comprar mientras BTC < EMA200 (ver filtro_macro.py).
    filtro_macro_btc_enabled: bool = True
    # Fear & Greed: bloquear entradas en codicia extrema (>75).
    filtro_fear_greed_enabled: bool = True
    fg_umbral_codicia: int = 75
    # zona_neutral: bloquear también pánico extremo (F&G < 25).
    # Estudio fear_greed: zona_neutral → +15.58% test vs +14.96% solo codicia.
    fg_umbral_miedo: int = 25
    # Límite alts correladas: máx 2 de {SOL, XRP, AVAX} simultáneas.
    max_posiciones_alts: int = 2
    # Rolling PF Guard: si el PF de las últimas 20 ops cae < 0.7, pausar entradas.
    pf_guard_enabled: bool = True
    # Equity DD Filter: reducir riesgo a la mitad cuando DD > 10%.
    # Estudio equity_filter: reducir_10% → DD↓1.4pp sin pérdida de retorno.
    equity_dd_filter_enabled: bool = True
    equity_dd_reduccion_umbral: float = 0.10
    # Per-Symbol Loss Streak Guard: activo en producción.
    # Tras 2 pérdidas consecutivas en un símbolo, reduce su riesgo al 50%.
    per_symbol_guard_enabled: bool = True
    per_symbol_losses_umbral: int = 2
    # Filtro noticias: off por defecto hasta validar con datos reales en vivo.
    # Activar con FILTRO_NOTICIAS_ENABLED=true cuando el bot lleve 30+ días.
    filtro_noticias_enabled: bool = False
    noticias_umbral_negativo: float = -0.3
    # RSI momentum filter: bloquea entradas cuando RSI < 50.
    # Estudio rsi_momentum (5yr, validación 30%): +0.74pp anual, PF 2.64→2.79, Sortino +1.71.
    rsi_min_entrada: float = 50.0
    # ── Robo-Advisor: DCA semanal automático ────────────────────────────────
    # Estudio: DCA reduce timing risk y mejora Sharpe en mercados volátiles.
    # Se combina con señales técnicas: DCA solo si filtros macro/riesgo OK.
    dca_enabled: bool = True
    dca_interval_days: int = 7       # DCA semanal
    dca_amount_usdt: float = 0.0     # 0 = usa capital disponible por símbolo
    # ── Robo-Advisor: Rebalanceo automático ─────────────────────────────────
    # Drift >5% desde peso objetivo → generar señal de rebalanceo.
    rebalancer_enabled: bool = True
    rebalancer_drift_threshold: float = 0.05
    # Fastpath de trader: mecanismo para velas 5m/15m que evita procesar
    # entradas cuando la cola está saturada (>800 velas pendientes).
    # En 1d hay máximo 1 vela/día → threshold nunca se alcanza → apagado
    # explícitamente para evitar checks condicionales innecesarios.
    trader_fastpath_enabled: bool = False
