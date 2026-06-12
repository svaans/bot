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
    # Filtro noticias: off por defecto hasta validar con datos reales en vivo.
    # Activar con FILTRO_NOTICIAS_ENABLED=true cuando el bot lleve 30+ días.
    filtro_noticias_enabled: bool = False
    noticias_umbral_negativo: float = -0.3
    # RSI mínimo de entrada: 50 = confirmar momentum alcista antes de entrar.
    # Validado en backtesting 5yr: +0.74pp anual (+22.60% → +23.34%), PF 2.79.
    rsi_min_entrada: float = 50.0
    # ADX mínimo: 20 = solo entrar en mercados con tendencia definida.
    # Validado en study4: elimina entradas en laterales, mejora PF OOS.
    adx_min_entrada: float = 20.0
    # DCA: compra automática semanal si no hay posición abierta.
    dca_enabled: bool = True
    dca_interval_days: int = 7
    dca_amount_usdt: float = 0.0
    # Rebalancer: activo para mantener 20% equal-weight por símbolo.
    rebalancer_enabled: bool = True
    rebalancer_drift_threshold: float = 0.05
