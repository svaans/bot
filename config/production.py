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
