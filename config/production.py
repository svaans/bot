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
    symbols: List[str] = field(default_factory=lambda: ["BTC/USDT", "ETH/USDT"])
    contradicciones_bloquean_entrada: bool = True
    entrada_cooldown_tras_crear_failed_sec: float = 0.0
    # Guardia adaptativa de volatilidad (calibración en DevelopmentConfig).
    regimen_entrada_enabled: bool = True
    # Filtro macro BTC: no comprar mientras BTC < EMA200 (ver filtro_macro.py).
    filtro_macro_btc_enabled: bool = True
