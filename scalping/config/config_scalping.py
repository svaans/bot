"""Configuración específica para el modo scalping."""

from dataclasses import dataclass
from typing import Dict

@dataclass
class ConfigScalping:
    symbol: str
    take_profit_pct: float = 0.1  # 0.1% por defecto
    stop_loss_pct: float = 0.05   # 0.05% por defecto
    max_duracion_min: int = 10
    buffer_trailing_pct: float = 0.02

# Configuración por símbolo
CONFIGS: Dict[str, ConfigScalping] = {
    "BTC/EUR": ConfigScalping("BTC/EUR", 0.15, 0.07, 5, 0.03),
    "ETH/EUR": ConfigScalping("ETH/EUR", 0.2, 0.1, 5, 0.03),
    "SOL/EUR": ConfigScalping("SOL/EUR", 0.25, 0.12, 5, 0.04),
}