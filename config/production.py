from dataclasses import dataclass, field
from typing import List

from core.operational_mode import OperationalMode

from .development import DevelopmentConfig


@dataclass(frozen=True)
class ProductionConfig(DevelopmentConfig):
    """Configuración base para producción."""
    modo_real: bool = True
    modo_operativo: OperationalMode = OperationalMode.REAL
    intervalo_velas: str = '5m'
    symbols: List[str] = field(default_factory=lambda : ['BTC/EUR',
        'ETH/EUR', 'ADA/EUR', 'SOL/EUR', 'BNB/EUR'])
    contradicciones_bloquean_entrada: bool = True
