from dataclasses import dataclass, field
from typing import List
from .development import DevelopmentConfig


@dataclass(frozen=True)
class ProductionConfig(DevelopmentConfig):
    """Configuración base para producción."""
    modo_real: bool = True
    intervalo_velas: str = '5m'
    symbols: List[str] = field(default_factory=lambda : ['BTC/EUR',
        'ETH/EUR', 'ADA/EUR', 'SOL/EUR', 'BNB/EUR'])
