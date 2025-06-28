from dataclasses import dataclass, field
from typing import List

from .development import DevelopmentConfig

@dataclass(frozen=True)
class ProductionConfig(DevelopmentConfig):
    """Configuración base para producción."""
    modo_real: bool = True
    intervalo_velas: str = "5m"
    symbols: List[str] = field(default_factory=lambda: [
        "BTC/EUR",
        "ETH/EUR",
        "ADA/EUR",
        "SOL/EUR",
        "BNB/EUR",
    ])
    candle_host: str = "localhost"
    candle_port: int = 9000
    ws_service_host: str = "localhost"
    ws_service_port: int = 8765
    orders_worker_host: str = "localhost"
    orders_worker_port: int = 9100
    backtest_grpc_host: str = "localhost"
    backtest_grpc_port: int = 9200
    use_grpc_backtest: bool = False