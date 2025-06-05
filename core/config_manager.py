from dataclasses import dataclass
from typing import List
import os


@dataclass(frozen=True)
class Config:
    """Configuración inmutable cargada desde el entorno."""
    api_key: str
    api_secret: str
    modo_real: bool
    intervalo_velas: str
    symbols: List[str]
    


class ConfigManager:
    """Carga y proporciona acceso a la configuración del bot."""

    @staticmethod
    def load_from_env() -> Config:
        symbols = os.getenv("SYMBOLS", "BTC/EUR,ETH/EUR,ADA/EUR").split(",")
        return Config(
            api_key=os.environ.get("BINANCE_API_KEY"),
            api_secret=os.environ.get("BINANCE_API_SECRET"),
            modo_real=os.getenv("MODO_REAL", "False").lower() == "true",
            intervalo_velas=os.getenv("INTERVALO_VELAS", "1m"),
            symbols=[s.strip() for s in symbols if s.strip()],
        )