from dataclasses import dataclass
from typing import List
import os
from dotenv import load_dotenv
from pathlib import Path

from core.logger import configurar_logger

log = configurar_logger("config_manager")

@dataclass(frozen=True)
class Config:
    """Configuración inmutable cargada desde el entorno."""
    api_key: str
    api_secret: str
    modo_real: bool
    intervalo_velas: str
    symbols: List[str]
    umbral_riesgo_diario: float
    min_order_eur: float


class ConfigManager:
    """Carga y proporciona acceso a la configuración del bot."""

    @staticmethod
    def load_from_env() -> Config:
        env_path = Path(__file__).resolve().parent.parent / "config" / "claves.env"
        load_dotenv(env_path)
        symbols_env = os.getenv("SYMBOLS", "BTC/EUR,ETH/EUR,ADA/EUR")

        api_key = os.environ.get("BINANCE_API_KEY")
        api_secret = os.environ.get("BINANCE_API_SECRET")
        symbols = [s.strip() for s in symbols_env.split(",") if s.strip()]

        missing = []
        if not api_key:
            missing.append("BINANCE_API_KEY")
        if not api_secret:
            missing.append("BINANCE_API_SECRET")
        if not symbols:
            missing.append("SYMBOLS")

        if missing:
            datos = ", ".join(missing)
            log.error(f"❌ Faltan variables de entorno requeridas: {datos}")
            raise ValueError(f"Faltan datos de configuración: {datos}")
        return Config(
            api_key=api_key,
            api_secret=api_secret,
            modo_real=os.getenv("MODO_REAL", "False").lower() == "true",
            intervalo_velas=os.getenv("INTERVALO_VELAS", "1m"),
            symbols=symbols,
            umbral_riesgo_diario=float(os.getenv("UMBRAL_RIESGO_DIARIO", 0.03)),
            min_order_eur=float(os.getenv("MIN_ORDER_EUR", 10)),
        )
