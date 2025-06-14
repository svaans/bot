from dataclasses import dataclass
from typing import List
import os
from dotenv import load_dotenv
from pathlib import Path

from core.logger import configurar_logger

log = configurar_logger("config_manager")


def _cargar_float(clave, valor_defecto):
    try:
        return float(os.getenv(clave, valor_defecto))
    except ValueError:
        log.warning(f"⚠️ Valor inválido para {clave}. Usando valor por defecto {valor_defecto}")
        return float(valor_defecto)


def _cargar_int(clave, valor_defecto):
    try:
        return int(os.getenv(clave, valor_defecto))
    except ValueError:
        log.warning(f"⚠️ Valor inválido para {clave}. Usando valor por defecto {valor_defecto}")
        return int(valor_defecto)


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
    persistencia_minima: int = 2
    peso_extra_persistencia: float = 0.5
    modo_capital_bajo: bool = False
    telegram_token: str | None = None
    telegram_chat_id: str | None = None
    umbral_score_tecnico: float = 1.0
    usar_score_tecnico: bool = True
    contradicciones_bloquean_entrada: bool = True
    registro_tecnico_csv: str = "logs/rechazos_tecnico.csv"
    fracciones_piramide: int = 1
    reserva_piramide: float = 0.0
    umbral_piramide: float = 0.005


class ConfigManager:
    """Carga y proporciona acceso a la configuración del bot."""

    @staticmethod
    def load_from_env() -> Config:
        env_path = Path(__file__).resolve().parent.parent / "config" / "claves.env"
        load_dotenv(env_path)

        symbols_env = os.getenv("SYMBOLS", "BTC/EUR,ETH/EUR,ADA/EUR,SOL/EUR,BNB/EUR")
        symbols = [s.strip().upper() for s in symbols_env.split(",") if s.strip()]

        api_key = os.environ.get("BINANCE_API_KEY")
        api_secret = os.environ.get("BINANCE_API_SECRET")

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
            intervalo_velas=os.getenv("INTERVALO_VELAS", "5m"),
            symbols=symbols,
            umbral_riesgo_diario=_cargar_float("UMBRAL_RIESGO_DIARIO", 0.03),
            min_order_eur=_cargar_float("MIN_ORDER_EUR", 10),
            persistencia_minima=_cargar_int("PERSISTENCIA_MINIMA", 2),
            peso_extra_persistencia=_cargar_float("PESO_EXTRA_PERSISTENCIA", 0.5),
            modo_capital_bajo=os.getenv("MODO_CAPITAL_BAJO", "False").lower() == "true",
            telegram_token=os.getenv("TELEGRAM_TOKEN"),
            telegram_chat_id=os.getenv("TELEGRAM_CHAT_ID"),
            umbral_score_tecnico=_cargar_float("UMBRAL_SCORE_TECNICO", 3.0),
            usar_score_tecnico=os.getenv("USAR_SCORE_TECNICO", "True").lower() == "true",
            contradicciones_bloquean_entrada=os.getenv("CONTRADICCIONES_BLOQUEAN_ENTRADA", "False").lower() == "true",
            registro_tecnico_csv=os.getenv("REGISTRO_TECNICO_CSV", "logs/rechazos_tecnico.csv"),
            fracciones_piramide=int(os.getenv("FRACCIONES_PIRAMIDE", 1)),
            reserva_piramide=float(os.getenv("RESERVA_PIRAMIDE", 0)),
            umbral_piramide=float(os.getenv("UMBRAL_PIRAMIDE", 0.006)),
        )
