from typing import Dict
import pandas as pd

from estrategias_entrada.gestor_entradas import evaluar_estrategias
from estrategias_salida.gestor_salidas import evaluar_salidas
from core.logger import configurar_logger


log = configurar_logger("engine", modo_silencioso=True)


class StrategyEngine:
    """Evalúa estrategias de entrada y salida."""

    def evaluar_entrada(self, df: pd.DataFrame) -> Dict[str, float]:
        return evaluar_estrategias(df)

    def evaluar_salida(self, df: pd.DataFrame, orden: Dict) -> Dict:
        return evaluar_salidas(df, orden)