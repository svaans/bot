from core.riesgo import riesgo_superado, actualizar_perdida
from core.logger import configurar_logger


log = configurar_logger("risk", modo_silencioso=True)


class RiskManager:
    """Gestiona las comprobaciones de riesgo."""

    def __init__(self, umbral: float) -> None:
        self.umbral = umbral

    def riesgo_superado(self, capital_total: float) -> bool:
        return riesgo_superado(self.umbral, capital_total)

    def registrar_perdida(self, symbol: str, perdida: float) -> None:
        actualizar_perdida(symbol, perdida)