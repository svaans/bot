"""Gestión de riesgo del bot."""

from __future__ import annotations

from core.riesgo import riesgo_superado as _riesgo_superado, actualizar_perdida
from core.reporting import reporter_diario
import numpy as np
from core.logger import configurar_logger


log = configurar_logger("risk", modo_silencioso=True)


class RiskManager:
    """Encapsula la lógica de control de riesgo."""

    def __init__(self, umbral: float) -> None:
        self.umbral = umbral

    def riesgo_superado(self, capital_total: float) -> bool:
        """Indica si el capital perdido supera el umbral configurado."""
        return _riesgo_superado(self.umbral, capital_total)

    def registrar_perdida(self, symbol: str, perdida: float) -> None:
        """Registra una pérdida para ``symbol``."""
        actualizar_perdida(symbol, perdida)

    def ajustar_umbral(self, segun_metricas: dict) -> None:
        """Modifica ``self.umbral`` de acuerdo al desempeño reciente.

        ``segun_metricas`` debe contener ``ganancia_semana`` y ``drawdown``.
        Si la ganancia semanal es mayor a 5% se incrementa ligeramente el
        umbral. Si el drawdown es negativo por debajo de -5%, se reduce.
        """
        if not isinstance(segun_metricas, dict):
            return

        ganancia = segun_metricas.get("ganancia_semana", 0.0)
        drawdown = segun_metricas.get("drawdown", 0.0)
        if not isinstance(ganancia, (int, float)) or not isinstance(drawdown, (int, float)):
            log.warning("⚠️ Métricas inválidas para ajuste de riesgo")
            return
        if np.isnan(ganancia) or np.isnan(drawdown):
            log.warning("⚠️ Métricas NaN para ajuste de riesgo")
            return

        anterior = self.umbral

        if ganancia > 0.05:
            self.umbral = round(min(0.5, self.umbral * 1.05), 4)
        elif drawdown < -0.05:
            self.umbral = round(max(0.01, self.umbral * 0.9), 4)

        if self.umbral != anterior:
            log.info(f"🔧 Umbral ajustado de {anterior:.4f} a {self.umbral:.4f}")

    def multiplicador_kelly(self, n_trades: int = 10) -> float:
        """Calcula un multiplicador para la fracción Kelly basándose en los
        últimos ``n_trades`` registrados.

        Retorna ``1.0`` si no hay datos suficientes o si ocurre algún error.
        """
        try:
            operaciones: list[dict] = []
            for ops in reporter_diario.ultimas_operaciones.values():
                operaciones.extend(ops[-n_trades:])
            if not operaciones:
                return 1.0
            operaciones = operaciones[-n_trades:]
            retornos = [
                float(o.get("retorno_total", 0.0))
                for o in operaciones
                if isinstance(o.get("retorno_total"), (int, float))
                and not np.isnan(o.get("retorno_total"))
            ]
            if not retornos:
                return 1.0
            promedio = sum(retornos) / len(retornos)
            factor = 1 + promedio
            factor = round(max(0.5, min(1.5, factor)), 3)
            log.debug(f"🔧 Multiplicador Kelly calculado: {factor:.3f}")
            return factor
        except Exception as e:  # noqa: BLE001
            log.warning(f"⚠️ Error calculando multiplicador Kelly: {e}")
            return 1.0

    def factor_volatilidad(
        self,
        volatilidad_actual: float,
        volatilidad_media: float,
        umbral: float = 2.0,
    ) -> float:
        """Devuelve un factor reductor para la fracción de posición.

        Si ``volatilidad_actual`` supera ``umbral`` veces ``volatilidad_media``
        se aplica una reducción inversamente proporcional al exceso.
        """
        if (
            not isinstance(volatilidad_actual, (int, float))
            or not isinstance(volatilidad_media, (int, float))
            or volatilidad_media <= 0
            or volatilidad_actual <= 0
        ):
            return 1.0

        limite = volatilidad_media * umbral
        if volatilidad_actual <= limite:
            return 1.0

        exceso = volatilidad_actual / limite
        factor = 1 / exceso
        return max(0.5, min(1.0, round(factor, 3)))