"""Gestión de riesgo del bot."""
from __future__ import annotations
import numpy as np
from core.utils.utils import configurar_logger
import core.risk_manager as risk_alias
from core.reporting import reporter_diario
from core.event_bus import EventBus
from typing import Any
log = configurar_logger('risk', modo_silencioso=True)


class RiskManager:
    """Encapsula la lógica de control de riesgo del bot."""

    def __init__(self, umbral: float, bus: EventBus | None = None) -> None:
        log.info('➡️ Entrando en __init__()')
        self.umbral = umbral
        self._factor_kelly_prev = None
        if bus:
            self.subscribe(bus)

    def subscribe(self, bus: EventBus) -> None:
        bus.subscribe('registrar_perdida', self._on_registrar_perdida)
        bus.subscribe('ajustar_riesgo', self._on_ajustar_riesgo)

    async def _on_registrar_perdida(self, data: Any) -> None:
        self.registrar_perdida(data.get('symbol'), data.get('perdida', 0.0))

    async def _on_ajustar_riesgo(self, data: Any) -> None:
        self.ajustar_umbral(data)

    def riesgo_superado(self, capital_total: float) ->bool:
        log.info('➡️ Entrando en riesgo_superado()')
        """Indica si el capital perdido supera el umbral configurado."""
        return risk_alias._riesgo_superado(self.umbral, capital_total)

    def registrar_perdida(self, symbol: str, perdida: float) ->None:
        log.info('➡️ Entrando en registrar_perdida()')
        """Registra una pérdida para ``symbol``."""
        if perdida < 0:
            risk_alias.actualizar_perdida(symbol, perdida)

    def ajustar_umbral(self, segun_metricas: dict) ->None:
        log.info('➡️ Entrando en ajustar_umbral()')
        """Ajusta ``self.umbral`` usando métricas recientes.

        ``segun_metricas`` puede incluir las siguientes claves:

        - ``ganancia_semana`` (float): ganancia semanal acumulada.
        - ``drawdown`` (float): pérdida máxima registrada en la semana.
        - ``winrate`` (float): winrate de las últimas operaciones (0-1).
        - ``capital_actual`` (float): capital disponible en la cuenta.
        - ``capital_inicial`` (float): capital al inicio del periodo.

        Reglas de ajuste:
        - Si ``ganancia_semana`` > 5% → aumenta el umbral hasta un máximo de
          ``0.5``.
        - Si ``drawdown`` < -5% → reduce el umbral hasta un mínimo de ``0.01``.
        - Si ``winrate`` > 0.6 **y** ``capital_actual`` > ``capital_inicial`` →
          incrementa un 20 % el umbral sin superar ``0.06``.
        """
        if not isinstance(segun_metricas, dict):
            log.warning(
                '⚠️ Métricas de riesgo no proporcionadas como diccionario')
            return
        ganancia = segun_metricas.get('ganancia_semana', 0.0)
        drawdown = segun_metricas.get('drawdown', 0.0)
        winrate = segun_metricas.get('winrate')
        capital_actual = segun_metricas.get('capital_actual')
        capital_inicial = segun_metricas.get('capital_inicial')
        if not isinstance(ganancia, (int, float)) or not isinstance(drawdown,
            (int, float)):
            log.warning('⚠️ Métricas inválidas para ajuste de riesgo')
            return
        if np.isnan(ganancia) or np.isnan(drawdown):
            log.warning('⚠️ Métricas NaN detectadas')
            return
        umbral_anterior = self.umbral
        if ganancia > 0.05:
            self.umbral = round(min(0.5, self.umbral * 1.05), 4)
        elif drawdown < -0.05:
            self.umbral = round(max(0.01, self.umbral * 0.9), 4)
        if isinstance(winrate, (int, float)) and isinstance(capital_actual,
            (int, float)) and isinstance(capital_inicial, (int, float)):
            if winrate > 0.6 and capital_actual > capital_inicial:
                self.umbral = round(min(0.06, self.umbral * 1.2), 4)
        vol_market = segun_metricas.get('volatilidad_market')
        vol_media = segun_metricas.get('volatilidad_media')
        if isinstance(vol_market, (int, float)) and isinstance(vol_media, (int, float)):
            if vol_market > vol_media * 1.5:
                self.umbral = round(max(0.01, self.umbral * 0.9), 4)
        exposicion = segun_metricas.get('exposicion_actual')
        if isinstance(exposicion, (int, float)) and exposicion > 0.5:
            self.umbral = round(max(0.01, self.umbral * 0.8), 4)
        corr_media = segun_metricas.get('correlacion_media')
        if isinstance(corr_media, (int, float)) and corr_media > 0.8:
            self.umbral = round(max(0.01, self.umbral * (1 - min(corr_media, 1))), 4)
        if self.umbral != umbral_anterior:
            log.info(
                f'🔧 Umbral ajustado de {umbral_anterior:.4f} → {self.umbral:.4f}'
                )

    def multiplicador_kelly(self, n_trades: int=10) ->float:
        log.info('➡️ Entrando en multiplicador_kelly()')
        """
        Calcula un factor de ajuste para la fracción de Kelly.

        Se basa en los últimos ``n_trades`` y su retorno.
        - Si no hay datos o fallan los cálculos, retorna 1.0 (sin ajuste).
        - El resultado se limita entre 0.5 y 1.5 para evitar extremos.
        """
        try:
            operaciones = []
            for trades in reporter_diario.ultimas_operaciones.values():
                operaciones.extend(trades[-n_trades:])
            operaciones = operaciones[-n_trades:]
            if not operaciones:
                return 1.0
            retornos = [float(o.get('retorno_total', 0.0)) for o in
                operaciones if isinstance(o.get('retorno_total'), (int,
                float)) and not np.isnan(o.get('retorno_total'))]
            if not retornos:
                return 1.0
            promedio = sum(retornos) / len(retornos)
            factor = max(0.5, min(1.5, 1 + promedio))
            if self._factor_kelly_prev is not None:
                factor = 0.7 * self._factor_kelly_prev + 0.3 * factor
            self._factor_kelly_prev = factor
            factor = round(factor, 3)
            log.debug(f'🔧 Multiplicador Kelly calculado: {factor:.3f}')
            return factor
        except Exception as e:
            log.warning(f'⚠️ Error calculando multiplicador Kelly: {e}')
            return 1.0

    def factor_volatilidad(self, volatilidad_actual: float,
        volatilidad_media: float, umbral: float=2.0) ->float:
        log.info('➡️ Entrando en factor_volatilidad()')
        """
        Devuelve un factor reductor si la volatilidad actual es anómala.

        Si la volatilidad actual excede en más de ``umbral`` veces a la media,
        se penaliza la exposición reduciendo el tamaño de la posición.
        """
        if not isinstance(volatilidad_actual, (int, float)) or not isinstance(
            volatilidad_media, (int, float)
            ) or volatilidad_actual <= 0 or volatilidad_media <= 0:
            return 1.0
        exceso = volatilidad_actual / (volatilidad_media * umbral)
        if exceso <= 1.0:
            return 1.0
        factor = 1.0 / exceso
        factor = round(max(0.5, min(1.0, factor)), 3)
        log.info(
            f'🌪️ Volatilidad excesiva, aplicando factor de reducción: {factor}'
            )
        return factor
