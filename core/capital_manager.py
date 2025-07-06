from __future__ import annotations
import asyncio
from datetime import datetime
from typing import Dict
from config.config_manager import Config
from binance_api.cliente import fetch_balance_async, load_markets_async
from core.utils.logger import configurar_logger
from core.risk import RiskManager
log = configurar_logger('capital_manager', modo_silencioso=True)


class CapitalManager:
    """Gestiona el capital disponible para trading."""

    def __init__(self, config: Config, cliente, risk: RiskManager,
        fraccion_kelly: float) ->None:
        log.info('➡️ Entrando en __init__()')
        self.config = config
        self.cliente = cliente
        self.risk = risk
        self.fraccion_kelly = fraccion_kelly
        self.modo_real = getattr(config, 'modo_real', False)
        self.modo_capital_bajo = config.modo_capital_bajo
        self.riesgo_maximo_diario = 1.0
        self._markets = None
        euros = 0.0
        if self.modo_real and self.cliente:
            try:
                balance = self.cliente.fetch_balance()
                euros = balance['total'].get('EUR', 0)
            except Exception as e:
                log.error(f'❌ Error al obtener balance: {e}')
        else:
            euros = 1000.0
        inicial = euros / max(len(config.symbols), 1)
        inicial = max(inicial, 20.0)
        self.capital_por_simbolo: Dict[str, float] = {s: inicial for s in
            config.symbols}
        self.capital_inicial_diario = self.capital_por_simbolo.copy()
        self.reservas_piramide: Dict[str, float] = {s: (0.0) for s in
            config.symbols}
        self.fecha_actual = datetime.utcnow().date()

    async def _obtener_minimo_binance(self, symbol: str) ->(float | None):
        log.info('➡️ Entrando en _obtener_minimo_binance()')
        if not self.modo_real or not self.cliente:
            return None
        try:
            if self._markets is None:
                self._markets = await load_markets_async(self.cliente)
            info = self._markets.get(symbol.replace('/', ''))
            minimo = info.get('limits', {}).get('cost', {}).get('min'
                ) if info else None
            return float(minimo) if minimo else None
        except Exception as e:
            log.debug(f'No se pudo obtener mínimo para {symbol}: {e}')
            return None

    async def calcular_cantidad_async(self, symbol: str, precio: float
        ) ->float:
        log.info('➡️ Entrando en calcular_cantidad_async()')
        if self.modo_real and self.cliente:
            balance = await fetch_balance_async(self.cliente)
            euros = balance['total'].get('EUR', 0)
        else:
            euros = self.capital_por_simbolo.get(symbol, 0)
        if euros <= 0:
            log.debug('Saldo en EUR insuficiente')
            return 0.0
        capital_symbol = self.capital_por_simbolo.get(symbol, euros / max(
            len(self.capital_por_simbolo), 1))
        fraccion = self.fraccion_kelly
        if self.modo_capital_bajo and euros < 500:
            deficit = (500 - euros) / 500
            fraccion = max(fraccion, 0.02 + deficit * 0.1)
        riesgo_teorico = capital_symbol * fraccion * self.risk.umbral
        minimo_dinamico = max(10.0, euros * 0.02)
        riesgo = max(riesgo_teorico, minimo_dinamico)
        riesgo = min(riesgo, euros * self.riesgo_maximo_diario)
        riesgo = min(riesgo, euros)
        minimo_binance = await self._obtener_minimo_binance(symbol)
        cantidad = riesgo / precio
        if cantidad * precio < minimo_dinamico:
            log.debug(
                f'Orden mínima {minimo_dinamico:.2f}€, intento {cantidad * precio:.2f}€'
                )
            return 0.0
        log.info(
            '⚖️ Kelly ajustada: %.4f | Riesgo teórico: %.2f€ | Mínimo dinámico: %.2f€ | Riesgo final: %.2f€'
            , fraccion, riesgo_teorico, minimo_dinamico, riesgo)
        log.info(
            '📊 Capital disponible: %.2f€ | Orden: %.2f€ | Mínimo Binance: %s | %s'
            , euros, cantidad * precio, f'{minimo_binance:.2f}€' if
            minimo_binance else 'desconocido', symbol)
        return round(cantidad, 6)

    def calcular_cantidad(self, symbol: str, precio: float) ->float:
        log.info('➡️ Entrando en calcular_cantidad()')
        return asyncio.run(self.calcular_cantidad_async(symbol, precio))

    def actualizar_capital(self, symbol: str, retorno_total: float) ->float:
        log.info('➡️ Entrando en actualizar_capital()')
        capital_inicial = self.capital_por_simbolo.get(symbol, 0.0)
        ganancia = capital_inicial * retorno_total
        capital_final = capital_inicial + ganancia
        self.capital_por_simbolo[symbol] = capital_final
        return capital_final
