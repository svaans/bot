from __future__ import annotations
import asyncio
from collections import Counter
from datetime import datetime
from typing import Dict
from config.config_manager import Config
from binance_api.cliente import fetch_balance_async, load_markets_async
from core.utils.logger import configurar_logger
from core.risk import RiskManager
from core.event_bus import EventBus
from core.contexto_externo import obtener_puntaje_contexto
log = configurar_logger('capital_manager', modo_silencioso=False)


class CapitalManager:
    """Gestiona el capital disponible para trading."""

    def __init__(self, config: Config, cliente, risk: RiskManager,
        fraccion_kelly: float, bus: EventBus | None = None) -> None:
        log.info('➡️ Entrando en __init__()')
        self.config = config
        self.cliente = cliente
        self.risk = risk
        self.fraccion_kelly = fraccion_kelly
        self.modo_real = getattr(config, 'modo_real', False)
        self.modo_capital_bajo = config.modo_capital_bajo
        self.riesgo_maximo_diario = 1.0
        self._markets = None
        self.capital_currency = getattr(config, 'capital_currency', None)
        if not self.capital_currency:
            self.capital_currency = self._detectar_divisa_principal(config.symbols)
        capital_total = 0.0
        if self.modo_real and self.cliente:
            try:
                balance = self.cliente.fetch_balance()
                capital_total = balance['total'].get(self.capital_currency, 0)
            except Exception as e:
                log.error(f'❌ Error al obtener balance: {e}')
        else:
            capital_total = 1000.0
        inicial = capital_total / max(len(config.symbols), 1)
        inicial = max(inicial, 20.0)
        self.capital_por_simbolo: Dict[str, float] = {s: inicial for s in config.symbols}
        self.capital_inicial_diario = self.capital_por_simbolo.copy()
        self.reservas_piramide: Dict[str, float] = {s: 0.0 for s in config.symbols}
        self.fecha_actual = datetime.utcnow().date()
        if bus:
            self.subscribe(bus)

    @staticmethod
    def _detectar_divisa_principal(symbols: list[str]) -> str:
        monedas = [s.split('/')[-1] for s in symbols if '/' in s]
        if not monedas:
            return 'EUR'
        return Counter(monedas).most_common(1)[0][0]

    def subscribe(self, bus: EventBus) -> None:
        bus.subscribe('calcular_cantidad', self._on_calcular_cantidad)
        bus.subscribe('actualizar_capital', self._on_actualizar_capital)

    async def _on_calcular_cantidad(self, data: dict) -> None:
        fut = data.get('future')
        symbol = data.get('symbol')
        precio = data.get('precio')
        exposicion = data.get('exposicion_total', 0.0)
        stop_loss = data.get('stop_loss')
        if fut:
            cantidad = await self.calcular_cantidad_async(
                symbol,
                precio,
                exposicion_total=exposicion,
                stop_loss=stop_loss,
            )
            fut.set_result(cantidad)

    async def _on_actualizar_capital(self, data: dict) -> None:
        fut = data.get('future')
        symbol = data.get('symbol')
        retorno = data.get('retorno_total', 0.0)
        if fut:
            fut.set_result(self.actualizar_capital(symbol, retorno))

    async def _obtener_minimo_binance(self, symbol: str) ->(float | None):
        log.info('➡️ Entrando en _obtener_minimo_binance()')
        if not self.modo_real or not self.cliente:
            return None
        try:
            if self._markets is None:
                self._markets = await load_markets_async(self.cliente)
            info = self._markets.get(symbol.replace('/', ''))
            minimo = info.get('limits', {}).get('cost', {}).get('min') if info else None
            return float(minimo) if minimo else None
        except Exception as e:
            log.warning(f'No se pudo obtener mínimo para {symbol}: {e}')
        return None

    async def calcular_cantidad_async(
        self,
        symbol: str,
        precio: float,
        exposicion_total: float = 0.0,
        stop_loss: float | None = None,
    ) -> float:
        log.info('➡️ Entrando en calcular_cantidad_async()')
        if self.modo_real and self.cliente:
            balance = await fetch_balance_async(self.cliente)
            capital_total = balance['total'].get(self.capital_currency, 0)
        else:
            capital_total = self.capital_por_simbolo.get(symbol, 0)
        if capital_total <= 0:
            log.warning(f'Saldo insuficiente en {self.capital_currency}')
            return 0.0
        capital_symbol = self.capital_por_simbolo.get(symbol, capital_total / max(
            len(self.capital_por_simbolo), 1))
        fraccion = self.fraccion_kelly
        puntaje_macro = obtener_puntaje_contexto(symbol)
        umbral_macro = getattr(self.config, 'umbral_puntaje_macro', 6)
        if abs(puntaje_macro) > umbral_macro:
            fraccion *= 0.5
            log.debug(f'📉 Ajuste por contexto macro {puntaje_macro:.2f} para {symbol}')
        if self.modo_capital_bajo and capital_total < 500:
            deficit = (500 - capital_total) / 500
            fraccion = max(fraccion, 0.02 + deficit * 0.1)
        riesgo_teorico = capital_symbol * fraccion * self.risk.umbral
        if exposicion_total > 0:
            ajuste = max(0.0, 1 - exposicion_total / (capital_total * self.riesgo_maximo_diario))
            riesgo_teorico *= ajuste
        minimo_dinamico = max(10.0, capital_total * 0.02)
        riesgo_permitido = max(riesgo_teorico, minimo_dinamico)
        riesgo_permitido = min(riesgo_permitido, capital_total * self.riesgo_maximo_diario)
        riesgo_permitido = min(riesgo_permitido, capital_total)
        minimo_binance = await self._obtener_minimo_binance(symbol)
        cantidad = 0.0
        distancia_sl = abs(precio - stop_loss) if isinstance(stop_loss, (int, float)) else None
        if not distancia_sl or distancia_sl <= 0:
            log.warning(
                f'⚠️ Stop Loss no especificado para {symbol}. Limitando la posición a una fracción del capital disponible.'
            )
            capital_necesario = riesgo_permitido
            cantidad = capital_necesario / precio
            riesgo_final = capital_necesario
            if capital_necesario < minimo_dinamico:
                log.debug(
                    f'Orden mínima {minimo_dinamico:.2f}{self.capital_currency}, intento {capital_necesario:.2f}{self.capital_currency}'
                )
                return 0.0
            if minimo_binance and capital_necesario < minimo_binance:
                log.warning(
                    f'⛔ Orden para {symbol} por {capital_necesario:.2f}{self.capital_currency} inferior al mínimo Binance {minimo_binance:.2f}{self.capital_currency}'
                )
                return 0.0
        else:
            cantidad = riesgo_permitido / distancia_sl
            capital_necesario = cantidad * precio
            if capital_necesario > capital_total:
                cantidad = capital_total / precio
                capital_necesario = capital_total
            riesgo_final = cantidad * distancia_sl
            if capital_necesario < minimo_dinamico:
                log.debug(
                    f'Orden mínima {minimo_dinamico:.2f}{self.capital_currency}, intento {capital_necesario:.2f}{self.capital_currency}'
                )
                return 0.0
            if minimo_binance and capital_necesario < minimo_binance:
                log.warning(
                    f'⛔ Orden para {symbol} por {capital_necesario:.2f}{self.capital_currency} inferior al mínimo Binance {minimo_binance:.2f}{self.capital_currency}'
                )
                return 0.0
        log.info(
            '⚖️ Kelly ajustada: %.4f | Riesgo teórico: %.2f%s | Mínimo dinámico: %.2f%s | Riesgo final: %.2f%s',
            fraccion,
            riesgo_teorico,
            self.capital_currency,
            minimo_dinamico,
            self.capital_currency,
            riesgo_final,
            self.capital_currency,
        )
        log.info(
            '📊 Capital disponible: %.2f%s | Orden: %.2f%s | Mínimo Binance: %s | %s',
            capital_total,
            self.capital_currency,
            capital_necesario,
            self.capital_currency,
            f'{minimo_binance:.2f}{self.capital_currency}' if minimo_binance else 'desconocido',
            symbol,
        )
        return round(cantidad, 6)

    def actualizar_capital(self, symbol: str, retorno_total: float) ->float:
        log.info('➡️ Entrando en actualizar_capital()')
        capital_inicial = self.capital_por_simbolo.get(symbol, 0.0)
        ganancia = capital_inicial * retorno_total
        capital_final = capital_inicial + ganancia
        self.capital_por_simbolo[symbol] = capital_final
        return capital_final
