from __future__ import annotations
from collections import Counter
from datetime import datetime, timezone
from typing import Dict
from config.config_manager import Config
from binance_api.cliente import fetch_balance_async, load_markets_async
from core.utils.logger import configurar_logger
from core.risk import RiskManager, size_order, MarketInfo
from core.event_bus import EventBus
from core.contexto_externo import obtener_puntaje_contexto

UTC = timezone.utc
log = configurar_logger('capital_manager', modo_silencioso=False)


class CapitalManager:
    """Gestiona el capital disponible para trading."""

    def __init__(self, config: Config, cliente, risk: RiskManager,
        fraccion_kelly: float, bus: EventBus | None = None) -> None:
        self.config = config
        self.cliente = cliente
        self.risk = risk
        self.fraccion_kelly = fraccion_kelly
        self.modo_real = getattr(config, 'modo_real', False)
        self.modo_capital_bajo = config.modo_capital_bajo
        self.riesgo_maximo_diario = getattr(config, 'riesgo_maximo_diario', 1.0)
        self._markets = None
        self._markets_cache: Dict[str, MarketInfo] = {}
        self.capital_currency = getattr(config, 'capital_currency', None)
        if not self.capital_currency:
            self.capital_currency = self._detectar_divisa_principal(config.symbols)
        capital_total = 1000.0
        inicial = capital_total / max(len(config.symbols), 1)
        inicial = max(inicial, 20.0)
        self.capital_por_simbolo: Dict[str, float] = {s: inicial for s in config.symbols}
        self.capital_inicial_diario = self.capital_por_simbolo.copy()
        self.reservas_piramide: Dict[str, float] = {s: 0.0 for s in config.symbols}
        self.fecha_actual = datetime.now(UTC).date()
        if bus:
            self.subscribe(bus)

    async def inicializar_capital_async(self) -> None:
        """Obtiene el capital actual de manera asíncrona."""
        capital_total = 1000.0
        if self.modo_real and self.cliente:
            try:
                balance = await fetch_balance_async(self.cliente)
                capital_total = balance['total'].get(self.capital_currency, 0)
            except Exception as e:
                log.error(f'❌ Error al obtener balance: {e}')
        inicial = capital_total / max(len(self.config.symbols), 1)
        inicial = max(inicial, 20.0)
        self.capital_por_simbolo = {s: inicial for s in self.config.symbols}
        self.capital_inicial_diario = self.capital_por_simbolo.copy()

    @staticmethod
    def _detectar_divisa_principal(symbols: list[str]) -> str:
        monedas = [s.split('/')[-1] for s in symbols if '/' in s]
        if not monedas:
            return 'EUR'
        return Counter(monedas).most_common(1)[0][0]

    def subscribe(self, bus: EventBus) -> None:
        bus.subscribe('calcular_cantidad', self._on_calcular_cantidad)
        bus.subscribe('actualizar_capital', self._on_actualizar_capital)

    def tiene_capital(self, symbol: str) -> bool:
        """Devuelve ``True`` si hay capital asignado a ``symbol``."""
        return self.capital_por_simbolo.get(symbol, 0.0) > 0
    
    def es_moneda_base(self, symbol: str) -> bool:
        """Verifica que ``symbol`` opere contra la ``capital_currency``."""
        return symbol.split('/')[-1] == self.capital_currency
    
    async def _on_calcular_cantidad(self, data: dict) -> None:
        fut = data.get('future')
        symbol = data.get('symbol')
        precio = data.get('precio')
        exposicion = data.get('exposicion_total', 0.0)
        stop_loss = data.get('stop_loss')
        if fut and not fut.done():
            precio_adj, cantidad = await self.calcular_cantidad_async(
                symbol,
                precio,
                exposicion_total=exposicion,
                stop_loss=stop_loss,
            )
            fut.set_result((precio_adj, cantidad))

    async def _on_actualizar_capital(self, data: dict) -> None:
        fut = data.get('future')
        symbol = data.get('symbol')
        retorno = data.get('retorno_total', 0.0)
        if fut and not fut.done():
            fut.set_result(self.actualizar_capital(symbol, retorno))

    async def _obtener_info_mercado(self, symbol: str) -> MarketInfo:
        if symbol in self._markets_cache:
            return self._markets_cache[symbol]
        if not self.modo_real or not self.cliente:
            info = MarketInfo(0.0, 0.0, 0.0)
            self._markets_cache[symbol] = info
            return info
        try:
            if self._markets is None:
                self._markets = await load_markets_async(self.cliente)
            key_base = symbol.replace('/', '')
            info = self._markets.get(symbol) or self._markets.get(key_base)
            if not info:
                for k, v in self._markets.items():
                    if k.replace('/', '') == key_base:
                        info = v
                        break
            tick = step = min_notional = 0.0
            if info:
                filters = {f['filterType']: f for f in info.get('info', {}).get('filters', [])}
                tick = float(filters.get('PRICE_FILTER', {}).get('tickSize', 0) or 0)
                step = float(filters.get('LOT_SIZE', {}).get('stepSize', 0) or 0)
                min_notional = float(
                    filters.get('MIN_NOTIONAL', {}).get('minNotional', 0)
                    or info.get('limits', {}).get('cost', {}).get('min', 0)
                    or 0
                )
            market = MarketInfo(tick, step, min_notional)
            self._markets_cache[symbol] = market
            return market
        except Exception as e:
            log.warning(f'No se pudo obtener info de mercado para {symbol}: {e}')
            info = MarketInfo(0.0, 0.0, 0.0)
            self._markets_cache[symbol] = info
            return info

    async def info_mercado(self, symbol: str) -> MarketInfo:
        return await self._obtener_info_mercado(symbol)
    
    def _validar_minimos(self, capital_necesario: float,
                         minimo_dinamico: float,
                         minimo_binance: float | None) -> tuple[bool, str | None]:
        if capital_necesario < minimo_dinamico:
            return False, (
                f'Orden mínima {minimo_dinamico:.2f}{self.capital_currency}, '
                f'intento {capital_necesario:.2f}{self.capital_currency}'
            )
        if minimo_binance and capital_necesario < minimo_binance:
            return False, (
                f'⛔ Orden para {{symbol}} por {capital_necesario:.2f}{self.capital_currency} '
                f'inferior al mínimo Binance {minimo_binance:.2f}{self.capital_currency}'
            )
        return True, None

    async def calcular_cantidad_async(
        self,
        symbol: str,
        precio: float,
        exposicion_total: float = 0.0,
        stop_loss: float | None = None,
        slippage_pct: float = 0.0,
        fee_pct: float = 0.0,
    ) -> tuple[float, float]:
        if self.modo_real and self.cliente:
            balance = await fetch_balance_async(self.cliente)
            capital_total = balance['total'].get(self.capital_currency, 0)
        else:
            capital_total = sum(self.capital_por_simbolo.values())
        capital_symbol = self.capital_por_simbolo.get(
            symbol, capital_total / max(len(self.capital_por_simbolo), 1)
        )
        if not self.es_moneda_base(symbol):
            log.warning(f'Moneda base incompatible para {symbol}')
            return precio, 0.0
        if capital_total <= 0 or capital_symbol <= 0:
            log.warning(f'Saldo insuficiente en {self.capital_currency}')
            return precio, 0.0
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
        riesgo_permitido = min(
            riesgo_permitido, capital_total * self.riesgo_maximo_diario
        )
        exposure_limit_global = capital_total * self.riesgo_maximo_diario
        disponible_global = max(0.0, exposure_limit_global - exposicion_total)
        if disponible_global <= 0:
            log.warning('Límite de exposición global alcanzado')
            return precio, 0.0
        exposure_disponible = min(disponible_global, capital_symbol)
        market = await self._obtener_info_mercado(symbol)
        distancia_sl = abs(precio - stop_loss) if isinstance(stop_loss, (int, float)) else 0.0
        costo_pct = max(slippage_pct, 0.0) + max(fee_pct, 0.0)
        if distancia_sl <= 0:
            log.warning(
                f'⚠️ Stop Loss no especificado para {symbol}. Limitando la posición a una fracción del capital disponible.'
            )
            costo_unitario = precio * (1 + costo_pct)
        else:
            costo_unitario = distancia_sl + precio * costo_pct
            exposure_limit = capital_total * self.riesgo_maximo_diario
        precio_adj, cantidad = size_order(
            price=precio,
            stop_price=precio - distancia_sl if distancia_sl > 0 else precio,
            market=market,
            risk_limit=riesgo_permitido,
            exposure_limit=exposure_limit,
            current_exposure=exposicion_total,
            fee_pct=fee_pct,
            slippage_pct=slippage_pct,
        )
        capital_necesario = precio_adj * cantidad
        costo_unitario = abs(precio_adj - (precio - distancia_sl if distancia_sl > 0 else precio_adj)) + precio_adj * costo_pct
        riesgo_final = cantidad * costo_unitario
        valido, error = self._validar_minimos(
            capital_necesario, minimo_dinamico, market.min_notional
        )
        if not valido:
            if error and error.startswith('⛔'):
                log.warning(error.format(symbol=symbol))
            else:
                log.debug(error)
            return precio_adj, 0.0
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
            f'{market.min_notional:.2f}{self.capital_currency}' if market.min_notional else 'desconocido',
            symbol,
        )
        return precio_adj, round(cantidad, 6)

    def actualizar_capital(self, symbol: str, retorno_total: float) -> float:
        capital_inicial = self.capital_por_simbolo.get(symbol, 0.0)
        ganancia = capital_inicial * retorno_total
        capital_final = capital_inicial + ganancia
        self.capital_por_simbolo[symbol] = capital_final
        return capital_final
