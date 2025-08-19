from __future__ import annotations
from collections import Counter
from datetime import datetime, timezone
from typing import Dict
from config.config_manager import Config
from binance_api.cliente import fetch_balance_async, load_markets_async
from core.utils.logger import configurar_logger
from core.risk import RiskManager
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
        self._minimos_cache: Dict[str, float] = {}
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

    async def _on_calcular_cantidad(self, data: dict) -> None:
        fut = data.get('future')
        symbol = data.get('symbol')
        precio = data.get('precio')
        exposicion = data.get('exposicion_total', 0.0)
        stop_loss = data.get('stop_loss')
        if fut and not fut.done():
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
        if fut and not fut.done():
            fut.set_result(self.actualizar_capital(symbol, retorno))

    async def _obtener_minimo_binance(self, symbol: str) -> (float | None):
        if symbol in self._minimos_cache:
            return self._minimos_cache[symbol]
        if not self.modo_real or not self.cliente:
            return None
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
            minimo = info.get('limits', {}).get('cost', {}).get('min') if info else None
            if minimo:
                self._minimos_cache[symbol] = float(minimo)
                return self._minimos_cache[symbol]
        except Exception as e:
            log.warning(f'No se pudo obtener mínimo para {symbol}: {e}')
        return None
    
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
    ) -> float:
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
        minimo_binance = await self._obtener_minimo_binance(symbol)
        cantidad = 0.0
        distancia_sl = abs(precio - stop_loss) if isinstance(stop_loss, (int, float)) else None
        costo_pct = max(slippage_pct, 0.0) + max(fee_pct, 0.0)
        if not distancia_sl or distancia_sl <= 0:
            log.warning(
                f'⚠️ Stop Loss no especificado para {symbol}. Limitando la posición a una fracción del capital disponible.'
            )
            capital_necesario = riesgo_permitido / (1 + costo_pct)
            cantidad = capital_necesario / precio
            riesgo_final = capital_necesario * (1 + costo_pct)
            valido, error = self._validar_minimos(
                capital_necesario, minimo_dinamico, minimo_binance
            )
            if not valido:
                if error and error.startswith('⛔'):
                    log.warning(error.format(symbol=symbol))
                else:
                    log.debug(error)
                return 0.0
        else:
            costo_unitario = distancia_sl + precio * costo_pct
            cantidad = riesgo_permitido / costo_unitario
            capital_necesario = cantidad * precio
            if capital_necesario > capital_total:
                cantidad = capital_total / precio
                capital_necesario = capital_total
            riesgo_final = cantidad * costo_unitario
            valido, error = self._validar_minimos(
                capital_necesario, minimo_dinamico, minimo_binance
            )
            if not valido:
                if error and error.startswith('⛔'):
                    log.warning(error.format(symbol=symbol))
                else:
                    log.debug(error)
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

    def actualizar_capital(self, symbol: str, retorno_total: float) -> float:
        capital_inicial = self.capital_por_simbolo.get(symbol, 0.0)
        ganancia = capital_inicial * retorno_total
        capital_final = capital_inicial + ganancia
        self.capital_por_simbolo[symbol] = capital_final
        return capital_final
