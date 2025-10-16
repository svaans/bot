"""Gestión de riesgo del bot."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

UTC = timezone.utc
from typing import Any, Dict, Set, TYPE_CHECKING

import numpy as np

from core.event_bus import EventBus
from core.reporting import reporter_diario
from core.risk.riesgo import actualizar_perdida
from core.risk.riesgo import riesgo_superado as _riesgo_superado
from core.registro_metrico import registro_metrico
from core.utils.feature_flags import is_flag_enabled
from core.utils.metrics_compat import Gauge
from core.utils.utils import configurar_logger

if TYPE_CHECKING:  # pragma: no cover - solo para tipado
    from core.capital_manager import CapitalManager
log = configurar_logger('risk', modo_silencioso=True)

RIESGO_CONSUMIDO_GAUGE = Gauge(
    'risk_daily_consumed',
    'Riesgo diario acumulado por el gestor de riesgo',
)
COOLDOWN_ACTIVO_GAUGE = Gauge(
    'risk_cooldown_active',
    'Indicador binario de cooldown global en el gestor de riesgo',
)


class RiskManager:
    """Encapsula la lógica de control de riesgo del bot."""

    def __init__(
        self,
        umbral: float,
        bus: EventBus | None = None,
        capital_manager: "CapitalManager" | None = None,
        cooldown_pct: float = 0.1,
        correlacion_ttl: int = 1800,
        cooldown_duracion: int = 300,
    ) -> None:
        self.umbral = umbral
        self._factor_kelly_prev = None
        self._bus: EventBus | None = None
        self.bus = bus
        self.capital_manager = capital_manager
        self.cooldown_pct = cooldown_pct
        self.cooldown_duracion = cooldown_duracion
        self.correlacion_ttl = timedelta(seconds=max(0, correlacion_ttl))
        self._cooldown_fin: datetime | None = None
        self.posiciones_abiertas: Set[str] = set()
        self.correlaciones: Dict[str, Dict[str, tuple[float, datetime]]] = {}
        self._fecha_riesgo = datetime.now(UTC).date()
        self.riesgo_diario = 0.0
        self._ultimo_factor_kelly = 1.0
        self._capital_guard_enabled = is_flag_enabled(
            "risk.capital_manager.enabled", default=False
        )
        if bus:
            self.subscribe(bus)

    @property
    def bus(self) -> EventBus | None:
        return self._bus

    @bus.setter
    def bus(self, value: EventBus | None) -> None:
        self._bus = value
        if value is None:
            return
        start = getattr(value, "start", None)
        if callable(start):
            try:
                start()
            except Exception:
                log.warning(
                    "No se pudo iniciar event_bus tras inyección en RiskManager",
                    exc_info=True,
                )

    def subscribe(self, bus: EventBus) -> None:
        bus.subscribe('registrar_perdida', self._on_registrar_perdida)

    async def _on_registrar_perdida(self, data: Any) -> None:
        self.registrar_perdida(data.get('symbol'), data.get('perdida', 0.0))

    def riesgo_superado(self, capital_total: float) ->bool:
        """Indica si el capital perdido supera el umbral configurado."""
        return _riesgo_superado(self.umbral, capital_total)

    def registrar_perdida(self, symbol: str, perdida: float) ->None:
        """Registra una pérdida para ``symbol``."""
        if perdida < 0:
            hoy = datetime.now(UTC).date()
            if hoy != self._fecha_riesgo:
                self._fecha_riesgo = hoy
                self.riesgo_diario = 0.0
                RIESGO_CONSUMIDO_GAUGE.set(self.riesgo_diario)
            perdida_abs = abs(perdida)
            self.riesgo_diario += perdida_abs
            RIESGO_CONSUMIDO_GAUGE.set(self.riesgo_diario)
            registro_metrico.registrar(
                "risk_drawdown",
                {
                    "symbol": symbol,
                    "loss": float(perdida_abs),
                    "riesgo_diario": float(self.riesgo_diario),
                },
            )
            actualizar_perdida(symbol, perdida)
            capital_symbol = 0.0
            if self.capital_manager:
                capital_symbol = self.capital_manager.capital_por_simbolo.get(symbol, 0.0)
            if capital_symbol > 0 and perdida_abs / capital_symbol > self.cooldown_pct:
                estaba_activo = self.cooldown_activo
                self._cooldown_fin = datetime.now(UTC) + timedelta(seconds=self.cooldown_duracion)
                COOLDOWN_ACTIVO_GAUGE.set(1.0)
                if self.bus and not estaba_activo:
                    payload = {
                        "symbol": symbol,
                        "perdida": float(perdida_abs),
                        "cooldown_fin": self._cooldown_fin.isoformat(),
                    }
                    self.bus.emit("risk.cooldown_activated", payload)
            elif self._cooldown_fin is None:
                COOLDOWN_ACTIVO_GAUGE.set(0.0)

    # --- Gestión de correlaciones entre posiciones ---
    def abrir_posicion(self, symbol: str) -> None:
        """Marca ``symbol`` como posición abierta."""
        self.posiciones_abiertas.add(symbol)

    def cerrar_posicion(self, symbol: str) -> None:
        """Elimina ``symbol`` de las posiciones abiertas y sus correlaciones."""
        self.posiciones_abiertas.discard(symbol)
        self.correlaciones.pop(symbol, None)
        for otros in self.correlaciones.values():
            otros.pop(symbol, None)

    def registrar_correlaciones(self, symbol: str, correlaciones: dict[str, float]) -> None:
        """Registra correlaciones de ``symbol`` con otras posiciones abiertas."""
        self._limpiar_correlaciones_expiradas()
        if symbol not in self.posiciones_abiertas:
            return
        self.correlaciones.setdefault(symbol, {})
        marca = datetime.now(UTC)
        for otro, rho in correlaciones.items():
            if otro in self.posiciones_abiertas and otro != symbol:
                self.correlaciones[symbol][otro] = (rho, marca)
                self.correlaciones.setdefault(otro, {})[symbol] = (rho, marca)

    def correlacion_media(self, symbol: str, correlaciones: dict[str, float]) -> float:
        """Calcula la correlación media absoluta con posiciones abiertas."""
        self._limpiar_correlaciones_expiradas()
        valores: list[float] = []
        correlaciones_existentes = self.correlaciones.get(symbol, {})
        marca = datetime.now(UTC)
        for abierta in self.posiciones_abiertas:
            if abierta == symbol:
                continue
            rho = None
            if abierta in correlaciones:
                rho = correlaciones[abierta]
            elif abierta in correlaciones_existentes:
                rho, ts = correlaciones_existentes[abierta]
                if not self._correlacion_vigente(ts, marca):
                    self._eliminar_correlacion(symbol, abierta)
                    rho = None
            if rho is not None:
                valores.append(abs(float(rho)))
        if not valores:
            return 0.0
        return float(np.mean(valores))

    def permite_entrada(
        self, symbol: str, correlaciones: dict[str, float], diversidad_minima: float
    ) -> bool:
        """Determina si se permite una nueva entrada según la correlación media."""
        if self.cooldown_activo:
            log.info('🚫 Cooldown activo, no se permiten nuevas entradas')
            return False
        if self.capital_manager and not self.capital_manager.hay_capital_libre():
            log.info('🚫 Sin capital libre para nuevas posiciones')
            return False
        if self.capital_manager and self._capital_guard_enabled:
            exposure_fn = getattr(self.capital_manager, "exposure_disponible", None)
            if callable(exposure_fn):
                try:
                    disponible_symbol = float(exposure_fn(symbol))
                except TypeError:
                    disponible_symbol = float(exposure_fn(symbol=symbol))  # type: ignore[call-arg]
                except Exception:
                    log.warning('⚠️ Error consultando exposición disponible', exc_info=True)
                    disponible_symbol = 0.0
                if disponible_symbol <= 0:
                    log.info('🚫 %s: sin exposición disponible', symbol)
                    return False
                try:
                    disponible_global = float(exposure_fn(None))
                except TypeError:
                    disponible_global = float(exposure_fn(symbol=None))  # type: ignore[call-arg]
                except Exception:
                    disponible_global = disponible_symbol
                if disponible_global <= 0:
                    log.info('🚫 Exposición global agotada')
                    return False
        media = self.correlacion_media(symbol, correlaciones)
        if media > diversidad_minima:
            log.info(
                f'🚫 {symbol}: correlación media {media:.2f} > {diversidad_minima:.2f}'
            )
            return False
        return True

    # --- Métricas internas ---
    @property
    def cooldown_activo(self) -> bool:
        if self._cooldown_fin is None:
            COOLDOWN_ACTIVO_GAUGE.set(0.0)
            return False
        ahora = datetime.now(UTC)
        if ahora >= self._cooldown_fin:
            self._cooldown_fin = None
            self._limpiar_correlaciones_expiradas(force=True)
            COOLDOWN_ACTIVO_GAUGE.set(0.0)
            return False
        COOLDOWN_ACTIVO_GAUGE.set(1.0)
        return True

    @property
    def riesgo_consumido(self) -> float:
        return self.riesgo_diario

    def metricas(self) -> dict[str, float | bool]:
        """Devuelve un resumen de métricas de riesgo."""
        riesgo = self.riesgo_consumido
        RIESGO_CONSUMIDO_GAUGE.set(riesgo)
        activo = self.cooldown_activo
        return {
            'riesgo_consumido': riesgo,
            'cooldown_activo': activo,
        }

    def ajustar_umbral(self, segun_metricas: dict) ->None:
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
            if self.capital_manager and abs(factor - self._ultimo_factor_kelly) > 1e-3:
                try:
                    self.capital_manager.aplicar_multiplicador_kelly(factor)
                except AttributeError:
                    log.debug('CapitalManager sin soporte para multiplicador Kelly')
            self._ultimo_factor_kelly = factor
            return factor
        except Exception as e:
            log.warning(f'⚠️ Error calculando multiplicador Kelly: {e}')
            return 1.0

    def factor_volatilidad(self, volatilidad_actual: float,
        volatilidad_media: float, umbral: float=2.0) ->float:
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
        factor = round(max(0.25, min(1.0, factor)), 3)
        log.info(
            f'🌪️ Volatilidad excesiva, aplicando factor de reducción: {factor}'
            )
        return factor
    
    def _correlacion_vigente(self, timestamp: datetime, ahora: datetime | None = None) -> bool:
        if self.correlacion_ttl.total_seconds() <= 0:
            return True
        if ahora is None:
            ahora = datetime.now(UTC)
        return timestamp >= (ahora - self.correlacion_ttl)

    def _eliminar_correlacion(self, symbol: str, otro: str) -> None:
        mapa = self.correlaciones.get(symbol)
        if mapa and otro in mapa:
            mapa.pop(otro, None)
            if not mapa:
                self.correlaciones.pop(symbol, None)
        mapa_otro = self.correlaciones.get(otro)
        if mapa_otro and symbol in mapa_otro:
            mapa_otro.pop(symbol, None)
            if not mapa_otro:
                self.correlaciones.pop(otro, None)

    def _limpiar_correlaciones_expiradas(self, *, force: bool = False) -> None:
        if not self.correlaciones:
            return
        if force:
            self.correlaciones.clear()
            return
        ahora = datetime.now(UTC)
        limite = ahora - self.correlacion_ttl
        for symbol, otros in list(self.correlaciones.items()):
            for otro, (_, timestamp) in list(otros.items()):
                if timestamp < limite:
                    self._eliminar_correlacion(symbol, otro)
    
    async def kill_switch(
        self,
        order_manager: Any,
        drawdown_diario: float,
        limite_drawdown: float,
        perdidas_consecutivas: int,
        max_perdidas: int,
    ) -> bool:
        """Cancela órdenes y cierra posiciones ante pérdidas excesivas."""
        if drawdown_diario < limite_drawdown and perdidas_consecutivas < max_perdidas:
            return False
        log.warning('🛑 Kill switch activado. Cerrando posiciones abiertas.')
        try:
            for symbol, orden in list(getattr(order_manager, 'ordenes', {}).items()):
                precio = getattr(orden, 'precio_entrada', 0.0)
                try:
                    await order_manager.cerrar_async(symbol, precio, 'Kill Switch')
                except Exception as e:
                    log.error(f'❌ Error cerrando {symbol} en kill switch: {e}')
        finally:
            if self.bus:
                await self.bus.publish('notify', {
                    'mensaje': '🛑 Kill switch activado: posiciones cerradas',
                    'tipo': 'CRITICAL',
                })
        return True
