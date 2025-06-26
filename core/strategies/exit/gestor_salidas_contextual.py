from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Dict

import pandas as pd

from core.utils.validacion import validar_dataframe
from core.utils import configurar_logger
from core.orders.order_model import Order
from indicators.atr import calcular_atr
from indicators.rsi import calcular_rsi
from indicators.momentum import calcular_momentum
from indicators.slope import calcular_slope

log = configurar_logger("gestor_salidas_contextual")


@dataclass
class ExitContext:
    symbol: str
    price: float
    stop_loss: float
    take_profit: float
    sl_distance: float
    tp_distance: float
    atr: float
    volatility: float
    score_tecnico: float
    momentum: float
    tendencia: str
    soporte_local: float
    resistencia_local: float
    volumen: float
    volumen_prom: float
    edad: int


class ExitManager:
    """Gestiona la lógica de salidas con contexto técnico y de riesgo."""

    def __init__(self, order: Order, df: pd.DataFrame, config: Optional[Dict] = None) -> None:
        self.order = order
        self.df = df
        self.config = config or {}
        self.context: ExitContext | None = None
        self.last_heartbeat: Optional[pd.Timestamp] = None

    # ------------------------------------------------------------------
    def heartbeat(self) -> None:
        """Actualiza la marca temporal del último chequeo."""
        self.last_heartbeat = pd.Timestamp.utcnow()
        log.debug(f"[{self.order.symbol}] Heartbeat registrado {self.last_heartbeat}")

    # ------------------------------------------------------------------
    def calcular_retornos_actuales(self) -> float:
        """Retorno porcentual desde el precio de entrada."""
        precio = float(self.df["close"].iloc[-1])
        direccion = 1 if self.order.direccion in ("long", "compra") else -1
        return direccion * (precio - self.order.precio_entrada) / self.order.precio_entrada

    # ------------------------------------------------------------------
    def _build_context(self) -> ExitContext:
        if not validar_dataframe(self.df, ["close", "high", "low", "volume"]):
            raise ValueError("DataFrame inválido para contexto de salida")

        price = float(self.df["close"].iloc[-1])
        atr = calcular_atr(self.df) or 0.0
        volatility = atr / price if price else 0.0
        rsi = calcular_rsi(self.df) or 50.0
        momentum = calcular_momentum(self.df) or 0.0
        slope = calcular_slope(self.df.tail(5)) or 0.0
        score_tecnico = (100 - rsi) / 100 + momentum + slope
        soporte = float(self.df["low"].rolling(window=5).min().iloc[-1])
        resistencia = float(self.df["high"].rolling(window=5).max().iloc[-1])
        vol = float(self.df["volume"].iloc[-1])
        vol_avg = float(self.df["volume"].rolling(window=20).mean().iloc[-1])
        edad = getattr(self.order, "duracion_en_velas", 0)
        sl_dist = abs(price - self.order.stop_loss)
        tp_dist = abs(self.order.take_profit - price)
        tendencia = getattr(self.order, "tendencia", "")
        return ExitContext(
            symbol=self.order.symbol,
            price=price,
            stop_loss=self.order.stop_loss,
            take_profit=self.order.take_profit,
            sl_distance=sl_dist,
            tp_distance=tp_dist,
            atr=atr,
            volatility=volatility,
            score_tecnico=score_tecnico,
            momentum=momentum,
            tendencia=tendencia,
            soporte_local=soporte,
            resistencia_local=resistencia,
            volumen=vol,
            volumen_prom=vol_avg,
            edad=edad,
        )

    # ------------------------------------------------------------------
    def evaluar_contexto_tecnico(self) -> float:
        """Calcula un puntaje técnico simplificado."""
        if self.context is None:
            self.context = self._build_context()
        ctx = self.context
        score = 0.0
        if ctx.price < ctx.soporte_local:
            score += 0.5
        if ctx.momentum < 0:
            score += 0.3
        if ctx.score_tecnico < 0:
            score += 0.2
        if ctx.volumen < 0.5 * ctx.volumen_prom:
            score += 0.2
        return score

    # ------------------------------------------------------------------
    def motivo_salida(self) -> str:
        """Devuelve una clasificación del motivo de cierre."""
        if self.context is None:
            self.context = self._build_context()
        ctx = self.context
        retorno = self.calcular_retornos_actuales()
        trailing_ratio = self.config.get("trailing_ratio", 1.0)
        trailing_stop = getattr(self.order, "max_price", ctx.price) - ctx.atr * trailing_ratio
        if ctx.price >= ctx.take_profit and retorno > 0:
            return "take_profit_confirmado"
        if ctx.price <= ctx.stop_loss:
            return "stop_loss_duro"
        if ctx.price <= trailing_stop:
            return "trailing_stop_activado"
        if self.evaluar_contexto_tecnico() >= 0.9:
            return "reversion_tecnica_confirmada"
        riesgo_lim = self.config.get("limite_perdida", 0.05)
        if retorno <= -riesgo_lim:
            return "riesgo_excedido"
        if ctx.volatility < self.config.get("umbral_volatilidad_baja", 0.001) and ctx.edad > self.config.get("max_velas_estancado", 20):
            return "abandono_por_estancamiento"
        return "mantener"

    # ------------------------------------------------------------------
    def deberia_salir(self) -> bool:
        motivo = self.motivo_salida()
        return motivo != "mantener"