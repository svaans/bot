from __future__ import annotations

import pandas as pd
from typing import Dict, Optional

from core.market_context import MarketContext
from core.strategies.strategy_engine import StrategyEngine
from core.utils.utils import configurar_logger
from indicators.momentum import calcular_momentum
from indicators.rsi import calcular_rsi
from indicators.slope import calcular_slope


log = configurar_logger("smart_decision")


class DecisionEngine:
    """Motor mejorado de decisiones basado en contexto."""

    def __init__(self, context: MarketContext, risk_profile: str = "moderado") -> None:
        self.context = context
        self.risk_profile = risk_profile
        self.strategy_engine = StrategyEngine()

    def evaluar_entrada(
        self,
        symbol: str,
        df: pd.DataFrame,
        tendencia: Optional[str] = None,
        config: Optional[dict] = None,
        pesos_symbol: Optional[dict] = None,
    ) -> Dict:
        resultado = self.strategy_engine.evaluar_entrada(
            symbol,
            df,
            tendencia=tendencia,
            config=config,
            pesos_symbol=pesos_symbol,
        )

        if not resultado.get("permitido", False):
            return resultado

        volatilidad = df["close"].pct_change().tail(20).std()
        volumen = df["volume"].iloc[-1] if "volume" in df.columns else 0.0
        self.context.actualizar_symbol(symbol, volatilidad, volumen, tendencia)

        if self.context.global_state.volatilidad_global < config.get("volatilidad_minima", 0.0):
            log.info(f"[{symbol}] Mercado global plano, se descarta la entrada")
            return {**resultado, "permitido": False, "motivo_rechazo": "mercado_plano"}

        max_pos = config.get("max_positions", 5)
        if len(self.context.portfolio_state.posiciones) >= max_pos:
            log.info(f"[{symbol}] Máximo de posiciones abierto")
            return {**resultado, "permitido": False, "motivo_rechazo": "max_posiciones"}

        rsi = calcular_rsi(df)
        momentum = calcular_momentum(df)
        slope = calcular_slope(df)
        score_ev = (rsi - 50) / 50 + momentum + slope
        ev_umbral = config.get("umbral_ev", 0.0)
        if score_ev < ev_umbral:
            log.info(f"[{symbol}] EV negativo basado en indicadores")
            return {**resultado, "permitido": False, "motivo_rechazo": "ev_negativo"}

        return {**resultado, "ev": score_ev, "permitido": True}

    def evaluar_salida(self, df: pd.DataFrame, orden: Dict) -> Dict:
        res = self.strategy_engine.evaluar_salida(df, orden)
        if not res:
            res = {}

        atr = df["high"].tail(14).max() - df["low"].tail(14).min()
        if atr:
            trailing_factor = atr * 0.5
            res.setdefault("trailing_stop", trailing_factor)
        return res
