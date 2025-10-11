"""Motor de estrategias para el bot de trading."""
from __future__ import annotations
import asyncio
from typing import Any, Awaitable, Callable, Dict, Mapping, Optional

import pandas as pd

from core.adaptador_umbral import calcular_umbral_adaptativo
from core.evaluacion_tecnica import evaluar_estrategias
from core.estrategias import TENDENCIA_IDEAL
from core.scoring import calcular_score_tecnico
from core.strategies.entry.validaciones_tecnicas import hay_contradicciones
from core.strategies.entry.validadores import (
    validar_bollinger,
    validar_max_min,
    validar_rsi,
    validar_slope,
    validar_spread,
    validar_volumen,
    validar_volumen_real,

)
from core.strategies.exit.gestor_salidas import evaluar_salidas
from core.strategies.pesos import gestor_pesos
from core.strategies.tendencia import detectar_tendencia
from core.utils.utils import configurar_logger, validar_dataframe
from indicadores.helpers import get_momentum, get_rsi, get_slope
from observability import metrics as obs_metrics

log = configurar_logger("engine", modo_silencioso=True)


PesoProvider = Callable[[str], Mapping[str, float]]
StrategyEvaluator = Callable[[str, pd.DataFrame, str], Awaitable[Dict[str, Any]]]
ExitEvaluator = Callable[..., Awaitable[Dict[str, Any]]]
TrendDetector = Callable[[str, pd.DataFrame], tuple[str, dict[str, bool]]]
ThresholdCalculator = Callable[[str, pd.DataFrame, Mapping[str, Any]], float]
TechnicalScorer = Callable[[
    pd.DataFrame,
    Optional[float],
    Optional[float],
    Optional[float],
    str,
    str,
], tuple[float, Any]]


class StrategyEngine:
    """Evalúa estrategias de entrada y salida."""

    def __init__(
        self,
        *,
        peso_provider: PesoProvider | None = None,
        strategy_evaluator: StrategyEvaluator | None = None,
        tendencia_detector: TrendDetector | None = None,
        threshold_calculator: ThresholdCalculator | None = None,
        technical_scorer: TechnicalScorer | None = None,
        rsi_getter: Callable[[pd.DataFrame], Any] | None = None,
        momentum_getter: Callable[[pd.DataFrame], Any] | None = None,
        slope_getter: Callable[[pd.DataFrame], Any] | None = None,
        metrics_module: Any = obs_metrics,
        salida_evaluator: ExitEvaluator | None = None,
    ) -> None:
        self._peso_provider = peso_provider or gestor_pesos.obtener_pesos_symbol
        self._strategy_evaluator = strategy_evaluator or evaluar_estrategias
        self._tendencia_detector = tendencia_detector or detectar_tendencia
        self._threshold_calculator = threshold_calculator or calcular_umbral_adaptativo
        self._technical_scorer = technical_scorer or calcular_score_tecnico
        self._get_rsi = rsi_getter or get_rsi
        self._get_momentum = momentum_getter or get_momentum
        self._get_slope = slope_getter or get_slope
        self._metrics = metrics_module
        self._salida_evaluator = salida_evaluator or evaluar_salidas
    async def evaluar_entrada(
        self,
        symbol: str,
        df: pd.DataFrame,
        tendencia: str | None = None,
        config: dict | None = None,
        pesos_symbol: Mapping[str, float] | None = None,
    ) -> Dict:
        """
        Evalúa si se cumplen condiciones para abrir una posición.

        Args:
            symbol: Símbolo del mercado (ej. "BTC/EUR").
            df: DataFrame con datos OHLCV.

        Returns:
            Diccionario con información de entrada:
                - estrategias_activas
                - puntaje_total
                - probabilidad
                - tendencia
        """
        if not symbol or not validar_dataframe(df, ["close", "high", "low", "volume"]):
            log.warning("⚠️ Entrada inválida: símbolo o DataFrame inválido.")
            return {
                "permitido": False,
                "motivo_rechazo": "datos_invalidos",
                "estrategias_activas": {},
                "score_total": 0.0,
                "umbral": 0.0,
                "diversidad": 0,
                "max_min": validar_max_min(df),
                "volumen_real": validar_volumen_real(df),
                "spread": validar_spread(df),
            }
        try:
            pesos = pesos_symbol or self._peso_provider(symbol)
            if tendencia is None:
                tendencia, _ = self._tendencia_detector(symbol, df)
            log.debug(f"[{symbol}] Tendencia usada: {tendencia}")
            resultado = await self._strategy_evaluator(symbol, df, tendencia)
            analisis = await asyncio.to_thread(
                self._procesar_resultado_entrada,
                symbol,
                df,
                tendencia,
                resultado,
                pesos,
                config or {},
            )
            return analisis
        except (ValueError, KeyError) as e:
            log.error(f"❌ Error de datos evaluando entrada para {symbol}: {e}")
            return {
                "permitido": False,
                "motivo_rechazo": "error",
                "estrategias_activas": {},
                "score_total": 0.0,
                "umbral": 0.0,
                "diversidad": 0,
            }
        except Exception:
            log.exception(
                f"❌ Error inesperado evaluando entrada para {symbol}"
            )
            return {
                "permitido": False,
                "motivo_rechazo": "error",
                "estrategias_activas": {},
                "score_total": 0.0,
                "umbral": 0.0,
                "diversidad": 0,
            }

    async def evaluar_salida(self, df: pd.DataFrame, orden: Dict) -> Dict:
        """
        Evalúa si se debe cerrar una orden activa.

        Args:
            df: DataFrame con datos recientes del mercado.
            orden: Diccionario con información de la orden activa.

        Returns:
            Diccionario con resultados de las estrategias de salida.
        """
        if df is None or df.empty or not orden:
            log.warning("⚠️ Evaluación de salida con datos insuficientes.")
            return {}
        try:
            return await self._salida_evaluator(orden, df)
        except (ValueError, KeyError) as e:
            log.error(f"❌ Error de datos evaluando salida: {e}")
            return {}
        except Exception:
            log.exception("❌ Error inesperado evaluando salida")
            return {}


def _procesar_resultado_entrada(
        self,
        symbol: str,
        df: pd.DataFrame,
        tendencia: str,
        resultado: Mapping[str, Any],
        pesos_symbol: Mapping[str, float],
        config: Mapping[str, Any],
    ) -> Dict[str, Any]:
        estrategias_activas = resultado.get("estrategias_activas", {})
        score_base = float(resultado.get("puntaje_total", 0.0))
        diversidad = int(resultado.get("diversidad", 0))
        sinergia = min(float(resultado.get("sinergia", 0.0)), 0.5)
        score_total = score_base * (1 + sinergia)

        rsi_val = self._normalizar_valor(self._get_rsi(df))
        slope_val = self._normalizar_valor(self._get_slope(df))
        mom_val = self._normalizar_valor(self._get_momentum(df))

        ventana_vol = min(20, len(df))
        vol_media = (
            df["volume"].rolling(ventana_vol).mean().iloc[-1]
            if ventana_vol > 0
            else 0.0
        )
        contexto = {
            "rsi": rsi_val,
            "slope": slope_val,
            "volumen": float(vol_media) if not pd.isna(vol_media) else 0.0,
            "tendencia": tendencia,
        }
        umbral = self._threshold_calculator(symbol, df, contexto)
        direccion = "short" if tendencia == "bajista" else "long"
        usar_score = bool(config.get("usar_score_tecnico", True))
        validaciones = {"volumen": validar_volumen(df)}
        if usar_score:
            validaciones.update(
                {
                    "rsi": validar_rsi(df, direccion),
                    "slope": validar_slope(df, tendencia),
                    "bollinger": validar_bollinger(df),
                }
            )
        validaciones_fallidas = [k for k, v in validaciones.items() if not v]

        contradiccion = hay_contradicciones(estrategias_activas)
        rsi_contra = rsi_val is not None and (rsi_val > 70 or rsi_val < 30)
        contradiccion = contradiccion or rsi_contra

        resolucion_conflicto = None
        if contradiccion:
            contradiccion, resolucion_conflicto = self._resolver_conflicto(
                symbol, estrategias_activas, pesos_symbol
            )

        score_tec, _ = self._technical_scorer(
            df,
            rsi_val,
            mom_val,
            slope_val,
            tendencia,
            direccion,
        )
        cumple_div = diversidad >= int(config.get("diversidad_minima", 1))
        umbral_score = float(config.get("umbral_score_tecnico", 1.0))
        empate = score_total == umbral or score_tec == umbral_score
        permitido = (
            score_total > umbral
            and score_tec > umbral_score
            and cumple_div
            and not validaciones_fallidas
            and not contradiccion
        )

        motivo = None
        if not permitido:
            if empate:
                motivo = "empate_umbral"
            elif contradiccion:
                motivo = "contradiccion"
            elif validaciones_fallidas:
                motivo = "validaciones_fallidas"
            elif score_tec <= umbral_score:
                motivo = "score_tecnico_bajo"
            elif score_total <= umbral:
                motivo = "score_bajo"
            elif not cumple_div:
                motivo = "diversidad_baja"
            else:
                motivo = "desconocido"

        return {
            "permitido": permitido,
            "motivo_rechazo": motivo,
            "estrategias_activas": estrategias_activas,
            "score_total": round(score_total, 2),
            "score_base": round(score_base, 2),
            "sinergia": round(sinergia, 2),
            "umbral": umbral,
            "umbral_score_tecnico": umbral_score,
            "empate": empate,
            "diversidad": diversidad,
            "tendencia": tendencia,
            "rsi": rsi_val,
            "slope": slope_val,
            "momentum": mom_val,
            "validaciones_fallidas": validaciones_fallidas,
            "score_tecnico": score_tec,
            "conflicto_resuelto": resolucion_conflicto is not None,
            "resolucion_conflicto": resolucion_conflicto,
        }

def _resolver_conflicto(
    self,
    symbol: str,
    estrategias_activas: Mapping[str, Any],
    pesos_symbol: Mapping[str, float],
) -> tuple[bool, Optional[str]]:
    bull_list = [
        e
        for e, act in estrategias_activas.items()
        if act and TENDENCIA_IDEAL.get(e) == "alcista"
    ]
    bear_list = [
        e
        for e, act in estrategias_activas.items()
        if act and TENDENCIA_IDEAL.get(e) == "bajista"
    ]
    peso_bull = sum(float(pesos_symbol.get(e, 1.0)) for e in bull_list)
    peso_bear = sum(float(pesos_symbol.get(e, 1.0)) for e in bear_list)

    if peso_bull > peso_bear * 1.1:
        log.info(
            f"[{symbol}] Conflicto BUY/SELL resuelto a favor de BUY (estrategias: {bull_list})"
        )
        if hasattr(self._metrics, "SIGNALS_CONFLICT_RESOLVED"):
            self._metrics.SIGNALS_CONFLICT_RESOLVED.labels(
                symbol=symbol, resolution="buy"
            ).inc()
        return False, "buy"

    if peso_bear > peso_bull * 1.1:
        log.info(
            f"[{symbol}] Conflicto BUY/SELL resuelto a favor de SELL (estrategias: {bear_list})"
        )
        if hasattr(self._metrics, "SIGNALS_CONFLICT_RESOLVED"):
            self._metrics.SIGNALS_CONFLICT_RESOLVED.labels(
                symbol=symbol, resolution="sell"
            ).inc()
        return False, "sell"

    log.warning(
        f"[{symbol}] Señales BUY y SELL simultáneas detectadas (empate)"
    )
    if hasattr(self._metrics, "SIGNALS_CONFLICT"):
        self._metrics.SIGNALS_CONFLICT.labels(symbol=symbol).inc()
    return True, None

@staticmethod
def _normalizar_valor(valor: Any) -> Optional[float]:
    if isinstance(valor, pd.Series):
        valor = valor.iloc[-1]
    try:
        if valor is None:
            return None
        return float(valor)
    except (TypeError, ValueError):
        return None
