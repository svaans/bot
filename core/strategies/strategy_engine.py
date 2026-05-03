"""Motor de estrategias para el bot de trading."""
from __future__ import annotations
import asyncio
import json
import math
import statistics
from collections import deque
from threading import Lock
from time import monotonic, perf_counter
from typing import TYPE_CHECKING, Any, Awaitable, Callable, Dict, Mapping, Optional
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
from core.strategies.regimen_mercado import (
    aplicar_multiplicadores_regimen,
    etiqueta_volatilidad,
)
from core.strategies.tendencia import detectar_tendencia
from core.utils.log_utils import format_exception_for_log
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


if TYPE_CHECKING:  # pragma: no cover - solo para type checkers
    from core.trader import EstadoSimbolo

class StrategyEngine:
    """Evalúa estrategias de entrada y salida.

    Encadena: señales por tendencia y pesos por símbolo, umbral adaptativo
    (:mod:`core.adaptador_umbral`), score técnico, diversidad mínima y
    validadores (volumen, RSI, pendiente, Bollinger). Las salidas delegan en
    :mod:`core.strategies.exit.gestor_salidas`.
    """

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
        self._synergy_cap = 0.5
        self._synergy_history_by_symbol: dict[str, deque[float]] = {}
        self._synergy_lock = Lock()
        self._synergy_check_interval = 300.0
        self._last_synergy_check_by_symbol: dict[str, float] = {}


    @staticmethod
    def _normalizar_tendencia(
        tendencia: str | None | "EstadoSimbolo",
    ) -> str | None:
        """Devuelve una tendencia str válida o ``None`` si no es usable."""

        if tendencia is None:
            return None
        if isinstance(tendencia, str):
            return tendencia
        candidata = getattr(tendencia, "tendencia", None)
        if isinstance(candidata, str):
            return candidata
        return None

    async def evaluar_entrada(
        self,
        symbol: str,
        df: pd.DataFrame,
        tendencia: str | None | "EstadoSimbolo" = None,
        config: dict | None = None,
        pesos_symbol: Mapping[str, float] | None = None,
    ) -> Dict[str, Any]:
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
        _t0_eval = perf_counter()
        try:
            pesos = pesos_symbol or self._peso_provider(symbol)
            tendencia_normalizada = self._normalizar_tendencia(tendencia)
            if tendencia_normalizada is None:
                tendencia_normalizada, _ = self._tendencia_detector(symbol, df)
            log.debug(f"[{symbol}] Tendencia usada: {tendencia_normalizada}")
            resultado = await self._strategy_evaluator(
                symbol, df, tendencia_normalizada
            )
            analisis = await asyncio.to_thread(
                self._procesar_resultado_entrada,
                symbol,
                df,
                tendencia_normalizada,
                resultado,
                pesos,
                config or {},
            )
            _elapsed = (perf_counter() - _t0_eval) * 1000
            log.info("diagnostico.evaluar_entrada_time", extra={"symbol": symbol, "elapsed_ms": round(_elapsed, 2)})
            return analisis
        except (ValueError, KeyError) as e:
            log.error(
                "❌ Error de datos evaluando entrada para %s: %s",
                symbol,
                format_exception_for_log(e),
            )
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

    async def evaluar_salida(self, df: pd.DataFrame, orden: Dict[str, Any]) -> Dict[str, Any]:
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
            log.error(
                "❌ Error de datos evaluando salida: %s",
                format_exception_for_log(e),
            )
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
        _t0 = perf_counter()
        estrategias_activas = resultado.get("estrategias_activas", {})
        score_base = float(resultado.get("puntaje_total", 0.0))
        diversidad = int(resultado.get("diversidad", 0))
        sinergia_raw = float(resultado.get("sinergia", 0.0))
        self._registrar_sinergia(symbol, sinergia_raw)
        sinergia = min(sinergia_raw, self._synergy_cap)
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

        score_tec, score_breakdown = self._technical_scorer(
            df,
            rsi_val,
            mom_val,
            slope_val,
            tendencia,
            direccion,
        )
        cumple_div = diversidad >= int(config.get("diversidad_minima", 1))
        umbral_score = float(config.get("umbral_score_tecnico", 1.0))
        periodo_atr = int(config.get("regimen_atr_periodo", 14) or 14)
        vol_etiqueta = etiqueta_volatilidad(
            df,
            umbral_alto=float(config.get("regimen_vol_atr_ratio_alto", 0.025) or 0.025),
            umbral_bajo=float(config.get("regimen_vol_atr_ratio_bajo", 0.008) or 0.008),
            periodo_atr=periodo_atr,
        )
        umbral, umbral_score = aplicar_multiplicadores_regimen(
            umbral, umbral_score, vol_etiqueta, config
        )
        _emp_tol = float(config.get("umbral_empate_abs_tol", 1e-6))
        empate = math.isclose(score_total, umbral, rel_tol=0.0, abs_tol=_emp_tol) or math.isclose(
            score_tec, umbral_score, rel_tol=0.0, abs_tol=_emp_tol
        )
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

        # ``verificar_entrada`` respeta ``permitido``; si es False no se abre aunque
        # el score técnico pase otros filtros aislados en el pipeline.

        # --- DIAGNÓSTICO: validación de indicadores anómalos ----------------
        _anomalias = []
        if rsi_val is not None and (rsi_val < 0 or rsi_val > 100):
            _anomalias.append(f"rsi_fuera_rango={rsi_val}")
        if slope_val is not None and (slope_val < -1 or slope_val > 1):
            _anomalias.append(f"slope_fuera_rango={slope_val}")
        if mom_val is not None and (mom_val < -1 or mom_val > 1):
            _anomalias.append(f"momentum_fuera_rango={mom_val}")
        if rsi_val is None:
            _anomalias.append("rsi_none")
        if slope_val is None:
            _anomalias.append("slope_none")
        if mom_val is None:
            _anomalias.append("momentum_none")
        if contexto.get("volumen", 0) <= 0:
            _anomalias.append(f"volumen_no_positivo={contexto.get('volumen')}")
        if math.isnan(score_total) or math.isinf(score_total):
            _anomalias.append(f"score_total_invalid={score_total}")
        if math.isnan(score_tec) or math.isinf(score_tec):
            _anomalias.append(f"score_tecnico_invalid={score_tec}")
        if math.isnan(umbral) or math.isinf(umbral):
            _anomalias.append(f"umbral_invalid={umbral}")
        if _anomalias:
            log.warning(
                "diagnostico.anomalia_indicadores",
                extra={"symbol": symbol, "anomalias": _anomalias},
            )
        # -------------------------------------------------------------------

        _elapsed_ms = (perf_counter() - _t0) * 1000
        _diag = {
            "symbol": symbol,
            "timestamp": int(df["timestamp"].iloc[-1]) if "timestamp" in df.columns else None,
            "rsi": rsi_val,
            "slope": slope_val,
            "momentum": mom_val,
            "volumen_media": contexto.get("volumen"),
            "tendencia": tendencia,
            "score_base": round(score_base, 4),
            "sinergia": round(sinergia, 4),
            "score_total": round(score_total, 4),
            "umbral": round(umbral, 4),
            "score_tecnico": round(score_tec, 4),
            "umbral_score_tecnico": round(umbral_score, 4),
            "diversidad": diversidad,
            "cumple_diversidad": cumple_div,
            "validaciones": validaciones,
            "validaciones_fallidas": validaciones_fallidas,
            "contradiccion": contradiccion,
            "empate": empate,
            "regimen_volatilidad": vol_etiqueta,
            "permitido": permitido,
            "motivo_rechazo": motivo,
            "score_breakdown": score_breakdown.to_dict() if hasattr(score_breakdown, "to_dict") else str(score_breakdown),
            "anomalias": _anomalias if _anomalias else None,
            "elapsed_ms": round(_elapsed_ms, 2),
        }
        log.info("diagnostico.decision_entrada", extra=_diag)

        return {
            "permitido": permitido,
            "motivo_rechazo": motivo,
            "estrategias_activas": estrategias_activas,
            "score_total": round(score_total, 2),
            "score_base": round(score_base, 2),
            "sinergia": round(sinergia, 2),
            "umbral": umbral,
            "umbral_score_tecnico": umbral_score,
            "regimen_volatilidad": vol_etiqueta,
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
            "score": score_tec,
            "side": direccion,
            "contradicciones": contradiccion,
            "score_tecnico_detalle": score_breakdown,
        }
    
    def _registrar_sinergia(self, symbol: str, sinergia: float) -> None:
        if not math.isfinite(sinergia) or sinergia < 0.0:
            return
        sym_key = str(symbol or "").strip().upper() or "__UNKNOWN__"
        with self._synergy_lock:
            hist = self._synergy_history_by_symbol.get(sym_key)
            if hist is None:
                hist = deque(maxlen=240)
                self._synergy_history_by_symbol[sym_key] = hist
            hist.append(min(sinergia, 1.0))
            maxlen = hist.maxlen or 240
            if len(hist) < max(10, maxlen // 4):
                return
            ahora = monotonic()
            last = self._last_synergy_check_by_symbol.get(sym_key, 0.0)
            if ahora - last < self._synergy_check_interval:
                return
            self._last_synergy_check_by_symbol[sym_key] = ahora
            valores = sorted(hist)
        dispersion = statistics.pstdev(valores) if len(valores) > 1 else 0.0
        index_90 = max(0, min(len(valores) - 1, math.ceil(0.9 * len(valores)) - 1))
        p90 = valores[index_90]
        saturaciones = sum(1 for valor in valores if valor >= self._synergy_cap * 0.98)
        saturacion = saturaciones / len(valores)
        try:
            if hasattr(self._metrics, "SYNERGY_CAP_DISPERSION"):
                self._metrics.SYNERGY_CAP_DISPERSION.set(dispersion)
            if hasattr(self._metrics, "SYNERGY_CAP_SATURATION"):
                self._metrics.SYNERGY_CAP_SATURATION.set(saturacion)
            if hasattr(self._metrics, "SYNERGY_CAP_P90"):
                self._metrics.SYNERGY_CAP_P90.set(p90)
        except Exception:  # pragma: no cover - publicar métricas no debe interrumpir el flujo
            log.exception("❌ Error actualizando métricas de sinergia")
        if saturacion > 0.3:
            log.warning(
                "⚠️ [%s] Sinergia alcanza el cap %.2f en %.1f%% de evaluaciones (p90=%.2f, disp=%.3f)",
                symbol,
                self._synergy_cap,
                saturacion * 100,
                p90,
                dispersion,
            )
        elif p90 < self._synergy_cap * 0.6 and dispersion < 0.1:
            log.info(
                "ℹ️ [%s] Sinergia muy por debajo del cap %.2f (p90=%.2f, disp=%.3f). Re-evaluar límite.",
                symbol,
                self._synergy_cap,
                p90,
                dispersion,
            )

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
