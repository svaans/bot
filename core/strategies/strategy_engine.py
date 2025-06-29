"""Motor de estrategias para el bot de trading."""

from __future__ import annotations

from core.strategies.pesos import gestor_pesos
from typing import Dict
from core.adaptador_umbral import calcular_umbral_adaptativo
from core.scoring import calcular_score_tecnico

from core.utils.validacion import validar_dataframe
from core.strategies.entry.validadores import (
    validar_volumen,
    validar_rsi,
    validar_slope,
    validar_bollinger,
    validar_max_min,
    validar_volumen_real,
    validar_spread,
)
from core.strategies.entry.validador_volumen_atipico import factor_volumen_atipico
from core.validaciones_comunes import validar_diversidad
from indicators.slope import calcular_slope
from indicators.momentum import calcular_momentum
from indicators.rsi import calcular_rsi

import pandas as pd

from core.evaluacion_tecnica import evaluar_estrategias
from core.strategies.entry.validaciones_tecnicas import hay_contradicciones
from core.strategies.exit.gestor_salidas import evaluar_salidas
from core.strategies.tendencia import detectar_tendencia
from core.utils.utils import configurar_logger
from core.utils import build_log_message

log = configurar_logger("engine", modo_silencioso=True)


class StrategyEngine:
    """Evalúa estrategias de entrada y salida."""

    @staticmethod
    def _verificar_checks(
        symbol: str,
        estrategias: dict[str, bool],
        score_total: float,
        score_tecnico: float,
        *,
        umbral: float,
        umbral_score: float,
        val_score: float,
        umbral_validacion: float,
        diversidad_minima: int,
        contradiccion: bool,
    ) -> tuple[bool, str | None]:
        """Evalúa diversidad, puntaje y coherencia y retorna el permiso."""

        cumple_div = validar_diversidad(symbol, estrategias, diversidad_minima)
        permitido = (
            score_total >= umbral
            and score_tecnico >= umbral_score
            and cumple_div
            and not contradiccion
            and val_score >= umbral_validacion
        )

        if permitido:
            return True, None

        if contradiccion:
            return False, "contradiccion"
        if val_score < umbral_validacion:
            return False, "validaciones_fallidas"
        if score_tecnico < umbral_score:
            return False, "score_tecnico_bajo"
        if score_total < umbral:
            return False, "score_bajo"
        if not cumple_div:
            return False, "diversidad_baja"
        return False, "desconocido"

    @staticmethod
    def evaluar_entrada(
        symbol: str,
        df: pd.DataFrame,
        tendencia: str | None = None,
        config: dict | None = None,
        pesos_symbol: dict | None = None,
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
                "spread": validar_spread(df, (config or {}).get("max_spread", 0.002)),
            }

        try:
            if pesos_symbol is None:
                pesos_symbol = gestor_pesos.obtener_pesos_symbol(symbol)

            
            if tendencia is None:
                tendencia, _ = detectar_tendencia(symbol, df)
            log.debug(f"[{symbol}] Tendencia usada: {tendencia}")

            
            resultado = evaluar_estrategias(symbol, df, tendencia)
            estrategias_activas = resultado.get("estrategias_activas", {})
            score_base = resultado.get("puntaje_total", 0.0)
            diversidad = resultado.get("diversidad", 0)
            sinergia = resultado.get("sinergia", 0.0)
            score_total = score_base * (1 + sinergia)

            factor_volumen = factor_volumen_atipico(df)
            score_total *= factor_volumen

            rsi_val = calcular_rsi(df)
            slope_val = calcular_slope(df)
            mom_val = calcular_momentum(df)

            vol_media = df["volume"].rolling(20).mean().iloc[-1]
            contexto = {
                "rsi": rsi_val,
                "slope": slope_val,
                "volumen": float(vol_media) if not pd.isna(vol_media) else 0.0,
                "tendencia": tendencia,
            }
            umbral = calcular_umbral_adaptativo(symbol, df, contexto)

            max_spread = (config or {}).get("max_spread", 0.002)
            validaciones = {
                "volumen": validar_volumen(df),
                "rsi": validar_rsi(df),
                "slope": validar_slope(df, tendencia),
                "bollinger": validar_bollinger(df),
                "spread": validar_spread(df, max_spread),
            }
            val_score = sum(validaciones.values()) / len(validaciones)
            umbral_validacion = (config or {}).get("umbral_validacion", 0.5)
            validaciones_fallidas = [k for k, v in validaciones.items() if v < umbral_validacion]

            contradiccion = hay_contradicciones(estrategias_activas)
            score_tec = calcular_score_tecnico(
                df, rsi_val, mom_val, slope_val, tendencia, symbol=symbol
            )

            umbral_score = (config or {}).get("umbral_score_tecnico", 1.0)
            permitido, motivo = StrategyEngine._verificar_checks(
                symbol,
                estrategias_activas,
                score_total,
                score_tec,
                umbral=umbral,
                umbral_score=umbral_score,
                val_score=val_score,
                umbral_validacion=umbral_validacion,
                diversidad_minima=(config or {}).get("diversidad_minima", 1),
                contradiccion=contradiccion,
            )

            log.info(
                build_log_message(
                    "entrada_evaluada",
                    symbol=symbol,
                    score=round(score_total, 2),
                    threshold=umbral,
                    score_tecnico=round(score_tec, 2),
                    umbral_score=umbral_score,
                    factor_volumen=factor_volumen,
                    permitido=permitido,
                    motivo=motivo,
                    checks_fallidas=validaciones_fallidas if not permitido else [],
                )
            )

            return {
                "permitido": permitido,
                "motivo_rechazo": motivo,
                "estrategias_activas": estrategias_activas,
                "score_total": round(score_total, 2),
                "factor_volumen": factor_volumen,
                "score_base": round(score_base, 2),
                "sinergia": round(sinergia, 2),
                "umbral": umbral,
                "umbral_score_tecnico": umbral_score,
                "diversidad": diversidad,
                "tendencia": tendencia,
                "rsi": rsi_val,
                "slope": slope_val,
                "momentum": mom_val,
                "validaciones_fallidas": validaciones_fallidas,
                "score_validaciones": round(val_score, 2),
                "score_tecnico": score_tec,
            }

        except Exception as e:  # noqa: BLE001
            log.error(f"❌ Error evaluando entrada para {symbol}: {e}")
            return {
                "permitido": False,
                "motivo_rechazo": "error",
                "estrategias_activas": {},
                "score_total": 0.0,
                "umbral": 0.0,
                "diversidad": 0,
            }


    @staticmethod
    def evaluar_salida(df: pd.DataFrame, orden: Dict) -> Dict:
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
            return evaluar_salidas(orden, df)
        except Exception as e:
            log.error(f"❌ Error evaluando salida: {e}")
            return {}
