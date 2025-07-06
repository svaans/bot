"""Motor de estrategias para el bot de trading."""
from __future__ import annotations
from core.strategies.pesos import gestor_pesos
from typing import Dict
from core.adaptador_umbral import calcular_umbral_adaptativo
from core.score_tecnico import calcular_score_tecnico
from core.utils.utils import validar_dataframe
from core.strategies.entry.validadores import validar_volumen, validar_rsi, validar_slope, validar_bollinger, validar_max_min, validar_volumen_real, validar_spread
from indicators.slope import calcular_slope
from indicators.momentum import calcular_momentum
from indicators.rsi import calcular_rsi
import pandas as pd
from core.evaluacion_tecnica import evaluar_estrategias
from core.strategies.entry.validaciones_tecnicas import hay_contradicciones
from core.strategies.exit.gestor_salidas import evaluar_salidas
from core.strategies.tendencia import detectar_tendencia
from core.utils.utils import configurar_logger
log = configurar_logger('engine', modo_silencioso=True)


class StrategyEngine:
    """Evalúa estrategias de entrada y salida."""

    @staticmethod
    def evaluar_entrada(symbol: str, df: pd.DataFrame, tendencia: (str |
        None)=None, config: (dict | None)=None, pesos_symbol: (dict | None)
        =None) ->Dict:
        log.info('➡️ Entrando en evaluar_entrada()')
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
        if not symbol or not validar_dataframe(df, ['close', 'high', 'low',
            'volume']):
            log.warning('⚠️ Entrada inválida: símbolo o DataFrame inválido.')
            return {'permitido': False, 'motivo_rechazo': 'datos_invalidos',
                'estrategias_activas': {}, 'score_total': 0.0, 'umbral': 
                0.0, 'diversidad': 0, 'max_min': validar_max_min(df),
                'volumen_real': validar_volumen_real(df), 'spread':
                validar_spread(df)}
        try:
            if pesos_symbol is None:
                pesos_symbol = gestor_pesos.obtener_pesos_symbol(symbol)
            if tendencia is None:
                tendencia, _ = detectar_tendencia(symbol, df)
            log.debug(f'[{symbol}] Tendencia usada: {tendencia}')
            resultado = evaluar_estrategias(symbol, df, tendencia)
            estrategias_activas = resultado.get('estrategias_activas', {})
            score_base = resultado.get('puntaje_total', 0.0)
            diversidad = resultado.get('diversidad', 0)
            sinergia = resultado.get('sinergia', 0.0)
            score_total = score_base * (1 + sinergia)
            rsi_val = calcular_rsi(df)
            slope_val = calcular_slope(df)
            mom_val = calcular_momentum(df)
            vol_media = df['volume'].rolling(20).mean().iloc[-1]
            contexto = {'rsi': rsi_val, 'slope': slope_val, 'volumen': 
                float(vol_media) if not pd.isna(vol_media) else 0.0,
                'tendencia': tendencia}
            umbral = calcular_umbral_adaptativo(symbol, df, contexto)
            validaciones = {'volumen': validar_volumen(df), 'rsi':
                validar_rsi(df), 'slope': validar_slope(df, tendencia),
                'bollinger': validar_bollinger(df)}
            validaciones_fallidas = [k for k, v in validaciones.items() if 
                not v]
            contradiccion = hay_contradicciones(estrategias_activas)
            score_tec = calcular_score_tecnico(df, rsi_val, mom_val,
                slope_val, tendencia)
            cumple_div = diversidad >= (config or {}).get('diversidad_minima',
                1)
            umbral_score = (config or {}).get('umbral_score_tecnico', 1.0)
            permitido = (score_total >= umbral and score_tec >=
                umbral_score and cumple_div and not contradiccion and not
                validaciones_fallidas)
            motivo = None
            if not permitido:
                if contradiccion:
                    motivo = 'contradiccion'
                elif validaciones_fallidas:
                    motivo = 'validaciones_fallidas'
                elif score_tec < umbral_score:
                    motivo = 'score_tecnico_bajo'
                elif score_total < umbral:
                    motivo = 'score_bajo'
                elif not cumple_div:
                    motivo = 'diversidad_baja'
                else:
                    motivo = 'desconocido'
            return {'permitido': permitido, 'motivo_rechazo': motivo,
                'estrategias_activas': estrategias_activas, 'score_total':
                round(score_total, 2), 'score_base': round(score_base, 2),
                'sinergia': round(sinergia, 2), 'umbral': umbral,
                'umbral_score_tecnico': umbral_score, 'diversidad':
                diversidad, 'tendencia': tendencia, 'rsi': rsi_val, 'slope':
                slope_val, 'momentum': mom_val, 'validaciones_fallidas':
                validaciones_fallidas, 'score_tecnico': score_tec}
        except Exception as e:
            log.error(f'❌ Error evaluando entrada para {symbol}: {e}')
            return {'permitido': False, 'motivo_rechazo': 'error',
                'estrategias_activas': {}, 'score_total': 0.0, 'umbral': 
                0.0, 'diversidad': 0}

    @staticmethod
    def evaluar_salida(df: pd.DataFrame, orden: Dict) ->Dict:
        log.info('➡️ Entrando en evaluar_salida()')
        """
        Evalúa si se debe cerrar una orden activa.

        Args:
            df: DataFrame con datos recientes del mercado.
            orden: Diccionario con información de la orden activa.

        Returns:
            Diccionario con resultados de las estrategias de salida.
        """
        if df is None or df.empty or not orden:
            log.warning('⚠️ Evaluación de salida con datos insuficientes.')
            return {}
        try:
            return evaluar_salidas(orden, df)
        except Exception as e:
            log.error(f'❌ Error evaluando salida: {e}')
            return {}
