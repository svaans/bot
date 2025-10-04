import pandas as pd
from typing import Dict, Any
from indicadores.helpers import get_rsi, get_momentum, get_slope
from indicadores.divergencia_rsi import detectar_divergencia_alcista
from core.strategies.tendencia import detectar_tendencia
from core.scoring import PESOS_SCORE_TECNICO
from core.utils import configurar_logger
log = configurar_logger('analisis_salidas')


def precio_cerca_de_soporte(
    df: pd.DataFrame, precio: float, ventana: int = 30, margen: float = 0.003
) -> bool:
    """Comprueba si ``precio`` está cerca de un soporte reciente y validado."""
    if not isinstance(df, pd.DataFrame):
        return False
    columnas_requeridas = {'low', 'open', 'close'}
    if not columnas_requeridas.issubset(df.columns) or len(df) < ventana:
        return False
    recientes = df.tail(ventana)
    soporte = recientes['low'].min()
    if soporte <= 0 or precio > soporte * (1 + margen):
        return False
    rebotes_validos = (recientes['low'] <= soporte * (1 + margen)).sum()
    if rebotes_validos < 3:
        return False
    mecha_inf = min(recientes.iloc[-1]['open'], recientes.iloc[-1]['close']
        ) - recientes.iloc[-1]['low']
    cuerpo = abs(recientes.iloc[-1]['close'] - recientes.iloc[-1]['open'])
    hammer = cuerpo > 0 and mecha_inf > cuerpo * 2
    envolvente = es_vela_envolvente_alcista(recientes)
    volumen_creciente = False
    if 'volume' in recientes.columns:
        vol = recientes['volume'].tail(3)
        volumen_creciente = vol.iloc[-1] > vol.mean()
    return (hammer or envolvente) and volumen_creciente


def es_vela_envolvente_alcista(df: pd.DataFrame) ->bool:
    """Detecta patrón de vela envolvente alcista en las dos últimas velas."""
    if len(df) < 2:
        return False
    prev = df.iloc[-2]
    curr = df.iloc[-1]
    cuerpo_prev = prev['close'] - prev['open']
    cuerpo_curr = curr['close'] - curr['open']
    return cuerpo_prev < 0 and cuerpo_curr > 0 and curr['close'] > prev['open'
        ] and curr['open'] < prev['close']


def _score_tecnico_basico(df: pd.DataFrame, direccion: str) ->float:
    """Calcula un score técnico sencillo aplicando pesos configurables."""
    rsi = get_rsi(df)
    momentum = get_momentum(df)
    slope = get_slope(df)
    tendencia, _ = detectar_tendencia('', df)
    puntos = 0.0
    peso_rsi = PESOS_SCORE_TECNICO.get('RSI', 1.0)
    if rsi is not None:
        if (rsi > 50 if direccion == 'long' else rsi < 50):
            puntos += peso_rsi
        elif 45 <= rsi <= 55:
            puntos += peso_rsi * 0.5
    if momentum is not None and abs(momentum) > 0.001:
        puntos += PESOS_SCORE_TECNICO.get('Momentum', 1.0)
    if slope > 0.01:
        puntos += PESOS_SCORE_TECNICO.get('Slope', 1.0)
    if direccion == 'long':
        if tendencia in {'alcista', 'lateral'}:
            puntos += PESOS_SCORE_TECNICO.get('Tendencia', 1.0)
    else:
        if tendencia in {'bajista', 'lateral'}:
            puntos += PESOS_SCORE_TECNICO.get('Tendencia', 1.0)
    return float(puntos)


def evaluar_condiciones_de_cierre_anticipado(symbol: str, df: pd.DataFrame,
    orden: Dict[str, Any], score: float, estrategias_activas: (Dict[str,
    Any] | None)=None) ->bool:
    """Reevalúa el contexto técnico antes de confirmar un cierre por SL.

    Retorna ``True`` si se debe permitir el cierre, ``False`` si las
    condiciones aún respaldan mantener la operación abierta.
    """
    estrategias_activas = estrategias_activas or {}
    direccion = orden.get('direccion', 'long')
    rsi = get_rsi(df)
    volumen_actual = df['volume'].iloc[-1] if 'volume' in df else 0.0
    volumen_promedio = df['volume'].iloc[-20:-1].mean(
        ) if 'volume' in df and len(df) > 20 else 0.0
    volumen_rel = (
        volumen_actual / volumen_promedio if volumen_promedio else 0.0
    )
    slope = get_slope(df)
    tendencia_actual, _ = detectar_tendencia(symbol, df)
    cond_rsi = rsi is not None and (rsi > 55 if direccion == 'long' else rsi < 45)
    cond_vol = volumen_rel >= 1.5
    cond_slope = slope > 0 if direccion == 'long' else slope < 0
    cond_tendencia = tendencia_actual == orden.get('tendencia', tendencia_actual)
    condiciones = [cond_rsi, cond_vol, cond_slope, cond_tendencia]
    favorables = sum(1 for c in condiciones if c)
    intentos = len(orden.get('sl_evitar_info', []))
    max_evitar = orden.get('max_evitar_sl', 2)
    log.info(
        f'🔍 Evaluación de cierre anticipado: condiciones favorables = {favorables}/4 | '
        f'intentos: {intentos}/{max_evitar}'
    )
    if favorables >= 3 and intentos < max_evitar:
        detalles: list[str] = []
        if cond_rsi:
            detalles.append(
                f'RSI alto: {rsi:.0f}' if direccion == 'long' else f'RSI bajo: {rsi:.0f}'
            )
        if cond_vol:
            detalles.append(f'volumen fuerte: {volumen_rel:.1f}x')
        if cond_slope:
            detalles.append('pendiente a favor')
        if cond_tendencia:
            detalles.append('tendencia intacta')
        log.info('🛡️ Cierre por SL evitado | %s', ', '.join(detalles))
        return False
    if intentos >= max_evitar:
        log.info('⚠️ Límite de evitaciones de SL alcanzado')
    return True


def permitir_cierre_tecnico(
    symbol: str, df: pd.DataFrame, sl: float, precio: float
) -> bool:
    """Decide si se permite cerrar la operación ignorando posibles rebotes."""
    orden = None
    if isinstance(precio, dict):
        orden = precio
        precio = sl
        sl = orden.get('stop_loss', precio)
    if not isinstance(df, pd.DataFrame) or len(df) < 40:
        log.warning(
            f'[{symbol}] Datos insuficientes para análisis técnico de salida.')
        return True
    columnas_requeridas = {'open', 'close', 'low', 'high', 'volume'}
    if not columnas_requeridas.issubset(df.columns):
        log.warning(
            f'[{symbol}] DataFrame inválido para análisis técnico de salida.')
        return True
    df = df.tail(60).copy()
    ultimas = df.tail(3)
    vela_actual = ultimas.iloc[-1]
    cierre = float(vela_actual['close'])
    apertura = float(vela_actual['open'])
    bajo = float(vela_actual['low'])
    alto = float(vela_actual['high'])
    cuerpo = abs(cierre - apertura)
    rsi = get_rsi(df)
    rsi_series = get_rsi(df, serie_completa=True)
    pendiente_rsi = get_slope(pd.DataFrame({'close': rsi_series.dropna()})
        ) if isinstance(rsi_series, pd.Series) and len(df) >= 30 else None
    direccion = orden.get('direccion', 'long') if orden else 'long'
    score = _score_tecnico_basico(df, direccion)
    envolvente = es_vela_envolvente_alcista(df)
    soporte_cerca = precio_cerca_de_soporte(df, precio)
    divergencia = detectar_divergencia_alcista(df)
    if score <= 1:
        if not evaluar_condiciones_de_cierre_anticipado(symbol, df, orden or
            {}, score, orden.get('estrategias_activas') if orden else {}):
            return False
    if score < 2:
        log.info(
            f'❌ {symbol} Score técnico {score:.2f} < 2. Cierre obligatorio.')
        return True
    if orden:
        pesos = [v for v in orden.get('estrategias_activas', {}).values() if
            isinstance(v, (int, float))]
        if pesos and max(pesos) < 0.3:
            log.info(
                f'❌ {symbol} Estrategias de bajo peso. Cierre recomendado.')
            return True
    if (rsi is not None and rsi < 35 and pendiente_rsi is not None and 
        pendiente_rsi > 0):
        log.info(
            f'⚠️ {symbol} RSI bajo pero subiendo ({rsi:.2f}) → posible rebote. Evitar cierre.'
            )
        return False
    soporte_reciente = df['low'].rolling(20).min().iloc[-1]
    if soporte_cerca:
        log.info(
            f'⚠️ {symbol} Soporte técnico validado cercano → posible rebote.')
        return False
    if ultimas.iloc[-2]['close'] < ultimas.iloc[-2]['open'
        ] and cierre > apertura and cierre > ultimas.iloc[-2]['open'
        ] and apertura < ultimas.iloc[-2]['close']:
        log.info(
            f'⚠️ {symbol} vela envolvente alcista detectada → posible rebote. Evitar cierre.'
            )
        return False
    volumen_actual = vela_actual['volume']
    media_vol = df['volume'].rolling(20).mean().iloc[-1]
    mecha_inf = min(apertura, cierre) - bajo
    if volumen_actual > 1.2 * media_vol and mecha_inf > cuerpo:
        log.info(
            f'⚠️ {symbol} rechazo con volumen alto → posible soporte defendido.'
            )
        return False
    if ultimas.iloc[-1]['close'] > ultimas.iloc[-1]['open'] and ultimas.iloc[-2
        ]['close'] > ultimas.iloc[-2]['open']:
        distancia_sl = abs(precio - sl) / precio
        if distancia_sl < 0.005:
            log.info(
                f'⚠️ {symbol} velas verdes + SL muy cerca. Mejor esperar rebote.'
                )
            return False
    if (
        rsi is not None
        and pendiente_rsi is not None
        and pendiente_rsi > 0
        and df['close'].iloc[-1] < df['close'].iloc[-2] < df['close'].iloc[-3]
    ):
        log.info(
            f'⚠️ {symbol} divergencia alcista en RSI detectada. Posible rebote. Evitar cierre.'
            )
        return False
    if (
        rsi is not None
        and rsi < 50
        and cierre < apertura
        and cuerpo > 0.6 * (alto - bajo)
        and volumen_actual < media_vol
    ):
        log.info(
            f'✅ {symbol} debilidad confirmada → cierre justificado por rechazo bajista.'
            )
        return True
    ema12 = df['close'].ewm(span=12).mean().iloc[-1]
    ema26 = df['close'].ewm(span=26).mean().iloc[-1]
    if ema12 < ema26:
        log.info(
            f'✅ {symbol} cruce bajista de medias (EMA12 < EMA26). Cierre justificado.'
            )
        return True
    highs = df['high'].tail(4).values
    if highs[3] < highs[2] < highs[1]:
        log.info(
            f'✅ {symbol} máximos decrecientes detectados. Cierre justificado por estructura bajista.'
            )
        return True
    if mecha_inf > cuerpo * 2:
        log.info(
            f'⚠️ {symbol} mecha inferior larga → rechazo bajista. Evitar cierre.'
            )
        return False
    log.info(
        f'✅ {symbol} No se detecta defensa técnica significativa. Cierre técnico permitido.'
        )
    return True
