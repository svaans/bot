import pandas as pd
from indicators.helpers import get_rsi
from indicators.adx import calcular_adx
from core.utils.utils import configurar_logger

log = configurar_logger('indicador_tendencia')

def obtener_tendencia(symbol: str, df: pd.DataFrame) -> str:
    """Calcula la tendencia del mercado para ``symbol``.

    Analiza medias móviles exponenciales, pendiente, RSI y ADX para
    determinar si la tendencia es alcista, bajista o lateral.
    """
    log.info('➡️ Entrando en obtener_tendencia()')
    if df is None or df.empty or 'close' not in df.columns or len(df) < 60:
        log.warning(f'⚠️ Datos insuficientes para detectar tendencia en {symbol}')
        return 'lateral'

    df = df.copy()
    df['ema_fast'] = df['close'].ewm(span=10, adjust=False).mean()
    df['ema_slow'] = df['close'].ewm(span=30, adjust=False).mean()
    delta = df['ema_fast'].iloc[-1] - df['ema_slow'].iloc[-1]
    slope = (df['ema_slow'].iloc[-1] - df['ema_slow'].iloc[-5]) / 5
    rsi = get_rsi(df)
    close_std = df['close'].std()
    umbral = max(close_std * 0.02, 0.1)

    puntos_alcista = 0
    puntos_bajista = 0

    if delta > umbral * 0.8:
        puntos_alcista += 1
    elif delta < -umbral * 0.8:
        puntos_bajista += 1

    if slope > 0.008:
        puntos_alcista += 1
    elif slope < -0.008:
        puntos_bajista += 1

    if rsi is not None:
        if rsi > 58:
            puntos_alcista += 1
        elif rsi < 42:
            puntos_bajista += 1

    adx = calcular_adx(df)
    if adx > 20:
        puntos_alcista += 1
        puntos_bajista += 1

    if puntos_alcista >= 2 and puntos_alcista > puntos_bajista:
        tendencia = 'alcista'
    elif puntos_bajista >= 2 and puntos_bajista > puntos_alcista:
        tendencia = 'bajista'
    else:
        tendencia = 'lateral'

    log.info({
        'evento': 'deteccion_tendencia',
        'symbol': symbol,
        'tendencia': tendencia,
        'delta_ema': round(delta, 6),
        'slope_local': round(slope, 6),
        'rsi': round(rsi, 2) if rsi else None,
        'adx': round(adx, 2) if adx else None,
    })
    return tendencia