import numpy as np
import pandas as pd
from ta.momentum import RSIIndicator
from core.utils.utils import configurar_logger
from indicators.helpers import get_slope
log = configurar_logger('persistencia')


def calcular_persistencia_minima(symbol: str, df: pd.DataFrame, tendencia:
    str, base_minimo: float=1.0) ->float:
    log.info('➡️ Entrando en calcular_persistencia_minima()')
    """Calcula un umbral de persistencia adaptativo según volatilidad, pendiente y RSI."""
    if df is None or len(df) < 10 or 'close' not in df.columns:
        log.debug(
            f'⚠️ [{symbol}] Datos insuficientes o inválidos para persistencia adaptativa'
            )
        return base_minimo
    ventana = df['close'].tail(10)
    media = np.mean(ventana)
    if np.isnan(media) or media == 0:
        log.debug(
            f'⚠️ [{symbol}] Media de cierre inválida. Usando mínimo base {base_minimo}'
            )
        return base_minimo
    volatilidad = np.std(ventana) / media
    slope = get_slope(df, periodo=10)
    if 'volume' in df.columns:
        vol_act = float(df['volume'].iloc[-1])
        vol_med = float(df['volume'].tail(10).mean())
        vol_factor = vol_act / vol_med if vol_med > 0 else 1.0
    else:
        vol_factor = 1.0
    try:
        rsi = RSIIndicator(close=df['close'], window=14).rsi().iloc[-1]
    except Exception as e:
        log.warning(f'⚠️ [{symbol}] Error calculando RSI: {e}')
        rsi = 50
    minimo = base_minimo * (1 + volatilidad * 0.5) * (1 + (vol_factor - 1) *
        0.5)
    if tendencia == 'lateral':
        minimo += 0.5 * vol_factor
    slope_thr = 0.002 * (1 + volatilidad)
    if (tendencia == 'alcista' and slope > slope_thr or tendencia ==
        'bajista' and slope < -slope_thr):
        minimo = max(minimo - 0.2 * vol_factor, 0.5)
    rsi_high = 70 - volatilidad * 20
    rsi_low = 30 + volatilidad * 20
    if rsi > rsi_high or rsi < rsi_low:
        minimo += 0.2 * (2 - min(vol_factor, 2))
    minimo = round(max(0.5, min(minimo, 5.0)), 2)
    log.debug(
        f'[{symbol}] Persistencia adaptada: Base {base_minimo:.2f} -> {minimo:.2f} | Vol {volatilidad:.4f} | Slope {slope:.4f} | RSI {rsi:.2f}'
        )
    return minimo
