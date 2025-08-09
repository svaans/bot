import pandas as pd

def actualizar_rsi_incremental(estado, periodo: int = 14) -> None:
    """Actualiza el RSI de forma incremental y lo almacena en cache.

    El resultado se guarda tanto en ``estado.indicadores_cache`` como en
    ``estado.df.attrs['_indicators_cache']`` para que las funciones helper puedan
    reutilizarlo.
    """
    df = getattr(estado, 'df', None)
    if df is None or df.empty or 'close' not in df.columns:
        return
    cache_global = getattr(estado, 'indicadores_cache', {})
    datos_rsi = cache_global.get('rsi')
    ultimo_cierre = float(df['close'].iloc[-1])
    if not datos_rsi or datos_rsi.get('periodo') != periodo or len(df) <= periodo:
        # Inicialización: calcular RSI completo y promedios
        delta = df['close'].diff()
        ganancia = delta.clip(lower=0)
        perdida = -delta.clip(upper=0)
        avg_gain = ganancia.ewm(alpha=1/periodo, adjust=False, min_periods=periodo).mean().iloc[-1]
        avg_loss = perdida.ewm(alpha=1/periodo, adjust=False, min_periods=periodo).mean().iloc[-1]
        if avg_loss == 0:
            rsi = 100.0
        else:
            rs = avg_gain / avg_loss
            rsi = 100 - 100 / (1 + rs)
    else:
        prev_close = datos_rsi['prev_close']
        delta = ultimo_cierre - prev_close
        gain = max(delta, 0.0)
        loss = max(-delta, 0.0)
        avg_gain = (datos_rsi['avg_gain'] * (periodo - 1) + gain) / periodo
        avg_loss = (datos_rsi['avg_loss'] * (periodo - 1) + loss) / periodo
        if avg_loss == 0:
            rsi = 100.0
        else:
            rs = avg_gain / avg_loss
            rsi = 100 - 100 / (1 + rs)
    cache_global['rsi'] = {
        'periodo': periodo,
        'avg_gain': float(avg_gain),
        'avg_loss': float(avg_loss),
        'prev_close': ultimo_cierre,
        'valor': float(rsi),
    }
    df.attrs.setdefault('_indicators_cache', {})[('rsi', periodo, False)] = float(rsi)