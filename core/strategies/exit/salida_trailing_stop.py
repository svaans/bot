import pandas as pd
from indicators.atr import calcular_atr
from core.utils import configurar_logger
from core.strategies.exit.salida_utils import resultado_salida
from config.exit_defaults import load_exit_config
log = configurar_logger('salida_trailing_stop')


def salida_trailing_stop(orden: dict, df: pd.DataFrame, config: dict=None
    ) ->dict:
    log.info('➡️ Entrando en salida_trailing_stop()')
    """
    Evalúa si debe cerrarse una orden utilizando lógica de trailing stop.

    Parámetros:
        - orden: dict con detalles de la orden
        - df: DataFrame con datos recientes del mercado
        - config: dict opcional con parámetros personalizados:
            * trailing_pct (ej. 0.015 para 1.5%)
            * modo (para variantes futuras del algoritmo)

    Devuelve:
        - dict con claves:
            * cerrar (bool): True si debe cerrarse
            * razon (str): explicación
    """
    try:
        if df is None or len(df) < 3 or not {'close'}.issubset(df.columns):
            return resultado_salida('Trailing Stop', False,
                'Datos insuficientes o mal formateados')
        precio_actual = df['close'].iloc[-1]
        direccion = orden.get('direccion', 'long')
        cfg = load_exit_config(orden.get('symbol', 'SYM'))
        if config:
            cfg.update(config)
        atr_mult = cfg['atr_multiplicador']
        atr = calcular_atr(df)
        if atr is None:
            return resultado_salida('Trailing Stop', False, 'ATR no disponible'
                )
        pct = cfg['trailing_pct']
        trailing_dist = max(atr * atr_mult, precio_actual * pct)
        if 'max_precio' not in orden:
            orden['max_precio'] = orden['precio_entrada']
        if direccion in ['compra', 'long']:
            if precio_actual > orden['max_precio']:
                orden['max_precio'] = precio_actual
            elif precio_actual < orden['max_precio'] - trailing_dist:
                return resultado_salida('Trailing Stop', True,
                    f"Trailing Stop activado (long) → Max: {orden['max_precio']:.2f}, Precio actual: {precio_actual:.2f}"
                    , logger=log)
        elif direccion in ['venta', 'short']:
            if precio_actual < orden['max_precio']:
                orden['max_precio'] = precio_actual
            elif precio_actual > orden['max_precio'] + trailing_dist:
                return resultado_salida('Trailing Stop', True,
                    f"Trailing Stop activado (short) → Min: {orden['max_precio']:.2f}, Precio actual: {precio_actual:.2f}"
                    , logger=log)
        return resultado_salida('Trailing Stop', False, 'Trailing no activado')
    except Exception as e:
        return resultado_salida('Trailing Stop', False,
            f'Error en trailing stop: {e}')


def verificar_trailing_stop(info: dict, precio_actual: float, df: (pd.DataFrame | None) = None, config: dict = None) -> tuple[bool, str]:
    log.info('➡️ Entrando en verificar_trailing_stop()')
    """
    Evalúa si debe cerrarse la orden usando lógica de trailing stop.

    Parámetros:
        - info: dict con información de la orden (precio_entrada, max_price, etc.)
        - precio_actual: último precio de mercado
        - config: configuración personalizada con claves:
            * trailing_start_ratio (ej: 1.015 para +1.5%)
            * trailing_distance_ratio (ej: 0.02 para -2%)

    Devuelve:
        - (True, "razón") si debe cerrarse
        - (False, "") si no
    """
    entrada = info['precio_entrada']
    max_price = info.get('max_price', entrada)
    cfg = load_exit_config(info.get('symbol', 'SYM'))
    if config:
        cfg.update(config)
    buffer_pct = cfg['trailing_buffer']
    if precio_actual > max_price * (1 + buffer_pct):
        info['max_price'] = precio_actual
        max_price = precio_actual
    trailing_start_ratio = cfg['trailing_start_ratio']
    atr_mult = cfg['atr_multiplicador']
    usar_atr = cfg['trailing_por_atr']
    atr = calcular_atr(df) if df is not None else None
    if atr is None:
        return False, 'ATR no disponible'
    trailing_trigger = entrada * trailing_start_ratio
    if max_price >= trailing_trigger:
        if usar_atr:
            trailing_stop = max_price - atr * atr_mult
        else:
            distancia_ratio = cfg['trailing_distance_ratio']
                ) if config else 0.02
            trailing_stop = max_price * (1 - distancia_ratio)
        if cfg['uso_trailing_technico'] and df is not None and len(df) >= 5:
            soporte = df['low'].rolling(window=5).min().iloc[-1]
            resistencia = df['high'].rolling(window=5).max().iloc[-1]
            if info.get('direccion', 'long') in ('long', 'compra'):
                trailing_stop = max(trailing_stop, soporte)
            else:
                trailing_stop = min(trailing_stop, resistencia)
        if info.get('direccion', 'long') in ('long', 'compra'):
            if precio_actual <= trailing_stop:
                return (True,
                    f'Trailing Stop activado — Máximo: {max_price:.2f}, Límite: {trailing_stop:.2f}, Precio actual: {precio_actual:.2f}'
                    )
        elif precio_actual >= trailing_stop:
            return (True,
                f'Trailing Stop activado — Mínimo: {max_price:.2f}, Límite: {trailing_stop:.2f}, Precio actual: {precio_actual:.2f}'
                )
        return (False,
            f'Trailing supervisando — Máx {max_price:.2f}, Límite {trailing_stop:.2f}'
            )
    return False, ''
