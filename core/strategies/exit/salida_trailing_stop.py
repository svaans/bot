import pandas as pd
from indicadores.helpers import get_atr
from core.utils import configurar_logger
from core.strategies.exit.salida_utils import resultado_salida
from config.exit_defaults import load_exit_config
log = configurar_logger('salida_trailing_stop')


def salida_trailing_stop(orden: dict, df: pd.DataFrame, config: dict=None
    ) ->dict:
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
        atr = get_atr(df)
        if atr is None:
            return resultado_salida('Trailing Stop', False, 'ATR no disponible'
                )
        pct = cfg['trailing_pct']
        activacion = cfg.get('trailing_activacion', 0.0)
        entrada = orden.get('precio_entrada', precio_actual)
        nivel_activacion = entrada + activacion if direccion in ('long', 'compra') else entrada - activacion
        if not orden.get('trailing_activo'):
            if (direccion in ('long', 'compra') and precio_actual >= nivel_activacion) or (
                direccion in ('venta', 'short') and precio_actual <= nivel_activacion
            ):
                orden['trailing_activo'] = True
                orden['max_precio'] = precio_actual
            else:
                return resultado_salida('Trailing Stop', False, 'Activación no alcanzada')
        trailing_dist = max(atr * atr_mult, precio_actual * pct)
        if 'max_precio' not in orden:
            orden['max_precio'] = entrada
        if direccion in ['compra', 'long']:
            if precio_actual > orden['max_precio']:
                orden['max_precio'] = precio_actual
            sl_nuevo = orden['max_precio'] - trailing_dist
            sl_actual = orden.get('sl_trailing')
            tick = orden.get('tickSize', orden.get('tick_size', 0)) or 0
            if sl_actual is None or (sl_nuevo - sl_actual >= 2 * tick and sl_nuevo > sl_actual):
                orden['sl_trailing'] = sl_nuevo
            if precio_actual <= orden.get('sl_trailing', sl_nuevo):
                return resultado_salida('Trailing Stop', True,
                    f"Trailing Stop activado (long) → Max: {orden['max_precio']:.2f}, SL: {orden['sl_trailing']:.2f}, Precio actual: {precio_actual:.2f}"
                    , logger=log)
        else:
            if precio_actual < orden['max_precio']:
                orden['max_precio'] = precio_actual
            sl_nuevo = orden['max_precio'] + trailing_dist
            sl_actual = orden.get('sl_trailing')
            tick = orden.get('tickSize', orden.get('tick_size', 0)) or 0
            if sl_actual is None or (sl_actual - sl_nuevo >= 2 * tick and sl_nuevo < sl_actual):
                orden['sl_trailing'] = sl_nuevo
            if precio_actual >= orden.get('sl_trailing', sl_nuevo):
                return resultado_salida('Trailing Stop', True,
                    f"Trailing Stop activado (short) → Min: {orden['max_precio']:.2f}, SL: {orden['sl_trailing']:.2f}, Precio actual: {precio_actual:.2f}"
                    , logger=log)
        return resultado_salida('Trailing Stop', False, 'Trailing no activado')
    except (KeyError, ValueError, TypeError) as e:
        log.error(
            f"Error en trailing stop para {orden.get('symbol', 'SYM')}: {e}"
        )
        return resultado_salida('Trailing Stop', False,
            f'Error en trailing stop: {e}', logger=log)


def verificar_trailing_stop(info: dict, precio_actual: float, df: (pd.DataFrame | None) = None, config: dict = None) -> tuple[bool, str]:
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
    activacion = cfg.get('trailing_activacion', 0.0)
    atr = get_atr(df) if df is not None else None
    if atr is None:
        return False, 'ATR no disponible'
    direccion = info.get('direccion', 'long')
    if activacion:
        trigger_price = entrada + activacion if direccion in ('long', 'compra') else entrada - activacion
    else:
        trigger_price = entrada * trailing_start_ratio
    if max_price >= trigger_price:
        if usar_atr:
            trailing_dist = atr * atr_mult
        else:
            distancia_ratio = (
                cfg['trailing_distance_ratio']
                if config is None
                else config.get('trailing_distance_ratio', cfg['trailing_distance_ratio'])
            )
            trailing_dist = max_price * distancia_ratio

        ratio_fuerte = cfg.get('trailing_senal_fuerte_ratio', 0)
        mult_fuerte = cfg.get('trailing_senal_fuerte_mult', 1.0)
        cond_fuerte = (
            (direccion in ('long', 'compra') and max_price >= entrada * ratio_fuerte)
            or (direccion not in ('long', 'compra') and max_price <= entrada / ratio_fuerte if ratio_fuerte else False)
        )
        if ratio_fuerte and cond_fuerte:
            trailing_dist *= mult_fuerte

        trailing_stop = max_price - trailing_dist if direccion in ('long', 'compra') else max_price + trailing_dist
        if cfg['uso_trailing_technico'] and df is not None and len(df) >= 5:
            soporte = df['low'].rolling(window=5).min().iloc[-1]
            resistencia = df['high'].rolling(window=5).max().iloc[-1]
            if direccion in ('long', 'compra'):
                trailing_stop = max(trailing_stop, soporte)
            else:
                trailing_stop = min(trailing_stop, resistencia)
        tick = info.get('tickSize', info.get('tick_size', 0)) or 0
        sl_actual = info.get('sl_trailing')
        if sl_actual is None:
            info['sl_trailing'] = trailing_stop
        else:
            movimiento = trailing_stop - sl_actual if direccion in ('long', 'compra') else sl_actual - trailing_stop
            if movimiento >= 2 * tick and ((direccion in ('long', 'compra') and trailing_stop > sl_actual) or (direccion not in ('long', 'compra') and trailing_stop < sl_actual)):
                info['sl_trailing'] = trailing_stop
        sl_usar = info.get('sl_trailing', trailing_stop)
        if direccion in ('long', 'compra'):
            if precio_actual <= sl_usar:
                return (True,
                    f'Trailing Stop activado — Máximo: {max_price:.2f}, Límite: {sl_usar:.2f}, Precio actual: {precio_actual:.2f}'
                    )
        elif precio_actual >= sl_usar:
            return (True,
                f'Trailing Stop activado — Mínimo: {max_price:.2f}, Límite: {sl_usar:.2f}, Precio actual: {precio_actual:.2f}'
                )
        return (False,
            f'Trailing supervisando — Máx {max_price:.2f}, Límite {sl_usar:.2f}'
            )
    return False, ''
