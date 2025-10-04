import pandas as pd
from indicadores.helpers import get_atr
from core.utils import configurar_logger
from config.exit_defaults import load_exit_config
log = configurar_logger('salida_break_even')


def salida_break_even(orden: dict, df: pd.DataFrame, config: (dict | None)=None
    ) -> dict:
    """Evalúa si se debe mover el Stop Loss a precio de entrada."""
    try:
        if df is None or len(df) < 15 or not {'close', 'high', 'low'}.issubset(
            df.columns):
            return {'cerrar': False}
        entrada = orden.get('precio_entrada')
        direccion = orden.get('direccion', 'long')
        if entrada is None:
            return {'cerrar': False}
        precio_actual = df['close'].iloc[-1]
        cfg = load_exit_config(orden.get('symbol', 'SYM'))
        if config:
            cfg.update(config)
        atr_periodo = cfg['periodo_atr']
        atr = get_atr(df, atr_periodo)
        if atr is None:
            return {'cerrar': False}
        multiplicador = cfg['break_even_atr_mult']
        umbral = atr * multiplicador
        tp1_alcanzado = orden.get('parcial_cerrado', False)
        if direccion in ('long', 'compra'):
            if tp1_alcanzado or precio_actual >= entrada + umbral:
                log.info(
                    f"🟡 Break-Even activado para {orden.get('symbol', 'SYM')} → SL movido a entrada: {entrada}"
                    )
                return {'cerrar': False, 'break_even': True, 'nuevo_sl':
                    entrada}
        elif tp1_alcanzado or precio_actual <= entrada - umbral:
            log.info(
                f"🟡 Break-Even activado para {orden.get('symbol', 'SYM')} → SL movido a entrada: {entrada}"
                )
            return {'cerrar': False, 'break_even': True, 'nuevo_sl': entrada}
        return {'cerrar': False}
    except (KeyError, ValueError, TypeError) as e:
        log.warning(f"Error en salida_break_even para {orden.get('symbol', 'SYM')}: {e}")
        return {'cerrar': False}
