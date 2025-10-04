import math
from typing import Dict, List

import pandas as pd
from indicadores.helpers import get_atr
from core.utils import configurar_logger
from core.strategies.exit.salida_utils import resultado_salida
from core.orders.order_model import ajustar_tick_size
log = configurar_logger('salida_takeprofit_atr')


def _ajustar_step_size(cantidad: float, step_size: float) -> float:
    """Ajusta ``cantidad`` al múltiplo inferior de ``step_size``."""
    if step_size <= 0:
        return cantidad
    return math.floor(cantidad / step_size) * step_size


def salida_takeprofit_atr(
    orden: Dict,
    df: pd.DataFrame,
    targets: List[Dict[str, float]] | None = None,
    tick_size: float = 0.0,
    step_size: float = 0.0,
) -> Dict:
    """Genera niveles de Take Profit basados en el ATR.

    Parameters
    ----------
    orden:
        Información de la orden actual. Se espera que incluya ``precio_entrada``
        y ``cantidad``.
    df:
        DataFrame de precios.
    targets:
        Lista de objetivos. Cada elemento debe contener ``porcentaje`` (multiplo
        del ATR) y ``qty_frac`` (fracción de la cantidad total a cerrar).
    tick_size:
        Incremento mínimo de precio permitido por el mercado.
    step_size:
        Incremento mínimo de cantidad permitido por el mercado.
    """

    try:
        if len(df) < 20 or 'close' not in df.columns:
            return resultado_salida('Take Profit', False, 'Datos insuficientes')
        direccion = orden.get('direccion', 'long')
        precio_actual = df['close'].iloc[-1]
        entrada = orden.get('precio_entrada')
        atr = get_atr(df)
        if atr is None or entrada is None:
            return resultado_salida(
                'Take Profit', False, 'ATR o entrada no disponibles'
            )

        cantidad_total = orden.get('cantidad', 0.0)
        if targets is None:
            targets = [{'porcentaje': 2.5, 'qty_frac': 1.0}]

        niveles: List[Dict[str, float]] = []
        alcanzados: List[Dict[str, float]] = []

        for t in targets:
            mult = t.get('porcentaje', 0.0)
            frac = t.get('qty_frac', 0.0)

            if direccion in ('long', 'compra'):
                precio = entrada + atr * mult
            else:
                precio = entrada - atr * mult

            precio = ajustar_tick_size(precio, tick_size, direccion)
            cantidad = _ajustar_step_size(cantidad_total * frac, step_size)
            data = {'price': precio, 'qty': cantidad}
            niveles.append(data)

            cond_hit = (
                direccion in ('long', 'compra') and precio_actual >= precio
            ) or (
                direccion in ('short', 'venta') and precio_actual <= precio
            )
            if cond_hit:
                alcanzados.append(data)

        if alcanzados:
            razon = f"TP-ATR alcanzado a {alcanzados[0]['price']:.4f}"
        else:
            razon = 'TP-ATR no alcanzado'

        return resultado_salida(
            'Take Profit', bool(alcanzados), razon, logger=log,
            targets=niveles, targets_hit=alcanzados
        )
    except (KeyError, ValueError, TypeError) as e:
        log.error(f"Error en TP-ATR para {orden.get('symbol', 'SYM')}: {e}")
        return resultado_salida(
            'Take Profit', False, f'Error TP-ATR: {e}', logger=log
        )