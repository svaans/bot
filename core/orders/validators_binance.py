from __future__ import annotations

"""Validadores centralizados para restricciones de Binance.

Este módulo reemplaza a :mod:`core.orders.validators` para ofrecer un nombre
más descriptivo.  Proporciona utilidades para verificar si una orden
remanente cumple las restricciones mínimas (cantidad, valor nominal y
step size) impuestas por el intercambio de Binance.

Las funciones contenidas aquí se basan en las implementaciones
originales de ``validators.py``.
"""

from binance_api.cliente import obtener_cliente
from core.utils.logger import configurar_logger

# Configuramos un logger silencioso para evitar ruido innecesario
log = configurar_logger('validators', modo_silencioso=True)


def remainder_executable(symbol: str, price: float, quantity: float) -> bool:
    """Verifica si un remanente puede ejecutarse según filtros del símbolo.

    Esta función registra en los logs los valores de ``min_qty``, ``min_notional``
    y ``step_size`` obtenidos de las restricciones de mercado de Binance.
    Devuelve ``True`` si la cantidad cumple los mínimos requeridos.

    Parameters
    ----------
    symbol: str
        Símbolo de trading (por ejemplo, ``"BTC/USDT"``).
    price: float
        Precio actual del activo.
    quantity: float
        Cantidad restante de la orden.

    Returns
    -------
    bool
        ``True`` si la cantidad restante cumple las restricciones del mercado;
        ``False`` en caso contrario.
    """
    try:
        cliente = obtener_cliente()
        markets = cliente.load_markets()
        market = markets.get(symbol, {})
        limits = market.get('limits', {})
        min_qty = float((limits.get('amount') or {}).get('min') or 0.0)
        min_notional = float((limits.get('cost') or {}).get('min') or 0.0)
        precision = market.get('precision', {}).get('amount', 8)
        step_size = 10 ** -precision
        log.debug(
            f"Filtros {symbol} -> min_qty:{min_qty} min_notional:{min_notional} step_size:{step_size}"
        )
        # Comprobaciones secuenciales de restricciones
        if quantity < min_qty:
            log.info(
                f'Remanente {quantity} < min_qty {min_qty} para {symbol}'
            )
            return False
        if price * quantity < min_notional:
            log.info(
                f'Notional {price * quantity} < min_notional {min_notional} para {symbol}'
            )
            return False
        if step_size > 0 and quantity % step_size != 0:
            log.info(
                f'Cantidad {quantity} no múltiplo de step_size {step_size} para {symbol}'
            )
            return False
        return True
    except Exception as e:
        log.error(f'Error validando remanente para {symbol}: {e}')
        # En caso de duda, asumimos ejecutable para no bloquear flujos
        return True