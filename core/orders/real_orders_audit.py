# core/orders/real_orders_audit.py — verificación cruzada tras errores REST
from __future__ import annotations

from typing import Any, Mapping

from core.orders.real_orders_parse import coincide_operation_id as _coincide_operation_id
from core.orders.real_orders_parse import extraer_float as _extraer_float
from core.utils.utils import configurar_logger

log = configurar_logger("ordenes")


def auditar_operacion_post_error(
    cliente: Any,
    symbol: str,
    operation_id: str | None,
    cantidad_esperada: float,
) -> dict[str, Any] | None:
    """Intenta confirmar ejecuciones usando fuentes alternativas tras un fallo REST."""

    if not operation_id:
        return None

    try:
        cerradas = cliente.fetch_closed_orders(symbol, limit=10)
    except Exception as cerr_err:
        log.warning(
            f"⚠️ Auditoría: error al consultar órdenes cerradas para {symbol}: {cerr_err}"
        )
        cerradas = []
    for orden in cerradas or []:
        if not isinstance(orden, Mapping):
            continue
        if not _coincide_operation_id(orden, operation_id):
            continue
        ejecutado = max(
            _extraer_float(orden, ("filled", "amount", "executedQty")),
            0.0,
        )
        restante = max(
            _extraer_float(orden, ("remaining", "remainingQty")),
            cantidad_esperada - ejecutado,
            0.0,
        )
        precio = _extraer_float(orden, ("average", "price", "stopPrice"))
        if ejecutado > 0:
            return {
                "ejecutado": ejecutado,
                "restante": restante,
                "precio": precio,
                "source": "closed_orders",
                "raw": orden,
            }

    try:
        import core.orders.real_orders as _ro_mod

        abiertas = _ro_mod.consultar_ordenes_abiertas(symbol)
    except Exception as abiertas_err:
        log.warning(
            f"⚠️ Auditoría: error al consultar órdenes abiertas para {symbol}: {abiertas_err}"
        )
        abiertas = []
    for orden in abiertas or []:
        if not isinstance(orden, Mapping):
            continue
        if not _coincide_operation_id(orden, operation_id):
            continue
        ejecutado = max(
            _extraer_float(orden, ("filled", "executedQty", "cummulativeQuoteQty")),
            0.0,
        )
        total = _extraer_float(orden, ("amount", "origQty", "quantity"))
        restante = max(total - ejecutado, 0.0)
        precio = _extraer_float(orden, ("average", "price"))
        if ejecutado > 0:
            return {
                "ejecutado": ejecutado,
                "restante": restante,
                "precio": precio,
                "source": "open_orders",
                "raw": orden,
            }
        return {
            "ejecutado": 0.0,
            "restante": restante if restante > 0 else total,
            "precio": precio,
            "source": "open_orders_pending",
            "raw": orden,
        }

    return None
