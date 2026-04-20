# core/orders/real_orders_parse.py — lectura de estructuras estilo CCXT
from __future__ import annotations

from typing import Any, Mapping


def coerce_open_orders(value: Any) -> list[dict]:
    if not value:
        return []
    if isinstance(value, list):
        return value
    return list(value)


def extraer_valor(mapeo: Mapping[str, Any] | None, claves: tuple[str, ...]) -> Any | None:
    """Obtiene el primer valor válido encontrado en ``claves`` dentro del mapeo.

    También revisa el campo ``info`` (estilo CCXT) para cubrir más formatos.
    """

    if not isinstance(mapeo, Mapping):
        return None
    for clave in claves:
        valor = mapeo.get(clave)
        if valor not in (None, ""):
            return valor
    info = mapeo.get("info")
    if isinstance(info, Mapping):
        for clave in claves:
            valor = info.get(clave)
            if valor not in (None, ""):
                return valor
    return None


def extraer_float(mapeo: Mapping[str, Any] | None, claves: tuple[str, ...]) -> float:
    """Convierte a ``float`` el primer valor encontrado en ``claves``."""

    valor = extraer_valor(mapeo, claves)
    try:
        if valor is None:
            return 0.0
        return float(valor)
    except (TypeError, ValueError):
        return 0.0


def coincide_operation_id(
    mapeo: Mapping[str, Any] | None,
    operation_id: str | None,
) -> bool:
    """Comprueba si ``operation_id`` coincide con el identificador de cliente."""

    if not operation_id or not isinstance(mapeo, Mapping):
        return False
    candidate = extraer_valor(
        mapeo,
        (
            "clientOrderId",
            "origClientOrderId",
            "clientOrderID",
            "orderId",
            "id",
        ),
    )
    if candidate is None:
        return False
    try:
        return str(candidate) == str(operation_id)
    except Exception:
        return False
