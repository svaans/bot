"""Utilidades comunes para el manejo de órdenes.

Estas funciones proporcionan conversiones y extracción de datos a partir de
estructuras ``meta`` utilizadas en distintos componentes del sistema de
órdenes. Extraerlas aquí permite reutilización y mantiene ``order_manager``
ligero.
"""
from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

META_NESTED_KEYS = ("orden", "order", "position", "trade", "entrada", "payload")
EXPOSICION_KEYS = (
    "exposicion_total",
    "exposicion",
    "exposure_total",
    "total_exposure",
    "exposure",
)
STOP_LOSS_META_KEYS = (
    "stop_loss",
    "sl",
    "precio_sl",
    "stop_loss_price",
    "stop",
)


def coerce_float(value: Any) -> float | None:
    """Convierte ``value`` en ``float`` si es posible."""

    if value is None:
        return None
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        if isinstance(value, float) and value != value:  # pragma: no cover - NaN defensivo
            return None
        return float(value)
    try:
        return float(str(value))
    except (TypeError, ValueError):
        return None


def coerce_int(value: Any) -> int | None:
    """Convierte ``value`` en ``int`` si es posible."""

    float_value = coerce_float(value)
    if float_value is None:
        return None
    try:
        return int(float_value)
    except (TypeError, ValueError):  # pragma: no cover - defensivo
        return None


def lookup_meta(meta: Mapping[str, Any], keys: Iterable[str]) -> Any:
    """Busca la primera coincidencia de ``keys`` en ``meta`` o sub-mappings conocidos."""

    for key in keys:
        if key in meta:
            return meta[key]
    for nested_key in META_NESTED_KEYS:
        nested = meta.get(nested_key)
        if isinstance(nested, Mapping):
            for key in keys:
                if key in nested:
                    return nested[key]
    return None


def resolve_quantity_from_meta(meta: Mapping[str, Any], precio: float) -> float:
    """Obtiene la cantidad a operar a partir de distintos campos del ``meta``."""

    quantity_value = lookup_meta(
        meta,
        (
            "cantidad",
            "cantidad_operar",
            "quantity",
            "qty",
            "size",
            "amount",
            "position_size",
        ),
    )
    quantity = coerce_float(quantity_value)
    if quantity is not None and quantity > 0:
        return float(quantity)

    capital_value = lookup_meta(
        meta,
        (
            "capital",
            "capital_operar",
            "capital_disponible",
            "risk_capital",
            "notional",
            "capital_usd",
        ),
    )
    capital = coerce_float(capital_value)
    if capital is not None and capital > 0 and precio > 0:
        return float(capital / precio)

    return 0.0


def extract_quantity(result: Any) -> float:
    """Normaliza distintos formatos de respuesta en una cantidad positiva."""

    if isinstance(result, (list, tuple)):
        candidate: Any | None = None
        if len(result) >= 2:
            candidate = result[1]
        elif result:
            candidate = result[0]
        value = coerce_float(candidate)
        return float(value) if value and value > 0 else 0.0

    if isinstance(result, Mapping):
        candidate = (
            result.get("cantidad")
            or result.get("quantity")
            or result.get("qty")
            or result.get("size")
        )
        candidate_value = coerce_float(candidate)
        return float(candidate_value) if candidate_value and candidate_value > 0 else 0.0

    value = coerce_float(result)
    return float(value) if value and value > 0 else 0.0
