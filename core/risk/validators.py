from __future__ import annotations

"""Rutinas compartidas para validar niveles de stop-loss y take-profit."""

from decimal import Decimal, ROUND_CEILING, ROUND_DOWN, ROUND_HALF_UP, InvalidOperation
from typing import Literal

from core.utils.logger import configurar_logger

log = configurar_logger('risk_validators', modo_silencioso=True)


class LevelValidationError(ValueError):
    """Error de validación con contexto estructurado."""

    def __init__(self, reason: str, context: dict[str, float | str]) -> None:
        super().__init__(reason)
        self.reason = reason
        self.context = context


_RoundingMode = Literal["down", "up", "nearest"]


def _coerce_log_value(value: float | str | Decimal) -> float | str:
    """Convierte valores a tipos logueables sin mutar el contexto original."""

    return float(value) if isinstance(value, Decimal) else value


def _raise_validation_error(reason: str, context: dict[str, float | str]) -> None:
    """Registra contexto estructurado y lanza la excepción de validación."""

    flat_context = {f"context_{k}": _coerce_log_value(v) for k, v in context.items()}
    log.warning(
        'Level validation error',
        extra={
            'event': 'level_validation_error',
            'reason': reason,
            **flat_context,
        },
    )
    raise LevelValidationError(reason, context)


def _to_decimal(value: float) -> Decimal:
    return Decimal(str(value))


def _quantize(value: float, increment: float, mode: _RoundingMode) -> float:
    if increment <= 0:
        return float(value)
    try:
        dec_value = _to_decimal(value)
        dec_increment = _to_decimal(increment)
    except InvalidOperation:  # fallback numérico
        return float(value)
    if mode == "down":
        rounded = (dec_value / dec_increment).to_integral_value(rounding=ROUND_DOWN)
    elif mode == "up":
        rounded = (dec_value / dec_increment).to_integral_value(rounding=ROUND_CEILING)
    else:
        rounded = (dec_value / dec_increment).to_integral_value(rounding=ROUND_HALF_UP)
    return float(rounded * dec_increment)


def validate_levels(
    side: str,
    entry: float,
    sl: float,
    tp: float,
    min_dist_pct: float,
    tick_size: float,
    step_size: float,
) -> tuple[float, float, float]:
    """Valida y ajusta niveles de SL/TP según reglas canónicas.

    El orden de validación es:

    1. Normalizar ``sl`` y ``tp`` a ``tick_size`` (o ``step_size`` si el
       ``tick_size`` es cero).
    2. Verificar que la jerarquía de precios sea coherente con la dirección
       (``long``: ``sl < entry < tp``; ``short``: ``tp < entry < sl``).
    3. Comprobar distancias mínimas con respecto a ``entry``.

    Parameters
    ----------
    side:
        Dirección de la operación (``"long"`` o ``"short"``).
    entry:
        Precio de entrada propuesto.
    sl:
        Stop loss propuesto.
    tp:
        Take profit propuesto.
    min_dist_pct:
        Distancia mínima porcentual respecto a ``entry``.
    tick_size:
        Incremento mínimo permitido por el exchange para el precio.
    step_size:
        Incremento alternativo utilizado si ``tick_size`` no está disponible.

    Returns
    -------
    tuple[float, float, float]
        ``(entry, sl, tp)`` ajustados.

    Raises
    ------
    LevelValidationError
        Si las reglas de validación no se cumplen.
    """

    contexto_base: dict[str, float | str] = {
        "side": side,
        "entry": float(entry),
        "sl": float(sl),
        "tp": float(tp),
        "min_dist_pct": float(min_dist_pct),
        "tick_size": float(tick_size),
        "step_size": float(step_size),
    }

    if not (entry and min_dist_pct is not None):
        _raise_validation_error("invalid_input", contexto_base)

    side_norm = side.lower()
    if side_norm not in {"long", "short"}:
        _raise_validation_error("invalid_side", contexto_base)

    price_increment = tick_size if tick_size and tick_size > 0 else 0.0
    if price_increment <= 0 and step_size and step_size > 0:
        price_increment = step_size

    sl_mode: _RoundingMode = "down" if side_norm == "long" else "up"
    tp_mode: _RoundingMode = "up" if side_norm == "long" else "down"

    sl_adj = _quantize(sl, price_increment, sl_mode)
    tp_adj = _quantize(tp, price_increment, tp_mode)

    exchange_min = price_increment if price_increment > 0 else 0.0
    contexto = {**contexto_base, "sl_adj": sl_adj, "tp_adj": tp_adj, "exchange_min": exchange_min}

    if side_norm == "long":
        if not (sl_adj < entry < tp_adj):
            _raise_validation_error("direction_mismatch", contexto)
    else:
        if not (tp_adj < entry < sl_adj):
            _raise_validation_error("direction_mismatch", contexto)

    min_dist_pct = max(float(min_dist_pct), 0.0)
    min_distance = max(entry * min_dist_pct, exchange_min)

    if abs(entry - sl_adj) < min_distance or abs(tp_adj - entry) < min_distance:
        contexto["min_distance"] = min_distance
        raise LevelValidationError("min_distance", contexto)

    return float(entry), float(sl_adj), float(tp_adj)