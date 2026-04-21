# core/orders/order_manager_crear.py — fachada `crear` (meta → abrir_async)
from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from core.metrics import registrar_crear_skip_quantity
from core.orders.order_helpers import coerce_float, coerce_int, lookup_meta
from core.orders.order_manager_helpers import fmt_exchange_err as _fmt_exchange_err
from core.orders.order_open_status import OrderOpenStatus
from core.utils.logger import configurar_logger
from core.utils.log_utils import safe_extra

log = configurar_logger("orders", modo_silencioso=True)


async def crear(
    manager: Any,
    *,
    symbol: str,
    side: str,
    precio: float,
    sl: float,
    tp: float,
    meta: Mapping[str, Any] | None = None,
) -> OrderOpenStatus:
    """Crea y envía una orden utilizando la interfaz moderna del Trader."""

    meta_map: Mapping[str, Any]
    if isinstance(meta, Mapping):
        meta_map = meta
    else:
        meta_map = {}

    direccion = str(side or "long").lower()
    if direccion in {"sell", "venta"}:
        direccion = "short"
    elif direccion not in {"long", "short"}:
        direccion = "long"

    estrategias_value = lookup_meta(
        meta_map,
        ("estrategias", "estrategias_activas", "strategies", "estrategias_detalle"),
    )
    estrategias = estrategias_value if isinstance(estrategias_value, dict) else {}

    tendencia_value = lookup_meta(meta_map, ("tendencia", "trend", "market_trend"))
    tendencia = str(tendencia_value) if tendencia_value is not None else ""

    cantidad, cantidad_source = await manager._resolve_quantity_with_fallback(
        symbol,
        precio,
        sl,
        meta_map,
    )
    if cantidad <= 0:
        registrar_crear_skip_quantity(symbol, direccion, cantidad_source)
        log.warning(
            "crear.skip_quantity",
            extra=safe_extra(
                {
                    "symbol": symbol,
                    "side": direccion,
                    "precio": precio,
                    "meta_keys": tuple(meta_map.keys()),
                    "quantity_source": cantidad_source,
                }
            ),
        )
        return OrderOpenStatus.FAILED
    if cantidad_source and cantidad_source != "meta":
        log.debug(
            "crear.quantity_fallback",
            extra=safe_extra(
                {
                    "symbol": symbol,
                    "side": direccion,
                    "source": cantidad_source,
                }
            ),
        )

    puntaje_val = lookup_meta(meta_map, ("score", "puntaje", "score_tecnico"))
    puntaje = coerce_float(puntaje_val) or 0.0

    umbral_val = lookup_meta(meta_map, ("umbral_score", "umbral", "threshold"))
    umbral = coerce_float(umbral_val) or 0.0

    score_tecnico_val = lookup_meta(
        meta_map,
        ("score_tecnico", "score_tecnico_final", "score_ajustado"),
    )
    score_tecnico = coerce_float(score_tecnico_val) or puntaje

    objetivo_val = lookup_meta(meta_map, ("objetivo", "cantidad_objetivo", "target_amount"))
    objetivo = coerce_float(objetivo_val)

    fracciones_val = lookup_meta(meta_map, ("fracciones", "fracciones_totales", "chunks"))
    fracciones_int = coerce_int(fracciones_val)
    fracciones = fracciones_int if fracciones_int and fracciones_int > 0 else 1

    detalles_value = lookup_meta(
        meta_map,
        ("detalles_tecnicos", "detalles", "technical_details"),
    )
    detalles_tecnicos = detalles_value if isinstance(detalles_value, dict) else None

    tick_size_val = lookup_meta(
        meta_map,
        ("tick_size", "precio_tick", "tick"),
    )
    step_size_val = lookup_meta(
        meta_map,
        ("step_size", "cantidad_step", "lot_step", "step"),
    )
    min_dist_val = lookup_meta(
        meta_map,
        ("min_dist_pct", "min_dist", "min_distance_pct"),
    )

    tick_size = coerce_float(tick_size_val)
    step_size = coerce_float(step_size_val)
    min_dist_pct = coerce_float(min_dist_val)

    filters_value = lookup_meta(meta_map, ("market_filters", "filtros", "filters"))
    if isinstance(filters_value, Mapping):
        if tick_size is None:
            tick_size = coerce_float(filters_value.get("tick_size"))
        if step_size is None:
            step_size = coerce_float(filters_value.get("step_size"))
        if min_dist_pct is None:
            min_dist_pct = coerce_float(filters_value.get("min_dist_pct"))

    candle_close_val = lookup_meta(
        meta_map,
        ("candle_close_ts", "timestamp", "bar_close_ts", "close_ts"),
    )
    candle_close_ts = coerce_int(candle_close_val)

    strategy_version_val = lookup_meta(
        meta_map,
        ("strategy_version", "version", "pipeline_version", "engine_version"),
    )
    strategy_version = str(strategy_version_val) if strategy_version_val is not None else None

    try:
        return await manager.abrir_async(
            symbol,
            precio,
            sl,
            tp,
            estrategias,
            tendencia or "",
            direccion=direccion,
            cantidad=cantidad,
            puntaje=puntaje,
            umbral=umbral,
            score_tecnico=score_tecnico,
            objetivo=objetivo,
            fracciones=fracciones,
            detalles_tecnicos=detalles_tecnicos,
            tick_size=tick_size,
            step_size=step_size,
            min_dist_pct=min_dist_pct,
            candle_close_ts=candle_close_ts,
            strategy_version=strategy_version,
        )
    except Exception as exc:  # pragma: no cover - defensivo
        log.exception(
            "crear.exception",
            extra=safe_extra(
                {
                    "symbol": symbol,
                    "side": direccion,
                    "error": _fmt_exchange_err(exc),
                }
            ),
        )
        return OrderOpenStatus.FAILED
