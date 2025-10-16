"""Herramientas asincrónicas para resolver cantidades a operar.

Separar la lógica de resolución de cantidades mantiene ``order_manager`` más
legible y permite reutilizar la misma política en otros administradores de
órdenes.
"""
from __future__ import annotations

import asyncio
from typing import Any, Mapping

from core.orders.order_helpers import (
    EXPOSICION_KEYS,
    STOP_LOSS_META_KEYS,
    extract_quantity,
    lookup_meta,
    resolve_quantity_from_meta,
    coerce_float,
)
from core.utils.log_utils import safe_extra


class QuantityResolver:
    """Resuelve la cantidad a operar utilizando diversas fuentes."""

    def __init__(
        self,
        *,
        log,
        bus,
        quantity_timeout: float,
    ) -> None:
        self.log = log
        self.bus = bus
        self._quantity_timeout = quantity_timeout

    async def resolve(
        self,
        symbol: str,
        precio: float,
        sl: float,
        meta: Mapping[str, Any],
        capital_manager: Any | None,
    ) -> tuple[float, str]:
        """Obtiene la cantidad final junto con la fuente utilizada."""

        base_quantity = resolve_quantity_from_meta(meta, precio)
        if base_quantity > 0:
            return float(base_quantity), "meta"

        exposicion_val = lookup_meta(meta, EXPOSICION_KEYS)
        exposicion = coerce_float(exposicion_val) or 0.0
        stop_loss_val = lookup_meta(meta, STOP_LOSS_META_KEYS)
        stop_loss = coerce_float(stop_loss_val)
        if stop_loss is None and sl > 0:
            stop_loss = float(sl)

        quantity = await self._solicitar_cantidad_capital_manager(
            capital_manager,
            symbol,
            precio,
            exposicion,
            stop_loss,
        )
        if quantity > 0:
            return quantity, "capital_manager"

        quantity = await self._solicitar_cantidad_bus(
            symbol,
            precio,
            exposicion,
            stop_loss,
        )
        if quantity > 0:
            return quantity, "event_bus"

        return 0.0, "none"

    async def _solicitar_cantidad_capital_manager(
        self,
        capital_manager: Any | None,
        symbol: str,
        precio: float,
        exposicion: float,
        stop_loss: float | None,
    ) -> float:
        manager = capital_manager
        if manager is None:
            return 0.0

        resolver = getattr(manager, "calcular_cantidad_async", None)
        if not callable(resolver):
            return 0.0

        try:
            resultado = resolver(
                symbol,
                precio,
                exposicion_total=exposicion,
                stop_loss=stop_loss,
            )
            if asyncio.iscoroutine(resultado):
                resultado = await resultado
            cantidad = extract_quantity(resultado)
            return cantidad if cantidad > 0 else 0.0
        except Exception:  # pragma: no cover - defensivo
            self.log.warning(
                "crear.quantity_fallback_capital_error",
                exc_info=True,
                extra=safe_extra(
                    {
                        "symbol": symbol,
                        "precio": precio,
                        "exposicion_total": exposicion,
                    }
                ),
            )
            return 0.0

    async def _solicitar_cantidad_bus(
        self,
        symbol: str,
        precio: float,
        exposicion: float,
        stop_loss: float | None,
    ) -> float:
        if not self.bus:
            return 0.0

        request = getattr(self.bus, "request", None)
        if not callable(request):
            return 0.0

        payload = {
            "symbol": symbol,
            "precio": precio,
            "exposicion_total": exposicion,
            "stop_loss": stop_loss,
        }

        try:
            respuesta = await request(
                "calcular_cantidad",
                payload,
                timeout=self._quantity_timeout,
            )
        except asyncio.TimeoutError:
            self.log.warning(
                "crear.quantity_fallback_bus_timeout",
                extra=safe_extra(
                    {
                        "symbol": symbol,
                        "precio": precio,
                        "timeout": self._quantity_timeout,
                    }
                ),
            )
            return 0.0

        except Exception:  # pragma: no cover - defensivo
            self.log.warning(
                "crear.quantity_fallback_bus_error",
                exc_info=True,
                extra=safe_extra(
                    {
                        "symbol": symbol,
                        "precio": precio,
                        "exposicion_total": exposicion,
                    }
                ),
            )
            return 0.0
        
        if not respuesta.get("ack", False):
            self.log.warning(
                "crear.quantity_fallback_bus_nack",
                extra=safe_extra(
                    {
                        "symbol": symbol,
                        "precio": precio,
                        "error": respuesta.get("error"),
                    }
                ),
            )
            return 0.0

        cantidad = extract_quantity(respuesta)
        return cantidad if cantidad > 0 else 0.0

    def update_bus(self, bus) -> None:
        """Permite actualizar dinámicamente el bus asociado."""

        self.bus = bus
