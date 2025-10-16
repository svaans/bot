"""
Gestor de Órdenes — apertura, cierres (totales/parciales) y registro.

Principios
----------
- **Inyección de dependencias**: no importes directamente tu EventBus, PositionManager, reporter, etc.
  Pásalos en el constructor para evitar acoplamiento.
- **Asíncrono y con timeouts**: toda operación que dependa de I/O externo usa `await` con timeout.
- **Reporting desacoplado**: permite cablear reporter/auditor en segundo plano si existen.

API
---
class GestorOrdenes:
    - abrir_operacion(symbol, precio, cantidad, direccion, *, detalles=None) -> bool
    - cerrar_operacion(symbol, precio, motivo: str, *, df=None) -> bool
    - cerrar_parcial(symbol, cantidad, precio, motivo: str, *, df=None) -> bool
    - reconciliar_ordenes(simbolos: list[str]) -> dict

Requisitos de los colaboradores (interfaces mínimas)
---------------------------------------------------
- bus.publish(evento: str, payload: dict) -> Awaitable[None]
- orders.obtener(symbol) -> orden | None
- orders.crear(symbol, precio, cantidad, direccion, extras) -> orden
- orders.actualizar(orden, **kwargs)
- orders.eliminar(symbol)
- reporter.registrar_operacion(dict)  # llamado en thread pool
- auditor.registrar(...)
- fetch_ticker_async(cliente, symbol) -> dict con key 'last'

Notas
-----
- Si no pasas `reporter`/`auditor`, simplemente no se registra nada (no rompe).
- Los cálculos de capital, Kelly y riesgo se dejan a otros módulos.
"""
from __future__ import annotations
import asyncio
import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Optional

UTC = timezone.utc

# Tipos simples para anotar colaboradores (duck typing)
class _Bus:
    async def publish(self, evento: str, payload: dict) -> None: ...

class _Orders:
    def obtener(self, symbol: str): ...
    def crear(self, symbol: str, precio: float, cantidad: float, direccion: str, extras: dict | None = None): ...
    def actualizar(self, orden, **kwargs): ...
    def eliminar(self, symbol: str): ...


@dataclass
class GestorOrdenes:
    bus: _Bus
    orders: _Orders
    reporter: Any | None = None,
    auditor: Any | None = None,
    cliente: Any | None = None,  # cliente de exchange (solo real)
    timeout_bus: float = 5.0,
    on_event: Callable[[str, dict], None] | None = None,

    def _emit(self, evt: str, data: dict) -> None:
        if self.on_event:
            try:
                self.on_event(evt, data)
            except Exception:
                pass

    # ---------------- utils ----------------
    async def _safe_publish(self, evento: str, payload: dict, *, timeout: float | None = None) -> Any:
        timeout_target = timeout or self.timeout_bus
        data = dict(payload)
        data.setdefault("ack", None)
        data.setdefault("error", None)
        request = getattr(self.bus, "request", None)
        if callable(request):
            try:
                respuesta = await request(evento, data, timeout=timeout_target)
            except asyncio.TimeoutError:
                self._emit("timeout", {"evento": evento})
                return None
            ack = bool(respuesta.get("ack"))
            if not ack:
                self._emit("nack", {"evento": evento, "error": respuesta.get("error")})
            return ack
        
        fut = asyncio.get_running_loop().create_future()
        data["future"] = fut
        await self.bus.publish(evento, data)
        try:
            result = await asyncio.wait_for(fut, timeout=timeout_target)
        except asyncio.TimeoutError:
            self._emit("timeout", {"evento": evento})
            return None
        finally:
            data.pop("future", None)
        if isinstance(result, dict):
            ack = bool(result.get("ack"))
            if not ack:
                self._emit("nack", {"evento": evento, "error": result.get("error")})
            return ack
        ack = bool(result)
        if not ack:
            self._emit("nack", {"evento": evento})
        return ack

    async def _ticker_precio(self, symbol: str, fallback: float | None = None) -> float | None:
        try:
            # Import tardío para que el módulo sea auto-contenido si no se usa
            from binance_api.cliente import fetch_ticker_async  # pragma: no cover
        except Exception:
            return fallback
        if self.cliente is None:
            return fallback
        try:
            data = await fetch_ticker_async(self.cliente, symbol)
            return float(data.get("last", fallback)) if isinstance(data, dict) else fallback
        except Exception:
            return fallback

    async def _reportar(self, info: dict) -> None:
        if not self.reporter:
            return
        loop = asyncio.get_running_loop()
        try:
            await loop.run_in_executor(None, self.reporter.registrar_operacion, info)
        except Exception:
            self._emit("report_error", {"symbol": info.get("symbol")})

    async def _auditar(self, **kwargs) -> None:
        if not self.auditor:
            return
        loop = asyncio.get_running_loop()
        try:
            await loop.run_in_executor(None, self.auditor.registrar, **kwargs)
        except Exception:
            self._emit("audit_error", kwargs)

    # --------------- operaciones ---------------
    async def abrir_operacion(
        self,
        symbol: str,
        precio: float,
        cantidad: float,
        direccion: str,
        *,
        extras: dict | None = None,
    ) -> bool:
        """Crea orden local y notifica al bus para ejecución en exchange si aplica."""
        if cantidad <= 0 or precio <= 0:
            return False
        orden = self.orders.crear(symbol, precio, cantidad, direccion, extras or {})
        ok = await self._safe_publish("abrir_orden", {"symbol": symbol, "precio": precio, "cantidad": cantidad, "direccion": direccion})
        if ok is False:
            # revertir creación si no se confirma
            try:
                self.orders.eliminar(symbol)
            except Exception:
                pass
            return False
        self._emit("orden_abierta", {"symbol": symbol, "precio": precio, "cantidad": cantidad, "direccion": direccion})
        return True

    async def cerrar_operacion(self, symbol: str, precio: float | None, motivo: str, *, df=None) -> bool:
        """Cierre total con registro (reporter/auditor si están)."""
        orden = self.orders.obtener(symbol)
        if not orden or getattr(orden, "cerrando", False):
            return False
        orden.cerrando = True
        # Precio
        if precio is None:
            precio = await self._ticker_precio(symbol, fallback=getattr(orden, "precio_entrada", None))
        if not precio:
            return False
        ok = await self._safe_publish("cerrar_orden", {"symbol": symbol, "precio": precio, "motivo": motivo})
        if not ok:
            return False
        # Reporte
        info = getattr(orden, "to_dict", lambda: {})()
        info.update({
            "symbol": symbol,
            "precio_cierre": float(precio),
            "fecha_cierre": datetime.now(UTC).isoformat(),
            "motivo_cierre": motivo,
        })
        await self._reportar(info)
        await self._auditar(symbol=symbol, evento=motivo, resultado="cierre", **info)
        self._emit("orden_cerrada", {"symbol": symbol, "motivo": motivo})
        return True

    async def cerrar_parcial(self, symbol: str, cantidad: float, precio: float | None, motivo: str, *, df=None) -> bool:
        orden = self.orders.obtener(symbol)
        if not orden or cantidad <= 0:
            return False
        if precio is None:
            precio = await self._ticker_precio(symbol, fallback=getattr(orden, "precio_entrada", None))
        if not precio:
            return False
        ok = await self._safe_publish("cerrar_parcial", {"symbol": symbol, "cantidad": cantidad, "precio": precio, "motivo": motivo})
        if not ok:
            return False
        # reporting simple
        info = getattr(orden, "to_dict", lambda: {})()
        info.update({
            "symbol": symbol,
            "precio_cierre": float(precio),
            "cantidad_cerrada": float(cantidad),
            "fecha_cierre": datetime.now(UTC).isoformat(),
            "motivo_cierre": motivo,
        })
        await self._reportar(info)
        await self._auditar(symbol=symbol, evento=motivo, resultado="cierre_parcial", **info)
        self._emit("orden_cierre_parcial", {"symbol": symbol, "cantidad": cantidad})
        return True

    async def reconciliar_ordenes(self, simbolos: list[str]) -> dict:
        """Crea o actualiza órdenes en memoria según estado externo (si lo tienes).
        Por defecto solo retorna un dict vacío para no acoplar aquí a la BD.
        """
        result = {"ok": True, "symbols": simbolos}
        self._emit("reconciliar", {"symbols": simbolos})
        return result
