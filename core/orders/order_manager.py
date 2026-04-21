"""Gestión de órdenes simuladas o reales."""
from __future__ import annotations

import asyncio
import os
from collections.abc import Mapping
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from core.orders.order_model import Order
from core.orders.order_open_status import OrderOpenStatus
from core.utils.feature_flags import is_flag_enabled
from core.utils.logger import configurar_logger
from core.utils.log_utils import format_exception_for_log, safe_extra
from core.orders import real_orders
# Importamos validadores con nombres más descriptivos.  El módulo
# ``validators_binance`` centraliza las restricciones de Binance.
from core.orders.validators_binance import remainder_executable  # renamed from validators
from core.risk.level_validators import (
    validate_levels,
    LevelValidationError,
)
from core.utils.utils import is_valid_number
from core.event_bus import EventBus
from core.metrics import (
    limpiar_registro_pendiente,
    subscribe_order_pnl_metrics,
)
from core.registro_metrico import registro_metrico
from binance_api.ccxt_client import obtener_ccxt as obtener_cliente
from config import config as app_config
from core.orders.market_retry_executor import ExecutionResult, MarketRetryExecutor
from core.orders.quantity_resolver import QuantityResolver
from core.orders.order_manager_helpers import (
    backtest_slippage_bps as _backtest_slippage_bps,
    exchange_side_open_position as _exchange_side_open_position,
    exchange_side_reduce_position as _exchange_side_reduce_position,
    fetch_balance_non_blocking as _fetch_balance_non_blocking,
    fmt_exchange_err as _fmt_exchange_err,
    is_short_direction as _is_short_direction,
    sim_aplicar_slippage_entrada_salida as _sim_aplicar_slippage_entrada_salida,
)
from core.orders import (
    order_manager_abrir,
    order_manager_cerrar,
    order_manager_crear,
    order_manager_execution,
    order_manager_reconcile_tasks,
    order_manager_registro_retry,
    order_manager_sync,
)

log = configurar_logger('orders', modo_silencioso=True)
UTC = timezone.utc

MAX_HISTORIAL_ORDENES = 1000


class OrderManager:
    """Abstrae la creación y cierre de órdenes.

    En **modo real**, el bucle de sync (véase :mod:`core.orders.order_manager_sync`) llama a
    :func:`real_orders.reconciliar_ordenes`, que fusiona SQLite con órdenes
    abiertas del exchange. Si la API falla, la reconciliación no modifica el
    estado persistido (solo se registra error). Las métricas
    ``registrar_orders_sync_*`` reflejan éxito o fallo por ciclo.
    """

    def __init__(
        self,
        modo_real: bool,
        bus: EventBus | None = None,
        max_historial: int = MAX_HISTORIAL_ORDENES,
		config: Any | None = None,
    ) -> None:
        self.modo_real = modo_real
        self.ordenes: Dict[str, Order] = {}
        self.historial: Dict[str, list] = {}
        self.bus = bus
        self.max_historial = max_historial
        self.abriendo: set[str] = set()
        self._locks: Dict[str, asyncio.Lock] = {}
        # Símbolos para los que ya se registró un mensaje de duplicado
        self._dup_warned: set[str] = set()
        self._sync_task: asyncio.Task | None = None
		# Símbolos con registro pendiente persistente (pausa nuevas entradas)
        self._registro_pendiente_paused: set[str] = set()
        self._sync_interval = 300
        self._sync_base_interval = self._sync_interval
        self._sync_min_interval = float(
            os.getenv("ORDERS_SYNC_MIN_INTERVAL", "60")
        )
        self._sync_max_interval = float(
            os.getenv("ORDERS_SYNC_MAX_INTERVAL", "900")
        )
        self._sync_backoff_factor = float(
            os.getenv("ORDERS_SYNC_BACKOFF_FACTOR", "2.0")
        )
        self._sync_jitter = float(os.getenv("ORDERS_SYNC_JITTER", "0.1"))
        self._sync_failures = 0
        self._quantity_timeout = max(
            0.1, float(os.getenv("ORDERS_QUANTITY_TIMEOUT", "5.0"))
        )
        self._market_executor = MarketRetryExecutor(log=log, bus=bus)
        self._quantity_resolver = QuantityResolver(
            log=log,
            bus=bus,
            quantity_timeout=self._quantity_timeout,
        )
        self._registro_retry_enabled = is_flag_enabled("orders.retry_persistencia.enabled")
        self._registro_retry_base_delay = max(
            0.1, float(os.getenv("ORDERS_RETRY_PERSISTENCIA_BASE_DELAY", "5.0"))
        )
        self._registro_retry_backoff = max(
            1.0, float(os.getenv("ORDERS_RETRY_PERSISTENCIA_BACKOFF", "2.0"))
        )
        self._registro_retry_max_delay = max(
            self._registro_retry_base_delay,
            float(os.getenv("ORDERS_RETRY_PERSISTENCIA_MAX_DELAY", "60.0")),
        )
        self._registro_retry_jitter = max(
            0.0, float(os.getenv("ORDERS_RETRY_PERSISTENCIA_JITTER", "0.0"))
        )
        self._registro_retry_attempts: Dict[str, int] = {}
        self._registro_retry_tasks: Dict[str, asyncio.Task] = {}
        self._registro_retry_error_keywords = (
            order_manager_registro_retry.PERSISTENCE_ERROR_KEYWORDS
        )
        self._partial_close_retry_delay = max(
            0.0, float(os.getenv("ORDERS_PARTIAL_CLOSE_RETRY_DELAY", "1.0"))
        )
        self.capital_manager: Any | None = None
        self.risk_manager: Any | None = None
        self._config = config or getattr(app_config, "cfg", None)
        self._flush_task: asyncio.Task | None = None
        self._reconcile_task: asyncio.Task | None = None
        self._flush_enabled = is_flag_enabled(
            "orders.flush_periodico.enabled",
            bool(getattr(self._config, "orders_flush_periodico_enabled", False)),
        )
        self._limit_enabled = is_flag_enabled(
            "orders.limit.enabled",
            bool(getattr(self._config, "orders_limit_enabled", False)),
        )
        self._execution_policy_default = self._resolve_config_policy_default()
        self._execution_policy_overrides = self._resolve_config_policy_overrides()
        self._limit_timeout = float(
            os.getenv("LIMIT_ORDER_TIMEOUT", str(getattr(real_orders, "_LIMIT_TIMEOUT", 10.0)))
            or getattr(real_orders, "_LIMIT_TIMEOUT", 10.0)
        )
        self._limit_offset = float(
            os.getenv("OFFSET_REPRICE", str(getattr(real_orders, "_OFFSET_REPRICE", 0.001)))
            or getattr(real_orders, "_OFFSET_REPRICE", 0.001)
        )
        self._limit_max_retry = int(
            os.getenv("LIMIT_ORDER_MAX_RETRY", str(getattr(real_orders, "_LIMIT_MAX_RETRY", 3)))
            or getattr(real_orders, "_LIMIT_MAX_RETRY", 3)
        )
        self._reconcile_enabled = self._resolve_reconcile_enabled()
        self._reconcile_interval = max(
            60.0,
            float(os.getenv("ORDERS_RECONCILE_INTERVAL", "1800.0") or 1800.0),
        )
        self._reconcile_limit = int(os.getenv("ORDERS_RECONCILE_LIMIT", "50") or 50)
        if bus:
            self.subscribe(bus)
        else:
            self._ensure_background_tasks()

    def _generar_operation_id(self, symbol: str) -> str:
        """Genera un identificador único para agrupar fills de una operación."""
        nonce = os.urandom(2).hex()
        ts = int(datetime.now(UTC).timestamp() * 1000)
        return f"{symbol.replace('/', '')}-{ts}-{nonce}"

    # Compatibilidad con tests existentes que ajustan parámetros de reintentos.
    @property
    def _market_retry_backoff(self) -> float:  # pragma: no cover - compatibilidad
        return self._market_executor._backoff

    @_market_retry_backoff.setter
    def _market_retry_backoff(self, value: float) -> None:  # pragma: no cover
        self._market_executor._backoff = value

    @property
    def _market_retry_backoff_multiplier(self) -> float:  # pragma: no cover
        return self._market_executor._backoff_multiplier

    @_market_retry_backoff_multiplier.setter
    def _market_retry_backoff_multiplier(self, value: float) -> None:  # pragma: no cover
        self._market_executor._backoff_multiplier = value

    @property
    def _market_retry_backoff_cap(self) -> float:  # pragma: no cover
        return self._market_executor._backoff_cap

    @_market_retry_backoff_cap.setter
    def _market_retry_backoff_cap(self, value: float) -> None:  # pragma: no cover
        self._market_executor._backoff_cap = value

    @property
    def _market_retry_max_attempts(self) -> int:  # pragma: no cover
        return self._market_executor._max_attempts

    @_market_retry_max_attempts.setter
    def _market_retry_max_attempts(self, value: int) -> None:  # pragma: no cover
        self._market_executor._max_attempts = int(value)

    @property
    def _market_retry_no_progress_limit(self) -> int:  # pragma: no cover
        return self._market_executor._no_progress_limit

    @_market_retry_no_progress_limit.setter
    def _market_retry_no_progress_limit(self, value: int) -> None:  # pragma: no cover
        self._market_executor._no_progress_limit = int(value)

    def _market_retry_sleep(self, attempt: int) -> float:  # pragma: no cover
        return self._market_executor._market_retry_sleep(attempt)


    def subscribe(self, bus: EventBus) -> None:
        self.bus = bus
        self._market_executor.update_bus(bus)
        self._quantity_resolver.update_bus(bus)
        bus.subscribe('abrir_orden', self._on_abrir)
        bus.subscribe('cerrar_orden', self._on_cerrar)
        bus.subscribe('cerrar_parcial', self._on_cerrar_parcial)
        subscribe_order_pnl_metrics(bus)
        if self.modo_real:
            self.start_sync()
        self._ensure_background_tasks()

    async def _publish_registrar_perdida(self, symbol: str, retorno: float, orden: Order) -> None:
        """Publica pérdida al bus; incluye importe en moneda cuando ``pnl_realizado`` es fiable."""
        if not self.bus or retorno >= 0:
            return
        payload: dict[str, Any] = {"symbol": symbol, "perdida": retorno}
        pnl_tot = float(getattr(orden, "pnl_realizado", 0.0) or 0.0)
        loss_quote = abs(min(0.0, pnl_tot))
        if loss_quote > 0:
            payload["perdida_moneda"] = loss_quote
        await self.bus.publish("registrar_perdida", payload)

    def _apply_realized_pnl_delta(
        self,
        symbol: str,
        orden: Order,
        delta: float,
        *,
        emit: bool = True,
    ) -> None:
        nuevo_valor = getattr(orden, 'pnl_realizado', 0.0) + float(delta)
        orden.pnl_realizado = nuevo_valor
        if emit:
            self._emit_pnl_update(symbol, orden)

    def _set_latent_pnl(
        self,
        symbol: str,
        orden: Order,
        value: float,
        *,
        emit: bool = True,
        extra: Mapping[str, Any] | None = None,
    ) -> None:
        orden.pnl_latente = float(value)
        if emit:
            self._emit_pnl_update(symbol, orden, extra=extra)

    def _emit_pnl_update(
        self,
        symbol: str,
        orden: Order,
        *,
        extra: Mapping[str, Any] | None = None,
    ) -> None:
        if not self.bus:
            return
        payload: dict[str, Any] = {
            'symbol': symbol,
            'pnl_realizado': float(getattr(orden, 'pnl_realizado', 0.0)),
            'pnl_latente': float(getattr(orden, 'pnl_latente', 0.0)),
            'pnl_total': float(getattr(orden, 'pnl_realizado', 0.0))
            + float(getattr(orden, 'pnl_latente', 0.0)),
            'modo_real': bool(self.modo_real),
            'timestamp': datetime.now(UTC).isoformat(),
        }
        numeric_fields = (
            'precio_entrada',
            'precio_cierre',
            'stop_loss',
            'take_profit',
            'cantidad',
            'cantidad_abierta',
        )
        for field in numeric_fields:
            value = getattr(orden, field, None)
            if is_valid_number(value):
                payload[field] = float(value)
        direccion = getattr(orden, 'direccion', None)
        if isinstance(direccion, str):
            payload['direccion'] = direccion
        operation_id = getattr(orden, 'operation_id', None)
        if isinstance(operation_id, str):
            payload['operation_id'] = operation_id
        retorno_total = getattr(orden, 'retorno_total', None)
        if is_valid_number(retorno_total):
            payload['retorno_total'] = float(retorno_total)
        if extra:
            for key, value in extra.items():
                if value is None:
                    continue
                payload[key] = value
        try:
            self.bus.emit('orders.pnl_update', payload)
        except Exception:  # pragma: no cover - defensivo
            log.warning(
                'orders.pnl_update.emit_failed',
                extra=safe_extra({'symbol': symbol}),
                exc_info=True,
            )

    def _ensure_background_tasks(self) -> None:
        self._maybe_start_flush_task()
        self._maybe_start_reconcile_task()

    def _maybe_start_flush_task(self) -> None:
        if not (self.modo_real and self._flush_enabled):
            return
        if self._flush_task is not None and not self._flush_task.done():
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        self._flush_task = loop.create_task(
            real_orders.flush_periodico(),
            name="orders.flush_periodico",
        )
        log.info(
            "orders.flush_periodico.started",
            extra=safe_extra({"interval": getattr(real_orders, "_FLUSH_INTERVAL", None)}),
        )

    def _maybe_start_reconcile_task(self) -> None:
        if not (self.modo_real and self._reconcile_enabled):
            return
        if self._reconcile_task is not None and not self._reconcile_task.done():
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        self._reconcile_task = loop.create_task(
            order_manager_reconcile_tasks.reconcile_trades_loop(self),
            name="orders.reconcile_trades",
        )

    def _resolve_config_policy_default(self) -> str:
        policy = getattr(self._config, "orders_execution_policy", "market")
        policy_str = str(policy).strip().lower()
        return policy_str if policy_str in {"market", "limit"} else "market"

    def _resolve_config_policy_overrides(self) -> dict[str, str]:
        overrides_raw = getattr(self._config, "orders_execution_policy_by_symbol", {})
        result: dict[str, str] = {}
        if isinstance(overrides_raw, Mapping):
            for key, value in overrides_raw.items():
                if not isinstance(key, str):
                    continue
                sym = key.strip().upper()
                policy = str(value).strip().lower()
                if policy in {"market", "limit"}:
                    result[sym] = policy
        return result

    def _resolve_reconcile_enabled(self) -> bool:
        """Activa reconciliación periódica solo en modo real, vía config o ``ORDERS_RECONCILE_ENABLED``."""
        if not self.modo_real:
            return False
        base_enabled = bool(getattr(self._config, "orders_reconcile_enabled", False))
        return is_flag_enabled("orders.reconcile.enabled", base_enabled)

    def _resolve_execution_policy(self, symbol: str, side: str) -> str:
        if not self._limit_enabled:
            return "market"
        symbol_key = symbol.upper().replace("-", "/")
        policy = self._execution_policy_overrides.get(symbol_key)
        if not policy:
            policy = self._execution_policy_default
        if policy != "limit":
            return "market"
        if side not in {"buy", "sell"}:
            return "market"
        return "limit"

    async def _execute_real_order(
        self,
        side: str,
        symbol: str,
        cantidad: float,
        operation_id: str,
        entrada: dict[str, Any],
        *,
        precio: float | None = None,
    ) -> ExecutionResult:
        return await order_manager_execution.execute_real_order(
            self, side, symbol, cantidad, operation_id, entrada, precio=precio
        )

    def _capital_comprometido(self, orden: Order | None) -> float:
        if orden is None:
            return 0.0
        cantidad = getattr(orden, "cantidad_abierta", None)
        if not is_valid_number(cantidad):
            cantidad = getattr(orden, "cantidad", 0.0)
        precio = getattr(orden, "precio_entrada", 0.0)
        if not is_valid_number(precio):
            precio = 0.0
        try:
            comprometido = float(abs(float(precio) * float(cantidad)))
        except Exception:
            comprometido = 0.0
        return comprometido if comprometido > 0 else 0.0

    def _capital_asignado(self, manager: Any, symbol: str, comprometido: float) -> float:
        obtener_asignado = getattr(manager, "exposure_asignada", None)
        if callable(obtener_asignado):
            try:
                return float(obtener_asignado(symbol))
            except TypeError:
                return float(obtener_asignado(symbol=symbol))  # type: ignore[call-arg]
            except Exception:
                pass
        disponible_actual = 0.0
        exposure_fn = getattr(manager, "exposure_disponible", None)
        if callable(exposure_fn):
            try:
                disponible_actual = float(exposure_fn(symbol))
            except TypeError:
                disponible_actual = float(exposure_fn(symbol=symbol))  # type: ignore[call-arg]
            except Exception:
                disponible_actual = 0.0
        else:
            capital_map = getattr(manager, "capital_por_simbolo", {})
            if isinstance(capital_map, Mapping):
                disponible_actual = float(
                    capital_map.get(symbol, capital_map.get(symbol.upper(), 0.0))
                )
        return max(disponible_actual + max(comprometido, 0.0), 0.0)

    def _actualizar_capital_disponible(
        self, symbol: str, orden: Order | None = None
    ) -> None:
        manager = self.capital_manager
        if manager is None:
            return
        actualizar = getattr(manager, "actualizar_exposure", None)
        if not callable(actualizar):
            return
        vigente = orden or self.ordenes.get(symbol)
        comprometido = self._capital_comprometido(vigente)
        risk = getattr(self, "risk_manager", None)
        if risk is not None:
            sincronizar = getattr(risk, "sincronizar_exposure", None)
            if callable(sincronizar):
                try:
                    sincronizar(symbol, comprometido)
                    return
                except Exception:
                    log.warning(
                        "orders.capital_sync_risk_failed",
                        extra=safe_extra({"symbol": symbol}),
                        exc_info=True,
                    )
        asignado = self._capital_asignado(manager, symbol, comprometido)
        disponible = max(asignado - comprometido, 0.0)
        try:
            actualizar(symbol, disponible)
        except Exception:
            log.warning(
                "orders.capital_update_failed",
                extra=safe_extra({"symbol": symbol}),
                exc_info=True,
            )

    def _requeue_partial_close(
        self,
        symbol: str,
        cantidad: float,
        precio: float,
        motivo: str,
        *,
        operation_id: str,
    ) -> bool:
        return order_manager_cerrar.requeue_partial_close(
            self, symbol, cantidad, precio, motivo, operation_id=operation_id
        )

    def _requeue_full_close(
        self,
        symbol: str,
        precio: float,
        motivo: str,
        *,
        operation_id: str,
    ) -> bool:
        return order_manager_cerrar.requeue_full_close(
            self, symbol, precio, motivo, operation_id=operation_id
        )

    async def _on_abrir(self, data: dict) -> None:
        try:
            status = await self.abrir_async(**data)
        except Exception as exc:
            log.exception("Fallo en handler abrir_orden (event_bus)")
            EventBus.respond(
                data,
                ack=False,
                error=_fmt_exchange_err(exc),
                result=OrderOpenStatus.FAILED.value,
            )
            return
        EventBus.respond(
            data,
            ack=status is OrderOpenStatus.OPENED,
            result=status.value,
        )

    async def _on_cerrar(self, data: dict) -> None:
        try:
            result = await self.cerrar_async(**data)
        except Exception as exc:
            log.exception("Fallo en handler cerrar_orden (event_bus)")
            EventBus.respond(data, ack=False, error=_fmt_exchange_err(exc), result=False)
            return
        EventBus.respond(data, ack=bool(result), result=result)

    async def _on_cerrar_parcial(self, data: dict) -> None:
        try:
            result = await self.cerrar_parcial_async(**data)
        except Exception as exc:
            log.exception("Fallo en handler cerrar_parcial (event_bus)")
            EventBus.respond(data, ack=False, error=_fmt_exchange_err(exc), result=False)
            return
        EventBus.respond(data, ack=bool(result), result=result)

    async def _resolve_quantity_with_fallback(
        self,
        symbol: str,
        precio: float,
        sl: float,
        meta: Mapping[str, Any],
    ) -> tuple[float, str]:
        return await self._quantity_resolver.resolve(
            symbol,
            precio,
            sl,
            meta,
            capital_manager=self.capital_manager,
        )

    def start_sync(self, intervalo: int | None = None) -> None:
        if intervalo:
            self._sync_interval = intervalo
            self._sync_base_interval = intervalo
        if self._sync_task is not None:
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        if loop.is_running():
            # Solo arrancamos el loop; dentro se hace un _sync_once inicial.
            self._sync_task = loop.create_task(order_manager_sync.run_sync_loop(self))

    async def _sync_once(self) -> bool:
        return await order_manager_sync.run_sync_once(self)

    async def _sync_loop(self) -> None:
        await order_manager_sync.run_sync_loop(self)

    async def aclose_background_tasks(self) -> None:
        """Cancela tareas periódicas (flush, reconciliación, sync) y reintentos de registro."""
        tasks: list[asyncio.Task[Any]] = []
        for attr in ("_flush_task", "_reconcile_task", "_sync_task"):
            t = getattr(self, attr, None)
            if t is not None and not t.done():
                t.cancel()
                tasks.append(t)
            setattr(self, attr, None)
        for sym, t in list(getattr(self, "_registro_retry_tasks", {}).items()):
            if t is not None and not t.done():
                t.cancel()
                tasks.append(t)
        self._registro_retry_tasks.clear()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    def _compute_next_sync_delay(self, success: bool) -> float:
        """Calcula el próximo retardo del loop de sincronización."""
        return order_manager_sync.compute_next_sync_delay(self, success)

    def _should_schedule_persistence_retry(self, exc: Exception | None) -> bool:
        return order_manager_registro_retry.should_schedule_persistence_retry(self, exc)

    def _schedule_registro_retry(self, symbol: str, *, reason: str | None = None) -> None:
        """Programa un reintento con backoff controlado tras fallas de persistencia."""
        order_manager_registro_retry.schedule_registro_retry(self, symbol, reason=reason)

    async def abrir_async(
        self,
        symbol: str,
        precio: float,
        sl: float,
        tp: float,
        estrategias: Dict,
        tendencia: str,
        direccion: str = 'long',
        cantidad: float = 0.0,
        puntaje: float = 0.0,
        umbral: float = 0.0,
        score_tecnico: float = 0.0,
        objetivo: float | None = None,
        fracciones: int = 1,
        detalles_tecnicos: dict | None = None,
        tick_size: float | None = None,
        step_size: float | None = None,
        min_dist_pct: float | None = None,
        *,
        candle_close_ts: int | None = None,   # reservado (no usado aquí)
        strategy_version: str | None = None,  # reservado (no usado aquí)
    ) -> OrderOpenStatus:
        return await order_manager_abrir.abrir_async(
            self,
            symbol,
            precio,
            sl,
            tp,
            estrategias,
            tendencia,
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

    async def agregar_parcial_async(self, symbol: str, precio: float, cantidad: float) -> bool:
        return await order_manager_cerrar.agregar_parcial_async(self, symbol, precio, cantidad)

    async def cerrar_async(self, symbol: str, precio: float | None, motivo: str) -> bool:
        return await order_manager_cerrar.cerrar_async(self, symbol, precio, motivo)

    async def cerrar_parcial_async(self, symbol: str, cantidad: float, precio: float, motivo: str) -> bool:
        return await order_manager_cerrar.cerrar_parcial_async(self, symbol, cantidad, precio, motivo)

    def obtener(self, symbol: str) -> Optional[Order]:
        return self.ordenes.get(symbol)

    async def crear(
        self,
        *,
        symbol: str,
        side: str,
        precio: float,
        sl: float,
        tp: float,
        meta: Mapping[str, Any] | None = None,
    ) -> OrderOpenStatus:
        return await order_manager_crear.crear(
            self,
            symbol=symbol,
            side=side,
            precio=precio,
            sl=sl,
            tp=tp,
            meta=meta,
        )

    def eliminar(self, symbol: str) -> None:
        """Elimina la orden local asociada a ``symbol`` si existe."""

        self.ordenes.pop(symbol, None)
        limpiar_registro_pendiente(symbol)
        self._registro_pendiente_paused.discard(symbol)

    def actualizar(self, orden: Order | None, **kwargs: Any) -> None:
        """Actualiza los atributos de ``orden`` con los ``kwargs`` provistos."""

        if orden is None:
            return
        for key, value in kwargs.items():
            try:
                setattr(orden, key, value)
            except Exception:  # pragma: no cover - defensivo
                continue

    def actualizar_mark_to_market(self, symbol: str, precio_actual: float) -> None:
        """Recalcula el PnL latente de ``symbol`` con el precio más reciente."""

        orden = self.ordenes.get(symbol)
        if orden is None:
            return

        if not is_valid_number(precio_actual):
            return

        precio = float(precio_actual)
        if precio <= 0.0:
            self._set_latent_pnl(symbol, orden, 0.0, extra={'precio_mark': precio})
            return

        cantidad = getattr(orden, "cantidad_abierta", None)
        if not is_valid_number(cantidad):
            cantidad = getattr(orden, "cantidad", 0.0)
        cantidad_float = float(cantidad or 0.0)
        if cantidad_float <= 0.0:
            self._set_latent_pnl(symbol, orden, 0.0, extra={'precio_mark': precio})
            return

        direccion = str(getattr(orden, "direccion", "long")).lower()
        signo = -1.0 if direccion in {"short", "venta"} else 1.0
        self._set_latent_pnl(
            symbol,
            orden,
            signo * precio * cantidad_float,
            extra={'precio_mark': precio},
        )
