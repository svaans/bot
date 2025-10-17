"""Gestión de órdenes simuladas o reales."""
from __future__ import annotations

import asyncio
import os
import random
import sqlite3
from collections.abc import Mapping
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, Optional

from core.orders.order_model import Order
from core.utils.feature_flags import is_flag_enabled
from core.utils.logger import configurar_logger, log_decision
from core.utils.log_utils import safe_extra
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
    registrar_buy_rejected_insufficient_funds,
    registrar_crear_skip_quantity,
    registrar_orden,
    registrar_orders_sync_failure,
    registrar_orders_sync_success,
    registrar_orders_retry_scheduled,
    registrar_partial_close_collision,
    registrar_registro_error,
    registrar_registro_pendiente,
    limpiar_registro_pendiente,
)
from core.registro_metrico import registro_metrico
from binance_api.cliente import obtener_cliente
from config import config as app_config
from core.orders.market_retry_executor import ExecutionResult, MarketRetryExecutor
from core.orders.order_helpers import coerce_float, coerce_int, lookup_meta
from core.orders.quantity_resolver import QuantityResolver

log = configurar_logger('orders', modo_silencioso=True)
UTC = timezone.utc

MAX_HISTORIAL_ORDENES = 1000


_PERSISTENCE_ERRORS: tuple[type[BaseException], ...] = (OSError,)
if sqlite3 is not None:  # pragma: no cover - sqlite siempre disponible pero defensivo
    _PERSISTENCE_ERRORS = _PERSISTENCE_ERRORS + (sqlite3.Error,)

_PERSISTENCE_ERROR_KEYWORDS = (
    "database",
    "sqlite",
    "disk",
    "io",
    "locked",
    "write",
)


class OrderOpenStatus(Enum):
    """Estado resultante al intentar abrir una orden."""

    OPENED = "opened"
    PENDING_REGISTRATION = "pending_registration"
    FAILED = "failed"


class OrderManager:
    """Abstrae la creación y cierre de órdenes."""

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
        self._registro_retry_error_keywords = _PERSISTENCE_ERROR_KEYWORDS
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
        self._bot_env = os.getenv("BOT_ENV", "").lower()
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
        if self.modo_real:
            self.start_sync()
        self._ensure_background_tasks()

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
            self._reconcile_loop(),
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
        base_enabled = bool(getattr(self._config, "orders_reconcile_enabled", False))
        flag_enabled = is_flag_enabled("orders.reconcile.enabled", base_enabled)
        if self._bot_env == "staging":
            return flag_enabled
        return False

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

    async def _reconcile_loop(self) -> None:
        while True:
            try:
                divergencias = await asyncio.to_thread(
                    real_orders.reconciliar_trades_binance,
                    None,
                    self._reconcile_limit,
                    False,
                    self._log_reconcile_report,
                )
                if divergencias:
                    log.warning(
                        "orders.reconcile_trades.detected",
                        extra=safe_extra({
                            "count": len(divergencias),
                        }),
                    )
            except Exception as exc:  # pragma: no cover - defensivo
                log.error(
                    "orders.reconcile_trades.error",
                    extra=safe_extra({"error": str(exc)}),
                )
            await asyncio.sleep(self._reconcile_interval)

    def _log_reconcile_report(self, divergencias: list[dict[str, Any]]) -> None:
        if not divergencias:
            log.debug("orders.reconcile_trades.clean")
            return
        for divergence in divergencias:
            log.warning(
                "orders.reconcile_trades.divergence",
                extra=safe_extra(divergence),
            )

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
        policy = self._resolve_execution_policy(symbol, side)
        if policy == "limit" and precio is not None and self._limit_enabled:
            try:
                resultado = await asyncio.to_thread(
                    real_orders.ejecutar_orden_limit,
                    symbol,
                    side,
                    float(precio),
                    float(cantidad),
                    operation_id,
                    self._limit_timeout,
                    self._limit_offset,
                    self._limit_max_retry,
                )
            except Exception as exc:  # pragma: no cover - defensivo
                log.error(
                    "orders.limit_execution.error",
                    extra=safe_extra(
                        {
                            "symbol": symbol,
                            "side": side,
                            "reason": str(exc),
                        }
                    ),
                )
                return ExecutionResult(0.0, 0.0, 0.0, "ERROR", remaining=float(cantidad))
            executed = float(resultado.get("ejecutado", 0.0))
            fee = float(resultado.get("fee", 0.0))
            pnl = float(resultado.get("pnl", 0.0))
            remaining = float(resultado.get("restante", 0.0))
            status = str(resultado.get("status") or "FILLED").upper()
            if remaining > 0:
                log.warning(
                    "orders.limit_execution.partial",
                    extra=safe_extra(
                        {
                            "symbol": symbol,
                            "side": side,
                            "remaining": remaining,
                            "operation_id": operation_id,
                        }
                    ),
                )
            return ExecutionResult(executed, fee, pnl, status, remaining=remaining)

        return await self._market_executor.ejecutar(side, symbol, cantidad, operation_id, entrada)

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
        if not self.bus:
            return False
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return False

        payload = {
            "symbol": symbol,
            "cantidad": cantidad,
            "precio": precio,
            "motivo": motivo,
        }

        async def _retry() -> None:
            try:
                if self._partial_close_retry_delay > 0:
                    await asyncio.sleep(self._partial_close_retry_delay)
                await self.bus.publish("cerrar_parcial", dict(payload))
            except Exception:
                log.warning(
                    "orders.partial_close.retry_failed",
                    extra=safe_extra(
                        {
                            "symbol": symbol,
                            "operation_id": operation_id,
                        }
                    ),
                    exc_info=True,
                )

        loop.create_task(
            _retry(),
            name=f"orders.retry_partial_close.{symbol.replace('/', '')}",
        )
        log.info(
            "orders.partial_close.reenqueued",
            extra=safe_extra(
                {
                    "symbol": symbol,
                    "cantidad": cantidad,
                    "precio": precio,
                    "operation_id": operation_id,
                    "delay": self._partial_close_retry_delay,
                }
            ),
        )
        return True

    async def _on_abrir(self, data: dict) -> None:
        status = await self.abrir_async(**data)
        EventBus.respond(
            data,
            ack=status is OrderOpenStatus.OPENED,
            result=status.value,
        )

    async def _on_cerrar(self, data: dict) -> None:
        result = await self.cerrar_async(**data)
        EventBus.respond(data, ack=bool(result), result=result)

    async def _on_cerrar_parcial(self, data: dict) -> None:
        result = await self.cerrar_parcial_async(**data)
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
            self._sync_task = loop.create_task(self._sync_loop())

    async def _sync_once(self) -> bool:
        actuales = set(self.ordenes.keys())
        try:
            ordenes_reconciliadas: Dict[str, Order] = await asyncio.to_thread(
                real_orders.reconciliar_ordenes
            )
        except Exception as e:
            log.error(f'❌ Error sincronizando órdenes: {e}')
            return

        reconciliadas = set(ordenes_reconciliadas.keys())
        local_only = actuales - reconciliadas
        exchange_only = reconciliadas - actuales

        if local_only or exchange_only:
            for sym in local_only:
                log.warning(f'⚠️ Orden local {sym} ausente en exchange; cerrada.')
                if self.bus:
                    await self.bus.publish(
                        'notify',
                        {
                            'mensaje': f'⚠️ Orden local {sym} cerrada por reconciliación',
                            'tipo': 'WARNING',
                        },
                    )
            for sym in exchange_only:
                log.warning(f'⚠️ Orden {sym} encontrada en exchange y añadida.')
                if self.bus:
                    await self.bus.publish(
                        'notify',
                        {
                            'mensaje': f'🔄 Orden sincronizada desde Binance: {sym}',
                            'tipo': 'WARNING',
                        },
                    )
            registro_metrico.registrar(
                'discrepancia_ordenes',
                {'local': len(local_only), 'exchange': len(exchange_only)},
                    )

        merged: Dict[str, Order] = {}

        # Actualiza/mezcla las que existen en remoto
        for sym, remoto in ordenes_reconciliadas.items():
            local = self.ordenes.get(sym)
            if local:
                # Preserva flags locales si aplican
                setattr(
                    remoto,
                    'registro_pendiente',
                    getattr(local, 'registro_pendiente', False)
                    or getattr(remoto, 'registro_pendiente', False),
                )
                # Si tienes otros campos efímeros, mapéalos aquí
            merged[sym] = remoto

        self.ordenes = merged

        # Intenta registrar las que quedaron pendientes
        errores_registro = False
        for sym, ord_ in list(self.ordenes.items()):
            if getattr(ord_, 'registro_pendiente', False):
                self._registro_pendiente_paused.add(sym)
                registrar_registro_pendiente(sym)
                try:
                    await asyncio.to_thread(
                        real_orders.registrar_orden,
                        sym,
                        ord_.precio_entrada,
                        ord_.cantidad_abierta or ord_.cantidad,
                        ord_.stop_loss,
                        ord_.take_profit,
                        ord_.estrategias_activas,
                        ord_.tendencia,
                        ord_.direccion,
                        ord_.operation_id,
                    )
                    ord_.registro_pendiente = False
                    limpiar_registro_pendiente(sym)
                    self._registro_pendiente_paused.discard(sym)
                    registrar_orden('opened')
                    log.info(f'🟢 Orden registrada tras reintento para {sym}')
                    if self.bus:
                        estrategias_txt = ', '.join(ord_.estrategias_activas.keys())
                        mensaje = (
                            f"""🟢 Compra {sym}\nPrecio: {ord_.precio_entrada:.2f} Cantidad: {ord_.cantidad_abierta or ord_.cantidad}\nSL: {ord_.stop_loss:.2f} TP: {ord_.take_profit:.2f}\nEstrategias: {estrategias_txt}"""
                        )
                        await self.bus.publish(
                            'notify',
                            {
                                'mensaje': mensaje,
                                'operation_id': ord_.operation_id,
                            },
                        )
                except Exception as e:
                    log.error(f'❌ Error registrando orden pendiente {sym}: {e}')
                    if self.bus:
                        await self.bus.publish(
                            'notify',
                            {
                                'mensaje': f'❌ Error registrando orden pendiente {sym}: {e}',
                                'tipo': 'CRITICAL',
                                'operation_id': ord_.operation_id,
                            },
                        )
                    registrar_orders_sync_failure(type(e).__name__)
                    errores_registro = True

        if errores_registro:
            return False

        registrar_orders_sync_success()
        return True


    async def _sync_loop(self) -> None:
        while True:
            success = await self._sync_once()
            delay = self._compute_next_sync_delay(success)
            await asyncio.sleep(delay)

    def _compute_next_sync_delay(self, success: bool) -> float:
        """Calcula el próximo retardo del loop de sincronización."""
        if success:
            self._sync_failures = 0
            self._sync_interval = self._sync_base_interval
        else:
            self._sync_failures += 1
            next_interval = self._sync_base_interval * (
                self._sync_backoff_factor ** self._sync_failures
            )
            self._sync_interval = min(self._sync_max_interval, next_interval)
        delay = max(self._sync_min_interval, self._sync_interval)
        if self._sync_jitter > 0:
            jitter = random.uniform(-self._sync_jitter, self._sync_jitter)
            delay *= 1 + jitter
        return max(self._sync_min_interval, min(delay, self._sync_max_interval))

    def _should_schedule_persistence_retry(self, exc: Exception | None) -> bool:
        if not self._registro_retry_enabled or exc is None:
            return False
        if isinstance(exc, _PERSISTENCE_ERRORS):
            return True
        message = str(exc).lower()
        return any(keyword in message for keyword in self._registro_retry_error_keywords)

    def _schedule_registro_retry(self, symbol: str, *, reason: str | None = None) -> None:
        """Programa un reintento con backoff controlado tras fallas de persistencia."""

        if not self._registro_retry_enabled:
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        orden = self.ordenes.get(symbol)
        if not orden or not getattr(orden, "registro_pendiente", False):
            self._registro_retry_attempts.pop(symbol, None)
            return

        existing = self._registro_retry_tasks.get(symbol)
        if existing and not existing.done():
            return

        attempt = self._registro_retry_attempts.get(symbol, 0) + 1
        self._registro_retry_attempts[symbol] = attempt

        delay = self._registro_retry_base_delay * (self._registro_retry_backoff ** (attempt - 1))
        delay = min(delay, self._registro_retry_max_delay)
        if self._registro_retry_jitter:
            jitter = random.uniform(-self._registro_retry_jitter, self._registro_retry_jitter)
            delay = max(0.0, delay * (1.0 + jitter))

        reason_label = (reason or "unknown").strip() or "unknown"
        registrar_orders_retry_scheduled(symbol, reason_label)
        log.info(
            "orders.retry_schedule",
            extra=safe_extra(
                {
                    "symbol": symbol,
                    "attempt": attempt,
                    "delay": round(delay, 3),
                    "retry_reason": reason_label,
                }
            ),
        )

        async def _retry() -> None:
            next_reason: str | None = None
            try:
                await asyncio.sleep(delay)
                current = self.ordenes.get(symbol)
                if not current or not getattr(current, "registro_pendiente", False):
                    self._registro_retry_attempts.pop(symbol, None)
                    return
                try:
                    await asyncio.to_thread(
                        real_orders.registrar_orden,
                        symbol,
                        current.precio_entrada,
                        current.cantidad_abierta or current.cantidad,
                        current.stop_loss,
                        current.take_profit,
                        current.estrategias_activas,
                        current.tendencia,
                        current.direccion,
                        current.operation_id,
                    )
                except Exception as exc:  # pragma: no cover - se prueba vía tests específicos
                    registrar_registro_error()
                    log.error(
                        "orders.retry_failed",
                        extra=safe_extra(
                            {
                                "symbol": symbol,
                                "attempt": attempt,
                                "reason": type(exc).__name__,
                            }
                        ),
                    )
                    if self._should_schedule_persistence_retry(exc):
                        next_reason = type(exc).__name__ or "Exception"
                    else:
                        self._registro_retry_attempts.pop(symbol, None)
                    return

                current.registro_pendiente = False
                limpiar_registro_pendiente(symbol)
                self._registro_pendiente_paused.discard(symbol)
                self._registro_retry_attempts.pop(symbol, None)
                registrar_orden("opened")
                log.info(
                    "orders.retry_success",
                    extra=safe_extra({"symbol": symbol, "attempt": attempt}),
                )
            except asyncio.CancelledError:  # pragma: no cover - cancelado en shutdown
                raise
            finally:
                self._registro_retry_tasks.pop(symbol, None)
                if next_reason:
                    self._schedule_registro_retry(symbol, reason=next_reason)

        task = loop.create_task(
            _retry(),
            name=f"registro_retry_{symbol.replace('/', '')}",
        )
        self._registro_retry_tasks[symbol] = task

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
        operation_id = self._generar_operation_id(symbol)
        tick_size_value = float(tick_size or 0.0)
        step_size_value = float(step_size or 0.0)
        try:
            min_dist_pct_value = float(min_dist_pct) if min_dist_pct is not None else 0.0
        except (TypeError, ValueError):
            min_dist_pct_value = 0.0

        try:
            precio, sl, tp = validate_levels(
                direccion,
                precio,
                sl,
                tp,
                min_dist_pct_value,
                tick_size_value,
                step_size_value,
            )
        except LevelValidationError as exc:
            contexto = dict(exc.context)
            contexto.update({'symbol': symbol, 'reason': exc.reason})
            entrada_log = {
                'symbol': symbol,
                'precio': contexto.get('entry', precio),
                'sl': contexto.get('sl', sl),
                'tp': contexto.get('tp', tp),
                'cantidad': cantidad,
            }
            log.warning(
                f'⚠️ {symbol}: validación SL/TP fallida ({exc.reason})',
                extra=contexto,
            )
            log_decision(
                log,
                'abrir',
                operation_id,
                entrada_log,
                {'niveles_validos': False},
                'reject',
                {'reason': 'invalid_levels', 'contexto': contexto},
            )
            registrar_orden('rejected')
            return OrderOpenStatus.FAILED
        
        entrada_log = {
            'symbol': symbol,
            'precio': precio,
            'sl': sl,
            'tp': tp,
            'cantidad': cantidad,
        }
        lock = self._locks.setdefault(symbol, asyncio.Lock())
        async with lock:
            # Evitar duplicados si ya se está abriendo o existe localmente
            if symbol in self.abriendo or symbol in self.ordenes:
                if symbol not in self._dup_warned:
                    log.warning(f'⚠️ Orden duplicada evitada para {symbol}')
                    self._dup_warned.add(symbol)
                log_decision(
                    log,
                    'abrir',
                    operation_id,
                    entrada_log,
                    {'duplicada': True},
                    'reject',
                    {'reason': 'duplicate'},
                )
                return OrderOpenStatus.FAILED
            if symbol in self._registro_pendiente_paused:
                log.warning(
                    '🚫 Apertura bloqueada para %s por registro pendiente persistente',
                    symbol,
                )
                if self.bus:
                    await self.bus.publish(
                        'notify',
                        {
                            'mensaje': f'🚫 Apertura bloqueada en {symbol} por registro pendiente',
                            'tipo': 'WARNING',
                            'operation_id': operation_id,
                        },
                    )
                registrar_orden('rejected')
                log_decision(
                    log,
                    'abrir',
                    operation_id,
                    entrada_log,
                    {'registro': 'bloqueado'},
                    'reject',
                    {'reason': 'registro_pendiente_bloqueado'},
                )
                return OrderOpenStatus.FAILED
            ordenes_api = {}
            if self.modo_real:
                reintentos_sync = 3
                ultimo_error_sync: Exception | None = None
                for intento in range(1, reintentos_sync + 1):
                    try:
                        ordenes_api = await asyncio.to_thread(
                            real_orders.sincronizar_ordenes_binance,
                            [symbol],
                            modo_real=self.modo_real,
                        )
                        ultimo_error_sync = None
                        break
                    except Exception as e:
                        ultimo_error_sync = e
                        log.warning(
                            '⚠️ Error verificando órdenes abiertas (intento %s/%s) para %s: %s',
                            intento,
                            reintentos_sync,
                            symbol,
                            e,
                        )
                        if intento < reintentos_sync:
                            await asyncio.sleep(0.25 * intento)
                if ultimo_error_sync is not None:
                    log.error(f'❌ Error verificando órdenes abiertas tras reintentos: {ultimo_error_sync}')
                    if self.bus:
                        await self.bus.publish(
                            'notify',
                            {
                                'mensaje': f'⚠️ No se pudo verificar órdenes abiertas en {symbol}',
                                'tipo': 'WARNING',
                                'operation_id': operation_id,
                            },
                        )
                    log_decision(
                        log,
                        'abrir',
                        operation_id,
                        entrada_log,
                        {'verificacion': 'error'},
                        'reject',
                        {'reason': 'sync_error', 'intentos': reintentos_sync},
                    )
                    return OrderOpenStatus.FAILED
                
            if symbol in ordenes_api:
                self.ordenes[symbol] = ordenes_api[symbol]
                if symbol not in self._dup_warned:
                    log.warning(f'⚠️ Orden duplicada evitada para {symbol}')
                    self._dup_warned.add(symbol)
                if self.bus:
                    await self.bus.publish(
                        'notify',
                        {'mensaje': f'⚠️ Orden ya abierta en Binance para {symbol}', 'operation_id': operation_id},
                    )
                log_decision(
                    log,
                    'abrir',
                    operation_id,
                    entrada_log,
                    {'duplicada': True},
                    'reject',
                    {'reason': 'already_open'},
                )
                return OrderOpenStatus.FAILED
            
            if self.modo_real:
                try:
                    cliente = obtener_cliente()
                    balance = cliente.fetch_balance()
                    quote = symbol.split('/')[1]
                    disponible = balance.get('free', {}).get(quote, 0.0)
                except Exception as e:
                    log.error(f'❌ Error obteniendo balance: {e}')
                    disponible = 0.0
                notional = precio * cantidad
                if not remainder_executable(symbol, precio, cantidad) or notional > disponible:
                    if self.bus:
                        await self.bus.publish(
                            'notify', {'mensaje': 'insuficiente', 'tipo': 'WARNING', 'operation_id': operation_id}
                        )
                    registrar_buy_rejected_insufficient_funds()
                    registrar_orden('rejected')
                    log_decision(
                        log,
                        'abrir',
                        operation_id,
                        entrada_log,
                        {
                            'saldo_disponible': disponible,
                            'notional': notional,
                        },
                        'reject',
                        {'reason': 'insufficient_funds'},
                    )
                    return OrderOpenStatus.FAILED

            self.abriendo.add(symbol)
            try:
                objetivo = objetivo if objetivo is not None else cantidad
                orden = Order(
                    symbol=symbol,
                    precio_entrada=precio,
                    cantidad=objetivo,
                    cantidad_abierta=cantidad,
                    stop_loss=sl,
                    take_profit=tp,
                    estrategias_activas=estrategias,
                    tendencia=tendencia,
                    timestamp=datetime.now(UTC).isoformat(),
                    max_price=precio,
                    direccion=direccion,
                    entradas=[{'precio': precio, 'cantidad': cantidad}],
                    fracciones_totales=fracciones,
                    fracciones_restantes=max(fracciones - 1, 0),
                    precio_ultima_piramide=precio,
                    puntaje_entrada=puntaje,
                    umbral_entrada=umbral,
					score_tecnico=score_tecnico,
                    detalles_tecnicos=detalles_tecnicos,
                    break_even_activado=False,
                    duracion_en_velas=0,
                    registro_pendiente=True,
                    operation_id=operation_id,
                )
                self.ordenes[symbol] = orden

                if self.bus:
                    estrategias_txt = ', '.join(estrategias.keys())
                    msg_pendiente = (
                        f"""📝 Compra creada (pendiente de registro) {symbol}
                        Precio: {precio:.2f} Cantidad: {cantidad}
                        SL: {sl:.2f} TP: {tp:.2f}
                        Estrategias: {estrategias_txt}"""
                    )
                    await self.bus.publish('notify', {'mensaje': msg_pendiente, 'operation_id': operation_id})

                try:
                    if self.modo_real and is_valid_number(cantidad) and cantidad > 0:
                        execution = await self._execute_real_order(
                            'buy',
                            symbol,
                            cantidad,
                            operation_id,
                            {'side': 'buy', 'symbol': symbol, 'cantidad': cantidad},
							precio=precio,
                        )
                        # actualiza con lo realmente ejecutado
                        cantidad = float(execution.executed)
                        orden.fee_total = getattr(orden, 'fee_total', 0.0) + execution.fee
                        orden.pnl_realizado = getattr(orden, 'pnl_realizado', 0.0) + execution.pnl
                        if cantidad <= 0:
                            if self.bus:
                                await self.bus.publish(
                                    'notify',
                                    {
                                        'mensaje': f'❌ Orden real no ejecutada en {symbol}',
                                        'tipo': 'CRITICAL',
                                        'operation_id': operation_id,
                                    },
                                )
                            registrar_orden('rejected')
                            log_decision(
                                log,
                                'abrir',
                                operation_id,
                                entrada_log,
                                {'ejecucion': 'sin_fills'},
                                'reject',
                                {'reason': 'no_fills'},
                            )
                            return OrderOpenStatus.FAILED
                        if execution.status == 'PARTIAL' and execution.remaining > 0:
                            log.warning(
                                'orders.execution.partial',
                                extra=safe_extra(
                                    {
                                        'symbol': symbol,
                                        'side': 'buy',
                                        'remaining': execution.remaining,
                                        'operation_id': operation_id,
                                    }
                                ),
                            )
                    else:
                        # Simulado: el “coste” inicial lo cargamos como PnL negativo hasta el cierre
                        orden.pnl_realizado = getattr(orden, 'pnl_realizado', 0.0) - (precio * cantidad)

                    if cantidad > 0:
                        if self.modo_real:
                            registrado = False
                            last_error: Exception | None = None
                            for _ in range(3):
                                try:
                                    await asyncio.to_thread(
                                        real_orders.registrar_orden,
                                        symbol,
                                        precio,
                                        cantidad,
                                        sl,
                                        tp,
                                        estrategias,
                                        tendencia,
                                        direccion,
                                        operation_id,
                                    )
                                    registrado = True
                                    break
                                except Exception as e:
                                    last_error = e
                                    registrar_registro_error()
                                    log.error(
                                        'Error registrando orden',
                                        extra={'symbol': symbol, 'error': str(e)},
                                    )
                            if not registrado:
                                msg = (
                                    str(last_error)
                                    if last_error is not None
                                    else 'Error desconocido'
                                )
                                log.error(
                                    'Error registrando orden',
                                    extra={'symbol': symbol, 'error': msg},
                                )
                                if self.bus:
                                    await self.bus.publish(
                                        'notify',
                                        {
                                            'mensaje': f'❌ Error registrando orden {symbol}: {msg}',
                                            'tipo': 'CRITICAL',
                                            'operation_id': operation_id,
                                        },
                                    )
                                if self._should_schedule_persistence_retry(last_error):
                                    reason_label = type(last_error).__name__ if last_error else 'unknown'
                                    self._schedule_registro_retry(symbol, reason=reason_label)
                            await asyncio.sleep(1)

                            if registrado:
                                orden.registro_pendiente = False
                                limpiar_registro_pendiente(symbol)
                                self._registro_pendiente_paused.discard(symbol)
                            else:
                                registrar_orden('failed')  # mantenemos etiqueta por compatibilidad
                                if self.bus:
                                    await self.bus.publish(
                                        'notify',
                                        {
                                            'mensaje': (
                                                f'⚠️ Orden {symbol} ejecutada pero registro pendiente; '
                                                'nuevas entradas pausadas hasta sincronización'
                                            ),
											'tipo': 'WARNING',
											'operation_id': operation_id,
                                        },
                                    )
                                registrar_registro_pendiente(symbol)
                                self._registro_pendiente_paused.add(symbol)

                            orden.cantidad_abierta = cantidad
                            orden.entradas[0]['cantidad'] = cantidad
                        
                        else:
                            if self.bus:
                                await self.bus.publish(
                                    'orden_simulada_creada',
                                    {
                                        'symbol': symbol,
                                        'precio': precio,
                                        'cantidad': cantidad,
                                        'sl': sl,
                                        'tp': tp,
                                        'estrategias': estrategias,
                                        'direccion': direccion,
                                        'operation_id': operation_id,
                                        'orden': orden.to_parquet_record(),
                                    },
                                )
                            orden.registro_pendiente = False
                            limpiar_registro_pendiente(symbol)
                            self._registro_pendiente_paused.discard(symbol)

                except Exception as e:
                    log.error(f'❌ No se pudo abrir la orden para {symbol}: {e}')
                    if self.bus:
                        await self.bus.publish(
                            'notify',
                            {'mensaje': f'❌ Error al abrir orden en {symbol}: {e}', 'tipo': 'CRITICAL'},
                        )
                    self.ordenes.pop(symbol, None)
                    limpiar_registro_pendiente(symbol)
                    self._registro_pendiente_paused.discard(symbol)
                    registrar_orden('failed')
                    return OrderOpenStatus.FAILED

            finally:
                self.abriendo.discard(symbol)
                self._dup_warned.discard(symbol)

            if orden.registro_pendiente:
                log.warning(f'⚠️ Orden {symbol} pendiente de registro')
                log_decision(
                    log,
                    'abrir',
                    operation_id,
                    entrada_log,
                    {'registro': 'pendiente'},
                    'reject',
                    {'reason': 'registro_pendiente'},
                )
                return OrderOpenStatus.PENDING_REGISTRATION

            registrar_orden('opened')
            log.info(f'🟢 Orden abierta para {symbol} @ {precio:.2f}')
            if self.bus and self.modo_real:
                estrategias_txt = ', '.join(estrategias.keys())
                mensaje = (
                    f"""🟢 Compra {symbol}\nPrecio: {precio:.2f} Cantidad: {cantidad}\nSL: {sl:.2f} TP: {tp:.2f}\nEstrategias: {estrategias_txt}"""
                )
                await self.bus.publish('notify', {'mensaje': mensaje, 'operation_id': operation_id})

            log_decision(
                log,
                'abrir',
                operation_id,
                entrada_log,
                {'validaciones': 'ok'},
                'accept',
                {'cantidad': cantidad},
            )
            self._actualizar_capital_disponible(symbol, orden)
            return OrderOpenStatus.OPENED
				
    async def agregar_parcial_async(self, symbol: str, precio: float, cantidad: float) -> bool:
        lock = self._locks.setdefault(symbol, asyncio.Lock())
        async with lock:
            """Aumenta la posición abierta agregando una compra parcial."""
            orden = self.ordenes.get(symbol)
            if not orden:
                return False
            
            operation_id = self._generar_operation_id(symbol)

            if self.modo_real:
                try:
                    if cantidad > 0:
                        execution = await self._execute_real_order(
                            'buy',
                            symbol,
                            cantidad,
                            operation_id,
                            {'side': 'buy', 'symbol': symbol, 'cantidad': cantidad},
							precio=precio,
                        )
                        cantidad = execution.executed
                        orden.fee_total = getattr(orden, 'fee_total', 0.0) + execution.fee
                        orden.pnl_realizado = getattr(orden, 'pnl_realizado', 0.0) + execution.pnl
                except Exception as e:
                    log.error(f'❌ No se pudo agregar posición real para {symbol}: {e}')
                    if self.bus:
                        await self.bus.publish(
                            'notify',
                            {
                                'mensaje': f'❌ Error al agregar posición en {symbol}: {e}',
                                'tipo': 'CRITICAL',
                                'operation_id': operation_id,
                            },
                        )
                    return False
            else:
                # Simulado: costea la compra al PnL
                orden.pnl_realizado = getattr(orden, 'pnl_realizado', 0.0) - (precio * cantidad)

            total_prev = orden.cantidad_abierta
            orden.cantidad_abierta += cantidad
            orden.cantidad += cantidad

            if orden.cantidad > 0:
                orden.precio_entrada = ((orden.precio_entrada * total_prev) + (precio * cantidad)) / orden.cantidad
            else:
                orden.precio_entrada = precio  # fallback defensivo

            orden.max_price = max(getattr(orden, 'max_price', precio), precio)
            if not getattr(orden, 'entradas', None):
                orden.entradas = []
            orden.entradas.append({'precio': precio, 'cantidad': cantidad})
            orden.precio_ultima_piramide = precio
            self._actualizar_capital_disponible(symbol, orden)
            return True

    async def cerrar_async(self, symbol: str, precio: float, motivo: str) -> bool:
        lock = self._locks.setdefault(symbol, asyncio.Lock())
        async with lock:
            """Cierra la orden indicada completamente."""
            orden = self.ordenes.get(symbol)
            if not orden:
                log.warning(f'⚠️ Se intentó verificar TP/SL sin orden activa en {symbol}')
                return False
            if getattr(orden, 'cerrando', False):
                if symbol not in self._dup_warned:
                    log.warning(f'⚠️ Orden duplicada evitada para {symbol}')
                    self._dup_warned.add(symbol)
                return False

            orden.cerrando = True
            operation_id = self._generar_operation_id(symbol)
            entrada_log = {'symbol': symbol, 'precio': precio, 'cantidad': orden.cantidad}
            try:
                venta_exitosa = True

                if self.modo_real:
                    venta_exitosa = False
                    cantidad = orden.cantidad if is_valid_number(orden.cantidad) else 0.0

                    if cantidad > 1e-08:
                        try:
                            execution = await self._execute_real_order(
                                'sell',
                                symbol,
                                cantidad,
                                operation_id,
                                {'side': 'sell', 'symbol': symbol, 'cantidad': cantidad},
								precio=precio,
                            )
                            restante = max(cantidad - execution.executed, 0.0)

                            if execution.executed > 0:
                                orden.fee_total = getattr(orden, 'fee_total', 0.0) + execution.fee
                                orden.pnl_realizado = getattr(orden, 'pnl_realizado', 0.0) + execution.pnl

                            if restante > 0 and not remainder_executable(symbol, precio, restante):
                                log.info(f'♻️ Resto no ejecutable para {symbol}: {restante}')
                                venta_exitosa = True
                                motivo += '|non_executable_remainder'
                            elif execution.executed > 0 and restante <= 1e-08:
                                venta_exitosa = True
                            elif execution.executed > 0 and restante > 0:
                                log.error(f'❌ Venta parcial pendiente ejecutable para {symbol}: restante={restante}')
                                real_orders.registrar_venta_fallida(symbol)
                            else:
                                log.error(f'❌ Venta no ejecutada para {symbol} (sin fills)')
                                real_orders.registrar_venta_fallida(symbol)

                            if self.bus and not venta_exitosa:
                                await self.bus.publish('notify', {'mensaje': f'❌ Venta fallida en {symbol}', 'operation_id': operation_id})
                        except Exception as e:
                            log.error(f'❌ Error al cerrar orden real en {symbol}: {e}')
                            if self.bus:
                                await self.bus.publish('notify', {'mensaje': f'❌ Venta fallida en {symbol}: {e}', 'operation_id': operation_id})
                else:
                    # Modo simulado: calcula PnL por diferencia
                    diff = (precio - orden.precio_entrada) * orden.cantidad
                    if orden.direccion in ('short', 'venta'):
                        diff = -diff
                    orden.pnl_realizado = getattr(orden, 'pnl_realizado', 0.0) + diff

                # Si la venta no fue exitosa, no alteramos estado de cierre ni borramos la orden
                if not venta_exitosa:
                    if self.bus and self.modo_real:
                        await self.bus.publish(
                            'notify',
                            {
                                'mensaje': f'⚠️ Venta no realizada, se reintentará en {symbol}',
                                'operation_id': operation_id,
                            },
                        )
                    log_decision(log, 'cerrar', operation_id, entrada_log, {'venta_exitosa': False}, 'reject', {'reason': 'venta_no_realizada'})
                    return False

                # Venta exitosa: cerrar y registrar
                orden.precio_cierre = precio
                orden.fecha_cierre = datetime.now(UTC).isoformat()
                orden.motivo_cierre = motivo
                orden.pnl_latente = 0.0

                base = orden.precio_entrada * orden.cantidad if orden.cantidad else 0.0
                retorno = (orden.pnl_realizado / base) if base else 0.0
                orden.retorno_total = retorno

                self.historial.setdefault(symbol, []).append(orden.to_dict())
                if len(self.historial[symbol]) > self.max_historial:
                    self.historial[symbol] = self.historial[symbol][-self.max_historial:]

                if retorno < 0 and self.bus:
                    await self.bus.publish('registrar_perdida', {'symbol': symbol, 'perdida': retorno})

                log.info(f'📤 Orden cerrada para {symbol} @ {precio:.2f} | {motivo}')
                if self.bus:
                    if self.modo_real:
                        mensaje = (
                            f"""📤 Venta {symbol}\nEntrada: {orden.precio_entrada:.2f} Salida: {precio:.2f}\nRetorno: {retorno * 100:.2f}%\nMotivo: {motivo}"""
                        )
                        await self.bus.publish('notify', {'mensaje': mensaje, 'operation_id': operation_id})
                    else:
                        await self.bus.publish(
                            'orden_simulada_cerrada',
                            {
                                'symbol': symbol,
                                'precio_cierre': precio,
                                'retorno': retorno,
                                'motivo': motivo,
                                'operation_id': operation_id,
                            },
                        )

                log_decision(log, 'cerrar', operation_id, entrada_log, {'venta_exitosa': True}, 'accept', {'retorno': retorno})

                registrar_orden('closed')

                # Finalmente, elimina del activo
                self.ordenes.pop(symbol, None)
                limpiar_registro_pendiente(symbol)
                self._registro_pendiente_paused.discard(symbol)
                self._actualizar_capital_disponible(symbol)
                return True

            finally:
                orden.cerrando = False
                self._dup_warned.discard(symbol)

    async def cerrar_parcial_async(self, symbol: str, cantidad: float, precio: float, motivo: str) -> bool:
        """Cierra parcialmente la orden activa."""
        lock = self._locks.setdefault(symbol, asyncio.Lock())
        if lock.locked():
            registrar_partial_close_collision(symbol)
            log.warning(
                'Cierre parcial concurrente; se encola segundo intento',
                extra={'symbol': symbol},
            )
        async with lock:
            orden = self.ordenes.get(symbol)
            order_id = getattr(orden, 'operation_id', 'N/A') if orden else 'N/A'
            log.debug(
                'Enter cerrar_parcial lock',
                extra={'symbol': symbol, 'order_id': order_id},
            )
            try:
                if not orden or orden.cantidad_abierta <= 0:
                    log.warning(
                        'Se intentó cierre parcial sin orden activa',
                        extra={'symbol': symbol},
                    )
                    return False

                cantidad = min(cantidad, orden.cantidad_abierta)

                entrada_log = {
                    "symbol": symbol,
                    "cantidad": cantidad,
                    "precio": precio,
                    "motivo": motivo,
                }
                if cantidad < 1e-08:
                    log.warning(
                        'Cantidad demasiado pequeña para vender',
                        extra={'symbol': symbol, 'cantidad': cantidad},
                    )
                    return False

                operation_id = self._generar_operation_id(symbol)
				
                if self.modo_real:
                    try:
                        execution = await self._execute_real_order(
                            'sell',
                            symbol,
                            cantidad,
                            operation_id,
                            {'side': 'sell', 'symbol': symbol, 'cantidad': cantidad},
							precio=precio,
                        )
                        executed_qty = float(execution.executed or 0.0)
                        if executed_qty <= 0.0:
                            log.warning(
                                "orders.partial_close.no_fill",
                                extra=safe_extra(
                                    {
                                        "symbol": symbol,
                                        "operation_id": operation_id,
                                        "requested_qty": cantidad,
                                        "status": execution.status,
                                    }
                                ),
                            )
                            requeued = self._requeue_partial_close(
                                symbol,
                                cantidad,
                                precio,
                                motivo,
                                operation_id=operation_id,
                            )
                            log_decision(
                                log,
                                'cerrar_parcial',
                                operation_id,
                                entrada_log,
                                {},
                                'reject',
                                {
                                    'reason': 'no_fill',
                                    'requeued': requeued,
                                    'status': execution.status,
                                },
                            )
                            return False
                        cantidad = executed_qty
                        orden.pnl_realizado = getattr(orden, 'pnl_realizado', 0.0) + execution.pnl
                        orden.pnl_operaciones = getattr(orden, 'pnl_operaciones', 0.0) + execution.pnl
                    except Exception as e:
                        log.error(
                            'Error en venta parcial',
                            extra={'symbol': symbol, 'error': str(e)},
                        )
                        if self.bus:
                            await self.bus.publish('notify', {'mensaje': f'❌ Venta parcial fallida en {symbol}: {e}', 'operation_id': operation_id})
                        log_decision(log, 'cerrar_parcial', operation_id, entrada_log, {}, 'reject', {'reason': str(e)})
                        return False
                else:
                    diff = (precio - orden.precio_entrada) * cantidad
                    if orden.direccion in ('short', 'venta'):
                        diff = -diff
                    orden.pnl_realizado = getattr(orden, 'pnl_realizado', 0.0) + diff

                orden.cantidad_abierta -= cantidad
                self.actualizar_mark_to_market(symbol, precio)

                log.info(f'📤 Cierre parcial de {symbol}: {cantidad} @ {precio:.2f} | {motivo}')
                if self.bus:
                    mensaje = f"""📤 Venta parcial {symbol}\nCantidad: {cantidad}\nPrecio: {precio:.2f}\nMotivo: {motivo}"""
                    await self.bus.publish('notify', {'mensaje': mensaje, 'operation_id': operation_id})

                if orden.cantidad_abierta <= 0:
                    orden.precio_cierre = precio
                    orden.fecha_cierre = datetime.now(UTC).isoformat()
                    orden.motivo_cierre = motivo
					orden.pnl_latente = 0.0
                    base = orden.precio_entrada * orden.cantidad if orden.cantidad else 0.0
                    retorno = (orden.pnl_realizado / base) if base else 0.0
                    orden.retorno_total = retorno
                    self.historial.setdefault(symbol, []).append(orden.to_dict())
                    if len(self.historial[symbol]) > self.max_historial:
                        self.historial[symbol] = self.historial[symbol][-self.max_historial:]
                    if retorno < 0 and self.bus:
                        await self.bus.publish('registrar_perdida', {'symbol': symbol, 'perdida': retorno})
                    log.info(f'📤 Orden cerrada para {symbol} @ {precio:.2f} | {motivo}')
                    if self.bus:
                        if self.modo_real:
                            mensaje = (
                                f"""📤 Venta {symbol}\nEntrada: {orden.precio_entrada:.2f} Salida: {precio:.2f}\nRetorno: {retorno * 100:.2f}%\nMotivo: {motivo}"""
                            )
                            await self.bus.publish('notify', {'mensaje': mensaje, 'operation_id': operation_id})
                        else:
                            await self.bus.publish(
                                'orden_simulada_cerrada',
                                {
                                    'symbol': symbol,
                                    'precio_cierre': precio,
                                    'retorno': retorno,
                                    'motivo': motivo,
                                    'operation_id': operation_id,
                                },
                            )
                    self.ordenes.pop(symbol, None)
                    limpiar_registro_pendiente(symbol)
                    self._registro_pendiente_paused.discard(symbol)

                    if self.modo_real:
                        try:
                            await asyncio.to_thread(real_orders.eliminar_orden, symbol)
                        except Exception as e:
                            log.error(f'❌ Error eliminando orden {symbol} de SQLite: {e}')
                    registrar_orden('closed')
                    log_decision(log, 'cerrar_parcial', operation_id, {'symbol': symbol, 'cantidad': cantidad}, {}, 'accept', {'retorno': retorno})
                    self._actualizar_capital_disponible(symbol)
                else:
                    registrar_orden('partial')
                    log_decision(log, 'cerrar_parcial', operation_id, {'symbol': symbol, 'cantidad': cantidad}, {}, 'accept', {'parcial': True})
                    self._actualizar_capital_disponible(symbol, orden)
                return True
            finally:
                log.debug(f'🔓 Exit cerrar_parcial lock {symbol} id={order_id}')

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

        cantidad, cantidad_source = await self._resolve_quantity_with_fallback(
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
            return await self.abrir_async(
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
                extra=safe_extra({"symbol": symbol, "side": direccion, "error": str(exc)}),
            )
            return OrderOpenStatus.FAILED

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
            orden.pnl_latente = 0.0
            return

        cantidad = getattr(orden, "cantidad_abierta", None)
        if not is_valid_number(cantidad):
            cantidad = getattr(orden, "cantidad", 0.0)
        cantidad_float = float(cantidad or 0.0)
        if cantidad_float <= 0.0:
            orden.pnl_latente = 0.0
            return

        direccion = str(getattr(orden, "direccion", "long")).lower()
        signo = -1.0 if direccion in {"short", "venta"} else 1.0
        orden.pnl_latente = signo * precio * cantidad_float
