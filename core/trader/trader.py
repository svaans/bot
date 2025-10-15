"""Implementación completa del trader con compatibilidad histórica."""

from __future__ import annotations

import asyncio
import contextlib
import time
from collections import OrderedDict, deque
from collections.abc import Iterable, Iterator, MutableMapping
from datetime import datetime, timezone
from copy import deepcopy
from typing import Any, Awaitable, Callable, Dict, Optional, TYPE_CHECKING

import pandas as pd

from core.streams.candle_filter import CandleFilter

from core.utils.feature_flags import is_flag_enabled
from core.utils.log_utils import safe_extra
from core.utils.utils import configurar_logger

from ._utils import (
    EstadoSimbolo,
    _maybe_await,
    _normalize_timestamp,
    _reason_none,
    _silence_task_result,
    tf_seconds,
)
from .trader_lite import TraderComponentFactories, TraderLite

if TYPE_CHECKING:  # pragma: no cover
    from core.notification_manager import NotificationManager
    from core.supervisor import Supervisor


UTC = timezone.utc
log = configurar_logger("trader_modular", modo_silencioso=True)


class HistorialPorSimbolo(MutableMapping[str, Any]):
    """Almacén ordenado con límites de tamaño y TTL opcional por símbolo."""

    __slots__ = ("_data", "_timestamps", "_max_entries", "_ttl_seconds")

    def __init__(self, *, max_entries: int, ttl_seconds: int | None) -> None:
        self._data: OrderedDict[str, Any] = OrderedDict()
        self._timestamps: dict[str, float] = {}
        self._max_entries = max(0, int(max_entries))
        self._ttl_seconds = ttl_seconds if ttl_seconds and ttl_seconds > 0 else None

    def __getitem__(self, key: str) -> Any:
        self.prune()
        return self._data[key]

    def __setitem__(self, key: str, value: Any) -> None:
        self.prune()
        self._data[key] = value
        self._timestamps[key] = time.time()
        self._enforce_bounds()

    def __delitem__(self, key: str) -> None:
        self.prune()
        del self._data[key]
        self._timestamps.pop(key, None)

    def __iter__(self) -> Iterator[str]:
        self.prune()
        return iter(self._data)

    def __len__(self) -> int:
        self.prune()
        return len(self._data)

    def clear(self) -> None:
        self._data.clear()
        self._timestamps.clear()

    def prune(self) -> None:
        """Elimina elementos expirados y aplica límites de tamaño."""

        if self._ttl_seconds is not None:
            now = time.time()
            expired = [
                key for key, ts in list(self._timestamps.items()) if now - ts > self._ttl_seconds
            ]
            for key in expired:
                self._data.pop(key, None)
                self._timestamps.pop(key, None)
        self._enforce_bounds()

    def _enforce_bounds(self) -> None:
        if self._max_entries <= 0:
            return
        while len(self._data) > self._max_entries:
            oldest_key, _ = self._data.popitem(last=False)
            self._timestamps.pop(oldest_key, None)


class HistorialCierresStore(MutableMapping[str, HistorialPorSimbolo]):
    """Agrupa ``HistorialPorSimbolo`` y ofrece utilidades agregadas."""

    __slots__ = ("_stores", "_max_entries", "_ttl_seconds")

    def __init__(
        self,
        symbols: Iterable[str],
        *,
        max_entries: int,
        ttl_seconds: int | None,
    ) -> None:
        self._max_entries = max_entries
        self._ttl_seconds = ttl_seconds
        self._stores: Dict[str, HistorialPorSimbolo] = {
            symbol: self._new_store() for symbol in symbols
        }

    def _new_store(self) -> HistorialPorSimbolo:
        return HistorialPorSimbolo(
            max_entries=self._max_entries,
            ttl_seconds=self._ttl_seconds,
        )

    def __getitem__(self, key: str) -> HistorialPorSimbolo:
        store = self._stores.setdefault(key, self._new_store())
        store.prune()
        return store

    def __setitem__(self, key: str, value: MutableMapping[str, Any]) -> None:
        if not isinstance(value, MutableMapping):
            raise TypeError("El historial por símbolo debe ser un mapeo mutable")
        store = self._stores.setdefault(key, self._new_store())
        store.clear()
        for sub_key, sub_value in value.items():
            store[sub_key] = sub_value

    def __delitem__(self, key: str) -> None:
        del self._stores[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self._stores)

    def __len__(self) -> int:
        return len(self._stores)

    def prune(self) -> None:
        for store in self._stores.values():
            store.prune()

    def prune_symbol(self, symbol: str) -> None:
        store = self._stores.get(symbol)
        if store is None:
            return
        store.prune()

    def total_entries(self) -> int:
        return sum(len(store) for store in self._stores.values())


class Trader(TraderLite):
    """Wrapper ligero que expone la interfaz histórica del bot."""

    def __init__(
        self,
        config: Any,
        *,
        candle_handler: Optional[Callable[[dict], Awaitable[None]]] = None,
        on_event: Optional[Callable[[str, dict], None]] = None,
        supervisor: Optional[Supervisor] = None,
        component_factories: TraderComponentFactories | None = None,
        strict_components: bool | None = None,
    ) -> None:
        super().__init__(
            config,
            candle_handler=candle_handler,
            on_event=on_event,
            supervisor=supervisor,
            component_factories=component_factories,
            strict_components=strict_components,
        )
        self.modo_real = bool(getattr(config, "modo_real", False))
        self.cliente = self._cliente
        self.data_feed = self.feed
        max_historial = int(getattr(config, "historial_cierres_max_per_symbol", 720) or 720)
        ttl_minutes = getattr(config, "historial_cierres_ttl_min", None)
        ttl_seconds: int | None
        if ttl_minutes is None:
            ttl_seconds = None
        else:
            ttl_seconds = max(0, int(ttl_minutes) * 60)
        self.historial_cierres = HistorialCierresStore(
            config.symbols,
            max_entries=max_historial,
            ttl_seconds=ttl_seconds,
        )
        self._historial_cierres_max = max_historial
        self._historial_cierres_ttl = ttl_seconds
        self._historial_purge_enabled = is_flag_enabled("trader.purge_historial.enabled")
        self.fecha_actual = datetime.now(UTC).date()
        self.estrategias_habilitadas = False
        self._eval_enabled: Dict[tuple[str, str], bool] = {}
        self._last_eval_skip_reason: str | None = None
        self._last_eval_skip_details: Dict[str, Any] | None = None
        self._bg_tasks: set[asyncio.Task] = set()
        self.notificador: NotificationManager | None = None
        self._verificar_entrada_provider: str | None = None
        offload_enabled = getattr(config, "trader_eval_offload_enabled", None)
        if offload_enabled is None:
            offload_enabled = getattr(config, "trader_eval_offload", False)
        threshold_raw = getattr(config, "trader_eval_offload_min_df", None)
        threshold = int(threshold_raw) if threshold_raw is not None else 750
        self._eval_offload_enabled = bool(offload_enabled)
        self._eval_offload_min_df = max(1, threshold)
        self._eval_offload_count = 0

        # Lazy construcciones (si los módulos existen)
        if getattr(self, "_EventBus", None):
            self.bus = self._EventBus()
            self._owned_event_bus = self.bus
        else:
            self.bus = None
        self.event_bus = self.bus
        feed = getattr(self, "feed", None)
        if feed is not None:
            try:
                setattr(feed, "event_bus", self.bus)
            except Exception:
                log.debug("No se pudo asociar event_bus al DataFeed", exc_info=True)

        bus = getattr(self, "bus", None)
        order_manager_cls = getattr(self, "_OrderManager", None)
        subscribe = getattr(bus, "subscribe", None)
        if order_manager_cls and bus is not None and callable(subscribe):
            self.orders = order_manager_cls(
                self.modo_real,
                bus,
                config=getattr(self, "config", None),
            )
            if not self.modo_real and getattr(self, "_sync_sim", None):
                try:
                    self._sync_sim(self.orders)
                except Exception:
                    log.exception("Fallo sincronizando órdenes simuladas")
        else:
            self.orders = None

        if getattr(self, "_StrategyEngine", None):
            self.engine = self._StrategyEngine()
        else:
            self.engine = None

        if getattr(self, "_PersistenciaTecnica", None):
            persistencia_min = int(getattr(config, "persistencia_minima", 1) or 1)
            persistencia_extra = float(getattr(config, "peso_extra_persistencia", 0.5) or 0.5)
            try:
                self.persistencia = self._PersistenciaTecnica(
                    minimo=persistencia_min, peso_extra=persistencia_extra
                )
            except Exception:
                self.persistencia = None
                log.exception("No se pudo inicializar PersistenciaTecnica")
        else:
            self.persistencia = None

        # Registro opcional de ventanas de cooldown por símbolo para entradas nuevas.
        self._entrada_cooldowns: Dict[str, datetime] = {}
        # Config por símbolo
        self.config_por_simbolo: Dict[str, dict] = {}
        config_pesos = getattr(config, "pesos_por_simbolo", None)
        if isinstance(config_pesos, dict):
            self.pesos_por_simbolo = {
                symbol: dict(config_pesos.get(symbol, {})) for symbol in config.symbols
            }
        else:
            self.pesos_por_simbolo = {symbol: {} for symbol in config.symbols}

    def start(self) -> None:
        """Arranca el trader y asegura la sincronización inicial de órdenes."""
        super().start()

        if not self.modo_real:
            return

        ordenes = getattr(self, "orders", None)
        if ordenes is None:
            return

        start_sync = getattr(ordenes, "start_sync", None)
        if not callable(start_sync):
            return

        try:
            start_sync()
        except Exception:  # pragma: no cover - log defensivo
            log.exception("No se pudo iniciar la sincronización de órdenes tras el arranque")

    async def ejecutar(self) -> None:
        """Inicia el trader y espera hasta que finalice la tarea principal."""
        self.start()
        if self._runner_task is not None:
            await self._runner_task

    async def cerrar(self) -> None:
        """Detiene el trader y limpia tareas en segundo plano."""
        await self.stop()
        while self._bg_tasks:
            task = self._bg_tasks.pop()
            if task.done():
                continue
            task.cancel()
            with contextlib.suppress(Exception):
                await task
        if getattr(self, "bus", None) is not None and hasattr(self.bus, "close"):
            with contextlib.suppress(Exception):
                await self.bus.close()

    def solicitar_parada(self) -> None:
        """Señala al trader que debe detenerse en cuanto sea posible."""
        self._stop_event.set()

    def _register_bg_task(self, task: asyncio.Task) -> None:
        """Mantiene controladas las tareas en segundo plano hasta el cierre."""

        self._bg_tasks.add(task)

        def _finalize(completed: asyncio.Task) -> None:
            self._bg_tasks.discard(completed)
            _silence_task_result(completed)

        task.add_done_callback(_finalize)

    async def _precargar_historico(self, velas: int | None = None) -> None:
        """Realiza un backfill inicial antes de abrir streams."""
        used_backfill = False
        if hasattr(self, "start_backfill") and callable(self.start_backfill):
            try:
                await self.start_backfill()
                used_backfill = True
            except Exception:
                log.exception("Fallo en backfill configurado; se intenta precargar desde DataFeed")
        if hasattr(self, "feed") and callable(getattr(self.feed, "precargar", None)):
            await self.feed.precargar(self.config.symbols, cliente=self._cliente, minimo=velas)
        if used_backfill:
            return

    def habilitar_estrategias(self) -> None:
        """Marca las estrategias como habilitadas (bandera de compatibilidad)."""
        self.estrategias_habilitadas = True

    def ajustar_capital_diario(self, *, fecha: Optional[Any] = None) -> None:
        """Actualiza la fecha de referencia utilizada por el capital manager."""
        target = fecha or datetime.now(UTC).date()
        self.fecha_actual = target

    def _puede_evaluar_entradas(self, symbol: str) -> bool:
        """Determina si ``symbol`` puede ser evaluado para una nueva entrada."""
        if getattr(self, "_stop_event", None) and self._stop_event.is_set():
            log.debug("[%s] Entrada bloqueada: stop solicitado", symbol)
            return False

        # Evitar duplicados cuando ya existe una orden o se está ejecutando la apertura.
        try:
            ordenes = getattr(self, "orders", None)
            if ordenes is not None:
                obtener_orden = getattr(ordenes, "obtener", None)
                if callable(obtener_orden):
                    orden_existente = obtener_orden(symbol)
                    if orden_existente is not None:
                        log.debug("[%s] Entrada bloqueada: orden existente", symbol)
                        return False
                abriendo = getattr(ordenes, "abriendo", None)
                if isinstance(abriendo, set) and symbol in abriendo:
                    log.debug("[%s] Entrada bloqueada: apertura en curso", symbol)
                    return False
        except Exception:
            log.warning("[%s] Error inspeccionando estado de órdenes", symbol, exc_info=True)

        # Respeta ventanas de cooldown externas (por ejemplo tras pérdidas consecutivas).
        cooldowns = getattr(self, "_entrada_cooldowns", None)
        if isinstance(cooldowns, dict):
            ventana = cooldowns.get(symbol)
            ahora = datetime.now(UTC)
            if isinstance(ventana, datetime) and ventana > ahora:
                log.debug(
                    "[%s] Entrada bloqueada: cooldown activo hasta %s",
                    symbol,
                    ventana.isoformat(),
                )
                return False
            if isinstance(ventana, (int, float)) and ventana > ahora.timestamp():
                log.debug(
                    "[%s] Entrada bloqueada: cooldown activo (timestamp %.0f)",
                    symbol,
                    ventana,
                )
                return False

        capital_manager = getattr(self, "capital_manager", None)
        if capital_manager is not None:
            try:
                if hasattr(capital_manager, "hay_capital_libre") and not capital_manager.hay_capital_libre():
                    log.debug("[%s] Entrada bloqueada: sin capital libre", symbol)
                    return False
                if hasattr(capital_manager, "tiene_capital") and not capital_manager.tiene_capital(symbol):
                    log.debug("[%s] Entrada bloqueada: sin capital asignado", symbol)
                    return False
            except Exception:
                log.warning("[%s] Error consultando capital disponible", symbol, exc_info=True)

        risk = getattr(self, "risk", None)
        if risk is not None and hasattr(risk, "permite_entrada"):
            try:
                correlaciones = {}
                if hasattr(risk, "correlaciones"):
                    correlaciones = risk.correlaciones.get(symbol, {})
                diversidad_minima = float(getattr(self.config, "diversidad_minima", 0.0) or 0.0)
                if not risk.permite_entrada(symbol, correlaciones, diversidad_minima):
                    log.debug("[%s] Entrada bloqueada por gestor de riesgo", symbol)
                    return False
            except Exception:
                log.warning("[%s] Error consultando gestor de riesgo", symbol, exc_info=True)

        return True

    async def evaluar_condiciones_de_entrada(
        self,
        symbol: str,
        df: pd.DataFrame,
        estado: Any,
    ) -> dict[str, Any] | None:
        """Evalúa condiciones de entrada delegando en el pipeline modular.

        Returns
        -------
        dict[str, Any] | None
            Diccionario con la propuesta de entrada listo para `_abrir_operacion_real`
            o ``None`` si no se cumplieron las condiciones.
        """
        self._last_eval_skip_reason = None
        self._last_eval_skip_details = None
        if not self.estrategias_habilitadas:
            log.debug("[%s] Estrategias deshabilitadas; entrada omitida", symbol)
            self._last_eval_skip_reason = "strategies_disabled"
            self._last_eval_skip_details = {"symbol": symbol}
            return None
        if not isinstance(df, pd.DataFrame) or df.empty:
            log.warning("[%s] DataFrame inválido al evaluar entrada", symbol)
            self._last_eval_skip_reason = "invalid_df"
            self._last_eval_skip_details = {"symbol": symbol}
            return None
        
        timeframe: Optional[str] = getattr(df, "tf", None)
        if timeframe is None:
            attrs_tf: Optional[str] = None
            if hasattr(df, "attrs"):
                try:
                    attrs_tf = df.attrs.get("tf")  # type: ignore[assignment]
                except Exception:
                    attrs_tf = None
            if attrs_tf:
                timeframe = str(attrs_tf)
            else:
                config_tf = getattr(getattr(self, "config", None), "intervalo_velas", None)
                timeframe = str(config_tf) if config_tf else None
        timeframe_str = str(timeframe) if timeframe else None

        buf_len = len(df)
        min_bars = self._resolve_min_bars_requirement()
        last_bar_ts_raw: Optional[int] = None
        if buf_len:
            try:
                last_bar_ts_raw = int(df.iloc[-1]["timestamp"])
            except Exception:
                last_bar_ts_raw = None

        tf_secs = tf_seconds(timeframe_str)

        bar_close_ts = _normalize_timestamp(last_bar_ts_raw)
        bar_open_raw: Any = None
        bar_close_raw: Any = None
        bar_event_raw: Any = None

        buffer_ref = getattr(estado, "buffer", None)
        last_candle: Any = None
        if buffer_ref:
            try:
                last_candle = buffer_ref[-1]
            except (IndexError, TypeError):
                last_candle = None

        if isinstance(last_candle, dict):
            bar_open_raw = last_candle.get("open_time")
            bar_close_raw = last_candle.get("close_time")
            bar_event_raw = (
                last_candle.get("event_time")
                or last_candle.get("close_time")
                or last_candle.get("timestamp")
            )

        bar_close_ts = _normalize_timestamp(bar_close_raw) or bar_close_ts
        bar_event_ts = _normalize_timestamp(
            bar_event_raw
            if bar_event_raw is not None
            else bar_close_raw
            if bar_close_raw is not None
            else last_bar_ts_raw
        )
        bar_open_ts = _normalize_timestamp(bar_open_raw)

        if bar_open_ts is None and bar_close_ts is not None and tf_secs > 0:
            bar_open_ts = bar_close_ts - tf_secs
        if bar_open_ts is None and bar_event_ts is not None and tf_secs > 0:
            bar_open_ts = bar_event_ts - tf_secs

        timing_ctx: Dict[str, Any] = {}
        reason = _reason_none(
            symbol,
            timeframe_str,
            buf_len,
            min_bars,
            bar_open_ts,
            bar_event_ts,
            interval_secs=tf_secs,
            bar_close_ts=bar_close_ts,
            context=timing_ctx,
        )

        interval_secs = timing_ctx.get("interval_secs") or tf_secs
        bar_open_ts = timing_ctx.get("bar_open_ts", bar_open_ts)
        bar_close_ts = timing_ctx.get("bar_close_ts", bar_close_ts)
        bar_event_ts = timing_ctx.get("bar_event_ts", bar_event_ts)
        elapsed_secs = timing_ctx.get("elapsed_secs")
        skew_allow = timing_ctx.get("skew_allow_secs", 1.5)
        remaining_secs = (
            (interval_secs - elapsed_secs)
            if (interval_secs and elapsed_secs is not None)
            else None
        )

        eval_key = (symbol.upper(), (timeframe_str or "unknown"))

        if reason == "warmup":
            self._eval_enabled[eval_key] = False
            log.debug(
                "[%s] Warmup incompleto; omitiendo evaluación",
                symbol,
                extra=safe_extra(
                    {
                        "symbol": symbol,
                        "timeframe": timeframe_str,
                        "reason": reason,
                        "buffer_len": buf_len,
                        "min_needed": min_bars,
                    }
                ),
            )
            self._last_eval_skip_reason = "warmup"
            self._last_eval_skip_details = {
                "timeframe": timeframe_str,
                "buffer_len": buf_len,
                "min_needed": min_bars,
            }
            return None

        if reason == "waiting_close":
            log.debug(
                "[%s] Esperando cierre de vela para evaluar",
                symbol,
                extra=safe_extra(
                    {
                        "symbol": symbol,
                        "timeframe": timeframe_str,
                        "reason": reason,
                        "buffer_len": buf_len,
                        "min_needed": min_bars,
                        "bar_open_ts": bar_open_ts,
                        "bar_close_ts": bar_close_ts,
                        "event_ts": bar_event_ts,
                        "elapsed_secs": elapsed_secs,
                        "elapsed_ms": int(elapsed_secs * 1000)
                        if elapsed_secs is not None
                        else None,
                        "remaining_secs": remaining_secs,
                        "remaining_ms": int(remaining_secs * 1000)
                        if remaining_secs is not None
                        else None,
                        "interval_secs": interval_secs,
                        "interval_ms": interval_secs * 1000 if interval_secs else None,
                        "skew_allow_secs": skew_allow,
                        "skew_allow_ms": int(skew_allow * 1000),
                        "last_bar_ts_raw": last_bar_ts_raw,
                    }
                ),
            )
            self._last_eval_skip_reason = "waiting_close"
            self._last_eval_skip_details = {
                "timeframe": timeframe_str,
                "buffer_len": buf_len,
                "min_needed": min_bars,
                "bar_open_ts": bar_open_ts,
                "bar_close_ts": bar_close_ts,
                "event_ts": bar_event_ts,
                "elapsed_secs": elapsed_secs,
                "elapsed_ms": int(elapsed_secs * 1000)
                if elapsed_secs is not None
                else None,
                "remaining_secs": remaining_secs,
                "remaining_ms": int(remaining_secs * 1000)
                if remaining_secs is not None
                else None,
                "interval_secs": interval_secs,
                "interval_ms": interval_secs * 1000 if interval_secs else None,
                "skew_allow_secs": skew_allow,
                "skew_allow_ms": int(skew_allow * 1000),
                "last_bar_ts_raw": last_bar_ts_raw,
            }
            return None

        if reason in {"bar_in_future", "bar_ts_out_of_range"}:
            log.warning(
                "[%s] Timestamp de vela inválido (%s)",
                symbol,
                reason,
                extra=safe_extra(
                    {
                        "symbol": symbol,
                        "timeframe": timeframe_str,
                        "reason": reason,
                        "buffer_len": buf_len,
                        "min_needed": min_bars,
                        "bar_open_ts": bar_open_ts,
                        "bar_close_ts": bar_close_ts,
                        "event_ts": bar_event_ts,
                        "elapsed_secs": elapsed_secs,
                        "elapsed_ms": int(elapsed_secs * 1000)
                        if elapsed_secs is not None
                        else None,
                        "interval_secs": interval_secs,
                        "interval_ms": interval_secs * 1000 if interval_secs else None,
                        "skew_allow_secs": skew_allow,
                        "skew_allow_ms": int(skew_allow * 1000),
                        "last_bar_ts_raw": last_bar_ts_raw,
                    }
                ),
            )
            self._last_eval_skip_reason = reason
            self._last_eval_skip_details = {
                "timeframe": timeframe_str,
                "buffer_len": buf_len,
                "min_needed": min_bars,
                "bar_open_ts": bar_open_ts,
                "bar_close_ts": bar_close_ts,
                "event_ts": bar_event_ts,
                "elapsed_secs": elapsed_secs,
                "elapsed_ms": int(elapsed_secs * 1000)
                if elapsed_secs is not None
                else None,
                "interval_secs": interval_secs,
                "interval_ms": interval_secs * 1000 if interval_secs else None,
                "skew_allow_secs": skew_allow,
                "skew_allow_ms": int(skew_allow * 1000),
                "last_bar_ts_raw": last_bar_ts_raw,
            }
            return None

        if not self._should_evaluate(symbol, timeframe_str, last_bar_ts_raw):
            log.debug(
                "[%s] Saltando evaluación (sin nueva vela cerrada)",
                symbol,
                extra=safe_extra(
                    {
                        "symbol": symbol,
                        "timeframe": timeframe_str,
                        "reason": "duplicate_bar",
                        "buffer_len": buf_len,
                        "min_needed": min_bars,
                    }
                ),
            )
            self._last_eval_skip_reason = "duplicate_bar"
            self._last_eval_skip_details = {
                "timeframe": timeframe_str,
                "buffer_len": buf_len,
                "min_needed": min_bars,
            }
            return None

        previously_disabled = self._eval_enabled.get(eval_key)
        if previously_disabled is False:
            self._eval_enabled[eval_key] = True
            log.info(
                "eval.enabled",
                extra=safe_extra(
                    {
                        "symbol": symbol,
                        "timeframe": timeframe_str,
                        "buffer_len": buf_len,
                        "min_needed": min_bars,
                    }
                ),
            )
        else:
            self._eval_enabled.setdefault(eval_key, True)

        base_on_event: Callable[[str, dict], None] | None = None
        if hasattr(self, "_emit"):
            base_on_event = self._emit
        elif callable(self.on_event):
            base_on_event = self.on_event

        captured_events: list[tuple[str, dict[str, Any]]] = []

        def _handle_event(evt: str, data: dict) -> None:
            captured_events.append((evt, data))
            if base_on_event is None:
                return
            try:
                base_on_event(evt, data)
            except Exception:
                pass

        try:
            resultado = await self._execute_pipeline(
                symbol,
                df,
                estado,
                _handle_event,
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("❌ Error evaluando condiciones de entrada para %s", symbol)
            return None

        if resultado:
            log.debug("[%s] Entrada candidata generada", symbol)
            self._last_eval_skip_reason = None
            self._last_eval_skip_details = None
            return resultado

        provider = getattr(self, "_verificar_entrada_provider", None)

        skip_reason: str | None = None
        skip_details: dict[str, Any] | None = None
        for evt, data in reversed(captured_events):
            if evt not in {"entry_skip", "entry_error", "entry_timeout", "entry_gate_blocked"}:
                continue
            reason_candidate = data.get("reason")
            skip_reason = str(reason_candidate) if reason_candidate else evt
            skip_details = dict(data)
            break

        if skip_reason is not None:
            if skip_details is not None and provider and "provider" not in skip_details:
                skip_details = {**skip_details, "provider": provider}
            self._last_eval_skip_reason = skip_reason
            self._last_eval_skip_details = skip_details
            return None
        
        self._last_eval_skip_reason = "no_signal" if provider else "pipeline_missing"
        pipeline_diagnostics: dict[str, Any] = {}
        if provider is None:
            pipeline_diagnostics = self._collect_pipeline_diagnostics()

        details: dict[str, Any] = {
            "timeframe": timeframe_str,
            "buffer_len": buf_len,
            "min_needed": min_bars,
            "provider": provider,
        }
        if pipeline_diagnostics:
            details.update(pipeline_diagnostics)
        self._last_eval_skip_details = details
        log.debug(
            "[%s] Sin condiciones de entrada válidas",
            symbol,
            extra=safe_extra(
                {
                    "symbol": symbol,
                    "timeframe": timeframe_str,
                    "reason": "sin_senal" if provider else "pipeline_missing",
                    "buffer_len": buf_len,
                    "min_needed": min_bars,
                    "provider": provider,
                    **{k: v for k, v in pipeline_diagnostics.items() if k.startswith("pipeline_")},
                }
            ),
        )
        return None

    async def _execute_pipeline(
        self,
        symbol: str,
        df: pd.DataFrame,
        estado: Any,
        on_event: Callable[[str, dict], None] | None,
    ) -> dict[str, Any] | None:
        """Ejecuta el pipeline de verificación respetando configuración de offload."""

        if self._should_offload_evaluation(df):
            return await self._run_eval_offloaded(symbol, df, estado, on_event)
        return await super()._execute_pipeline(symbol, df, estado, on_event)

    def _should_offload_evaluation(self, df: pd.DataFrame) -> bool:
        if not self._eval_offload_enabled:
            return False
        try:
            length = len(df)
        except Exception:
            return False
        return length >= self._eval_offload_min_df

    async def _run_eval_offloaded(
        self,
        symbol: str,
        df: pd.DataFrame,
        estado: Any,
        on_event: Callable[[str, dict], None] | None,
    ) -> dict[str, Any] | None:
        """Delegates synchronous pipelines to a worker thread to avoid blocking."""

        loop = asyncio.get_running_loop()
        df_copy = df.copy(deep=False)
        estado_copy = self._snapshot_offload_state(estado)

        _threadsafe_event: Callable[[str, dict], None] | None
        if on_event is not None:

            def _threadsafe_event(evt: str, data: dict) -> None:
                loop.call_soon_threadsafe(on_event, evt, data)

        else:
            _threadsafe_event = None

        self._eval_offload_count += 1
        return await asyncio.to_thread(
            self._blocking_verificar_entrada,
            symbol,
            df_copy,
            estado_copy,
            _threadsafe_event,
        )

    def _blocking_verificar_entrada(
        self,
        symbol: str,
        df: pd.DataFrame,
        estado: Any,
        on_event: Callable[[str, dict], None] | None,
    ) -> dict[str, Any] | None:
        async def _runner() -> dict[str, Any] | None:
            return await TraderLite._execute_pipeline(self, symbol, df, estado, on_event)

        return asyncio.run(_runner())
    
    def _snapshot_offload_state(self, estado: Any) -> Any:
        """Devuelve una copia aislada de ``estado`` para evaluaciones offloaded."""

        if estado is None:
            return None
        if isinstance(estado, EstadoSimbolo):
            return self._clone_estado_simbolo(estado)
        try:
            return deepcopy(estado)
        except Exception:  # pragma: no cover - fallback defensivo
            return estado

    def _clone_estado_simbolo(self, estado: EstadoSimbolo) -> EstadoSimbolo:
        """Crea una copia superficial segura del ``EstadoSimbolo``."""

        buffer_items = [
            dict(item) if isinstance(item, dict) else item
            for item in estado.buffer
        ]
        buffer_copy = deque(buffer_items, maxlen=estado.buffer.maxlen)

        estrategias_items = [
            dict(item) if isinstance(item, dict) else item
            for item in estado.estrategias_buffer
        ]
        estrategias_copy = deque(
            estrategias_items,
            maxlen=estado.estrategias_buffer.maxlen,
        )

        indicadores_copy: dict[str, dict[str, float | None]] = {
            key: dict(value)
            for key, value in estado.indicadores_cache.items()
        }

        candle_filter = estado.candle_filter
        candle_filter_copy = CandleFilter(
            ultimo_timestamp=candle_filter.ultimo_timestamp,
            ultimo_close=candle_filter.ultimo_close,
            estadisticas=dict(candle_filter.estadisticas),
        )

        return EstadoSimbolo(
            buffer=buffer_copy,
            estrategias_buffer=estrategias_copy,
            ultimo_timestamp=estado.ultimo_timestamp,
            candle_filter=candle_filter_copy,
            indicadores_cache=indicadores_copy,
            fastpath_mode=estado.fastpath_mode,
        )

    def purge_historial_cierres(self, symbol: str | None = None) -> None:
        """Fuerza la poda del historial de cierres.

        Cuando ``symbol`` es ``None`` limpia todos los símbolos; en caso
        contrario solo aplica la poda al símbolo indicado para minimizar el
        impacto en ciclos críticos.
        """

        if symbol:
            self.historial_cierres.prune_symbol(symbol)
        else:
            self.historial_cierres.prune()

    def get_historial_cierres_stats(self) -> dict[str, Any]:
        """Devuelve métricas agregadas del historial para monitoreo."""

        self.historial_cierres.prune()
        return {
            "max_por_simbolo": self._historial_cierres_max,
            "ttl_segundos": self._historial_cierres_ttl,
            "por_simbolo": {symbol: len(store) for symbol, store in self.historial_cierres.items()},
            "total": self.historial_cierres.total_entries(),
        }
    
    def _after_procesar_vela(self, symbol: str) -> None:
        super_hook = getattr(super(), "_after_procesar_vela", None)
        if callable(super_hook):
            try:
                super_hook(symbol)
            except Exception:
                log.debug("No se pudo ejecutar hook base tras procesar vela", exc_info=True)

        if not self._historial_purge_enabled:
            return

        try:
            self.purge_historial_cierres(symbol=symbol)
        except Exception:
            log.debug(
                "No se pudo purgar historial de cierres tras procesar vela",
                extra={"symbol": symbol},
                exc_info=True,
            )

    def get_eval_offload_stats(self) -> dict[str, Any]:
        """Estadísticas del mecanismo de offload de evaluaciones."""

        return {
            "enabled": self._eval_offload_enabled,
            "threshold": self._eval_offload_min_df,
            "count": self._eval_offload_count,
        }
    
    def _collect_pipeline_diagnostics(self) -> dict[str, Any]:
        """Expone información de apoyo cuando el pipeline no está disponible."""

        pipeline = getattr(self, "_verificar_entrada", None)
        present = pipeline is not None
        callable_pipeline = callable(pipeline)
        required_flag = bool(getattr(self.config, "trader_require_pipeline", False))
        component_requirements = getattr(self, "_component_requirements", None)
        if isinstance(component_requirements, set):
            required_flag = required_flag or "verificar_entrada" in component_requirements

        diagnostics: dict[str, Any] = {
            "pipeline_present": present,
            "pipeline_callable": callable_pipeline,
            "pipeline_required": required_flag,
        }

        if callable_pipeline:
            return diagnostics

        error: Exception | None = None
        try:
            error = self.get_component_error("verificar_entrada")
        except Exception:
            error = None

        if error is not None:
            diagnostics["pipeline_error_type"] = type(error).__name__
            diagnostics["pipeline_error_message"] = str(error)

        return diagnostics

    async def verificar_entrada(
        self,
        symbol: str,
        df: pd.DataFrame,
        estado: Any,
        *,
        on_event: Callable[[str, dict], None] | None = None,
    ) -> dict[str, Any] | None:
        """Punto único para decidir entradas.

        Siempre existe para evitar advertencias por interfaces faltantes.
        Devuelve ``None`` cuando no hay pipeline disponible o la estrategia no
        produce una propuesta.
        """

        pipeline = getattr(self, "_verificar_entrada", None)
        captured_events: list[tuple[str, dict]] = []
        deferred_events: list[tuple[str, dict]] = []
        provider_used: str | None = None

        def _capture_event(evt: str, data: dict) -> None:
            captured_events.append((evt, data))

        def _drain(events: list[tuple[str, dict]]) -> None:
            if on_event is None or not events:
                return
            while events:
                evt, data = events.pop(0)
                try:
                    on_event(evt, data)
                except Exception:
                    pass

        pipeline_handler = _capture_event if on_event is not None else None
        if callable(pipeline):
            attempts: list[Callable[[], Any]] = []
            if on_event is not None:
                attempts.append(
                    lambda: pipeline(
                        self,
                        symbol,
                        df,
                        estado,
                        on_event=pipeline_handler,
                    )
                )
            attempts.append(lambda: pipeline(self, symbol, df, estado))
            if on_event is not None:
                attempts.append(
                    lambda: pipeline(
                        symbol,
                        df,
                        estado,
                        on_event=pipeline_handler,
                    )
                )
            attempts.append(lambda: pipeline(symbol, df, estado))
            if on_event is not None:
                attempts.append(lambda: pipeline(self, symbol, df, on_event=pipeline_handler))
                attempts.append(lambda: pipeline(symbol, df, on_event=pipeline_handler))
            attempts.append(lambda: pipeline(self, symbol, df))
            attempts.append(lambda: pipeline(symbol, df))

            for attempt in attempts:
                try:
                    result = attempt()
                except TypeError:
                    continue

                provider_used = "pipeline"
                resolved = await _maybe_await(result)
                if resolved is not None:
                    self._verificar_entrada_provider = provider_used
                    _drain(captured_events)
                    return resolved

            if captured_events:
                deferred_events = list(captured_events)
                captured_events.clear()

        engine = getattr(self, "engine", None)
        if engine is not None:
            for attr in ("verificar_entrada", "evaluar_condiciones_de_entrada"):
                fn = getattr(engine, attr, None)
                if not callable(fn):
                    continue

                provider_candidate = f"engine.{attr}"

                attempts = []
                if on_event is not None:
                    attempts.append(lambda: fn(symbol, df, estado, on_event=on_event))
                attempts.append(lambda: fn(symbol, df, estado))
                if on_event is not None:
                    attempts.append(
                        lambda: fn(self, symbol, df, estado, on_event=on_event)
                    )
                attempts.append(lambda: fn(self, symbol, df, estado))
                if on_event is not None:
                    attempts.append(lambda: fn(symbol, df, on_event=on_event))
                    attempts.append(lambda: fn(self, symbol, df, on_event=on_event))
                attempts.append(lambda: fn(symbol, df))
                attempts.append(lambda: fn(self, symbol, df))

                for attempt in attempts:
                    try:
                        result = attempt()
                    except TypeError:
                        continue
                    provider_used = provider_candidate
                    resolved = await _maybe_await(result)
                    if resolved is not None:
                        self._verificar_entrada_provider = provider_used
                        return resolved

        self._verificar_entrada_provider = provider_used
        _drain(deferred_events)
        return None

    # Compat helpers -------------------------------------------------------
    def enqueue_notification(self, mensaje: str, nivel: str = "INFO") -> None:
        if self.on_event:
            try:
                self.on_event("notify", {"mensaje": mensaje, "nivel": nivel})
            except Exception:
                pass

        manager = getattr(self, "notificador", None)
        if manager is None:
            return

        try:
            asyncio.get_running_loop()
        except RuntimeError:
            with contextlib.suppress(Exception):
                manager.enviar(mensaje, nivel)
            return

        task = asyncio.create_task(manager.enviar_async(mensaje, nivel))
        self._register_bg_task(task)

    def enqueue_persistence(self, tipo: str, datos: dict, *, immediate: bool = False) -> None:
        if self.on_event:
            payload = {"tipo": tipo, "datos": datos, "inmediato": immediate}
            try:
                self.on_event("persistencia", payload)
            except Exception:
                pass
