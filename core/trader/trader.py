"""Implementación completa del trader con compatibilidad histórica."""

from __future__ import annotations

import asyncio
import contextlib
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Dict, Optional, TYPE_CHECKING

import pandas as pd

from core.utils.log_utils import safe_extra
from core.utils.utils import configurar_logger

from ._utils import (
    _maybe_await,
    _normalize_timestamp,
    _reason_none,
    _silence_task_result,
    tf_seconds,
)
from .trader_lite import TraderLite

if TYPE_CHECKING:  # pragma: no cover
    from core.notification_manager import NotificationManager
    from core.supervisor import Supervisor


UTC = timezone.utc
log = configurar_logger("trader_modular", modo_silencioso=True)


class Trader(TraderLite):
    """Wrapper ligero que expone la interfaz histórica del bot."""

    def __init__(
        self,
        config: Any,
        *,
        candle_handler: Optional[Callable[[dict], Awaitable[None]]] = None,
        on_event: Optional[Callable[[str, dict], None]] = None,
        supervisor: Optional[Supervisor] = None,
    ) -> None:
        super().__init__(
            config,
            candle_handler=candle_handler,
            on_event=on_event,
            supervisor=supervisor,
        )
        self.modo_real = bool(getattr(config, "modo_real", False))
        self.cliente = self._cliente
        self.data_feed = self.feed
        self.historial_cierres: Dict[str, dict] = {s: {} for s in config.symbols}
        self.fecha_actual = datetime.now(UTC).date()
        self.estrategias_habilitadas = False
        self._eval_enabled: Dict[tuple[str, str], bool] = {}
        self._last_eval_skip_reason: str | None = None
        self._last_eval_skip_details: Dict[str, Any] | None = None
        self._bg_tasks: set[asyncio.Task] = set()
        self.notificador: NotificationManager | None = None
        self._verificar_entrada_provider: str | None = None

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

        if getattr(self, "_OrderManager", None) and getattr(self, "bus", None) is not None:
            self.orders = self._OrderManager(self.modo_real, self.bus)
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

        on_event_cb: Callable[[str, dict], None] | None = None
        if hasattr(self, "_emit"):
            on_event_cb = self._emit
        elif callable(self.on_event):
            on_event_cb = self.on_event

        try:
            resultado = await self.verificar_entrada(
                symbol,
                df,
                estado,
                on_event=on_event_cb,
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
        self._last_eval_skip_reason = "no_signal" if provider else "pipeline_missing"
        self._last_eval_skip_details = {
            "timeframe": timeframe_str,
            "buffer_len": buf_len,
            "min_needed": min_bars,
            "provider": provider,
        }
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
                }
            ),
        )
        return None

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

        def _capture_event(evt: str, data: dict) -> None:
            captured_events.append((evt, data))

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

                resolved = await _maybe_await(result)
                if resolved is not None:
                    self._verificar_entrada_provider = "pipeline"
                    if on_event is not None and captured_events:
                        for evt, data in captured_events:
                            try:
                                on_event(evt, data)
                            except Exception:
                                pass
                        captured_events.clear()
                    return resolved

            self._verificar_entrada_provider = None

        engine = getattr(self, "engine", None)
        if engine is not None:
            for attr in ("verificar_entrada", "evaluar_condiciones_de_entrada"):
                fn = getattr(engine, attr, None)
                if not callable(fn):
                    continue

                self._verificar_entrada_provider = f"engine.{attr}"

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
                    resolved = await _maybe_await(result)
                    if on_event is not None and captured_events:
                        for evt, data in captured_events:
                            try:
                                on_event(evt, data)
                            except Exception:
                                pass
                        captured_events.clear()
                    return resolved

        self._verificar_entrada_provider = None
        if on_event is not None and captured_events:
            for evt, data in captured_events:
                try:
                    on_event(evt, data)
                except Exception:
                    pass
            captured_events.clear()
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
        task.add_done_callback(_silence_task_result)

    def enqueue_persistence(self, tipo: str, datos: dict, *, immediate: bool = False) -> None:
        if self.on_event:
            payload = {"tipo": tipo, "datos": datos, "inmediato": immediate}
            try:
                self.on_event("persistencia", payload)
            except Exception:
                pass
