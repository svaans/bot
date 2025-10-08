"""Implementación modular mínima del núcleo del trader."""

from __future__ import annotations

import asyncio
import contextlib
import inspect
import os
from types import SimpleNamespace
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple, TYPE_CHECKING

from binance_api.cliente import fetch_ohlcv_async
from core.backfill_service import BackfillService
from core.metrics import (
    BACKFILL_DURATION_SECONDS,
    BACKFILL_GAPS_FOUND_TOTAL,
    BACKFILL_KLINES_FETCHED_TOTAL,
    BACKFILL_REQUESTS_TOTAL,
    BUFFER_SIZE_V2,
)
from core.procesar_vela import get_buffer_manager
from core.utils.timeframes import tf_to_ms

from core.utils.log_utils import safe_extra
from core.utils.utils import configurar_logger

from ._utils import EstadoSimbolo, _is_awaitable, _maybe_await

if TYPE_CHECKING:  # pragma: no cover
    from core.supervisor import Supervisor


log = configurar_logger("trader_modular", modo_silencioso=True)


def _compat_module():
    from core import trader_modular as compat

    return compat


def _data_feed_cls():
    return getattr(_compat_module(), "DataFeed")


def _supervisor_cls():
    return getattr(_compat_module(), "Supervisor")


def _crear_cliente_factory():
    return getattr(_compat_module(), "crear_cliente", None)


class TraderLite:
    """Orquesta DataFeedLite y delega el procesamiento de velas.

    Parámetros
    ----------
    config: objeto con al menos:
        - symbols: list[str]
        - intervalo_velas: str (p. ej. "1m")
        - modo_real: bool
    candle_handler: async callable(dict) -> None
        - Función que procesará cada vela cerrada (p. ej. tu `procesar_vela`).
        - Si no se provee y existe `core.procesar_vela.procesar_vela`, se intentará usarla.
    on_event: callable(evt: str, data: dict) -> None | None
        - Hook opcional para métricas / notificaciones.
    supervisor: SupervisorLite | None
        - Si no se provee, se crea uno interno.
    """

    def __init__(
        self,
        config: Any,
        *,
        candle_handler: Optional[Callable[[dict], Awaitable[None]]] = None,
        on_event: Optional[Callable[[str, dict], None]] = None,
        supervisor: Optional[Supervisor] = None,
    ) -> None:
        if not getattr(config, "symbols", None):
            raise ValueError("config.symbols vacío o no definido")
        if not getattr(config, "intervalo_velas", None):
            raise ValueError("config.intervalo_velas no definido")

        self.config = config
        self.on_event = on_event
        supervisor_cls = _supervisor_cls()
        self.supervisor = supervisor or supervisor_cls(on_event=on_event)

        log.info(
            "Fastpath trader activo",
            extra=safe_extra(
                {
                    "event": "config.fastpath",
                    "trader_fastpath_enabled": bool(getattr(config, "trader_fastpath_enabled", True)),
                    "trader_fastpath_threshold": int(getattr(config, "trader_fastpath_threshold", 350)),
                    "trader_fastpath_recovery": int(getattr(config, "trader_fastpath_recovery", 200)),
                    "trader_fastpath_skip_notifications": bool(
                        getattr(config, "trader_fastpath_skip_notifications", True)
                    ),
                    "trader_fastpath_skip_entries": bool(
                        getattr(config, "trader_fastpath_skip_entries", False)
                    ),
                    "trader_fastpath_skip_trend": bool(
                        getattr(config, "trader_fastpath_skip_trend", True)
                    ),
                }
            ),
        )

        # Estado por símbolo
        self.estado: Dict[str, EstadoSimbolo] = {s: EstadoSimbolo() for s in config.symbols}
        # Registro de tendencia por símbolo (utilizado por procesar_vela)
        self.estado_tendencia: Dict[str, Any] = {s: None for s in config.symbols}
        self._last_evaluated_bar: Dict[tuple[str, str], int] = {}
        self._eval_enabled: Dict[tuple[str, str], bool] = {}
        self._buffer_manager: Any | None = None
        self._backfill_service: BackfillService | None = None
        self._backfill_task: Optional[asyncio.Task] = None
        self._backfill_ready_flags: Dict[Tuple[str, str], bool] = {}
        self._backfill_mode = os.getenv("BACKFILL_MODE", "A").upper()
        self._backfill_min_needed = int(os.getenv("BACKFILL_MIN_NEEDED", "400"))
        self._backfill_warmup_extra = int(os.getenv("BACKFILL_WARMUP_EXTRA", "300"))
        self._backfill_headroom = int(os.getenv("BACKFILL_HEADROOM", "200"))
        self._backfill_enabled = os.getenv("BACKFILL_ENABLED", "true").lower() != "false"
        log.info(
            "Backfill trader configurado",
            extra=safe_extra(
                {
                    "event": "config.backfill",
                    "backfill_mode": self._backfill_mode,
                    "backfill_min_needed": self._backfill_min_needed,
                    "backfill_warmup_extra": self._backfill_warmup_extra,
                    "backfill_headroom": self._backfill_headroom,
                    "backfill_enabled": self._backfill_enabled,
                }
            ),
        )
        self._init_backfill_service()

        # Protección dinámica ante spreads amplios (lazy import abajo).
        self.spread_guard: Any | None = None

        # Handler de velas
        self._handler = candle_handler or self._descubrir_handler_default()
        if not asyncio.iscoroutinefunction(self._handler):
            raise TypeError("candle_handler debe ser async (async def …)")
        self._handler_invoker = self._build_handler_invoker(self._handler)

        # DataFeedLite
        data_feed_cls = _data_feed_cls()
        self.feed = data_feed_cls(
            config.intervalo_velas,
            on_event=on_event,
            **self._resolve_data_feed_kwargs(),
        )
        # Señal para el StartupManager: este feed se arranca desde TraderLite
        try:
            setattr(self.feed, "_managed_by_trader", True)
        except Exception:
            log.debug("No se pudo marcar DataFeed como gestionado por Trader")

        self._datafeed_connected_emitted = False
        self._ws_started_logged = False
        self._connection_signal_task: asyncio.Task | None = None

        # Cliente de exchange (solo modo real)
        self._cliente = None
        crear_cliente_fn = _crear_cliente_factory()
        if bool(getattr(config, "modo_real", False)) and crear_cliente_fn is not None:
            try:
                self._cliente = crear_cliente_fn(config)
            except Exception:
                # no bloquea el arranque en simulado
                self._cliente = None

        # Tareas
        self._stop_event = asyncio.Event()
        self._runner_task: Optional[asyncio.Task] = None

        # --- Lazy imports de módulos pesados / acoplados ---
        # Se difieren hasta ahora para evitar ciclos.
        try:
            from core.risk import SpreadGuard as _SpreadGuard
            self._SpreadGuard = _SpreadGuard
        except Exception:
            self._SpreadGuard = None
        try:
            from core.event_bus import EventBus as _EventBus
            from core.orders.order_manager import OrderManager as _OrderManager
            from core.orders.storage_simulado import sincronizar_ordenes_simuladas as _sync_sim
            from core.strategies import StrategyEngine as _StrategyEngine
            from core.persistencia_tecnica import PersistenciaTecnica as _PersistenciaTecnica
            from core.strategies.entry.verificar_entradas import verificar_entrada as _verificar_entrada
            self._EventBus = _EventBus
            self._OrderManager = _OrderManager
            self._sync_sim = _sync_sim
            self._StrategyEngine = _StrategyEngine
            self._PersistenciaTecnica = _PersistenciaTecnica
            self._verificar_entrada = _verificar_entrada
        except Exception:
            # Se inicializarán a None; el wrapper Trader aplicará guardas.
            self._EventBus = None
            self._OrderManager = None
            self._sync_sim = None
            self._StrategyEngine = None
            self._PersistenciaTecnica = None
            self._verificar_entrada = None

        # Configurar guardia de spread (puede devolver None si está deshabilitado).
        self.spread_guard = self._create_spread_guard()

    def _resolve_data_feed_kwargs(self) -> Dict[str, Any]:
        """Obtiene parámetros del ``DataFeed`` combinando config y entorno."""

        def _resolve(
            attr: str | None,
            env_key: str,
            default: Any,
            caster: Callable[[Any], Any],
        ) -> Any:
            if attr and hasattr(self.config, attr):
                value = getattr(self.config, attr)
                if value is not None:
                    try:
                        return caster(value)
                    except (TypeError, ValueError):
                        log.warning(
                            "Valor inválido para %s=%r en Config; usando fallback",
                            attr,
                            value,
                        )
            env_value = os.getenv(env_key)
            if env_value is not None:
                try:
                    return caster(env_value)
                except (TypeError, ValueError):
                    log.warning(
                        "Valor inválido para %s=%r en entorno; usando %r",
                        env_key,
                        env_value,
                        default,
                    )
            return caster(default)

        kwargs: Dict[str, Any] = {
            "handler_timeout": _resolve("handler_timeout", "DF_HANDLER_TIMEOUT_SEC", 2.0, float),
            "inactivity_intervals": _resolve("inactivity_intervals", "DF_INACTIVITY_INTERVALS", 10, int),
            "queue_max": _resolve("df_queue_default_limit", "DF_QUEUE_MAX", 2000, int),
            "queue_policy": _resolve("df_queue_policy", "DF_QUEUE_POLICY", "drop_oldest", lambda x: str(x).lower()),
            "monitor_interval": _resolve("monitor_interval", "DF_MONITOR_INTERVAL", 5.0, float),
            "cancel_timeout": _resolve(None, "DF_CANCEL_TIMEOUT", 5.0, float),
        }
        kwargs["backpressure"] = self._resolve_backpressure()
        return kwargs

    def _resolve_backpressure(self) -> bool:
        valor = getattr(self.config, "df_backpressure", None)
        if valor is not None:
            return bool(valor)
        raw = os.getenv("DF_BACKPRESSURE")
        if raw is None:
            return True
        return str(raw).lower() == "true"
    
    # --------------------------- backfill ---------------------------
    def _init_backfill_service(self) -> None:
        if not self._backfill_enabled:
            return

        try:
            metrics = SimpleNamespace(
                backfill_requests_total=BACKFILL_REQUESTS_TOTAL,
                backfill_klines_fetched_total=BACKFILL_KLINES_FETCHED_TOTAL,
                backfill_duration_seconds=BACKFILL_DURATION_SECONDS,
                backfill_gaps_found_total=BACKFILL_GAPS_FOUND_TOTAL,
                buffer_size_v2=BUFFER_SIZE_V2,
            )
            self._backfill_service = BackfillService(
                fetch_klines=self._fetch_klines_for_backfill,
                buffer_append_many=self._buffer_append_many,
                buffer_get=self._buffer_get,
                set_ready_flag=self._backfill_set_ready,
                logger=configurar_logger("backfill_service"),
                metrics=metrics,
                headroom=self._backfill_headroom,
            )
        except Exception:
            self._backfill_service = None
            self._backfill_enabled = False
            log.exception("No se pudo inicializar BackfillService; se deshabilita")

    async def start_backfill(self) -> None:
        if not self._backfill_service:
            return

        symbols = [s.upper() for s in self.config.symbols]
        try:
            if self._backfill_mode == "B":
                if self._backfill_task and not self._backfill_task.done():
                    return
                self._backfill_task = asyncio.create_task(
                    self._backfill_service.run(
                        symbols,
                        self.config.intervalo_velas,
                        self._backfill_min_needed,
                        self._backfill_warmup_extra,
                        mode="B",
                    ),
                    name="backfill_service",
                )
                self._backfill_task.add_done_callback(self._backfill_task_done)
            else:
                await self._backfill_service.run(
                    symbols,
                    self.config.intervalo_velas,
                    self._backfill_min_needed,
                    self._backfill_warmup_extra,
                    mode="A",
                )
        except Exception:
            log.exception("Error ejecutando backfill inicial")
            raise

    def _backfill_task_done(self, task: asyncio.Task) -> None:
        try:
            task.result()
        except Exception:
            log.exception("Backfill en paralelo finalizó con error")

    def _backfill_set_ready(self, symbol: str, timeframe: str, ready: bool) -> None:
        key = (symbol.upper(), timeframe.lower())
        self._backfill_ready_flags[key] = bool(ready)

    def is_symbol_ready(self, symbol: str, timeframe: Optional[str] = None) -> bool:
        if not self._backfill_service or not self._backfill_enabled:
            return True
        if not self._backfill_ready_flags and self._backfill_mode != "B":
            return True
        tf = timeframe or getattr(self.config, "intervalo_velas", "")
        if not tf or tf == "unknown":
            tf = getattr(self.config, "intervalo_velas", "")
        key = (symbol.upper(), str(tf).lower())
        if key in self._backfill_ready_flags:
            return self._backfill_ready_flags[key]
        return self._backfill_service.is_ready(symbol, str(tf))

    def _buffer_append_many(self, symbol: str, timeframe: str, candles: List[Dict[str, Any]]) -> None:
        manager = self._get_buffer_manager()
        manager.extend(symbol, candles)

    def _buffer_get(self, symbol: str, timeframe: str) -> List[Dict[str, Any]]:
        manager = self._get_buffer_manager()
        return manager.snapshot(symbol)

    def _get_buffer_manager(self):
        if self._buffer_manager is None:
            self._buffer_manager = get_buffer_manager()
        return self._buffer_manager

    async def _fetch_klines_for_backfill(
        self,
        symbol: str,
        timeframe: str,
        start: int,
        end: int,
        limit: int,
    ) -> List[Dict[str, Any]]:
        since = int(start)
        tf_ms = tf_to_ms(timeframe)
        raw = await fetch_ohlcv_async(self._cliente, symbol, timeframe, since=since, limit=limit)
        max_open = end - tf_ms
        result: List[Dict[str, Any]] = []
        for entry in raw:
            if isinstance(entry, dict):
                open_time = entry.get("open_time") or entry.get("openTime") or entry.get("timestamp")
                open_price = entry.get("open") or entry.get("o")
                high_price = entry.get("high") or entry.get("h")
                low_price = entry.get("low") or entry.get("l")
                close_price = entry.get("close") or entry.get("c")
                volume = entry.get("volume") or entry.get("v")
            else:
                if not entry or len(entry) < 6:
                    continue
                open_time, open_price, high_price, low_price, close_price, volume = entry[:6]
            try:
                open_ts = int(float(open_time))
            except (TypeError, ValueError):
                continue
            if open_ts < start or open_ts > max_open:
                continue
            result.append(
                {
                    "symbol": symbol.upper(),
                    "timeframe": timeframe,
                    "open_time": open_ts,
                    "open": float(open_price),
                    "high": float(high_price),
                    "low": float(low_price),
                    "close": float(close_price),
                    "volume": float(volume),
                    "close_time": open_ts + tf_ms - 1,
                }
            )
        return result

    @staticmethod
    def _extract_symbol(candle: dict, *, fallback: str = "") -> str:
        if not isinstance(candle, dict):
            return str(fallback).upper()
        for key in ("symbol", "s"):
            value = candle.get(key)
            if value:
                return str(value).upper()
        return str(fallback).upper()

    @staticmethod
    def _extract_timestamp(candle: dict) -> Any:
        if not isinstance(candle, dict):
            return None
        for key in ("timestamp", "close_time", "closeTime", "open_time", "openTime"):
            value = candle.get(key)
            if value is None:
                continue
            try:
                return int(value)
            except (TypeError, ValueError):
                try:
                    return float(value)
                except (TypeError, ValueError):
                    return value
        return None

    @staticmethod
    def _set_skip_reason(candle: dict, reason: str | None, details: Optional[dict] = None) -> None:
        if not isinstance(candle, dict):
            return
        if reason:
            candle["_df_skip_reason"] = reason
            if details:
                candle["_df_skip_details"] = details
            elif "_df_skip_details" in candle:
                candle.pop("_df_skip_details", None)
        else:
            candle.pop("_df_skip_reason", None)
            candle.pop("_df_skip_details", None)

    def _update_estado_con_candle(self, candle: dict) -> bool:
        """Actualiza buffers/estado y decide si se procesa la vela."""

        sym = self._extract_symbol(candle)
        ts = self._extract_timestamp(candle)
        self._set_skip_reason(candle, None)
        log.debug(
            "update_estado.enter",
            extra=safe_extra(
                {
                    "symbol": sym,
                    "timestamp": ts,
                    "stage": "Trader._update_estado_con_candle",
                }
            ),
        )
        if not sym or sym not in self.estado:
            log.debug(
                "update_estado.exit",
                extra=safe_extra(
                    {
                        "symbol": sym,
                        "timestamp": ts,
                        "stage": "Trader._update_estado_con_candle",
                        "result": "passthrough",
                    }
                ),
            )
            return True
        
        timeframe_raw = (
            candle.get("timeframe")
            or candle.get("interval")
            or candle.get("tf")
            or getattr(self.config, "intervalo_velas", None)
        )
        timeframe = str(timeframe_raw or "") or str(getattr(self.config, "intervalo_velas", ""))
        candle["symbol"] = sym
        if timeframe:
            candle["timeframe"] = timeframe

        if self._backfill_service is not None:
            should_process, normalized = self._backfill_service.handle_live_candle(sym, timeframe, candle)
            if not should_process:
                self._set_skip_reason(candle, "backfill_not_ready")
                log.debug(
                    "update_estado.skip",
                    extra=safe_extra(
                        {
                            "symbol": sym,
                            "timestamp": ts,
                            "stage": "Trader._update_estado_con_candle",
                            "reason": "backfill_not_ready",
                        }
                    ),
                )
                log.debug(
                    "update_estado.exit",
                    extra=safe_extra(
                        {
                            "symbol": sym,
                            "timestamp": ts,
                            "stage": "Trader._update_estado_con_candle",
                            "result": "skipped",
                        }
                    ),
                )
                return False
            if normalized is not None:
                for key in ("open_time", "close_time", "timeframe", "timestamp"):
                    if key not in candle and key in normalized:
                        candle[key] = normalized[key]
                timeframe = str(candle.get("timeframe", timeframe))
                ts = self._extract_timestamp(candle)

        if not self.is_symbol_ready(sym, timeframe):
            self._set_skip_reason(candle, "symbol_not_ready")
            log.debug(
                "update_estado.skip",
                extra=safe_extra(
                    {
                        "symbol": sym,
                        "timestamp": ts,
                        "stage": "Trader._update_estado_con_candle",
                        "reason": "symbol_not_ready",
                    }
                ),
            )
            log.debug(
                "update_estado.exit",
                extra=safe_extra(
                    {
                        "symbol": sym,
                        "timestamp": ts,
                        "stage": "Trader._update_estado_con_candle",
                        "result": "skipped",
                    }
                ),
            )
            return False

        estado = self.estado[sym]
        if not estado.candle_filter.accept(candle):
            stats = dict(estado.candle_filter.estadisticas)
            self._set_skip_reason(candle, "candle_filter", {"stats": stats})
            log.debug(
                "update_estado.skip",
                extra=safe_extra(
                    {
                        "symbol": sym,
                        "timestamp": ts,
                        "stage": "Trader._update_estado_con_candle",
                        "reason": "candle_filter",
                        "estadisticas": stats,
                    }
                ),
            )
            log.debug(
                "update_estado.exit",
                extra=safe_extra(
                    {
                        "symbol": sym,
                        "timestamp": ts,
                        "stage": "Trader._update_estado_con_candle",
                        "result": "skipped",
                    }
                ),
            )
            return False

        estado.ultimo_timestamp = candle.get("timestamp", estado.ultimo_timestamp)
        estado.buffer.append(candle)
        try:
            self.supervisor.tick_data(sym)
        except Exception:
            log.exception("Error notificando tick_data al supervisor")
        ts = self._extract_timestamp(candle)
        log.debug(
            "update_estado.exit",
            extra=safe_extra(
                {
                    "symbol": sym,
                    "timestamp": ts,
                    "stage": "Trader._update_estado_con_candle",
                    "result": "accepted",
                }
            ),
        )
        return True

    # -------------------- API pública --------------------
    def start(self) -> None:
        """Arranca supervisor y la tarea principal del Trader."""
        # Soporta start_supervision sync/async
        try:
            res = self.supervisor.start_supervision()
            if _is_awaitable(res):
                asyncio.create_task(res, name="supervisor_start")
        except Exception:
            log.exception("Error iniciando supervisor (start_supervision)")
        if self._runner_task is None or self._runner_task.done():
            self._runner_task = asyncio.create_task(self._run(), name="trader_main")
        self._ensure_connection_signal_task()

    async def stop(self) -> None:
        """Solicita parada ordenada y espera cierre del feed."""
        self._stop_event.set()
        if self._connection_signal_task is not None:
            self._connection_signal_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self._connection_signal_task
            self._connection_signal_task = None
        # Cierre del feed: soporta detener() sync/async/ausente
        try:
            detener = getattr(self.feed, "detener", None)
            if callable(detener):
                await _maybe_await(detener())
        except Exception:
            log.exception("Error deteniendo DataFeed")
        # Cierre del supervisor: sync/async
        try:
            await _maybe_await(self.supervisor.shutdown())
        except Exception:
            log.exception("Error cerrando supervisor")
        if self._runner_task:
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self._runner_task

    # ------------------- utilidades internas -------------------
    def _create_spread_guard(self) -> Any | None:
        """Instancia ``SpreadGuard`` cuando la configuración lo habilita."""
        base_limit = float(getattr(self.config, "max_spread_ratio", 0.0) or 0.0)
        dynamic_enabled = bool(getattr(self.config, "spread_dynamic", False))
        if not dynamic_enabled or base_limit <= 0:
            return None

        window = int(getattr(self.config, "spread_guard_window", 50) or 50)
        hysteresis = float(getattr(self.config, "spread_guard_hysteresis", 0.15) or 0.15)
        max_limit_default = base_limit * 5 if base_limit > 0 else 0.05
        max_limit = float(
            getattr(self.config, "spread_guard_max_limit", max_limit_default) or max_limit_default
        )

        # Normalizar parámetros para evitar valores inválidos.
        window = max(5, window)
        hysteresis = min(max(hysteresis, 0.0), 1.0)
        max_limit = max(base_limit, max_limit)

        try:
            if self._SpreadGuard is None:
                return None
            guard = self._SpreadGuard(
                base_limit=base_limit,
                max_limit=max_limit,
                window=window,
                hysteresis=hysteresis,
            )
        except Exception:
            log.exception("No se pudo inicializar SpreadGuard; se usará el límite estático")
            return None

        log.debug(
            "SpreadGuard habilitado",
            extra={
                "base_limit": base_limit,
                "max_limit": max_limit,
                "window": window,
                "hysteresis": hysteresis,
            },
        )
        return guard

    # ------------------- ciclo principal -------------------
    async def _run(self) -> None:
        symbols = list({s.upper() for s in self.config.symbols})
        self._emit("trader_start", {"symbols": symbols, "intervalo": self.config.intervalo_velas})

        async def _handler(c: dict) -> None:
            sym = self._extract_symbol(c)
            ts = self._extract_timestamp(c)
            self._maybe_emit_datafeed_connected(sym, ts)
            handler_ref = getattr(self, "_handler", None)
            handler_id = id(handler_ref) if handler_ref is not None else None
            handler_line = (
                getattr(getattr(handler_ref, "__code__", None), "co_firstlineno", None)
                if handler_ref is not None
                else None
            )
            handler_qualname = getattr(handler_ref, "__qualname__", None) if handler_ref else None
            if hasattr(self, "feed") and hasattr(self.feed, "_note_handler_log"):
                try:
                    self.feed._note_handler_log()
                except Exception:
                    pass
            log.debug(
                "handler.enter",
                extra=safe_extra(
                    {
                        "symbol": sym,
                        "timestamp": ts,
                        "stage": "Trader._handler",
                        "handler_id": handler_id,
                        "handler_qualname": handler_qualname,
                        "handler_line": handler_line,
                    }
                ),
            )
            outcome = "accepted"
            try:
                should_process = self._update_estado_con_candle(c)
                if not should_process:
                    outcome = "skipped"
                    return
                await self._procesar_vela(c)
                outcome = "processed"
            except Exception:
                outcome = "error"
                raise
            finally:
                log.debug(
                    "handler.exit",
                    extra=safe_extra(
                        {
                            "symbol": sym,
                            "timestamp": ts,
                            "stage": "Trader._handler",
                            "result": outcome,
                            "handler_id": handler_id,
                            "handler_qualname": handler_qualname,
                            "handler_line": handler_line,
                        }
                    ),
                )

        # Registrar latidos periódicos
        self.supervisor.supervised_task(
            lambda: self._heartbeat_loop(), name="heartbeat_loop", expected_interval=60
        )

        # Lanzar DataFeedLite supervisado
        log.info(
            "trader:starting_ws",
            extra={
                "symbols": symbols,
                "intervalo": getattr(self.config, "intervalo_velas", None),
                "stage": "Trader",
            },
        )
        self.supervisor.supervised_task(
            lambda: self.feed.escuchar(symbols, _handler, cliente=self._cliente),
            name="data_feed",
            expected_interval=int(os.getenv("DF_EXPECTED_INTERVAL", "60")),
        )

        # Bucle de espera hasta stop
        while not self._stop_event.is_set():
            await asyncio.sleep(0.5)
        self._emit("trader_stop", {})

    async def _heartbeat_loop(self, interval: int = 60) -> None:
        while not self._stop_event.is_set():
            self.supervisor.beat("trader")
            await asyncio.sleep(interval)

    # --------------------- utilidades ---------------------
    def _emit(self, evt: str, data: dict) -> None:
        if self.on_event:
            try:
                self.on_event(evt, data)
            except Exception:
                pass

    def _maybe_emit_datafeed_connected(self, symbol: str | None, ts: int | None) -> None:
        if self._datafeed_connected_emitted:
            return
        bus = getattr(self, "event_bus", None) or getattr(self, "bus", None)
        payload: Dict[str, Any] = {}
        if symbol:
            payload["symbol"] = symbol
        else:
            try:
                symbols = [s.upper() for s in getattr(self.config, "symbols", [])]
            except Exception:
                symbols = []
            if symbols:
                payload["symbols"] = symbols
        if ts is not None:
            payload["timestamp"] = ts
        log_payload: Dict[str, Any] = {
            "intervalo": getattr(self.config, "intervalo_velas", None),
        }
        log_payload.update(payload)
        if not self._ws_started_logged:
            log.info(
                "trader:ws_started",
                extra={**log_payload, "stage": "Trader"},
            )
            self._ws_started_logged = True
        if bus is None:
            return
        emit = getattr(bus, "emit", None)
        if callable(emit):
            try:
                emit("datafeed_connected", payload)
                self._datafeed_connected_emitted = True
                return
            except Exception:
                log.debug("No se pudo emitir evento datafeed_connected", exc_info=True)
        publish = getattr(bus, "publish", None)
        if callable(publish):
            try:
                asyncio.create_task(publish("datafeed_connected", payload))
                self._datafeed_connected_emitted = True
            except Exception:
                log.debug("No se pudo publicar evento datafeed_connected", exc_info=True)

    def _ensure_connection_signal_task(self) -> None:
        if self._connection_signal_task is not None and not self._connection_signal_task.done():
            return
        feed = getattr(self, "feed", None)
        if feed is None:
            return
        evt = getattr(feed, "ws_connected_event", None)
        if evt is None or not hasattr(evt, "wait"):
            return

        failed_evt = getattr(feed, "ws_failed_event", None)

        async def _await_ws_connected() -> None:
            waiters: list[asyncio.Task[Any]] = []
            labels: dict[asyncio.Task[Any], str] = {}
            try:
                connected_wait = evt.wait()
                if not _is_awaitable(connected_wait):
                    return
                connected_task = asyncio.create_task(connected_wait)
                waiters.append(connected_task)
                labels[connected_task] = "connected"

                if failed_evt is not None and hasattr(failed_evt, "wait"):
                    failed_wait = failed_evt.wait()
                    if _is_awaitable(failed_wait):
                        failed_task = asyncio.create_task(failed_wait)
                        waiters.append(failed_task)
                        labels[failed_task] = "failed"

                stop_wait = self._stop_event.wait()
                if _is_awaitable(stop_wait):
                    stop_task = asyncio.create_task(stop_wait)
                    waiters.append(stop_task)
                    labels[stop_task] = "stopped"

                done, _ = await asyncio.wait(waiters, return_when=asyncio.FIRST_COMPLETED)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.debug("No se pudo esperar ws_connected_event", exc_info=True)
                return
            finally:
                pending = [task for task in waiters if not task.done()]
                for task in pending:
                    task.cancel()
                if pending:
                    with contextlib.suppress(Exception):
                        await asyncio.gather(*pending, return_exceptions=True)

            outcome = next((labels.get(task) for task in done if task in labels), None)
            if outcome == "connected":
                self._maybe_emit_datafeed_connected(None, None)
            elif outcome == "failed":
                reason = getattr(feed, "_ws_failure_reason", None)
                if reason:
                    log.debug(
                        "datafeed_connected_watch: ws_failed_event activado (%s)",
                        reason,
                    )
                else:
                    log.debug(
                        "datafeed_connected_watch: ws_failed_event activado"
                    )

        self._connection_signal_task = asyncio.create_task(
            _await_ws_connected(), name="datafeed_connected_watch"
        )

    def _descubrir_handler_default(self) -> Callable[[dict], Awaitable[None]]:
        """Intento suave de usar `core.procesar_vela.procesar_vela` si existe.
        Si no, devuelve un placeholder async (no-op).
        """
        try:  # pragma: no cover
            from core.procesar_vela import procesar_vela  # tu pipeline existente
            if asyncio.iscoroutinefunction(procesar_vela):
                return procesar_vela
        except Exception:
            pass

        async def _placeholder(_: dict) -> None:
            # No hace nada: pensado para pruebas de arranque
            return None

        return _placeholder

    def _build_handler_invoker(
        self, handler: Callable[..., Awaitable[None]]
    ) -> Callable[[dict], Awaitable[None]]:
        """Adapta firmas comunes de `candle_handler`.

        Permite reutilizar funciones existentes que esperan `(trader, vela)` o
        únicamente la `vela`. Si el handler requiere argumentos adicionales sin
        valores por defecto, se notifica explícitamente para evitar fallos
        silenciosos.
        """
        signature = inspect.signature(handler)
        trader_aliases = {"trader", "bot", "manager"}
        candle_aliases = ("vela", "candle", "kline", "bar", "candle_data")

        positional_params = [
            p
            for p in signature.parameters.values()
            if p.kind in (
                inspect.Parameter.POSITIONAL_ONLY,
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
            )
        ]

        first_positional_name = positional_params[0].name if positional_params else None
        needs_trader = any(alias in signature.parameters for alias in trader_aliases)
        # Heurística: si hay ≥2 posicionales y el primero NO parece ser la vela, asumimos (trader, vela)
        if not needs_trader and len(positional_params) >= 2:
            if first_positional_name not in candle_aliases:
                needs_trader = True

        async def _invoke(candle: dict) -> None:
            args: List[Any] = []
            kwargs: Dict[str, Any] = {}
            candle_assigned = False

            for name, param in signature.parameters.items():
                if param.kind == inspect.Parameter.VAR_POSITIONAL:
                    continue
                if name == "self":
                    continue
                if name in trader_aliases:
                    if param.kind in (
                        inspect.Parameter.POSITIONAL_ONLY,
                        inspect.Parameter.POSITIONAL_OR_KEYWORD,
                    ):
                        args.append(self)
                    else:
                        kwargs[name] = self
                    continue
                if name in candle_aliases:
                    if param.kind in (
                        inspect.Parameter.POSITIONAL_ONLY,
                        inspect.Parameter.POSITIONAL_OR_KEYWORD,
                    ):
                        args.append(candle)
                    else:
                        kwargs[name] = candle
                    candle_assigned = True
                    continue
                if not candle_assigned and param.kind in (
                    inspect.Parameter.POSITIONAL_ONLY,
                    inspect.Parameter.POSITIONAL_OR_KEYWORD,
                ) and param.default is inspect._empty:
                    args.append(candle)
                    candle_assigned = True
                    continue
                if (
                    not candle_assigned
                    and param.kind == inspect.Parameter.KEYWORD_ONLY
                    and param.default is inspect._empty
                ):
                    kwargs[name] = candle
                    candle_assigned = True
                    continue
                if (
                    param.default is inspect._empty
                    and param.kind
                    not in (
                        inspect.Parameter.VAR_POSITIONAL,
                        inspect.Parameter.VAR_KEYWORD,
                    )
                ):
                    raise TypeError(
                        "candle_handler requiere parámetro obligatorio "
                        f"'{name}' sin valor por defecto; usa un partial o "
                        "ajusta la firma para incluir solo (trader, vela)."
                    )

            if needs_trader and all(alias not in signature.parameters for alias in trader_aliases):
                # Si inferimos que necesita trader por la cantidad de posicionales,
                # lo inyectamos al inicio.
                args.insert(0, self)

            if not candle_assigned:
                if any(p.kind == inspect.Parameter.VAR_KEYWORD for p in signature.parameters.values()):
                    kwargs.setdefault("vela", candle)
                    candle_assigned = True
                else:
                    raise TypeError(
                        "candle_handler no define un parámetro para la vela; "
                        "asegúrate de aceptar (trader, vela) o solo la vela."
                    )

            await handler(*args, **kwargs)

        return _invoke

    async def _procesar_vela(self, candle: dict) -> None:
        """Procesa la vela aceptada por ``_update_estado_con_candle``."""

        sym = self._extract_symbol(candle)
        ts = self._extract_timestamp(candle)
        timeframe = (
            candle.get("timeframe")
            or candle.get("interval")
            or candle.get("tf")
            or getattr(self.config, "intervalo_velas", None)
        )
        tf_str = str(timeframe) if timeframe else None
        log.debug(
            "procesar_vela.enter",
            extra=safe_extra(
                {
                    "symbol": sym,
                    "timestamp": ts,
                    "timeframe": tf_str,
                    "stage": "Trader._procesar_vela",
                }
            ),
        )
        outcome = "ok"
        try:
            await self._handler_invoker(candle)
        except Exception:
            outcome = "error"
            raise
        else:
            skip_reason = candle.get("_df_skip_reason")
            if skip_reason == "warmup":
                log.debug(
                    "procesar_vela.skip",
                    extra=safe_extra(
                        {
                            "symbol": sym,
                            "timestamp": ts,
                            "timeframe": tf_str,
                            "stage": "Trader._procesar_vela",
                            "reason": skip_reason,
                        }
                    ),
                )
        finally:
            log.debug(
                "procesar_vela.exit",
                extra=safe_extra(
                    {
                        "symbol": sym,
                        "timestamp": ts,
                        "timeframe": tf_str,
                        "stage": "Trader._procesar_vela",
                        "result": outcome,
                    }
                ),
            )
    
    def _resolve_min_bars_requirement(self) -> int:
        """Obtiene el mínimo de velas requerido para evaluar entradas."""

        candidatos: List[Any] = [getattr(self, "min_bars", None)]
        cfg = getattr(self, "config", None)
        if cfg is not None:
            for attr in ("min_bars", "min_buffer_candles", "min_velas", "min_velas_evaluacion"):
                candidatos.append(getattr(cfg, attr, None))

        for valor in candidatos:
            if valor is None:
                continue
            try:
                entero = int(valor)
            except (TypeError, ValueError):
                continue
            if entero > 0:
                return entero

        return 0

    def _should_evaluate(
        self,
        symbol: str,
        timeframe: Optional[str],
        last_bar_ts: Optional[int],
    ) -> bool:
        """Controla que solo se evalúe una vez por vela cerrada."""

        if last_bar_ts is None:
            return True

        try:
            normalized_ts = int(last_bar_ts)
        except (TypeError, ValueError):
            return True

        key = (symbol.upper(), str(timeframe or "").lower())
        prev = self._last_evaluated_bar.get(key)
        if prev is not None and normalized_ts <= prev:
            return False

        self._last_evaluated_bar[key] = normalized_ts
        return True
