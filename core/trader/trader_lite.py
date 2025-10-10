"""Implementación modular mínima del núcleo del trader."""

from __future__ import annotations

import asyncio
import contextlib
import os
from collections import defaultdict
from typing import Any, Awaitable, Callable, Dict, Optional, Tuple, TYPE_CHECKING

from core.backfill_service import BackfillService

from core.utils.log_utils import safe_extra
from core.utils.utils import configurar_logger

from ._utils import EstadoSimbolo, _is_awaitable, _maybe_await
from .trader_lite_backfill import TraderLiteBackfillMixin
from .trader_lite_processing import TraderLiteProcessingMixin

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


class TraderLite(TraderLiteBackfillMixin, TraderLiteProcessingMixin):
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
        self._waiting_close_streak: Dict[tuple[str, str], int] = defaultdict(int)
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

        self._skew_allow_secs = self._resolve_skew_allow()

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
        self._owned_event_bus: Any | None = None

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
        except Exception:
            self._SpreadGuard = None
        else:
            self._SpreadGuard = _SpreadGuard

        # Inicializar referencias lazy por defecto para que un fallo parcial
        # no anule al resto de componentes opcionales.
        self._EventBus = None
        self._OrderManager = None
        self._sync_sim = None
        self._StrategyEngine = None
        self._PersistenciaTecnica = None
        self._verificar_entrada = None
        try:
            from core.event_bus import EventBus as _EventBus
        except Exception:
            log.debug("Fallo importando EventBus; se continúa sin bus", exc_info=True)
        else:
            self._EventBus = _EventBus
        try:
            from core.orders.order_manager import OrderManager as _OrderManager
        except Exception:
            log.debug("Fallo importando OrderManager; se deshabilitan órdenes reales", exc_info=True)
        else:
            self._OrderManager = _OrderManager
        try:
            from core.orders.storage_simulado import sincronizar_ordenes_simuladas as _sync_sim
        except Exception:
            log.debug("Fallo importando sincronizador de órdenes simuladas", exc_info=True)
        else:
            self._sync_sim = _sync_sim
        try:
            from core.strategies import StrategyEngine as _StrategyEngine
        except Exception:
            log.debug("Fallo importando StrategyEngine; se usará pipeline directo", exc_info=True)
        else:
            self._StrategyEngine = _StrategyEngine
        try:
            from core.persistencia_tecnica import PersistenciaTecnica as _PersistenciaTecnica
        except Exception:
            log.debug("Fallo importando PersistenciaTecnica; persistencia inactiva", exc_info=True)
        else:
            self._PersistenciaTecnica = _PersistenciaTecnica
        try:
            from core.strategies.entry.verificar_entradas import (
                verificar_entrada as _verificar_entrada,
            )
        except Exception:
            log.debug(
                "Fallo importando pipeline de verificación de entradas", exc_info=True
            )
        else:
            self._verificar_entrada = _verificar_entrada

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
    
    def _resolve_skew_allow(self) -> float:
        """Determina la tolerancia de skew permitida para velas en segundos."""

        candidatos = [
            getattr(self.config, "df_event_skew_allow", None),
            getattr(self.config, "df_event_skew_allow_secs", None),
            getattr(self.config, "event_skew_allow", None),
            getattr(self.config, "event_skew_allow_secs", None),
            os.getenv("DF_EVENT_SKEW_ALLOW_SECS"),
            os.getenv("TRADER_EVENT_SKEW_ALLOW_SECS"),
        ]
        for valor in candidatos:
            if valor is None:
                continue
            try:
                parsed = float(valor)
            except (TypeError, ValueError):
                log.debug(
                    "Valor inválido para skew_allow=%r; usando fallback",
                    valor,
                )
                continue
            if parsed >= 0:
                return parsed
        return 1.5
    
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
        await self._close_owned_event_bus()

    async def _close_owned_event_bus(self) -> None:
        """Cierra el EventBus creado internamente si sigue activo."""

        if self._owned_event_bus is None:
            return

        bus = self._owned_event_bus
        self._owned_event_bus = None

        try:
            await bus.close()
        except Exception:
            log.exception("Error cerrando EventBus del Trader")

        if getattr(self, "bus", None) is bus:
            self.bus = None
        if getattr(self, "event_bus", None) is bus:
            self.event_bus = None

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
