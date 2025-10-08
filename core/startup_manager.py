import asyncio
import json
import time
import os
import inspect
from contextlib import suppress
from pathlib import Path
from typing import Optional, TYPE_CHECKING, Any
from dataclasses import replace, is_dataclass

import aiohttp

from config.config_manager import ConfigManager
from core.trader_modular import Trader
from core.utils.utils import configurar_logger
from core.data.bootstrap import warmup_inicial
from core.diag.phase_logger import phase

if TYPE_CHECKING:  # pragma: no cover - solo para hints
    from config.config_manager import Config
else:  # Compatibilidad con stubs de tests que omiten Config
    Config = Any  # type: ignore[assignment]

SNAPSHOT_PATH = Path('estado/startup_snapshot.json')


class StartupManager:
    """Orquesta las fases de arranque del bot: config → bootstrap → feeds → trader → estrategias."""

    def __init__(
        self,
        trader: Optional[Trader] = None,
        *,
        data_feed: Any | None = None,
        feed: Any | None = None,
        config: Optional[Config] = None,
        ws_timeout: float | None = None,
        startup_timeout: float | None = None,
    ) -> None:
        self.trader = trader
        self.data_feed = data_feed if data_feed is not None else feed
        if self.data_feed is None and trader is not None:
            self.data_feed = getattr(trader, "data_feed", None)

        self.config: Optional[Config] = config or getattr(trader, 'config', None)
        if self.trader is not None and self.config is not None:
            with suppress(Exception):
                self.trader.config = self.config

        if self.trader is not None and self.data_feed is not None:
            with suppress(Exception):
                setattr(self.trader, "data_feed", self.data_feed)

        self.task: Optional[asyncio.Task] = None
        self._feed_task: Optional[asyncio.Task] = None
        self._trader_hold: Optional[asyncio.Event] = None
        self._fallback_ws_signal_task: Optional[asyncio.Task] = None
        self._ws_started_logged = False
        self._datafeed_connected_announced = False

        self.ws_timeout = ws_timeout
        if startup_timeout is None:
            env_timeout = os.getenv("STARTUP_TIMEOUT")
            try:
                self.startup_timeout = float(env_timeout) if env_timeout else 90.0
            except (TypeError, ValueError):
                self.startup_timeout = 90.0
        else:
            try:
                self.startup_timeout = float(startup_timeout)
            except (TypeError, ValueError):
                self.startup_timeout = 90.0

        self.event_bus = None
        if self.trader is not None:
            self.event_bus = getattr(self.trader, "event_bus", None) or getattr(self.trader, "bus", None)

        if self.config is not None and ws_timeout is not None:
            with suppress(Exception):
                setattr(self.config, "ws_timeout", ws_timeout)

        self.log = configurar_logger('startup')
        self._restart_alert_seconds = self._resolve_restart_alert_threshold()
        self._previous_snapshot: dict[str, Any] | None = None

    async def run(self) -> tuple[Trader, asyncio.Task, Config]:
        """Ejecuta la secuencia de arranque y devuelve (trader, tarea_trader, config)."""
        executed = [self._stop_trader]  # rollbacks en orden inverso
        try:
            await self._load_config()
            # (no re-agregar _stop_trader)

            with phase("_bootstrap"):
                await asyncio.wait_for(self._bootstrap(), timeout=15)

            assert self.trader is not None, "Trader no inicializado tras bootstrap"
            feed = getattr(self.trader, "data_feed", None) or getattr(self, "data_feed", None)
            if feed is not None:
                with suppress(Exception):
                    setattr(self.trader, "data_feed", feed)

            # Verificación de continuidad (si el feed expone la API)
            feed = getattr(self.trader, "data_feed", None) or getattr(self, "data_feed", None)
            if feed is not None and hasattr(feed, "verificar_continuidad"):
                if not feed.verificar_continuidad():
                    raise RuntimeError("DataFeed sin continuidad al arrancar")
            else:
                self.log.debug(
                    "Omitiendo verificación de continuidad del DataFeed (stub de tests o feed no disponible)."
                )

            await self._validate_feeds()
            await self._open_streams()
            executed.append(self._stop_streams)

            ws_timeout = self._resolve_ws_timeout()
            with phase("_wait_ws", extra={"timeout": ws_timeout}):
                await asyncio.wait_for(
                    self._wait_ws(ws_timeout),
                    timeout=ws_timeout + 5,
                )

            await self._enable_strategies()

            assert self.task is not None, "La tarea principal del Trader no quedó inicializada"
            assert self.config is not None, "Config no inicializada"

            return self.trader, self.task, self.config  # type: ignore[return-value]
        except Exception as e:
            self.log.error(f'Fallo en arranque: {e}')
            for rollback in reversed(executed):
                with suppress(Exception):
                    await rollback()
            raise

    async def _load_config(self) -> None:
        if self.trader is not None and self.config is not None:
            self._inspect_previous_snapshot()
            self._restore_persistencia_state()
            return
        if self.trader is not None and self.config is None:
            # Si viene trader, intenta tomar su config
            self.config = getattr(self.trader, "config", None)
        if self.config is None:
            self.config = ConfigManager.load_from_env()
        if self.trader is None:
            self.trader = Trader(self.config)  # type: ignore[arg-type]
        if self.trader is not None and self.event_bus is None:
            self.event_bus = getattr(self.trader, "event_bus", None) or getattr(self.trader, "bus", None)

        self._inspect_previous_snapshot()
        self._restore_persistencia_state()

    async def _bootstrap(self) -> None:
        assert self.trader is not None and self.config is not None
        await warmup_inicial(
            self.config.symbols,
            self.config.intervalo_velas,
            min_bars=int(os.getenv("MIN_BARS", "400")),
        )
        precargar = getattr(self.trader, "_precargar_historico", None)
        if precargar:
            try:
                if inspect.iscoroutinefunction(precargar):
                    await precargar()
                else:
                    precargar()
            except Exception as exc:  # nosec
                self.log.warning(
                    "Fallo al ejecutar _precargar_historico(): %s (continuando)",
                    exc,
                )
        else:
            self.log.debug("_precargar_historico() no definido en Trader; se omite.")

    async def _validate_feeds(self) -> None:
        assert self.trader is not None and self.config is not None
        if getattr(self.config, "modo_real", False) and not getattr(self.trader, "cliente", None):
            msg = (
                "Cliente Binance no inicializado. "
                "Verifica las claves API y las variables de entorno "
                "BINANCE_API_KEY/BINANCE_SECRET."
            )
            self.log.error(msg)
            raise RuntimeError(msg)

    async def _open_streams(self) -> None:
        """Crea la tarea principal del Trader y arranca el DataFeed cuando aplique."""
        assert self.trader is not None

        start_fn = getattr(self.trader, "ejecutar", None) or getattr(self.trader, "run", None)
        if start_fn is None:
            raise AttributeError("Trader no expone métodos ejecutar() ni run()")

        self._trader_hold = asyncio.Event()

        async def _run_trader() -> None:
            exc: BaseException | None = None
            try:
                result = start_fn()
                if inspect.isawaitable(result):
                    await result
            except asyncio.CancelledError:
                # Señalamos que se permite terminar
                if self._trader_hold and not self._trader_hold.is_set():
                    self._trader_hold.set()
                raise
            except GeneratorExit:
                if self._trader_hold and not self._trader_hold.is_set():
                    self._trader_hold.set()
                raise
            except BaseException as err:  # pragma: no cover
                exc = err
                if self._trader_hold and not self._trader_hold.is_set():
                    self._trader_hold.set()
                self.log.error("Trader finalizó con error inesperado: %s", err)
            finally:
                # Asegura liberar la barrera de salida
                if getattr(self, "_trader_hold", None) is not None:
                    with suppress(Exception):
                        self._trader_hold.set()

                    # Sólo await si .wait() es realmente awaitable (evita warnings en tests con mocks)
                    wait_fn = getattr(self._trader_hold, "wait", None)
                    if callable(wait_fn):
                        try:
                            res = wait_fn()
                            if inspect.isawaitable(res):
                                with suppress(Exception):
                                    await asyncio.wait_for(res, timeout=1.0)
                            # si no es awaitable (mock), no await → sin warning
                        except Exception:
                            pass

                # re-lanzar excepción si hubo fallo en la tarea principal
                if exc is not None:
                    raise exc

        task = asyncio.create_task(_run_trader(), name="TraderMain")
        self.task = task

        # Arranque del DataFeed si no está gestionado por el Trader
        feed = getattr(self.trader, "data_feed", None) or getattr(self, "data_feed", None)
        manual_feed = bool(getattr(feed, "_managed_by_trader", False)) if feed is not None else False

        start_feed = getattr(feed, "start", None) or getattr(feed, "iniciar", None) if feed else None

        async def _launch_feed(reason: str) -> None:
            if start_feed is None:
                return
            try:
                result = start_feed()
            except TypeError:
                result = None
            if inspect.isawaitable(result):
                self._feed_task = asyncio.create_task(result, name="DataFeedStart")
                # dar turno al loop para efectos secundarios inmediatos del feed
                await asyncio.sleep(0)
            self.log.debug("DataFeed iniciado (%s).", reason)
            
        bus = None
        if self.trader is not None:
            bus = getattr(self.trader, "event_bus", None) or getattr(self.trader, "bus", None)
            if bus is not None:
                self.event_bus = bus

        wait_fn = getattr(bus, "wait", None) if bus is not None else None
        schedule_ws_signal = False

        if start_feed is not None and not manual_feed:
            await _launch_feed("startup_manager")
        elif manual_feed:
            if callable(wait_fn):
                self.log.debug("DataFeed gestionado por Trader; se omite arranque automático.")
            else:
                self.log.warning(
                    "DataFeed gestionado pero sin soporte de EventBus.wait(); se arranca como fallback.",
                )
                if feed is not None:
                    symbols_cfg = getattr(self.config, "symbols", None)
                    symbols_list: list[Any] = []
                    if symbols_cfg is not None:
                        try:
                            symbols_list = [str(symbol).upper() for symbol in symbols_cfg]
                        except Exception:
                            try:
                                symbols_list = list(symbols_cfg)  # type: ignore[arg-type]
                            except Exception:
                                symbols_list = []
                    feed._symbols = symbols_list  # type: ignore[attr-defined]
                    feed._handler = getattr(self.trader, "_handler", None)  # type: ignore[attr-defined]
                    if hasattr(self.trader, "_cliente"):
                        feed._cliente = getattr(self.trader, "_cliente")  # type: ignore[attr-defined]
                self._set_config_value("ws_managed_by_trader", False)
                if feed is not None:
                    with suppress(Exception):
                        setattr(feed, "_managed_by_trader", False)
                await _launch_feed("fallback_autostart")
                schedule_ws_signal = True

        if schedule_ws_signal and feed is not None:
            self._schedule_fallback_ws_signal(feed)

    def _resolve_ws_timeout(self) -> float:
        assert self.trader is not None and self.config is not None

        raw_timeout = getattr(self.config, "ws_timeout", None) or self.ws_timeout
        if raw_timeout is None:
            raw_timeout = float(os.getenv("WS_TIMEOUT", "10"))
            with suppress(Exception):
                setattr(self.config, "ws_timeout", raw_timeout)
            trader_cfg = getattr(self.trader, "config", None)
            if trader_cfg is not None and trader_cfg is not self.config:
                with suppress(Exception):
                    setattr(trader_cfg, "ws_timeout", raw_timeout)

        try:
            timeout_val = float(raw_timeout)
        except (TypeError, ValueError):
            timeout_val = float(os.getenv("WS_TIMEOUT", "30"))

        self.ws_timeout = timeout_val
        return timeout_val

    def _schedule_fallback_ws_signal(self, feed: Any, *, timeout: float = 5.0) -> None:
        """Programa la publicación del evento ``datafeed_connected`` tras un fallback."""

        task = self._fallback_ws_signal_task
        if task is not None and not task.done():
            task.cancel()

        async def _runner() -> None:
            try:
                await self._await_fallback_ws_connected(feed, timeout=max(0.1, float(timeout)))
            except asyncio.CancelledError:
                raise
            except Exception:
                self.log.debug(
                    "Error al esperar confirmación de WS tras fallback_autostart.",
                    exc_info=True,
                )

        self._fallback_ws_signal_task = asyncio.create_task(
            _runner(), name="StartupFallbackWsSignal"
        )

    async def _await_fallback_ws_connected(self, feed: Any, *, timeout: float) -> None:
        if self._datafeed_connected_announced:
            return

        connected_event = getattr(feed, "ws_connected_event", None)
        failure_event = getattr(feed, "ws_failed_event", None)

        def _event_is_set(evt: Any) -> bool:
            if evt is None:
                return False
            is_set = getattr(evt, "is_set", None)
            if callable(is_set):
                with suppress(Exception):
                    return bool(is_set())
            return False

        if _event_is_set(connected_event):
            await self._finalize_fallback_ws_connected(feed)
            return

        wait_tasks: dict[str, asyncio.Task[Any]] = {}

        def _prepare_wait(evt: Any, label: str) -> None:
            if evt is None:
                return
            wait_fn = getattr(evt, "wait", None)
            if not callable(wait_fn):
                return
            try:
                result = wait_fn()
            except Exception:
                return
            if not inspect.isawaitable(result):
                return
            wait_tasks[label] = asyncio.create_task(result, name=f"fallback_ws_{label}")

        _prepare_wait(connected_event, "connected")
        _prepare_wait(failure_event, "failed")

        if wait_tasks:
            done: set[asyncio.Task[Any]] = set()
            pending: set[asyncio.Task[Any]] = set()
            try:
                done, pending = await asyncio.wait(
                    wait_tasks.values(),
                    timeout=timeout,
                    return_when=asyncio.FIRST_COMPLETED,
                )
            finally:
                for task in pending:
                    task.cancel()
                    with suppress(asyncio.CancelledError, Exception):
                        await task
                for task in done:
                    with suppress(Exception):
                        task.result()

            if not done:
                self.log.warning(
                    "No se confirmó la conexión del DataFeed tras fallback_autostart (timeout).",
                )
                return
            if wait_tasks.get("failed") in done:
                self.log.warning(
                    "WS no conectado tras fallback_autostart; feed reportó ws_failed_event.",
                    extra={"reason": getattr(feed, "ws_failure_reason", None)},
                )
                return
            if wait_tasks.get("connected") not in done:
                # Otro evento completó antes que el connected; no hay señal inequívoca.
                return

            await self._finalize_fallback_ws_connected(feed)
            return

        if await self._coerce_feed_connected(feed):
            await self._finalize_fallback_ws_connected(feed)

    async def _coerce_feed_connected(self, feed: Any) -> bool:
        connected_attr = getattr(feed, "connected", None)
        try:
            if isinstance(connected_attr, bool):
                return connected_attr
            if callable(connected_attr):
                result = connected_attr()
                if inspect.isawaitable(result):
                    result = await result
                return bool(result)
        except Exception:
            self.log.debug("Fallo al consultar feed.connected durante fallback.", exc_info=True)
        return False

    async def _finalize_fallback_ws_connected(self, feed: Any) -> None:
        if self._datafeed_connected_announced:
            return

        signal_fn = getattr(feed, "_signal_ws_connected", None)
        if callable(signal_fn):
            with suppress(Exception):
                signal_fn(None)

        symbols_cfg = self._get_config_value("symbols", None)
        symbols: list[str] = []
        if symbols_cfg:
            try:
                symbols_iter = list(symbols_cfg)
            except Exception:
                symbols_iter = []
            else:
                try:
                    symbols = [str(symbol).upper() for symbol in symbols_iter]
                except Exception:
                    symbols = []

        bus_payload: dict[str, Any] = {}
        if symbols:
            bus_payload["symbols"] = symbols

        intervalo = self._get_config_value("intervalo_velas", None)
        log_extra: dict[str, Any] = {"stage": "StartupManager"}
        if intervalo is not None:
            log_extra["intervalo"] = intervalo
        if symbols:
            log_extra["symbols"] = symbols
        log_extra["reason"] = "fallback_autostart"

        if not self._ws_started_logged:
            self.log.info("trader:ws_started", extra=log_extra)
            self._ws_started_logged = True

        bus = self.event_bus or getattr(self.trader, "event_bus", None) or getattr(self.trader, "bus", None)
        announced = False
        if bus is not None:
            emit = getattr(bus, "emit", None)
            if callable(emit):
                try:
                    emit("datafeed_connected", dict(bus_payload))
                    announced = True
                except Exception:
                    self.log.debug("No se pudo emitir datafeed_connected en event_bus.", exc_info=True)
            if not announced:
                publish = getattr(bus, "publish", None)
                if callable(publish):
                    try:
                        asyncio.create_task(publish("datafeed_connected", dict(bus_payload)))
                        announced = True
                    except Exception:
                        self.log.debug(
                            "No se pudo publicar datafeed_connected en event_bus.",
                            exc_info=True,
                        )

        if announced or bus is None:
            self._datafeed_connected_announced = True

    async def _enable_strategies(self) -> None:
        assert self.trader is not None and self.config is not None

        # Verificación de desincronización de reloj
        with phase("_check_clock_drift"):
            clock_ok = await asyncio.wait_for(self._check_clock_drift(), timeout=5)
        if not clock_ok:
            if self.config is not None:
                if is_dataclass(self.config):
                    self.config = replace(self.config, modo_real=False)
                else:
                    with suppress(Exception):
                        setattr(self.config, "modo_real", False)
            if hasattr(self.trader, "config"):
                with suppress(Exception):
                    self.trader.config = self.config
            with suppress(Exception):
                setattr(self.trader, "modo_real", False)
            with suppress(Exception):
                setattr(self.trader, "cliente", None)

        if not await self._check_storage():
            raise RuntimeError(
                'Storage no disponible. Verifica los permisos de escritura en el directorio de datos.'
            )

        if hasattr(self.trader, "habilitar_estrategias"):
            self.trader.habilitar_estrategias()
        else:
            self.log.debug("Trader sin habilitar_estrategias(); se omite la activación.")

        self._snapshot()

    async def _wait_ws(self, timeout: float, *, grace_for_trader: float = 5.0) -> None:
        """Espera a que el DataFeed reporte actividad hasta `timeout` segundos."""
        assert self.trader is not None

        feed = getattr(self.trader, "data_feed", None) or getattr(self, "data_feed", None)
        managed = bool(getattr(feed, "_managed_by_trader", False)) if feed is not None else False
        managed_override = self._get_config_value("ws_managed_by_trader", None)
        if managed_override is not None:
            managed = bool(managed_override)

        grace = max(0.0, float(grace_for_trader))
        if grace > timeout:
            grace = float(timeout)
        self.log.debug(
            "wait_ws:begin",
            extra={
                "timeout": timeout,
                "grace_for_trader": grace,
                "managed_by_trader": managed,
            },
        )

        if await self._poll_ws_connected():
            self.log.debug(
                "wait_ws:already_connected",
                extra={"timeout": timeout},
            )
            return

        trader_connected = False
        trader_activity = False

        if managed and grace > 0.0:
            trader_connected, trader_activity = await self._wait_for_trader_grace(grace)
            if trader_connected:
                self.log.debug(
                    "wait_ws:trader_ready",
                    extra={"grace_for_trader": grace},
                )
                return
            if not trader_activity:
                self.log.warning("Trader no inició WS en la gracia; forzando ensure_running()")
                self._set_config_value("ws_managed_by_trader", False)
                if feed is not None:
                    with suppress(Exception):
                        setattr(feed, "_managed_by_trader", False)
                managed = False

        feed = getattr(self.trader, "data_feed", None) or getattr(self, "data_feed", None)

        if feed is not None and hasattr(feed, "ensure_running"):
            if not trader_activity and not await self._is_ws_attempt_in_progress():
                self.log.debug(
                    "wait_ws:ensure_running_fallback",
                    extra={"grace_for_trader": grace},
                )
                ensure_running = getattr(feed, "ensure_running")
                try:
                    result = ensure_running()
                except TypeError:
                    result = None
                if inspect.isawaitable(result):
                    await result
                else:
                    await asyncio.sleep(0)

        feed = getattr(self.trader, "data_feed", None) or getattr(self, "data_feed", None)

        def _log_waiting(event_bus_flag: bool, asyncio_flag: bool, metric_flag: bool, polling_flag: bool) -> None:
            self.log.info(
                "waiting on: event_bus=%s, asyncio_event=%s, metric=%s, polling=%s",
                event_bus_flag,
                asyncio_flag,
                metric_flag,
                polling_flag,
            )

        def _log_missing(event_bus_flag: bool, asyncio_flag: bool, metric_flag: bool, polling_flag: bool) -> None:
            self.log.warning(
                "no se detectó señal event_bus=%s, asyncio_event=%s, metric=%s, polling=%s",
                event_bus_flag,
                asyncio_flag,
                metric_flag,
                polling_flag,
            )

        metric_available = bool(getattr(self, "metrics", None))
        polling_active = False

        if managed:
            bus = self.event_bus or getattr(self.trader, "event_bus", None) or getattr(self.trader, "bus", None)
            wait_fn = getattr(bus, "wait", None) if bus is not None else None
            if callable(wait_fn):
                _log_waiting(True, False, metric_available, False)
                try:
                    await asyncio.wait_for(wait_fn("datafeed_connected"), timeout=timeout)
                    return
                except asyncio.TimeoutError as exc:
                    _log_missing(True, False, metric_available, False)
                    raise RuntimeError("WS no conectado") from exc
            else:
                self.log.debug(
                    "EventBus sin soporte de wait() o ausente; se recurre a sondeo de actividad.",
                )

        success_event = getattr(feed, "ws_connected_event", None)
        failure_event = getattr(feed, "ws_failed_event", None)

        def _event_is_set(evt: Any) -> bool:
            if evt is None:
                return False
            if isinstance(evt, asyncio.Event):
                return evt.is_set()
            is_set = getattr(evt, "is_set", None)
            if callable(is_set):
                try:
                    return bool(is_set())
                except Exception:
                    return False
            return False

        def _failure_message() -> str:
            reason = getattr(feed, "ws_failure_reason", None)
            base = "WS no conectado"
            if reason:
                return f"{base}: {reason}"
            return base

        if _event_is_set(failure_event):
            raise RuntimeError(_failure_message())
        if _event_is_set(success_event):
            return

        async_events: dict[str, asyncio.Event] = {}
        thread_events: dict[str, Any] = {}
        if isinstance(success_event, asyncio.Event):
            async_events["success"] = success_event
        elif success_event is not None and callable(getattr(success_event, "wait", None)):
            thread_events["success"] = success_event
        if isinstance(failure_event, asyncio.Event):
            async_events["failure"] = failure_event
        elif failure_event is not None and callable(getattr(failure_event, "wait", None)):
            thread_events["failure"] = failure_event

        asyncio_available = bool(async_events or thread_events)
        polling_active = not asyncio_available
        _log_waiting(False, asyncio_available, metric_available, polling_active)

        async def _wait_async_evt(evt: asyncio.Event) -> bool:
            try:
                await asyncio.wait_for(evt.wait(), timeout=timeout)
            except asyncio.TimeoutError:
                return False
            return True

        async def _wait_thread_evt(evt: Any) -> bool | None:
            loop = asyncio.get_running_loop()
            wait_fn = getattr(evt, "wait", None)
            if not callable(wait_fn):
                return None
            try:
                result = await loop.run_in_executor(None, lambda: wait_fn(timeout))
            except Exception:
                self.log.debug("Fallo al esperar threading.Event; degradando a sondeo.", exc_info=True)
                return None
            return bool(result)

        wait_tasks: dict[str, asyncio.Task[bool | None]] = {}
        for label, evt in async_events.items():
            wait_tasks[label] = asyncio.create_task(_wait_async_evt(evt), name=f"wait_ws_{label}")
        for label, evt in thread_events.items():
            wait_tasks[label] = asyncio.create_task(_wait_thread_evt(evt), name=f"wait_ws_{label}")

        if wait_tasks:
            try:
                done, _ = await asyncio.wait(
                    wait_tasks.values(), timeout=timeout, return_when=asyncio.FIRST_COMPLETED
                )
            except Exception:
                self.log.debug("Fallo al esperar ws_connected_event; se recurre a sondeo.", exc_info=True)
            else:
                if not done:
                    _log_missing(False, asyncio_available, metric_available, polling_active)
                    raise RuntimeError(_failure_message())
                degrade_to_poll = False
                for label, task in wait_tasks.items():
                    if task in done:
                        try:
                            triggered = task.result()
                        except Exception:
                            triggered = None
                        if triggered is None or triggered is False:
                            degrade_to_poll = True
                            continue
                        if label == "success" and triggered:
                            return
                        if label == "failure" and triggered:
                            raise RuntimeError(_failure_message())
                if degrade_to_poll:
                    self.log.debug("Eventos incompatibles con espera directa; degradando a sondeo.")
                    asyncio_available = False
                    polling_active = True
                    _log_waiting(False, False, metric_available, polling_active)
                else:
                    # Si ninguna tarea reportó éxito/fallo se considera timeout
                    _log_missing(False, asyncio_available, metric_available, polling_active)
                    raise RuntimeError(_failure_message())
            finally:
                for task in wait_tasks.values():
                    if not task.done():
                        task.cancel()
                await asyncio.gather(*wait_tasks.values(), return_exceptions=True)
                
        start = time.time()
        while time.time() - start < timeout:
            feed = getattr(self.trader, "data_feed", None) or getattr(self, "data_feed", None)
            if feed is None:
                await asyncio.sleep(0.1)
                continue


            if await self._poll_ws_connected():
                return

            activo = False
            success_event = getattr(feed, "ws_connected_event", success_event)
            failure_event = getattr(feed, "ws_failed_event", failure_event)
            if _event_is_set(failure_event):
                raise RuntimeError(_failure_message())
            if _event_is_set(success_event):
                return
            # Propiedad o método `activos`
            if hasattr(feed, "activos"):
                try:
                    val = feed.activos  # property
                    activo = bool(val)
                except TypeError:
                    with suppress(Exception):
                        activo = bool(feed.activos())  # callable
                except Exception:
                    activo = False
            # Fallback `is_active()`
            if not activo and hasattr(feed, "is_active"):
                with suppress(Exception):
                    activo = bool(feed.is_active())

            if activo:
                return
            await asyncio.sleep(0.1)
        _log_missing(False, asyncio_available, metric_available, True)
        raise RuntimeError(_failure_message())
    
    def _get_config_value(self, key: str, default: Any | None = None) -> Any | None:
        cfg = self.config or getattr(self.trader, "config", None)
        if cfg is None:
            return default
        getter = getattr(cfg, "get", None)
        if callable(getter):
            try:
                return getter(key, default)
            except Exception:
                return default
        return getattr(cfg, key, default)

    def _set_config_value(self, key: str, value: Any) -> None:
        cfg = self.config or getattr(self.trader, "config", None)
        if cfg is None:
            return
        if hasattr(cfg, "__setitem__"):
            try:
                cfg[key] = value
                return
            except Exception:
                pass
        with suppress(Exception):
            setattr(cfg, key, value)

    async def _wait_for_trader_grace(self, timeout: float) -> tuple[bool, bool]:
        if timeout <= 0:
            connected = await self._poll_ws_connected()
            if connected:
                return True, True
            return False, await self._is_ws_attempt_in_progress()

        bus = self.event_bus or getattr(self.trader, "event_bus", None) or getattr(self.trader, "bus", None)
        wait_fn = getattr(bus, "wait", None) if bus is not None else None

        tasks: dict[str, asyncio.Task[Any]] = {}
        if callable(wait_fn):
            for event_name in ("datafeed_connected", "ws_connected", "ws_connecting"):
                try:
                    result = wait_fn(event_name)
                except Exception:
                    continue
                if not inspect.isawaitable(result):
                    continue
                tasks[event_name] = asyncio.create_task(result, name=f"wait_ws_grace_{event_name}")

        attempt_detected = False
        deadline = time.monotonic() + timeout
        try:
            while True:
                for name, task in list(tasks.items()):
                    if not task.done():
                        continue
                    try:
                        _ = task.result()
                    except asyncio.CancelledError:
                        continue
                    except Exception:
                        tasks.pop(name, None)
                        continue
                    attempt_detected = True
                    if name in {"datafeed_connected", "ws_connected"}:
                        return True, True
                    tasks.pop(name, None)

                if await self._poll_ws_connected():
                    return True, True

                if await self._is_ws_attempt_in_progress():
                    attempt_detected = True

                now = time.monotonic()
                remaining = deadline - now
                if remaining <= 0:
                    break
                await asyncio.sleep(min(0.1, remaining))
        finally:
            for task in tasks.values():
                if not task.done():
                    task.cancel()
            if tasks:
                await asyncio.gather(*tasks.values(), return_exceptions=True)

        return False, attempt_detected

    async def _is_ws_attempt_in_progress(self) -> bool:
        feed = getattr(self.trader, "data_feed", None) or getattr(self, "data_feed", None)
        if feed is None:
            return False

        failure_event = getattr(feed, "ws_failed_event", None)
        if isinstance(failure_event, asyncio.Event) and failure_event.is_set():
            return False
        maybe_is_set = getattr(failure_event, "is_set", None)
        if callable(maybe_is_set):
            with suppress(Exception):
                if bool(maybe_is_set()):
                    return False

        if await self._poll_ws_connected():
            return True

        if bool(getattr(feed, "_running", False)):
            return True

        activos = getattr(feed, "activos", None)
        try:
            if isinstance(activos, bool):
                if activos:
                    return True
            elif callable(activos):
                result = activos()
                if inspect.isawaitable(result):
                    result = await result
                if bool(result):
                    return True
        except Exception:
            pass

        is_active = getattr(feed, "is_active", None)
        if callable(is_active):
            try:
                result = is_active()
                if inspect.isawaitable(result):
                    result = await result
                if bool(result):
                    return True
            except Exception:
                pass

        tasks = getattr(feed, "_tasks", None)
        if isinstance(tasks, dict):
            for task in tasks.values():
                if isinstance(task, asyncio.Task) and not task.done():
                    return True

        connecting_event = getattr(feed, "ws_connecting_event", None)
        if isinstance(connecting_event, asyncio.Event) and connecting_event.is_set():
            return True
        maybe_is_set = getattr(connecting_event, "is_set", None)
        if callable(maybe_is_set):
            with suppress(Exception):
                if bool(maybe_is_set()):
                    return True

        return False

    async def _poll_ws_connected(self) -> bool:
        feed = getattr(self.trader, "data_feed", None) or getattr(self, "data_feed", None)
        if feed is None:
            return False

        connected_attr = getattr(feed, "connected", None)
        try:
            if isinstance(connected_attr, bool):
                if connected_attr:
                    return True
            elif callable(connected_attr):
                result = connected_attr()
                if inspect.isawaitable(result):
                    result = await result
                if bool(result):
                    return True
        except Exception:
            pass

        evt = getattr(feed, "ws_connected_event", None)
        if isinstance(evt, asyncio.Event):
            if evt.is_set():
                return True
        else:
            maybe_is_set = getattr(evt, "is_set", None)
            if callable(maybe_is_set):
                with suppress(Exception):
                    if bool(maybe_is_set()):
                        return True

        metrics = getattr(self, "metrics", None)
        gauge = getattr(metrics, "ws_connected_gauge", None) if metrics is not None else None
        if gauge is not None:
            getter = getattr(gauge, "get", None)
            if callable(getter):
                with suppress(Exception):
                    return bool(getter())

        return False

    async def _check_clock_drift(self) -> bool:
        """Comprueba desincronización respecto al server de Binance. Tolerante a fallos de red."""
        try:
            async with aiohttp.ClientSession() as session:
                request = session.get("https://api.binance.com/api/v3/time", timeout=5)
                ctx = await request if inspect.isawaitable(request) else request
                if hasattr(ctx, "__aenter__"):
                    async with ctx as resp:
                        data = await resp.json()
                elif hasattr(ctx, "json"):
                    data = await ctx.json()
                else:
                    text = await ctx.text() if hasattr(ctx, "text") else "{}"
                    data = json.loads(text or "{}")
            server = data.get("serverTime", 0) / 1000
            drift = abs(server - time.time())
            return drift < 0.5
        except Exception as e:
            client_error = getattr(aiohttp, "ClientError", ())
            if not isinstance(client_error, tuple):
                client_error = (client_error,)
            tolerable = client_error + (asyncio.TimeoutError,)
            if isinstance(e, tolerable):
                self.log.warning(
                    "No se pudo obtener la hora de Binance: %s. Omitiendo verificación de reloj.",
                    e,
                )
                return True
            return False

    async def _check_storage(self) -> bool:
        """Verifica que se pueda escribir en el directorio de estado."""
        try:
            SNAPSHOT_PATH.parent.mkdir(parents=True, exist_ok=True)
            tmp = SNAPSHOT_PATH.parent / 'tmp_check'
            tmp.write_text('ok')
            tmp.unlink()
            return True
        except Exception:
            return False

    def _resolve_restart_alert_threshold(self) -> float:
        """Obtiene desde el entorno el umbral para alertar reinicios prematuros."""
        raw = os.getenv("STARTUP_RESTART_ALERT_SECONDS")
        try:
            value = float(raw) if raw else 300.0
        except (TypeError, ValueError):
            value = 300.0
        return max(value, 0.0)

    def _read_snapshot(self) -> dict[str, Any] | None:
        """Lee el snapshot previo si existe y tiene formato válido."""
        path = SNAPSHOT_PATH
        try:
            with path.open('r', encoding='utf-8') as fh:
                data = json.load(fh)
        except FileNotFoundError:
            return None
        except json.JSONDecodeError as exc:
            self.log.warning("Snapshot previo ilegible: %s", exc)
            return None
        except Exception as exc:  # pragma: no cover - errores inesperados
            self.log.debug("Fallo al leer snapshot previo: %s", exc, exc_info=True)
            return None
        if not isinstance(data, dict):
            self.log.warning("Snapshot previo con formato inesperado: %s", type(data).__name__)
            return None
        return data

    def _inspect_previous_snapshot(self) -> None:
        """Registra información diagnóstica basada en el snapshot anterior."""
        self._previous_snapshot = None
        snapshot = self._read_snapshot()
        if not snapshot:
            return
        self._previous_snapshot = snapshot

        symbols = snapshot.get('symbols')
        if not isinstance(symbols, list):
            symbols = [symbols] if symbols is not None else []
        modo_real = bool(snapshot.get('modo_real', False))
        timestamp = snapshot.get('timestamp')
        age: float | None
        if isinstance(timestamp, (int, float)):
            age = max(0.0, time.time() - float(timestamp))
        else:
            age = None

        log_payload = {
            'symbols': symbols,
            'modo_real': modo_real,
            'age_seconds': age,
        }
        self.log.info(
            'Snapshot previo detectado',
            extra={'startup_snapshot': log_payload},
        )

        if (
            age is not None
            and self._restart_alert_seconds > 0
            and age < self._restart_alert_seconds
        ):
            self.log.warning(
                'Reinicio detectado %.1f segundos después del snapshot previo; revisar estabilidad.',
                age,
                extra={'startup_snapshot': log_payload},
            )

    def _snapshot(self) -> None:
        """Persistencia de snapshot mínimo de estado de arranque."""
        assert self.config is not None
        data = {
            'symbols': getattr(self.config, 'symbols', []),
            'modo_real': getattr(self.config, 'modo_real', False),
            'timestamp': time.time(),
        }
        trader = self.trader
        if trader is not None:
            persistencia = getattr(trader, 'persistencia', None)
            export_fn = getattr(persistencia, 'export_state', None)
            if callable(export_fn):
                try:
                    estado = export_fn()
                except Exception:
                    self.log.debug(
                        'No se pudo exportar estado de PersistenciaTecnica para snapshot',
                        exc_info=True,
                    )
                else:
                    if estado:
                        data['persistencia_tecnica'] = estado
        try:
            SNAPSHOT_PATH.parent.mkdir(parents=True, exist_ok=True)
            with open(SNAPSHOT_PATH, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            self.log.error(f'No se pudo guardar snapshot: {e}')

    def _restore_persistencia_state(self) -> None:
        """Restaura el estado técnico persistido si está disponible."""
        trader = self.trader
        if trader is None:
            return
        snapshot = self._previous_snapshot
        if not snapshot:
            return
        estado = snapshot.get('persistencia_tecnica')
        if not estado:
            return
        persistencia = getattr(trader, 'persistencia', None)
        if persistencia is None:
            return
        load_fn = getattr(persistencia, 'load_state', None)
        if callable(load_fn):
            try:
                load_fn(estado)
            except Exception:
                self.log.debug(
                    'No se pudo restaurar estado de PersistenciaTecnica desde snapshot',
                    exc_info=True,
                )

    async def _stop_streams(self) -> None:
        """Detiene feed y tarea del trader respetuosamente."""
        if getattr(self, "_feed_task", None) is not None:
            self._feed_task.cancel()
            with suppress(asyncio.CancelledError, Exception):
                await self._feed_task
            self._feed_task = None
        if getattr(self, "_fallback_ws_signal_task", None) is not None:
            self._fallback_ws_signal_task.cancel()
            with suppress(asyncio.CancelledError, Exception):
                await self._fallback_ws_signal_task
            self._fallback_ws_signal_task = None
        if getattr(self, "_trader_hold", None) is not None:
            self._trader_hold.set()
        if self.task is not None:
            self.task.cancel()
            with suppress(asyncio.CancelledError, Exception):
                await self.task
        self.task = None

    async def _stop_trader(self) -> None:
        """Cierra el Trader si expone cerrar() (sync o async)."""
        trader = self.trader
        if trader is None:
            return
        cerrar = getattr(trader, "cerrar", None)
        if not callable(cerrar):
            return
        try:
            result = cerrar()
            if inspect.isawaitable(result):
                with suppress(asyncio.CancelledError, Exception):
                    await result
        except Exception:
            # Registro suavizado: en rollback no queremos interrumpir
            self.log.debug("Excepción al cerrar Trader durante rollback", exc_info=True)

