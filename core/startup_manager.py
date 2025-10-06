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
                if inspect.iscoroutinefunction(start_fn):
                    await start_fn()
                else:
                    # Ejecuta método sync en hilo para no bloquear el loop
                    await asyncio.to_thread(start_fn)
            except asyncio.CancelledError:
                # Señalamos que se permite terminar
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

        start_feed = None
        if feed is not None and not manual_feed:
            start_feed = getattr(feed, "start", None) or getattr(feed, "iniciar", None)

        if start_feed is not None:
            try:
                result = start_feed()
            except TypeError:
                result = None
            if inspect.isawaitable(result):
                self._feed_task = asyncio.create_task(result, name="DataFeedStart")
                # dar turno al loop para efectos secundarios inmediatos del feed
                await asyncio.sleep(0)
        elif manual_feed:
            self.log.debug("DataFeed gestionado por Trader; se omite arranque automático.")

        if self.trader is not None:
            bus = getattr(self.trader, "event_bus", None) or getattr(self.trader, "bus", None)
            if bus is not None:
                self.event_bus = bus

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

    async def _wait_ws(self, timeout: float) -> None:
        """Espera a que el DataFeed reporte actividad hasta `timeout` segundos."""
        assert self.trader is not None

        feed = getattr(self.trader, "data_feed", None) or getattr(self, "data_feed", None)
        managed = bool(getattr(feed, "_managed_by_trader", False)) if feed is not None else False

        if managed:
            bus = self.event_bus or getattr(self.trader, "event_bus", None) or getattr(self.trader, "bus", None)
            wait_fn = getattr(bus, "wait", None) if bus is not None else None
            if callable(wait_fn):
                try:
                    await asyncio.wait_for(wait_fn("datafeed_connected"), timeout=timeout)
                    return
                except asyncio.TimeoutError as exc:
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

        async def _wait_async_evt(evt: asyncio.Event) -> bool:
            await evt.wait()
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
                    raise RuntimeError(_failure_message())
                degrade_to_poll = False
                for label, task in wait_tasks.items():
                    if task in done:
                        try:
                            triggered = task.result()
                        except Exception:
                            triggered = None
                        if triggered is None:
                            degrade_to_poll = True
                            continue
                        if label == "success" and triggered:
                            return
                        if label == "failure" and triggered:
                            raise RuntimeError(_failure_message())
                if degrade_to_poll:
                    self.log.debug("Eventos incompatibles con espera directa; degradando a sondeo.")
                else:
                    # Si ninguna tarea reportó éxito/fallo se considera timeout
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
        raise RuntimeError(_failure_message())

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

    def _snapshot(self) -> None:
        """Persistencia de snapshot mínimo de estado de arranque."""
        assert self.config is not None
        data = {
            'symbols': getattr(self.config, 'symbols', []),
            'modo_real': getattr(self.config, 'modo_real', False),
            'timestamp': time.time(),
        }
        try:
            SNAPSHOT_PATH.parent.mkdir(parents=True, exist_ok=True)
            with open(SNAPSHOT_PATH, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            self.log.error(f'No se pudo guardar snapshot: {e}')

    async def _stop_streams(self) -> None:
        """Detiene feed y tarea del trader respetuosamente."""
        if getattr(self, "_feed_task", None) is not None:
            self._feed_task.cancel()
            with suppress(asyncio.CancelledError, Exception):
                await self._feed_task
            self._feed_task = None
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

