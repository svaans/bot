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

if TYPE_CHECKING:  # pragma: no cover - solo para hints
    from config.config_manager import Config
else:  # Compatibilidad con stubs de tests que omiten Config
    Config = Any  # type: ignore[assignment]

SNAPSHOT_PATH = Path('estado/startup_snapshot.json')


class StartupManager:
    """Orquesta las fases de arranque del bot."""

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
            try:
                self.trader.config = self.config
            except Exception:
                pass
        if self.trader is not None and self.data_feed is not None:
            try:
                setattr(self.trader, "data_feed", self.data_feed)
            except Exception:
                pass
        self.task: Optional[asyncio.Task] = None
        self._feed_task: Optional[asyncio.Task] = None
        self._trader_hold: Optional[asyncio.Event] = None
        self.ws_timeout = ws_timeout
        self.startup_timeout = startup_timeout
        if self.config is not None and ws_timeout is not None:
            try:
                setattr(self.config, "ws_timeout", ws_timeout)
            except Exception:
                pass
        self.log = configurar_logger('startup')

    async def run(self) -> tuple[Trader, asyncio.Task, Config]:
        executed = [self._stop_trader]
        try:
            await self._load_config()
            executed.append(self._stop_trader)
            await self._bootstrap()
            assert self.trader is not None
            feed = getattr(self.trader, "data_feed", None)
            if feed is None:
                feed = getattr(self, "data_feed", None)
                if feed is not None:
                    try:
                        setattr(self.trader, "data_feed", feed)
                    except Exception:
                        pass

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
            await self._enable_strategies()
            return self.trader, self.task, self.config  # type: ignore
        except Exception as e:
            self.log.error(f'Fallo en arranque: {e}')
            for rollback in reversed(executed):
                with suppress(Exception):
                    await rollback()
            raise

    async def _load_config(self) -> None:
        if self.trader is not None:
            return
        self.config = ConfigManager.load_from_env()
        self.trader = Trader(self.config)  # type: ignore[arg-type]

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
            except Exception as exc:  # nosec - queremos registrar y continuar
                self.log.warning(
                    "Fallo al ejecutar _precargar_historico(): %s (continuando)",
                    exc,
                )
        else:
            self.log.debug(
                "_precargar_historico() no definido en Trader; se omite."
            )

    async def _validate_feeds(self) -> None:
        assert self.trader is not None and self.config is not None
        if self.config.modo_real and not self.trader.cliente:
            msg = (
                "Cliente Binance no inicializado. "
                "Verifica las claves API y las variables de entorno "
                "BINANCE_API_KEY/BINANCE_SECRET."
            )
            self.log.error(msg)
            raise RuntimeError(msg)

    async def _open_streams(self) -> None:
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
                    await asyncio.to_thread(start_fn)
            except asyncio.CancelledError:
                self._trader_hold.set()
                raise
            except BaseException as err:  # pragma: no cover - logging
                exc = err
                self._trader_hold.set()
                self.log.error("Trader finalizó con error inesperado: %s", err)
            finally:
                await self._trader_hold.wait()
                if exc is not None:
                    raise exc

        task = asyncio.create_task(_run_trader())

        feed = getattr(self.trader, "data_feed", None) or getattr(self, "data_feed", None)
        start_feed = getattr(feed, "start", None) if feed is not None else None
        if start_feed is None and feed is not None:
            start_feed = getattr(feed, "iniciar", None)
        if start_feed is not None:
            try:
                result = start_feed()
            except TypeError:
                result = None
            if inspect.isawaitable(result):
                self._feed_task = asyncio.create_task(result)

        self._trader_task = task
        self.task = task

    async def _enable_strategies(self) -> None:
        assert self.trader is not None and self.config is not None
        raw_timeout = getattr(self.config, "ws_timeout", None)
        if raw_timeout is None:
            raw_timeout = self.ws_timeout
        if raw_timeout is None:
            raw_timeout = float(os.getenv("WS_TIMEOUT", "30"))
            try:
                setattr(self.config, "ws_timeout", raw_timeout)
            except Exception:
                pass
            trader_cfg = getattr(self.trader, "config", None)
            if trader_cfg is not None and trader_cfg is not self.config:
                try:
                    setattr(trader_cfg, "ws_timeout", raw_timeout)
                except Exception:
                    pass

        await self._wait_ws(float(raw_timeout))
        if not await self._check_clock_drift():
            if self.config is not None:
                if is_dataclass(self.config):
                    self.config = replace(self.config, modo_real=False)
                else:
                    try:
                        setattr(self.config, "modo_real", False)
                    except Exception:
                        pass
            if hasattr(self.trader, "config"):
                try:
                    self.trader.config = self.config
                except Exception:
                    pass
            try:
                setattr(self.trader, "modo_real", False)
            except Exception:
                pass
            try:
                setattr(self.trader, "cliente", None)
            except Exception:
                pass
        if not await self._check_storage():
            raise RuntimeError(
                'Storage no disponible. '
                'Verifica los permisos de escritura en el directorio de datos.'
            )
        if hasattr(self.trader, "habilitar_estrategias"):
            self.trader.habilitar_estrategias()
        else:
            self.log.debug("Trader sin habilitar_estrategias(); se omite la activación.")
        self._snapshot()

    async def _wait_ws(self, timeout: float) -> None:
        assert self.trader is not None
        start = time.time()
        while time.time() - start < timeout:
            feed = (
                getattr(self.trader, "data_feed", None)
                or getattr(self, "data_feed", None)
            )
            if feed is None:
                await asyncio.sleep(0.1)
                continue
            activo = False
            if hasattr(feed, "activos"):
                try:
                    activo = bool(feed.activos)
                except TypeError:
                    try:
                        activo = bool(feed.activos())  # type: ignore[operator]
                    except Exception:
                        activo = False
            if not activo and hasattr(feed, "is_active"):
                try:
                    activo = bool(feed.is_active())
                except Exception:
                    activo = False
            if activo:
                return
            await asyncio.sleep(0.1)
        raise RuntimeError('WS no conectado')

    async def _check_clock_drift(self) -> bool:
        try:
            async with aiohttp.ClientSession() as session:
                request = session.get(
                    "https://api.binance.com/api/v3/time", timeout=5
                )
                if inspect.isawaitable(request):
                    ctx = await request
                else:
                    ctx = request
                if hasattr(ctx, "__aenter__") and hasattr(ctx, "__aexit__"):
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
        try:
            SNAPSHOT_PATH.parent.mkdir(parents=True, exist_ok=True)
            tmp = SNAPSHOT_PATH.parent / 'tmp_check'
            tmp.write_text('ok')
            tmp.unlink()
            return True
        except Exception:
            return False

    def _snapshot(self) -> None:
        assert self.config is not None
        data = {
            'symbols': self.config.symbols,
            'modo_real': self.config.modo_real,
            'timestamp': time.time(),
        }
        try:
            SNAPSHOT_PATH.parent.mkdir(parents=True, exist_ok=True)
            with open(SNAPSHOT_PATH, 'w') as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            self.log.error(f'No se pudo guardar snapshot: {e}')

    async def _stop_streams(self) -> None:
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
        if self.trader is not None:
            with suppress(Exception):
                await self.trader.cerrar()
