import asyncio
import json
import time
import os
from contextlib import suppress
from pathlib import Path
from typing import Optional
from dataclasses import replace

import aiohttp

from config.config_manager import ConfigManager, Config
from core.trader_modular import Trader
from core.utils.utils import configurar_logger
from core.data.bootstrap import warmup_inicial

SNAPSHOT_PATH = Path('estado/startup_snapshot.json')


class StartupManager:
    """Orquesta las fases de arranque del bot."""

    def __init__(self, trader: Optional[Trader] = None) -> None:
        self.trader = trader
        self.config: Optional[Config] = getattr(trader, 'config', None)
        self.task: Optional[asyncio.Task] = None
        self.log = configurar_logger('startup')

    async def run(self) -> tuple[Trader, asyncio.Task, Config]:
        executed = [self._stop_trader]
        try:
            await self._load_config()
            executed.append(self._stop_trader)
            await self._bootstrap()
            assert self.trader is not None
            if not self.trader.data_feed.verificar_continuidad():
                raise RuntimeError('Backfill inicial no contiguo')
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
        await self.trader._precargar_historico()

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
        self.task = asyncio.create_task(self.trader.ejecutar())

    async def _enable_strategies(self) -> None:
        assert self.trader is not None and self.config is not None
        await self._wait_ws(self.config.ws_timeout)
        if not await self._check_clock_drift():
            self.log.critical('Desincronización de reloj >500ms. Trading real deshabilitado')
            self.config = replace(self.config, modo_real=False)
            self.trader.config = self.config
            self.trader.modo_real = False
            self.trader.cliente = None
        if not await self._check_storage():
            raise RuntimeError(
                'Storage no disponible. '
                'Verifica los permisos de escritura en el directorio de datos.'
            )
        self.trader.habilitar_estrategias()
        self._snapshot()

    async def _wait_ws(self, timeout: float) -> None:
        assert self.trader is not None
        start = time.time()
        while time.time() - start < timeout:
            if self.trader.data_feed.activos:
                return
            await asyncio.sleep(0.1)
        raise RuntimeError('WS no conectado')

    async def _check_clock_drift(self) -> bool:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    "https://api.binance.com/api/v3/time", timeout=5
                ) as r:
                    data = await r.json()
            server = data.get("serverTime", 0) / 1000
            drift = abs(server - time.time())
            return drift < 0.5
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            self.log.warning(
                "No se pudo obtener la hora de Binance: %s. Omitiendo verificación de reloj.",
                e,
            )
            return True
        except Exception:
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
        if self.task is not None:
            self.task.cancel()
            with suppress(Exception):
                await self.task

    async def _stop_trader(self) -> None:
        if self.trader is not None:
            with suppress(Exception):
                await self.trader.cerrar()
