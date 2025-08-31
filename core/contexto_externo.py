"""Gestión de contexto fundamental recibido por streaming desde Binance."""
from __future__ import annotations
import asyncio
import json
from datetime import datetime, timezone
from typing import Awaitable, Callable, Dict, Iterable
import time
from math import isclose
import websockets
from binance_api.websocket import _keepalive
from core.utils.utils import configurar_logger
from core.supervisor import supervised_task, tick

UTC = timezone.utc

log = configurar_logger('contexto_externo')
_PUNTAJES: Dict[str, float] = {}
_DATOS_EXTERNOS: Dict[str, dict] = {}
CONTEXT_WS_URL = 'wss://stream.binance.com:9443/ws/{symbol}@kline_5m'


def obtener_puntaje_contexto(symbol: str) ->float:
    """Devuelve el último puntaje conocido para ``symbol``."""
    valor = _PUNTAJES.get(symbol)
    try:
        return float(valor) if valor is not None else 0.0
    except (TypeError, ValueError):
        return 0.0


def obtener_todos_puntajes() ->dict:
    """Devuelve todos los puntajes actuales almacenados."""
    return dict(_PUNTAJES)


def obtener_datos_externos(symbol: str) ->dict:
    """Devuelve los últimos datos externos conocidos para ``symbol``."""
    return dict(_DATOS_EXTERNOS.get(symbol, {}))


class StreamContexto:
    """Conecta con Binance y actualiza el contexto en tiempo real."""

    def __init__(
        self,
        url_template: str | None = None,
        monitor_interval: int = 30,
        inactivity_timeout: int = 300,
        cancel_timeout: float = 5,
    ) -> None:
        self.url_template = url_template or CONTEXT_WS_URL
        self.monitor_interval = max(1, monitor_interval)
        self.inactivity_timeout = inactivity_timeout
        self.cancel_timeout = cancel_timeout
        self._tasks: Dict[str, asyncio.Task] = {}
        self._last: Dict[str, datetime] = {}
        self._last_monotonic: Dict[str, float] = {}
        self._monitor_task: asyncio.Task | None = None
        self._handler_actual: Callable[[str, float], Awaitable[None]] | None = None
        self._running = False
        self._symbols: list[str] = []

    def actualizar_datos_externos(self, symbol: str, datos: dict) ->None:
        actual = _DATOS_EXTERNOS.setdefault(symbol, {})
        actual.update(datos)

    async def _stream(
        self, symbol: str, handler: Callable[[str, float], Awaitable[None]]
    ) -> None:
        symbol_norm = symbol.replace('/', '').lower()
        url = self.url_template.format(symbol=symbol_norm)
        try:
            async with websockets.connect(
                url,
                open_timeout=15,
                ping_interval=20,
                ping_timeout=10,
                close_timeout=5,
                max_queue=1000,
                compression="deflate",
            ) as ws:
                log.info(f'🔌 Contexto conectado para {symbol}')
                self._last[symbol] = datetime.now(UTC)
                self._last_monotonic[symbol] = time.monotonic()
                keeper = asyncio.create_task(_keepalive(ws, symbol, 20))
                try:
                    async for msg in ws:
                        try:
                            self._last[symbol] = datetime.now(UTC)
                            self._last_monotonic[symbol] = time.monotonic()
                            data = json.loads(msg)
                            vela = data.get('k')
                            if vela is None:
                                continue
                            close = float(vela.get('c', 0.0))
                            open_ = float(vela.get('o', 0.0))
                            vol = float(vela.get('v', 0.0))
                            if isclose(open_, 0.0, rel_tol=1e-12, abs_tol=1e-12) or vol < 1e-08:
                                continue
                            variacion_pct = (close - open_) / open_
                            puntaje = variacion_pct * vol
                            _PUNTAJES[symbol] = puntaje
                            try:
                                await handler(symbol, puntaje)
                                tick('context_stream')
                            except Exception as e:
                                log.warning(
                                    f'⚠️ Handler contexto {symbol} falló: {e}'
                                )
                        except asyncio.CancelledError:
                            log.info(
                                f'🛑 Stream contexto {symbol} cancelado (mensaje).'
                            )
                            raise
                        except Exception as e:
                            log.warning(
                                f'⚠️ Error procesando contexto de {symbol}: {e}'
                            )
                finally:
                    keeper.cancel()
                    try:
                        await keeper
                    except Exception as e:
                        log.debug(f'Error al esperar keepalive cancelado: {e}')
                    log.debug(f'Tareas activas tras cierre: {len(asyncio.all_tasks())}')
        except asyncio.CancelledError:
            log.info(f'🛑 Stream contexto {symbol} cancelado')
            raise
        except asyncio.TimeoutError as e:
            log.warning(
                f'⏱️ Timeout handshake contexto {symbol} en {url}: {e}'
            )
            raise
        except Exception as e:
            log.warning(
                f'⚠️ Stream de contexto {symbol} en {url} finalizó con error: {e}'
            )
            raise

    async def _monitor_inactividad(self) -> None:
        """Vigila la actividad de los streams y los reinicia al quedar inactivos."""
        try:
            while True:
                tick('context_monitor')
                await asyncio.sleep(self.monitor_interval)
                if not self._running:
                    break
                ahora = time.monotonic()
                for sym, task in list(self._tasks.items()):
                    ultimo = self._last_monotonic.get(sym)
                    if task.done():
                        log.warning(
                            f'🔄 Stream contexto {sym} finalizado; reiniciando'
                        )
                        self._tasks[sym] = supervised_task(
                            lambda sym=sym: self._stream(sym, self._handler_actual),
                            name=f"context_stream_{sym}",
                        )
                        continue
                    if ultimo is not None and (ahora - ultimo) > self.inactivity_timeout:
                        if sym not in _PUNTAJES:
                            log.debug(
                                f'⏳ Contexto {sym} sin datos recientes; omitiendo reinicio'
                            )
                            self._last[sym] = datetime.now(UTC)
                            self._last_monotonic[sym] = ahora
                            continue
                        log.warning(
                            f'🔄 Stream contexto {sym} inactivo; reiniciando'
                        )
                        task.cancel()
                        try:
                            await task
                        except Exception:
                            pass
                        self._tasks[sym] = supervised_task(
                            lambda sym=sym: self._stream(sym, self._handler_actual),
                            name=f"context_stream_{sym}",
                        )
        except asyncio.CancelledError:
            pass

    async def escuchar(self, symbols: Iterable[str], handler: Callable[[str, float], Awaitable[None]]) -> None:
        """Inicia un stream supervisado por cada símbolo."""
        await self.detener()
        self._handler_actual = handler
        self._symbols = list(symbols)
        self._running = True
        if self._monitor_task is None or self._monitor_task.done():
            self._monitor_task = asyncio.create_task(self._monitor_inactividad())
        for sym in self._symbols:
            if sym in self._tasks:
                log.warning(f'⚠️ Stream duplicado para {sym}. Ignorando.')
                continue
            self._tasks[sym] = supervised_task(
                lambda sym=sym: self._stream(sym, handler),
                name=f"context_stream_{sym}",
            )
        try:
            while self._running:
                await asyncio.sleep(1)
        finally:
            self._running = False

    async def detener(self) -> None:
        """Cancela todos los streams en ejecución."""
        if self._monitor_task and not self._monitor_task.done():
            self._monitor_task.cancel()
            try:
                await asyncio.wait_for(
                    self._monitor_task,
                    timeout=self.cancel_timeout,
                )
            except asyncio.TimeoutError:
                log.warning('🧟 Timeout cancelando monitor global (tarea zombie)')
            except Exception:
                pass
        self._monitor_task = None
        for task in self._tasks.values():
            task.cancel()
        for nombre, task in list(self._tasks.items()):
            if task.done():
                continue
            try:
                await asyncio.wait_for(
                    task,
                    timeout=self.cancel_timeout,
                )
            except asyncio.TimeoutError:
                log.warning(f'🧟 Timeout cancelando stream {nombre} (tarea zombie)')
            except Exception:
                pass
        self._tasks.clear()
        self._symbols = []


__all__ = ['StreamContexto', 'obtener_puntaje_contexto',
    'obtener_todos_puntajes', 'obtener_datos_externos']
