"""Gestión de contexto fundamental recibido por streaming desde Binance."""
from __future__ import annotations
import asyncio
import json
from datetime import datetime
from typing import Awaitable, Callable, Dict, Iterable
import websockets
from core.utils.utils import configurar_logger
from core.supervisor import supervised_task, tick
log = configurar_logger('contexto_externo')
_PUNTAJES: Dict[str, float] = {}
CONTEXT_WS_URL = 'wss://stream.binance.com:9443/ws/{symbol}@kline_1m'


def obtener_puntaje_contexto(symbol: str) ->float:
    log.info('➡️ Entrando en obtener_puntaje_contexto()')
    """Devuelve el último puntaje conocido para ``symbol``."""
    valor = _PUNTAJES.get(symbol)
    try:
        return float(valor) if valor is not None else 0.0
    except (TypeError, ValueError):
        return 0.0


def obtener_todos_puntajes() ->dict:
    log.info('➡️ Entrando en obtener_todos_puntajes()')
    """Devuelve todos los puntajes actuales almacenados."""
    return dict(_PUNTAJES)


class StreamContexto:
    """Conecta con Binance y actualiza el contexto en tiempo real."""

    def __init__(
        self,
        url_template: str | None = None,
        monitor_interval: int = 30,
        inactivity_timeout: int = 300,
    ) -> None:
        log.info('➡️ Entrando en __init__()')
        self.url_template = url_template or CONTEXT_WS_URL
        self.monitor_interval = max(1, monitor_interval)
        self.inactivity_timeout = inactivity_timeout
        self._tasks: Dict[str, asyncio.Task] = {}
        self._last: Dict[str, datetime] = {}
        self._monitor_task: asyncio.Task | None = None
        self._handler_actual: Callable[[str, float], Awaitable[None]] | None = None
        self._running = False
        self._symbols: list[str] = []

    async def _stream(
        self, symbol: str, handler: Callable[[str, float], Awaitable[None]]
    ) -> None:
        log.info('➡️ Entrando en _stream()')
        symbol_norm = symbol.replace('/', '').lower()
        url = self.url_template.format(symbol=symbol_norm)
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                log.info(f'🔌 Contexto conectado para {symbol}')
                self._last[symbol] = datetime.utcnow()
                async for msg in ws:
                    try:
                        self._last[symbol] = datetime.utcnow()
                        data = json.loads(msg)
                        vela = data.get('k')
                        if vela is None:
                            continue
                        close = float(vela.get('c', 0.0))
                        open_ = float(vela.get('o', 0.0))
                        vol = float(vela.get('v', 0.0))
                        if open_ == 0 or vol < 1e-08:
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
        except asyncio.CancelledError:
            log.info(f'🛑 Stream contexto {symbol} cancelado')
            raise
        except Exception as e:
            log.warning(
                f'⚠️ Stream de contexto {symbol} finalizó con error: {e}'
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
                ahora = datetime.utcnow()
                for sym, task in list(self._tasks.items()):
                    ultimo = self._last.get(sym)
                    if task.done() or (
                        ultimo
                        and (ahora - ultimo).total_seconds() > self.inactivity_timeout
                    ):
                        log.warning(
                            f'🔄 Stream contexto {sym} inactivo o finalizado; reiniciando'
                        )
                        if not task.done():
                            task.cancel()
                            await asyncio.gather(task, return_exceptions=True)
                        self._tasks[sym] = supervised_task(
                            lambda sym=sym: self._stream(sym, self._handler_actual),
                            name=f"context_stream_{sym}",
                        )
        except asyncio.CancelledError:
            pass

    async def escuchar(self, symbols: Iterable[str], handler: Callable[[str, float], Awaitable[None]]) -> None:
        log.info('➡️ Entrando en escuchar()')
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
        if self._tasks:
            await asyncio.gather(*self._tasks.values())
        self._running = False

    async def detener(self) -> None:
        log.info('➡️ Entrando en detener()')
        """Cancela todos los streams en ejecución."""
        for task in self._tasks.values():
            task.cancel()
        await asyncio.gather(*self._tasks.values(), return_exceptions=True)
        self._tasks.clear()
        self._symbols = []
        if self._monitor_task and not self._monitor_task.done():
            self._monitor_task.cancel()
        await asyncio.gather(
            *(t for t in [self._monitor_task] if t), return_exceptions=True
        )
        self._tasks.clear()


__all__ = ['StreamContexto', 'obtener_puntaje_contexto',
    'obtener_todos_puntajes']
