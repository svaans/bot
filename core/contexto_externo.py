"""Gestión de contexto fundamental recibido por streaming desde Binance."""
from __future__ import annotations
import asyncio
import json
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

    def __init__(self, url_template: (str | None)=None) ->None:
        log.info('➡️ Entrando en __init__()')
        self.url_template = url_template or CONTEXT_WS_URL
        self._tasks: Dict[str, asyncio.Task] = {}

    async def _stream(
        self, symbol: str, handler: Callable[[str, float], Awaitable[None]]
    ) -> None:
        log.info('➡️ Entrando en _stream()')
        symbol_norm = symbol.replace('/', '').lower()
        url = self.url_template.format(symbol=symbol_norm)
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                log.info(f'🔌 Contexto conectado para {symbol}')
                async for msg in ws:
                    try:
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

    async def escuchar(self, symbols: Iterable[str], handler: Callable[[str,
        float], Awaitable[None]]) ->None:
        log.info('➡️ Entrando en escuchar()')
        """Inicia un stream supervisado por cada símbolo."""
        for sym in symbols:
            self._tasks[sym] = supervised_task(
                lambda sym=sym: self._stream(sym, handler),
                name=f"context_stream_{sym}",
            )
        await asyncio.gather(*self._tasks.values())

    async def detener(self) ->None:
        log.info('➡️ Entrando en detener()')
        """Cancela todos los streams en ejecución."""
        for task in self._tasks.values():
            task.cancel()
        await asyncio.gather(*self._tasks.values(), return_exceptions=True)


__all__ = ['StreamContexto', 'obtener_puntaje_contexto',
    'obtener_todos_puntajes']
