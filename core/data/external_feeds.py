"""Conectores para feeds externos de funding, open interest y noticias."""
from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Dict, Iterable

import aiohttp
import websockets

from core.contexto_externo import StreamContexto
from core.utils.utils import configurar_logger

log = configurar_logger('external_feeds')
UTC = timezone.utc


def _now_ts() -> int:
    return int(datetime.now(UTC).timestamp() * 1000)


def normalizar_funding_rate(raw: Dict[str, Any]) -> Dict[str, Any]:
    """Normaliza respuesta de funding rate."""
    return {
        'type': 'funding_rate',
        'symbol': raw.get('symbol'),
        'value': float(raw.get('fundingRate', 0.0)),
        'timestamp': int(raw.get('fundingTime') or _now_ts()),
    }


def normalizar_open_interest(raw: Dict[str, Any]) -> Dict[str, Any]:
    """Normaliza respuesta de open interest."""
    return {
        'type': 'open_interest',
        'symbol': raw.get('symbol'),
        'value': float(raw.get('openInterest') or raw.get('sumOpenInterest') or 0.0),
        'timestamp': int(raw.get('timestamp') or raw.get('time') or _now_ts()),
    }


def normalizar_noticia(raw: Dict[str, Any]) -> Dict[str, Any]:
    """Normaliza mensajes de noticias o alertas."""
    return {
        'type': 'news',
        'symbol': raw.get('symbol'),
        'title': raw.get('title'),
        'body': raw.get('body'),
        'timestamp': int(raw.get('timestamp') or _now_ts()),
    }


class ExternalFeeds:
    """Gestiona la obtención periódica de datos externos."""

    def __init__(self, stream: StreamContexto | None = None, session: aiohttp.ClientSession | None = None) -> None:
        self.stream = stream
        self.session = session
        self._tasks: list[asyncio.Task] = []
        self._running = False

    async def _ensure_session(self) -> None:
        if self.session is None:
            self.session = aiohttp.ClientSession()

    async def funding_rate_rest(self, symbol: str) -> Dict[str, Any]:
        await self._ensure_session()
        url = f"https://fapi.binance.com/fapi/v1/fundingRate?symbol={symbol}&limit=1"
        async with self.session.get(url) as resp:
            data = await resp.json()
        return data[0] if isinstance(data, list) else data

    async def open_interest_rest(self, symbol: str) -> Dict[str, Any]:
        await self._ensure_session()
        url = (
            "https://fapi.binance.com/futures/data/openInterestHist?"
            f"symbol={symbol}&period=5m&limit=1"
        )
        async with self.session.get(url) as resp:
            data = await resp.json()
        return data[0] if isinstance(data, list) else data

    async def news_ws(self, url: str):
        async with websockets.connect(url, ping_interval=None, ping_timeout=None) as ws:
            async for msg in ws:
                yield json.loads(msg)

    async def _poll_funding(self, symbol: str, interval: int) -> None:
        while self._running:
            try:
                raw = await self.funding_rate_rest(symbol)
                dato = normalizar_funding_rate(raw)
                if self.stream:
                    self.stream.actualizar_datos_externos(symbol, {'funding_rate': dato})
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.warning(f'⚠️ Funding rate falló {symbol}: {e}')
            await asyncio.sleep(interval)

    async def _poll_open_interest(self, symbol: str, interval: int) -> None:
        while self._running:
            try:
                raw = await self.open_interest_rest(symbol)
                dato = normalizar_open_interest(raw)
                if self.stream:
                    self.stream.actualizar_datos_externos(symbol, {'open_interest': dato})
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.warning(f'⚠️ Open interest falló {symbol}: {e}')
            await asyncio.sleep(interval)

    async def _listen_news(self, url: str) -> None:
        async for raw in self.news_ws(url):
            try:
                dato = normalizar_noticia(raw)
                symbol = dato.get('symbol') or 'GLOBAL'
                if self.stream:
                    self.stream.actualizar_datos_externos(symbol, {'news': dato})
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.warning(f'⚠️ Error procesando noticia: {e}')

    async def escuchar(self, symbols: Iterable[str], interval: int = 60, news_url: str | None = None) -> None:
        """Inicia tareas para escuchar los distintos feeds."""
        self._running = True
        for sym in symbols:
            self._tasks.append(asyncio.create_task(self._poll_funding(sym, interval)))
            self._tasks.append(asyncio.create_task(self._poll_open_interest(sym, interval)))
        if news_url:
            self._tasks.append(asyncio.create_task(self._listen_news(news_url)))
        try:
            await asyncio.gather(*self._tasks)
        finally:
            self._running = False

    async def detener(self) -> None:
        self._running = False
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()
        if self.session:
            await self.session.close()
            self.session = None


__all__ = [
    'ExternalFeeds',
    'normalizar_funding_rate',
    'normalizar_open_interest',
    'normalizar_noticia',
]
