"""Conectores para feeds externos de funding, open interest y noticias."""
from __future__ import annotations

import asyncio
import json
import time
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Dict, Iterable

import aiohttp
import websockets

from core.contexto_externo import StreamContexto
from core.supervisor import beat
from core.metrics import (
    registrar_feed_funding_missing,
    registrar_feed_open_interest_missing,
)
from core.utils.utils import configurar_logger

log = configurar_logger('external_feeds')
UTC = timezone.utc
CACHE_TTL = 600  # 10 minutos


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


_futures_symbols_global: set[str] | None = None


async def es_futuros(symbol: str, session: aiohttp.ClientSession | None = None) -> bool:
    """Autodetecta si un símbolo existe en los mercados de futuros de Binance."""

    global _futures_symbols_global
    symbol = symbol.upper()
    if _futures_symbols_global is not None:
        return symbol in _futures_symbols_global

    close_session = False
    if session is None:
        session = aiohttp.ClientSession()
        close_session = True
    symbols: set[str] = set()
    for url in [
        "https://fapi.binance.com/fapi/v1/exchangeInfo",
        "https://dapi.binance.com/dapi/v1/exchangeInfo",
    ]:
        try:
            async with session.get(url) as resp:
                data = await resp.json()
            symbols.update({s.get("symbol") for s in data.get("symbols", [])})
        except Exception:
            continue
    if close_session:
        await session.close()
    _futures_symbols_global = symbols
    return symbol in _futures_symbols_global


class ExternalFeeds:
    """Gestiona la obtención periódica de datos externos."""

    def __init__(self, stream: StreamContexto | None = None, session: aiohttp.ClientSession | None = None) -> None:
        self.stream = stream
        self.session = session
        self._tasks: list[asyncio.Task] = []
        self._running = False
        self._funding_cache: Dict[str, tuple[float, Dict[str, Any]]] = {}
        self._oi_cache: Dict[str, tuple[float, Dict[str, Any]]] = {}
        self._funding_permanent_missing: set[str] = set()
        self._oi_permanent_missing: set[str] = set()
        self._not_applicable_logged: set[str] = set()
        self._futures_symbols: set[str] | None = None

    async def _ensure_session(self) -> None:
        if self.session is None:
            self.session = aiohttp.ClientSession()

    async def es_futuros(self, symbol: str) -> bool:
        """Determina si el símbolo es de futuros en Binance."""

        if self._futures_symbols is not None:
            return symbol.upper() in self._futures_symbols
        result = await es_futuros(symbol, self.session)
        self._futures_symbols = _futures_symbols_global or set()
        return result

    async def funding_rate_rest(self, symbol: str) -> tuple[Dict[str, Any] | None, str]:
        if symbol in self._funding_permanent_missing:
            return None, "sin_datos"
        now = time.time()
        cached = self._funding_cache.get(symbol)
        if cached and cached[0] > now:
            return cached[1], "ok"
        await self._ensure_session()
        url = f"https://fapi.binance.com/fapi/v1/fundingRate?symbol={symbol}&limit=1"
        delay = 1
        for intento in range(3):
            try:
                async with self.session.get(url) as resp:
                    if resp.status == 404:
                        self._funding_permanent_missing.add(symbol)
                        registrar_feed_funding_missing(symbol, "not_listed")
                        log.info(f"ℹ️ Funding rate no listado {symbol}")
                        return None, "sin_datos"
                    if resp.status == 429:
                        await asyncio.sleep(delay)
                        return None, "rate_limit"
                    data = await resp.json()
                if isinstance(data, list):
                    data = data[0] if data else None
                if data:
                    self._funding_cache[symbol] = (time.time() + CACHE_TTL, data)
                return data, "ok" if data else "sin_datos"
            except Exception:
                if intento == 2:
                    registrar_feed_funding_missing(symbol, "unavailable")
                    return None, "backoff"
                await asyncio.sleep(delay)
                delay *= 2
        registrar_feed_funding_missing(symbol, "unavailable")
        return None, "backoff"

    async def open_interest_rest(self, symbol: str) -> tuple[Dict[str, Any] | None, str]:
        if symbol in self._oi_permanent_missing:
            return None, "sin_datos"
        now = time.time()
        cached = self._oi_cache.get(symbol)
        if cached and cached[0] > now:
            return cached[1], "ok"
        await self._ensure_session()
        url = (
            "https://fapi.binance.com/futures/data/openInterestHist?"
            f"symbol={symbol}&period=5m&limit=1"
        )
        delay = 1
        for intento in range(3):
            try:
                async with self.session.get(url) as resp:
                    if resp.status == 404:
                        self._oi_permanent_missing.add(symbol)
                        registrar_feed_open_interest_missing(symbol, "not_listed")
                        log.info(f"ℹ️ Open interest no listado {symbol}")
                        return None, "sin_datos"
                    if resp.status == 429:
                        await asyncio.sleep(delay)
                        return None, "rate_limit"
                    data = await resp.json()
                if isinstance(data, list):
                    data = data[0] if data else None
                if data:
                    self._oi_cache[symbol] = (time.time() + CACHE_TTL, data)
                return data, "ok" if data else "sin_datos"
            except Exception:
                if intento == 2:
                    registrar_feed_open_interest_missing(symbol, "unavailable")
                    return None, "backoff"
                await asyncio.sleep(delay)
                delay *= 2
        registrar_feed_open_interest_missing(symbol, "unavailable")
        return None, "backoff"

    async def news_ws(self, url: str):
        async with websockets.connect(url, ping_interval=None, ping_timeout=None) as ws:
            async for msg in ws:
                yield json.loads(msg)

    async def _poll_funding(self, symbol: str, interval: int) -> None:
        while self._running:
            if symbol in self._funding_permanent_missing:
                beat('external_feeds', 'sin_datos')
                return
            try:
                raw, cause = await self.funding_rate_rest(symbol)
                if not raw:
                    beat('external_feeds', cause)
                    if symbol in self._funding_permanent_missing:
                        return
                    log.warning(f'⚠️ Funding rate no disponible {symbol}')
                else:
                    dato = normalizar_funding_rate(raw)
                    if self.stream:
                        self.stream.actualizar_datos_externos(symbol, {'funding_rate': dato})
                    beat('external_feeds')
            except asyncio.CancelledError:
                raise
            except Exception as e:
                beat('external_feeds', 'backoff')
                log.warning(f'⚠️ Funding rate falló {symbol}: {e}')
            await asyncio.sleep(interval)

    async def _poll_open_interest(self, symbol: str, interval: int) -> None:
        while self._running:
            if symbol in self._oi_permanent_missing:
                beat('external_feeds', 'sin_datos')
                return
            try:
                raw, cause = await self.open_interest_rest(symbol)
                if not raw:
                    beat('external_feeds', cause)
                    if symbol in self._oi_permanent_missing:
                        return
                    log.warning(f'⚠️ Open interest no disponible {symbol}')
                else:
                    dato = normalizar_open_interest(raw)
                    if self.stream:
                        self.stream.actualizar_datos_externos(symbol, {'open_interest': dato})
                    beat('external_feeds')
            except asyncio.CancelledError:
                raise
            except Exception as e:
                beat('external_feeds', 'backoff')
                log.warning(f'⚠️ Open interest falló {symbol}: {e}')
            await asyncio.sleep(interval)

    async def _listen_news(self, url: str) -> None:
        async for raw in self.news_ws(url):
            try:
                dato = normalizar_noticia(raw)
                symbol = dato.get('symbol') or 'GLOBAL'
                if self.stream:
                    self.stream.actualizar_datos_externos(symbol, {'news': dato})
                beat('external_feeds')
            except asyncio.CancelledError:
                raise
            except Exception as e:
                beat('external_feeds', 'backoff')
                log.warning(f'⚠️ Error procesando noticia: {e}')

    async def escuchar(self, symbols: Iterable[str], interval: int = 60, news_url: str | None = None) -> None:
        """Inicia tareas para escuchar los distintos feeds."""
        self._running = True
        for sym in symbols:
            if not await self.es_futuros(sym):
                if sym not in self._not_applicable_logged:
                    log.info(f"ℹ️ Derivatives no aplican para {sym} (spot)")
                    registrar_feed_funding_missing(sym, "not_applicable")
                    registrar_feed_open_interest_missing(sym, "not_applicable")
                    self._not_applicable_logged.add(sym)
                continue
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
    'es_futuros',
]
