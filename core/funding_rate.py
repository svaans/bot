from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional, Dict

import aiohttp
from aiohttp.client_exceptions import ServerTimeoutError

from core.utils import configurar_logger

try:  # pragma: no cover - métricas opcionales
    from core.metrics import registrar_feed_funding_missing
except Exception:  # pragma: no cover - entorno minimalista
    def registrar_feed_funding_missing(*_args, **_kwargs) -> None:  # type: ignore[override]
        return None

log = configurar_logger('funding_rate')


@dataclass(frozen=True)
class FundingResult:
    symbol_spot: str
    mapped_symbol: Optional[str]
    segment: Optional[str]
    available: bool
    rate: Optional[float]
    fetched_at: datetime
    reason: Optional[str]
    source: str


FUTURES_MAPPING: Dict[str, Dict[str, str]] = {
    "BTC/EUR": {"symbol": "BTCUSDT", "segment": "usdtm"},
    "ETH/EUR": {"symbol": "ETHUSDT", "segment": "usdtm"},
    "ADA/EUR": {"symbol": "ADAUSDT", "segment": "usdtm"},
    "BNB/EUR": {"symbol": "BNBUSDT", "segment": "usdtm"},
    "SOL/EUR": {"symbol": "SOLUSDT", "segment": "usdtm"},
    # Soporta símbolos spot directos sin separador.
    "BTCUSDT": {"symbol": "BTCUSDT", "segment": "usdtm"},
    "ETHUSDT": {"symbol": "ETHUSDT", "segment": "usdtm"},
    "ADAUSDT": {"symbol": "ADAUSDT", "segment": "usdtm"},
    "BNBUSDT": {"symbol": "BNBUSDT", "segment": "usdtm"},
    "SOLUSDT": {"symbol": "SOLUSDT", "segment": "usdtm"},
}


class FundingClient:
    """Cliente asíncrono para obtener funding rate de Binance Futures."""

    RETRIES = 2
    ENDPOINTS = {
        "usdtm": "https://fapi.binance.com/fapi/v1/premiumIndex",
        "coinm": "https://dapi.binance.com/dapi/v1/premiumIndex",
    }

    def __init__(self, session: aiohttp.ClientSession | None = None) -> None:
        self._session = session or aiohttp.ClientSession()
        self._own_session = session is None

    async def close(self) -> None:
        if self._own_session:
            await self._session.close()

    async def fetch(self, symbol_spot: str, mapped_symbol: str, segment: str) -> FundingResult:
        url_base = self.ENDPOINTS[segment]
        url = f"{url_base}?symbol={mapped_symbol}"
        backoff = 1.0
        reason = "UNKNOWN"
        for attempt in range(self.RETRIES + 1):
            try:
                async with self._session.get(url, timeout=10) as resp:
                    if resp.status == 404:
                        reason = "EXCHANGE_404"
                        break
                    resp.raise_for_status()
                    data = await resp.json()
                    rate = float(data.get("lastFundingRate", 0.0))
                    return FundingResult(
                        symbol_spot=symbol_spot,
                        mapped_symbol=mapped_symbol,
                        segment=segment,
                        available=True,
                        rate=rate,
                        fetched_at=datetime.now(timezone.utc),
                        reason=None,
                        source="binance",
                    )
            except (asyncio.TimeoutError, ServerTimeoutError):
                reason = "TIMEOUT"
            except aiohttp.ClientError as exc:
                text = f"{exc.__class__.__name__}:{exc}"
                reason = "TIMEOUT" if "Timeout" in text else "NETWORK"
            if attempt < self.RETRIES:
                await asyncio.sleep(backoff)
                backoff *= 2
                continue
            break
        return FundingResult(
            symbol_spot=symbol_spot,
            mapped_symbol=mapped_symbol,
            segment=segment,
            available=False,
            rate=None,
            fetched_at=datetime.now(timezone.utc),
            reason=reason,
            source="binance",
        )


async def obtener_funding(symbol_spot: str) -> FundingResult:
    mapping = FUTURES_MAPPING.get(symbol_spot)
    if not mapping:
        result = FundingResult(
            symbol_spot=symbol_spot,
            mapped_symbol=None,
            segment=None,
            available=False,
            rate=None,
            fetched_at=datetime.now(timezone.utc),
            reason="NO_MAPPING",
            source="binance",
        )
        registrar_feed_funding_missing(symbol_spot, "no_mapping")
        log.info("Sin mapping de futuros para %s", symbol_spot)
        return result
    client = FundingClient()
    try:
        result = await client.fetch(symbol_spot, mapping["symbol"], mapping["segment"])
    finally:
        await client.close()
    if not result.available:
        reason = (result.reason or "unknown").lower()
        registrar_feed_funding_missing(symbol_spot, reason)
        level = log.info if result.reason == "NO_MAPPING" else log.warning
        level("Funding rate no disponible para %s: %s", symbol_spot, result.reason)
    return result
