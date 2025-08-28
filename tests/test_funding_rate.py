import asyncio

import pytest
from aioresponses import aioresponses

from core.funding_rate import obtener_funding, FUTURES_MAPPING


@pytest.mark.asyncio
async def test_no_mapping_returns_expected_result():
    result = await obtener_funding("XRP/EUR")
    assert not result.available
    assert result.reason == "NO_MAPPING"
    assert result.mapped_symbol is None


@pytest.mark.asyncio
async def test_successful_fetch_parses_rate():
    spot = "BTC/EUR"
    mapping = FUTURES_MAPPING[spot]
    url = f"https://fapi.binance.com/fapi/v1/premiumIndex?symbol={mapping['symbol']}"
    with aioresponses() as m:
        m.get(url, payload={"lastFundingRate": "0.0001"})
        result = await obtener_funding(spot)
    assert result.available
    assert result.rate == pytest.approx(0.0001)
    assert result.mapped_symbol == mapping['symbol']
    assert result.segment == mapping['segment']


@pytest.mark.asyncio
async def test_exchange_404():
    spot = "BTC/EUR"
    mapping = FUTURES_MAPPING[spot]
    url = f"https://fapi.binance.com/fapi/v1/premiumIndex?symbol={mapping['symbol']}"
    with aioresponses() as m:
        m.get(url, status=404)
        result = await obtener_funding(spot)
    assert not result.available
    assert result.reason == "EXCHANGE_404"


@pytest.mark.asyncio
async def test_timeout_handled_gracefully():
    spot = "BTC/EUR"
    mapping = FUTURES_MAPPING[spot]
    url = f"https://fapi.binance.com/fapi/v1/premiumIndex?symbol={mapping['symbol']}"
    async def raise_timeout(url, **kwargs):
        raise asyncio.TimeoutError

    with aioresponses() as m:
        m.get(url, callback=raise_timeout)
        result = await obtener_funding(spot)
    assert not result.available
    assert result.reason == "TIMEOUT"
