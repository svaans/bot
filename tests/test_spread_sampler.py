from __future__ import annotations

import asyncio

import pytest

from core.data_feed.spread_sampler import SpreadSampler, SpreadSample


@pytest.mark.asyncio
async def test_spread_sampler_caches_results() -> None:
    sampler = SpreadSampler(ttl=5.0)
    calls = 0

    async def fake_fetch(_cliente: object | None, symbol: str) -> dict[str, float]:
        nonlocal calls
        calls += 1
        assert symbol == "BTCUSDT"
        return {"bid": 100.0, "ask": 100.1, "timestamp": 123456, "source": "test"}

    sample1 = await sampler.sample("BTCUSDT", fake_fetch, cliente=None)
    assert isinstance(sample1, SpreadSample)
    assert sample1.ratio == pytest.approx((100.1 - 100.0) / 100.05)
    assert calls == 1

    sample2 = await sampler.sample("BTCUSDT", fake_fetch, cliente=None)
    assert sample2 is sample1
    assert calls == 1, "La segunda llamada debe usar la caché"


@pytest.mark.asyncio
async def test_spread_sampler_invalid_data_returns_none() -> None:
    sampler = SpreadSampler(ttl=5.0)

    async def fake_fetch(_cliente: object | None, _symbol: str) -> dict[str, float]:
        return {"bid": None, "ask": None}

    sample = await sampler.sample("ETHUSDT", fake_fetch, cliente=None)
    assert sample is None


@pytest.mark.asyncio
async def test_spread_sampler_concurrent_calls_share_fetch(monkeypatch: pytest.MonkeyPatch) -> None:
    sampler = SpreadSampler(ttl=0.5)
    calls = 0

    async def fake_fetch(_cliente: object | None, symbol: str) -> dict[str, float]:
        nonlocal calls
        calls += 1
        await asyncio.sleep(0.01)
        return {"bid": 50.0, "ask": 50.05, "timestamp": 999, "source": "test"}

    results = await asyncio.gather(
        sampler.sample("BNBUSDT", fake_fetch, cliente=None),
        sampler.sample("BNBUSDT", fake_fetch, cliente=None),
    )

    assert all(isinstance(result, SpreadSample) for result in results)
    assert calls == 1
