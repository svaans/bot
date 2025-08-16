import pandas as pd
import pytest

from core.strategies.tendencia import obtener_tendencia


def _df_trend(direction: str) -> pd.DataFrame:
    if direction == 'up':
        prices = list(range(100))
        close = [p + 0.5 for p in prices]
    else:
        prices = list(range(100, 0, -1))
        close = [p - 0.5 for p in prices]
    data = {
        'open': prices,
        'high': [p + 1 for p in prices],
        'low': [p - 1 for p in prices],
        'close': close,
    }
    return pd.DataFrame(data)


@pytest.mark.asyncio
async def test_obtener_tendencia_alcista():
    df = _df_trend('up')
    assert obtener_tendencia('BTC/USDT', df) == 'alcista'


@pytest.mark.asyncio
async def test_obtener_tendencia_bajista():
    df = _df_trend('down')
    assert obtener_tendencia('BTC/USDT', df) == 'bajista'