from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Dict


@dataclass(slots=True)
class FakeCandle:
    """Pequeña representación de una vela para los tests."""

    open_ts: int
    close_ts: int
    symbol: str
    timeframe: str
    open: float = 0.0
    high: float = 0.0
    low: float = 0.0
    close: float = 0.0
    volume: float = 0.0
    extra: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convierte la vela en el formato esperado por DataFeed/Trader."""

        payload: Dict[str, Any] = {
            "symbol": self.symbol.upper(),
            "timeframe": self.timeframe,
            "open_time": self.open_ts,
            "close_time": self.close_ts,
            "timestamp": self.close_ts,
            "event_time": self.close_ts,
            "open": float(self.open),
            "high": float(self.high or self.open),
            "low": float(self.low or self.open),
            "close": float(self.close or self.open),
            "volume": float(self.volume),
            "is_closed": True,
        }
        payload.update(self.extra)
        return payload


def _resolve_prices(base: float) -> Dict[str, float]:
    return {
        "open": base,
        "high": base * 1.01,
        "low": base * 0.99,
        "close": base,
        "volume": 10.0,
    }


def mk_candle_in_range(
    open_ts: int,
    *,
    interval_secs: int = 300,
    symbol: str = "BTCUSDT",
    timeframe: str = "5m",
    price: float = 25_000.0,
    **overrides: Any,
) -> FakeCandle:
    """Genera una vela cerrada dentro del rango temporal aceptado."""

    close_ts = open_ts + interval_secs
    prices = _resolve_prices(float(price))
    prices.update({k: v for k, v in overrides.items() if k in prices})
    extra = {k: v for k, v in overrides.items() if k not in prices}
    return FakeCandle(
        open_ts=open_ts,
        close_ts=close_ts,
        symbol=symbol,
        timeframe=timeframe,
        **prices,
        extra=extra,
    )


def mk_candle_in_future(
    *,
    seconds_ahead: int = 600,
    interval_secs: int = 300,
    symbol: str = "BTCUSDT",
    timeframe: str = "5m",
    price: float = 25_000.0,
    **overrides: Any,
) -> FakeCandle:
    """Genera una vela cuyo cierre se encuentra en el futuro inmediato."""

    base = int(time.time()) + max(1, seconds_ahead)
    return mk_candle_in_range(
        base,
        interval_secs=interval_secs,
        symbol=symbol,
        timeframe=timeframe,
        price=price,
        **overrides,
    )


def mk_candle_out_of_range(
    *,
    seconds_behind: int = 3_600,
    interval_secs: int = 300,
    symbol: str = "BTCUSDT",
    timeframe: str = "5m",
    price: float = 25_000.0,
    **overrides: Any,
) -> FakeCandle:
    """Genera una vela con timestamps claramente fuera del rango admitido."""

    base = int(time.time()) - max(interval_secs, seconds_behind)
    # Retrocedemos dos intervalos completos para disparar out_of_range.
    open_ts = base - interval_secs
    return mk_candle_in_range(
        open_ts,
        interval_secs=interval_secs,
        symbol=symbol,
        timeframe=timeframe,
        price=price,
        **overrides,
    )
