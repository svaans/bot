from __future__ import annotations

import numpy as np

try:
    from .fast_indicators_ext import rsi, atr, slope
    HAS_FAST = True
except Exception:  # pragma: no cover - extension optional
    HAS_FAST = False

    def rsi(close: np.ndarray, periodo: int) -> np.ndarray:
        n = close.size
        if n < periodo + 1:
            return np.empty(0, dtype=float)
        res = np.full(n, np.nan, dtype=float)
        alpha = 1.0 / periodo
        prev_gain = np.nan
        prev_loss = np.nan
        for i in range(1, n):
            cambio = close[i] - close[i - 1]
            gain = cambio if cambio > 0 else 0.0
            loss = -cambio if cambio < 0 else 0.0
            if np.isnan(prev_gain):
                prev_gain = gain
                prev_loss = loss
            else:
                prev_gain = (1.0 - alpha) * prev_gain + alpha * gain
                prev_loss = (1.0 - alpha) * prev_loss + alpha * loss
            if i >= periodo:
                rs = prev_gain / prev_loss
                res[i] = 100.0 - 100.0 / (1.0 + rs)
        return res

    def atr(high: np.ndarray, low: np.ndarray, close: np.ndarray, periodo: int) -> float:
        n = high.size
        if n < periodo + 1:
            return float("nan")
        tr = np.empty(n, dtype=float)
        tr[0] = high[0] - low[0]
        for i in range(1, n):
            tr1 = high[i] - low[i]
            tr2 = abs(high[i] - close[i - 1])
            tr3 = abs(low[i] - close[i - 1])
            tr[i] = max(tr1, tr2, tr3)
        alpha = 1.0 / periodo
        ema = tr[0]
        for i in range(1, n):
            ema = (1.0 - alpha) * ema + alpha * tr[i]
        return float(ema)

    def slope(close: np.ndarray, periodo: int) -> float:
        if close.size < periodo or periodo < 2:
            return 0.0
        y = close[-periodo:]
        x = np.arange(periodo)
        m, _ = np.polyfit(x, y, 1)
        return float(m)

__all__ = ["rsi", "atr", "slope", "HAS_FAST"]