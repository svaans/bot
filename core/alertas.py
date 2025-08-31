from __future__ import annotations

import time
from collections import defaultdict, deque
from typing import Dict, Tuple, DefaultDict

from core.utils.logger import configurar_logger

log = configurar_logger("alertas")

class RateAlertManager:
    """Rastrea eventos para calcular tasas y detectar degradaciones sostenidas."""

    def __init__(
        self,
        window: int = 60,
        hysteresis: int = 3,
        thresholds: Dict[str, float] | None = None,
    ) -> None:
        self.window = window
        self.hysteresis = hysteresis
        self.thresholds: Dict[str, float] = thresholds or {}
        self.events: DefaultDict[Tuple[str, str], deque[float]] = defaultdict(deque)
        self.breaches: DefaultDict[Tuple[str, str], int] = defaultdict(int)

    def _purge(self, dq: deque[float], now: float) -> None:
        while dq and now - dq[0] > self.window:
            dq.popleft()

    def record(self, category: str, symbol: str, count: int = 1) -> float:
        """Registra ``count`` eventos y retorna la tasa por segundo."""

        now = time.time()
        key = (symbol, category)
        dq = self.events[key]
        for _ in range(count):
            dq.append(now)
        self._purge(dq, now)
        rate = len(dq) / self.window
        threshold = self.thresholds.get(category)
        if threshold is not None:
            if rate > threshold:
                self.breaches[key] += 1
            else:
                self.breaches[key] = 0
        return rate

    def should_alert(self, category: str, symbol: str) -> bool:
        key = (symbol, category)
        return self.breaches.get(key, 0) >= self.hysteresis

    def summary(self, seconds: int) -> Dict[str, Dict[str, int]]:
        """Retorna conteos por símbolo y categoría en ``seconds`` recientes."""

        now = time.time()
        cutoff = now - seconds
        data: DefaultDict[str, DefaultDict[str, int]] = defaultdict(lambda: defaultdict(int))
        for (symbol, category), dq in self.events.items():
            count = sum(1 for t in dq if t >= cutoff)
            if count:
                data[symbol][category] = count
        return {s: dict(c) for s, c in data.items()}

    def format_summary(self, seconds: int) -> str:
        """Devuelve un resumen legible de eventos recientes."""

        data = self.summary(seconds)
        if not data:
            return "Sin eventos"
        lineas = []
        for symbol, categorias in sorted(data.items()):
            partes = ", ".join(
                f"{cat}={cnt}" for cat, cnt in sorted(categorias.items())
            )
            lineas.append(f"{symbol}: {partes}")
        return "\n".join(lineas)


# Umbrales por segundo (~1/min) ajustables via variables de entorno
DEFAULT_THRESHOLDS = {
    "candles_duplicates": 1 / 60,
    "feeds_missing": 1 / 60,
    "watchdog_restart": 1 / 60,
}

alert_manager = RateAlertManager(thresholds=DEFAULT_THRESHOLDS)