"""Filtro dinámico de *spread* con histéresis.

Mantiene un historial reciente de valores de spread por símbolo y ajusta
el límite máximo permitido. Cuando el mercado se vuelve volátil el límite
se eleva temporalmente y sólo vuelve a su valor base cuando la
volatilidad cae por debajo de una banda de histéresis.
"""
from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from typing import Deque, Dict

import numpy as np


@dataclass
class SpreadGuard:
    """Controla el spread permitido en función de la volatilidad reciente.

    Parameters
    ----------
    base_limit:
        Límite base de spread expresado como ratio (por ejemplo ``0.003``
        para 0.3 %).
    max_limit:
        Límite máximo absoluto que puede tomar el filtro.
    window:
        Número de observaciones recientes a considerar.
    hysteresis:
        Margen relativo para reducir oscilaciones del límite dinámico.
    """

    base_limit: float
    max_limit: float = 0.05
    window: int = 50
    hysteresis: float = 0.1
    _history: Dict[str, Deque[float]] = field(default_factory=dict)
    _limit: Dict[str, float] = field(default_factory=dict)
    _elevated: Dict[str, bool] = field(default_factory=dict)

    def observe(self, symbol: str, spread: float) -> float:
        """Registra ``spread`` y actualiza el límite dinámico.

        Devuelve el límite vigente tras incorporar la observación.
        """
        hist = self._history.setdefault(symbol, deque(maxlen=self.window))
        hist.append(spread)
        limit = self._limit.get(symbol, self.base_limit)
        elevated = self._elevated.get(symbol, False)
        if len(hist) >= max(3, self.window // 2):
            arr = np.fromiter(hist, dtype=float)
            p90 = float(np.quantile(arr, 0.9))
            if elevated:
                if p90 < self.base_limit * (1 - self.hysteresis):
                    limit = self.base_limit
                    elevated = False
            else:
                if p90 > self.base_limit * (1 + self.hysteresis):
                    limit = min(self.max_limit, p90)
                    elevated = True
        self._limit[symbol] = limit
        self._elevated[symbol] = elevated
        return limit

    def allows(self, symbol: str, spread: float) -> bool:
        """Indica si ``spread`` cumple el límite dinámico para ``symbol``."""
        limit = self.observe(symbol, spread)
        return spread <= limit

    def current_limit(self, symbol: str) -> float:
        """Devuelve el límite actual para ``symbol``."""
        return self._limit.get(symbol, self.base_limit)