from __future__ import annotations
from dataclasses import dataclass
from collections import deque
from typing import Deque, Dict, Optional

# Numpy es opcional; si no está, usamos un cálculo simple de percentil.
try:
    import numpy as _np  # type: ignore
except Exception:  # pragma: no cover
    _np = None  # type: ignore


def _p90(values: Deque[float]) -> float:
    """Devuelve el percentil 90 de la ventana."""
    if not values:
        return 0.0
    if _np is not None:
        return float(_np.quantile(_np.asarray(list(values), dtype=float), 0.90))
    # Fallback sin numpy
    data = sorted(values)
    if len(data) == 1:
        return float(data[0])
    k = int(round(0.90 * (len(data) - 1)))
    return float(data[min(max(k, 0), len(data) - 1)])


@dataclass
class SpreadGuard:
    """
    Guardia dinámica para spreads con ventana deslizante e histéresis.

    Parámetros
    ----------
    base_limit : float
        Límite base del ratio de spread permitido (p.ej. 0.003 == 0.3%).
    max_limit : float
        Techo absoluto del límite dinámico.
    window : int
        Número de muestras en la ventana para estimar el spread típico.
    hysteresis : float
        Factor [0..1] para suavizar reducciones del límite (0 = sin suavizado).

    API pública
    -----------
    - observe(symbol, spread) -> float
        Registra una nueva observación y devuelve el límite vigente para el símbolo.
    - allows(symbol, spread) -> bool
        True si el spread actual está por debajo del límite dinámico.
    - current_limit(symbol) -> float
        Límite vigente (o base_limit si no hay historial).
    - update(symbol, spread) -> float
        Alias de observe(...).

    Compatibilidad con Trader
    -------------------------
    - permite_entrada(symbol, correlaciones, diversidad_minima) -> bool
        Firma usada por Trader._puede_evaluar_entradas(...). Aquí solo
        evaluamos el spread; los parámetros extra se ignoran de forma segura.
    """

    base_limit: float = 0.003
    max_limit: float = 0.015
    window: int = 50
    hysteresis: float = 0.15

    def __post_init__(self) -> None:
        self.window = max(5, int(self.window))
        self.hysteresis = min(max(float(self.hysteresis), 0.0), 1.0)
        # Historial y límites por símbolo
        self._history: Dict[str, Deque[float]] = {}
        self._limit: Dict[str, float] = {}
        self._max_limit = max(float(self.max_limit), float(self.base_limit))

    # ----------------- API pública -----------------

    def observe(self, symbol: str, spread_ratio: float) -> float:
        """Registra una observación de spread y actualiza el límite dinámico."""
        if not (isinstance(spread_ratio, (int, float)) and spread_ratio >= 0.0):
            # Entrada inválida: no cambia el límite
            return self.current_limit(symbol)

        sym = symbol.upper()
        hist = self._history.get(sym)
        if hist is None:
            hist = self._history[sym] = deque(maxlen=self.window)
        hist.append(float(spread_ratio))

        # Estimación robusta: P90 de la ventana (protege ante outliers)
        p90 = _p90(hist)
        target = max(self.base_limit, min(p90, self._max_limit))

        # Histéresis sólo al bajar el límite (para evitar serrucho)
        current = self._limit.get(sym, float(self.base_limit))
        if target < current:
            new_limit = current * self.hysteresis + target * (1.0 - self.hysteresis)
        else:
            new_limit = target

        # Normalizar contra límites globales
        new_limit = max(self.base_limit, min(new_limit, self._max_limit))
        self._limit[sym] = float(new_limit)
        return self._limit[sym]

    # Alias habitual
    def update(self, symbol: str, spread_ratio: float) -> float:
        return self.observe(symbol, spread_ratio)

    def allows(self, symbol: str, spread_ratio: float) -> bool:
        """Devuelve True si el spread actual está por debajo del límite."""
        limit = self.observe(symbol, spread_ratio)
        try:
            val = float(spread_ratio)
        except Exception:
            return True  # No bloqueamos por dato corrupto
        return val <= limit

    def current_limit(self, symbol: str) -> float:
        """Límite vigente para el símbolo (o base_limit si sin historial)."""
        return float(self._limit.get(symbol.upper(), self.base_limit))

    # ----------------- Compat con Trader -----------------

    def permite_entrada(self, symbol: str, correlaciones: dict | None = None, diversidad_minima: float | None = None) -> bool:
        """
        Compatibilidad con Trader._puede_evaluar_entradas(...).

        Aquí sólo evaluamos el spread. Usamos la última observación conocida
        como proxy del spread actual; si no hay historial, usamos base_limit
        como referencia y no bloqueamos.
        """
        sym = symbol.upper()
        hist = self._history.get(sym)
        if not hist:
            # Sin datos: no bloqueamos por spread
            return True
        spread_actual = float(hist[-1])
        return self.allows(sym, spread_actual)


__all__ = ["SpreadGuard"]
