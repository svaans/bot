"""
Gestor de Capital — lógica de fracción Kelly, redistribución y piramidación.

Principios
----------
- **Separado de TraderLite**: este módulo no ejecuta órdenes ni escucha velas.
- **Funciones puras o casi puras**: devuelven números y dicts, no efectos secundarios.
- **Backtesting/real**: el mismo código aplica si pasas métricas de rendimiento.

API
---
- calcular_fraccion_kelly(win_rate, payoff_ratio, riesgo_max=0.02) -> float
- redistribuir_capital(capital_total, metricas: dict, correlaciones: dict, config: dict) -> dict[symbol,float]
- aplicar_piramidacion(orden, precio_actual, params: dict) -> float | None

Notas
-----
- `metricas` puede incluir sharpe, drawdown, volatilidad, etc.
- `correlaciones` es un dict { (sym1,sym2): rho } que puedes usar para diversificar.
- `config` permite parámetros como riesgo_base, riesgo_max, capital_min.
- `params` en piramidación: { 'paso':0.01, 'max_adds':3, 'riesgo_max':0.05 }
"""
from __future__ import annotations
from typing import Dict, Optional


def calcular_fraccion_kelly(win_rate: float, payoff_ratio: float, riesgo_max: float = 0.02) -> float:
    """Calcula fracción de capital a arriesgar según fórmula Kelly.

    Kelly = W - (1-W)/R ; W=prob ganar, R= payoff ratio.
    Limitada a [0, riesgo_max].
    """
    if payoff_ratio <= 0:
        return 0.0
    k = win_rate - (1 - win_rate) / payoff_ratio
    if k < 0:
        return 0.0
    return min(riesgo_max, max(0.0, k))


def redistribuir_capital(
    capital_total: float,
    metricas: Dict[str, dict],
    correlaciones: Dict[tuple, float],
    config: Dict[str, float],
) -> Dict[str, float]:
    """Asigna capital entre símbolos según métricas y correlaciones.

    Estrategia simple: peso ∝ 1/(vol*sqrt(corr_sum)) limitado por config.
    """
    if capital_total <= 0:
        return {}
    riesgo_base = config.get("riesgo_base", 0.01)
    riesgo_max = config.get("riesgo_max", 0.02)
    capital_min = config.get("capital_min", 10.0)

    pesos: Dict[str, float] = {}
    suma_pesos = 0.0
    for sym, m in metricas.items():
        vol = max(1e-6, float(m.get("volatilidad", 1.0)))
        corr_sum = 1.0
        for (a, b), rho in correlaciones.items():
            if sym in (a, b):
                corr_sum += abs(rho)
        peso = 1.0 / (vol * (corr_sum ** 0.5))
        pesos[sym] = peso
        suma_pesos += peso

    res: Dict[str, float] = {}
    for sym, peso in pesos.items():
        frac = peso / suma_pesos if suma_pesos else 0.0
        asignado = max(capital_min, capital_total * frac * riesgo_base)
        asignado = min(asignado, capital_total * riesgo_max)
        res[sym] = asignado
    return res


def aplicar_piramidacion(orden: object, precio_actual: float, params: Dict[str, float]) -> Optional[float]:
    """Devuelve cantidad adicional a abrir si aplica piramidación.

    Condición simple: cada vez que el precio avanza `paso`% a favor de la operación,
    añade posición hasta `max_adds` veces, sin superar riesgo_max.
    """
    if not orden or precio_actual <= 0:
        return None
    paso = params.get("paso", 0.01)
    max_adds = int(params.get("max_adds", 3))
    riesgo_max = params.get("riesgo_max", 0.05)

    if not hasattr(orden, "precio_entrada") or not hasattr(orden, "direccion"):
        return None
    if not hasattr(orden, "adds_realizadas"):
        orden.adds_realizadas = 0
    if orden.adds_realizadas >= max_adds:
        return None

    entrada = float(orden.precio_entrada)
    direccion = getattr(orden, "direccion", "long")
    avance = (precio_actual - entrada) / entrada if direccion == "long" else (entrada - precio_actual) / entrada

    if avance >= paso * (orden.adds_realizadas + 1):
        # calcular nueva cantidad: proporcional al riesgo remanente
        cantidad_base = getattr(orden, "cantidad", 0.0)
        nueva = cantidad_base * 0.5  # ej: la mitad de la posición base
        orden.adds_realizadas += 1
        if (orden.adds_realizadas * nueva) > (cantidad_base * riesgo_max / paso):
            return None
        return nueva
    return None
