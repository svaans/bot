"""Portfolio Rebalancer: mantiene pesos objetivo por símbolo.

Detecta drift y sugiere vender lo sobrepondero / comprar lo subpondero.
No ejecuta órdenes — genera recomendaciones. Thread-safe. Sin dependencias externas.
"""
from __future__ import annotations

import json
import threading
from typing import Any

from core.utils.logger import configurar_logger
from core.utils.utils import ESTADO_DIR

log = configurar_logger("rebalancer", modo_silencioso=True)

_lock = threading.Lock()
_TARGET_PATH = ESTADO_DIR / "portfolio_target.json"

_DEFAULT_SYMBOLS = ["BTC/USDT", "ETH/USDT", "SOL/USDT", "XRP/USDT", "AVAX/USDT"]


def _pesos_default(symbols: list[str]) -> dict[str, float]:
    n = len(symbols)
    return {s: round(1.0 / n, 6) for s in symbols} if n > 0 else {}


def cargar_pesos_objetivo(symbols: list[str] | None = None) -> dict[str, float]:
    """Carga pesos objetivo desde archivo o retorna equal weight."""
    try:
        data = json.loads(_TARGET_PATH.read_text(encoding="utf-8"))
        if isinstance(data, dict) and data:
            total = sum(data.values())
            if abs(total - 1.0) < 0.01:
                return data
    except Exception:
        pass
    return _pesos_default(symbols or _DEFAULT_SYMBOLS)


def guardar_pesos_objetivo(pesos: dict[str, float]) -> None:
    """Persiste pesos objetivo normalizados."""
    total = sum(pesos.values())
    if total <= 0:
        log.warning("rebalancer: pesos inválidos (suma=%.4f), no se guardan", total)
        return
    normalizados = {k: round(v / total, 6) for k, v in pesos.items()}
    try:
        _TARGET_PATH.parent.mkdir(parents=True, exist_ok=True)
        _TARGET_PATH.write_text(json.dumps(normalizados, indent=2), encoding="utf-8")
        log.info("rebalancer: pesos objetivo guardados: %s", normalizados)
    except Exception as exc:
        log.warning("rebalancer: no se pudo guardar pesos objetivo: %s", exc)


def calcular_drift(
    valores_actuales: dict[str, float],
    pesos_objetivo: dict[str, float],
) -> dict[str, float]:
    """Calcula drift (peso_actual - peso_objetivo) por símbolo."""
    total = sum(valores_actuales.values())
    if total <= 0:
        return {s: 0.0 for s in pesos_objetivo}

    resultado: dict[str, float] = {}
    for sym, target in pesos_objetivo.items():
        actual = valores_actuales.get(sym, 0.0)
        peso_actual = actual / total
        resultado[sym] = round(peso_actual - target, 4)

    return resultado


def necesita_rebalanceo(
    valores_actuales: dict[str, float],
    pesos_objetivo: dict[str, float],
    umbral: float = 0.05,
) -> bool:
    """True si algún símbolo supera el umbral de drift."""
    drift = calcular_drift(valores_actuales, pesos_objetivo)
    return any(abs(d) > umbral for d in drift.values())


def generar_acciones_rebalanceo(
    valores_actuales: dict[str, float],
    pesos_objetivo: dict[str, float],
    capital_total: float | None = None,
    umbral: float = 0.05,
) -> list[dict[str, Any]]:
    """Genera lista de acciones de rebalanceo ordenadas por urgencia."""
    total = capital_total or sum(valores_actuales.values())
    if total <= 0:
        return []

    drift = calcular_drift(valores_actuales, pesos_objetivo)
    acciones = []

    for sym, d in drift.items():
        if abs(d) <= umbral:
            continue
        accion = "reducir" if d > 0 else "ampliar"
        usdt_delta = abs(d) * total
        acciones.append({
            "symbol": sym,
            "accion": accion,
            "drift_pct": round(d * 100, 2),
            "usdt_delta": round(usdt_delta, 2),
        })

    acciones.sort(key=lambda x: abs(x["drift_pct"]), reverse=True)
    return acciones


def resumen_cartera(
    valores_actuales: dict[str, float],
    pesos_objetivo: dict[str, float],
) -> str:
    """Genera resumen legible del estado de cartera vs objetivo."""
    total = sum(valores_actuales.values())
    if total <= 0:
        return "Cartera vacía"

    drift = calcular_drift(valores_actuales, pesos_objetivo)
    lines = [f"Portfolio total: {total:.2f} USDT"]
    for sym in sorted(pesos_objetivo):
        val = valores_actuales.get(sym, 0.0)
        peso_actual = val / total * 100
        peso_target = pesos_objetivo[sym] * 100
        d = drift.get(sym, 0.0) * 100
        marker = "OK" if abs(d) <= 5 else ("ALTA" if d > 0 else "BAJA")
        lines.append(
            f"  [{marker}] {sym:12} | actual={peso_actual:5.1f}% "
            f"target={peso_target:5.1f}% drift={d:+.1f}%"
        )
    return "\n".join(lines)
