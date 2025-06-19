from __future__ import annotations

from typing import Dict, Optional

from core.logger import configurar_logger

log = configurar_logger("adaptador_umbral")


def calcular_umbral_salida_adaptativo(symbol: str, config: Dict | None = None, contexto: Optional[Dict] = None) -> float:
    """Calcula un umbral dinámico para cierres basado en contexto de mercado."""
    if config is None:
        config = {}
    base = config.get("umbral_salida_base", 1.5)
    volatilidad = contexto.get("volatilidad", 1.0) if contexto else 1.0
    tendencia = contexto.get("tendencia", "lateral") if contexto else "lateral"
    factor_tend = 1.2 if tendencia in ["alcista", "bajista"] else 1.0
    umbral = base * volatilidad * factor_tend
    log.debug(
        f"[{symbol}] Umbral salida adaptativo: {umbral:.2f} | Base: {base} | Vol: {volatilidad:.3f} | Tend: {tendencia}"
    )
    return umbral