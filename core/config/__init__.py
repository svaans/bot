"""Parámetros globales de configuración."""

# Comisión por operación (0.1% por defecto)
COMISION: float = 0.001

# Deslizamiento estimado de precios en cada operación
SLIPPAGE: float = 0.0

__all__ = ["COMISION", "SLIPPAGE"]
