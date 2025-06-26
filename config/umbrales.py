"""Constantes de umbral para las validaciones técnicas del bot."""

# Distancia relativa mínima al máximo o mínimo diario para considerar entrada
# (0.2% expresado en valor decimal)
DISTANCIA_EXTREMOS_MIN = 0.002

# Proporción mínima del cuerpo de la vela para no considerarla doji
MIN_CUERPO_NO_DOJI = 0.3

# Relación mínima TP/SL aceptable
TP_SL_MINIMO = 1.2

# Variación diaria máxima permitida antes de descartar entrada (-2%)
PERDIDA_DIA_MAX_PCT = -2.0

# Volumen actual mínimo respecto a su media para validar fortaleza
VOLUMEN_REL_MIN = 0.6

# Nivel RSI a partir del cual se considera sobrecompra
RSI_SOBRECOMPRA = 75

# Factor de multiplicación para detectar mechas superiores largas
FACTOR_MECHA_LARGA = 2.0

# Proporción mínima del cuerpo de una vela alcista fuerte
CUERPO_ALCISTA_MIN = 0.6

__all__ = [
    "DISTANCIA_EXTREMOS_MIN",
    "MIN_CUERPO_NO_DOJI",
    "TP_SL_MINIMO",
    "PERDIDA_DIA_MAX_PCT",
    "VOLUMEN_REL_MIN",
    "RSI_SOBRECOMPRA",
    "FACTOR_MECHA_LARGA",
    "CUERPO_ALCISTA_MIN",
]