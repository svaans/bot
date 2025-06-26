"""Valores de pesos para cálculos técnicos."""

PESOS_SCORE_TECNICO = {
    "RSI": 1.0,
    "Momentum": 0.5,
    "Slope": 1.0,
    "Tendencia": 1.0,
}

# Pesos alternativos según el régimen de mercado
PESOS_SCORE_REGIMEN = {
    "tendencial": {
        "RSI": 1.0,
        "Momentum": 1.0,
        "Slope": 1.2,
        "Tendencia": 1.0,
    },
    "lateral": {
        "RSI": 1.2,
        "Momentum": 0.5,
        "Slope": 0.8,
        "Tendencia": 0.5,
    },
}