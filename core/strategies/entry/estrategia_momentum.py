from __future__ import annotations

import pandas as pd
from indicadores.helpers import get_momentum, resolve_momentum_threshold


def estrategia_momentum(df: pd.DataFrame) -> dict:
    if len(df) < 15:
        return {'activo': False, 'mensaje': 'Insuficientes datos'}
    momentum = get_momentum(df)
    if momentum is None:
        return {'activo': False, 'mensaje': 'Momentum no disponible'}
    symbol_attr = df.attrs.get("symbol")
    threshold = resolve_momentum_threshold(symbol_attr if symbol_attr else None)
    try:
        momentum_value = float(momentum)
    except (TypeError, ValueError):
        return {'activo': False, 'mensaje': 'Momentum inválido'}
    if threshold > 0 and abs(momentum_value) < threshold:
        return {
            'activo': False,
            'mensaje': f'Momentum en zona neutra ({momentum_value:.2%})',
        }
    if momentum_value > 0:
        return {'activo': True,
                'mensaje': f'Momentum positivo: +{momentum_value:.2%}'}
    return {'activo': False,
            'mensaje': f'Momentum negativo: {momentum_value:.2%}'}
