import pandas as pd


def measured_move_up(df: pd.DataFrame) ->dict:
    if len(df) < 25:
        return {'activo': False, 'mensaje': 'Insuficientes datos'}
    tramo1 = df['close'].iloc[-25:-17].mean()
    correccion = df['close'].iloc[-17:-9].mean()
    tramo2 = df['close'].iloc[-9:].mean()
    if tramo1 < correccion and tramo2 > correccion and abs(tramo2 - tramo1
        ) / tramo1 < 0.05:
        return {'activo': True, 'mensaje':
            'Movimiento medido alcista detectado'}
    return {'activo': False, 'mensaje': 'Sin movimiento medido alcista'}
