import pandas as pd
from indicadores.macd import calcular_macd


def estrategia_macd(df: pd.DataFrame) ->dict:
    """
    Detecta cruce alcista del MACD como señal de entrada.

    [FIX C-05] Protección contra look-ahead bias en vela no cerrada:
    - Si 'is_closed' está en el DataFrame, calcular_macd ya filtra velas abiertas
      vía filtrar_cerradas(). El slicing df.iloc[:-1] garantiza que 'anterior'
      usa la penúltima vela cerrada y 'actual' usa la última cerrada.
    - Si 'is_closed' NO está presente (feed sin metadatos de cierre), se asume
      que la última vela puede estar en formación y se comparan df.iloc[:-2] vs
      df.iloc[:-1] para evaluar solo velas que ya cerraron con seguridad.
    """
    if len(df) < 36:
        return {'activo': False, 'mensaje': 'Datos insuficientes'}

    tiene_is_closed = 'is_closed' in df.columns

    if tiene_is_closed:
        # calcular_macd filtrará las no cerradas internamente
        df_anterior = df.iloc[:-1]
        df_actual = df
    else:
        # Sin metadato is_closed, la última vela puede estar incompleta.
        # Comparamos las dos últimas velas *definitivamente* cerradas.
        df_anterior = df.iloc[:-2]
        df_actual = df.iloc[:-1]

    macd_anterior, signal_anterior, _ = calcular_macd(df_anterior)
    macd_actual, signal_actual, _ = calcular_macd(df_actual)
    if macd_anterior is None or macd_actual is None:
        return {'activo': False, 'mensaje': 'MACD no disponible'}
    if macd_anterior < signal_anterior and macd_actual > signal_actual:
        return {'activo': True, 'mensaje': 'Cruce MACD alcista detectado'}
    return {'activo': False, 'mensaje': 'Sin cruce MACD'}
