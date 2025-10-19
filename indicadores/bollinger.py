"""Implementaciones relacionadas con las Bandas de Bollinger.

Este módulo expone un cálculo sencillo de bandas de Bollinger pensado para
mantener interoperabilidad con bibliotecas externas como *pandas-ta* o
*ta-lib*. En particular, documentamos explícitamente que la desviación
estándar utilizada es **muestral** (``ddof=1``), tal y como hace pandas de
forma predeterminada. Al fijar el ``ddof`` evitamos discrepancias con
implementaciones que utilicen la desviación poblacional.
"""

import pandas as pd

from indicadores.helpers import filtrar_cerradas


def calcular_bollinger(
    df: pd.DataFrame,
    periodo: int = 20,
    desviacion: float = 2.0,
):
    """Calcula las bandas de Bollinger a partir de cierres filtrados.

    Parameters
    ----------
    df
        Serie histórica con, al menos, la columna ``close`` y un índice
        cronológico.
    periodo
        Número de velas utilizado para el promedio móvil simple (SMA) y la
        desviación estándar muestral. Por defecto, ``20``.
    desviacion
        Factor multiplicador aplicado a la desviación estándar muestral.

    Returns
    -------
    tuple[float | None, float | None, float | None]
        Una tupla con ``(banda_inferior, banda_superior, precio_cierre)``. Si
        no hay datos suficientes para el período especificado se devuelve un
        triplete ``(None, None, None)``.

    Notas
    -----
    * La desviación estándar se calcula con ``ddof=1`` (desviación muestral),
      lo que alinea el resultado con la convención empleada por pandas y
      mantiene la compatibilidad con indicadores externos.
    * ``filtrar_cerradas`` se aplica antes del cálculo para descartar velas en
      formación y garantizar que únicamente se utilicen cierres consolidados.
    """

    df = filtrar_cerradas(df)
    if 'close' not in df or len(df) < periodo:
        return None, None, None
    ma = df['close'].rolling(window=periodo).mean()
    std = df['close'].rolling(window=periodo).std(ddof=1)
    banda_superior = ma + desviacion * std
    banda_inferior = ma - desviacion * std
    return banda_inferior.iloc[-1], banda_superior.iloc[-1], df['close'].iloc[
        -1]
