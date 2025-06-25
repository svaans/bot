import pandas as pd


def validar_dataframe(df: pd.DataFrame, columnas_requeridas) -> bool:
    """Verifica que ``df`` contenga las columnas necesarias y no esté vacío."""
    if df is None or not isinstance(df, pd.DataFrame):
        return False
    if df.empty:
        return False
    return all(col in df.columns for col in columnas_requeridas)