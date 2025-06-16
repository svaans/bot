import pandas as pd
from indicadores.rsi import calcular_rsi
from indicadores.divergencia_rsi import detectar_divergencia_alcista


def precio_cerca_de_soporte(df: pd.DataFrame, precio: float, ventana: int = 30, margen: float = 0.003) -> bool:
    """Comprueba si ``precio`` está cerca de un soporte reciente."""
    if "low" not in df or len(df) < 2:
        return False
    recientes = df["low"].tail(ventana)
    soporte = recientes.min()
    if soporte <= 0:
        return False
    return precio <= soporte * (1 + margen)


def es_vela_envolvente_alcista(df: pd.DataFrame) -> bool:
    """Detecta patrón de vela envolvente alcista en las dos últimas velas."""
    if len(df) < 2:
        return False
    prev = df.iloc[-2]
    curr = df.iloc[-1]
    cuerpo_prev = prev["close"] - prev["open"]
    cuerpo_curr = curr["close"] - curr["open"]
    return (
        cuerpo_prev < 0
        and cuerpo_curr > 0
        and curr["close"] > prev["open"]
        and curr["open"] < prev["close"]
    )


def permitir_cierre_tecnico(symbol: str, df: pd.DataFrame, precio: float, orden: dict) -> bool:
    """Evalúa si técnicamente debe permitirse el cierre de una posición de compra."""
    columnas = {"open", "high", "low", "close", "volume"}
    if not columnas.issubset(df.columns) or len(df) < 4:
        return True

    ultimas = df.tail(4)
    rsi_series = calcular_rsi(df, serie_completa=True)
    rsi = rsi_series.iloc[-1] if rsi_series is not None else None
    rsi_subiendo = False
    if rsi_series is not None and len(rsi_series) >= 3:
        rsi_subiendo = rsi_series.iloc[-1] > rsi_series.iloc[-2] > rsi_series.iloc[-3]

    cerca_soporte = precio_cerca_de_soporte(df, precio)
    envolvente = es_vela_envolvente_alcista(df)
    volumen_medio = df["volume"].rolling(window=20).mean().iloc[-1]
    volumen_alto = ultimas["volume"].iloc[-1] > volumen_medio if not pd.isna(volumen_medio) else False
    ultimas_alcistas = (ultimas["close"] > ultimas["open"]).iloc[-2:].all()
    sl = orden.get("stop_loss", 0)
    sl_cercano = sl > 0 and (precio - sl) / precio <= 0.003

    # Condiciones para rechazar el cierre
    if rsi is not None and rsi < 35 and rsi_subiendo:
        return False
    if cerca_soporte:
        return False
    if envolvente or volumen_alto:
        return False
    if ultimas_alcistas and sl_cercano:
        return False

    divergencia = detectar_divergencia_alcista(df)

    # Condiciones para permitir el cierre
    soporte_prev = df["low"].tail(5)[:-1].min()
    rompimiento_soporte = df["low"].iloc[-1] < soporte_prev * 0.995
    rsi_bajando = False
    if rsi_series is not None and len(rsi_series) >= 3:
        rsi_bajando = rsi_series.iloc[-1] < rsi_series.iloc[-2] < rsi_series.iloc[-3]
    volumen_decreciente = (
        ultimas["volume"].iloc[-1] < ultimas["volume"].iloc[-2] < ultimas["volume"].iloc[-3]
    )
    cuerpo = abs(ultimas["close"].iloc[-1] - ultimas["open"].iloc[-1])
    rango = ultimas["high"].iloc[-1] - ultimas["low"].iloc[-1]
    vela_bajista_fuerte = (
        ultimas["close"].iloc[-1] < ultimas["open"].iloc[-1]
        and rango > 0
        and cuerpo >= rango * 0.6
    )

    if rompimiento_soporte and not divergencia:
        return True
    if (
        rsi is not None
        and rsi < 40
        and rsi_bajando
        and volumen_decreciente
        and vela_bajista_fuerte
    ):
        return True

    return False