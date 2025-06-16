import pandas as pd
from indicadores.rsi import calcular_rsi
from indicadores.slope import calcular_slope
from indicadores.momentum import calcular_momentum
from indicadores.divergencia_rsi import detectar_divergencia_alcista
from core.tendencia import detectar_tendencia
from core.logger import configurar_logger

log = configurar_logger("analisis_salidas")


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


def _score_tecnico_basico(df: pd.DataFrame, direccion: str) -> float:
    """Calcula un score técnico sencillo (0-4)."""
    rsi = calcular_rsi(df)
    momentum = calcular_momentum(df)
    slope = calcular_slope(df)
    tendencia, _ = detectar_tendencia("", df)

    puntos = 0
    if rsi is not None:
        puntos += 1 if (rsi > 50 if direccion == "long" else rsi < 50) else 0
    if momentum is not None and abs(momentum) > 0.001:
        puntos += 1
    if slope > 0.01:
        puntos += 1
    if direccion == "long":
        puntos += 1 if tendencia in {"alcista", "lateral"} else 0
    else:
        puntos += 1 if tendencia in {"bajista", "lateral"} else 0
    return float(puntos)


def permitir_cierre_tecnico(symbol: str, df: pd.DataFrame, sl: float, precio: float) -> bool:
    """Decide si se permite cerrar la operación ignorando posibles rebotes."""

    orden = None
    if isinstance(precio, dict):
        # Compatibilidad con llamadas antiguas
        orden = precio
        precio = sl
        sl = orden.get("stop_loss", precio)
    if df is None or len(df) < 40:
        log.warning(f"[{symbol}] Datos insuficientes para análisis técnico de salida.")
        return True  # Mejor prevenir si no hay datos suficientes

    df = df.tail(60).copy()
    ultimas = df.tail(3)
    vela_actual = ultimas.iloc[-1]
    cierre = float(vela_actual["close"])
    apertura = float(vela_actual["open"])
    bajo = float(vela_actual["low"])
    alto = float(vela_actual["high"])
    cuerpo = abs(cierre - apertura)

    rsi = calcular_rsi(df)
    pendiente_rsi = calcular_slope(
        pd.DataFrame({"rsi": df["close"].rolling(14).apply(calcular_rsi)})
    ) if len(df) >= 30 else None

    direccion = orden.get("direccion", "long") if orden else "long"
    score = _score_tecnico_basico(df, direccion)
    envolvente = es_vela_envolvente_alcista(df)
    soporte_cerca = precio_cerca_de_soporte(df, precio)
    divergencia = detectar_divergencia_alcista(df)

    if score < 2:
        log.info(f"❌ {symbol} Score técnico {score:.2f} < 2. Cierre obligatorio.")
        return True

    if orden:
        pesos = [v for v in orden.get("estrategias_activas", {}).values() if isinstance(v, (int, float))]
        if pesos and max(pesos) < 0.3:
            log.info(f"❌ {symbol} Estrategias de bajo peso. Cierre recomendado.")
            return True
    # 1️⃣ RSI muy bajo pero subiendo (posible rebote)
    if rsi is not None and rsi < 35 and pendiente_rsi is not None and pendiente_rsi > 0:
        log.info(f"⚠️ {symbol} RSI bajo pero subiendo ({rsi:.2f}) → posible rebote. Evitar cierre.")
        return False

    # 2️⃣ Proximidad a soporte reciente (rebote potencial)
    soporte_reciente = df["low"].rolling(20).min().iloc[-1]
    if precio - soporte_reciente < 0.005 * precio:
        log.info(f"⚠️ {symbol} muy cerca del soporte ({soporte_reciente:.2f}) → posible rebote.")
        return False

    # 3️⃣ Vela envolvente alcista (posible reversión)
    if ultimas.iloc[-2]["close"] < ultimas.iloc[-2]["open"] and cierre > apertura and cierre > ultimas.iloc[-2]["open"] and apertura < ultimas.iloc[-2]["close"]:
        log.info(f"⚠️ {symbol} vela envolvente alcista detectada → posible rebote. Evitar cierre.")
        return False

    # 4️⃣ Volumen alto con rechazo bajista
    volumen_actual = vela_actual["volume"]
    media_vol = df["volume"].rolling(20).mean().iloc[-1]
    mecha_inf = min(apertura, cierre) - bajo
    if volumen_actual > 1.2 * media_vol and mecha_inf > cuerpo:
        log.info(f"⚠️ {symbol} rechazo con volumen alto → posible soporte defendido.")
        return False

    # 5️⃣ Últimas 2 velas alcistas + SL muy cercano
    if ultimas.iloc[-1]["close"] > ultimas.iloc[-1]["open"] and ultimas.iloc[-2]["close"] > ultimas.iloc[-2]["open"]:
        distancia_sl = abs(precio - sl) / precio
        if distancia_sl < 0.005:
            log.info(f"⚠️ {symbol} velas verdes + SL muy cerca. Mejor esperar rebote.")
            return False

    # 6️⃣ Divergencia alcista RSI (bajo precio, RSI sube)
    if rsi and pendiente_rsi and pendiente_rsi > 0 and df["close"].iloc[-1] < df["close"].iloc[-2] < df["close"].iloc[-3]:
        log.info(f"⚠️ {symbol} divergencia alcista en RSI detectada. Posible rebote. Evitar cierre.")
        return False

    # 7️⃣ Rechazo bajista fuerte: RSI descendente + volumen bajo + vela roja grande
    if rsi and rsi < 50 and cierre < apertura and cuerpo > 0.6 * (alto - bajo) and volumen_actual < media_vol:
        log.info(f"✅ {symbol} debilidad confirmada → cierre justificado por rechazo bajista.")
        return True

    # 8️⃣ Cruce bajista de medias móviles (EMA12 < EMA26)
    ema12 = df["close"].ewm(span=12).mean().iloc[-1]
    ema26 = df["close"].ewm(span=26).mean().iloc[-1]
    if ema12 < ema26:
        log.info(f"✅ {symbol} cruce bajista de medias (EMA12 < EMA26). Cierre justificado.")
        return True

    # 9️⃣ Secuencia de máximos decrecientes
    highs = df["high"].tail(4).values
    if highs[3] < highs[2] < highs[1]:
        log.info(f"✅ {symbol} máximos decrecientes detectados. Cierre justificado por estructura bajista.")
        return True

    # 🔟 Mecha inferior larga actual (rechazo a seguir bajando)
    if mecha_inf > cuerpo * 2:
        log.info(f"⚠️ {symbol} mecha inferior larga → rechazo bajista. Evitar cierre.")
        return False

    # 🟢 En caso contrario, permitir cierre por defecto
    log.info(f"✅ {symbol} No se detecta defensa técnica significativa. Cierre técnico permitido.")
    return True
