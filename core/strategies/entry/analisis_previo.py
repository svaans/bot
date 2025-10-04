import pandas as pd
from core.utils.utils import configurar_logger
from indicadores.helpers import get_rsi
from core.utils.logger import _should_log
UMBRAL_PERDIDA_DIA = -0.02
UMBRAL_CERCANIA_EXTREMO = 0.003
UMBRAL_CUERPO_DOJI = 0.3
UMBRAL_RSI_BAJO = 35
UMBRAL_CUERPO_ALCISTA = 0.6
log = configurar_logger('analisis_previo')


def validar_condiciones_tecnicas_extra(symbol: str, datos: pd.DataFrame,
    precio: float, sl: float, tp: float) ->bool:
    if datos is None or len(datos) < 30:
        if _should_log(f"datos_insuf_validacion:{symbol}", every=2.0):
            log.warning(f'[{symbol}] Datos insuficientes para validación previa.')
        return False
    datos = datos.tail(60).copy()
    vela = datos.iloc[-1]
    cierre = float(vela['close'])
    apertura = float(vela['open'])
    alto = float(vela['high'])
    bajo = float(vela['low'])
    volumen_actual = float(vela['volume'])
    cuerpo = abs(cierre - apertura)
    rango_total = alto - bajo
    rsi = get_rsi(datos)
    cambio_diario_pct = (cierre - datos['close'].iloc[-2]) / datos['close'
        ].iloc[-2] * 100
    tp_sl_ratio = (tp - precio) / (precio - sl) if sl != precio else 999
    max_dia = datos['high'].max()
    min_dia = datos['low'].min()
    media_vol = datos['volume'].rolling(20).mean().iloc[-1]
    ema12 = datos['close'].ewm(span=12).mean().iloc[-1]
    ema26 = datos['close'].ewm(span=26).mean().iloc[-1]
    if (max_dia - precio) / max_dia < 0.002:
        log.info(
            f'❌ {symbol} muy cerca del máximo. Precio: {precio:.2f}, Máximo: {max_dia:.2f}'
            )
        return False
    if (precio - min_dia) / min_dia < 0.002 and cierre <= apertura:
        log.info(
            f'❌ {symbol} muy cerca del mínimo diario sin reversión. Precio: {precio:.2f}, Mínimo: {min_dia:.2f}'
            )
        return False
    if cuerpo / rango_total < 0.2:
        log.info(
            f'❌ {symbol} vela de indecisión detectada (Doji). Cuerpo/Rango: {cuerpo:.2f}/{rango_total:.2f}'
            )
        return False
    if tp_sl_ratio < 1.2:
        log.info(
            f'❌ {symbol} relación TP/SL desfavorable. TP: {tp}, SL: {sl}, Ratio: {tp_sl_ratio:.2f}'
            )
        return False
    if cambio_diario_pct < -2:
        log.info(
            f'❌ {symbol} caída diaria del {cambio_diario_pct:.2f}%. No es buen momento para entrar.'
            )
        return False
    if cierre > apertura and volumen_actual < datos['volume'].iloc[-2]:
        log.info(
            f'❌ {symbol} volumen decreciente con cierre alcista. Riesgo de falso rompimiento.'
            )
        return False
    if volumen_actual < 0.6 * media_vol:
        log.info(
            f'❌ {symbol} volumen muy bajo ({volumen_actual:.2f} vs media {media_vol:.2f})'
            )
        return False
    if rsi is not None and rsi > 75:
        log.info(
            f'❌ {symbol} RSI demasiado alto ({rsi:.2f}). Riesgo de sobrecompra.'
            )
        return False
    if ema12 < ema26:
        log.info(
            f'❌ {symbol} EMA12 ({ema12:.2f}) < EMA26 ({ema26:.2f}). No hay tendencia alcista clara.'
            )
        return False
    mecha_sup = alto - max(cierre, apertura)
    if mecha_sup > 2 * cuerpo:
        log.info(
            f'❌ {symbol} mecha superior larga. Rechazo de precios altos. Mecha: {mecha_sup:.2f}, Cuerpo: {cuerpo:.2f}'
            )
        return False
    if cierre > apertura and cuerpo > 0.6 * rango_total:
        if volumen_actual < media_vol:
            log.info(
                f'❌ {symbol} sin volumen suficiente en vela fuerte. Volumen: {volumen_actual:.2f} < Media: {media_vol:.2f}'
                )
            return False
    return True
