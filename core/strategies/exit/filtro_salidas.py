from indicadores.helpers import get_rsi
from indicadores.macd import calcular_macd
from config.exit_defaults import load_exit_config
from core.utils import configurar_logger

log = configurar_logger('filtro_salidas')


def validar_necesidad_de_salida(df, orden, estrategias_activas: dict,
    puntaje=None, umbral=None, config: dict=None) ->bool:
    try:
        rsi = get_rsi(df)
        cfg = load_exit_config(orden.get('symbol', 'SYM'))
        if config:
            cfg.update(config)
        umbral_rsi = cfg['umbral_rsi_salida']
        factor_umbral_puntaje = cfg['factor_umbral_validacion_salida']
        if (
            orden.get('direccion') == 'long'
            and rsi is not None
            and rsi > umbral_rsi
        ):
            if puntaje and umbral and puntaje > factor_umbral_puntaje * umbral:
                return False
        macd_line, signal = calcular_macd(df)
        if macd_line is not None and signal is not None:
            if macd_line[-2] < signal[-2] and macd_line[-1] > signal[-1]:
                return False
    except (KeyError, ValueError, TypeError) as e:
        log.warning(
            f"⚠️ Error evaluando filtros de salida para {orden.get('symbol', 'SYM')}: {e}"
        )
    return True
