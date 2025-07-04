from core.utils import configurar_logger
log = configurar_logger('filtro_entradas')


def evaluar_validez_estrategica(symbol, df, estrategias_activas: dict,
    pesos=None, minimo_peso_total=0.5, min_diversidad=2) ->bool:
    activas = [k for k, v in estrategias_activas.items() if v]
    if len(activas) < min_diversidad:
        log.info(
            f'🚫 Sin entrada en {symbol}: diversidad {len(activas)} < {min_diversidad}'
            )
        return False
    if pesos:
        peso_total = sum(pesos.get(k, 0) for k in activas)
        if peso_total < minimo_peso_total:
            log.info(
                f'🚫 Sin entrada en {symbol}: peso total {peso_total:.2f} < {minimo_peso_total:.2f}'
                )
            return False
    return True


def verificar_liquidez_orden(df, cantidad_orden: float, ventana: int=20,
    factor: float=0.2) ->bool:
    """Valida la proporción entre ``cantidad_orden`` y el volumen promedio.

    Retorna ``True`` cuando la orden no supera ``factor`` veces el volumen medio
    de las ``ventana`` últimas velas. Si no hay suficientes datos de volumen,
    la función asume que la orden es válida.
    """
    if 'volume' not in df or len(df) < ventana or cantidad_orden <= 0:
        return True
    volumen_promedio = df['volume'].iloc[-ventana:].mean()
    if volumen_promedio <= 0:
        return True
    return cantidad_orden <= volumen_promedio * factor
