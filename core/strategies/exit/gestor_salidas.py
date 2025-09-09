import pandas as pd
import asyncio
import inspect
from datetime import datetime, timezone
from core.utils.utils import validar_dataframe
from core.strategies.tendencia import detectar_tendencia
from core.strategies.entry.gestor_entradas import evaluar_estrategias
from core.adaptador_umbral import calcular_umbral_adaptativo, calcular_umbral_salida_adaptativo
from core.strategies.pesos import obtener_peso_salida
from core.utils import configurar_logger
log = configurar_logger('gestor_salidas')
UTC = timezone.utc
from .loader_salidas import cargar_estrategias_salida



# Mapa de prioridades para políticas de salida.
# Un número mayor implica mayor prioridad de ejecución.
PRIORIDADES = {
    "Kill Switch": 5,
    "Stop-Loss": 4,
    "Take-Profit": 3,
    "Trailing/Break-even": 2,
    "Cierre por tiempo": 1,
}


def _clasificar_politica(evento: str, resultado: dict) -> str | None:
    """Devuelve la política asociada a un evento de salida."""
    e = (evento or "").lower()
    if "kill" in e or resultado.get("kill_switch"):
        return "Kill Switch"
    if "trailing" in e:
        return "Trailing/Break-even"
    if "break" in e and "even" in e or resultado.get("break_even"):
        return "Trailing/Break-even"
    if "take" in e and "profit" in e or "tp" in e:
        return "Take-Profit"
    if "stop" in e and "loss" in e or "sl" in e:
        return "Stop-Loss"
    if "tiempo" in e or "t_max" in e or "perdida" in e and "tiempo" in e:
        return "Cierre por tiempo"
    return None


async def evaluar_salidas(orden: dict, df, config=None, contexto=None):
    symbol = orden.get('symbol', 'SYM')
    if not validar_dataframe(df, ['close', 'high', 'low', 'volume']):
        log.warning(f'[{symbol}] DataFrame inválido para gestor de salidas')
        return {'cerrar': False, 'razon': 'Datos insuficientes'}
    
    cfg = config or {}
    now = datetime.now(UTC)
    acciones_prioritarias = []

    # Kill switch global
    if cfg.get('kill_switch') or orden.get('kill_switch'):
        acciones_prioritarias.append(
            {
                'evento': 'Kill Switch',
                'resultado': {'cerrar': True, 'razon': 'Kill Switch'},
                'prioridad': PRIORIDADES['Kill Switch'],
            }
        )

    # Cierre por tiempo de vida de la orden
    t_max = cfg.get('t_max')
    timestamp = orden.get('timestamp')
    if t_max and timestamp:
        try:
            abierto = datetime.fromisoformat(str(timestamp))
            if (now - abierto).total_seconds() >= t_max:
                acciones_prioritarias.append(
                    {
                        'evento': 'Expiración t_max',
                        'resultado': {'cerrar': True, 'razon': 'Cierre por tiempo'},
                        'prioridad': PRIORIDADES['Cierre por tiempo'],
                    }
                )
        except Exception as e:
            log.warning(f'[{symbol}] Error evaluando t_max: {e}')
    t_max_loss = cfg.get('t_max_loss')
    if t_max_loss:
        precio_actual = float(df['close'].iloc[-1])
        precio_entrada = orden.get('precio_entrada', precio_actual)
        direccion = orden.get('direccion', 'long')
        en_perdida = (
            direccion in ('long', 'compra') and precio_actual < precio_entrada
        ) or (
            direccion in ('short', 'venta') and precio_actual > precio_entrada
        )
        if en_perdida:
            inicio = orden.get('t_inicio_perdida')
            if not inicio:
                orden['t_inicio_perdida'] = now.isoformat()
            else:
                try:
                    t_inicio = datetime.fromisoformat(str(inicio))
                    if (now - t_inicio).total_seconds() >= t_max_loss:
                        acciones_prioritarias.append(
                            {
                                'evento': 'Tiempo en pérdida excedido',
                                'resultado': {'cerrar': True, 'razon': 'Cierre por tiempo'},
                                'prioridad': PRIORIDADES['Cierre por tiempo'],
                            }
                        )
                except Exception:
                    orden['t_inicio_perdida'] = now.isoformat()
        else:
            orden.pop('t_inicio_perdida', None)
    funciones = cargar_estrategias_salida()
    señales = []

    for f in funciones:
        if not callable(f):
            continue
        try:
            params = list(inspect.signature(f).parameters.keys())
            if 'symbol' in params and 'orden' in params and 'config' in params:
                resultado = f(symbol, orden, df, config=config)
            elif 'symbol' in params and 'orden' in params:
                resultado = f(symbol, orden, df)
            elif 'symbol' in params and 'config' in params:
                resultado = f(symbol, df, config=config)
            elif 'symbol' in params:
                resultado = f(symbol, df)
            elif 'orden' in params and 'config' in params:
                resultado = f(orden, df, config=config)
            elif 'orden' in params:
                resultado = f(orden, df)
            elif 'config' in params:
                resultado = f(df, config=config)
            else:
                resultado = f(df)
            if asyncio.iscoroutine(resultado):
                resultado = await resultado
        except Exception as e:
            log.error(
                f'❌ Error ejecutando estrategia de salida {getattr(f, "__name__", f)} en {symbol}: {e}'
            )
            raise

        # Manejo de escalado: reducir posición cuando se llenan targets
        if resultado.get('targets_hit'):
            cantidad_abierta = orden.get('cantidad_abierta', orden.get('cantidad', 0.0))
            for t in resultado['targets_hit']:
                cantidad_abierta -= t.get('qty', 0.0)
            orden['cantidad_abierta'] = max(cantidad_abierta, 0.0)
            restantes = [t for t in resultado.get('targets', []) if t not in resultado['targets_hit']]
            if restantes:
                orden['targets'] = restantes
            else:
                log.info(f'[{symbol}] Todos los targets alcanzados. Posición cerrada')
                return {'cerrar': True, 'razon': 'Targets completados'}
            continue

        evento = resultado.get('evento') or resultado.get('razon') or resultado.get('motivo')
        politica = None
        if resultado.get('cerrar') or resultado.get('break_even') or resultado.get('kill_switch'):
            politica = _clasificar_politica(evento, resultado)

        if politica:
            acciones_prioritarias.append(
                {
                    'evento': evento or politica,
                    'resultado': resultado,
                    'prioridad': PRIORIDADES.get(politica, 0),
                }
            )
            continue

        if resultado.get('cerrar'):
            señales.append(evento or 'Sin motivo')

    if acciones_prioritarias:
        accion = max(acciones_prioritarias, key=lambda x: x['prioridad'])
        res = accion['resultado']
        res.setdefault('razon', accion['evento'])
        return res
    
    peso_total = sum(obtener_peso_salida(razon, symbol) for razon in señales)
    umbral = calcular_umbral_salida_adaptativo(symbol, config or {}, contexto)
    min_conf = (config or {}).get('min_confirmaciones_salida', 1)
    log.info(
        f'[SALIDA] {symbol} | Score: {peso_total:.2f} | Umbral: {umbral:.2f} | Señales: {señales}'
    )
    cerrar = peso_total >= umbral and len(señales) >= min_conf
    if cerrar:
        razon = f'Score {peso_total:.2f} ≥ {umbral:.2f} con {len(señales)} señales'
    else:
        razon = (
            f'Score insuficiente {peso_total:.2f} < {umbral:.2f}'
            if len(señales) >= min_conf
            else f'Señales insuficientes: {len(señales)}/{min_conf}'
        )
    return {
        'cerrar': cerrar,
        'razon': razon,
        'detalles': señales,
        'score': peso_total,
        'umbral': umbral,
    }


async def verificar_filtro_tecnico(symbol, df, estrategias_activas, pesos_symbol,
    config=None):
    if not validar_dataframe(df, ['high', 'low', 'close']):
        return False
    tendencia, _ = detectar_tendencia(symbol, df)
    evaluacion = await evaluar_estrategias(symbol, df, tendencia)
    if not evaluacion:
        return False
    activas = [k for k, v in evaluacion['estrategias_activas'].items() if v]
    puntaje = evaluacion['puntaje_total']
    umbral = calcular_umbral_adaptativo(symbol, df)
    return len(activas) >= 1 and puntaje >= 0.4 * umbral
