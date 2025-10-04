import json
import os
import asyncio
from typing import Dict
import pandas as pd
from indicadores.helpers import get_rsi
from data_feed.candle_builder import backfill
from core.utils.utils import configurar_logger
log = configurar_logger('eval_tecnico')
RUTA_PESOS = 'config/pesos_tecnicos.json'
# PESOS_DEFECTO define la ponderación inicial de cada condición técnica.
# Cada peso representa cuántos puntos suma o resta la condición en
# ``evaluar_puntaje_tecnico``. A mayor valor, mayor influencia en el
# puntaje total.
PESOS_DEFECTO = {
    'rsi': 1.0,                # RSI dentro de un rango saludable.
    'volumen': 1.0,            # Volumen por encima de su media.
    'tp_sl': 0.7,              # Relación Take Profit / Stop Loss equilibrada.
    'no_doji': 0.5,            # Evitar velas tipo doji con cuerpo muy pequeño.
    'no_sobrecompra': 1.0,     # No estar en zona de sobrecompra extrema.
    'cuerpo_sano': 1.0,        # Cierre por encima de la apertura con buen cuerpo.
    'rsi_creciente': 0.6,      # RSI en ascenso respecto a la vela previa.
    'volumen_creciente': 0.5,  # Volumen superior al de la vela anterior.
    'sin_mecha_sup_larga': 0.4,# Mecha superior contenida.
    'distancia_extremos': 0.6  # Alejado de los máximos y mínimos del día.
}
_pesos_cache: dict | None = None
_pesos_lock = asyncio.Lock()
_backfill_tasks: Dict[str, asyncio.Task] = {}
_backfill_lock = asyncio.Lock()


def _cargar_pesos(symbol: str) ->dict:
    """Devuelve los pesos técnicos para ``symbol``.

    Debe ejecutarse con ``_pesos_lock`` adquirido por el llamador. Si ``symbol``
    no está presente en el archivo JSON se devuelven los valores por defecto.
    Además, se rellenan claves faltantes con ``PESOS_DEFECTO``.
    """
    global _pesos_cache
    if _pesos_cache is None:
        if os.path.exists(RUTA_PESOS):
            try:
                with open(RUTA_PESOS, 'r', encoding='utf-8') as fh:
                    _pesos_cache = json.load(fh)
            except Exception as e:
                log.warning(
                    f'Error leyendo {RUTA_PESOS}: {e}. Usando pesos por defecto'
                    )
                _pesos_cache = {}
        else:
            _pesos_cache = {}
    datos_simbolo = _pesos_cache.get(symbol) or _pesos_cache.get('default')
    if not isinstance(datos_simbolo, dict):
        datos_simbolo = {}
    pesos = PESOS_DEFECTO.copy()
    pesos.update(datos_simbolo)
    return pesos


async def cargar_pesos_tecnicos(symbol: str) -> dict:
    """Interfaz pública para obtener los pesos de un símbolo.

    Usa un ``asyncio.Lock`` para proteger el acceso concurrente al cache de
    pesos.
    """
    async with _pesos_lock:
        return _cargar_pesos(symbol)

async def evaluar_puntaje_tecnico(
    symbol: str,
    df: pd.DataFrame,
    precio: float,
    sl: float,
    tp: float,
    window_size: int = 60,
) -> dict:
    """Evalúa condiciones técnicas y retorna un puntaje acumulado."""
    pesos = await cargar_pesos_tecnicos(symbol)
    if df is None or len(df) < window_size:
        log.warning(f'[{symbol}] datos insuficientes para score tecnico, backfill en background')
        faltantes = window_size if df is None else window_size - len(df)
        if faltantes > 0:
            async with _backfill_lock:
                task = _backfill_tasks.get(symbol)
                if task is None or task.done():
                    nueva_tarea = asyncio.create_task(backfill(symbol, faltantes))
                    _backfill_tasks[symbol] = nueva_tarea

                    def _cleanup_backfill(t: asyncio.Task, *, sym: str = symbol) -> None:
                        try:
                            t.result()
                        except Exception as exc:  # pragma: no cover - logging de error
                            log.warning(f'[{sym}] backfill asincrono fallo: {exc}')
                        finally:
                            _backfill_tasks.pop(sym, None)

                    nueva_tarea.add_done_callback(_cleanup_backfill)
        return {'score_total': 0.0, 'score_normalizado': 0.0, 'detalles': {}}
    df = df.tail(window_size).copy()
    vela = df.iloc[-1]
    cierre = float(vela['close'])
    apertura = float(vela['open'])
    alto = float(vela['high'])
    bajo = float(vela['low'])
    cuerpo = abs(cierre - apertura)
    rango_total = alto - bajo
    rsi = get_rsi(df)
    rsi_ant = get_rsi(df.iloc[:-1]) if len(df) > 15 else rsi
    volumen_actual = float(vela['volume'])
    volumen_prev = float(df.iloc[-2]['volume'])
    media_vol = df['volume'].rolling(20).mean().iloc[-1]
    detalles: dict[str, float] = {}
    total = 0.0

    def _add(clave: str, condicion: (bool | None)) ->None:
        """Suma o resta puntaje según ``condicion`` y registra detalles."""
        nonlocal total
        peso = pesos.get(clave, 0.0)
        if condicion is None:
            puntos = 0.0
        elif condicion:
            puntos = peso
        else:
            puntos = -peso * 0.5
        detalles[clave] = float(puntos)
        total += puntos
        if puntos < 0:
            log.debug(f'[{symbol}] {clave} penaliza {puntos:.2f}')
    _add('rsi', rsi is not None and 40 <= rsi <= 70)
    _add('volumen', media_vol > 0 and volumen_actual > media_vol)
    ratio = (tp - precio) / (precio - sl) if precio != sl else None
    _add('tp_sl', ratio is not None and ratio >= 1.2)
    _add('no_doji', rango_total > 0 and cuerpo / rango_total >= 0.3)
    _add('no_sobrecompra', rsi is None or rsi < 75)
    _add('cuerpo_sano', cierre > apertura and cuerpo >= 0.6 * rango_total)
    _add('rsi_creciente', rsi is not None and rsi_ant is not None and rsi >
        rsi_ant)
    _add('volumen_creciente', volumen_actual > volumen_prev)
    mecha_sup = alto - max(cierre, apertura)
    _add('sin_mecha_sup_larga', mecha_sup <= 2 * cuerpo)
    max_dia = df['high'].max()
    min_dia = df['low'].min()
    distancia_max = (max_dia - precio) / max_dia if max_dia else None
    distancia_min = (precio - min_dia) / min_dia if min_dia else None
    _add('distancia_extremos', None not in (distancia_max, distancia_min) and
        distancia_max > 0.002 and distancia_min > 0.002)
    log.info(f'[ENTRY ANALYSIS] {symbol}')
    for k, v in detalles.items():
        signo = '+' if v >= 0 else ''
        log.info(f"- {k}: {'✅' if v > 0 else '❌'} ({signo}{v})")
    score_max = sum(pesos.values())
    score_normalizado = total / score_max if score_max else 0.0
    log.info(f'- Total score: {total:.2f} / {score_max:.2f} = {score_normalizado:.2f}')
    return {'score_total': round(total, 2), 'score_normalizado': round(score_normalizado, 2), 'detalles': detalles}


async def actualizar_pesos_tecnicos(symbol: str, detalles: dict, retorno: float,
    factor: float = 0.05) -> None:
    """Ajusta pesos del JSON según rendimiento de la operación.

    El acceso a ``_pesos_cache`` está protegido por ``_pesos_lock`` para evitar
    condiciones de carrera.
    """
    if not detalles:
        return
    async with _pesos_lock:
        pesos = _cargar_pesos(symbol)
        modificados = False
        for clave, puntaje in detalles.items():
            peso_actual = pesos.get(clave, PESOS_DEFECTO.get(clave, 0.0))
            if peso_actual <= 0:
                continue
            if retorno > 0 and puntaje > 0:
                nuevo = peso_actual * (1 + factor)
            elif retorno < 0 and puntaje > 0:
                nuevo = peso_actual * (1 - factor)
            else:
                continue
            pesos[clave] = max(0.1, round(nuevo, 3))
            modificados = True
        if modificados:
            _pesos_cache[symbol] = pesos
            def _escribir_pesos() -> None:
                with open(RUTA_PESOS, 'w', encoding='utf-8') as fh:
                    json.dump(_pesos_cache, fh, indent=2)
            try:
                await asyncio.to_thread(_escribir_pesos)
                log.info(f'[{symbol}] Pesos tecnicos actualizados')
            except Exception as e:
                log.warning(f'[{symbol}] Error guardando pesos: {e}')
