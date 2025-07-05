"""Módulo de validación y ejecución de entradas"""

from __future__ import annotations
import asyncio
from typing import Dict, List
import pandas as pd
from datetime import datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from core.trader.trader import Trader
from core.strategies.tendencia import detectar_tendencia
from core.utils.utils import configurar_logger
from core.strategies.entry.verificar_entradas import verificar_entrada
from core.strategies.entry.validador_entradas import evaluar_validez_estrategica
from learning.aprendizaje_en_linea import registrar_resultado_trade
from core.reporting import reporter_diario
from core.registro_metrico import registro_metrico
from core.strategies.evaluador_tecnico import actualizar_pesos_tecnicos
from learning.entrenador_estrategias import actualizar_pesos_estrategias_symbol

log = configurar_logger('trader')

async def evaluar_condiciones_entrada(trader: Trader, symbol: str, df: pd.DataFrame) -> None:
    """
    Evalúa y ejecuta una entrada si todas las condiciones se cumplen.
    """
    estado = trader.estado[symbol]
    config_actual = trader.config_por_simbolo.get(symbol, {})
    dinamica = trader._adaptar_config_dinamica(symbol, df)
    if dinamica:
        config_actual.update(dinamica)
    config_actual = trader._adaptar_config_base(symbol, df, config_actual)
    trader.config_por_simbolo[symbol] = config_actual

    tendencia_actual = trader.estado_tendencia.get(symbol)
    if not tendencia_actual:
        tendencia_actual, _ = detectar_tendencia(symbol, df)
        trader.estado_tendencia[symbol] = tendencia_actual

    loop = asyncio.get_running_loop()
    try:
        resultado = await asyncio.wait_for(
            loop.run_in_executor(
                None,
                lambda: trader.engine.evaluar_entrada(
                    symbol,
                    df,
                    tendencia=tendencia_actual,
                    config=config_actual,
                    pesos_symbol=trader.pesos_por_simbolo.get(symbol, {})
                )
            ),
            timeout=30
        )
    except asyncio.TimeoutError:
        log.warning(f"⚠️ Timeout en evaluar_entrada para {symbol}")
        trader._rechazo(symbol, "timeout_engine", estrategias=[])
        return

    estrategias = resultado.get('estrategias_activas', {})
    estado.buffer[-1]['estrategias_activas'] = estrategias
    trader.persistencia.actualizar(symbol, estrategias)

    precio_actual = float(df['close'].iloc[-1])
    if not resultado.get('permitido'):
        if trader.usar_score_tecnico:
            rsi = resultado.get('rsi')
            mom = resultado.get('momentum')
            score, puntos = trader._calcular_score_tecnico(
                df, rsi, mom,
                tendencia_actual,
                'short' if tendencia_actual == 'bajista' else 'long'
            )
            trader._registrar_rechazo_tecnico(
                symbol, score, puntos, tendencia_actual, precio_actual,
                resultado.get('motivo_rechazo', 'desconocido'),
                estrategias
            )
        trader._rechazo(
            symbol,
            resultado.get('motivo_rechazo', 'desconocido'),
            puntaje=resultado.get('score_total'),
            estrategias=list(estrategias.keys())
        )
        return

    info = await evaluar_condiciones_de_entrada(trader, symbol, df, estado)
    if not info:
        trader._rechazo(
            symbol,
            'filtros_post_engine',
            puntaje=resultado.get('score_total'),
            estrategias=list(estrategias.keys())
        )
        return

    await abrir_operacion_real(trader, **info)


async def evaluar_condiciones_de_entrada(trader: Trader, symbol: str, df: pd.DataFrame, estado) -> dict | None:
    if not trader._validar_config(symbol):
        return None
    return await verificar_entrada(trader, symbol, df, estado)


async def abrir_operacion_real(trader: Trader, symbol: str, precio: float, sl: float, tp: float,
                               estrategias: Dict | List, tendencia: str, direccion: str,
                               puntaje: float = 0.0, umbral: float = 0.0,
                               detalles_tecnicos: dict | None = None, **kwargs) -> None:
    cantidad_total = await trader.capital_manager.calcular_cantidad_async(symbol, precio)
    if cantidad_total <= 0:
        return
    fracciones = trader.piramide_fracciones
    cantidad = cantidad_total / fracciones
    if isinstance(estrategias, dict):
        estrategias_dict = estrategias
    else:
        pesos_symbol = trader.pesos_por_simbolo.get(symbol, {})
        estrategias_dict = {e: pesos_symbol.get(e, 0.0) for e in estrategias}
    await trader.orders.abrir_async(symbol, precio, sl, tp,
                                    estrategias_dict, tendencia, direccion,
                                    cantidad, puntaje, umbral,
                                    objetivo=cantidad_total,
                                    fracciones=fracciones,
                                    detalles_tecnicos=detalles_tecnicos or {})
    estrategias_list = list(estrategias_dict.keys())
    log.info(f'🟢 ENTRADA: {symbol} | Puntaje: {puntaje:.2f} / Umbral: {umbral:.2f} | Estrategias: {estrategias_list}')
    registro_metrico.registrar('entrada', {
        'symbol': symbol,
        'puntaje': puntaje,
        'umbral': umbral,
        'estrategias': ','.join(estrategias_list),
        'precio': precio
    })
    try:
        registrar_resultado_trade(symbol, {}, puntaje)
        reporter_diario.registrar_operacion({
            'symbol': symbol,
            'precio': precio,
            'estrategias': estrategias_list
        })
    except Exception as e:
        log.debug(f"No se pudo registrar auditoría de entrada: {e}")
