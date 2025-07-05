"""Módulo de gestión de cierres de operaciones"""

from __future__ import annotations
import pandas as pd
import os
from datetime import datetime
from typing import Dict
from core.utils.utils import configurar_logger
from core.trader.trader import Trader  # Asegúrate de que la ruta de importación sea correcta
from core.reporting import reporter_diario
from core.registro_metrico import registro_metrico
from learning.entrenador_estrategias import actualizar_pesos_estrategias_symbol
from learning.aprendizaje_en_linea import registrar_resultado_trade
from core.strategies.evaluador_tecnico import actualizar_pesos_tecnicos
from core.auditoria import registrar_auditoria

log = configurar_logger("trader")

async def cerrar_operacion(trader: Trader, symbol: str, precio: float, motivo: str) -> None:
    """
    Cierra una orden y actualiza los pesos si corresponde.
    """
    if not await trader.orders.cerrar_async(symbol, precio, motivo):
        log.debug(f'🔁 Intento duplicado de cierre ignorado para {symbol}')
        return
    actualizar_pesos_estrategias_symbol(symbol)
    try:
        trader.pesos_por_simbolo = trader.engine.cargar_pesos_estrategias()
    except ValueError as e:
        log.error(f'❌ {e}')
        return
    log.info(f"✅ Orden cerrada: {symbol} a {precio:.2f}€ por '{motivo}'")


async def cerrar_y_reportar(trader: Trader, orden, precio: float, motivo: str,
                            tendencia: str | None = None, df: pd.DataFrame | None = None) -> None:
    """
    Cierra orden y registra la operación para el reporte diario.
    """
    retorno_total = (precio - orden.precio_entrada) / orden.precio_entrada if orden.precio_entrada else 0.0
    info = orden.to_dict()
    info.update({
        'precio_cierre': precio,
        'fecha_cierre': datetime.utcnow().isoformat(),
        'motivo_cierre': motivo,
        'retorno_total': retorno_total,
        'capital_inicial': trader.capital_por_simbolo.get(orden.symbol, 0.0)
    })

    if not await trader.orders.cerrar_async(orden.symbol, precio, motivo):
        log.warning(f"❌ No se pudo confirmar el cierre de {orden.symbol}. Se omitirá el registro.")
        return

    capital_inicial = trader.capital_por_simbolo.get(orden.symbol, 0.0)
    ganancia = capital_inicial * retorno_total
    capital_final = capital_inicial + ganancia
    trader.capital_por_simbolo[orden.symbol] = capital_final
    info['capital_final'] = capital_final

    if getattr(orden, 'sl_evitar_info', None):
        log_impacto_sl(trader, orden, precio)

    reporter_diario.registrar_operacion(info)
    registrar_resultado_trade(orden.symbol, info, retorno_total)

    try:
        if orden.detalles_tecnicos:
            actualizar_pesos_tecnicos(orden.symbol, orden.detalles_tecnicos, retorno_total)
    except Exception as e:
        log.debug(f"No se pudo actualizar pesos tecnicos: {e}")

    actualizar_pesos_estrategias_symbol(orden.symbol)

    try:
        trader.pesos_por_simbolo = trader.engine.cargar_pesos_estrategias()
    except ValueError as e:
        log.error(f'❌ {e}')
        return

    duracion = 0.0
    try:
        apertura = datetime.fromisoformat(orden.timestamp)
        duracion = (datetime.utcnow() - apertura).total_seconds() / 60
    except Exception:
        pass

    trader.historial_cierres[orden.symbol] = {
        'timestamp': datetime.utcnow().isoformat(),
        'motivo': motivo.lower().strip(),
        'velas': 0,
        'precio': precio,
        'tendencia': tendencia,
        'duracion': duracion,
        'retorno_total': retorno_total
    }

    log.info(f'✅ CIERRE {motivo.upper()}: {orden.symbol} | Beneficio: {ganancia:.2f} €')
    registro_metrico.registrar('cierre', {
        'symbol': orden.symbol,
        'motivo': motivo,
        'retorno': retorno_total,
        'beneficio': ganancia
    })

    registrar_auditoria(
        symbol=orden.symbol,
        evento=motivo,
        resultado='ganancia' if retorno_total > 0 else 'pérdida',
        estrategias_activas=orden.estrategias_activas,
        score=None,
        rsi=None,
        tendencia=tendencia,
        capital_actual=capital_final,
        config_usada=trader.config_por_simbolo.get(orden.symbol, {})
    )


def log_impacto_sl(trader: Trader, orden, precio: float) -> None:
    """
    Registra el impacto de evitar el stop loss.
    """
    os.makedirs('logs', exist_ok=True)
    for ev in orden.sl_evitar_info:
        sl_val = ev.get('sl', 0.0)
        peor = precio < sl_val if orden.direccion in ('long', 'compra') else precio > sl_val
        mensaje = (
            f'❗ Evitar SL en {orden.symbol} resultó en pérdida mayor ({precio:.2f} vs {sl_val:.2f})'
            if peor else
            f'👍 Evitar SL en {orden.symbol} fue beneficioso ({precio:.2f} vs {sl_val:.2f})'
        )
        with open('logs/impacto_sl.log', 'a') as f:
            f.write(mensaje + '\n')
        log.info(mensaje)
    orden.sl_evitar_info = []


async def cerrar_parcial_y_reportar(trader: Trader, orden, cantidad: float, precio: float, motivo: str,
                                    df: pd.DataFrame | None = None) -> bool:
    """
    Cierra parcial y actualiza reporte.
    """
    if not await trader.orders.cerrar_parcial_async(orden.symbol, cantidad, precio, motivo):
        log.warning(f"❌ No se pudo confirmar el cierre parcial de {orden.symbol}. Se omite registro.")
        return False

    retorno_unitario = (precio - orden.precio_entrada) / orden.precio_entrada if orden.precio_entrada else 0.0
    fraccion = cantidad / orden.cantidad if orden.cantidad else 0.0
    retorno_total = retorno_unitario * fraccion

    info = orden.to_dict()
    info.update({
        'precio_cierre': precio,
        'fecha_cierre': datetime.utcnow().isoformat(),
        'motivo_cierre': motivo,
        'retorno_total': retorno_total,
        'cantidad_cerrada': cantidad,
        'capital_inicial': trader.capital_por_simbolo.get(orden.symbol, 0.0)
    })

    reporter_diario.registrar_operacion(info)
    registrar_resultado_trade(orden.symbol, info, retorno_total)

    capital_inicial = trader.capital_por_simbolo.get(orden.symbol, 0.0)
    ganancia = capital_inicial * retorno_total
    capital_final = capital_inicial + ganancia
    trader.capital_por_simbolo[orden.symbol] = capital_final
    info['capital_final'] = capital_final

    log.info(f'✅ CIERRE PARCIAL: {orden.symbol} | Beneficio: {ganancia:.2f} €')
    registro_metrico.registrar('cierre_parcial', {
        'symbol': orden.symbol,
        'retorno': retorno_total,
        'beneficio': ganancia
    })

    return True


async def verificar_salidas(trader: Trader, symbol: str, df: pd.DataFrame) -> None:
    from core.strategies.exit.verificar_salidas import verificar_salidas
    await verificar_salidas(trader, symbol, df)
