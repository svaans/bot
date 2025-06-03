import os
import json
from datetime import datetime
from binance_api.cliente import obtener_cliente
from core.logger import configurar_logger

log = configurar_logger("ordenes")

RUTA_ORDENES = os.path.join("ordenes_reales", "ordenes_reales.json")
_CACHE_ORDENES = None

def cargar_ordenes():
    global _CACHE_ORDENES
    if _CACHE_ORDENES is not None:
        return _CACHE_ORDENES

    if os.path.exists(RUTA_ORDENES):
        try:
            with open(RUTA_ORDENES, "r") as f:
                _CACHE_ORDENES = json.load(f)
                return _CACHE_ORDENES
        except Exception as e:
            log.warning(f"⚠️ Error al leer archivo de órdenes: {e}. Se usará uno limpio.")
    _CACHE_ORDENES = {}
    return _CACHE_ORDENES

def guardar_ordenes(ordenes):
    global _CACHE_ORDENES
    try:
        os.makedirs(os.path.dirname(RUTA_ORDENES), exist_ok=True)
        with open(RUTA_ORDENES, "w") as f:
            json.dump(ordenes, f, indent=2)
        _CACHE_ORDENES = ordenes
        log.info("💾 Órdenes guardadas correctamente.")
    except Exception as e:
        log.error(f"❌ Error al guardar órdenes: {e}")

def obtener_orden(symbol):
    return cargar_ordenes().get(symbol)

def obtener_todas_las_ordenes():
    return cargar_ordenes()

def actualizar_orden(symbol, data):
    ordenes = cargar_ordenes()
    ordenes[symbol] = data
    guardar_ordenes(ordenes)
    log.info(f"📌 Orden actualizada para {symbol}.")

def eliminar_orden(symbol):
    ordenes = cargar_ordenes()
    if symbol in ordenes:
        del ordenes[symbol]
        guardar_ordenes(ordenes)
        log.info(f"🗑️ Orden eliminada para {symbol}.")
    else:
        log.warning(f"⚠️ Se intentó eliminar una orden inexistente: {symbol}.")

def registrar_orden(symbol, precio, cantidad, sl, tp, estrategias, tendencia):
    orden = {
        "symbol": symbol,
        "precio_entrada": precio,
        "cantidad": cantidad,
        "stop_loss": sl,
        "take_profit": tp,
        "timestamp": datetime.utcnow().isoformat(),
        "estrategias_activas": estrategias,
        "tendencia": tendencia,
        "max_price": precio
    }
    actualizar_orden(symbol, orden)

def ejecutar_orden_market(symbol, cantidad):
    try:
        cliente = obtener_cliente()
        response = cliente.create_market_buy_order(symbol.replace("/", ""), cantidad)
        log.info(f"🟢 Orden real ejecutada: {symbol}, cantidad: {cantidad}")
        return response
    except Exception as e:
        log.error(f"❌ Error ejecutando orden real para {symbol}: {e}")
        return None
