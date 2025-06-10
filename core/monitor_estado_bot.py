import os
import json
import asyncio
from datetime import datetime
import sqlite3
from binance_api.cliente import crear_cliente
from ccxt.base.errors import AuthenticationError, NetworkError
from core.logger import configurar_logger
from core import ordenes_reales

ORDENES_DB_PATH = os.path.join("ordenes_reales", "ordenes.db")
ESTADOS_EMOCION = {
    "ganancia": "😄 Eufórico",
    "perdida": "😢 Frustrado",
    "expirada": "😐 Impaciente",
    "esperando": "🧘 En calma",
    "activo": "😎 Determinado",
}

log = configurar_logger("estado_bot")

def obtener_orden_abierta():
    if os.path.exists(ORDENES_DB_PATH):
        try:
            ordenes = ordenes_reales.cargar_ordenes()
            return ordenes if ordenes else None
        except (OSError, sqlite3.Error) as e:
            log.warning(f"⚠️ Error al leer órdenes desde la base de datos: {e}")
            return None
    return None

def estimar_estado_emocional(ultima_orden):
    if not ultima_orden:
        return ESTADOS_EMOCION["esperando"]
    if isinstance(ultima_orden, dict):
        motivo = ultima_orden.get("motivo_cierre")
    else:
        motivo = getattr(ultima_orden, "motivo_cierre", None)

    # ``motivo`` puede ser ``None`` si no se especificó un motivo de cierre.
    # Convertimos a cadena vacía para evitar ``AttributeError`` al llamar ``lower``.
    motivo_normalizado = (motivo or "").lower()
    for clave in ESTADOS_EMOCION:
        if clave in motivo_normalizado:
            return ESTADOS_EMOCION[clave]
    return ESTADOS_EMOCION["activo"]

def monitorear_estado_bot():
    try:
        cliente = crear_cliente()
        balance = cliente.fetch_balance()
        euros = balance["total"].get("EUR", 0)
        orden_abierta = obtener_orden_abierta()

        log.info("======= 🤖 ESTADO ACTUAL DEL BOT =======")
        log.info(
            f"🕒 Hora actual: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC"
        )
        log.info(f"💶 Saldo disponible (EUR): {euros:.2f}")

        if orden_abierta:
            for symbol, orden in orden_abierta.items():
                precio = (
                    orden.get("precio_entrada")
                    if isinstance(orden, dict)
                    else getattr(orden, "precio_entrada", None)
                )
                sl = (
                    orden.get("stop_loss")
                    if isinstance(orden, dict)
                    else getattr(orden, "stop_loss", None)
                )
                tp = (
                    orden.get("take_profit")
                    if isinstance(orden, dict)
                    else getattr(orden, "take_profit", None)
                )
                log.info(
                    f"📈 Orden abierta: {symbol} → Entrada: {precio} | SL: {sl} | TP: {tp}"
                )
        else:
            log.info("📭 No hay órdenes abiertas.")

        estado_emocional = estimar_estado_emocional(
            list(orden_abierta.values())[-1] if orden_abierta else None
        )
        log.info(f"🧠 Estado emocional del bot: {estado_emocional}")
        log.info("========================================")

    except AuthenticationError:
        log.error("🔒 Error de autenticación con Binance API. Verifica tus claves.")
    except NetworkError:
        log.error("📡 Error de red al contactar con Binance. Verifica tu conexión.")
    except Exception as e:
        log.error(f"❌ Error inesperado en monitoreo del bot: {e}")
        raise

async def monitorear_estado_periodicamente(self, intervalo=300):
    """Ejecuta ``monitorear_estado_bot`` de forma periódica sin bloquear el loop."""

    loop = asyncio.get_running_loop()
    while True:
        try:
            await loop.run_in_executor(None, monitorear_estado_bot)
            log.info("🧭 Monitoreo de estado completado.")
            log.debug(f"📌 Órdenes abiertas: {list(self.ordenes_abiertas.keys())}")
            await asyncio.sleep(intervalo)
        except asyncio.CancelledError:
            log.info("⏹️ Monitoreo cancelado. Cerrando tarea.")
            break
        except Exception as e:
            log.warning(f"⚠️ Error durante el monitoreo de estado: {e}")
            await asyncio.sleep(intervalo)
        

