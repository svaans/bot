import os
import json
import asyncio
from datetime import datetime
from binance_api.cliente import crear_cliente
from ccxt.base.errors import AuthenticationError, NetworkError
from core.logger import configurar_logger

ORDENES_REALES_PATH = os.path.join("ordenes_reales", "ordenes_reales.json")
ESTADOS_EMOCION = {
    "ganancia": "😄 Eufórico",
    "perdida": "😢 Frustrado",
    "expirada": "😐 Impaciente",
    "esperando": "🧘 En calma",
    "activo": "😎 Determinado"
}

log = configurar_logger("estado_bot")

def obtener_orden_abierta():
    if os.path.exists(ORDENES_REALES_PATH):
        try:
            with open(ORDENES_REALES_PATH, "r") as f:
                ordenes = json.load(f)
            return ordenes if ordenes else None
        except Exception as e:
            log.warning(f"⚠️ Error al leer órdenes: {e}")
    return None

def estimar_estado_emocional(ultima_orden):
    if not ultima_orden:
        return ESTADOS_EMOCION["esperando"]
    motivo = ultima_orden.get("motivo_cierre", "")
    for clave in ESTADOS_EMOCION:
        if clave in motivo.lower():
            return ESTADOS_EMOCION[clave]
    return ESTADOS_EMOCION["activo"]

def monitorear_estado_bot():
    try:
        cliente = crear_cliente()
        balance = cliente.fetch_balance()
        euros = balance['total'].get('EUR', 0)
        orden_abierta = obtener_orden_abierta()

        log.info("======= 🤖 ESTADO ACTUAL DEL BOT =======")
        log.info(f"🕒 Hora actual: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC")
        log.info(f"💶 Saldo disponible (EUR): {euros:.2f}")

        if orden_abierta:
            for symbol, orden in orden_abierta.items():
                log.info(f"📈 Orden abierta: {symbol} → Entrada: {orden['precio_entrada']} | SL: {orden['stop_loss']} | TP: {orden['take_profit']}")
        else:
            log.info("📭 No hay órdenes abiertas.")

        estado_emocional = estimar_estado_emocional(list(orden_abierta.values())[-1] if orden_abierta else None)
        log.info(f"🧠 Estado emocional del bot: {estado_emocional}")
        log.info("========================================")

    except AuthenticationError:
        log.error("🔒 Error de autenticación con Binance API. Verifica tus claves.")
    except NetworkError:
        log.error("📡 Error de red al contactar con Binance. Verifica tu conexión.")
    except Exception as e:
        log.error(f"❌ Error inesperado en monitoreo del bot: {e}")

async def monitorear_estado_periodicamente(self, intervalo=300):
        while True:
            try:
                monitorear_estado_bot()  # puedes hacer que retorne resumen si quieres
                log.info("🧭 Monitoreo de estado completado.")
                log.debug(f"📌 Órdenes abiertas: {list(self.ordenes_abiertas.keys())}")
            except Exception as e:
                log.warning(f"⚠️ Error durante el monitoreo de estado: {e}")
            await asyncio.sleep(intervalo)

