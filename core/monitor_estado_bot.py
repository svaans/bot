import os
import asyncio
from datetime import datetime
import sqlite3
from binance_api.cliente import crear_cliente
from ccxt.base.errors import AuthenticationError, NetworkError
from core.logger import configurar_logger
from core import ordenes_reales
from core.reporting import reporter_diario
from core.riesgo import cargar_estado_riesgo

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ORDENES_DB_PATH = os.path.join(BASE_DIR, "ordenes_reales", "ordenes.db")



log = configurar_logger("estado_bot")

def obtener_orden_abierta():
    if os.path.exists(ORDENES_DB_PATH):
        try:
            ordenes = ordenes_reales.cargar_ordenes()
            if not ordenes:
                ordenes = ordenes_reales.sincronizar_ordenes_binance()
            return ordenes if ordenes else None
        except (OSError, sqlite3.Error) as e:
            log.warning(f"⚠️ Error al leer órdenes desde la base de datos: {e}")
            return None
    return None

def estimar_estado_emocional(_ultima_orden=None):
    """Determina el estado emocional actual del bot basado en el desempeño."""

    operaciones: list[dict] = []
    for ops in reporter_diario.ultimas_operaciones.values():
        operaciones.extend(ops)
    operaciones.sort(key=lambda o: o.get("fecha_cierre", ""))

    ganancias = 0
    perdidas = 0
    for op in reversed(operaciones):
        retorno = op.get("retorno_total", 0)
        if retorno > 0 and perdidas == 0:
            ganancias += 1
        elif retorno < 0 and ganancias == 0:
            perdidas += 1
        else:
            break

    riesgo = cargar_estado_riesgo().get("perdida_acumulada", 0.0)

    if ganancias >= 3:
        return "😎 Determinado"
    if perdidas >= 2 or riesgo > 1.5:
        return "😰 Cauteloso"
    if ganancias == 0 and riesgo < 0.5:
        return "😐 Observador"
    return "🤔 Neutro"

def resumen_emocional() -> str:
    """Genera una breve justificación del estado emocional."""
    operaciones = []
    for ops in reporter_diario.ultimas_operaciones.values():
        operaciones.extend(ops)
    operaciones.sort(key=lambda o: o.get("fecha_cierre", ""))
    ganancias = 0
    perdidas = 0
    for op in reversed(operaciones):
        retorno = op.get("retorno_total", 0)
        if retorno > 0 and perdidas == 0:
            ganancias += 1
        elif retorno < 0 and ganancias == 0:
            perdidas += 1
        else:
            break
    riesgo = cargar_estado_riesgo().get("perdida_acumulada", 0.0)
    return (
        f"{ganancias} ganancias consecutivas, {perdidas} pérdidas consecutivas, "
        f"riesgo acumulado {riesgo:.2f}%"
    )

def monitorear_estado_bot(ordenes_memoria: dict | None = None):
    """Muestra el estado del bot y las órdenes activas.

    Si no se encuentran órdenes en la base de datos y ``ordenes_memoria`` está
    provisto, se utilizarán esos datos en su lugar. Esto permite monitorizar las
    operaciones también en modo simulado.
    """
    try:
        cliente = crear_cliente()
        balance = cliente.fetch_balance()
        euros = balance["total"].get("EUR", 0)
        orden_abierta = obtener_orden_abierta()
        if not orden_abierta and ordenes_memoria:
            orden_abierta = ordenes_memoria

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
        log.info(
            f"🧠 Estado emocional del bot: {estado_emocional} — {resumen_emocional()}"
        )
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
            await loop.run_in_executor(None, monitorear_estado_bot, dict(self.ordenes_abiertas))
            log.info("🧭 Monitoreo de estado completado.")
            log.debug(f"📌 Órdenes abiertas: {list(self.ordenes_abiertas.keys())}")
            await asyncio.sleep(intervalo)
        except asyncio.CancelledError:
            log.info("⏹️ Monitoreo cancelado. Cerrando tarea.")
            break
        except Exception as e:
            log.warning(f"⚠️ Error durante el monitoreo de estado: {e}")
            await asyncio.sleep(intervalo)
        

