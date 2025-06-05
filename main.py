import asyncio
import platform
import signal
import traceback
from config.config import MODO_REAL
from core.pesos import cargar_pesos_estrategias
from aprendizaje.reset_pesos import resetear_pesos_diarios_si_corresponde
from aprendizaje.reset_configuracion import resetear_configuracion_diaria_si_corresponde
from core.config_manager import ConfigManager

# 📌 Selección dinámica de clase Trader
if MODO_REAL:
    from core.trader_modular import Trader
    print("🟢 Modo REAL activado: usando Trader modular")
else:
    from core.trader_simulado import TraderSimulado as Trader
    print("🟡 Modo SIMULADO activado: usando TraderSimulado")

def mostrar_banner():
    print("\n===============================")
    print("    🤖 BOT DE TRADING ACTIVO")
    print("===============================\n")

async def main():
    try:
        resetear_configuracion_diaria_si_corresponde()
        resetear_pesos_diarios_si_corresponde()
        cargar_pesos_estrategias()
    except Exception as e:
        print("❌ Error al cargar los pesos desde backtest:")
        traceback.print_exc()

    mostrar_banner()
    print(f"🚀 Iniciando bot de trading... Modo real: {MODO_REAL}")

    config = ConfigManager.load_from_env()
    bot = Trader(config)
    tarea_bot = asyncio.create_task(bot.ejecutar())
    stop_event = asyncio.Event()

    def detener_bot():
        print("\n🛑 Señal de detención recibida.")
        stop_event.set()

    if platform.system() != "Windows":
        loop = asyncio.get_running_loop()
        loop.add_signal_handler(signal.SIGINT, detener_bot)
        loop.add_signal_handler(signal.SIGTERM, detener_bot)

    try:
        await asyncio.gather(tarea_bot, stop_event.wait())
    except asyncio.CancelledError:
        print("🛑 Cancelación detectada.")
    except KeyboardInterrupt:
        print("🛑 Interrupción por teclado detectada.")
    finally:
        await bot.cerrar()
        print("👋 Bot finalizado correctamente.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 Bot detenido manualmente.")
    except Exception as e:
        print("\n❌ Error inesperado:")
        traceback.print_exc()
        # 📁 Guardar errores también en log.txt si lo deseas:
        # with open("logs/error.log", "a") as f:
        #     f.write(traceback.format_exc() + "\n")




