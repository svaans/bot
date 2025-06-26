import asyncio
import platform
import signal
import traceback
from pathlib import Path
import logging

from core.hot_reload import start_hot_reload, stop_hot_reload
from learning.reset_configuracion import resetear_configuracion_diaria_si_corresponde
from config.config_manager import ConfigManager

def mostrar_banner():
    print("\n===============================")
    print("    🤖 BOT DE TRADING ACTIVO")
    print("===============================\n")

async def main():
    logging.basicConfig(level=logging.DEBUG)
    config = ConfigManager.load_from_env()

    # ✅ Observador sin usar hilos externos
    observer = start_hot_reload(path=Path.cwd(), modules=None, exclude=("venv",))

    try:
        from learning.reset_pesos import resetear_pesos_diarios_si_corresponde
        from core.trader_modular import Trader
    except ValueError as e:
        print(f"❌ {e}")
        return

    if config.modo_real:
        print("🟢 Modo REAL activado")
    else:
        print("🟡 Modo SIMULADO activado")

    try:
        resetear_configuracion_diaria_si_corresponde()
        resetear_pesos_diarios_si_corresponde()
    except Exception:
        print("❌ Error al cargar los pesos desde backtest:")
        traceback.print_exc()

    mostrar_banner()
    print(f"🚀 Iniciando bot de trading... Modo real: {config.modo_real}")

    try:
        bot = Trader(config)
    except ValueError as e:
        print(f"❌ {e}")
        return

    tarea_bot = asyncio.create_task(bot.ejecutar())
    stop_event = asyncio.Event()
    tarea_stop = asyncio.create_task(stop_event.wait())

    def detener_bot():
        print("\n🛑 Señal de detención recibida.")
        stop_event.set()

    if platform.system() != "Windows":
        loop = asyncio.get_running_loop()
        loop.add_signal_handler(signal.SIGINT, detener_bot)
        loop.add_signal_handler(signal.SIGTERM, detener_bot)

    try:
        await asyncio.wait(
            [tarea_bot, tarea_stop],
            return_when=asyncio.FIRST_COMPLETED,
        )
    except asyncio.CancelledError:
        print("🛑 Cancelación detectada.")
    except KeyboardInterrupt:
        print("🛑 Interrupción por teclado detectada.")
    finally:
        stop_event.set()
        tarea_bot.cancel()
        await asyncio.gather(tarea_bot, return_exceptions=True)
        stop_hot_reload(observer)
        await bot.cerrar()
        print("👋 Bot finalizado correctamente.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 Bot detenido manualmente.")
    except Exception:
        print("\n❌ Error inesperado:")
        traceback.print_exc()


