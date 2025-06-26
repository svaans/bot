import asyncio
import platform
import signal
import traceback
from pathlib import Path
import logging
from logging.handlers import RotatingFileHandler
import fcntl
import os
import sys
import io


class SingleInstance:
    """Garantiza que solo una instancia del bot se ejecute simultáneamente."""

    def __init__(self, path: str = "/tmp/pegaso.lock") -> None:
        self.path = path
        self._file: io.TextIOWrapper | None = None

    def __enter__(self) -> "SingleInstance":
        self._file = open(self.path, "w")
        try:
            fcntl.flock(self._file, fcntl.LOCK_EX | fcntl.LOCK_NB)
            self._file.write(str(os.getpid()))
            self._file.flush()
        except OSError:
            print("\u274c Otra instancia del bot ya está en ejecución.")
            raise SystemExit(1)
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if self._file:
            try:
                fcntl.flock(self._file, fcntl.LOCK_UN)
            finally:
                self._file.close()

from core.hot_reload import start_hot_reload, stop_hot_reload
from learning.reset_configuracion import resetear_configuracion_diaria_si_corresponde
from config.config_manager import ConfigManager

def mostrar_banner():
    print("\n===============================")
    print("    🤖 BOT DE TRADING ACTIVO")
    print("===============================\n")

async def auto_restart(event: asyncio.Event, delay: int = 1800) -> None:
    """Reinicia el bot de forma controlada tras ``delay`` segundos."""
    await asyncio.sleep(delay)
    logging.getLogger(__name__).warning("♻️ Reinicio automático ejecutado")
    event.set()

async def heartbeat(event: asyncio.Event, interval: int = 30) -> None:
    """Escribe periódicamente una señal de vida en los logs."""
    log = logging.getLogger(__name__)
    while not event.is_set():
        log.info("bot vivo")
        try:
            await asyncio.wait_for(event.wait(), timeout=interval)
        except asyncio.TimeoutError:
            continue


async def main():
    logging.basicConfig(level=logging.INFO)

    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    )
    handler = RotatingFileHandler(
        "logs/bot.log", maxBytes=5_000_000, backupCount=3
    )
    handler.setFormatter(formatter)
    root_logger = logging.getLogger()
    root_logger.addHandler(handler)
    logging.getLogger("websockets").setLevel(logging.WARNING)

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
    tarea_reinicio = asyncio.create_task(auto_restart(stop_event))
    tarea_heartbeat = asyncio.create_task(heartbeat(stop_event))

    def detener_bot():
        print("\n🛑 Señal de detención recibida.")
        stop_event.set()

    if platform.system() != "Windows":
        loop = asyncio.get_running_loop()
        loop.add_signal_handler(signal.SIGINT, detener_bot)
        loop.add_signal_handler(signal.SIGTERM, detener_bot)

    try:
        await asyncio.wait(
            [tarea_bot, tarea_stop, tarea_reinicio, tarea_heartbeat],
            return_when=asyncio.FIRST_COMPLETED,
        )
    except asyncio.CancelledError:
        print("🛑 Cancelación detectada.")
    except KeyboardInterrupt:
        print("🛑 Interrupción por teclado detectada.")
    finally:
        stop_event.set()
        tarea_bot.cancel()
        tarea_reinicio.cancel()
        tarea_stop.cancel()
        tarea_heartbeat.cancel()
        await asyncio.gather(
            tarea_bot, tarea_reinicio, tarea_stop, tarea_heartbeat, return_exceptions=True
        )
        stop_hot_reload(observer)
        await bot.cerrar()
        print("👋 Bot finalizado correctamente.")

if __name__ == "__main__":
    with SingleInstance():
        try:
            asyncio.run(main())
        except KeyboardInterrupt:
            print("\n🛑 Bot detenido manualmente.")
        except Exception:
            logging.getLogger(__name__).exception("Error inesperado")
            print("\n❌ Error inesperado:")
            traceback.print_exc()


