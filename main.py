"""Script principal del bot de trading."""

# --- Bloqueo para evitar múltiples instancias ---
import os
import sys
import fcntl
import psutil


def ya_esta_activo() -> bool:
    """Comprueba si ya existe otra instancia activa de ``main.py``."""
    actual = os.getpid()
    for p in psutil.process_iter(["pid", "cmdline"]):
        try:
            cmd = " ".join(p.info.get("cmdline", []))
            if p.pid != actual and "main.py" in cmd:
                return True
        except Exception:
            continue
    return False


def acquire_lock(path: str = "/tmp/pegaso_bot.lock"):
    """Asegura que solo una instancia del bot esté activa."""

    while True:
        lock_fd = open(path, "a+")
        try:
            # Intentar obtener un bloqueo exclusivo sin esperar
            fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            # El archivo está bloqueado: ¿la instancia sigue viva?
            lock_fd.seek(0)
            pid_str = lock_fd.read().strip()
            lock_fd.close()
            if pid_str.isdigit() and not psutil.pid_exists(int(pid_str)):
                # El proceso registrado ya no existe -> lock huérfano
                try:
                    os.remove(path)
                except OSError:
                    pass
                continue
            mensaje = "🛑 Ya hay una instancia en ejecución"
            if pid_str.isdigit():
                mensaje += f" (PID {pid_str})"
            print(mensaje + ". Finalizando esta ejecución.")
            sys.exit(1)
        else:
            # Bloqueo adquirido correctamente -> escribir PID
            lock_fd.seek(0)
            lock_fd.truncate()
            lock_fd.write(str(os.getpid()))
            lock_fd.flush()
            return lock_fd
        
if ya_esta_activo():
    print("🚫 Ya hay una instancia corriendo.")
    sys.exit(1)


lock_fd = acquire_lock()

import asyncio
import platform
import signal
import traceback
from pathlib import Path
import logging
from logging.handlers import RotatingFileHandler

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
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 Bot detenido manualmente.")
    except Exception:
        logging.getLogger(__name__).exception("Error inesperado")
        print("\n❌ Error inesperado:")
        traceback.print_exc()
    finally:
        # Liberar el bloqueo antes de salir
        try:
            fcntl.flock(lock_fd, fcntl.LOCK_UN)
        finally:
            lock_fd.close()


