# --- Debug remoto opcional (endurecido) ---
import os
try:
    if os.environ.get("DEBUGPY") == "1":
        import debugpy  # puede no estar instalado en prod
        host = os.environ.get("DEBUGPY_HOST", "127.0.0.1")  # no expongas públicamente
        port = int(os.environ.get("DEBUGPY_PORT", "5678"))
        if host != "127.0.0.1":
            print(f"[debugpy] ADVERTENCIA: host no local: {host}")
        debugpy.listen((host, port))
        print(f"[debugpy] Esperando conexión en {host}:{port} ...")
        # Por defecto NO bloqueamos el arranque si no se conecta el depurador.
        if os.environ.get("DEBUGPY_WAIT_FOR_CLIENT", "0") == "1":
            debugpy.wait_for_client()
except Exception as e:
    print(f"[debugpy] Deshabilitado por error: {e}")
# --- fin debug remoto ---


import asyncio
import platform
import signal
import traceback
from pathlib import Path

from core.hot_reload import start_hot_reload, stop_hot_reload
from core.supervisor import start_supervision, stop_supervision
from core.notification_manager import crear_notification_manager_desde_env
from core.startup_manager import StartupManager
from core.metrics import iniciar_exporter


# --- Runner con aiomonitor opcional (cierre limpio del loop) ---
def run_with_optional_aiomonitor(coro):
    if os.environ.get("AIOMONITOR") == "1":
        from aiomonitor import Monitor
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            print("🔎 Ejecutando con aiomonitor (telnet 127.0.0.1:50101)…")
            with Monitor(loop):  # abre telnet en 127.0.0.1:50101
                loop.run_until_complete(coro)
        finally:
            loop.close()
    else:
        asyncio.run(coro)
# --- fin runner ---


def mostrar_banner():
    print('\n===============================')
    print('    🤖 BOT DE TRADING ACTIVO')
    print('===============================\n')


async def main():
    exporter_server = None
    observer = None
    bot = None
    tarea_bot = None
    config = None

    # 1) Arranque del bot
    try:
        startup = StartupManager()
        bot, tarea_bot, config = await startup.run()
    except Exception as e:
        msg = str(e)
        if 'Desincronización de reloj' in msg:
            print('❌ Desincronización de reloj detectada. '
                  'Sincroniza la hora del sistema (p.ej., usando NTP) y reinicia el bot.')
        elif 'Storage no disponible' in msg:
            print('❌ Almacenamiento no disponible. '
                  'Verifica los permisos de escritura en el directorio de datos.')
        else:
            print(f'❌ {msg}')
        traceback.print_exc()
        return

    # 2) Infraestructura auxiliar (exporter, supervisor, hot-reload)
    try:
        exporter_server = iniciar_exporter()
        start_supervision()
        observer = start_hot_reload(path=Path.cwd(), modules=None)
    except Exception:
        print('❌ Fallo durante la inicialización de infraestructura:')
        traceback.print_exc()
        # Limpieza defensiva si algo arrancó parcialmente
        try:
            if observer:
                stop_hot_reload(observer)
        except Exception:
            pass
        try:
            await stop_supervision()
        except Exception:
            pass
        try:
            if exporter_server:
                exporter_server.shutdown()
                exporter_server.server_close()
        except Exception:
            pass
        return

    # 3) Notificación de modo y banner
    if getattr(config, "modo_real", False):
        print('🟢 Modo REAL activado')
    else:
        print('🟡 Modo SIMULADO activado')

    mostrar_banner()
    print(f'🚀 Iniciando bot de trading... Modo real: {getattr(config, "modo_real", False)}')

    notificador = crear_notification_manager_desde_env()

    # 4) Señales/parada
    stop_event = asyncio.Event()
    tarea_stop = asyncio.create_task(stop_event.wait())

    def detener_bot():
        print('\n🛑 Señal de detención recibida (solicitando parada)…')
        stop_event.set()
        try:
            if bot:
                bot.solicitar_parada()
        except Exception:
            pass

    if platform.system() != 'Windows':
        loop = asyncio.get_running_loop()
        loop.add_signal_handler(
            signal.SIGINT, lambda: (print("\n🛑 SIGINT recibida"), detener_bot())
        )
        loop.add_signal_handler(
            signal.SIGTERM, lambda: (print("\n🛑 SIGTERM recibida"), detener_bot())
        )

    # 5) Bucle de vida y reintentos
    last_pending = set()
    max_retries = 5
    retries = 0
    backoff_base = 5

    try:
        while True:
            done, last_pending = await asyncio.wait(
                [tarea_bot, tarea_stop],
                return_when=asyncio.FIRST_COMPLETED,
            )

            # Ruta: terminó la tarea principal del bot
            if tarea_bot in done:
                exc = tarea_bot.exception()
                if exc:
                    print(f'❌ Error en la tarea del bot: {exc}')
                    traceback.print_exception(type(exc), exc, exc.__traceback__)
                    retries += 1
                    if retries > max_retries:
                        print('🚨 Número máximo de reintentos alcanzado. Deteniendo bot.')
                        try:
                            notificador.enviar(
                                'Bot detenido tras errores consecutivos', 'CRITICAL'
                            )
                        except Exception:
                            pass
                        break

                    delay = min(backoff_base * 2 ** (retries - 1), 300)
                    print(f'⏳ Reinicio del bot en {delay}s (intento {retries}/{max_retries})')

                    # Intentar cerrar bot previo con timeout
                    try:
                        if bot:
                            await asyncio.wait_for(bot.cerrar(), timeout=15)
                    except Exception as e:
                        print(f"⚠️ Error cerrando bot previo: {e}")

                    await asyncio.sleep(delay)
                    print('🔄 Reiniciando bot…')
                    try:
                        startup = StartupManager()
                        bot, tarea_bot, config = await startup.run()
                    except Exception as e:
                        print(f"❌ Error reiniciando bot: {e}")
                        traceback.print_exc()
                        continue  # reintento contará en la siguiente vuelta
                    retries = 0
                    continue
                else:
                    print('✅ Bot finalizado sin errores.')
                    break

            # Ruta: se solicitó parada (tarea_stop) 
            if tarea_stop in done:
                try:
                    if bot:
                        bot.solicitar_parada()
                except Exception:
                    pass
                break

    except asyncio.CancelledError:
        print('🛑 Cancelación detectada.')
    except KeyboardInterrupt:
        print('🛑 Interrupción por teclado detectada.')
    finally:
        # Secuencia de apagado ordenada
        stop_event.set()

        # 1) Pedir parada explícita (si existe)
        try:
            if bot:
                bot.solicitar_parada()
        except Exception:
            pass

        # 2) Dar tiempo a que la tarea principal salga sola
        try:
            if tarea_bot and not tarea_bot.done():
                await asyncio.wait_for(tarea_bot, timeout=10)
        except Exception:
            pass  # puede haberse cancelado o terminado ya

        # 3) Cancelar lo que quede pendiente
        if last_pending:
            for t in list(last_pending):
                if not t.done():
                    t.cancel()
            try:
                await asyncio.gather(*last_pending, return_exceptions=True)
            except Exception:
                pass

        # 4) Hot-reload
        try:
            if observer:
                stop_hot_reload(observer)
        except Exception as e:
            print(f"⚠️ Error deteniendo hot-reload: {e}")

        # 5) Cierre del bot con timeout
        try:
            if bot:
                await asyncio.wait_for(bot.cerrar(), timeout=15)
        except asyncio.TimeoutError:
            print('⏰ Timeout al cerrar el bot.')
        except Exception as e:
            print(f'⚠️ Error en bot.cerrar(): {e}')

        # 6) Supervisor y exporter con guardas
        try:
            await stop_supervision()
        except Exception as e:
            print(f"⚠️ Error parando supervisor: {e}")

        try:
            if exporter_server:
                exporter_server.shutdown()
                exporter_server.server_close()
        except Exception as e:
            print(f"⚠️ Error cerrando exporter: {e}")

        print('👋 Bot finalizado correctamente.')


if __name__ == '__main__':
    try:
        run_with_optional_aiomonitor(main())
    except KeyboardInterrupt:
        print('\n🛑 Bot detenido manualmente.')
    except Exception:
        print('\n❌ Error inesperado:')
        traceback.print_exc()

