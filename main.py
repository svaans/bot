import asyncio
import platform
import signal
import traceback
from pathlib import Path
from core.hot_reload import start_hot_reload, stop_hot_reload
from core.supervisor import start_supervision
from learning.reset_configuracion import resetear_configuracion_diaria_si_corresponde
from config.config_manager import ConfigManager
from core.notificador import crear_notificador_desde_env


def mostrar_banner():
    print('\n===============================')
    print('    🤖 BOT DE TRADING ACTIVO')
    print('===============================\n')


async def main():
    start_supervision()
    try:
        config = ConfigManager.load_from_env()
    except ValueError as e:
        print(f'❌ Error de configuración: {e}')
        return
    observer = start_hot_reload(path=Path.cwd(), modules=None)
    try:
        from learning.reset_pesos import resetear_pesos_diarios_si_corresponde
        from core.trader_modular import Trader
    except ValueError as e:
        print(f'❌ {e}')
        return
    if config.modo_real:
        print('🟢 Modo REAL activado')
    else:
        print('🟡 Modo SIMULADO activado')
    try:
        resetear_configuracion_diaria_si_corresponde()
        resetear_pesos_diarios_si_corresponde()
    except Exception:
        print('❌ Error al cargar los pesos desde backtest:')
        traceback.print_exc()
    mostrar_banner()
    print(f'🚀 Iniciando bot de trading... Modo real: {config.modo_real}')
    try:
        bot = Trader(config)
    except ValueError as e:
        print(f'❌ {e}')
        return
    except Exception as e:
        print(f'❌ Error al inicializar el Trader: {e}')
        traceback.print_exc()
        return
    notificador = crear_notificador_desde_env()
    tarea_bot = asyncio.create_task(bot.ejecutar())
    stop_event = asyncio.Event()
    tarea_stop = asyncio.create_task(stop_event.wait())

    def detener_bot():
        print('\n🛑 Señal de detención recibida.')
        stop_event.set()
    if platform.system() != 'Windows':
        loop = asyncio.get_running_loop()
        loop.add_signal_handler(signal.SIGINT, detener_bot)
        loop.add_signal_handler(signal.SIGTERM, detener_bot)

    pending = set()
    max_retries = 5
    retries = 0
    backoff_base = 5
    try:
        while True:
            done, pending = await asyncio.wait(
                [tarea_bot, tarea_stop],
                return_when=asyncio.FIRST_COMPLETED,
            )
            if tarea_bot in done and tarea_bot.done():
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
                    await asyncio.sleep(delay)
                    print('🔄 Reiniciando bot...')
                    tarea_bot = asyncio.create_task(bot.ejecutar())
                    continue
                else:
                    print('✅ Bot finalizado sin errores.')
                break
            if tarea_stop in done:
                break
    except asyncio.CancelledError:
        print('🛑 Cancelación detectada.')
    except KeyboardInterrupt:
        print('🛑 Interrupción por teclado detectada.')
    finally:
        stop_event.set()
        for t in pending:
            t.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        stop_hot_reload(observer)
        await bot.cerrar()
        print('👋 Bot finalizado correctamente.')


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print('\n🛑 Bot detenido manualmente.')
    except Exception:
        print('\n❌ Error inesperado:')
        traceback.print_exc()
