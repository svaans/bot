#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Ejecutable principal del bot de trading.

Endurecimientos aplicados:
- Debug remoto opcional con fallback seguro.
- Runner con aiomonitor opcional (import protegido).
- Comprobaciones defensivas de interfaces (bot/exporter/supervisor/hot-reload).
- Reintentos con backoff exponencial y cierre ordenado con timeouts.
- Manejo robusto de señales (UNIX) y KeyboardInterrupt cross-platform.
- ``try``/``finally`` global: cualquier salida de ``main()`` ejecuta apagado idempotente.

Reintentos (``max_retries`` / ``retries``):
- ``retries`` cuenta **caídas distintas** de la tarea principal del bot (se usa
  ``id(tarea_bot)`` para no volver a incrementar al reobservar la misma Task
  muerta mientras fallan reinicios de ``StartupManager``).
- Tras un reinicio **exitoso**, ``retries`` se pone a 0 (nueva ola).
- ``max_retries`` es el tope de esas caídas antes de salir del bucle con aviso.
"""

# --- Debug remoto opcional (endurecido) ---
import os
try:
    if os.environ.get("DEBUGPY") == "1":
        try:
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
            try:
                from core.utils.log_utils import format_exception_for_log as _fmt_exc
                _dbg = _fmt_exc(e, 400)
            except Exception:
                _dbg = str(e)[:400] + ("..." if len(str(e)) > 400 else "")
            print(f"[debugpy] Deshabilitado por error: {_dbg}")
except Exception as e:
    try:
        from core.utils.log_utils import format_exception_for_log as _fmt_exc
        _dbg = _fmt_exc(e, 400)
    except Exception:
        _dbg = str(e)[:400] + ("..." if len(str(e)) > 400 else "")
    print(f"[debugpy] Deshabilitado por error: {_dbg}")
# --- fin debug remoto ---


import asyncio
import inspect
import platform
import signal
import traceback
from pathlib import Path
from typing import Any, Optional, Tuple

from core.utils.logger import configurar_logger

# Dependencias internas (protegidas en tiempo de uso)
from core.hot_reload import ModularReloadRule, start_hot_reload, stop_hot_reload
from core.supervisor import start_supervision, stop_supervision
from core.notification_manager import crear_notification_manager_desde_env
from core.operational_mode import OperationalModeService
from core.diag.phase_logger import phase
from core.startup_manager import StartupManager
from core.metrics import iniciar_exporter
from core.state import persist_critical_state, restore_critical_state
from core.utils.log_utils import format_exception_for_log

_log = configurar_logger("entrypoint")


# --- Utilidades internas ---


async def _startup_run_with_timeout(startup: StartupManager) -> Tuple[Any, Any, Any]:
    st = float(getattr(startup, "startup_timeout", 90.0) or 90.0)
    return await asyncio.wait_for(startup.run(), timeout=st + 5.0)


def _try_start_alert_dispatcher(trader: Any) -> Any:
    try:
        from observability.alerts import AlertDispatcher

        bus = getattr(trader, "event_bus", None) or getattr(trader, "bus", None)
        if bus is None:
            return None
        return AlertDispatcher(bus=bus)
    except Exception as exc:
        _log.warning(
            "AlertDispatcher no disponible: %s", format_exception_for_log(exc)
        )
        return None


async def _maybe_await(maybe_coro):
    """Await si es coroutine/awaitable; si no, devuelve tal cual."""
    if inspect.isawaitable(maybe_coro):
        return await maybe_coro
    return maybe_coro


def _safe_call(obj: Any, method: str):
    """Invoca un método si existe, capturando errores (sync)."""
    if obj is None:
        return
    try:
        fn = getattr(obj, method, None)
        if callable(fn):
            return fn()
    except Exception:
        _log.exception(
            "Fallo en llamada defensiva sync",
            extra={"phase": {"event": "safe_call", "method": method, "obj": repr(obj)}},
        )


async def _safe_acall(obj: Any, method: str, timeout: Optional[float] = None):
    """Invoca un método (posible async) si existe; respeta timeout si se provee."""
    if obj is None:
        return
    fn = getattr(obj, method, None)
    if not callable(fn):
        return
    try:
        result = fn()
        if inspect.isawaitable(result):
            if timeout is not None:
                return await asyncio.wait_for(result, timeout=timeout)
            return await result
        # sync
        return result
    except asyncio.TimeoutError:
        _log.warning(
            "Timeout en llamada async defensiva",
            extra={"phase": {"event": "safe_acall", "method": method, "obj": repr(obj)}},
        )
    except Exception:
        _log.exception(
            "Fallo en llamada defensiva async",
            extra={"phase": {"event": "safe_acall", "method": method, "obj": repr(obj)}},
        )


def _normalize_tarea_bot(tarea_bot: Any) -> asyncio.Task:
    """Convierte la tarea devuelta por StartupManager en :class:`asyncio.Task`."""
    if tarea_bot is None:
        raise RuntimeError("tarea_bot es None")
    if isinstance(tarea_bot, asyncio.Task):
        return tarea_bot
    if inspect.iscoroutine(tarea_bot):
        return asyncio.create_task(tarea_bot)
    if callable(getattr(tarea_bot, "__await__", None)):
        return asyncio.create_task(tarea_bot)  # type: ignore[arg-type]
    raise RuntimeError("tarea_bot no es Task ni coroutine")


def _start_mode_service_safe(
    bot: Any, config: Any
) -> OperationalModeService | None:
    try:
        if bot is None or config is None:
            return None
        bus = getattr(bot, "event_bus", None) or getattr(bot, "bus", None)
        mode_service = OperationalModeService(config=config, trader=bot, event_bus=bus)
        mode_service.start()
        return mode_service
    except Exception as exc:
        _log.warning(
            "No se pudo iniciar el servicio de modos operativos: %s",
            format_exception_for_log(exc),
        )
        return None


def _set_bot_notificador(bot: Any, notificador: Any) -> None:
    """Asigna ``notificador`` al trader si expone el atributo."""
    if bot is None or not hasattr(bot, "notificador"):
        return
    try:
        setattr(bot, "notificador", notificador)
    except Exception:
        _log.exception(
            "No se pudo asignar notificador al trader",
            extra={"phase": {"event": "bind_notificador"}},
        )


def _bind_trader_after_restart(
    bot: Any, config: Any, notificador: Any
) -> tuple[OperationalModeService | None, Any]:
    """Tras un ``StartupManager.run`` exitoso en reinicio: notificador, modos, alertas."""
    _set_bot_notificador(bot, notificador)
    mode = _start_mode_service_safe(bot, config)
    alerts = _try_start_alert_dispatcher(bot) if bot is not None else None
    return mode, alerts


# --- Runner con aiomonitor opcional (cierre limpio del loop) ---
def run_with_optional_aiomonitor(coro):
    if os.environ.get("AIOMONITOR") == "1":
        try:
            from aiomonitor import Monitor  # puede no estar instalado
        except Exception as e:
            print(
                f"⚠️ AIOMONITOR=1, pero no se pudo importar aiomonitor: {format_exception_for_log(e)}"
            )
            asyncio.run(coro)
            return

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            print("🔎 Ejecutando con aiomonitor (telnet 127.0.0.1:50101)…")
            with Monitor(loop):  # abre telnet en 127.0.0.1:50101
                loop.run_until_complete(coro)
        finally:
            try:
                pending = asyncio.all_tasks(loop)
                for t in pending:
                    t.cancel()
                loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
            except Exception:
                pass
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
    tarea_bot: Optional[asyncio.Task] = None
    config = None
    mode_service: OperationalModeService | None = None
    alert_dispatcher: Any = None
    stop_event: Optional[asyncio.Event] = None
    tarea_stop: Optional[asyncio.Task] = None
    last_pending: set[asyncio.Task] = set()
    notificador: Any = None
    last_counted_crash_task_id: Optional[int] = None

    async def shutdown_runtime() -> None:
        """Apagado idempotente: se ejecuta siempre al salir de ``main()``."""
        nonlocal observer, exporter_server, mode_service, alert_dispatcher

        if stop_event is not None and not stop_event.is_set():
            stop_event.set()

        _safe_call(bot, "solicitar_parada")

        try:
            if tarea_bot is not None and not tarea_bot.done():
                await asyncio.wait_for(tarea_bot, timeout=10)
        except Exception:
            pass

        if tarea_stop is not None and not tarea_stop.done():
            tarea_stop.cancel()
            try:
                await tarea_stop
            except Exception:
                pass

        if last_pending:
            for t in list(last_pending):
                if not t.done():
                    t.cancel()
            try:
                await asyncio.gather(*last_pending, return_exceptions=True)
            except Exception:
                pass

        obs = observer
        observer = None
        if obs is not None:
            try:
                stop_hot_reload(obs)
            except Exception as e:
                _log.warning(
                    "Error deteniendo hot-reload: %s", format_exception_for_log(e)
                )

        if mode_service is not None:
            try:
                await mode_service.stop()
            except Exception as exc:
                _log.warning(
                    "Error deteniendo servicio de modos: %s",
                    format_exception_for_log(exc),
                )
            mode_service = None

        if alert_dispatcher is not None:
            try:
                await alert_dispatcher.aclose()
            except Exception:
                pass
            alert_dispatcher = None

        try:
            persist_critical_state(reason="shutdown")
        except Exception:
            _log.exception("Fallo persistiendo estado crítico en apagado")

        await _safe_acall(bot, "cerrar", timeout=15)

        try:
            await _maybe_await(stop_supervision())
        except Exception as e:
            _log.warning("Error parando supervisor: %s", format_exception_for_log(e))

        exp = exporter_server
        exporter_server = None
        if exp is not None:
            try:
                if hasattr(exp, "shutdown"):
                    exp.shutdown()
                if hasattr(exp, "server_close"):
                    exp.server_close()
            except Exception as e:
                _log.warning("Error cerrando exporter: %s", format_exception_for_log(e))

        _log.info("Apagado del entrypoint completado")

    try:
        # 1) Arranque del bot
        try:
            restore_critical_state()
            startup = StartupManager()
            with phase("StartupManager.run"):
                triple: Tuple[Any, Any, Any] = await _startup_run_with_timeout(startup)
            if not isinstance(triple, tuple) or len(triple) != 3:
                raise RuntimeError("StartupManager.run() no devolvió (bot, tarea_bot, config)")
            bot, tarea_bot, config = triple
            tarea_bot = _normalize_tarea_bot(tarea_bot)
            mode_service = _start_mode_service_safe(bot, config)
        except Exception as e:
            msg = str(e)
            if "Storage no disponible" in msg:
                print(
                    "❌ Almacenamiento no disponible. "
                    "Verifica los permisos de escritura en el directorio de datos."
                )
            else:
                print(f"❌ {format_exception_for_log(e)}")
            traceback.print_exc()
            return

        # 2) Infraestructura auxiliar (exporter, supervisor, hot-reload)
        try:
            exporter_server = iniciar_exporter()
            await _maybe_await(start_supervision())
            observer = start_hot_reload(
                path=Path.cwd(),
                modules=None,
                watch_paths=(
                    "core",
                    "data_feed",
                    "indicadores",
                    "trader_modular.py",
                    "main.py",
                ),
                modular_reload=(
                    ModularReloadRule(
                        module="indicadores",
                        aliases=("indicators",),
                    ),
                ),
            )
        except Exception:
            print("❌ Fallo durante la inicialización de infraestructura:")
            traceback.print_exc()
            return

        # 3) Notificación de modo y banner
        if getattr(config, "modo_real", False):
            print("🟢 Modo REAL activado")
        else:
            print("🟡 Modo SIMULADO activado")
            print(
                "   Si esperabas REAL: en config/claves.env pon MODO_REAL=true "
                "(o MODO_OPERATIVO=real), o BOT_ENV=production. "
                "Sin eso, development usa paper por defecto. "
                "Si ya tenías REAL y ves esto, revisa desfase de reloj (CLOCK_DRIFT)."
            )

        mostrar_banner()
        print(f"🚀 Iniciando bot de trading... Modo real: {getattr(config, 'modo_real', False)}")

        try:
            notificador = crear_notification_manager_desde_env()
        except Exception as e:
            print(f"⚠️ No se pudo crear el notificador: {format_exception_for_log(e)}")
            notificador = None
        _set_bot_notificador(bot, notificador)

        if bot is not None:
            alert_dispatcher = _try_start_alert_dispatcher(bot)

        # 4) Señales/parada
        stop_event = asyncio.Event()
        tarea_stop = asyncio.create_task(stop_event.wait())

        def detener_bot():
            _log.info(
                "Parada solicitada (handler interno)",
                extra={"phase": {"event": "shutdown_request", "source": "detener_bot"}},
            )
            print("\n🛑 Señal de detención recibida (solicitando parada)…")
            if stop_event is not None:
                stop_event.set()
            _safe_call(bot, "solicitar_parada")

        if platform.system() == "Windows":

            def _win_sigint(_signum, _frame):
                _log.info(
                    "SIGINT recibida (Windows)",
                    extra={"phase": {"event": "shutdown_request", "source": "SIGINT"}},
                )
                print("\n🛑 SIGINT recibida (Windows); solicitando parada…")
                if stop_event is not None:
                    stop_event.set()
                _safe_call(bot, "solicitar_parada")

            try:
                signal.signal(signal.SIGINT, _win_sigint)
            except (ValueError, OSError):
                pass

        if platform.system() != "Windows":
            try:
                loop = asyncio.get_running_loop()

                def _on_sigint():
                    _log.info(
                        "SIGINT recibida",
                        extra={"phase": {"event": "shutdown_request", "source": "SIGINT"}},
                    )
                    print("\n🛑 SIGINT recibida")
                    detener_bot()

                def _on_sigterm():
                    _log.info(
                        "SIGTERM recibida",
                        extra={"phase": {"event": "shutdown_request", "source": "SIGTERM"}},
                    )
                    print("\n🛑 SIGTERM recibida")
                    detener_bot()

                loop.add_signal_handler(signal.SIGINT, _on_sigint)
                loop.add_signal_handler(signal.SIGTERM, _on_sigterm)
            except NotImplementedError:
                pass

        # 5) Bucle de vida y reintentos (semántica de ``retries`` / ``max_retries``: ver docstring del módulo)
        max_retries = 5
        retries = 0
        backoff_base = 5

        while True:
            if tarea_bot is None:
                raise RuntimeError("tarea_bot es None tras el arranque")

            done, last_pending = await asyncio.wait(
                [tarea_bot, tarea_stop],
                return_when=asyncio.FIRST_COMPLETED,
            )

            if tarea_bot in done:
                exc = tarea_bot.exception()
                if exc:
                    tid = id(tarea_bot)
                    if last_counted_crash_task_id != tid:
                        last_counted_crash_task_id = tid
                        retries += 1
                        print(f"❌ Error en la tarea del bot: {format_exception_for_log(exc)}")
                        traceback.print_exception(type(exc), exc, exc.__traceback__)
                    if retries > max_retries:
                        print("🚨 Número máximo de reintentos alcanzado. Deteniendo bot.")
                        try:
                            if notificador and hasattr(notificador, "enviar"):
                                notificador.enviar(
                                    "Bot detenido tras errores consecutivos", "CRITICAL"
                                )
                        except Exception:
                            pass
                        break

                    delay = min(backoff_base * 2 ** (retries - 1), 300)
                    print(f"⏳ Reinicio del bot en {delay}s (intento {retries}/{max_retries})")

                    await _safe_acall(bot, "cerrar", timeout=15)

                    await asyncio.sleep(delay)
                    print("🔄 Reiniciando bot…")
                    try:
                        if mode_service is not None:
                            await mode_service.stop()
                            mode_service = None
                        if alert_dispatcher is not None:
                            try:
                                await alert_dispatcher.aclose()
                            except Exception:
                                pass
                            alert_dispatcher = None
                        startup = StartupManager()
                        triple = await _startup_run_with_timeout(startup)
                        if not isinstance(triple, tuple) or len(triple) != 3:
                            raise RuntimeError(
                                "StartupManager.run() no devolvió (bot, tarea_bot, config)"
                            )
                        bot, tarea_bot, config = triple
                        tarea_bot = _normalize_tarea_bot(tarea_bot)
                        mode_service, alert_dispatcher = _bind_trader_after_restart(
                            bot, config, notificador
                        )
                    except Exception as e:
                        print(f"❌ Error reiniciando bot: {format_exception_for_log(e)}")
                        traceback.print_exc()
                        continue
                    last_counted_crash_task_id = None
                    retries = 0
                    continue
                print("✅ Bot finalizado sin errores.")
                break

            if tarea_stop is not None and tarea_stop in done:
                _safe_call(bot, "solicitar_parada")
                break

    except asyncio.CancelledError:
        print("🛑 Cancelación detectada.")
        raise
    except KeyboardInterrupt:
        print("🛑 Interrupción por teclado detectada.")
    finally:
        await shutdown_runtime()


if __name__ == "__main__":
    try:
        run_with_optional_aiomonitor(main())
    except KeyboardInterrupt:
        print("\n🛑 Bot detenido manualmente.")
    except Exception:
        print("\n❌ Error inesperado:")
        traceback.print_exc()
