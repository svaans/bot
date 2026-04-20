"""Alinear el reloj del sistema con la hora UTC de Binance.

Motivación
----------
Binance rechaza peticiones firmadas cuando ``recvWindow`` se queda pequeño
frente al desfase del reloj local (error ``-1021``). Aunque CCXT compensa
internamente con ``adjustForTimeDifference``, la UX del bot mejora cuando el
reloj del sistema está cerca de la hora UTC real: los logs cuadran, la base de
datos de operaciones no retrocede en el tiempo tras un reinicio, y los
``timestamps`` de las velas no chocan con los que emite Binance.

En Windows, ``SetSystemTime`` requiere proceso elevado. Si el bot se ejecuta
sin privilegios de administrador, la llamada falla. Este módulo añade:

* ``is_windows_admin()``: detección del rol antes de intentar sincronizar.
* ``try_sync_via_w32tm_elevated()``: fallback con ``w32tm /resync`` lanzado a
  través de ``ShellExecuteEx`` con verbo ``runas`` (dispara UAC una sola vez).
* ``auto_sync_system_clock_from_binance()``: orquestador cross-platform que
  decide el mejor método disponible y devuelve un dict con el resultado, listo
  para loggear.

Uso manual (consola **como administrador**), desde la raíz del repo::

    python -m core.utils.binance_time_sync

Arranque del bot: controla el comportamiento con ``BINANCE_SYNC_SYSTEM_TIME``
en ``config/claves.env``. Valores aceptados (case-insensitive):

* ``off`` / ``false`` / ``0`` / ``no``  → no hace nada.
* ``auto`` / ``true`` / ``1`` / ``on`` (**default**) → intenta ``SetSystemTime``
  si el proceso ya es admin; si no lo es, registra un warning informativo sin
  levantar UAC.
* ``elevate``                           → si el proceso no es admin, lanza
  ``w32tm /resync`` en una shell elevada (se muestra popup UAC una vez por
  ejecución del bot).
"""
from __future__ import annotations

import json
import logging
import os
import shutil
import subprocess
import sys
import urllib.request
from datetime import datetime, timezone
from typing import Final

_BINANCE_TIME_URL: Final = "https://api.binance.com/api/v3/time"

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def is_windows_admin() -> bool:
    """Devuelve ``True`` si el proceso actual tiene privilegios de administrador.

    Solo tiene sentido en Windows; en otras plataformas devuelve ``False``
    porque ``SetSystemTime`` no aplica (la ruta UNIX usa ``sudo``).
    """
    if sys.platform != "win32":
        return False
    try:
        import ctypes

        return bool(ctypes.windll.shell32.IsUserAnAdmin())
    except Exception:
        return False


def _fetch_binance_server_ms(timeout: float) -> int | None:
    try:
        req = urllib.request.Request(
            _BINANCE_TIME_URL,
            headers={"User-Agent": "bot-binance-time-sync/1.0"},
        )
        with urllib.request.urlopen(req, timeout=timeout) as resp:  # nosec B310
            data = json.loads(resp.read().decode())
        server_ms = int(data.get("serverTime", 0))
        return server_ms or None
    except Exception as exc:  # pragma: no cover - red externa
        logger.debug("No se pudo obtener serverTime de Binance: %s", exc)
        return None


# ---------------------------------------------------------------------------
# Windows: SetSystemTime (requiere admin)
# ---------------------------------------------------------------------------
def try_sync_windows_system_time_from_binance(*, timeout: float = 5.0) -> bool:
    """Ajusta el reloj del sistema a la hora UTC de Binance.

    Solo Windows y requiere privilegios de administrador. Devuelve ``True`` si
    ``SetSystemTime`` tuvo éxito.
    """
    if sys.platform != "win32":
        return False

    import ctypes
    from ctypes import wintypes

    server_ms = _fetch_binance_server_ms(timeout)
    if not server_ms:
        return False

    secs = server_ms / 1000.0
    ms_part = int(server_ms % 1000)
    t = datetime.fromtimestamp(secs, tz=timezone.utc)

    class SYSTEMTIME(ctypes.Structure):
        _fields_ = [
            ("wYear", wintypes.WORD),
            ("wMonth", wintypes.WORD),
            ("wDayOfWeek", wintypes.WORD),
            ("wDay", wintypes.WORD),
            ("wHour", wintypes.WORD),
            ("wMinute", wintypes.WORD),
            ("wSecond", wintypes.WORD),
            ("wMilliseconds", wintypes.WORD),
        ]

    st = SYSTEMTIME()
    st.wYear = t.year
    st.wMonth = t.month
    st.wDayOfWeek = 0
    st.wDay = t.day
    st.wHour = t.hour
    st.wMinute = t.minute
    st.wSecond = t.second
    st.wMilliseconds = ms_part

    return bool(ctypes.windll.kernel32.SetSystemTime(ctypes.byref(st)))


# ---------------------------------------------------------------------------
# Windows: elevación vía UAC + w32tm /resync
# ---------------------------------------------------------------------------
def try_sync_via_w32tm_elevated(*, wait_timeout: float = 30.0) -> bool:
    """Lanza ``w32tm /resync`` en una shell elevada.

    En Windows la API ``ShellExecuteEx`` con verbo ``runas`` dispara UAC una
    sola vez para esta llamada. Si el proceso ya tiene privilegios de
    administrador, se ejecuta ``w32tm`` directamente. Devuelve ``True`` cuando
    el proceso elevado termina con ``exit code`` 0.

    No se usa en otras plataformas (devuelve ``False``).
    """
    if sys.platform != "win32":
        return False

    if is_windows_admin():
        try:
            completed = subprocess.run(  # nosec B603,B607
                ["w32tm", "/resync", "/force"],
                check=False,
                capture_output=True,
                text=True,
                timeout=wait_timeout,
            )
            ok = completed.returncode == 0
            if not ok:
                logger.debug(
                    "w32tm /resync /force devolvió %s: %s",
                    completed.returncode,
                    (completed.stderr or completed.stdout or "").strip(),
                )
            return ok
        except Exception as exc:  # pragma: no cover
            logger.debug("w32tm /resync falló: %s", exc)
            return False

    try:
        import ctypes
        from ctypes import wintypes

        SEE_MASK_NOCLOSEPROCESS = 0x00000040
        SEE_MASK_NOASYNC = 0x00000100
        SW_HIDE = 0
        INFINITE = 0xFFFFFFFF

        class SHELLEXECUTEINFOW(ctypes.Structure):
            _fields_ = [
                ("cbSize", wintypes.DWORD),
                ("fMask", wintypes.ULONG),
                ("hwnd", wintypes.HWND),
                ("lpVerb", wintypes.LPCWSTR),
                ("lpFile", wintypes.LPCWSTR),
                ("lpParameters", wintypes.LPCWSTR),
                ("lpDirectory", wintypes.LPCWSTR),
                ("nShow", ctypes.c_int),
                ("hInstApp", wintypes.HINSTANCE),
                ("lpIDList", ctypes.c_void_p),
                ("lpClass", wintypes.LPCWSTR),
                ("hkeyClass", wintypes.HKEY),
                ("dwHotKey", wintypes.DWORD),
                ("hIconOrMonitor", wintypes.HANDLE),
                ("hProcess", wintypes.HANDLE),
            ]

        info = SHELLEXECUTEINFOW()
        info.cbSize = ctypes.sizeof(SHELLEXECUTEINFOW)
        info.fMask = SEE_MASK_NOCLOSEPROCESS | SEE_MASK_NOASYNC
        info.hwnd = None
        info.lpVerb = "runas"
        info.lpFile = "cmd.exe"
        # /c cierra la consola tras ejecutar. Encadenamos w32tm start para
        # asegurar que el servicio esté arriba antes de pedir un resync.
        info.lpParameters = (
            "/c net start w32time >nul 2>&1 & w32tm /resync /force"
        )
        info.lpDirectory = None
        info.nShow = SW_HIDE
        info.hInstApp = None

        shell32 = ctypes.windll.shell32
        shell32.ShellExecuteExW.argtypes = [ctypes.POINTER(SHELLEXECUTEINFOW)]
        shell32.ShellExecuteExW.restype = wintypes.BOOL

        if not shell32.ShellExecuteExW(ctypes.byref(info)):
            # Usuario canceló UAC o falló el shell execute.
            return False

        if not info.hProcess:
            return False

        kernel32 = ctypes.windll.kernel32
        # WaitForSingleObject recibe milisegundos.
        wait_ms = int(max(1.0, wait_timeout) * 1000)
        kernel32.WaitForSingleObject(info.hProcess, wait_ms)

        exit_code = wintypes.DWORD(0)
        got = kernel32.GetExitCodeProcess(info.hProcess, ctypes.byref(exit_code))
        kernel32.CloseHandle(info.hProcess)
        if not got:
            return False
        return exit_code.value == 0
    except Exception as exc:  # pragma: no cover
        logger.debug("ShellExecuteEx(runas, w32tm) falló: %s", exc)
        return False


# ---------------------------------------------------------------------------
# Linux / macOS: sntp / ntpdate con sudo -n (sin contraseña)
# ---------------------------------------------------------------------------
def _try_sync_unix(*, timeout: float = 15.0) -> bool:
    if sys.platform == "win32":
        return False

    # En macOS ``sntp -sS`` existe; en Linux moderno también (ntpsec/chrony
    # proveen su propio binario). Caemos a ``ntpdate`` si está disponible.
    candidates: list[list[str]] = []
    if shutil.which("sntp"):
        candidates.append(["sntp", "-sS", "time.google.com"])
    if shutil.which("ntpdate"):
        candidates.append(["ntpdate", "-u", "time.google.com"])

    if not candidates:
        return False

    sudo = shutil.which("sudo")
    for base_cmd in candidates:
        cmd = [sudo, "-n", *base_cmd] if sudo else base_cmd
        try:
            completed = subprocess.run(  # nosec B603
                cmd,
                check=False,
                capture_output=True,
                text=True,
                timeout=timeout,
            )
        except Exception as exc:  # pragma: no cover
            logger.debug("Sync UNIX con %s falló: %s", cmd, exc)
            continue
        if completed.returncode == 0:
            return True
    return False


# ---------------------------------------------------------------------------
# Orquestador
# ---------------------------------------------------------------------------
def _normalize_mode(raw: str | None) -> str:
    """Traduce el valor crudo del env var a un modo canónico.

    Valores aceptados:
      - ``off`` / ``false`` / ``0`` / ``no``            → ``"off"``
      - ``auto`` / ``true`` / ``1`` / ``yes`` / ``on``  → ``"auto"`` (default)
      - ``elevate`` / ``uac`` / ``runas``               → ``"elevate"``

    Cualquier otro valor (incluye ``None``) se trata como ``"auto"`` para que
    el comportamiento por defecto sea sincronizar.
    """
    value = (raw or "").strip().lower()
    if value in {"off", "false", "0", "no", "disable", "disabled"}:
        return "off"
    if value in {"elevate", "uac", "runas", "admin"}:
        return "elevate"
    # auto / true / on / yes / '' → auto
    return "auto"


def auto_sync_system_clock_from_binance(
    *,
    mode: str | None = None,
    timeout: float = 5.0,
) -> dict[str, object]:
    """Intenta sincronizar el reloj del SO con la hora UTC de Binance.

    Parameters
    ----------
    mode:
        Valor normalizado (``"auto"``, ``"off"``, ``"elevate"``). Si es
        ``None`` se lee ``BINANCE_SYNC_SYSTEM_TIME`` del entorno.
    timeout:
        Segundos máximos para la llamada HTTP a ``api.binance.com``.

    Returns
    -------
    dict
        ``{"synced": bool, "method": str, "mode": str, "platform": str,
        "elevated": bool}``. Diseñado para ser serializado directamente a log.
    """
    effective_mode = _normalize_mode(
        mode if mode is not None else os.getenv("BINANCE_SYNC_SYSTEM_TIME")
    )
    result: dict[str, object] = {
        "synced": False,
        "method": "skipped",
        "mode": effective_mode,
        "platform": sys.platform,
        "elevated": False,
    }

    if effective_mode == "off":
        return result

    if sys.platform == "win32":
        if is_windows_admin():
            if try_sync_windows_system_time_from_binance(timeout=timeout):
                result.update(synced=True, method="SetSystemTime", elevated=True)
                return result
            result["method"] = "SetSystemTime:failed"
            return result

        if effective_mode == "elevate":
            if try_sync_via_w32tm_elevated():
                result.update(
                    synced=True, method="w32tm/resync+uac", elevated=True
                )
                return result
            result["method"] = "w32tm/resync+uac:failed"
            return result

        result["method"] = "SetSystemTime:requires_admin"
        return result

    if _try_sync_unix(timeout=max(timeout, 10.0)):
        result.update(synced=True, method="sntp/ntpdate")
        return result
    result["method"] = "sntp/ntpdate:unavailable_or_failed"
    return result


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    outcome = auto_sync_system_clock_from_binance(mode="elevate")
    if outcome["synced"]:
        print(
            f"Reloj del sistema actualizado ({outcome['method']} "
            f"/ elevated={outcome['elevated']})."
        )
        sys.exit(0)
    print(
        "No se pudo sincronizar el reloj automáticamente. "
        f"Detalle: {outcome}. Ejecuta esta consola como administrador o "
        "sincroniza con w32tm /resync manualmente."
    )
    sys.exit(1)


if __name__ == "__main__":
    main()
