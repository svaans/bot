"""PID lock file para evitar instancias duplicadas del bot.

Dos procesos ``python main.py`` en modo REAL compartiendo las mismas claves
generan órdenes duplicadas y descuadran la base de operaciones. Este módulo
implementa un lock basado en un fichero PID con dos capas de verificación:

1. ``os.kill(pid, 0)`` comprueba si el PID sigue vivo.
2. Se verifica además que el ejecutable del PID apunte a ``python``, evitando
   falsos positivos si el SO reutilizó el PID para otro proceso.

Uso típico al principio de ``main.py``::

    from core.utils.pid_lock import PidLockError, acquire_pid_lock, release_pid_lock

    try:
        lock = acquire_pid_lock()
    except PidLockError as exc:
        print(f"❌ {exc}")
        sys.exit(1)

    try:
        ...  # arrancar bot
    finally:
        release_pid_lock(lock)

El lock es voluntario: si se omite o falla la escritura (p.ej. disco lleno),
el bot arranca igualmente con un warning. Nunca aborta el proceso por un fallo
del propio mecanismo de lock, solo cuando detecta que ya hay otro vivo.

Se fuerza el override si se exporta ``BOT_PID_LOCK_FORCE=true``; útil cuando
se sabe que el PID grabado es huérfano de un crash sin apagado limpio.
"""
from __future__ import annotations

import json
import logging
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

DEFAULT_PID_PATH = Path("estado") / "bot.pid"


class PidLockError(RuntimeError):
    """Se levanta cuando ya hay otra instancia del bot corriendo."""


@dataclass(frozen=True)
class PidLock:
    path: Path
    pid: int


def _is_force_override() -> bool:
    return os.getenv("BOT_PID_LOCK_FORCE", "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def _process_is_alive(pid: int) -> bool:
    """Devuelve ``True`` si existe un proceso con ese PID.

    En POSIX usa ``os.kill(pid, 0)``. En Windows usa ``OpenProcess`` sobre
    ``kernel32`` (un PID inexistente falla con error 87 o devuelve 0).
    """
    if pid <= 0:
        return False
    if os.name == "nt":
        try:
            import ctypes
            from ctypes import wintypes

            PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
            STILL_ACTIVE = 259
            kernel32 = ctypes.windll.kernel32
            kernel32.OpenProcess.restype = wintypes.HANDLE
            handle = kernel32.OpenProcess(
                PROCESS_QUERY_LIMITED_INFORMATION, False, pid
            )
            if not handle:
                return False
            try:
                exit_code = wintypes.DWORD(0)
                if kernel32.GetExitCodeProcess(handle, ctypes.byref(exit_code)):
                    return exit_code.value == STILL_ACTIVE
                return True
            finally:
                kernel32.CloseHandle(handle)
        except Exception:
            return False
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        # Existe pero no podemos señalizar: otro usuario/elevación.
        return True
    except OSError:
        return False


def _process_is_python(pid: int) -> bool:
    """Heurística para confirmar que ``pid`` es otro intérprete Python.

    Si no se puede determinar (permisos, plataforma) devolvemos ``True`` para
    quedarnos del lado conservador (no pisar el lock).
    """
    if os.name == "nt":
        try:
            import ctypes
            from ctypes import wintypes

            PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
            kernel32 = ctypes.windll.kernel32
            kernel32.OpenProcess.restype = wintypes.HANDLE
            handle = kernel32.OpenProcess(
                PROCESS_QUERY_LIMITED_INFORMATION, False, pid
            )
            if not handle:
                return True
            try:
                QueryFullProcessImageNameW = kernel32.QueryFullProcessImageNameW
                buf = ctypes.create_unicode_buffer(1024)
                size = wintypes.DWORD(len(buf))
                if QueryFullProcessImageNameW(
                    handle, 0, buf, ctypes.byref(size)
                ):
                    exe = (buf.value or "").lower()
                    return "python" in exe
                return True
            finally:
                kernel32.CloseHandle(handle)
        except Exception:
            return True
    try:
        exe_link = Path(f"/proc/{pid}/exe")
        if exe_link.exists():
            target = os.readlink(exe_link)
            return "python" in Path(target).name.lower()
    except Exception:
        pass
    return True


def _read_existing(path: Path) -> Optional[dict]:
    try:
        raw = path.read_text(encoding="utf-8").strip()
    except FileNotFoundError:
        return None
    except OSError as exc:
        logger.debug("No se pudo leer PID lock %s: %s", path, exc)
        return None
    if not raw:
        return None
    try:
        data = json.loads(raw)
        if isinstance(data, dict) and "pid" in data:
            return data
    except json.JSONDecodeError:
        try:
            pid = int(raw)
            return {"pid": pid}
        except ValueError:
            return None
    return None


def acquire_pid_lock(path: Path | str | None = None) -> PidLock:
    """Crea un fichero PID exclusivo para este proceso.

    Parameters
    ----------
    path:
        Ruta del fichero. Default ``estado/bot.pid``.

    Raises
    ------
    PidLockError
        Si ya hay otra instancia viva del bot.
    """
    target = Path(path) if path is not None else DEFAULT_PID_PATH
    target = target.resolve() if target.is_absolute() else target
    try:
        target.parent.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        logger.warning(
            "No se pudo crear el directorio del PID lock %s: %s", target.parent, exc
        )
        return PidLock(path=target, pid=os.getpid())

    existing = _read_existing(target)
    if existing is not None:
        other_pid = int(existing.get("pid", 0) or 0)
        if other_pid and other_pid != os.getpid():
            alive = _process_is_alive(other_pid)
            same_type = alive and _process_is_python(other_pid)
            if alive and same_type and not _is_force_override():
                raise PidLockError(
                    "Ya hay otra instancia del bot corriendo en PID "
                    f"{other_pid} (según {target}). Ciérrala antes de "
                    "lanzar una nueva, o exporta BOT_PID_LOCK_FORCE=true "
                    "si sabes que el PID es huérfano."
                )
            if alive and not same_type:
                logger.warning(
                    "PID lock %s apunta a %s pero el proceso no parece un "
                    "intérprete Python; se sobrescribe como stale.",
                    target,
                    other_pid,
                )

    payload = {
        "pid": os.getpid(),
        "started_at": time.time(),
        "argv": sys.argv,
    }
    try:
        target.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    except OSError as exc:
        logger.warning(
            "No se pudo escribir el PID lock %s: %s (continuando sin lock)",
            target,
            exc,
        )
    return PidLock(path=target, pid=os.getpid())


def release_pid_lock(lock: PidLock | None) -> None:
    """Borra el fichero PID si el lock pertenece a este proceso.

    Nunca levanta excepciones: se limita a registrar en debug.
    """
    if lock is None:
        return
    if lock.pid != os.getpid():
        return
    try:
        existing = _read_existing(lock.path)
        if existing and int(existing.get("pid", 0) or 0) != os.getpid():
            return
        lock.path.unlink(missing_ok=True)
    except Exception as exc:  # pragma: no cover - best effort cleanup
        logger.debug("No se pudo borrar PID lock %s: %s", lock.path, exc)
