# core/hot_reload.py
from __future__ import annotations

import errno
import os
import sys
import time
import threading
from pathlib import Path
from typing import Iterable, Optional, Set

try:
    from watchdog.events import FileSystemEventHandler, FileSystemEvent
    from watchdog.observers import Observer
    try:
        # PollingObserver existe en watchdog>=2.1
        from watchdog.observers.polling import PollingObserver  # type: ignore
    except Exception:  # pragma: no cover
        PollingObserver = Observer  # type: ignore
except Exception as e:  # pragma: no cover
    raise RuntimeError(
        "watchdog no está instalado. Instálalo con: pip install watchdog"
    ) from e


DEFAULT_EXCLUDES: Set[str] = {
    ".git",
    ".idea",
    ".hg",
    ".ropeproject",
    ".svn",
    ".vscode",
    "__pycache__",
    ".mypy_cache",
    ".pytest_cache",
    ".ruff_cache",
    ".venv",
    "venv",
    "env",
    "node_modules",
    "dist",
    "build",
    "logs",
}

# Directorios de estado que suelen cambiar en runtime y que no deberían reiniciar
STATE_EXCLUDES: Set[str] = {
    "estado",
    "data",
    "tmp",
    "cache",
}


def _path_is_excluded(path: Path, exclude: Set[str]) -> bool:
    """Devuelve True si el path toca alguno de los directorios excluidos."""
    parts = set(p.name for p in path.resolve().parents) | {path.name}
    return bool(parts & exclude)


class _DebouncedReloader(FileSystemEventHandler):
    """
    Observa cambios en *.py y reinicia el proceso tras un periodo de quietud (debounce).

    - Agrupa múltiples cambios rápidos en una sola acción de reinicio.
    - Ignora rutas dentro de 'exclude'.
    """

    def __init__(
        self,
        root: Path,
        *,
        debounce_seconds: float = 1.0,
        exclude: Optional[Iterable[str]] = None,
        verbose: bool = True,
    ) -> None:
        super().__init__()
        self.root = root
        self.debounce = max(0.1, float(debounce_seconds))
        self.exclude: Set[str] = set(exclude or set())
        self.verbose = verbose

        self._timer: Optional[threading.Timer] = None
        self._lock = threading.Lock()
        self._last_event_ts: float = 0.0
        self._last_path: Optional[Path] = None

    # ---- Watchdog callbacks ----

    def on_any_event(self, event: FileSystemEvent) -> None:  # type: ignore[override]
        # Nos interesan solo creaciones/modificaciones/borrados de archivos .py
        if event.is_directory:
            return
        try:
            p = Path(event.src_path)
        except Exception:
            return

        if p.suffix.lower() != ".py":
            return
        if _path_is_excluded(p, self.exclude):
            return

        with self._lock:
            self._last_event_ts = time.time()
            self._last_path = p

            if self._timer and self._timer.is_alive():
                # Reinicia la cuenta regresiva
                self._timer.cancel()
                self._timer = None

            self._timer = threading.Timer(self.debounce, self._maybe_restart)
            self._timer.daemon = True
            self._timer.start()

            if self.verbose:
                rel = p.relative_to(self.root) if str(p).startswith(str(self.root)) else p
                print(f"👀 Cambio detectado: {rel} (reinicio en {self.debounce:.2f}s)")
                sys.stdout.flush()

    # ---- Internals ----

    def _maybe_restart(self) -> None:
        with self._lock:
            # Si hubo otra modificación durante la espera, reprograma
            delta = time.time() - self._last_event_ts
            if delta < self.debounce * 0.5:
                # ruido; reintentar
                self._timer = threading.Timer(self.debounce, self._maybe_restart)
                self._timer.daemon = True
                self._timer.start()
                return
            trig = self._last_path

        if self.verbose and trig is not None:
            try:
                rel = trig.relative_to(self.root)
            except Exception:
                rel = trig
            print(f"♻️  Hot-reload: reiniciando por cambio en {rel}")
            sys.stdout.flush()

        # Reinicio de proceso: reemplaza el binario actual y preserva argv/env
        python = sys.executable
        argv = [python] + sys.argv
        # Asegura flush de stdout/stderr
        try:
            sys.stdout.flush()
            sys.stderr.flush()
        except Exception:
            pass

        # En Windows, os.execv no reemplaza el proceso como en POSIX,
        # pero sigue siendo la vía más directa. Si falla, fallback a exit+spawn.
        try:
            os.execv(python, argv)
        except Exception:
            import subprocess
            subprocess.Popen(argv, env=os.environ.copy(), close_fds=False)
            os._exit(0)

def _schedule_observer(
    observer,
    handler: FileSystemEventHandler,
    root: Path,
    *,
    ignore_patterns: Iterable[str] | None = None,
) -> None:
    """Helper to schedule the observer with optional ignore patterns."""

    extra_kwargs = {}
    if ignore_patterns:
        patterns = sorted(set(ignore_patterns))
        if patterns:
            extra_kwargs = {
                "ignore_patterns": patterns,
                "ignore_directories": True,
            }

    if extra_kwargs:
        try:
            observer.schedule(handler, str(root), recursive=True, **extra_kwargs)
            return
        except TypeError:
            # Backend no soporta ignore_patterns; reintenta sin filtros.
            pass

    observer.schedule(handler, str(root), recursive=True)


def start_hot_reload(
    path: Path,
    modules: Optional[Iterable[str]] = None,  # mantenido por compatibilidad (no usado aquí)
    *,
    debounce_seconds: float = 1.0,
    exclude: Optional[Iterable[str]] = None,
    polling: Optional[bool] = None,
    ignore_patterns: Optional[Iterable[str]] = None,
    verbose: bool = True,
):
    """
    Inicia el observador de hot-reload. Devuelve el observer para detenerlo luego.

    Parameters
    ----------
    path : Path
        Directorio raíz a observar.
    modules : Iterable[str] | None
        (Compat) Lista de módulos a recargar. En esta versión reiniciamos el proceso,
        por lo que no se utiliza.
    debounce_seconds : float
        Tiempo de inactividad requerido antes de reiniciar (agrupa cambios rápidos).
    exclude : Iterable[str] | None
        Nombres de carpetas a excluir (se cruzan con DEFAULT_EXCLUDES/STATE_EXCLUDES).
    polling : bool | None
        Forzar PollingObserver (útil en FS remotos o contenedores).
    verbose : bool
    ignore_patterns : Iterable[str] | None
        Patrones shell-style adicionales a ignorar en watchdog (si el backend lo soporta).
        Mostrar logs en consola.
    """
    root = Path(path).resolve()
    root.mkdir(parents=True, exist_ok=True)

    # Excluye directorios comunes + estado por defecto; permite extender por parámetro
    excludes = set(DEFAULT_EXCLUDES) | set(STATE_EXCLUDES)
    if exclude:
        excludes |= {str(x) for x in exclude}

    handler = _DebouncedReloader(root, debounce_seconds=debounce_seconds, exclude=excludes, verbose=verbose)
    ignore_patterns_set: Set[str] = set()
    for item in excludes:
        if not item:
            continue
        # Ignora el directorio y su contenido
        ignore_patterns_set.add(f"*/{item}")
        ignore_patterns_set.add(f"*/{item}/*")
    if ignore_patterns:
        ignore_patterns_set.update(str(p) for p in ignore_patterns)

    # Heurística para elegir backend
    force_poll = polling if polling is not None else (os.getenv("WATCHDOG_POLLING", "0") == "1")
    observer_cls = PollingObserver if force_poll else Observer

    def _instantiate(cls):
        try:
            return cls(timeout=1.0)  # type: ignore[call-arg]
        except TypeError:
            return cls()  # type: ignore[call-arg]

    def _start(cls):
        observer_instance = _instantiate(cls)
        _schedule_observer(
            observer_instance,
            handler,
            root,
            ignore_patterns=ignore_patterns_set,
        )
        observer_instance.start()
        return observer_instance

    if observer_cls is PollingObserver:
        observer = _start(observer_cls)
    else:
        try:
            observer = _start(observer_cls)
        except OSError as exc:
            if getattr(exc, "errno", None) == errno.ENOSPC:
                if verbose:
                    print(
                        "⚠️  Límite de inotify alcanzado; cambiando a modo polling.",
                    )
                    sys.stdout.flush()
                observer_cls = PollingObserver
                observer = _start(observer_cls)
            else:
                raise


    if verbose:
        excludes_str = ", ".join(sorted(excludes))
        mode = "polling" if observer_cls is PollingObserver else "native"
        print(f"🔄 Hot-reload iniciado en {root} (modo {mode}, debounce={debounce_seconds}s)")
        print(f"   Excluyendo: {excludes_str}")
        sys.stdout.flush()

    return observer


def stop_hot_reload(observer) -> None:
    """Detiene el observador de hot-reload con espera de cierre limpio."""
    try:
        observer.stop()
    except Exception:
        pass
    try:
        observer.join(timeout=3.0)
    except Exception:
        pass


