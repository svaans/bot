# core/hot_reload.py
from __future__ import annotations

import errno
import os
import sys
import time
import threading
import logging
from pathlib import Path
from typing import Iterable, Optional, Sequence, Set

from core.state import persist_critical_state

try:
    from watchdog.events import (
        FileSystemEventHandler,
        FileSystemEvent,
        PatternMatchingEventHandler,
    )
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
    "site-packages",
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


NOISY_FILE_PATTERNS: Set[str] = {
    "*.pyc",
    "*.pyo",
    "*.log",
    "*.tmp",
    "*.swp",
    "*.swo",
    "*.cache",
    "*.tmp.*",
    "*.dist-info",
    "**/*.pyc",
    "**/*.pyo",
    "**/*.log",
    "**/*.tmp",
    "**/*.swp",
    "**/*.swo",
    "**/*.dist-info",
}


_NOISY_WATCHDOG_LOGGERS: tuple[str, ...] = (
    "watchdog.observers.inotify_buffer",
    "watchdog.observers.inotify_c",
    "watchdog.observers.inotify",
    "watchdog.observers.read_buffer",
)


def _configure_watchdog_logging(*, min_level: int = logging.WARNING) -> None:
    """Eleva el nivel de logging de watchdog para evitar ruido en DEBUG.

    Watchdog emite eventos ``DEBUG`` por cada archivo observado. En entornos donde
    el árbol del proyecto contiene un ``venv/`` local, esto genera una cantidad
    masiva de mensajes (por ejemplo ``IN_ISDIR|IN_OPEN``) que saturan la salida.

    Para mantener el output limpio, aumentamos el nivel mínimo de los loggers
    ruidosos sólo si actualmente están en ``NOTSET`` o en un nivel más permisivo
    que ``min_level``. De esta forma, respetamos cualquier configuración explícita
    que haya hecho el usuario.
    """

    for name in _NOISY_WATCHDOG_LOGGERS:
        logger = logging.getLogger(name)
        if logger.level in (logging.NOTSET, 0) or logger.level < min_level:
            logger.setLevel(min_level)


def _path_is_excluded(path: Path, exclude: Set[str]) -> bool:
    """Devuelve True si el path toca alguno de los directorios excluidos."""
    parts = set(p.name for p in path.resolve().parents) | {path.name}
    return bool(parts & exclude)


def _path_matches_whitelist(path: Path, allowed: Path) -> bool:
    """Devuelve True si `path` coincide exactamente o es descendiente de `allowed`."""
    if allowed == path:
        return True
    if allowed.is_file() or allowed.suffix:
        return False
    try:
        path.relative_to(allowed)
        return True
    except ValueError:
        return False


class _DebouncedReloader(PatternMatchingEventHandler):
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
        watch_whitelist: Optional[Sequence[Path]] = None,
        ignore_patterns: Optional[Iterable[str]] = None,
    ) -> None:
        super().__init__(
            patterns=("*.py",),
            ignore_patterns=tuple(ignore_patterns or ()),
            ignore_directories=True,
            case_sensitive=False,
        )
        self.root = root
        self.debounce = max(0.1, float(debounce_seconds))
        self.exclude: Set[str] = set(exclude or set())
        self.verbose = verbose
        self._watch_whitelist: tuple[Path, ...] = tuple(
            Path(p).resolve() for p in (watch_whitelist or ())
        )

        self._timer: Optional[threading.Timer] = None
        self._lock = threading.Lock()
        self._last_event_ts: float = 0.0
        self._last_path: Optional[Path] = None

    # ---- Watchdog callbacks ----

    def on_modified(self, event: FileSystemEvent) -> None:  # type: ignore[override]
        self._handle_event(event)

    def on_created(self, event: FileSystemEvent) -> None:  # type: ignore[override]
        self._handle_event(event)

    def on_moved(self, event: FileSystemEvent) -> None:  # type: ignore[override]
        self._handle_event(event)

    def _handle_event(self, event: FileSystemEvent) -> None:
        if getattr(event, "is_directory", False):
            return

        candidate_paths = []
        dest_path = getattr(event, "dest_path", None)
        if dest_path:
            candidate_paths.append(dest_path)
        src_path = getattr(event, "src_path", None)
        if src_path:
            candidate_paths.append(src_path)

        selected_path: Optional[Path] = None
        for raw_path in candidate_paths:
            try:
                p = Path(raw_path)
            except Exception:
                continue

            if p.suffix.lower() != ".py":
                continue
            if _path_is_excluded(p, self.exclude):
                continue

            if self._watch_whitelist:
                try:
                    resolved = p.resolve()
                except FileNotFoundError:
                    resolved = p.absolute()
                if not any(
                    _path_matches_whitelist(resolved, allowed)
                    for allowed in self._watch_whitelist
                ):
                    continue

            selected_path = p
            break

        if selected_path is None:
            return

        with self._lock:
            self._last_event_ts = time.time()
            self._last_path = selected_path

            if self._timer and self._timer.is_alive():
                # Reinicia la cuenta regresiva
                self._timer.cancel()
                self._timer = None

            self._timer = threading.Timer(self.debounce, self._maybe_restart)
            self._timer.daemon = True
            self._timer.start()

            if self.verbose:
                rel = (
                    selected_path.relative_to(self.root)
                    if str(selected_path).startswith(str(self.root))
                    else selected_path
                )
                print(
                    f"👀 Cambio detectado: {rel} (reinicio en {self.debounce:.2f}s)"
                )
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
        try:
            persist_critical_state(reason="hot_reload")
        except Exception:
            logging.getLogger("hot_reload").exception(
                "No se pudo persistir el estado crítico antes del reinicio"
            )
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
    path: Path,
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
            observer.schedule(handler, str(path), recursive=True, **extra_kwargs)
            return
        except TypeError:
            # Backend no soporta ignore_patterns; reintenta sin filtros.
            pass

    observer.schedule(handler, str(path), recursive=True)


def start_hot_reload(
    path: Path,
    modules: Optional[Iterable[str]] = None,  # mantenido por compatibilidad (no usado aquí)
    *,
    debounce_seconds: float = 1.0,
    exclude: Optional[Iterable[str]] = None,
    polling: Optional[bool] = None,
    ignore_patterns: Optional[Iterable[str]] = None,
    verbose: bool = True,
    watch_paths: Optional[Iterable[Path | str]] = None,
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
    watch_paths : Iterable[Path | str] | None
        Subconjunto de rutas dentro de `path` a vigilar. Si se omite, se observa todo el árbol.
    """
    _configure_watchdog_logging()
    
    root = Path(path).resolve()
    root.mkdir(parents=True, exist_ok=True)

    # Excluye directorios comunes + estado por defecto; permite extender por parámetro
    excludes = set(DEFAULT_EXCLUDES) | set(STATE_EXCLUDES)
    if exclude:
        excludes |= {str(x) for x in exclude}

    watch_whitelist: list[Path] = []
    schedule_targets: list[Path] = []
    watch_whitelist_set: Set[Path] = set()
    schedule_target_set: Set[Path] = set()
    if watch_paths:
        for raw in watch_paths:
            target = Path(raw)
            if not target.is_absolute():
                target = (root / target).resolve()
            else:
                target = target.resolve()

            try:
                target.relative_to(root)
            except ValueError:
                continue

            if target.is_file():
                parent = target.parent
                if target not in watch_whitelist_set:
                    watch_whitelist.append(target)
                    watch_whitelist_set.add(target)
                if parent not in schedule_target_set:
                    schedule_targets.append(parent)
                    schedule_target_set.add(parent)
            elif target.is_dir():
                if target not in watch_whitelist_set:
                    watch_whitelist.append(target)
                    watch_whitelist_set.add(target)
                if target not in schedule_target_set:
                    schedule_targets.append(target)
                    schedule_target_set.add(target)

    if not schedule_targets:
        schedule_targets = [root]
        schedule_target_set = {root}
        if not watch_whitelist:
            watch_whitelist = [root]
            watch_whitelist_set = {root}

    ignore_patterns_set: Set[str] = set()
    for item in excludes:
        if not item:
            continue
        # Ignora el directorio y su contenido
        ignore_patterns_set.add(f"*/{item}")
        ignore_patterns_set.add(f"*/{item}/*")
    if ignore_patterns:
        ignore_patterns_set.update(str(p) for p in ignore_patterns)
    ignore_patterns_set.update(NOISY_FILE_PATTERNS)
    
    handler = _DebouncedReloader(
        root,
        debounce_seconds=debounce_seconds,
        exclude=excludes,
        verbose=verbose,
        watch_whitelist=watch_whitelist,
        ignore_patterns=ignore_patterns_set,
    )

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
        scheduled: Set[str] = set()
        for target in schedule_targets:
            target_path = target.resolve()
            key = str(target_path)
            if key in scheduled:
                continue
            scheduled.add(key)
            _schedule_observer(
                observer_instance,
                handler,
                target_path,
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


