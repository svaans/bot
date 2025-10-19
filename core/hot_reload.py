# core/hot_reload.py
from __future__ import annotations

from __future__ import annotations

import ast
import errno
import importlib
import json
import logging
import os
import sys
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from types import ModuleType
from typing import Callable, Iterable, Optional, Sequence, Set

from core.state import persist_critical_state
from observability.metrics import (
    HOT_RELOAD_BACKEND,
    HOT_RELOAD_DEBOUNCE_SECONDS,
    HOT_RELOAD_ERRORS_TOTAL,
    HOT_RELOAD_RESTARTS_TOTAL,
    HOT_RELOAD_SCAN_DURATION_SECONDS,
)

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


@dataclass(slots=True, frozen=True)
class ModularReloadRule:
    """Describe un paquete o módulo elegible para recarga modular."""

    module: str
    recursive: bool = True
    aliases: Sequence[str] = ()


class ModularReloadController:
    """Aplica recargas modulares seguras en módulos puros."""

    def __init__(
        self,
        *,
        root: Path,
        rules: Sequence[ModularReloadRule],
        logger: logging.Logger | None = None,
    ) -> None:
        self._root = root.resolve()
        self._rules = list(rules)
        self._logger = logger or logging.getLogger("hot_reload.modular")
        self._lock = threading.Lock()

    def handle_change(
        self,
        path: Path,
        *,
        restart_callback: Callable[[], None],
    ) -> bool:
        resolved = self._safe_resolve(path)
        if resolved is None:
            return False

        for rule in self._rules:
            if not self._matches_rule(rule, resolved):
                continue

            modules = self._collect_modules(rule)
            if not modules:
                return False

            if not self._validate_modules(modules, rule):
                return False

            self._spawn_reload_thread(modules, restart_callback)
            return True

        return False

    # ---- helpers ----

    def _safe_resolve(self, path: Path) -> Path | None:
        try:
            return path.resolve()
        except FileNotFoundError:
            return path.absolute()

    def _matches_rule(self, rule: ModularReloadRule, path: Path) -> bool:
        try:
            module = importlib.import_module(rule.module)
        except Exception:
            return False

        module_path = self._module_path(module)
        if module_path is None:
            return False

        if path == module_path:
            return True

        if not rule.recursive:
            return False

        return self._is_relative_to(path, module_path.parent)

    def _collect_modules(self, rule: ModularReloadRule) -> list[str]:
        prefixes = {rule.module, *rule.aliases}
        modules: set[str] = set()
        with self._lock:
            for name, module in list(sys.modules.items()):
                if module is None:
                    continue
                if any(name == prefix or name.startswith(f"{prefix}.") for prefix in prefixes):
                    modules.add(name)
            if rule.module not in sys.modules:
                try:
                    importlib.import_module(rule.module)
                except Exception:
                    return []
                modules.add(rule.module)
        return sorted(modules)

    def _validate_modules(self, modules: Sequence[str], rule: ModularReloadRule) -> bool:
        allowed_prefixes = {rule.module, *rule.aliases}
        for module_name in modules:
            module = sys.modules.get(module_name)
            if module is None:
                continue
            module_path = self._module_path(module)
            if module_path is None or not module_path.exists():
                continue
            try:
                source = module_path.read_text(encoding="utf-8")
            except Exception:
                return False
            for imported in self._iter_imports(source, module_name):
                if imported is None:
                    continue
                if any(
                    imported == prefix or imported.startswith(f"{prefix}.")
                    for prefix in allowed_prefixes
                ):
                    continue
                if not self._is_local_module(imported):
                    continue
                self._logger.debug(
                    "Recarga modular denegada",
                    extra={"module_name": module_name, "dependency": imported},
                )
                return False
        return True

    def _spawn_reload_thread(
        self,
        modules: Sequence[str],
        restart_callback: Callable[[], None],
    ) -> None:
        def _runner() -> None:
            try:
                self._logger.info(
                    "Iniciando recarga modular",
                    extra={"modules": list(modules)},
                )
                for name in modules:
                    module = sys.modules.get(name)
                    if module is None:
                        module = importlib.import_module(name)
                    importlib.reload(module)
                self._logger.info(
                    "Recarga modular completada",
                    extra={"modules": list(modules)},
                )
            except Exception:
                self._logger.exception(
                    "Recarga modular fallida",
                    extra={"modules": list(modules)},
                )
                restart_callback()

        threading.Thread(
            target=_runner,
            name="modular-reload",
            daemon=True,
        ).start()

    def _module_path(self, module: ModuleType) -> Path | None:
        filename = getattr(module, "__file__", None)
        if not filename:
            return None
        path = Path(filename)
        try:
            path = path.resolve()
        except FileNotFoundError:
            path = path.absolute()
        if path.suffix == ".pyc":
            path = path.with_suffix(".py")
        return path

    def _iter_imports(self, source: str, module_name: str) -> Set[str | None]:
        try:
            tree = ast.parse(source)
        except SyntaxError:
            return set()

        imports: Set[tuple[str | None, int]] = set()

        class _Visitor(ast.NodeVisitor):
            def visit_Import(self, node: ast.Import) -> None:  # type: ignore[override]
                for alias in node.names:
                    imports.add((alias.name, 0))

            def visit_ImportFrom(self, node: ast.ImportFrom) -> None:  # type: ignore[override]
                imports.add((node.module, node.level))

        _Visitor().visit(tree)

        resolved: Set[str | None] = set()
        for name, level in imports:
            resolved.add(self._resolve_import(module_name, name, level))
        return resolved

    def _resolve_import(
        self,
        current_module: str,
        name: str | None,
        level: int,
    ) -> str | None:
        if level == 0:
            return name

        package = current_module.rsplit(".", 1)[0] if "." in current_module else ""
        parts = package.split(".") if package else []
        if level - 1 > len(parts):
            return None
        if level > 0:
            parts = parts[: len(parts) - (level - 1)]
        if name:
            parts.extend(name.split("."))
        joined = ".".join(filter(None, parts))
        return joined or None

    def _is_local_module(self, module_name: str) -> bool:
        parts = module_name.split(".")
        candidate = self._root.joinpath(*parts)
        if candidate.with_suffix(".py").exists():
            return True
        if candidate.is_dir() and (candidate / "__init__.py").exists():
            return True
        return False

    def _is_relative_to(self, path: Path, base: Path) -> bool:
        try:
            path.relative_to(base)
            return True
        except ValueError:
            return False


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
        reload_handler: Callable[[Path, Callable[[], None]], bool] | None = None,
        extra_extensions: Optional[Iterable[str]] = None,
        restart_cooldown_seconds: float | None = None,
    ) -> None:
        extensions = {".py"}
        if extra_extensions:
            for ext in extra_extensions:
                if not ext:
                    continue
                normalized = ext if ext.startswith(".") else f".{ext}"
                extensions.add(normalized.lower())

        patterns = tuple(f"*{ext}" for ext in sorted(extensions)) or ("*.py",)
        super().__init__(
            patterns=patterns,
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
        self._reload_handler = reload_handler
        self._extensions: Set[str] = set(extensions)

        cooldown = 0.0
        if restart_cooldown_seconds is not None:
            try:
                cooldown = float(restart_cooldown_seconds)
            except (TypeError, ValueError):
                cooldown = 0.0
        self._restart_cooldown = max(0.0, cooldown)

        self._timer: Optional[threading.Timer] = None
        self._lock = threading.Lock()
        self._last_event_ts: float = 0.0
        self._last_path: Optional[Path] = None
        self._last_restart_ts: float = 0.0
        self._logger = logging.getLogger("hot_reload")

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

            suffix = p.suffix.lower()
            if suffix not in self._extensions:
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
            now = time.time()
            self._last_event_ts = now
            self._last_path = selected_path

            if self._timer and self._timer.is_alive():
                # Reinicia la cuenta regresiva
                self._timer.cancel()
                self._timer = None

            self._timer = threading.Timer(self.debounce, self._maybe_restart)
            self._timer.daemon = True
            self._timer.start()
            HOT_RELOAD_DEBOUNCE_SECONDS.labels(kind="scheduled").inc(self.debounce)

            if self.verbose:
                rel = (
                    selected_path.relative_to(self.root)
                    if str(selected_path).startswith(str(self.root))
                    else selected_path
                )
                payload = {
                    "event": "hot_reload_change_detected",
                    "path": str(rel),
                    "debounce": round(self.debounce, 3),
                    "restart_scheduled_in": round(self.debounce, 3),
                    "ts": now,
                }
                self._logger.info(json.dumps(payload))

    # ---- Internals ----

    def _maybe_restart(self) -> None:
        with self._lock:
            # Si hubo otra modificación durante la espera, reprograma
            now = time.time()
            delta = now - self._last_event_ts
            if delta < self.debounce * 0.5:
                # ruido; reintentar
                self._timer = threading.Timer(self.debounce, self._maybe_restart)
                self._timer.daemon = True
                self._timer.start()
                HOT_RELOAD_DEBOUNCE_SECONDS.labels(kind="rescheduled").inc(self.debounce)
                return
            if self._restart_cooldown > 0:
                elapsed_since_restart = now - self._last_restart_ts
                if elapsed_since_restart < self._restart_cooldown:
                    wait = self._restart_cooldown - elapsed_since_restart
                    self._timer = threading.Timer(wait, self._maybe_restart)
                    self._timer.daemon = True
                    self._timer.start()
                    HOT_RELOAD_DEBOUNCE_SECONDS.labels(kind="cooldown_wait").inc(wait)
                    return
            trig = self._last_path

        if self.verbose and trig is not None:
            try:
                rel = trig.relative_to(self.root)
            except Exception:
                rel = trig
            payload = {
                "event": "hot_reload_restart_triggered",
                "path": str(rel),
                "restart_reason": "file_change",
                "debounce": round(self.debounce, 3),
                "duration_since_change": round(delta, 3),
                "ts": now,
            }
            self._logger.info(json.dumps(payload))

        HOT_RELOAD_DEBOUNCE_SECONDS.labels(kind="elapsed").inc(delta)

        if trig is not None and self._reload_handler is not None:
            try:
                handled = self._reload_handler(trig, self._perform_restart)
            except Exception:
                HOT_RELOAD_ERRORS_TOTAL.labels(stage="modular_handler").inc()
                logging.getLogger("hot_reload").exception(
                    "Falló el manejador de recarga modular",
                    extra={"path": str(trig)},
                )
            else:
                if handled:
                    return
        
        self._last_restart_ts = time.time()
        HOT_RELOAD_RESTARTS_TOTAL.labels(reason="file_change").inc()
        self._perform_restart()

    def _perform_restart(self) -> None:

        # Reinicio de proceso: reemplaza el binario actual y preserva argv/env
        python = sys.executable
        argv = [python] + sys.argv
        try:
            persist_critical_state(reason="hot_reload")
        except Exception:
            HOT_RELOAD_ERRORS_TOTAL.labels(stage="persist_state").inc()
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
            HOT_RELOAD_ERRORS_TOTAL.labels(stage="execv").inc()
            import subprocess

            subprocess.Popen(argv, env=os.environ.copy(), close_fds=False)
            os._exit(0)

def _schedule_observer(
    observer,
    handler: FileSystemEventHandler,
    path: Path,
    *,
    ignore_patterns: Iterable[str] | None = None,
    mode: str,
) -> float:
    """Helper to schedule the observer with optional ignore patterns.

    Returns the elapsed seconds spent scheduling so callers can log/aggregate.
    """

    logger = logging.getLogger("hot_reload")

    extra_kwargs = {}
    filters_applied = False
    if ignore_patterns:
        patterns = sorted(set(ignore_patterns))
        if patterns:
            filters_applied = True
            extra_kwargs = {
                "ignore_patterns": patterns,
                "ignore_directories": True,
            }

    duration = 0.0

    def _timed_schedule(**kwargs) -> float:
        start = time.perf_counter()
        observer.schedule(handler, str(path), recursive=True, **kwargs)
        return time.perf_counter() - start

    if extra_kwargs:
        try:
            duration = _timed_schedule(**extra_kwargs)
        except TypeError:
            # Backend no soporta ignore_patterns; reintenta sin filtros.
            filters_applied = False
            duration = _timed_schedule()
    else:
        duration = _timed_schedule()

    HOT_RELOAD_SCAN_DURATION_SECONDS.labels(mode=mode).observe(duration)

    logger.info(
        json.dumps(
            {
                "event": "hot_reload_scan_scheduled",
                "mode": mode,
                "path": str(path),
                "duration_seconds": round(duration, 6),
                "filters_applied": filters_applied,
            }
        )
    )

    return duration


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
    modular_reload: Optional[Sequence[ModularReloadRule]] = None,
    extra_extensions: Optional[Iterable[str]] = None,
    restart_cooldown_seconds: float | None = None,
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
        extra_extensions : Iterable[str] | None
        Extensiones adicionales (incluyendo el punto) a monitorear además de ``.py``.
    restart_cooldown_seconds : float | None
        Ventana mínima entre reinicios consecutivos del proceso tras aplicar cambios.
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

    modular_controller: ModularReloadController | None = None
    if modular_reload:
        modular_controller = ModularReloadController(
            root=root,
            rules=tuple(modular_reload),
        )

    def _reload_handler(path: Path, restart_cb: Callable[[], None]) -> bool:
        if modular_controller is None:
            return False
        return modular_controller.handle_change(path, restart_callback=restart_cb)
    
    handler = _DebouncedReloader(
        root,
        debounce_seconds=debounce_seconds,
        exclude=excludes,
        verbose=verbose,
        watch_whitelist=watch_whitelist,
        ignore_patterns=ignore_patterns_set,
        reload_handler=_reload_handler if modular_controller else None,
        extra_extensions=extra_extensions,
        restart_cooldown_seconds=restart_cooldown_seconds,
    )

    # Heurística para elegir backend
    force_poll = polling if polling is not None else (os.getenv("WATCHDOG_POLLING", "0") == "1")
    observer_cls = PollingObserver if force_poll else Observer

    def _instantiate(cls):
        try:
            return cls(timeout=1.0)  # type: ignore[call-arg]
        except TypeError:
            return cls()  # type: ignore[call-arg]
        
    logger = logging.getLogger("hot_reload")

    def _start(cls):
        observer_instance = _instantiate(cls)
        scheduled: Set[str] = set()
        total_duration = 0.0
        mode_label = "polling" if cls is PollingObserver else "native"
        for target in schedule_targets:
            target_path = target.resolve()
            key = str(target_path)
            if key in scheduled:
                continue
            scheduled.add(key)
            total_duration += _schedule_observer(
                observer_instance,
                handler,
                target_path,
                ignore_patterns=ignore_patterns_set,
                mode=mode_label,
            )

        status = "started"
        try:
            observer_instance.start()
        except Exception:
            status = "error"
            raise
        finally:
            logger.info(
                json.dumps(
                    {
                        "event": "hot_reload_observer_prepared",
                        "mode": mode_label,
                        "scheduled_targets": len(scheduled),
                        "scan_duration_seconds": round(total_duration, 6),
                        "status": status,
                    }
                )
            )
        return observer_instance

    if observer_cls is PollingObserver:
        observer = _start(observer_cls)
    else:
        try:
            observer = _start(observer_cls)
        except OSError as exc:
            if getattr(exc, "errno", None) == errno.ENOSPC:
                if verbose:
                    logging.getLogger("hot_reload").info(
                        json.dumps(
                            {
                                "event": "hot_reload_backend_switch",
                                "mode": "polling",
                                "restart_reason": "inotify_limit",
                                "path": str(root),
                                "debounce": round(debounce_seconds, 3),
                            }
                        )
                    )
                observer_cls = PollingObserver
                observer = _start(observer_cls)
            else:
                HOT_RELOAD_ERRORS_TOTAL.labels(stage="backend_start").inc()
                raise


    mode = "polling" if observer_cls is PollingObserver else "native"
    HOT_RELOAD_BACKEND.labels(mode=mode).inc()

    logger.info(
        json.dumps(
            {
                "event": "hot_reload_backend_selected",
                "mode": mode,
                "observer_class": f"{observer_cls.__module__}.{observer_cls.__name__}",
                "targets": len(schedule_targets),
                "polling_forced": force_poll,
            }
        )
    )


    if verbose:
        logger.info(
            json.dumps(
                {
                    "event": "hot_reload_started",
                    "mode": mode,
                    "path": str(root),
                    "debounce": round(debounce_seconds, 3),
                    "excludes": sorted(excludes),
                }
            )
        )

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


