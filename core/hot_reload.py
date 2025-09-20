from __future__ import annotations
import os
import sys
import time
from pathlib import Path
from typing import Iterable, Optional
import threading
import errno

from core.utils.utils import configurar_logger
from watchdog.observers import Observer  # runtime class
from watchdog.observers.polling import PollingObserver
from watchdog.events import PatternMatchingEventHandler
from watchdog.observers.api import BaseObserver  # <- use this for typing

log = configurar_logger('hot_reload')


def _patch_watchdog_for_py313() -> None:
    """Ajusta Watchdog para Python 3.13 (firma de Thread.start cambia)."""
    if sys.version_info >= (3, 13):
        try:
            from watchdog.utils import BaseThread  # type: ignore

            def _start(self) -> None:  # type: ignore
                self.on_thread_start()
                # La firma de Thread.start en 3.13 acepta "*" y kwargs; evitamos kwargs
                threading.Thread.start(self)

            BaseThread.start = _start  # type: ignore[attr-defined]
        except Exception as exc:  # pragma: no cover
            log.debug(f'No se pudo parchear watchdog: {exc}')


DEFAULT_MODULES: list[str] = ['bot']


def restart_bot() -> None:
    """Reinicia por completo el proceso actual para aplicar cambios."""
    log.warning('♻️ Reiniciando bot por cambios en el código...')
    python = sys.executable
    os.execl(python, python, *sys.argv)


class _ReloadHandler(PatternMatchingEventHandler):
    def __init__(self, base: Path, modules: Iterable[str], exclude: Optional[Iterable[str]] = None):
        super().__init__(patterns=['*.py'], ignore_directories=True)
        self.base = base.resolve()
        self.modules = list(modules)
        self.exclude = list(exclude) if exclude else []
        self._last_reload: dict[Path, float] = {}

    def _should_reload(self, module_name: str) -> bool:
        if any(module_name.startswith(e) for e in self.exclude):
            return False
        if not self.modules:
            return True
        return any(module_name == m or module_name.startswith(m + '.') for m in self.modules)

    def _module_from_path(self, path: Path) -> str | None:
        try:
            path = path.resolve()
            if not path.is_file():
                return None
            relative = path.relative_to(self.base)
            parts = list(relative.with_suffix('').parts)
            if not parts:
                return None
            return '.'.join(parts)
        except Exception as e:
            log.debug(f'⚠️ No se pudo obtener el módulo desde {path}: {e}')
            return None

    def on_modified(self, event):  # type: ignore[override]
        self._reload_path(Path(event.src_path))

    def on_created(self, event):  # type: ignore[override]
        self._reload_path(Path(event.src_path))

    def on_moved(self, event):  # type: ignore[override]
        dest = getattr(event, 'dest_path', None)
        if dest:
            self._reload_path(Path(dest))
        self._remove_module(Path(event.src_path))

    def on_deleted(self, event):  # type: ignore[override]
        self._remove_module(Path(event.src_path))

    def _remove_module(self, path: Path) -> None:
        if path.suffix != '.py':
            return
        module_name = self._module_from_path(path)
        if module_name and module_name in sys.modules:
            del sys.modules[module_name]

    def _reload_path(self, path: Path) -> None:
        if path.suffix != '.py':
            return
        module_name = self._module_from_path(path)
        if not module_name or not self._should_reload(module_name):
            log.debug(f'🔍 Ignorando recarga para {path.name} ({module_name})')
            return
        try:
            mtime = path.stat().st_mtime
            last = self._last_reload.get(path)
            if last and mtime - last < 1:
                time.sleep(1 - (mtime - last))
            self._last_reload[path] = time.time()
            restart_bot()
        except Exception as exc:
            log.info(f'❌ Error al intentar reiniciar por {module_name}: {exc}')

    def _actualizar_referencias_importadas(self, module_name: str, new_module) -> None:
        for nombre_mod, mod in sys.modules.items():
            if not mod or not hasattr(mod, '__dict__'):
                continue
            for key, val in list(mod.__dict__.items()):
                if hasattr(val, '__module__') and val.__module__ == module_name:
                    nuevo_val = getattr(new_module, key, None)
                    if nuevo_val and nuevo_val is not val:
                        setattr(mod, key, nuevo_val)
                        log.debug(f'🔁 Actualizado {key} en {nombre_mod} desde {module_name}')


def start_hot_reload(
    path: Optional[str | Path] = None,
    modules: Optional[Iterable[str]] = DEFAULT_MODULES,
    exclude: Optional[Iterable[str]] = None,
) -> BaseObserver:  # <- tipado estable
    """Inicia un observador en ``path``.

    Cuando se detecta un cambio en un archivo ``.py`` se reinicia el proceso para
    cargar el código actualizado. Si ``modules`` es ``None`` o una lista vacía,
    se vigilarán todos los paquetes dentro de ``path`` salvo los indicados en
    ``exclude``.
    """
    _patch_watchdog_for_py313()
    base = Path(path or Path.cwd())
    mods = list(modules) if modules is not None else []
    texto_mods = ', '.join(mods) if mods else 'todos'
    log.info(f'👀 Observando carpeta {base} con módulos: {texto_mods}')
    handler = _ReloadHandler(base, mods, exclude)

    # Creamos el observer real (runtime) pero lo anotamos como BaseObserver
    obs: BaseObserver
    try:
        obs = Observer()  # type: ignore[assignment]
        obs.schedule(handler, str(base), recursive=True)
        obs.start()
        return obs
    except OSError as exc:
        if exc.errno == errno.ENOSPC:
            log.warning('⚠️ Límite de inotify alcanzado. Usando PollingObserver.')
            obs = PollingObserver()  # type: ignore[assignment]
            obs.schedule(handler, str(base), recursive=True)
            obs.start()
            return obs
        raise


def stop_hot_reload(observer: BaseObserver) -> None:
    if observer and getattr(observer, 'is_alive', lambda: False)():
        observer.stop()
        observer.join()
    else:
        log.warning('⚠️ El observador no está activo o ya fue detenido.')

