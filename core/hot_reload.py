import os
import sys
import time
from pathlib import Path
from typing import Iterable
import threading
from core.utils.utils import configurar_logger
from watchdog.observers import Observer
from watchdog.observers.polling import PollingObserver
from watchdog.events import PatternMatchingEventHandler
import errno
log = configurar_logger('hot_reload')


def _patch_watchdog_for_py313() ->None:
    log.info('➡️ Entrando en _patch_watchdog_for_py313()')
    """Ajusta Watchdog para Python 3.13."""
    if sys.version_info >= (3, 13):
        try:
            from watchdog.utils import BaseThread

            def _start(self) ->None:
                log.info('➡️ Entrando en _start()')
                self.on_thread_start()
                threading.Thread.start(self, handle=None)
            BaseThread.start = _start
        except Exception as exc:
            log.debug(f'No se pudo parchear watchdog: {exc}')


DEFAULT_MODULES: list[str] = ['bot']


def restart_bot() ->None:
    log.info('➡️ Entrando en restart_bot()')
    """Reinicia por completo el proceso actual para aplicar cambios."""
    log.warning('♻️ Reiniciando bot por cambios en el código...')
    python = sys.executable
    os.execl(python, python, *sys.argv)


class _ReloadHandler(PatternMatchingEventHandler):

    def __init__(self, base: Path, modules: Iterable[str], exclude: (
        Iterable[str] | None)=None):
        log.info('➡️ Entrando en __init__()')
        super().__init__(patterns=['*.py'], ignore_directories=True)
        self.base = base.resolve()
        self.modules = list(modules)
        self.exclude = list(exclude) if exclude else []
        self._last_reload: dict[Path, float] = {}

    def _should_reload(self, module_name: str) ->bool:
        log.info('➡️ Entrando en _should_reload()')
        if any(module_name.startswith(e) for e in self.exclude):
            return False
        if not self.modules:
            return True
        return any(module_name == m or module_name.startswith(m + '.') for
            m in self.modules)

    def _module_from_path(self, path: Path) ->(str | None):
        log.info('➡️ Entrando en _module_from_path()')
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

    def on_modified(self, event):
        log.info('➡️ Entrando en on_modified()')
        self._reload_path(Path(event.src_path))

    def on_created(self, event):
        log.info('➡️ Entrando en on_created()')
        self._reload_path(Path(event.src_path))

    def on_moved(self, event):
        log.info('➡️ Entrando en on_moved()')
        dest = getattr(event, 'dest_path', None)
        if dest:
            self._reload_path(Path(dest))
        self._remove_module(Path(event.src_path))

    def on_deleted(self, event):
        log.info('➡️ Entrando en on_deleted()')
        self._remove_module(Path(event.src_path))

    def _remove_module(self, path: Path) ->None:
        log.info('➡️ Entrando en _remove_module()')
        if path.suffix != '.py':
            return
        module_name = self._module_from_path(path)
        if module_name and module_name in sys.modules:
            del sys.modules[module_name]

    def _reload_path(self, path: Path) ->None:
        log.info('➡️ Entrando en _reload_path()')
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

    def _actualizar_referencias_importadas(self, module_name: str, new_module):
        log.info('➡️ Entrando en _actualizar_referencias_importadas()')
        for nombre_mod, mod in sys.modules.items():
            if not mod or not hasattr(mod, '__dict__'):
                continue
            for key, val in list(mod.__dict__.items()):
                if hasattr(val, '__module__'
                    ) and val.__module__ == module_name:
                    nuevo_val = getattr(new_module, key, None)
                    if nuevo_val and nuevo_val is not val:
                        setattr(mod, key, nuevo_val)
                        log.debug(
                            f'🔁 Actualizado {key} en {nombre_mod} desde {module_name}'
                            )


def start_hot_reload(path: (str | Path)=None, modules: (Iterable[str] |
    None)=DEFAULT_MODULES, exclude: (Iterable[str] | None)=None) ->Observer:
    log.info('➡️ Entrando en start_hot_reload()')
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
    observer: Observer = Observer()
    observer.schedule(handler, str(base), recursive=True)
    try:
        observer.start()
    except OSError as exc:
        if exc.errno == errno.ENOSPC:
            log.warning(
                '⚠️ Límite de inotify alcanzado. Usando PollingObserver.')
            observer = PollingObserver()
            observer.schedule(handler, str(base), recursive=True)
            observer.start()
        else:
            raise
    return observer


def stop_hot_reload(observer: Observer) ->None:
    log.info('➡️ Entrando en stop_hot_reload()')
    if observer and observer.is_alive():
        observer.stop()
        observer.join()
    else:
        log.warning('⚠️ El observador no está activo o ya fue detenido.')
