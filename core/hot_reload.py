import importlib
import sys
from pathlib import Path
from typing import Iterable
import threading
from core.logger import configurar_logger

from watchdog.observers import Observer
from watchdog.events import PatternMatchingEventHandler

log = configurar_logger("hot_reload")

DEFAULT_MODULES: list[str] = [
    "core",
    "estrategias_entrada",
    "estrategias_salida",
    "filtros",
    "indicadores",
    "config",

]

class _ReloadHandler(PatternMatchingEventHandler):
    def __init__(self, base: Path, modules: Iterable[str], exclude: Iterable[str] | None = None):
        super().__init__(patterns=["*.py"], ignore_directories=True)
        self.base = base.resolve()
        self.modules = list(modules)
        self.exclude = list(exclude) if exclude else []

    def _should_reload(self, module_name: str) -> bool:
        if any(module_name.startswith(e) for e in self.exclude):
            return False
        if not self.modules:
            return True
        return any(module_name == m or module_name.startswith(m + ".") for m in self.modules)

    def _module_from_path(self, path: Path) -> str | None:
        try:
            rel = path.resolve().relative_to(self.base)
        except ValueError:
            return None
        module_name = rel.with_suffix("").as_posix().replace("/", ".")
        return module_name

    def on_modified(self, event):
        self._reload_path(Path(event.src_path))

    def on_created(self, event):
        self._reload_path(Path(event.src_path))

    def _reload_path(self, path: Path) -> None:
        if path.suffix != ".py":
            return
        module_name = self._module_from_path(path)
        if not module_name or not self._should_reload(module_name):
            log.debug(f"🔍 Ignorando recarga para {path.name} ({module_name})")
            return
        try:
            module = sys.modules.get(module_name)
            if module:
                importlib.reload(module)
            else:
                module = importlib.import_module(module_name)
            log.warning(f"🔄 Cambio detectado en {path.name}, recargando módulo {module_name}...")
            log.warning(f"✅ Recarga completada con éxito: {module_name}")
        except Exception as exc:
            log.info(f"❌ Error al recargar {module_name}: {exc}")


def start_hot_reload(
    path: str | Path = None,
    modules: Iterable[str] | None = DEFAULT_MODULES,
    exclude: Iterable[str] | None = None,
) -> Observer:
    """Inicia un observador en ``path`` para recargar ``modules`` en caliente.

    Si ``modules`` es ``None`` o una lista vacía, se intentarán recargar todos los
    paquetes dentro de ``path`` salvo los indicados en ``exclude``.
    """
    base = Path(path or Path.cwd())
    mods = list(modules) if modules is not None else []
    texto_mods = ", ".join(mods) if mods else "todos"
    log.info(f"👀 Observando carpeta {base} con módulos: {texto_mods}")
    handler = _ReloadHandler(base, mods, exclude)
    observer = Observer()
    observer.schedule(handler, str(base), recursive=True)
    thread = threading.Thread(target=observer.start, daemon=True)
    thread.start()
    return observer


def stop_hot_reload(observer: Observer) -> None:
    observer.stop()
    observer.join()