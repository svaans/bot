import os
import importlib.util
from core.utils.utils import configurar_logger

log = configurar_logger("loader_entradas")

EXCLUIR_PREFIX = (
    "__", "gestor", "loader", "analisis", "validadores",
    "validaciones", "validador", "verificar"
)

def _es_archivo_estrategia(nombre: str) -> bool:
    return nombre.endswith(".py") and not nombre.startswith(EXCLUIR_PREFIX)

def cargar_estrategias():
    """
    Carga todas las funciones definidas en archivos .py dentro de `strategies/entry`,
    cuyo nombre de función coincide con el nombre del archivo.
    Retorna: dict {nombre_estrategia: funcion}
    """
    estrategias = {}
    ruta_base = os.path.dirname(__file__)

    for archivo in os.listdir(ruta_base):
        if not _es_archivo_estrategia(archivo):
            continue

        nombre_modulo = archivo[:-3]
        ruta_completa = os.path.join(ruta_base, archivo)
        full_name = f"core.strategies.entry.{nombre_modulo}"

        try:
            spec = importlib.util.spec_from_file_location(full_name, ruta_completa)
            if spec is None or spec.loader is None:
                log.error(f"❌ Spec inválida para {nombre_modulo} ({ruta_completa})")
                continue

            module_obj = importlib.util.module_from_spec(spec)
            # Ejecuta el código del módulo dentro de su propio namespace
            try:
                spec.loader.exec_module(module_obj)
            except Exception as e:
                log.error(f"❌ Error ejecutando {nombre_modulo}: {e}")
                continue

            func = getattr(module_obj, nombre_modulo, None)
            if callable(func):
                estrategias[nombre_modulo] = func
            else:
                log.warning(f"⚠️ {nombre_modulo} no expone función `{nombre_modulo}()`")

        except Exception as e:
            # ¡Ojo! No referenciar `module_obj` aquí si pudo no crearse
            log.error(f"❌ Error importando {nombre_modulo}: {e}")

    log.info(f"🧩 Estrategias cargadas: {len(estrategias)}")
    if not estrategias:
        log.warning("⚠️ No se cargó ninguna estrategia. Revisa exclusiones y errores arriba.")

    return estrategias
