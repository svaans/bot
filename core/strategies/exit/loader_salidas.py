import importlib
import inspect
import os
from core.utils import configurar_logger
from core.utils.log_utils import format_exception_for_log

log = configurar_logger('loader_salidas')
CARPETA_SALIDAS = os.path.dirname(__file__)
_PACKAGE = "core.strategies.exit"

# Funciones que deben excluirse aunque su nombre comience por 'verificar_'.
# Estas tienen firmas distintas (no son estrategias de salida directas).
_EXCLUDED_FUNCTIONS: frozenset[str] = frozenset({
    'verificar_trailing_stop',
    'verificar_reversion_tendencia',
    'evaluar_evitar_stoploss',
})


def es_estrategia_salida_valida(funcion) -> bool:
    """True si la función acepta ≤ 3 parámetros (interfaz de salida canónica)."""
    try:
        firma = inspect.signature(funcion)
        return len(firma.parameters) <= 3
    except (ValueError, TypeError) as e:
        log.warning(
            'Función de salida inválida %s: %s',
            getattr(funcion, '__name__', funcion),
            format_exception_for_log(e),
        )
        return False


def cargar_estrategias_salida() -> list:
    """Carga todas las estrategias de salida del paquete.

    Usa ``importlib.import_module`` (no ``spec_from_file_location``) para que:
    - Los módulos se registren en ``sys.modules`` y se reutilicen en llamadas sucesivas.
    - Los imports relativos dentro de cada estrategia funcionen correctamente.
    - El orden de carga sea determinista (``sorted``).
    """
    funciones: list = []
    for archivo in sorted(os.listdir(CARPETA_SALIDAS)):
        if not (archivo.startswith('salida_') and archivo.endswith('.py')):
            continue
        nombre_modulo = archivo[:-3]
        full_name = f"{_PACKAGE}.{nombre_modulo}"
        try:
            modulo = importlib.import_module(full_name)
        except Exception as e:
            log.error(
                '❌ Error importando %s: %s',
                nombre_modulo,
                format_exception_for_log(e),
            )
            raise
        for attr in dir(modulo):
            es_salida = attr.startswith('salida_')
            es_verificar = attr.startswith('verificar_') and attr not in _EXCLUDED_FUNCTIONS
            if not (es_salida or es_verificar):
                continue
            funcion = getattr(modulo, attr)
            if callable(funcion) and es_estrategia_salida_valida(funcion):
                funciones.append(funcion)
            else:
                log.warning(
                    '⚠️ %s.%s ignorada: no es función de salida válida.',
                    nombre_modulo,
                    attr,
                )
    return funciones
