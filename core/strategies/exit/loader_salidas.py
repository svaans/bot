import importlib
import inspect
import os
from typing import Callable
from core.utils import configurar_logger
from core.utils.log_utils import format_exception_for_log

log = configurar_logger('loader_salidas')
CARPETA_SALIDAS = os.path.dirname(__file__)
_PACKAGE = "core.strategies.exit"

# Funciones que deben excluirse del loop genérico de evaluar_salidas.
_EXCLUDED_FUNCTIONS: frozenset[str] = frozenset({
    # 'verificar_*' tienen firmas distintas (no son estrategias de salida directas).
    'verificar_trailing_stop',
    'verificar_reversion_tendencia',
    'evaluar_evitar_stoploss',
    # salida_trailing_stop es dead code en el loop genérico: no puede activarse
    # porque resetea max_precio = precio_actual en cada llamada (estado perdido
    # por operar sobre Order.to_dict()), haciendo imposible sl_nuevo < precio_actual.
    # El trailing stop real es verificar_trailing_stop, llamado directamente desde
    # _manejar_trailing_stop en verificar_salidas.py con estado persistido en Order.
    'salida_trailing_stop',
})

# Cache del resultado de cargar_estrategias_salida() y de las firmas.
# Las estrategias de salida no cambian en runtime; cargar y analizar firmas
# una sola vez evita os.listdir + inspect.signature por vela con orden abierta.
_ESTRATEGIAS_CACHE: list[Callable] | None = None
_PARAMS_CACHE: dict[int, frozenset[str]] = {}  # id(f) → params


def get_params(f: Callable) -> frozenset[str]:
    """Devuelve los nombres de parámetros de ``f`` desde cache (evita signature per-call)."""
    key = id(f)
    cached = _PARAMS_CACHE.get(key)
    if cached is not None:
        return cached
    try:
        result = frozenset(inspect.signature(f).parameters.keys())
    except (ValueError, TypeError):
        result = frozenset()
    _PARAMS_CACHE[key] = result
    return result


def _reset_cache_for_tests() -> None:  # pragma: no cover — solo tests
    """Invalida el cache de estrategias y firmas (útil tras monkeypatching en tests)."""
    global _ESTRATEGIAS_CACHE
    _ESTRATEGIAS_CACHE = None
    _PARAMS_CACHE.clear()


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


def cargar_estrategias_salida() -> list[Callable]:
    """Carga todas las estrategias de salida del paquete (resultado cacheado).

    Usa ``importlib.import_module`` (no ``spec_from_file_location``) para que:
    - Los módulos se registren en ``sys.modules`` y se reutilicen en llamadas sucesivas.
    - Los imports relativos dentro de cada estrategia funcionen correctamente.
    - El orden de carga sea determinista (``sorted``).

    El resultado se cachea en ``_ESTRATEGIAS_CACHE`` para evitar ``os.listdir``
    e ``inspect.signature`` en cada vela con orden abierta.
    """
    global _ESTRATEGIAS_CACHE
    if _ESTRATEGIAS_CACHE is not None:
        return _ESTRATEGIAS_CACHE

    funciones: list[Callable] = []
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
                # Pre-cachear la firma para que gestor_salidas no llame a
                # inspect.signature en cada evaluación.
                get_params(funcion)
            else:
                log.warning(
                    '⚠️ %s.%s ignorada: no es función de salida válida.',
                    nombre_modulo,
                    attr,
                )

    _ESTRATEGIAS_CACHE = funciones
    return _ESTRATEGIAS_CACHE
