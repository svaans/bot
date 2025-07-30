import os
import importlib.util
import inspect
from core.utils import configurar_logger

log = configurar_logger('loader_salidas')
CARPETA_SALIDAS = os.path.dirname(__file__)


def es_estrategia_salida_valida(funcion):
    try:
        firma = inspect.signature(funcion)
        return len(firma.parameters) <= 3
    except (ValueError, TypeError) as e:
        log.warning(
            f'Función de salida inválida {getattr(funcion, "__name__", funcion)}: {e}'
        )
        return False


def cargar_estrategias_salida():
    funciones = []
    for archivo in os.listdir(CARPETA_SALIDAS):
        if archivo.startswith('salida_') and archivo.endswith('.py'):
            ruta = os.path.join(CARPETA_SALIDAS, archivo)
            nombre_modulo = archivo[:-3]
            try:
                spec = importlib.util.spec_from_file_location(nombre_modulo,
                    ruta)
                modulo = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(modulo)
                for attr in dir(modulo):
                    if attr.startswith('salida_') or attr.startswith(
                        'verificar_') and attr not in [
                        'verificar_trailing_stop',
                        'verificar_reversion_tendencia',
                        'evaluar_evitar_stoploss']:
                        funcion = getattr(modulo, attr)
                        if callable(funcion) and es_estrategia_salida_valida(
                            funcion):
                            funciones.append(funcion)
                        else:
                            log.warning(
                                f'⚠️ {nombre_modulo}.{attr} ignorada: no es función de salida válida.'
                            )
            except Exception as e:
                log.error(
                    f'❌ Error importando {nombre_modulo} desde {ruta}: {e}'
                )
                raise
    return funciones
