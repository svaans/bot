import os
import sys
import importlib.util
from collections.abc import Callable

from core.utils import configurar_logger


log = configurar_logger('loader_entradas')


def cargar_estrategias() -> dict[str, Callable]:
    """Carga estrategias de entrada.

    Escanea archivos ``.py`` en ``strategies/entry`` cuyo nombre de función
    coincide con el nombre del archivo y devuelve un diccionario
    ``{nombre_estrategia: funcion}``.
    """
    
    estrategias: dict[str, Callable] = {}
    ruta_base = os.path.dirname(__file__)
    for archivo in os.listdir(ruta_base):
        if archivo.endswith('.py') and not archivo.startswith(('_',
            'gestor', 'loader', 'analisis', 'validadores', 'validaciones',
            'validador', 'verificar')):
            nombre_modulo = archivo[:-3]
            ruta_completa = os.path.join(ruta_base, archivo)
            try:
                full_name = f'core.strategies.entry.{nombre_modulo}'
                spec = importlib.util.spec_from_file_location(full_name,
                    ruta_completa)
                if spec is None or spec.loader is None:
                    log.error(f'Spec inválido para {ruta_completa}')
                    continue
                sys.modules[full_name] = modulo
                modulo = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(modulo)
                funcion = getattr(modulo, nombre_modulo, None)
                if callable(funcion):
                    estrategias[nombre_modulo] = funcion
                else:
                    log.warning(
                        f'⚠️ {nombre_modulo} no contiene función con su mismo nombre.'
                    )
            except Exception as e:
                log.error(f'❌ Error importando {nombre_modulo}: {e}')
    return estrategias
