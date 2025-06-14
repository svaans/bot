import os
import importlib.util

def cargar_estrategias():
    """
    Carga todas las funciones definidas en archivos .py dentro de estrategias_entrada,
    cuyo nombre de función coincide con el nombre del archivo.
    Retorna: dict {nombre_estrategia: funcion}
    """
    estrategias = {}
    ruta_base = os.path.dirname(__file__)

    for archivo in os.listdir(ruta_base):
        if archivo.endswith(".py") and not archivo.startswith(("__", "gestor", "loader", "analisis")):
            nombre_modulo = archivo[:-3]  # quitar ".py"
            ruta_completa = os.path.join(ruta_base, archivo)

            try:
                spec = importlib.util.spec_from_file_location(nombre_modulo, ruta_completa)
                modulo = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(modulo)

                funcion = getattr(modulo, nombre_modulo, None)
                if callable(funcion):
                    estrategias[nombre_modulo] = funcion
                else:
                    print(f"⚠️ {nombre_modulo} no contiene función con su mismo nombre.")
            except Exception as e:
                print(f"❌ Error importando {nombre_modulo}: {e}")

    return estrategias

