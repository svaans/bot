import os
import json

RUTA_CONFIG_SIMBOLOS = "config/configuraciones_optimas.json"

def cargar_configuracion_simbolo(symbol):
    if not os.path.exists(RUTA_CONFIG_SIMBOLOS):
        raise FileNotFoundError(f"❌ Archivo de configuración no encontrado: {RUTA_CONFIG_SIMBOLOS}")

    try:
        with open(RUTA_CONFIG_SIMBOLOS, "r") as f:
            configuraciones = json.load(f)

        if not isinstance(configuraciones, dict):
            raise ValueError("❌ El archivo debe contener un diccionario de configuraciones.")

        config = configuraciones.get(symbol)
        if config is None:
            print(f"⚠️ No se encontró configuración para {symbol}. Se usará config vacía.")
            return {}

        # Validación opcional de claves mínimas
        claves_esperadas = ["factor_umbral", "peso_minimo_total", "diversidad_minima"]
        for clave in claves_esperadas:
            if clave not in config:
                print(f"⚠️ Falta clave '{clave}' en configuración de {symbol}.")

        return config

    except json.JSONDecodeError as e:
        raise ValueError(f"❌ Error al parsear el archivo JSON: {e}")
    


def guardar_configuracion_simbolo(symbol, config):
    if os.path.exists(RUTA_CONFIG_SIMBOLOS):
        with open(RUTA_CONFIG_SIMBOLOS, "r") as f:
            try:
                datos = json.load(f)
            except json.JSONDecodeError:
                datos = {}
    else:
        datos = {}

    datos[symbol] = config

    with open(RUTA_CONFIG_SIMBOLOS, "w") as f:
        json.dump(datos, f, indent=4)

    print(f"✅ Configuración guardada para {symbol} en {RUTA_CONFIG_SIMBOLOS}")

