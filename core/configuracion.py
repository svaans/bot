import os
import json
from core.logger import configurar_logger

RUTA_CONFIG_SIMBOLOS = "config/configuraciones_optimas.json"

log = configurar_logger("config_service")

class ConfigurationService:
    """Gestiona la carga y almacenamiento de configuraciones por símbolo."""

    def __init__(self, ruta: str = RUTA_CONFIG_SIMBOLOS) -> None:
        self.ruta = ruta

    def load(self, symbol: str) -> dict:
        if not os.path.exists(self.ruta):
            log.warning(f"❌ Archivo de configuración no encontrado: {self.ruta}")
            return {}
        try:
            with open(self.ruta, "r") as f:
                configuraciones = json.load(f)
        except json.JSONDecodeError as e:
            log.error(f"❌ Error al parsear el archivo JSON: {e}")
            return {}

        if not isinstance(configuraciones, dict):
            log.error("❌ El archivo debe contener un diccionario de configuraciones.")
            return {}
        config = configuraciones.get(symbol)
        if config is None:
            log.warning(f"⚠️ No se encontró configuración para {symbol}. Se usará config vacía.")
            return {}

        # Validación opcional de claves mínimas
        claves_esperadas = ["factor_umbral", "peso_minimo_total", "diversidad_minima"]
        for clave in claves_esperadas:
            if clave not in config:
                log.warning(f"⚠️ Falta clave '{clave}' en configuración de {symbol}.")

        return config

    def save(self, symbol: str, config: dict) -> None:
        if os.path.exists(self.ruta):
            with open(self.ruta, "r") as f:
                try:
                    datos = json.load(f)
                except json.JSONDecodeError:
                    datos = {}
        else:
            datos = {}

        datos[symbol] = config
        with open(self.ruta, "w") as f:
            json.dump(datos, f, indent=4)
        log.info(f"✅ Configuración guardada para {symbol} en {self.ruta}")
    


_service = ConfigurationService()

def cargar_configuracion_simbolo(symbol: str) -> dict:
    """Función de compatibilidad."""
    return _service.load(symbol)

def guardar_configuracion_simbolo(symbol: str, config: dict) -> None:
    """Función de compatibilidad."""
    _service.save(symbol, config)

