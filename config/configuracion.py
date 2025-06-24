import os
import json
import shutil
from datetime import datetime
from core.utils.utils import configurar_logger

RUTA_CONFIG_SIMBOLOS = "config/configuraciones_optimas.json"
log = configurar_logger("config_service")

# Esquema base de configuración mínima por símbolo
CONFIG_BASE = {
    "factor_umbral": 1.0,
    "ajuste_volatilidad": 1.0,
    "riesgo_maximo_diario": 2.0,
    "ponderar_por_diversidad": True,
    "modo_agresivo": False,
    "multiplicador_estrategias_recurrentes": 1.5,
    "peso_minimo_total": 2.0,
    "diversidad_minima": 2,
    "cooldown_tras_perdida": 3,
    "sl_ratio": 1.5,
    "tp_ratio": 3.0,
    "ratio_minimo_beneficio": 1.3,
    "uso_trailing_technico": True,
    "trailing_buffer": 0.01,
    "trailing_por_atr": True,
    "usar_cierre_parcial": True,
    "umbral_operacion_grande": 30.0,
    "beneficio_minimo_parcial": 5.0
}


def backup_json(path: str):
    if os.path.exists(path):
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        shutil.copy(path, path + f".bak_{ts}")
        log.info(f"📦 Backup creado: {path}.bak_{ts}")


class ConfigurationService:
    """Gestiona la carga y almacenamiento de configuraciones por símbolo."""

    def __init__(self, ruta: str = RUTA_CONFIG_SIMBOLOS) -> None:
        self.ruta = ruta

    def load(self, symbol: str) -> dict:
        if not os.path.exists(self.ruta):
            log.error(f"❌ Archivo de configuración no encontrado: {self.ruta}")
            raise ValueError("Archivo de configuración inexistente")

        try:
            with open(self.ruta, "r") as f:
                configuraciones = json.load(f)
        except json.JSONDecodeError as e:
            log.error(f"❌ Error al parsear el archivo JSON: {e}")
            raise

        if not isinstance(configuraciones, dict) or not configuraciones:
            log.error("❌ El archivo debe contener un diccionario de configuraciones válido")
            raise ValueError("Configuraciones inválidas")

        config = configuraciones.get(symbol, {}).copy()

        # Completar faltantes con base
        for clave, valor_defecto in CONFIG_BASE.items():
            if clave not in config:
                log.warning(f"⚠️ {symbol} - Faltante: '{clave}'. Usando valor por defecto: {valor_defecto}")
                config[clave] = valor_defecto

        return config

    def save(self, symbol: str, config: dict) -> None:
        if not isinstance(config, dict):
            log.error(f"❌ Configuración inválida para guardar: {symbol}")
            raise ValueError("La configuración debe ser un diccionario")

        if os.path.exists(self.ruta):
            with open(self.ruta, "r") as f:
                try:
                    datos = json.load(f)
                except json.JSONDecodeError:
                    datos = {}
        else:
            datos = {}

        datos[symbol] = config
        backup_json(self.ruta)

        with open(self.ruta, "w") as f:
            json.dump(datos, f, indent=4)
        log.info(f"✅ Configuración guardada para {symbol} en {self.ruta}")


# Interfaces públicas
_service = ConfigurationService()

def cargar_configuracion_simbolo(symbol: str) -> dict:
    return _service.load(symbol)

def guardar_configuracion_simbolo(symbol: str, config: dict) -> None:
    _service.save(symbol, config)
