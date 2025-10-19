import os
import json
import shutil
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any

from core.ajustador_riesgo import (
    MODO_AGRESIVO_SLOPE_THRESHOLD,
    MODO_AGRESIVO_VOL_THRESHOLD,
)
from core.utils.utils import configurar_logger
RUTA_CONFIG_SIMBOLOS = 'config/configuraciones_optimas.json'
log = configurar_logger('config_service')
CONFIG_BASE = {
    'factor_umbral': 1.0,
    'ajuste_volatilidad': 1.0,
    'riesgo_maximo_diario': 0.06,
    'riesgo_por_trade': 0.02,
    'ponderar_por_diversidad': True,
    'modo_agresivo': False,
    'modo_agresivo_vol_threshold': MODO_AGRESIVO_VOL_THRESHOLD,
    'modo_agresivo_slope_threshold': MODO_AGRESIVO_SLOPE_THRESHOLD,
    'multiplicador_estrategias_recurrentes': 1.5,
    'peso_minimo_total': 0.5,
    'diversidad_minima': 2,
    'umbral_peso_estrategia_unica': 3.5,
    'umbral_score_estrategia_unica': 5.0,
    'cooldown_tras_perdida': 3,
    'sl_ratio': 1.5,
    'tp_ratio': 3.0,
    'ratio_minimo_beneficio': 1.3,
    'uso_trailing_technico': True,
    'trailing_buffer': 0.01,
    'trailing_por_atr': True,
    'usar_cierre_parcial': True,
    'umbral_operacion_grande': 30.0,
    'beneficio_minimo_parcial': 5.0,
}

FALLBACK_DIR = Path(tempfile.gettempdir()) / "bot_config_fallbacks"
FALLBACK_ALERT_THRESHOLD = int(os.getenv('FALLBACK_PERMISSION_ALERT_THRESHOLD', '3'))


def _emit_recurrent_permission_alert(src: Path) -> None:
    """Emite una alerta cuando los fallos de permisos son reiterados."""

    if FALLBACK_ALERT_THRESHOLD <= 0:
        return

    # ``glob`` no crea el directorio si no existe; garantizamos su presencia
    FALLBACK_DIR.mkdir(parents=True, exist_ok=True)
    pattern = f"{src.name}.bak_*"
    fallbacks = list(FALLBACK_DIR.glob(pattern))
    total_backups = len(fallbacks)
    if total_backups < FALLBACK_ALERT_THRESHOLD:
        return
    if total_backups == FALLBACK_ALERT_THRESHOLD or total_backups % FALLBACK_ALERT_THRESHOLD == 0:
        log.error(
            '🚨 Permisos denegados recurrentes al crear backup',
            extra={
                "path_origen": str(src),
                "fallback_dir": str(FALLBACK_DIR),
                "total_backups": total_backups,
                "umbral": FALLBACK_ALERT_THRESHOLD,
            },
        )


def backup_json(path: str) -> None:
    """Genera una copia de respaldo manejando errores de E/S."""

    src = Path(path)
    if not src.exists():
        return

    ts = datetime.now().strftime('%Y%m%d_%H%M%S')
    backup_name = src.name + f'.bak_{ts}'
    destino = src.parent / backup_name

    try:
        shutil.copy(src, destino)
        log.info(
            '📦 Backup creado',
            extra={"path_origen": str(src), "path_respaldo": str(destino)},
        )
    except PermissionError as exc:
        log.error(
            '❌ Error de permisos al crear backup',
            extra={"path": str(destino), "error": str(exc)},
        )
        fallback = FALLBACK_DIR / backup_name
        fallback.parent.mkdir(parents=True, exist_ok=True)
        try:
            shutil.copy(src, fallback)
            log.warning(
                '⚠️ Backup redirigido a directorio temporal',
                extra={
                    "path_origen": str(src),
                    "path_respaldo": str(fallback),
                    "motivo": "permission_error",
                },
            )
        except (OSError, PermissionError) as inner_exc:
            log.error(
                '❌ No fue posible crear el backup',
                extra={
                    "path_origen": str(src),
                    "error": str(inner_exc),
                },
            )
    except OSError as exc:
        log.error(
            '❌ Error de E/S al crear backup',
            extra={"path": str(destino), "error": str(exc)},
        )


def _fallback_path(path: Path) -> Path:
    FALLBACK_DIR.mkdir(parents=True, exist_ok=True)
    return FALLBACK_DIR / path.name


class ConfigurationService:
    """Gestiona la carga y almacenamiento de configuraciones por símbolo."""

    def __init__(self, ruta: str=RUTA_CONFIG_SIMBOLOS) ->None:
        self.ruta = ruta

    def load(self, symbol: str) ->dict[str, Any]:
        objetivo = Path(self.ruta)
        if not objetivo.exists():
            log.error(
                '❌ Archivo de configuración no encontrado',
                extra={"path": str(objetivo)},
            )
            raise ValueError('Archivo de configuración inexistente')
        try:
            configuraciones = self._leer_json(objetivo)
        except json.JSONDecodeError as e:
            log.error(
                '❌ Error al parsear el archivo JSON',
                extra={"path": str(objetivo), "error": str(e)},
            )
            raise
        if not isinstance(configuraciones, dict) or not configuraciones:
            log.error(
                '❌ El archivo debe contener un diccionario de configuraciones válido'
                )
            raise ValueError('Configuraciones inválidas')
        config = configuraciones.get(symbol, {}).copy()
        for clave, valor_defecto in CONFIG_BASE.items():
            if clave not in config:
                log.warning(
                    f"⚠️ {symbol} - Faltante: '{clave}'. Usando valor por defecto: {valor_defecto}"
                    )
                config[clave] = valor_defecto
        return config

    def save(self, symbol: str, config: dict[str, Any]) ->None:
        if not isinstance(config, dict):
            log.error(f'❌ Configuración inválida para guardar: {symbol}')
            raise ValueError('La configuración debe ser un diccionario')
        objetivo = Path(self.ruta)
        if objetivo.exists():
            try:
                datos: dict[str, Any] = self._leer_json(objetivo)
            except json.JSONDecodeError:
                datos = {}
        else:
            datos = {}
        datos[symbol] = config
        backup_json(self.ruta)
        destino = self._escribir_json(objetivo, datos)
        log.info(
            '✅ Configuración guardada',
            extra={
                "symbol": symbol,
                "path": str(destino),
            },
        )

    def _leer_json(self, path: Path) -> dict[str, Any]:
        try:
            with path.open('r', encoding='utf-8') as fh:
                return json.load(fh)
        except PermissionError as exc:
            log.error(
                '❌ Permiso denegado al leer configuración',
                extra={"path": str(path), "error": str(exc)},
            )
            fallback = _fallback_path(path)
            if fallback.exists():
                log.warning(
                    '⚠️ Leyendo configuración desde fallback',
                    extra={"path": str(fallback)},
                )
                with fallback.open('r', encoding='utf-8') as fh:
                    return json.load(fh)
            raise
        except OSError as exc:
            log.error(
                '❌ Error de E/S al leer configuración',
                extra={"path": str(path), "error": str(exc)},
            )
            fallback = _fallback_path(path)
            if fallback.exists():
                log.warning(
                    '⚠️ Reintentando lectura en directorio temporal',
                    extra={"path": str(fallback)},
                )
                with fallback.open('r', encoding='utf-8') as fh:
                    return json.load(fh)
            raise

    def _escribir_json(self, path: Path, data: dict[str, Any]) -> Path:
        try:
            with path.open('w', encoding='utf-8') as fh:
                json.dump(data, fh, indent=4)
            return path
        except PermissionError as exc:
            log.error(
                '❌ Permiso denegado al escribir configuración',
                extra={"path": str(path), "error": str(exc)},
            )
            fallback = _fallback_path(path)
            fallback.parent.mkdir(parents=True, exist_ok=True)
            with fallback.open('w', encoding='utf-8') as fh:
                json.dump(data, fh, indent=4)
            log.warning(
                '⚠️ Configuración persistida en directorio temporal',
                extra={"path": str(fallback)},
            )
            return fallback
        except OSError as exc:
            log.error(
                '❌ Error de E/S al escribir configuración',
                extra={"path": str(path), "error": str(exc)},
            )
            fallback = _fallback_path(path)
            fallback.parent.mkdir(parents=True, exist_ok=True)
            try:
                with fallback.open('w', encoding='utf-8') as fh:
                    json.dump(data, fh, indent=4)
            except (OSError, PermissionError) as inner_exc:
                log.error(
                    '❌ No fue posible persistir la configuración',
                    extra={"path": str(fallback), "error": str(inner_exc)},
                )
                raise
            log.warning(
                '⚠️ Configuración persistida en fallback por error de E/S',
                extra={
                    "path": str(fallback),
                    "error_original": str(exc),
                },
            )
            return fallback


_service = ConfigurationService()


def cargar_configuracion_simbolo(symbol: str) ->dict:
    return _service.load(symbol)


def guardar_configuracion_simbolo(symbol: str, config: dict) ->None:
    _service.save(symbol, config)
