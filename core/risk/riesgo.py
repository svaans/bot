import os
import json
from datetime import datetime
from filelock import FileLock
from core.utils.utils import configurar_logger
log = configurar_logger('riesgo')
RUTA_ESTADO = 'config/riesgo.json'
RUTA_ESTADO_BAK = RUTA_ESTADO + '.bak'
_LOCK_PATH = RUTA_ESTADO + '.lock'


def cargar_estado_riesgo_seguro() ->dict:
    log.info('➡️ Entrando en cargar_estado_riesgo_seguro()')
    """Lee ``riesgo.json`` de forma segura usando bloqueo de archivo."""
    lock = FileLock(_LOCK_PATH)
    with lock:
        if not os.path.exists(RUTA_ESTADO):
            if os.path.exists(RUTA_ESTADO_BAK):
                try:
                    with open(RUTA_ESTADO_BAK, 'r') as f:
                        return json.load(f)
                except Exception as e:  # pragma: no cover - error de lectura
                    log.warning(f'⚠️ Backup corrupto: {e}')
            return {'fecha': '', 'perdida_acumulada': 0.0}
        try:
            with open(RUTA_ESTADO, 'r') as f:
                estado = json.load(f)
            if not isinstance(estado, dict):
                raise ValueError('❌ Formato inválido en estado de riesgo.')
            return estado
        except (OSError, json.JSONDecodeError) as e:
            log.warning(f'⚠️ Error al cargar estado de riesgo: {e}')
            if os.path.exists(RUTA_ESTADO_BAK):
                try:
                    with open(RUTA_ESTADO_BAK, 'r') as f:
                        return json.load(f)
                except Exception as e2:
                    log.warning(f'⚠️ Backup también falló: {e2}')
            return {'fecha': '', 'perdida_acumulada': 0.0}


def cargar_estado_riesgo() ->dict:
    log.info('➡️ Entrando en cargar_estado_riesgo()')
    """Compatibilidad retro: delega en :func:`cargar_estado_riesgo_seguro`."""
    return cargar_estado_riesgo_seguro()


def guardar_estado_riesgo_seguro(estado: dict) ->None:
    log.info('➡️ Entrando en guardar_estado_riesgo_seguro()')
    """Guarda ``estado`` en ``riesgo.json`` de forma atómica y segura."""
    lock = FileLock(_LOCK_PATH)
    try:
        with lock, open(RUTA_ESTADO, 'w') as f:
            json.dump(estado, f, indent=4)
        try:
            with open(RUTA_ESTADO_BAK, 'w') as fb:
                json.dump(estado, fb, indent=4)
        except OSError as e:
            log.warning(f'⚠️ No se pudo escribir backup: {e}')
        log.info(f'💾 Estado de riesgo actualizado: {estado}')
    except OSError as e:
        log.error(f'❌ No se pudo guardar estado de riesgo: {e}')
        raise


def guardar_estado_riesgo(estado: dict) ->None:
    log.info('➡️ Entrando en guardar_estado_riesgo()')
    """Compatibilidad retro: delega en :func:`guardar_estado_riesgo_seguro`."""
    guardar_estado_riesgo_seguro(estado)


def actualizar_perdida(simbolo: str, perdida: float):
    log.info('➡️ Entrando en actualizar_perdida()')
    """Registra una pérdida para el día actual (valor absoluto)."""
    estado = cargar_estado_riesgo_seguro()
    hoy = datetime.utcnow().date().isoformat()
    if estado.get('fecha') != hoy:
        estado = {'fecha': hoy, 'perdida_acumulada': 0.0}
    estado['perdida_acumulada'] += abs(perdida)
    guardar_estado_riesgo_seguro(estado)
    log.info(
        f"📉 {simbolo}: pérdida registrada {perdida:.2f} | Total hoy: {estado['perdida_acumulada']:.2f}"
        )


def riesgo_superado(umbral: float, capital_total: float) ->bool:
    log.info('➡️ Entrando en riesgo_superado()')
    """Evalúa si el umbral de pérdida diaria ha sido superado."""
    estado = cargar_estado_riesgo_seguro()
    hoy = datetime.utcnow().date().isoformat()
    if estado.get('fecha') != hoy:
        return False
    if capital_total <= 0:
        log.warning('⚠️ Capital total es 0. No se puede evaluar riesgo.')
        return False
    porcentaje_perdido = estado['perdida_acumulada'] / capital_total
    log.debug(
        f'Evaluación de riesgo: {porcentaje_perdido:.2%} perdido (umbral: {umbral:.2%})'
        )
    return porcentaje_perdido >= umbral
