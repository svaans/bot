import os
import logging
from logging.handlers import TimedRotatingFileHandler
import json
from datetime import datetime, timezone
from pathlib import Path
from dotenv import load_dotenv


class FiltroRelevante(logging.Filter):
    """Filtra mensajes poco informativos para el archivo de log."""
    PALABRAS_CLAVE_DESCARTAR = [
        'entrada no válida',
        'entrada rechazada',
        'entrada bloqueada',
        'entrando en',
    ]

    def filter(self, record: logging.LogRecord) ->bool:
        mensaje = record.getMessage().lower()
        if any(p in mensaje for p in self.PALABRAS_CLAVE_DESCARTAR):
            logger = logging.getLogger(record.name)
            if logger.getEffectiveLevel() <= logging.DEBUG:
                return True
            return False
        return True


archivo_global = None
loggers_configurados = {}
load_dotenv(Path(__file__).resolve().parent.parent / 'config' / 'claves.env')


class JsonFormatter(logging.Formatter):
    """Formatter que genera cada entrada en formato JSON."""

    def format(self, record: logging.LogRecord) ->str:
        ts = getattr(record, 'timestamp', None)
        if ts is None:
            ts = record.created
        if isinstance(ts, (int, float)):
            ts_iso = datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
        else:
            try:
                ts_iso = datetime.fromtimestamp(float(ts), tz=timezone.utc).isoformat()
            except Exception:
                ts_iso = str(ts)
        data = {
            'symbol': getattr(record, 'symbol', None),
            'timeframe': getattr(record, 'timeframe', None),
            'timestamp': ts_iso,
            'evento': record.getMessage(),
            'level': record.levelname,
            'logger': record.name,
        }
        return json.dumps(data, ensure_ascii=False)


def configurar_logger(
    nombre: str,
    nivel=logging.INFO,
    carpeta_logs: str | None = None,
    modo_silencioso: bool = False,
    estructurado=None,
    *,
    backup_count: int = 7,
    when: str = 'midnight',
):
    if nombre in loggers_configurados:
        return loggers_configurados[nombre]
    logger = logging.getLogger(nombre)
    nivel_env = os.getenv('LOG_LEVEL')
    if nivel_env:
        nivel = getattr(logging, nivel_env.upper(), nivel)
    logger.setLevel(nivel)
    logger.propagate = False
    global archivo_global
    if carpeta_logs is None:
        carpeta_logs = os.getenv('LOG_DIR', 'logs')
    if estructurado is None:
        estructurado = os.getenv('MODO_REAL', 'False').lower() == 'true'
    if not logger.handlers:
        if estructurado:
            formato = JsonFormatter()
        else:
            formato = logging.Formatter(
                '%(asctime)s - %(levelname)s - %(message)s', datefmt=
                '%Y-%m-%d %H:%M:%S')
        if not modo_silencioso:
            consola = logging.StreamHandler()
            consola.setLevel(nivel)
            consola.setFormatter(formato)
            logger.addHandler(consola)
        if archivo_global is None:
            os.makedirs(carpeta_logs, exist_ok=True)
            ruta_log = os.path.join(carpeta_logs, 'bot.log')
            archivo_global = TimedRotatingFileHandler(
                ruta_log, when=when, backupCount=backup_count)
            archivo_global.setLevel(nivel)
            archivo_global.setFormatter(formato)
            archivo_global.addFilter(FiltroRelevante())
        elif nivel < archivo_global.level:
            archivo_global.setLevel(nivel)
        logger.addHandler(archivo_global)
    loggers_configurados[nombre] = logger
    return logger


def log_decision(logger: logging.Logger, accion: str, operation_id: str | None,
                 entrada: dict, validaciones: dict, decision: str,
                 salida: dict) -> None:
    """Emite un log estructurado de una decisión tomada por el bot."""
    payload = {
        'accion': accion,
        'operation_id': operation_id,
        'entrada': entrada,
        'validaciones': validaciones,
        'decision': decision,
        'salida': salida,
    }
    logger.info(json.dumps(payload, ensure_ascii=False))


def log_resumen_operacion(tipo, symbol, **kwargs):
    log.info('➡️ Entrando en log_resumen_operacion()')
    log = logging.getLogger('resumen')
    if tipo == 'entrada':
        log.info(
            f"🟢 Entrada {symbol} | Puntaje: {kwargs.get('puntaje')} / {kwargs.get('umbral'):.2f} | Peso: {kwargs.get('peso')} | Diversidad: {kwargs.get('diversidad')} | Estrategias: {kwargs.get('estrategias')}"
            )
    elif tipo == 'bloqueo':
        log.info(
            f"🚫 Entrada BLOQUEADA {symbol} | Puntaje: {kwargs.get('puntaje')} / {kwargs.get('umbral')} | Razón: {kwargs.get('razon')}"
            )
    elif tipo == 'sl':
        log.warning(
            f"🛑 Stop Loss ejecutado {symbol} | Precio: {kwargs.get('precio')} | SL: {kwargs.get('stop_loss')}"
            )
    elif tipo == 'tp':
        log.info(
            f"🎯 Take Profit alcanzado {symbol} | Precio: {kwargs.get('precio')} | TP: {kwargs.get('take_profit')}"
            )
    elif tipo == 'cierre':
        log.info(
            f"📤 Cierre operación {symbol} | Entrada: {kwargs.get('entrada')} | Salida: {kwargs.get('salida')} | Retorno: {kwargs.get('retorno')}% | Motivo: {kwargs.get('motivo')}"
            )
    elif tipo == 'persistencia':
        log.info(f'🔁 Señales técnicas fuertes repetidas {symbol}')
