import os
import logging
from logging.handlers import (
    TimedRotatingFileHandler,
    QueueHandler,
    QueueListener,
)
import queue
import json
from datetime import datetime, timezone
import time
from pathlib import Path
from dotenv import load_dotenv
from typing import Optional, Dict


class FiltroRelevante(logging.Filter):
    """Filtra mensajes poco informativos para el archivo de log."""
    PALABRAS_CLAVE_DESCARTAR = [
        'entrada no válida',
        'entrada rechazada',
        'entrada bloqueada',
        'entrando en',
    ]

    def filter(self, record: logging.LogRecord) -> bool:
        mensaje = record.getMessage().lower()
        if any(p in mensaje for p in self.PALABRAS_CLAVE_DESCARTAR):
            logger = logging.getLogger(record.name)
            # Si el logger está en DEBUG, deja pasar todo
            if logger.getEffectiveLevel() <= logging.DEBUG:
                return True
            return False
        return True


class DebugSamplingFilter(logging.Filter):
    """Muestra solo uno de cada ``rate`` mensajes DEBUG."""

    def __init__(self, rate: int) -> None:
        super().__init__()
        self.rate = max(1, rate)
        self._count = 0

    def filter(self, record: logging.LogRecord) -> bool:
        if record.levelno == logging.DEBUG:
            self._count = (self._count + 1) % self.rate
            return self._count == 0
        return True


archivo_global: Optional[logging.Handler] = None
loggers_configurados: Dict[str, logging.Logger] = {}
load_dotenv(Path(__file__).resolve().parent.parent / 'config' / 'claves.env')

FAST_MODE = os.getenv("LOG_FAST") == "1"
DEBUG_SAMPLE = int(os.getenv("LOG_SAMPLE_DEBUG", "1"))
_LOG_QUEUE: Optional[queue.Queue] = None
_LISTENER: Optional[QueueListener] = None

# Registro de últimos logs para limitar spam
_last_log: dict[str, float] = {}


def _should_log(key: str, every: float = 3.0) -> bool:
    """Devuelve ``True`` si han pasado ``every`` segundos desde el último log.

    Permite reducir el ruido de logs repetitivos. ``key`` identifica el
    evento a controlar.
    """
    now = time.time()
    prev = _last_log.get(key, 0.0)
    if now - prev >= every:
        _last_log[key] = now
        return True
    return False


class JsonFormatter(logging.Formatter):
    """Formatter que genera cada entrada en formato JSON."""

    def format(self, record: logging.LogRecord) -> str:
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
            'id': getattr(record, 'id', None),
            'status_code': getattr(record, 'status_code', None),
            'body': getattr(record, 'body', None),
        }
        return json.dumps(data, ensure_ascii=False)


def configurar_logger(
    nombre: str,
    nivel: int = logging.INFO,
    carpeta_logs: Optional[str] = None,
    modo_silencioso: bool = False,
    estructurado: Optional[bool] = None,
    *,
    backup_count: int = 7,
    when: str = 'midnight',
) -> logging.Logger:
    """Configura (una sola vez) un logger asíncrono con rotación diaria."""
    if nombre in loggers_configurados:
        return loggers_configurados[nombre]

    logger = logging.getLogger(nombre)

    # Ajustes globales rápidos
    if FAST_MODE:
        nivel = logging.WARNING
    nivel_env = os.getenv('LOG_LEVEL')
    if nivel_env:
        nivel = getattr(logging, nivel_env.upper(), nivel)
    logger.setLevel(nivel)
    logger.propagate = False

    global archivo_global, _LOG_QUEUE, _LISTENER
    if carpeta_logs is None:
        carpeta_logs = os.getenv('LOG_DIR', 'logs')
    if estructurado is None:
        estructurado = os.getenv('MODO_REAL', 'False').lower() == 'true'
    if FAST_MODE:
        estructurado = False

    # Inicializar infraestructura compartida (queue + listener) una vez
    if _LOG_QUEUE is None:
        _LOG_QUEUE = queue.Queue(-1)
        handlers: list[logging.Handler] = []

        if not modo_silencioso:
            consola = logging.StreamHandler()
            handlers.append(consola)

        os.makedirs(carpeta_logs, exist_ok=True)
        ruta_log = os.path.join(carpeta_logs, 'bot.log')
        archivo_global = TimedRotatingFileHandler(
            ruta_log, when=when, backupCount=backup_count
        )
        handlers.append(archivo_global)

        if estructurado:
            formato: logging.Formatter = JsonFormatter()
        elif FAST_MODE:
            formato = logging.Formatter('%(asctime)s %(levelname)s %(name)s: %(message)s')
        else:
            formato = logging.Formatter(
                '%(asctime)s - %(levelname)s - %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S',
            )

        for h in handlers:
            h.setFormatter(formato)
            if h is archivo_global:
                h.addFilter(FiltroRelevante())

        _LISTENER = QueueListener(_LOG_QUEUE, *handlers, respect_handler_level=True)
        _LISTENER.start()

    # Conectar el logger a la cola
    if not logger.handlers:
        qh = QueueHandler(_LOG_QUEUE)  # type: ignore[arg-type]
        qh.setLevel(nivel)
        if DEBUG_SAMPLE > 1:
            qh.addFilter(DebugSamplingFilter(DEBUG_SAMPLE))
        logger.addHandler(qh)

    loggers_configurados[nombre] = logger
    return logger


def log_decision(
    logger: logging.Logger,
    accion: str,
    operation_id: Optional[str],
    entrada: dict,
    validaciones: dict,
    decision: str,
    salida: dict,
) -> None:
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


def detener_logger() -> None:
    """Detiene el ``QueueListener`` y cierra los manejadores globales.

    Útil en tests que inicializan el logger varias veces en el mismo proceso.
    """
    global _LISTENER, _LOG_QUEUE, archivo_global, loggers_configurados
    if _LISTENER is not None:
        _LISTENER.stop()
        _LISTENER = None
    if archivo_global is not None:
        archivo_global.close()
        archivo_global = None
    _LOG_QUEUE = None
    loggers_configurados.clear()




