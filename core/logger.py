import os
import logging
from datetime import datetime, timezone

# Diccionario global para evitar múltiples handlers duplicados
loggers_configurados = {}

def configurar_logger(nombre: str, nivel=logging.DEBUG, carpeta_logs="logs", modo_silencioso=False):
    if nombre in loggers_configurados:
        return loggers_configurados[nombre]

    logger = logging.getLogger(nombre)
    logger.setLevel(nivel)
    logger.propagate = False

    if not logger.handlers:
        formato = logging.Formatter(
            "%(asctime)s - %(levelname)s - %(name)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )

        if not modo_silencioso:
            consola = logging.StreamHandler()
            consola.setLevel(logging.INFO)
            consola.setFormatter(formato)
            logger.addHandler(consola)

        os.makedirs(carpeta_logs, exist_ok=True)
        ruta_log = os.path.join(carpeta_logs, f"{nombre}.log")
        archivo = logging.FileHandler(ruta_log)
        archivo.setLevel(logging.WARNING if modo_silencioso else nivel)
        archivo.setFormatter(formato)
        logger.addHandler(archivo)

    loggers_configurados[nombre] = logger
    return logger

# Registro de última ejecución por clave para evitar spam
registro_temporal = {}

def log_unico_por_tiempo(clave, mensaje, nivel="info", cooldown=10):
    logger = logging.getLogger("cooldown")
    ahora = datetime.now(timezone.utc).timestamp()
    ultimo = registro_temporal.get(clave, 0)
    if ahora - ultimo >= cooldown:
        if nivel == "info":
            logger.info(mensaje)
        elif nivel == "warning":
            logger.warning(mensaje)
        elif nivel == "error":
            logger.error(mensaje)
        elif nivel == "debug":
            logger.debug(mensaje)
        registro_temporal[clave] = ahora

def log_resumen_operacion(tipo, symbol, **kwargs):
    log = logging.getLogger("resumen")
    if tipo == "entrada":
        log.info(f"🟢 Entrada {symbol} | Puntaje: {kwargs.get('puntaje')} / {kwargs.get('umbral'):.2f} | Peso: {kwargs.get('peso')} | Diversidad: {kwargs.get('diversidad')} | Estrategias: {kwargs.get('estrategias')}")
    elif tipo == "bloqueo":
        log.info(f"🚫 Entrada BLOQUEADA {symbol} | Puntaje: {kwargs.get('puntaje')} / {kwargs.get('umbral')} | Razón: {kwargs.get('razon')}")
    elif tipo == "sl":
        log.warning(f"🛑 Stop Loss ejecutado {symbol} | Precio: {kwargs.get('precio')} | SL: {kwargs.get('stop_loss')}")
    elif tipo == "tp":
        log.info(f"🎯 Take Profit alcanzado {symbol} | Precio: {kwargs.get('precio')} | TP: {kwargs.get('take_profit')}")
    elif tipo == "cierre":
        log.info(f"📤 Cierre operación {symbol} | Entrada: {kwargs.get('entrada')} | Salida: {kwargs.get('salida')} | Retorno: {kwargs.get('retorno')}% | Motivo: {kwargs.get('motivo')}")
    elif tipo == "persistencia":
        log.info(f"🔁 Señales técnicas fuertes repetidas {symbol}")


