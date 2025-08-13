import os
import time
from datetime import datetime
import pandas as pd
import threading
from core.utils.logger import configurar_logger


LOG_DIR = os.getenv('LOG_DIR', 'logs')

log = configurar_logger(__name__)

class RegistroMetrico:
    """Gestiona el almacenamiento de métricas de trading con seguridad y eficiencia."""

    def __init__(self, carpeta: str | None = None, buffer_max=100) ->None:
        if carpeta is None:
            carpeta = os.path.join(LOG_DIR, 'metricas')
        self.carpeta = carpeta
        os.makedirs(self.carpeta, exist_ok=True)
        self.buffer = []
        self.buffer_max = buffer_max
        self.lock = threading.Lock()

    def registrar(self, tipo: str, datos: dict, guardar_inmediatamente=False
        ) ->None:
        """Agrega un nuevo registro. Guarda si se supera el límite o si se indica guardar ya."""
        registro = {'timestamp': datetime.utcnow().isoformat(), 'tipo': tipo}
        registro.update(datos)
        with self.lock:
            self.buffer.append(registro)
            debe_exportar = guardar_inmediatamente or len(self.buffer) >= self.buffer_max

        if debe_exportar:
            self.exportar()

    def exportar(self, intentos: int = 3) ->None:
        """Guarda todos los registros actuales del buffer a disco."""
        for intento in range(1, intentos + 1):
            with self.lock:
                if not self.buffer:
                    return
                df = pd.DataFrame(self.buffer)
                fecha = datetime.utcnow().strftime('%Y%m%d')
                archivo = os.path.join(self.carpeta, f'{fecha}.csv')
                try:
                    modo = 'a' if os.path.exists(archivo) else 'w'
                    cab = not os.path.exists(archivo)
                    df.to_csv(archivo, mode=modo, header=cab, index=False)
                    self.buffer = []
                    return
                except Exception as e:
                    if intento == intentos:
                        log.warning(f'⚠️ Error exportando métricas tras {intentos} intentos: {e}')
            time.sleep(1)


registro_metrico = RegistroMetrico()
