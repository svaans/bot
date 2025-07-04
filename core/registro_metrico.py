import os
from datetime import datetime
import pandas as pd
import threading


class RegistroMetrico:
    """Gestiona el almacenamiento de métricas de trading con seguridad y eficiencia."""

    def __init__(self, carpeta='logs/metricas', buffer_max=100) ->None:
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
            if guardar_inmediatamente or len(self.buffer) >= self.buffer_max:
                self.exportar()

    def exportar(self) ->None:
        """Guarda todos los registros actuales del buffer a disco."""
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
            except Exception as e:
                print(f'❌ Error exportando métricas: {e}')


registro_metrico = RegistroMetrico()
