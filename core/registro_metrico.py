import os
import json
from datetime import datetime
import pandas as pd

class RegistroMetrico:
    """Gestiona el almacenamiento de métricas de trading."""

    def __init__(self, carpeta="logs/metricas") -> None:
        self.carpeta = carpeta
        os.makedirs(self.carpeta, exist_ok=True)
        self.buffer = []

    def registrar(self, tipo: str, datos: dict) -> None:
        registro = {"timestamp": datetime.utcnow().isoformat(), "tipo": tipo}
        registro.update(datos)
        self.buffer.append(registro)

    def exportar(self) -> None:
        if not self.buffer:
            return
        df = pd.DataFrame(self.buffer)
        fecha = datetime.utcnow().strftime("%Y%m%d")
        archivo = os.path.join(self.carpeta, f"{fecha}.csv")
        modo = "a" if os.path.exists(archivo) else "w"
        cab = not os.path.exists(archivo)
        df.to_csv(archivo, mode=modo, header=cab, index=False)
        self.buffer = []

registro_metrico = RegistroMetrico()