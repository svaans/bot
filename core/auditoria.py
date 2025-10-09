import json
import os
from datetime import UTC, datetime
from threading import Lock
import pandas as pd
_lock = Lock()


def _serializar(valor):
    if isinstance(valor, (dict, list)):
        try:
            return json.dumps(valor, ensure_ascii=False)
        except Exception:
            return str(valor)
    return valor


def registrar_auditoria(symbol: str, evento: str, resultado: str, *,
    estrategias_activas=None, score=None, rsi=None, volumen_relativo=None,
    tendencia=None, razon=None, capital_actual=None, config_usada=None,
    comentario=None, archivo: str='informes/auditoria_bot.csv', formato:
    str='csv') ->None:
    """Registra decisiones críticas del bot en un archivo CSV o Parquet."""
    os.makedirs('logs_auditoria', exist_ok=True)
    ruta_archivo = Path(archivo)
    directorio_archivo = ruta_archivo.parent
    if directorio_archivo != Path('.'):
        directorio_archivo.mkdir(parents=True, exist_ok=True)
    registro = {'timestamp': datetime.now(UTC).isoformat(), 'symbol':
        symbol, 'evento': evento, 'resultado': resultado,
        'estrategias_activas': _serializar(estrategias_activas), 'score':
        score, 'rsi': rsi, 'volumen_relativo': volumen_relativo,
        'tendencia': tendencia, 'razon': _serializar(razon),
        'capital_actual': capital_actual, 'config_usada': _serializar(
        config_usada), 'comentario': comentario}
    df = pd.DataFrame([registro])
    with _lock:
        if formato == 'parquet':
            if os.path.exists(archivo):
                try:
                    existente = pd.read_parquet(archivo)
                    df = pd.concat([existente, df], ignore_index=True)
                except Exception:
                    pass
            df.to_parquet(archivo, index=False)
        else:
            modo = 'a' if os.path.exists(archivo) else 'w'
            cab = not os.path.exists(archivo)
            df.to_csv(archivo, mode=modo, header=cab, index=False)
