import os
import json
import time
from datetime import UTC, datetime, timedelta
import pandas as pd
from core.utils.utils import leer_csv_seguro
from core.utils.logger import configurar_logger

log = configurar_logger(__name__)


class MetricasTracker:
    """Acumula eventos relevantes para el reporte semanal."""

    def __init__(self, archivo="reportes_diarios/metricas_semana.json"):
        self.archivo = archivo
        self.data = {
            "filtradas_persistencia": 0,
            "filtradas_umbral": 0,
            "filtradas_diversidad": 0,
            "diferencias_umbral": [],
            "sl_evitas": 0,
        }
        self._cargar()

    def _cargar(self) -> None:
        if os.path.exists(self.archivo):
            try:
                with open(self.archivo) as f:
                    datos = json.load(f)
                    if isinstance(datos, dict):
                        self.data.update(datos)
            except Exception:
                pass

    def _guardar(self, intentos: int = 3) -> None:
        os.makedirs(os.path.dirname(self.archivo), exist_ok=True)
        for intento in range(1, intentos + 1):
            try:
                with open(self.archivo, "w") as f:
                    json.dump(self.data, f)
                break
            except Exception as e:
                if intento == intentos:
                    log.warning(f'⚠️ No se pudo guardar métricas tras {intentos} intentos: {e}')
                else:
                    time.sleep(1)

    def registrar_filtro(self, tipo: str) -> None:
        """Incrementa el contador para el ``tipo`` de filtro dado."""
        key = f"filtradas_{tipo}"
        self.data[key] = self.data.get(key, 0) + 1
        self._guardar()

    def registrar_diferencia_umbral(self, diferencia: float) -> None:
        self.data.setdefault("diferencias_umbral", [])
        self.data["diferencias_umbral"].append(round(float(diferencia), 4))
        self._guardar()

    def registrar_sl_evitado(self) -> None:
        self.data["sl_evitas"] += 1
        self._guardar()

    def reset(self) -> None:
        self.data = {
            "filtradas_persistencia": 0,
            "filtradas_umbral": 0,
            "filtradas_diversidad": 0,
            "diferencias_umbral": [],
            "sl_evitas": 0,
        }
        self._guardar()


metricas_tracker = MetricasTracker()


def metricas_semanales(carpeta: str = "reportes_diarios") -> pd.DataFrame:
    """Calcula métricas de la última semana para cada par."""
    if not os.path.isdir(carpeta):
        return pd.DataFrame()
    fin = datetime.now(UTC).date()
    inicio = fin - timedelta(days=7)
    datos = []
    for archivo in os.listdir(carpeta):
        if not archivo.endswith(".csv"):
            continue
        try:
            fecha = datetime.fromisoformat(archivo.replace(".csv", "")).date()
        except ValueError:
            continue
        if not inicio <= fecha < fin:
            continue
        df = leer_csv_seguro(os.path.join(carpeta, archivo), expected_cols=20)
        if df.empty:
            continue
        if "symbol" not in df.columns and "simbolo" in df.columns:
            df["symbol"] = df["simbolo"]
        datos.append(df)
    if not datos:
        return pd.DataFrame()
    df = pd.concat(datos, ignore_index=True)
    resultados = []
    for symbol, grupo in df.groupby("symbol"):
        retornos = grupo["retorno_total"].astype(float)
        if retornos.empty:
            continue
        winrate = (retornos > 0).mean() * 100
        ganancia_promedio = retornos.mean()
        equity = retornos.cumsum()
        drawdown = (equity - equity.cummax()).min()
        resultados.append(
            {
                "symbol": symbol,
                "operaciones": len(retornos),
                "winrate": round(winrate, 2),
                "ganancia_promedio": round(ganancia_promedio, 4),
                "drawdown_max": round(drawdown, 4),
            }
        )
    return pd.DataFrame(resultados)
