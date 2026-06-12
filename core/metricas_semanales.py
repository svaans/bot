import os
import json
import tempfile
from datetime import datetime, timedelta, timezone

UTC = timezone.utc
import pandas as pd

# Directorio raíz de reportes. Configurable vía env var para evitar depender
# del CWD en producción (mismo patrón que kelly.py → _REPORTES_DIR).
_REPORTES_DIR: str = os.getenv("REPORTES_DIARIOS_PATH", "reportes_diarios")
from core.utils.utils import leer_csv_seguro
from core.utils.log_utils import format_exception_for_log
from core.utils.logger import configurar_logger

log = configurar_logger(__name__)


class MetricasTracker:
    """Acumula eventos relevantes para el reporte semanal."""

    def __init__(self, archivo: str = os.path.join(_REPORTES_DIR, "metricas_semana.json")):
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
                log.exception("Error al cargar métricas semanales desde %s", self.archivo)

    def _guardar(self, intentos: int = 3) -> None:
        """Persiste métricas a disco usando escritura atómica (tmp → replace).

        No usa ``time.sleep`` entre reintentos para no bloquear el event loop
        en contextos async. Cada reintento es inmediato; si el sistema de
        archivos está saturado, reintentar un segundo después es igualmente
        inútil pero sí bloquea el loop.
        """
        directorio = os.path.dirname(self.archivo)
        if directorio:
            os.makedirs(directorio, exist_ok=True)
        tmp_path = None
        for intento in range(1, intentos + 1):
            try:
                fd, tmp_str = tempfile.mkstemp(dir=directorio or ".", suffix=".tmp")
                tmp_path = tmp_str
                with os.fdopen(fd, "w", encoding="utf-8") as f:
                    json.dump(self.data, f)
                    f.flush()
                    os.fsync(f.fileno())
                os.replace(tmp_path, self.archivo)
                tmp_path = None
                return
            except Exception as e:
                if tmp_path is not None:
                    try:
                        os.unlink(tmp_path)
                    except OSError:
                        pass
                    tmp_path = None
                if intento == intentos:
                    log.warning(
                        '⚠️ No se pudo guardar métricas tras %s intentos: %s',
                        intentos,
                        format_exception_for_log(e),
                    )

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


def metricas_semanales(carpeta: str = _REPORTES_DIR) -> pd.DataFrame:
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
        df = leer_csv_seguro(os.path.join(carpeta, archivo))
        if df.empty:
            continue
        if "retorno_total" not in df.columns:
            log.debug(
                "Reporte %s sin columna 'retorno_total' (%d cols); ignorado",
                archivo,
                df.shape[1],
            )
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

        # Sortino: media / downside_std (sólo retornos negativos)
        neg = retornos[retornos < 0]
        dd_std = (neg ** 2).mean() ** 0.5 if not neg.empty else 0.0
        sortino = (float(ganancia_promedio) / dd_std) if dd_std > 0 else float("nan")

        # Profit factor: suma ganancias / suma pérdidas
        bruto_gan = float(retornos[retornos > 0].sum())
        bruto_per = float((-retornos[retornos < 0]).sum())
        profit_factor = (bruto_gan / bruto_per) if bruto_per > 0 else float("inf")

        # Racha máxima de pérdidas consecutivas
        racha_max = racha_actual = 0
        for r in retornos:
            if r < 0:
                racha_actual += 1
                racha_max = max(racha_max, racha_actual)
            else:
                racha_actual = 0

        # Calmar (semanal): retorno_total / drawdown_max
        calmar = (float(retornos.sum()) / abs(float(drawdown))) if drawdown < 0 else float("nan")

        resultados.append(
            {
                "symbol": symbol,
                "operaciones": len(retornos),
                "winrate": round(winrate, 2),
                "ganancia_promedio": round(ganancia_promedio, 4),
                "drawdown_max": round(float(drawdown), 4),
                "profit_factor": round(profit_factor, 3) if profit_factor != float("inf") else None,
                "sortino": round(sortino, 3) if not (sortino != sortino) else None,
                "calmar": round(calmar, 3) if not (calmar != calmar) else None,
                "racha_max_perdidas": racha_max,
            }
        )
    return pd.DataFrame(resultados)
