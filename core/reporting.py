import os
import json
import atexit
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime, timezone
from math import isclose
from typing import Any, Iterable, Mapping

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
from core.state import register_state
from core.utils.io_metrics import observe_disk_write, report_generation_timer
from core.utils.utils import configurar_logger
from core.utils.utils import leer_csv_seguro
from observability.metrics import REPORT_IO_ERRORS_TOTAL

UTC = timezone.utc

log = configurar_logger('reporte_diario')

_executor = ProcessPoolExecutor(max_workers=1)
atexit.register(_executor.shutdown)


class ReporterDiario:

    _COLUMN_SCHEMA: dict[str, float] = {
        "retorno_total": 0.0,
        "precio_entrada": 0.0,
        "precio_cierre": 0.0,
        "stop_loss": 0.0,
        "take_profit": 0.0,
        "cantidad": 0.0,
    }

    _OPTIONAL_NUMERIC: dict[str, float] = {
        "latencia_ejecucion_ms": 0.0,
        "latencia_ms": 0.0,
        "latency_ms": 0.0,
        "execution_latency_ms": 0.0,
        "slippage": 0.0,
        "slippage_pct": 0.0,
        "slippage_percent": 0.0,
        "exposicion_total": 0.0,
        "exposicion": 0.0,
        "exposure_total": 0.0,
        "exposure": 0.0,
        "notional": 0.0,
        "max_price": 0.0,
    }

    _LATENCY_COLUMNS: tuple[str, ...] = (
        "latencia_ejecucion_ms",
        "latencia_ms",
        "latency_ms",
        "execution_latency_ms",
    )
    _SLIPPAGE_COLUMNS: tuple[str, ...] = ("slippage", "slippage_pct", "slippage_percent")
    _EXPOSURE_COLUMNS: tuple[str, ...] = (
        "exposicion_total",
        "exposicion",
        "exposure_total",
        "exposure",
        "notional",
    )

    def __init__(self, carpeta='reportes_diarios', max_operaciones=1000):
        self.carpeta = carpeta
        os.makedirs(self.carpeta, exist_ok=True)
        self.fecha_actual = datetime.now(UTC).date()
        self.log = configurar_logger('reporte')
        self.estadisticas_archivo = os.path.join(
            self.carpeta, 'estadisticas.csv'
        )
        self._cargar_estadisticas()
        self.ultimas_operaciones = {}
        self.max_operaciones = max_operaciones
        try:
            register_state(
                "reporter_diario",
                dump=self._export_critical_state,
                load=self._restore_critical_state,
                priority=10,
            )
        except Exception:
            self.log.debug(
                "No se pudo registrar la persistencia de reporter_diario", exc_info=True
            )

    def _cargar_estadisticas(self):
        columnas = ['symbol', 'operaciones', 'wins', 'retorno_acumulado',
            'max_equity', 'drawdown', 'buy_hold_start', 'last_price']
        if os.path.exists(self.estadisticas_archivo):
            try:
                self.estadisticas = leer_csv_seguro(self.
                    estadisticas_archivo, expected_cols=len(columnas))
            except Exception:
                REPORT_IO_ERRORS_TOTAL.labels(operation="report_stats_read").inc()
                self.estadisticas = pd.DataFrame(columns=columnas)
        else:
            self.estadisticas = pd.DataFrame(columns=columnas)

    def _guardar_estadisticas(self):
        if self.estadisticas.empty:
            return
        df = self.estadisticas.copy()
        df['winrate'] = df['wins'] / df['operaciones'] * 100
        df['buy_hold'] = (df['last_price'] - df['buy_hold_start']) / df[
            'buy_hold_start']
        try:
            observe_disk_write(
                "report_estadisticas_csv",
                self.estadisticas_archivo,
                lambda: df.to_csv(self.estadisticas_archivo, index=False),
            )
        except Exception:
            REPORT_IO_ERRORS_TOTAL.labels(operation="report_estadisticas_csv").inc()
            raise

    def _actualizar_estadisticas(self, info: dict):
        symbol = info.get('symbol') or info.get('simbolo')
        if not symbol:
            return
        retorno = float(info.get('retorno_total', 0.0))
        entrada = float(info.get('precio_entrada', 0.0))
        cierre = float(info.get('precio_cierre', entrada))
        if symbol not in self.estadisticas['symbol'].values:
            nueva = {'symbol': symbol, 'operaciones': 0, 'wins': 0,
                'retorno_acumulado': 0.0, 'max_equity': 0.0, 'drawdown': 
                0.0, 'buy_hold_start': entrada if entrada else cierre,
                'last_price': cierre}
            frames = [df for df in [self.estadisticas, pd.DataFrame([nueva]
                )] if not df.empty]
            self.estadisticas = pd.concat(frames, ignore_index=True
                ) if frames else pd.DataFrame([nueva])
        if self.estadisticas.empty:
            return
        filas = self.estadisticas[self.estadisticas['symbol'] == symbol]
        if filas.empty:
            return
        idx = filas.index[0]
        self.estadisticas.loc[idx, 'operaciones'] += 1
        self.estadisticas.loc[idx, 'retorno_acumulado'] += retorno
        if retorno > 0:
            self.estadisticas.loc[idx, 'wins'] += 1
        acum = self.estadisticas.loc[idx, 'retorno_acumulado']
        max_eq = self.estadisticas.loc[idx, 'max_equity']
        max_eq = max(max_eq, acum)
        self.estadisticas.loc[idx, 'max_equity'] = max_eq
        self.estadisticas.loc[idx, 'drawdown'] = min(self.estadisticas.loc[
            idx, 'drawdown'], acum - max_eq)
        if isclose(self.estadisticas.loc[idx, 'buy_hold_start'], 0.0, rel_tol=1e-12, abs_tol=1e-12):
            self.estadisticas.loc[idx, 'buy_hold_start'
                ] = entrada if entrada else cierre
        self.estadisticas.loc[idx, 'last_price'] = cierre
        self._guardar_estadisticas()

    def registrar_operacion(self, info: dict):
        fecha = datetime.now(UTC).date()
        archivo = os.path.join(self.carpeta, f'{fecha}.csv')
        df = pd.DataFrame([info])
        if os.path.exists(archivo):
            try:
                observe_disk_write(
                    "report_operacion_csv_append",
                    archivo,
                    lambda: df.to_csv(archivo, mode='a', header=False, index=False),
                )
            except Exception:
                REPORT_IO_ERRORS_TOTAL.labels(operation="report_operacion_csv_append").inc()
                raise
        else:
            try:
                observe_disk_write(
                    "report_operacion_csv_create",
                    archivo,
                    lambda: df.to_csv(archivo, index=False),
                )
            except Exception:
                REPORT_IO_ERRORS_TOTAL.labels(operation="report_operacion_csv_create").inc()
                raise
        symbol = info.get('symbol') or info.get('simbolo')
        if symbol:
            ops = self.ultimas_operaciones.setdefault(symbol, [])
            ops.append(info)
            if len(ops) > self.max_operaciones:
                self.ultimas_operaciones[symbol] = ops[-self.max_operaciones:]
        self.log.info(f'📝 Operación registrada para reporte {fecha}')
        self._actualizar_estadisticas(info)
        if fecha != self.fecha_actual:
            try:
                _executor.submit(self.generar_informe, self.fecha_actual)
            except Exception:
                REPORT_IO_ERRORS_TOTAL.labels(operation="report_async_submit").inc()
                self.log.exception('Error al generar informe en proceso separado')
            self.fecha_actual = fecha

    def _export_critical_state(self) -> dict[str, Any]:
        if not self.ultimas_operaciones:
            return {
                "ultimas_operaciones": {},
                "fecha_actual": self.fecha_actual.isoformat(),
            }
        datos: dict[str, list[dict[str, Any]]] = {}
        for symbol, operaciones in self.ultimas_operaciones.items():
            if not isinstance(symbol, str) or not isinstance(operaciones, list):
                continue
            registros: list[dict[str, Any]] = []
            for item in operaciones[-self.max_operaciones :]:
                if isinstance(item, dict):
                    registros.append({str(k): v for k, v in item.items()})
            if registros:
                datos[symbol] = registros
        return {
            "ultimas_operaciones": datos,
            "fecha_actual": self.fecha_actual.isoformat(),
        }

    def _restore_critical_state(self, payload: Mapping[str, Any]) -> None:
        if not isinstance(payload, Mapping):
            return
        ultimas = payload.get("ultimas_operaciones")
        if isinstance(ultimas, Mapping):
            restaurado: dict[str, list[dict[str, Any]]] = {}
            for symbol, operaciones in ultimas.items():
                if not isinstance(symbol, str) or not isinstance(operaciones, list):
                    continue
                registros: list[dict[str, Any]] = []
                for item in operaciones[: self.max_operaciones]:
                    if isinstance(item, Mapping):
                        registros.append({str(k): v for k, v in item.items()})
                if registros:
                    restaurado[symbol] = registros
            if restaurado:
                self.ultimas_operaciones.update(restaurado)
        fecha = payload.get("fecha_actual")
        if isinstance(fecha, str):
            try:
                self.fecha_actual = datetime.fromisoformat(fecha).date()
            except ValueError:
                pass

    def generar_informe(self, fecha):
        archivo = os.path.join(self.carpeta, f'{fecha}.csv')
        with report_generation_timer() as set_status:
            if not os.path.exists(archivo):
                set_status('missing')
                return
            try:
                df = leer_csv_seguro(archivo)
            except Exception:
                REPORT_IO_ERRORS_TOTAL.labels(operation="report_daily_read").inc()
                raise
            if df.empty:
                set_status('empty')
                return
        df = self._aplicar_esquema_operaciones(df)
        ganancia_total = df['retorno_total'].sum()
        winrate = (df['retorno_total'] > 0).mean() * 100
        curva = df['retorno_total'].cumsum()
        drawdown = (curva - curva.cummax()).min()
        total_ops = len(df)
        ganadas = (df['retorno_total'] > 0).sum()
        perdidas = total_ops - ganadas
        beneficio_prom = df[df['retorno_total'] > 0]['retorno_total'].mean()
        perdida_prom = df[df['retorno_total'] <= 0]['retorno_total'].mean()
        mejor = df['retorno_total'].max()
        peor = df['retorno_total'].min()
        estrategias_usadas = {}
        if 'estrategias' in df.columns:
            col = df['estrategias']
        else:
            col = df.get('estrategias_activas')
        if col is not None:
            for val in col.dropna():
                if isinstance(val, str):
                    try:
                        data = json.loads(val.replace("'", '"'))
                        if isinstance(data, dict):
                            val = list(data.keys())
                    except Exception:
                        val = [val]
                if isinstance(val, dict):
                    val = list(val.keys())
                if isinstance(val, list):
                    for e in val:
                        estrategias_usadas[e] = estrategias_usadas.get(e, 0
                            ) + 1
        mas_usadas = ','.join(sorted(estrategias_usadas, key=
            estrategias_usadas.get, reverse=True)[:3])
        puntaje_prom = df.get('puntaje_entrada', pd.Series()).mean()
        capital_inicial = 0.0
        capital_final = 0.0
        if 'capital_inicial' in df.columns:
            capital_inicial = df.groupby('symbol')['capital_inicial'].first(
                ).sum()
        if 'capital_final' in df.columns:
            capital_final = df.groupby('symbol')['capital_final'].last().sum()
        latencia_prom = self._calcular_promedio(df, self._LATENCY_COLUMNS)
        slippage_prom = self._calcular_promedio(df, self._SLIPPAGE_COLUMNS)
        exposicion_max = self._calcular_exposicion_maxima(df)
        ratio_rr = self._calcular_ratio_riesgo_recompensa(df)
        resumen = {'fecha': fecha, 'operaciones': total_ops, 'ganadas':
            ganadas, 'perdidas': perdidas, 'beneficio_promedio': round(
            beneficio_prom or 0, 6), 'perdida_promedio': round(perdida_prom or
            0, 6), 'mejor': round(mejor or 0, 6), 'peor': round(peor or 0, 
            6), 'estrategias_top': mas_usadas, 'puntaje_promedio': round(
            puntaje_prom or 0, 4), 'capital_inicial': round(capital_inicial,
            2), 'capital_final': round(capital_final, 2),
            'latencia_promedio_ms': round(latencia_prom, 2),
            'slippage_promedio': round(slippage_prom, 6),
            'exposicion_maxima': round(exposicion_max, 2),
            'ratio_riesgo_recompensa': round(ratio_rr, 4)}
        self.log.info(
            f'📊 Informe {fecha}: Ganancia={ganancia_total:.2f}, Winrate={winrate:.2f}%, Drawdown={drawdown:.4f}'
            )
        resumen_path_txt = os.path.join(self.carpeta, f'{fecha}_resumen.txt')
        resumen_path_csv = os.path.join(self.carpeta, f'{fecha}_resumen.csv')
        def _write_resumen_txt() -> None:
            with open(resumen_path_txt, 'w') as f:
                for k, v in resumen.items():
                    f.write(f'{k}: {v}\n')

        try:
            observe_disk_write('report_resumen_txt', resumen_path_txt, _write_resumen_txt)
        except Exception:
            REPORT_IO_ERRORS_TOTAL.labels(operation="report_resumen_txt").inc()
            raise

        try:
            observe_disk_write(
                'report_resumen_csv',
                resumen_path_csv,
                lambda: pd.DataFrame([resumen]).to_csv(resumen_path_csv, index=False),
            )
        except Exception:
            REPORT_IO_ERRORS_TOTAL.labels(operation="report_resumen_csv").inc()
            raise
        self._guardar_pdf(df, fecha, ganancia_total, winrate, drawdown)

    def _guardar_pdf(self, df, fecha, ganancia, winrate, drawdown):
        pdf_path = os.path.join(self.carpeta, f'{fecha}.pdf')
        def _write_pdf() -> None:
            with PdfPages(pdf_path) as pdf:
                fig, ax = plt.subplots()
                df['retorno_total'].cumsum().plot(ax=ax)
                ax.set_title('Retorno acumulado')
                ax.set_xlabel('Operaciones')
                ax.set_ylabel('Beneficio')
                pdf.savefig(fig)
                plt.close(fig)
                fig, ax = plt.subplots(figsize=(8, 2))
                ax.axis('off')
                texto = f"""Ganancia total: {ganancia:.2f}
                            Winrate: {winrate:.2f}%
                            Drawdown: {drawdown:.4f}"""
                ax.text(0.01, 0.8, texto, fontsize=12)
                pdf.savefig(fig)
                plt.close(fig)

        try:
            observe_disk_write('report_pdf', pdf_path, _write_pdf)
        except Exception:
            REPORT_IO_ERRORS_TOTAL.labels(operation="report_pdf").inc()
            raise
        self.log.info(f'🗒️ Reporte PDF guardado en {pdf_path}')


    def _aplicar_esquema_operaciones(self, df: pd.DataFrame) -> pd.DataFrame:
        """Valida y normaliza columnas obligatorias para cálculos de métricas."""
        sanitized = df.copy()
        for columna, default in self._COLUMN_SCHEMA.items():
            if columna not in sanitized.columns:
                self.log.warning(
                    '⚠️ Columna %s ausente en reporte diario; se usará valor por defecto.',
                    columna,
                )
                sanitized[columna] = default
            sanitized[columna] = pd.to_numeric(sanitized[columna], errors='coerce').fillna(default)
        for columna, default in self._OPTIONAL_NUMERIC.items():
            if columna in sanitized.columns:
                sanitized[columna] = pd.to_numeric(sanitized[columna], errors='coerce').fillna(default)
        return sanitized

    def _columna_disponible(self, df: pd.DataFrame, opciones: Iterable[str]) -> str | None:
        for opcion in opciones:
            if opcion in df.columns:
                return opcion
        return None

    def _calcular_promedio(self, df: pd.DataFrame, columnas: Iterable[str]) -> float:
        columna = self._columna_disponible(df, columnas)
        if not columna:
            return 0.0
        serie = pd.to_numeric(df[columna], errors='coerce').replace([np.inf, -np.inf], np.nan).dropna()
        if serie.empty:
            return 0.0
        return float(serie.mean())

    def _calcular_exposicion_maxima(self, df: pd.DataFrame) -> float:
        columna_exposicion = self._columna_disponible(df, self._EXPOSURE_COLUMNS)
        if columna_exposicion:
            serie = pd.to_numeric(df[columna_exposicion], errors='coerce').replace([np.inf, -np.inf], np.nan).dropna()
            if serie.empty:
                return 0.0
            return float(serie.max())
        cantidad = pd.to_numeric(df.get('cantidad', pd.Series(dtype=float)), errors='coerce').fillna(0.0)
        if cantidad.empty:
            return 0.0
        precios_candidatos = []
        for col in ('precio_entrada', 'precio_cierre', 'max_price'):
            if col in df.columns:
                precios_candidatos.append(pd.to_numeric(df[col], errors='coerce'))
        if not precios_candidatos:
            return 0.0
        precios = pd.concat(precios_candidatos, axis=1).replace([np.inf, -np.inf], np.nan)
        max_precio = precios.max(axis=1, skipna=True).fillna(0.0)
        exposicion = (cantidad * max_precio).replace([np.inf, -np.inf], np.nan).fillna(0.0)
        if exposicion.empty:
            return 0.0
        return float(exposicion.max())

    def _calcular_ratio_riesgo_recompensa(self, df: pd.DataFrame) -> float:
        if not {'precio_entrada', 'stop_loss', 'take_profit'}.issubset(df.columns):
            return 0.0
        precio_entrada = pd.to_numeric(df['precio_entrada'], errors='coerce')
        stop_loss = pd.to_numeric(df['stop_loss'], errors='coerce')
        take_profit = pd.to_numeric(df['take_profit'], errors='coerce')
        risk = (precio_entrada - stop_loss).abs().replace([np.inf, -np.inf], np.nan)
        reward = (take_profit - precio_entrada).abs().replace([np.inf, -np.inf], np.nan)
        mask = (risk > 1e-12) & reward.notna()
        ratios = (reward / risk).where(mask).replace([np.inf, -np.inf], np.nan).dropna()
        if ratios.empty:
            return 0.0
        return float(ratios.mean())


reporter_diario = ReporterDiario()
