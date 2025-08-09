import os
import json
import atexit
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
from core.utils.utils import configurar_logger
from core.utils.utils import leer_csv_seguro

log = configurar_logger('reporte_diario')

_executor = ProcessPoolExecutor(max_workers=1)
atexit.register(_executor.shutdown)


class ReporterDiario:

    def __init__(self, carpeta='reportes_diarios', max_operaciones=1000):
        log.info('➡️ Entrando en __init__()')
        self.carpeta = carpeta
        os.makedirs(self.carpeta, exist_ok=True)
        self.fecha_actual = datetime.utcnow().date()
        self.log = configurar_logger('reporte')
        self.estadisticas_archivo = os.path.join(
            self.carpeta, 'estadisticas.csv'
        )
        self._cargar_estadisticas()
        self.ultimas_operaciones = {}
        self.max_operaciones = max_operaciones

    def _cargar_estadisticas(self):
        log.info('➡️ Entrando en _cargar_estadisticas()')
        columnas = ['symbol', 'operaciones', 'wins', 'retorno_acumulado',
            'max_equity', 'drawdown', 'buy_hold_start', 'last_price']
        if os.path.exists(self.estadisticas_archivo):
            try:
                self.estadisticas = leer_csv_seguro(self.
                    estadisticas_archivo, expected_cols=len(columnas))
            except Exception:
                self.estadisticas = pd.DataFrame(columns=columnas)
        else:
            self.estadisticas = pd.DataFrame(columns=columnas)

    def _guardar_estadisticas(self):
        log.info('➡️ Entrando en _guardar_estadisticas()')
        if self.estadisticas.empty:
            return
        df = self.estadisticas.copy()
        df['winrate'] = df['wins'] / df['operaciones'] * 100
        df['buy_hold'] = (df['last_price'] - df['buy_hold_start']) / df[
            'buy_hold_start']
        df.to_csv(self.estadisticas_archivo, index=False)

    def _actualizar_estadisticas(self, info: dict):
        log.info('➡️ Entrando en _actualizar_estadisticas()')
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
        if self.estadisticas.loc[idx, 'buy_hold_start'] == 0:
            self.estadisticas.loc[idx, 'buy_hold_start'
                ] = entrada if entrada else cierre
        self.estadisticas.loc[idx, 'last_price'] = cierre
        self._guardar_estadisticas()

    def registrar_operacion(self, info: dict):
        log.info('➡️ Entrando en registrar_operacion()')
        fecha = datetime.utcnow().date()
        archivo = os.path.join(self.carpeta, f'{fecha}.csv')
        df = pd.DataFrame([info])
        if os.path.exists(archivo):
            df.to_csv(archivo, mode='a', header=False, index=False)
        else:
            df.to_csv(archivo, index=False)
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
                self.log.exception('Error al generar informe en proceso separado')
            self.fecha_actual = fecha

    def generar_informe(self, fecha):
        log.info('➡️ Entrando en generar_informe()')
        archivo = os.path.join(self.carpeta, f'{fecha}.csv')
        if not os.path.exists(archivo):
            return
        df = leer_csv_seguro(archivo, expected_cols=20)
        if df.empty:
            return
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
        resumen = {'fecha': fecha, 'operaciones': total_ops, 'ganadas':
            ganadas, 'perdidas': perdidas, 'beneficio_promedio': round(
            beneficio_prom or 0, 6), 'perdida_promedio': round(perdida_prom or
            0, 6), 'mejor': round(mejor or 0, 6), 'peor': round(peor or 0, 
            6), 'estrategias_top': mas_usadas, 'puntaje_promedio': round(
            puntaje_prom or 0, 4), 'capital_inicial': round(capital_inicial,
            2), 'capital_final': round(capital_final, 2)}
        self.log.info(
            f'📊 Informe {fecha}: Ganancia={ganancia_total:.2f}, Winrate={winrate:.2f}%, Drawdown={drawdown:.4f}'
            )
        resumen_path_txt = os.path.join(self.carpeta, f'{fecha}_resumen.txt')
        resumen_path_csv = os.path.join(self.carpeta, f'{fecha}_resumen.csv')
        with open(resumen_path_txt, 'w') as f:
            for k, v in resumen.items():
                f.write(f'{k}: {v}\n')
        pd.DataFrame([resumen]).to_csv(resumen_path_csv, index=False)
        self._guardar_pdf(df, fecha, ganancia_total, winrate, drawdown)

    def _guardar_pdf(self, df, fecha, ganancia, winrate, drawdown):
        log.info('➡️ Entrando en _guardar_pdf()')
        pdf_path = os.path.join(self.carpeta, f'{fecha}.pdf')
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
        self.log.info(f'🗒️ Reporte PDF guardado en {pdf_path}')


reporter_diario = ReporterDiario()
