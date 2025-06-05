import os
from datetime import datetime
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
from core.logger import configurar_logger


class ReporterDiario:
    def __init__(self, carpeta="reportes_diarios"):
        self.carpeta = carpeta
        os.makedirs(self.carpeta, exist_ok=True)
        self.fecha_actual = datetime.utcnow().date()
        self.log = configurar_logger("reporte")

    def registrar_operacion(self, info: dict):
        fecha = datetime.utcnow().date()
        archivo = os.path.join(self.carpeta, f"{fecha}.csv")
        df = pd.DataFrame([info])
        if os.path.exists(archivo):
            df.to_csv(archivo, mode="a", header=False, index=False)
        else:
            df.to_csv(archivo, index=False)
        self.log.info(f"📝 Operación registrada para reporte {fecha}")
        if fecha != self.fecha_actual:
            self.generar_informe(self.fecha_actual)
            self.fecha_actual = fecha

    def generar_informe(self, fecha):
        archivo = os.path.join(self.carpeta, f"{fecha}.csv")
        if not os.path.exists(archivo):
            return
        df = pd.read_csv(archivo)
        if df.empty:
            return
        ganancia_total = df["retorno_total"].sum()
        winrate = (df["retorno_total"] > 0).mean() * 100
        curva = df["retorno_total"].cumsum()
        drawdown = (curva - curva.cummax()).min()
        self.log.info(
            f"📊 Informe {fecha}: Ganancia={ganancia_total:.2f}, Winrate={winrate:.2f}%, Drawdown={drawdown:.4f}"
        )
        self._guardar_pdf(df, fecha, ganancia_total, winrate, drawdown)

    def _guardar_pdf(self, df, fecha, ganancia, winrate, drawdown):
        pdf_path = os.path.join(self.carpeta, f"{fecha}.pdf")
        with PdfPages(pdf_path) as pdf:
            fig, ax = plt.subplots()
            df["retorno_total"].cumsum().plot(ax=ax)
            ax.set_title("Retorno acumulado")
            ax.set_xlabel("Operaciones")
            ax.set_ylabel("Beneficio")
            pdf.savefig(fig)
            plt.close(fig)

            fig, ax = plt.subplots(figsize=(8, 2))
            ax.axis("off")
            texto = (
                f"Ganancia total: {ganancia:.2f}\n"
                f"Winrate: {winrate:.2f}%\n"
                f"Drawdown: {drawdown:.4f}"
            )
            ax.text(0.01, 0.8, texto, fontsize=12)
            pdf.savefig(fig)
            plt.close(fig)
        self.log.info(f"🗒️ Reporte PDF guardado en {pdf_path}")


reporter_diario = ReporterDiario()