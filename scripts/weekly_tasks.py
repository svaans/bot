import os
import asyncio
from datetime import datetime, timedelta

from backtesting.backtest import backtest_modular
from aprendizaje.recalibrar_semana import recalibrar_pesos_semana
from aprendizaje.train_rl_policy import main as train_rl_policy

SYMBOLS = ["BTC/EUR", "ETH/EUR", "ADA/EUR", "SOL/EUR", "BNB/EUR"]
RUTA_CONTROL = "config/weekly_tasks_fecha.txt"

async def ejecutar_backtest_semana() -> None:
    fin = datetime.utcnow()
    inicio = fin - timedelta(days=7)
    await backtest_modular(SYMBOLS, fecha_inicio=inicio, fecha_fin=fin)


def tarea_semanal() -> None:
    asyncio.run(ejecutar_backtest_semana())
    recalibrar_pesos_semana()
    train_rl_policy()


def ejecutar_si_corresponde() -> None:
    """Ejecuta la tarea semanal si han pasado 7 días desde la última vez."""
    ahora = datetime.utcnow()
    if os.path.exists(RUTA_CONTROL):
        with open(RUTA_CONTROL, "r") as f:
            fecha_txt = f.read().strip()
        try:
            ultima = datetime.strptime(fecha_txt, "%Y-%m-%d")
        except ValueError:
            ultima = ahora - timedelta(days=7)
        if (ahora - ultima) < timedelta(days=7):
            return
    tarea_semanal()
    with open(RUTA_CONTROL, "w") as f:
        f.write(ahora.strftime("%Y-%m-%d"))


async def supervisor(intervalo_horas: int = 24) -> None:
    """Comprueba diariamente si corresponde ejecutar la tarea semanal."""
    while True:
        await asyncio.to_thread(ejecutar_si_corresponde)
        await asyncio.sleep(intervalo_horas * 3600)

if __name__ == "__main__":
    ejecutar_si_corresponde()