import os
import pandas as pd
from core.orders.order_model import Orden
from core.orders import real_orders

PARQUET_PATH = os.path.join("ordenes_reales", "ordenes_reales.parquet")


def migrate():
    if not os.path.exists(PARQUET_PATH):
        print("No hay archivo Parquet para migrar.")
        return
    try:
        df = pd.read_parquet(PARQUET_PATH)
    except Exception as e:
        print(f"Error leyendo archivo Parquet: {e}")
        return

    count = 0
    for _, row in df.iterrows():
        orden = Orden.from_dict(row.to_dict())
        real_orders.actualizar_orden(orden.symbol, orden)
        count += 1

    backup = PARQUET_PATH + ".bak"
    os.rename(PARQUET_PATH, backup)
    print(f"Migradas {count} órdenes. Archivo original movido a {backup}")


if __name__ == "__main__":
    migrate()