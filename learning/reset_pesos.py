import os
import json
from datetime import datetime
from pathlib import Path
from core.strategies.pesos import gestor_pesos

BASE_DIR = Path(__file__).resolve().parents[1]
RUTA_BASE = BASE_DIR / "config" / "estrategias_pesos_base.json"
RUTA_ACTUAL = BASE_DIR / "config" / "estrategias_pesos.json"
RUTA_CONTROL = BASE_DIR / "config" / "reset_pesos_fecha.txt"

def resetear_pesos_diarios_si_corresponde():
    hoy = datetime.utcnow().strftime("%Y-%m-%d")

    # Verificar si ya se hizo hoy
    if os.path.exists(RUTA_CONTROL):
        with open(RUTA_CONTROL, "r") as f:
            ultima_fecha = f.read().strip()
        if ultima_fecha == hoy:
            return  # Ya se hizo el reset hoy

    # Leer pesos base
    with open(RUTA_BASE, "r", encoding="utf-8") as f:
        pesos_base = json.load(f)

    # Guardar backup del actual
    if os.path.exists(RUTA_ACTUAL):
        with open(RUTA_ACTUAL, "r", encoding="utf-8") as f:
            pesos_actuales = json.load(f)
        backup_name = BASE_DIR / f"config/estrategias_pesos_BACKUP_{hoy}.json"
        with open(backup_name, "w", encoding="utf-8") as f:
            json.dump(pesos_actuales, f, indent=2)

    # Aplicar reset
    gestor_pesos.guardar(pesos_base)
    print("🔁 Pesos reiniciados desde base.")

    # Actualizar fecha de control
    with open(RUTA_CONTROL, "w") as f:
        f.write(hoy)
