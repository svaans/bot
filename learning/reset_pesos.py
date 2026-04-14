import json
import shutil
from datetime import datetime, timezone
from pathlib import Path

UTC = timezone.utc
from core.strategies.pesos import gestor_pesos
from core.strategies.pesos_governance import EntryWeightSource, persist_entry_weights

_ROOT = Path(__file__).resolve().parents[1]
_CFG = _ROOT / "config"
RUTA_BASE = _CFG / "estrategias_pesos_base.json"
RUTA_CONTROL = _CFG / "reset_pesos_fecha.txt"


def resetear_pesos_diarios_si_corresponde():
    hoy = datetime.now(UTC).strftime('%Y-%m-%d')
    if RUTA_CONTROL.exists():
        with open(RUTA_CONTROL, 'r', encoding='utf-8') as f:
            ultima_fecha = f.read().strip()
        if ultima_fecha == hoy:
            return
    with open(RUTA_BASE, 'r', encoding='utf-8') as f:
        pesos_base = json.load(f)
    ruta_viva = gestor_pesos.ruta
    if ruta_viva.exists():
        backup_name = ruta_viva.parent / f"{ruta_viva.stem}_BACKUP_{hoy}{ruta_viva.suffix}"
        shutil.copy2(ruta_viva, backup_name)
    persist_entry_weights(
        gestor_pesos,
        pesos_base,
        source=EntryWeightSource.RESET_DIARIO,
        detail=str(RUTA_BASE),
    )
    print('🔁 Pesos reiniciados desde base.')
    with open(RUTA_CONTROL, 'w', encoding='utf-8') as f:
        f.write(hoy)
