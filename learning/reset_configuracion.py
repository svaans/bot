import json
from datetime import datetime, timezone
from pathlib import Path

UTC = timezone.utc
_ROOT = Path(__file__).resolve().parents[1]
_CFG = _ROOT / "config"
RUTA_BASE = _CFG / "configuraciones_base.json"
RUTA_ACTUAL = _CFG / "configuraciones_optimas.json"
RUTA_CONTROL = _CFG / "reset_config_fecha.txt"


def resetear_configuracion_diaria_si_corresponde():
    hoy = datetime.now(UTC).strftime('%Y-%m-%d')
    if RUTA_CONTROL.exists():
        with open(RUTA_CONTROL, 'r', encoding='utf-8') as f:
            ultima_fecha = f.read().strip()
        if ultima_fecha == hoy:
            return
    with open(RUTA_BASE, 'r', encoding='utf-8') as f:
        config_base = json.load(f)
    if RUTA_ACTUAL.exists():
        with open(RUTA_ACTUAL, 'r', encoding='utf-8') as f:
            config_actual = json.load(f)
        backup_name = _CFG / f"configuraciones_optimas_BACKUP_{hoy}.json"
        with open(backup_name, 'w', encoding='utf-8') as f:
            json.dump(config_actual, f, indent=2)
    with open(RUTA_ACTUAL, 'w', encoding='utf-8') as f:
        json.dump(config_base, f, indent=4)
    print('🔁 Configuración reiniciada desde base.')
    with open(RUTA_CONTROL, 'w', encoding='utf-8') as f:
        f.write(hoy)
