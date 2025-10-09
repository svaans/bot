import os
import json
from datetime import UTC, datetime
RUTA_BASE = 'config/configuraciones_base.json'
RUTA_ACTUAL = 'config/configuraciones_optimas.json'
RUTA_CONTROL = 'config/reset_config_fecha.txt'


def resetear_configuracion_diaria_si_corresponde():
    hoy = datetime.now(UTC).strftime('%Y-%m-%d')
    if os.path.exists(RUTA_CONTROL):
        with open(RUTA_CONTROL, 'r') as f:
            ultima_fecha = f.read().strip()
        if ultima_fecha == hoy:
            return
    with open(RUTA_BASE, 'r', encoding='utf-8') as f:
        config_base = json.load(f)
    if os.path.exists(RUTA_ACTUAL):
        with open(RUTA_ACTUAL, 'r', encoding='utf-8') as f:
            config_actual = json.load(f)
        backup_name = f'config/configuraciones_optimas_BACKUP_{hoy}.json'
        with open(backup_name, 'w', encoding='utf-8') as f:
            json.dump(config_actual, f, indent=2)
    with open(RUTA_ACTUAL, 'w', encoding='utf-8') as f:
        json.dump(config_base, f, indent=4)
    print('🔁 Configuración reiniciada desde base.')
    with open(RUTA_CONTROL, 'w') as f:
        f.write(hoy)
