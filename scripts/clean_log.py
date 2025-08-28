import os
import re

# Carpeta raíz de tu proyecto
ROOT_DIR = "."

# Regex para detectar logs de entrada tanto en debug como info
# Ej: log.debug('➡️ Entrando en ejecutar()') o log.info("➡️ Entrando en procesar()")
LOG_REGEX = re.compile(
    r"^\s*log\.(debug|info)\(\s*['\"]➡️ Entrando en .*['\"]\s*\)\s*$"
)

def limpiar_logs_en_archivo(filepath: str):
    with open(filepath, "r", encoding="utf-8") as f:
        lineas = f.readlines()

    nuevas_lineas = []
    cambios = 0

    for linea in lineas:
        if LOG_REGEX.match(linea):
            cambios += 1
            continue  # eliminar esa línea
        nuevas_lineas.append(linea)

    if cambios > 0:
        with open(filepath, "w", encoding="utf-8") as f:
            f.writelines(nuevas_lineas)
        print(f"🧹 {filepath} → {cambios} log(s) eliminados")

def recorrer_directorio(root_dir: str):
    for subdir, _, files in os.walk(root_dir):
        for file in files:
            if file.endswith(".py"):
                filepath = os.path.join(subdir, file)
                limpiar_logs_en_archivo(filepath)

if __name__ == "__main__":
    recorrer_directorio(ROOT_DIR)

