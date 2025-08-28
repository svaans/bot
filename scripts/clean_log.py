import os
import re

ROOT_DIR = "."

# Coincidencias amplias:

def limpiar_archivo(filepath: str):
    with open(filepath, "r", encoding="utf-8") as f:
        lineas = f.readlines()

    nuevas = []
    borradas = 0

    for linea in lineas:
        if PATRON_ENTRANDO.search(linea):
            borradas += 1
            continue
        nuevas.append(linea)

    if borradas:
        with open(filepath, "w", encoding="utf-8") as f:
            f.writelines(nuevas)
        print(f"🧹 {filepath} → {borradas} línea(s) eliminadas")

def recorrer(root: str):
    for subdir, _, files in os.walk(root):
        for file in files:
            if file.endswith(".py"):
                limpiar_archivo(os.path.join(subdir, file))

if __name__ == "__main__":
    recorrer(ROOT_DIR)



