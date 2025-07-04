import os
import re

# directorio base del proyecto
base_dir = "."

# palabras clave a buscar
patrones = [
    r"\btrader\.cerrar\(",
    r"\bself\.cerrar\(",
    r"\btask\.cancel\(",
    r"\bloop\.stop\(",
    r"\bloop\.close\(",
    r"asyncio\.get_event_loop\(\)\.stop\(",
]

# compila regex
regexps = [re.compile(p) for p in patrones]

# carpetas a excluir
carpetas_excluidas = {"venv", ".git", "__pycache__"}

for root, dirs, files in os.walk(base_dir):
    # elimina carpetas excluidas
    dirs[:] = [d for d in dirs if d not in carpetas_excluidas]

    for f in files:
        if f.endswith(".py"):
            path = os.path.join(root, f)
            try:
                with open(path, encoding="utf-8") as file:
                    for i, line in enumerate(file, 1):
                        for r in regexps:
                            if r.search(line):
                                print(f"{path}:{i}: {line.strip()}")
            except Exception as e:
                print(f"⚠️ No pude leer {path}: {e}")
