import os
import ast

# reglas que podemos modularizar luego
def detectar_try_sin_except(tree):
    resultados = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Try):
            if not node.handlers:
                resultados.append({
                    "linea": node.lineno,
                    "descripcion": "bloque try sin except detectado",
                    "prioridad": "media"
                })
    return resultados

def detectar_while_true_sin_break(tree):
    resultados = []
    for node in ast.walk(tree):
        if isinstance(node, ast.While):
            if isinstance(node.test, ast.Constant) and node.test.value is True:
                tiene_break = any(
                    isinstance(subnode, ast.Break) or isinstance(subnode, ast.Return)
                    for subnode in ast.walk(node)
                )
                if not tiene_break:
                    resultados.append({
                        "linea": node.lineno,
                        "descripcion": "while True sin break ni return",
                        "prioridad": "alta"
                    })
    return resultados

def detectar_run_in_executor_sin_timeout(tree):
    resultados = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            if hasattr(node.func, "attr") and node.func.attr == "run_in_executor":
                resultados.append({
                    "linea": node.lineno,
                    "descripcion": "run_in_executor sin timeout explícito detectado (revisa el control manual)",
                    "prioridad": "media"
                })
    return resultados

def recorrer_archivos(path_base, excluir=("venv", "__pycache__")):
    """
    Devuelve lista de archivos .py en el proyecto (excepto venv y similares).
    """
    lista = []
    for root, dirs, files in os.walk(path_base):
        dirs[:] = [d for d in dirs if d not in excluir]
        for file in files:
            if file.endswith(".py"):
                lista.append(os.path.join(root, file))
    return lista

def analizar_archivo(path_archivo):
    with open(path_archivo, "r", encoding="utf-8") as f:
        try:
            codigo = f.read()
            tree = ast.parse(codigo)
        except Exception as e:
            print(f"⚠️ Error al parsear {path_archivo}: {e}")
            return []
    resultados = []
    resultados += detectar_try_sin_except(tree)
    resultados += detectar_while_true_sin_break(tree)
    resultados += detectar_run_in_executor_sin_timeout(tree)
    return resultados

def main():
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    archivos = recorrer_archivos(base_dir)
    print(f"🔎 Analizando {len(archivos)} archivos...\n")
    total_alertas = 0
    for archivo in archivos:
        resultado = analizar_archivo(archivo)
        for r in resultado:
            prioridad = r.get("prioridad", "media")
            print(f"📌 {archivo}:{r['linea']} [{prioridad}] {r['descripcion']}")
            total_alertas += 1
    print(f"\n✅ Auditoría completa. Total alertas: {total_alertas}")

if __name__ == "__main__":
    main()
