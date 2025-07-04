# super_refactor.py
import ast
import os

EXCLUDE_DIRS = {"venv", "__pycache__", ".git"}

def is_while_true_without_break(node: ast.While) -> bool:
    """
    Detecta while True sin break ni return
    """
    if isinstance(node.test, ast.Constant) and node.test.value is True:
        for n in ast.walk(node):
            if isinstance(n, (ast.Break, ast.Return)):
                return False
        return True
    return False

def is_try_without_except(node: ast.Try) -> bool:
    """
    Detecta try sin except
    """
    return len(node.handlers) == 0

def is_run_in_executor_without_timeout(node: ast.Call, ancestors) -> bool:
    """
    Detecta run_in_executor sin asyncio.wait_for envolvente
    """
    if isinstance(node.func, ast.Attribute) and node.func.attr == "run_in_executor":
        for parent in ancestors:
            if isinstance(parent, ast.Call):
                if isinstance(parent.func, ast.Attribute):
                    if parent.func.attr == "wait_for":
                        return False
        return True
    return False

def analyze_file(filepath):
    try:
        with open(filepath, encoding="utf-8") as f:
            source = f.read()
        tree = ast.parse(source)
    except Exception as e:
        print(f"⚠️ No se pudo analizar {filepath}: {e}")
        return []

    findings = []

    # recorrer nodos con ancestros
    ancestors = []

    for node in ast.walk(tree):
        ancestors.append(node)
        if isinstance(node, ast.While):
            if is_while_true_without_break(node):
                findings.append({
                    "archivo": filepath,
                    "linea": node.lineno,
                    "tipo": "while True",
                    "prioridad": "alta",
                    "razon": "bucle infinito sin break/return"
                })
        if isinstance(node, ast.Try):
            if is_try_without_except(node):
                findings.append({
                    "archivo": filepath,
                    "linea": node.lineno,
                    "tipo": "try",
                    "prioridad": "media",
                    "razon": "try sin except"
                })
        if isinstance(node, ast.Call):
            if is_run_in_executor_without_timeout(node, ancestors):
                findings.append({
                    "archivo": filepath,
                    "linea": node.lineno,
                    "tipo": "run_in_executor",
                    "prioridad": "media",
                    "razon": "run_in_executor sin wait_for envolvente"
                })
    return findings

def scan_project(root_dir="."):
    all_findings = []
    for dirpath, dirnames, filenames in os.walk(root_dir):
        # excluir carpetas
        dirnames[:] = [d for d in dirnames if d not in EXCLUDE_DIRS]
        for file in filenames:
            if file.endswith(".py"):
                fullpath = os.path.join(dirpath, file)
                findings = analyze_file(fullpath)
                all_findings.extend(findings)
    return all_findings

if __name__ == "__main__":
    print("🔎 Escaneando proyecto (análisis semántico AST, no se parchea)...")
    results = scan_project()
    if results:
        print(f"\n=== Reporte de puntos críticos detectados ===")
        for item in results:
            print(f"📌 {item['archivo']}:{item['linea']} [{item['prioridad']}] {item['razon']}")
    else:
        print("✅ Sin problemas detectados a nivel de sintaxis estructural.")


