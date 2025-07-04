import os
import re
import json
EXCLUDE_DIRS = {'venv', '__pycache__', '.git', 'node_modules'}
RISK_REPORT = []


def analizar_linea(archivo, num_linea, linea):
    stripped = linea.strip()
    if re.search('while\\s+True', stripped):
        RISK_REPORT.append({'archivo': archivo, 'linea': num_linea, 'tipo':
            'while True', 'prioridad': 'alta', 'razon':
            'bucle infinito sin condición de parada'})
    if re.match('try\\s*:', stripped):
        RISK_REPORT.append({'archivo': archivo, 'linea': num_linea, 'tipo':
            'try', 'prioridad': 'media', 'razon':
            'bloque try sin verificación explícita de except'})
    if 'sleep' in stripped:
        match = re.search('sleep\\((\\d+)', stripped)
        if match:
            duracion = int(match.group(1))
            if duracion > 600:
                RISK_REPORT.append({'archivo': archivo, 'linea': num_linea,
                    'tipo': 'sleep', 'prioridad': 'media', 'razon':
                    f'suspensión demasiado larga: {duracion}s'})
    if 'run_in_executor' in stripped:
        RISK_REPORT.append({'archivo': archivo, 'linea': num_linea, 'tipo':
            'executor', 'prioridad': 'media', 'razon':
            'run_in_executor sin control de timeout explícito'})
    if re.match('async\\s+for\\s', stripped):
        RISK_REPORT.append({'archivo': archivo, 'linea': num_linea, 'tipo':
            'async for', 'prioridad': 'media', 'razon':
            'posible falta de except dentro de bucle async for'})


def escanear_directorio(path='.'):
    for root, dirs, files in os.walk(path):
        dirs[:] = [d for d in dirs if d not in EXCLUDE_DIRS]
        for file in files:
            if file.endswith('.py'):
                archivo = os.path.join(root, file)
                with open(archivo, encoding='utf-8', errors='ignore') as f:
                    for i, linea in enumerate(f, 1):
                        if not linea.strip().startswith('#'):
                            analizar_linea(archivo, i, linea)


if __name__ == '__main__':
    escanear_directorio()
    print('=== REPORTE DE POSIBLES BLOQUEOS ===')
    print(json.dumps(RISK_REPORT, indent=2, ensure_ascii=False))
