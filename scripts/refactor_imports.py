from rope.base.project import Project
from rope.base.libutils import path_to_resource
import os
ruta_proyecto = os.path.abspath('.')
proyecto = Project(ruta_proyecto)
print("🚀 Iniciando refactorización de imports ''...")
EXCLUIR_DIRS = {'venv', '.git', '__pycache__', '.pytest_cache', 'node_modules'}
for carpeta, _, archivos in os.walk(ruta_proyecto):
    if any(excluir in carpeta for excluir in EXCLUIR_DIRS):
        continue
    for archivo in archivos:
        if archivo.endswith('.py'):
            ruta_archivo = os.path.join(carpeta, archivo)
            try:
                recurso = path_to_resource(proyecto, ruta_archivo)
                contenido = recurso.read()
                if '' in contenido:
                    nuevo_contenido = contenido.replace('', '')
                    recurso.write(nuevo_contenido)
                    print(f'✅ Refactorizado: {ruta_archivo}')
            except Exception as e:
                print(f'⚠️ Error en {ruta_archivo}: {e}')
proyecto.close()
print("""
🎉 Refactorización completada sin uso de .decode() innecesario.""")
