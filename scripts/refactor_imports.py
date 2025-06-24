from rope.base.project import Project
from rope.base.libutils import path_to_resource
import os

# Ruta base del proyecto (asume que estás en la raíz)
ruta_proyecto = os.path.abspath(".")
proyecto = Project(ruta_proyecto)

print("🚀 Iniciando refactorización de imports ''...")

# Directorios que deben ser ignorados
EXCLUIR_DIRS = {"venv", ".git", "__pycache__", ".pytest_cache", "node_modules"}

for carpeta, _, archivos in os.walk(ruta_proyecto):
    # Salta cualquier carpeta que contenga uno de los excluidos
    if any(excluir in carpeta for excluir in EXCLUIR_DIRS):
        continue

    for archivo in archivos:
        if archivo.endswith(".py"):
            ruta_archivo = os.path.join(carpeta, archivo)
            try:
                recurso = path_to_resource(proyecto, ruta_archivo)
                contenido = recurso.read()  # ← esto ya devuelve un `str`
                if "" in contenido:
                    nuevo_contenido = contenido.replace("", "")
                    recurso.write(nuevo_contenido)  # ← escribimos str directamente
                    print(f"✅ Refactorizado: {ruta_archivo}")
            except Exception as e:
                print(f"⚠️ Error en {ruta_archivo}: {e}")

proyecto.close()
print("\n🎉 Refactorización completada sin uso de .decode() innecesario.")

