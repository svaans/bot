#!/usr/bin/env bash
# Script de build para Render.com
# El paquete 'ta' falla con setuptools>=68 (AttributeError: install_layout).
# Solución: usar distutils de stdlib en lugar del parcheado de setuptools.
set -e

echo "==> Instalando dependencias (con fix para ta==0.11.0)..."
SETUPTOOLS_USE_DISTUTILS=stdlib pip install -r requirements.txt

echo "==> Build completado."
