#!/usr/bin/env bash
# Actualiza el bot a lo último de main y lo reinicia. Ejecutar como root:
#   bash /opt/bot/deploy/update.sh
set -euo pipefail
APP_DIR=/opt/bot

echo "==> git pull"
sudo -u botuser git -C "$APP_DIR" pull origin main

echo "==> dependencias"
sudo -u botuser "$APP_DIR/venv/bin/pip" install -r "$APP_DIR/requirements.txt" -q

echo "==> reinicio del servicio"
systemctl restart tradingbot
sleep 3
systemctl --no-pager -l status tradingbot | head -12
