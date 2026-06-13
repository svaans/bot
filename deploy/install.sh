#!/usr/bin/env bash
# Instalación del bot en un VPS Ubuntu 22.04/24.04. Ejecutar como root:
#   bash deploy/install.sh
# Idempotente: se puede re-ejecutar sin romper nada.
set -euo pipefail

APP_DIR=/opt/bot

if [ "$(id -u)" -ne 0 ]; then
  echo "Ejecuta como root (sudo bash deploy/install.sh)"; exit 1
fi

echo "==> Paquetes del sistema"
apt-get update -y
apt-get install -y --no-install-recommends git ufw python3-venv python3-pip

PY=$(command -v python3.12 || command -v python3.11 || command -v python3)
echo "==> Python: $PY ($($PY --version))"
case "$($PY --version)" in
  *" 3.11."*|*" 3.12."*|*" 3.13."*) ;;
  *) echo "ADVERTENCIA: el proyecto está probado en Python 3.11-3.12";;
esac

echo "==> Usuario de servicio"
id -u botuser >/dev/null 2>&1 || useradd -m -r -s /bin/bash botuser

if [ ! -d "$APP_DIR/.git" ]; then
  echo "ERROR: clona primero el repo en $APP_DIR (ver docs/DESPLIEGUE_VPS.md)"; exit 1
fi
chown -R botuser:botuser "$APP_DIR"

echo "==> Entorno virtual + dependencias"
sudo -u botuser "$PY" -m venv "$APP_DIR/venv"
sudo -u botuser "$APP_DIR/venv/bin/pip" install --upgrade pip -q
sudo -u botuser "$APP_DIR/venv/bin/pip" install -r "$APP_DIR/requirements.txt" -q

echo "==> claves.env"
if [ ! -f "$APP_DIR/config/claves.env" ]; then
  sudo -u botuser cp "$APP_DIR/config/claves.env.example" "$APP_DIR/config/claves.env"
  chmod 600 "$APP_DIR/config/claves.env"
  echo "    creado desde la plantilla — RELLENA las claves antes de arrancar"
fi

echo "==> Servicio systemd"
cp "$APP_DIR/deploy/tradingbot.service" /etc/systemd/system/tradingbot.service
systemctl daemon-reload
systemctl enable tradingbot >/dev/null

echo "==> Firewall (solo SSH abierto al exterior)"
ufw allow OpenSSH >/dev/null
ufw --force enable >/dev/null

cat <<'FIN'

Instalación completa. Pasos finales:
  1) nano /opt/bot/config/claves.env      (rellenar claves)
  2) systemctl start tradingbot
  3) journalctl -u tradingbot -f          (ver logs en vivo)

Dashboard: escucha solo en localhost. Desde tu movil:
  - con Tailscale instalado:  http://IP-tailscale-del-vps:8080  (pon DASHBOARD_HOST=la IP tailscale en el service)
  - o tunel SSH en Termius:   reenviar puerto 8080 -> localhost:8080
FIN
