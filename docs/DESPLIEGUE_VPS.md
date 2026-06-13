# Despliegue en VPS (guía pensada para hacerse desde el móvil)

Objetivo: el bot corriendo 24/7 en un VPS (Hetzner CX23 o similar, Ubuntu
24.04), controlado desde el móvil con Termius (SSH), Telegram
(notificaciones) y el dashboard web.

## 1. Crear el servidor (web de Hetzner)

- Producto: **Cloud → CX23** (2 vCPU x86, 4 GB RAM) — suficiente de sobra.
- Imagen: **Ubuntu 24.04**.
- Ubicación: cualquiera de Europa (el bot opera velas de 5m; la latencia no
  es crítica).
- SSH key: si no tienes, elige contraseña por email (luego Termius la
  recuerda).

## 2. Conectar desde el móvil

Instala **Termius** (iOS/Android), crea un host con la IP del VPS, usuario
`root` y la contraseña/clave. Abre la terminal.

## 3. Clonar el repo

El repo es privado: crea un token en GitHub (móvil o web):
*Settings → Developer settings → Fine-grained tokens → Generate new token*,
con acceso solo a `svaans/bot` y permiso **Contents: Read-only**.

```bash
git clone https://TU_TOKEN@github.com/svaans/bot.git /opt/bot
```

## 4. Instalar

```bash
bash /opt/bot/deploy/install.sh
```

El script instala dependencias, crea el usuario de servicio `botuser`, el
venv, el servicio systemd `tradingbot` (arranque automático y reinicio si
se cae) y activa el firewall dejando solo SSH abierto.

## 5. Claves y arranque

```bash
nano /opt/bot/config/claves.env     # rellenar Binance/Telegram; MODO_REAL=False
systemctl start tradingbot
journalctl -u tradingbot -f         # logs en vivo (Ctrl+C para salir)
```

Con Telegram configurado, las operaciones llegan al móvil sin nada más.

## 6. Dashboard desde el móvil

El dashboard NO tiene autenticación, así que no se expone a internet
(escucha solo en `127.0.0.1`, y el firewall bloquea el puerto igualmente).
Dos opciones:

**Opción A — Tailscale (recomendada, gratis):**

```bash
curl -fsSL https://tailscale.com/install.sh | sh
tailscale up        # abre el enlace que imprime y autoriza
tailscale ip -4     # apunta esta IP (100.x.y.z)
```

Edita el servicio para escuchar en esa IP privada:

```bash
systemctl edit tradingbot
# añade:
# [Service]
# Environment=DASHBOARD_HOST=0.0.0.0
systemctl restart tradingbot
```

Instala Tailscale también en el móvil (misma cuenta) y abre
`http://100.x.y.z:8080`. Solo tus dispositivos pueden llegar a esa IP.

**Opción B — túnel SSH en Termius:** crea un Port Forwarding
(local 8080 → destino 127.0.0.1:8080) y abre `http://localhost:8080`
en el navegador del móvil mientras el túnel está activo.

## 7. Operación diaria

| Acción | Comando |
|---|---|
| Ver logs | `journalctl -u tradingbot -f` |
| Reiniciar | `systemctl restart tradingbot` |
| Parar | `systemctl stop tradingbot` |
| Actualizar a lo último de main | `bash /opt/bot/deploy/update.sh` |
| Estado del servicio | `systemctl status tradingbot` |

## 8. Pasar a modo real (cuando toque)

1. Verifica en Binance que la API key solo tiene *Spot Trading* (sin
   retiros) y, ya con el VPS, restringe la key a la IP pública del VPS.
2. `nano /opt/bot/config/claves.env` → `MODO_REAL=True`
3. `systemctl restart tradingbot`
