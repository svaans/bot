# Política de seguridad

Este documento describe **riesgos reales** del bot de trading en este
repositorio y cómo reportar vulnerabilidades. No sustituye a un análisis de
riesgos formal de vuestra organización.

## Alcance del software

- Bot en **Python** que se conecta principalmente a **Binance** (REST/WebSocket)
  vía `python-binance`, `ccxt` y módulos propios bajo `binance_api/` y
  `data_feed/`.
- **Modo real vs simulado**: el arranque y la configuración determinan si se
  envían órdenes reales. Revisad `main.py`, `config/` y variables de entorno
  (`MODO_REAL`, `MODO_OPERATIVO`, `BOT_ENV`, etc.) antes de operar con capital.
- **Hot-reload** puede **reiniciar el proceso** al detectar cambios en archivos
  vigilados; no es un mecanismo de aislamiento de seguridad.

## Datos sensibles y secretos

- **API keys y secretos** suelen cargarse desde `config/claves.env` mediante
  `python-dotenv`. El archivo es **texto plano** en disco: proteged permisos
  del sistema de archivos, no lo versionéis y usad `config/claves.env.example`
  como plantilla.
- **`estado/`** y JSON bajo `config/` pueden contener información sobre capital,
  posiciones u operaciones. Tratad copias de seguridad y logs como **datos
  sensibles**.
- **Logs JSON** (`core` loggers, `hot_reload`, etc.) pueden incluir símbolos,
  precios, IDs de órdenes o mensajes de error del exchange. Restricted access a
  ficheros de log y agregación (p. ej. Prometheus en `METRICS_PORT`).

## Superficie de ataque

- **Dependencias de terceros** (exchange, DNS, TLS, librerías HTTP): el bot
  depende de la disponibilidad y seguridad de esos servicios; usad versiones
  fijadas en `requirements.txt` y revisad actualizaciones en entornos aislados.
- **Persistencia local** (SQLite/Parquet según implementación): acceso al host
  compromete integridad y confidencialidad de la operación.
- **No hay backend web ni panel** en este monorepo: lo que expongáis aparte
  (dashboards, APIs) debe tener su propia política de seguridad.

## Divulgación responsable (reporting)

1. **No abráis issues públicos** con credenciales, capturas de `claves.env` ni
   payloads que revelen API keys.
2. Enviad un resumen del impacto (componente afectado, prerequisitos, gravedad
   estimada) al **canal privado** que defina el mantenedor del repositorio
   (correo interno, formulario de seguridad de la organización, etc.). Si no
   hay canal publicado, usad “Security” de la plataforma git con visibilidad
   restringida si está disponible.
3. Permitid un margen razonable para análisis y parche antes de divulgación
   pública coordinada.

## Qué no cubre este archivo

- Políticas de la **cuenta de Binance** (IP allowlist, permisos de API,
  retiros deshabilitados, etc.): son responsabilidad del operador.
- Cumplimiento legal/fiscal del trading automatizado en vuestra jurisdicción.

## Versiones soportadas

La rama principal del repositorio es el foco de correcciones de seguridad
relacionadas con el código propio. Para ramas o forks antiguos, el equipo debe
valorar backports manualmente.
