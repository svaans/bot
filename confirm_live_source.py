"""Confirmación de fuente real de datos en MODO_REAL=True."""
import os
import sys

print("="*70)
print("CONFIRMACIÓN DE FUENTE DE DATOS - MODO_REAL")
print("="*70)

# 1. Verificar configuración actual
print("\n1. VERIFICACIÓN DE CONFIGURACIÓN:")
print("-"*70)

# Leer variables de entorno
modo_real_env = os.getenv("MODO_REAL", "no_seteado")
bot_env = os.getenv("BOT_ENV", "no_seteado")
modo_operativo = os.getenv("MODO_OPERATIVO", "no_seteado")

print(f"  MODO_REAL (env): {modo_real_env}")
print(f"  BOT_ENV: {bot_env}")
print(f"  MODO_OPERATIVO: {modo_operativo}")

# Verificar archivo de configuración
print("\n  Buscando config/claves.env...")
config_file = "config/claves.env"
if os.path.exists(config_file):
    print(f"  ✅ Archivo encontrado: {config_file}")
    with open(config_file, "r") as f:
        lines = f.readlines()
        for line in lines:
            if 'MODO_REAL' in line or 'BOT_ENV' in line or 'MODO_OPERATIVO' in line:
                print(f"    {line.rstrip()}")
else:
    print(f"  ❌ No encontrado: {config_file}")

# 2. Rastrear inicialización de DataFeed en runtime
print("\n2. RASTREO DE INICIALIZACIÓN DE DataFeed:")
print("-"*70)

# Buscar dónde se crea el DataFeed
print("\n  Buscando en core/trader/...")
import subprocess

# Buscar la clase TraderLite y cómo crea el DataFeed
result = subprocess.run(
    ["findstr", "/n", "DataFeed", "core\\trader\\trader_lite.py"],
    capture_output=True,
    text=True,
    shell=True
)
if result.stdout:
    print("  trader_lite.py:")
    for line in result.stdout.split('\n')[:10]:
        if line.strip():
            print(f"    {line}")

# Buscar en startup_manager
print("\n  Buscando en core/startup_manager.py...")
if os.path.exists("core/startup_manager.py"):
    result = subprocess.run(
        ["findstr", "/n", "DataFeed", "core\\startup_manager.py"],
        capture_output=True,
        text=True,
        shell=True
    )
    if result.stdout:
        print("  startup_manager.py:")
        for line in result.stdout.split('\n')[:10]:
            if line.strip():
                print(f"    {line}")

# 3. Verificar si el WebSocket se conecta en vivo
print("\n3. VERIFICACIÓN DE WEBSOCKET EN VIVO:")
print("-"*70)

# Buscar cómo se inicia la conexión WebSocket
print("\n  Buscando configuración de WebSocket...")
ws_files = [
    "core/data_feed/datafeed.py",
    "core/data_feed/streaming.py",
    "binance_api/websocket.py"
]

for file in ws_files:
    if os.path.exists(file):
        print(f"\n  Archivo: {file}")
        with open(file, "r", encoding="utf-8") as f:
            lines = f.readlines()
            for i, line in enumerate(lines):
                if 'wss://' in line or 'stream.binance' in line or 'websocket' in line.lower():
                    print(f"    Línea {i+1}: {line.rstrip()}")

# 4. Verificar si usa cache/CSV en runtime
print("\n4. VERIFICACIÓN DE USO DE CACHE EN RUNTIME:")
print("-"*70)

print("\n  Buscando carga de CSV en runtime...")
result = subprocess.run(
    ["findstr", "/r", "read_csv|csv|cache", "core\\data_feed\\*.py"],
    capture_output=True,
    text=True,
    shell=True
)
if result.stdout:
    print("  Posibles referencias a CSV:")
    for line in result.stdout.split('\n')[:15]:
        if line.strip() and 'test' not in line.lower():
            print(f"    {line.strip()}")

# 5. Verificar el flujo: WebSocket -> DataFeed -> DataFrame
print("\n5. FLUJO DE DATOS WebSocket -> INDICADORES:")
print("-"*70)

print("\n  El flujo normal en MODO_REAL es:")
print("  1. binance_api/websocket.py: Escucha wss://stream.binance.com")
print("  2. core/data_feed/streaming.py: Procesa mensajes WS")
print("  3. core/data_feed/datafeed.py: DataFeed almacena en buffer/queue")
print("  4. core/vela/pipeline.py: Procesa vela -> actualiza DataFrame")
print("  5. indicadores/*: Calcula sobre DataFrame actualizado")

# 6. Verificar si el backtest usa los mismos archivos
print("\n6. VERIFICACIÓN: ¿Backtest usa MISMOS archivos que live?")
print("-"*70)

print("\n  Al ejecutar el bot con MODO_REAL=true:")
print("  - NO debería usar CSVs de cache/")
print("  - El WebSocket debería conectarse a Binance API")
print("  - Los datos deberían venir del stream en tiempo real")

# 7. Agregar logging para confirmar fuente
print("\n7. CÓDIGO PARA INSTRUMENTAR LOGS EN RUNTIME:")
print("-"*70)

codigo_logging = '''
# Agregar en core/data_feed/handlers.py, función que procesa velas:

def _log_candle_source(symbol, timestamp, close, source="websocket"):
    """Log de diagnóstico para confirmar fuente de datos."""
    from core.utils.log_utils import safe_extra
    log.info(
        "diagnostico.candle_source",
        extra=safe_extra({
            "symbol": symbol,
            "timestamp": timestamp,
            "close": close,
            "source": source  # "websocket" | "csv_cache" | "backfill"
        })
    )

# En el handler de velas, llamar:
# _log_candle_source(symbol, candle['timestamp'], candle['close'], "websocket")
'''

print(codigo_logging)

print("\n" + "="*70)
print("CONCLUSIÓN:")
print("="*70)
print("""
  Para confirmar la fuente real en runtime:
  
  1. Ejecutar el bot: MODO_REAL=true python main.py
  2. Observar logs de conexión WebSocket (buscar "ws_connected")
  3. Verificar que NO aparezca "cargando desde cache"
  4. Los datos deben venir de wss://stream.binance.com
  
  Los CSVs en cache/ son SOLO para backtesting/diagnóstico.
  En MODO_REAL, el bot debería usar WebSocket en vivo.
""")
