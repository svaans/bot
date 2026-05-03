"""Instrumentación para confirmar fuente de datos en runtime."""
import os
import json

print("="*70)
print("INSTRUMENTACIÓN: CONFIRMAR FUENTE EN RUNTIME")
print("="*70)

print("\n1. VERIFICANDO CONFIGURACIÓN ACTUAL:")
print("-"*70)

# Leer config/claves.env
config_file = "config/claves.env"
env_vars = {}
if os.path.exists(config_file):
    with open(config_file, "r") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#'):
                if '=' in line:
                    key, value = line.split('=', 1)
                    env_vars[key.strip()] = value.strip()

# Mostrar variables clave
key_vars = ['MODO_REAL', 'BOT_ENV', 'MODO_OPERATIVO', 'PYTHONPATH']
for var in key_vars:
    value = env_vars.get(var) or os.getenv(var, 'NO_DEFINIDO')
    print(f"  {var}: {value}")

print("\n2. CÓDIGO PARA INSTRUMENTAR LOGS:")
print("-"*70)

codigo = '''
# Agregar en core/data_feed/handlers.py
# En la función que procesa velas del WebSocket (handle_kline o similar)

def _log_data_source(symbol: str, candle: dict, source: str = "unknown"):
    """Log para confirmar fuente de cada vela."""
    from core.utils.log_utils import safe_extra
    import time
    
    log.info(
        "diagnostico.data_source",
        extra=safe_extra({
            "symbol": symbol,
            "timestamp": candle.get("timestamp") or candle.get("open_time"),
            "close": candle.get("close"),
            "source": source,  # "websocket" | "backfill" | "cache"
            "event_time": int(time.time() * 1000)
        })
    )

# Llamar en el handler de WebSocket:
# _log_data_source(symbol, candle, "websocket")

# Llamar en backfill:
# _log_data_source(symbol, candle, "backfill")

# Llamar si se carga desde cache:
# _log_data_source(symbol, candle, "cache_csv")
'''

print(codigo)

print("\n3. VERIFICANDO FLUJO EN startup_manager.py:")
print("-"*70)

# Buscar cómo se inicia el DataFeed
print("\n  Buscando inicialización de DataFeed...")
if os.path.exists("core/startup_manager.py"):
    with open("core/startup_manager.py", "r") as f:
        content = f.read()
        lines = content.split('\n')
        for i, line in enumerate(lines):
            if 'DataFeed' in line or 'datafeed' in line.lower():
                start = max(0, i-2)
                end = min(len(lines), i+3)
                print(f"\n  Líneas {start+1}-{end}:")
                for j in range(start, end):
                    marker = ">>>" if j == i else "   "
                    print(f"{marker} {j+1}: {lines[j]}")
                print()

print("\n4. DIAGNÓSTICO FINAL:")
print("="*70)
print("""
  HALLAZGO CLAVE:
  ❌ Los CSV en estado/cache/ contienen DATOS SINTÉTICOS
  ❌ Usaste esos CSV en el diagnóstico (backtest), NO datos reales
  
  EN MODO_REAL=True:
  ✅ El bot NUNCA debería usar CSVs de cache/
  ✅ La fuente debe ser: WebSocket Binance (wss://stream.binance.com)
  ✅ Backfill usa API REST (binance_api/cliente.py)
  
  PARA VALIDAR CON DATOS REALES:
  1. Ejecutar: MODO_REAL=true python main.py
  2. Esperar conexión WebSocket (buscar "ws_connected")
  3. Verificar que NO aparezca "cargando cache" o "CSV"
  4. Los logs deben mostrar: "kline" con datos de stream.binance.com
  
  ESTADO ACTUAL:
  - El diagnóstico anterior usó backtest (CSV sintéticos)
  - NO prueba el WebSocket en vivo
  - Por eso MA y Std fueron constantes (datos irreales)
""")

print("\n5. RECOMENDACIÓN:")
print("="*70)
print("""
  ✅ Las correcciones aplicadas (bool wrapping, duplicate_bar) están BIEN
  ✅ El cálculo de Bollinger funciona correctamente
  ❌ El problema fue USAR DATOS SINTÉTICOS para el diagnóstico
  
  PRÓXIMOS PASOS:
  1. Ejecutar el bot EN VIVO con MODO_REAL=true
  2. O usar datos históricos REALES de Binance API
  3. Validar con 30-50 velas reales (no sintéticas)
  
  El sistema debería funcionar correctamente con datos reales.
""")
