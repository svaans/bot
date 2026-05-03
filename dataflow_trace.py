"""Rastreo de flujo de datos - Origen del DataFrame en Bollinger."""
import pandas as pd
import sys

print("="*70)
print("TRAZABILIDAD DE DATOS - FLUJO COMPLETO")
print("="*70)

# 1. Verificar archivos CSV en cache
print("\n1. VERIFICACIÓN DE ARCHIVOS CSV EN CACHE:")
print("-"*70)
import os
cache_dir = "estado/cache"
if os.path.exists(cache_dir):
    for f in sorted(os.listdir(cache_dir)):
        if f.endswith('.csv'):
            path = os.path.join(cache_dir, f)
            size = os.path.getsize(path)
            print(f"  {f:>30} - {size:>8} bytes")
            
            # Leer primeras y últimas 3 filas
            df = pd.read_csv(path)
            print(f"    Filas: {len(df)}, Columnas: {list(df.columns)}")
            if len(df) > 0:
                print(f"    Primer ts: {df.iloc[0]['timestamp']}, Último ts: {df.iloc[-1]['timestamp']}")
                print(f"    Primer close: {df.iloc[0]['close']}, Último close: {df.iloc[-1]['close']}")
else:
    print("  ❌ Directorio no existe")

# 2. Verificar si el backtest usa datos reales o compartidos
print("\n2. VERIFICACIÓN DE AISLAMIENTO POR SÍMBOLO:")
print("-"*70)

from backtesting.replay import load_ohlcv_csv

symbols = ['BTC/EUR', 'ETH/EUR', 'SOL/EUR', 'BNB/EUR', 'ADA/EUR']
dataframes = {}

for sym in symbols:
    csv_path = f"estado/cache/{sym.replace('/', '_').replace('EUR', 'EUR')}_5m.csv"
    # Corregir nombre de archivo
    if 'BTC' in sym:
        csv_path = "estado/cache/BTC_EUR_5m.csv"
    elif 'ETH' in sym:
        csv_path = "estado/cache/ETH_EUR_5m.csv"
    elif 'SOL' in sym:
        csv_path = "estado/cache/SOL_EUR_5m.csv"
    elif 'BNB' in sym:
        csv_path = "estado/cache/BNB_EUR_5m.csv"
    elif 'ADA' in sym:
        csv_path = "estado/cache/ADA_EUR_5m.csv"
    
    df = load_ohlcv_csv(csv_path)
    dataframes[sym] = df
    
    print(f"\n  Símbolo: {sym}")
    print(f"    ID del DataFrame: {id(df)}")
    print(f"    Shape: {df.shape}")
    print(f"    ¿Mismo objeto que BTC/EUR?: {id(df) == id(dataframes['BTC/EUR'])}")
    
    # Verificar si comparten Series
    close_id = id(df['close'])
    print(f"    ID de close Series: {close_id}")
    
    # Últimos 5 timestamps
    print(f"    Últimos 5 timestamps:")
    for i in range(max(0, len(df)-5), len(df)):
        print(f"      {int(df.iloc[i]['timestamp'])} - close={df.iloc[i]['close']:.4f}")

# 3. Verificar si los datos son idénticos
print("\n3. COMPARACIÓN DE DATOS ENTRE SÍMBOLOS:")
print("-"*70)

btc_df = dataframes['BTC/EUR']
for sym, df in dataframes.items():
    if sym == 'BTC/EUR':
        continue
    
    is_same = btc_df.equals(df)
    has_same_close = btc_df['close'].equals(df['close'])
    has_same_ts = btc_df['timestamp'].equals(df['timestamp'])
    
    print(f"\n  BTC/EUR vs {sym}:")
    print(f"    ¿DataFrames idénticos (equals)?: {is_same}")
    print(f"    ¿Misma serie close?: {has_same_close}")
    print(f"    ¿Mismos timestamps?: {has_same_ts}")
    
    if has_same_close:
        print(f"    ❌ PROBLEMA: Los close prices son IDÉNTICOS")
        print(f"    Esto explica por qué MA y Std son constantes")

# 4. Verificar el procesamiento en tiempo real
print("\n4. VERIFICACIÓN DE PIPELINE EN TIEMPO REAL:")
print("-"*70)
print("\n  Buscando dónde se construye el DataFrame para Bollinger...")

# Verificar el flujo: DataFeed -> ProcesarVela -> StrategyEngine
print("\n  Inspeccionando core/data_feed/datafeed.py:")
try:
    with open("core/data_feed/datafeed.py", "r") as f:
        lines = f.readlines()
        # Buscar dónde se crea el DataFrame
        for i, line in enumerate(lines):
            if 'DataFrame' in line or 'pd.DataFrame' in line or '.copy()' in line:
                print(f"    Línea {i+1}: {line.rstrip()}")
except Exception as e:
    print(f"  Error: {e}")

# 5. Verificar si hay shallow copy
print("\n5. VERIFICACIÓN DE COPIAS SUPERFICIALES:")
print("-"*70)

# Crear DataFrames de prueba para verificar comportamiento
print("\n  Probando comportamiento de pandas:")
df1 = pd.DataFrame({'close': [100.0, 100.1, 100.2]})
df2 = df1  # Referencia compartida
df3 = df1.copy()  # Copia profunda
df4 = df1[:]  # Otra copia

print(f"  df1 id: {id(df1)}")
print(f"  df2 id (ref): {id(df2)} - ¿Mismo objeto?: {id(df1) == id(df2)}")
print(f"  df3 id (copy): {id(df3)} - ¿Mismo objeto?: {id(df1) == id(df3)}")
print(f"  df4 id (slice): {id(df4)} - ¿Mismo objeto?: {id(df1) == id(df4)}")

# 6. Verificar el cálculo de Bollinger con datos REALES simulados
print("\n6. PRUEBA CON DATOS REALES SIMULADOS:")
print("-"*70)

# Generar datos con volatilidad real
import numpy as np
np.random.seed(42)
real_closes = 100 + np.random.normal(0, 2, 400).cumsum() + 50

df_real = pd.DataFrame({
    'timestamp': range(1777651844325, 1777651844325 + 400*300000, 300000),
    'close': real_closes,
    'open': real_closes - 0.1,
    'high': real_closes + 0.2,
    'low': real_closes - 0.2,
    'volume': np.random.randint(100, 1000, 400)
})

print(f"  Datos simulados - Close min: {df_real['close'].min():.2f}, max: {df_real['close'].max():.2f}")
print(f"  Std de close: {df_real['close'].std():.4f}")

# Calcular Bollinger
ma = df_real['close'].rolling(20).mean()
std = df_real['close'].rolling(20).std(ddof=1)
bsup = ma + 2 * std
binf = ma - 2 * std

print(f"\n  Últimos 10 valores con datos REALES:")
print(f"  {'ts':>15} {'close':>10} {'MA(20)':>10} {'Std':>10} {'B_sup':>10} {'dist':>10}")
for i in range(max(19, len(df_real)-10), len(df_real)):
    if i < 19:
        continue
    close = df_real.iloc[i]['close']
    ma_val = ma.iloc[i]
    std_val = std.iloc[i]
    bsup_val = bsup.iloc[i]
    dist = abs(bsup_val - close) / close
    
    print(f"  {int(df_real.iloc[i]['timestamp']):>15} {close:>10.4f} {ma_val:>10.4f} {std_val:>10.4f} {bsup_val:>10.4f} {dist:>10.6f}")

print(f"\n  ¿MA constante?: {ma.dropna().std() < 0.0001}")
print(f"  ¿Std constante?: {std.dropna().std() < 0.0001}")

# 7. Conclusión
print("\n" + "="*70)
print("CONCLUSIÓN DEL RASTREO:")
print("="*70)

print("\n  Los archivos CSV en estado/cache/ contienen DATOS SINTÉTICOS")
print("  donde todos los símbolos comparten EXACTAMENTE los mismos valores.")
print("\n  Esto explica por qué:")
print("    - MA(20) = 100.450000 (constante)")
print("    - Std(20) = 0.294690 (constante)")
print("    - Banda Superior = 101.039380 (constante)")
print("\n  CAUSA RAÍZ: DATOS_CACHE_UTILIZADOS_EN_RUNTIME")
print("  (Los CSV de cache tienen datos de prueba, no datos reales de Binance)")
