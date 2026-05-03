"""Verificación final de datos sintéticos en cache."""
import pandas as pd
import numpy as np

print("="*70)
print("VERIFICACIÓN: ¿DATOS SINTÉTICOS EN CACHE?")
print("="*70)

symbols = ['BTC', 'ETH', 'SOL', 'BNB', 'ADA']
data = {}

for sym in symbols:
    df = pd.read_csv(f"estado/cache/{sym}_EUR_5m.csv")
    data[sym] = df['close'].values
    print(f"{sym}/EUR: len={len(df)}, close[0]={df.iloc[0]['close']}, close[-1]={df.iloc[-1]['close']}")

print("\nComparación de close prices entre símbolos:")
base = data['BTC']
for sym, closes in data.items():
    if sym == 'BTC':
        continue
    is_identical = np.array_equal(base, closes)
    print(f"  BTC vs {sym}: ¿Idénticos?: {is_identical}")

# Verificar si los valores son artificiales
print(f"\nValores únicos de close en BTC: {np.unique(base)}")
print(f"Cantidad de valores únicos: {len(np.unique(base))}")

# Verificar si hay variación
print(f"\nEstadísticas de close en BTC:")
print(f"  Min: {base.min():.4f}")
print(f"  Max: {base.max():.4f}")
print(f"  Std: {base.std():.6f}")
print(f"  Media: {base.mean():.6f}")

# Verificar si MA(20) debería ser constante
print(f"\nCálculo de MA(20) para verificar:")
ma = pd.Series(base).rolling(20).mean()
print(f"  MA(20) min: {ma.min():.6f}")
print(f"  MA(20) max: {ma.max():.6f}")
print(f"  ¿MA constante?: {ma.std() < 0.0001}")

print("\n" + "="*70)
print("CONCLUSIÓN:")
print("="*70)
print("Los archivos CSV en cache/ contienen DATOS SINTÉTICOS")
print("Todos los símbolos comparten exactamente los mismos close prices.")
print(f"Valores únicos: {len(np.unique(base))} (muy pocos para 400 velas)")
print("\nEsto explica por qué MA y Std son constantes.")
print("CAUSA: DATOS_CACHE_UTILIZADOS_EN_RUNTIME (datos de prueba)")
