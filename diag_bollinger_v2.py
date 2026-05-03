"""Diagnóstico detallado de Bollinger Bands - Versión corregida."""
import pandas as pd
import numpy as np

print("=" * 70)
print("DIAGNÓSTICO BOLLINGER - ANÁLISIS DETALLADO")
print("=" * 70)

symbols = ['BTC', 'ETH', 'SOL', 'BNB', 'ADA']

for sym in symbols:
    print(f"\n{'=' * 70}")
    print(f"SÍMBOLO: {sym}/EUR")
    print("=" * 70)
    
    df = pd.read_csv(f"estado/cache/{sym}_EUR_5m.csv")
    print(f"Total velas: {len(df)}")
    
    # Verificar orden cronológico
    timestamps = df['timestamp'].values
    is_sorted = all(timestamps[i] <= timestamps[i+1] for i in range(len(timestamps)-1))
    print(f"¿Orden cronológico?: {is_sorted}")
    
    periodo = 20
    desviacion = 2.0
    
    # Calcular Bollinger PARA TODAS LAS VELAS (no solo las últimas 20)
    closes = df['close'].values
    ma_all = df['close'].rolling(window=periodo).mean()
    std_all = df['close'].rolling(window=periodo).std(ddof=1)
    banda_sup_all = ma_all + desviacion * std_all
    banda_inf_all = ma_all - desviacion * std_all
    
    # Últimas 20 velas - mostrar evolución
    print(f"\nÚltimas 20 velas - EVOLUCIÓN:")
    print(f"{'ts':>15} {'close':>10} {'MA(20)':>10} {'std':>10} {'B_inf':>10} {'B_sup':>10} {'dist_Bsup':>12} {'¿>=1%?':>8}")
    print("-" * 95)
    
    for i in range(max(0, len(df)-20), len(df)):
        ts = int(df.iloc[i]['timestamp'])
        close = df.iloc[i]['close']
        ma = ma_all.iloc[i]
        std = std_all.iloc[i]
        bsup = banda_sup_all.iloc[i]
        binf = banda_inf_all.iloc[i]
        dist = abs(bsup - close) / close if close > 0 else 0
        
        cumple = "SÍ" if dist >= 0.01 else "NO"
        print(f"{ts:>15} {close:>10.4f} {ma:>10.4f} {std:>10.4f} {binf:>10.4f} {bsup:>10.4f} {dist:>12.6f} {cumple:>8}")
    
    # Verificar si las bandas son constantes
    print(f"\nVerificación de dinamismo (últimas 20 velas):")
    ma_last_20 = ma_all.tail(20).values
    std_last_20 = std_all.tail(20).values
    bsup_last_20 = banda_sup_all.tail(20).values
    
    print(f"MA(20) - min: {ma_last_20.min():.6f}, max: {ma_last_20.max():.6f}, std: {ma_last_20.std():.6f}")
    print(f"Std(20) - min: {std_last_20.min():.6f}, max: {std_last_20.max():.6f}, std: {std_last_20.std():.6f}")
    print(f"B_sup - min: {bsup_last_20.min():.6f}, max: {bsup_last_20.max():.6f}, std: {bsup_last_20.std():.6f}")
    
    # Verificar si TODO el dataset tiene bandas constantes
    ma_all_vals = ma_all.dropna().values
    is_constant = ma_all_vals.std() < 0.0001
    print(f"\n¿MA(20) constante en todo el dataset?: {is_constant}")
    if is_constant:
        print(f"  Valor constante: {ma_all_vals[0]:.6f}")
    
    # Verificar los primeros 30 valores para ver si hay variación
    print(f"\nPrimeros 30 valores de MA(20) y Std:")
    for i in range(19, min(50, len(df))):
        ma_i = ma_all.iloc[i]
        std_i = std_all.iloc[i]
        bsup_i = banda_sup_all.iloc[i]
        close_i = df.iloc[i]['close']
        if i < 25 or i % 5 == 0:
            print(f"  índice {i}: close={close_i:.4f}, MA={ma_i:.6f}, std={std_i:.6f}, Bsup={bsup_i:.6f}")
