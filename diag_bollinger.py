"""Diagnóstico detallado de Bollinger Bands."""
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
    print(f"Columnas: {list(df.columns)}")
    print(f"Primera vela ts: {df.iloc[0]['timestamp']}")
    print(f"Última vela ts: {df.iloc[-1]['timestamp']}")
    
    # Verificar orden cronológico
    timestamps = df['timestamp'].values
    is_sorted = all(timestamps[i] <= timestamps[i+1] for i in range(len(timestamps)-1))
    print(f"¿Orden cronológico?: {is_sorted}")
    
    # Tomar últimas 20 velas
    periodo = 20
    desviacion = 2.0
    
    print(f"\nÚltimas 20 velas:")
    header = f"{'ts':>15} {'close':>10} {'MA':>10} {'std':>10} {'B_inf':>10} {'B_sup':>10} {'dist_Bsup':>10}"
    print(header)
    print("-" * 75)
    
    for i in range(max(0, len(df)-20), len(df)):
        window = df.iloc[max(0, i-19):i+1].copy()
        
        if len(window) < periodo:
            continue
            
        ma = window['close'].rolling(window=periodo).mean().iloc[-1]
        std = window['close'].rolling(window=periodo).std(ddof=1).iloc[-1]
        banda_inf = ma - desviacion * std
        banda_sup = ma + desviacion * std
        precio = window['close'].iloc[-1]
        
        dist_sup = abs(banda_sup - precio) / precio
        
        ts = int(window['timestamp'].iloc[-1])
        print(f"{ts:>15} {precio:>10.4f} {ma:>10.4f} {std:>10.4f} {banda_inf:>10.4f} {banda_sup:>10.4f} {dist_sup:>10.6f}")
    
    # Verificar si las bandas son constantes
    print(f"\nVerificación de dinamismo (últimas 20 velas):")
    closes = df.tail(20)['close'].values
    print(f"Close min: {closes.min():.4f}, max: {closes.max():.4f}, std: {closes.std():.4f}")
    
    # Calcular bandas para última vela
    last_window = df.tail(20).copy()
    ma_last = last_window['close'].mean()
    std_last = last_window['close'].std(ddof=1)
    bsup_last = ma_last + 2.0 * std_last
    binf_last = ma_last - 2.0 * std_last
    precio_last = last_window['close'].iloc[-1]
    
    print(f"\nÚltima vela:")
    print(f"  Precio: {precio_last:.4f}")
    print(f"  MA(20): {ma_last:.4f}")
    print(f"  Std(20): {std_last:.4f}")
    print(f"  Banda inf: {binf_last:.4f}")
    print(f"  Banda sup: {bsup_last:.4f}")
    print(f"  Distancia a bsup: {abs(bsup_last - precio_last) / precio_last:.6f}")
    print(f"  ¿Dist >= 0.01?: {abs(bsup_last - precio_last) / precio_last >= 0.01}")
    
    # Verificar función original
    print(f"\nVerificando con calcular_bollinger original:")
    from indicadores.bollinger import calcular_bollinger
    banda_inf_2, banda_sup_2, precio_2 = calcular_bollinger(last_window)
    if banda_sup_2 is not None:
        dist_2 = abs(banda_sup_2 - precio_2) / precio_2
        print(f"  Banda sup (calcular_bollinger): {banda_sup_2:.4f}")
        print(f"  Precio (calcular_bollinger): {precio_2:.4f}")
        print(f"  Distancia: {dist_2:.6f}")
        print(f"  ¿Dist >= 0.01?: {dist_2 >= 0.01}")
        print(f"  ¿Mismo resultado?: {abs(bsup_last - precio_last) / precio_last:.6f == dist_2:.6f}")
        print(f"  Valores son iguales?: {abs(bsup_last - precio_last) / precio_last == dist_2}")
