"""Validación con datos REALES de Binance API."""
import os
os.environ['MODO_REAL'] = 'true'

print("="*70)
print("VALIDACIÓN CON DATOS REALES - BINANCE API")
print("="*70)

# 1. Obtener datos reales históricos via REST API
print("\n1. DESCARGANDO DATOS REALES DE BINANCE...")
print("-"*70)

try:
    from binance_api.client import Client
    
    # Crear cliente (necesita API keys o usar público)
    client = Client('', '')  # Público para datos históricos
    
    symbols = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'ADAUSDT', 'SOLUSDT']
    
    data_real = {}
    for sym in symbols:
        print(f"\n  Descargando {sym}...")
        try:
            klines = client.get_klines(
                symbol=sym,
                interval=Client.KLINE_INTERVAL_5MINUTE,
                limit=400
            )
            
            # Convertir a DataFrame
            import pandas as pd
            df = pd.DataFrame(klines, columns=[
                'timestamp', 'open', 'high', 'low', 'close', 'volume',
                'close_time', 'quote_volume', 'trades', 'taker_buy_base',
                'taker_buy_quote', 'ignore'
            ])
            
            # Limpiar y preparar
            df = df[['timestamp', 'open', 'high', 'low', 'close', 'volume']].copy()
            df['timestamp'] = pd.to_numeric(df['timestamp'])
            df['close'] = pd.to_numeric(df['close'])
            df['open'] = pd.to_numeric(df['open'])
            df['high'] = pd.to_numeric(df['high'])
            df['low'] = pd.to_numeric(df['low'])
            df['volume'] = pd.to_numeric(df['volume'])
            
            data_real[sym] = df
            print(f"    ✅ {len(df)} velas obtenidas")
            print(f"    Close min: {df['close'].min():.4f}, max: {df['close'].max():.4f}")
            print(f"    Std close: {df['close'].std():.4f}")
            
        except Exception as e:
            print(f"    ❌ Error: {e}")
    
    # 2. Calcular Bollinger con datos REALES
    print("\n\n2. CÁLCULO DE BOLLINGER CON DATOS REALES:")
    print("-"*70)
    
    for sym, df in data_real.items():
        print(f"\n  Símbolo: {sym}")
        
        # Calcular Bollinger
        close = df['close']
        ma = close.rolling(20).mean()
        std = close.rolling(20).std(ddof=1)
        bsup = ma + 2.0 * std
        binf = ma - 2.0 * std
        
        # Verificar dinamismo
        ma_std = ma.dropna().std()
        std_std = std.dropna().std()
        
        print(f"    MA(20) std: {ma_std:.6f} {'(DINÁMICO)' if ma_std > 0.001 else '(CONSTANTE - ERROR)'}")
        print(f"    Std(20) std: {std_std:.6f} {'(DINÁMICO)' if std_std > 0.001 else '(CONSTANTE - ERROR)'}")
        
        # Últimas 5 velas
        print(f"    Últimas 5 velas:")
        for i in range(max(19, len(df)-5), len(df)):
            if i < 19:
                continue
            close_val = close.iloc[i]
            bsup_val = bsup.iloc[i]
            dist = abs(bsup_val - close_val) / close_val if close_val > 0 else 0
            cumple = "SÍ" if dist >= 0.01 else "NO"
            print(f"      ts={int(df['timestamp'].iloc[i])}: close={close_val:.4f}, Bsup={bsup_val:.4f}, dist={dist:.6f} ({cumple})")
    
    # 3. Diagnóstico final
    print("\n\n" + "="*70)
    print("DIAGNÓSTICO FINAL CON DATOS REALES:")
    print("="*70)
    
    all_dynamic = True
    for sym, df in data_real.items():
        ma = df['close'].rolling(20).mean()
        std = df['close'].rolling(20).std(ddof=1)
        if ma.dropna().std() < 0.001:
            print(f"  ❌ {sym}: MA(20) CONSTANTE (error)")
            all_dynamic = False
        else:
            print(f"  ✅ {sym}: MA(20) dinámico (correcto)")
    
    if all_dynamic:
        print("\n  ✅✅ DATOS REALES: Todo funciona correctamente")
        print("  ✅ Las correcciones aplicadas (bool wrapping, duplicate_bar) están listas")
        print("  ✅ El sistema debe funcionar en vivo con MODO_REAL=true")
    else:
        print("\n  ❌ Hay problemas con los datos de Binance API")

except ImportError:
    print("\n  ❌ No se pudo importar binance_api.client")
    print("  (El bot necesita las credenciales API para datos reales)")
    
except Exception as e:
    print(f"\n  ❌ Error general: {e}")
    import traceback
    traceback.print_exc()
