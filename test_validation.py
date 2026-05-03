"""Script de validación para verificar los fixes aplicados."""

import asyncio
import json
from pathlib import Path
from collections import Counter

from backtesting.replay import load_ohlcv_csv, replay_entradas_async, summarize_replay


SYMBOLS_CSV = {
    "BTC/EUR": "estado/cache/BTC_EUR_5m.csv",
    "ETH/EUR": "estado/cache/ETH_EUR_5m.csv",
    "SOL/EUR": "estado/cache/SOL_EUR_5m.csv",
    "BNB/EUR": "estado/cache/BNB_EUR_5m.csv",
    "ADA/EUR": "estado/cache/ADA_EUR_5m.csv",
}

WINDOW = 50  # Ventana menor para tener más evaluaciones
STEP = 5  # Evaluar cada 5 velas


async def test_symbol(symbol: str, csv_path: str) -> dict:
    """Ejecuta replay para un símbolo y devuelve métricas."""
    print(f"\n{'='*60}")
    print(f"📊 Evaluando {symbol}...")
    print(f"{'='*60}")

    df = load_ohlcv_csv(csv_path)
    print(f"   Velas cargadas: {len(df)}")

    if len(df) < WINDOW:
        print(f"   ⚠️  Insuficientes velas (mínimo {WINDOW})")
        return {"symbol": symbol, "error": "insufficient_data"}

    # Configuración base
    config = {
        "usar_score_tecnico": True,
        "umbral_score_tecnico": 2.0,
        "diversidad_minima": 2,
    }

    rows = await replay_entradas_async(
        symbol, df, window=WINDOW, step=STEP, config=config
    )

    print(f"   Velas evaluadas: {len(rows)}")

    # Contar tipos de rechazo
    permitidos = sum(1 for r in rows if r.get("permitido"))
    rechazados = len(rows) - permitidos

    # Contar validaciones fallidas
    motivos = Counter()
    validaciones_stats = {
        "volumen": {"pass": 0, "fail": 0},
        "rsi": {"pass": 0, "fail": 0},
        "slope": {"pass": 0, "fail": 0},
        "bollinger": {"pass": 0, "fail": 0},
    }

    duplicate_count = 0
    volume_zero_count = 0

    for r in rows:
        if not r.get("permitido"):
            m = r.get("motivo_rechazo")
            motivos[m or "sin_motivo"] += 1

        # Validaciones
        vals = r.get("validaciones", {})
        for vname, vresult in vals.items():
            if vname in validaciones_stats:
                if vresult:
                    validaciones_stats[vname]["pass"] += 1
                else:
                    validaciones_stats[vname]["fail"] += 1

    # Calcular métricas
    permitido_rate = (permitidos / len(rows) * 100) if len(rows) > 0 else 0

    print(f"\n   📈 RESULTADOS:")
    print(f"   ├─ Velas totales: {len(rows)}")
    print(f"   ├─ Permitidas: {permitidos} ({permitido_rate:.1f}%)")
    print(f"   ├─ Rechazadas: {rechazados} ({100 - permitido_rate:.1f}%)")

    print(f"\n   🔍 VALIDACIONES (fallos):")
    for vname, stats in validaciones_stats.items():
        total = stats["pass"] + stats["fail"]
        if total > 0:
            fail_pct = stats["fail"] / total * 100
            print(f"   ├─ {vname}: {stats['fail']}/{total} fallos ({fail_pct:.1f}%)")

    print(f"\n   🚫 MOTIVOS RECHAZO:")
    for motivo, count in motivos.most_common(5):
        print(f"   ├─ {motivo}: {count} ({count/len(rows)*100:.1f}%)")

    return {
        "symbol": symbol,
        "evaluadas": len(rows),
        "permitidas": permitidos,
        "permitido_rate": permitido_rate,
        "validaciones": validaciones_stats,
        "motivos_rechazo": dict(motivos),
    }


async def main():
    print("🔧 VALIDACIÓN DE FIXES APLICADOS")
    print("=" * 60)
    print("✓ Fix 1: duplicate_bar (clear en ws_connected)")
    print("✓ Fix 2: Bollinger validator (bool wrapping)")
    print("✓ Fix 3: Volume validators (bool wrapping)")
    print("=" * 60)

    resultados = []
    for symbol, csv_path in SYMBOLS_CSV.items():
        try:
            r = await test_symbol(symbol, csv_path)
            resultados.append(r)
        except Exception as e:
            print(f"   ❌ Error: {e}")
            resultados.append({"symbol": symbol, "error": str(e)})

    # Resumen global
    print(f"\n\n{'='*60}")
    print("📋 RESUMEN GLOBAL")
    print(f"{'='*60}")

    total_evaluadas = sum(r.get("evaluadas", 0) for r in resultados if "evaluadas" in r)
    total_permitidas = sum(r.get("permitidas", 0) for r in resultados if "permitidas" in r)

    print(f"Total velas evaluadas: {total_evaluadas}")
    print(f"Total señales permitidas: {total_permitidas}")
    print(f"Ratio global: {total_permitidas/total_evaluadas*100:.1f}%")

    # Diagnóstico
    print(f"\n🏥 DIAGNÓSTICO:")

    all_good = True

    for r in resultados:
        if "error" in r:
            print(f"   ❌ {r['symbol']}: ERROR - {r['error']}")
            all_good = False
            continue

        rate = r.get("permitido_rate", 0)
        if rate < 5:
            print(f"   ⚠️  {r['symbol']}: Muy restrictivo ({rate:.1f}% permitido)")
            all_good = False
        elif rate < 20:
            print(f"   ⚠️  {r['symbol']}: Poco activo ({rate:.1f}% permitido)")
        else:
            print(f"   ✅ {r['symbol']}: OK ({rate:.1f}% permitido)")

        # Check Bollinger failure rate
        vals = r.get("validaciones", {})
        if "bollinger" in vals:
            bfail = vals["bollinger"]["fail"]
            btotal = bfail + vals["bollinger"]["pass"]
            if btotal > 0 and bfail / btotal > 0.8:
                print(f"      ⚠️  Bollinger falla {bfail/btotal*100:.0f}% (revisar umbral)")

    if all_good and total_permitidas > 0:
        print(f"\n✅ ESTADO: FUNCIONANDO_CORRECTAMENTE")
    elif all_good:
        print(f"\n⚠️  ESTADO: VALIDACIONES_DEMASIADO_RESTRICTIVAS")
    else:
        print(f"\n❌ ESTADO: REQUIERE REVISIÓN")

    # Guardar resultados
    output_path = Path("logs/validation_results.json")
    output_path.parent.mkdir(exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(resultados, f, indent=2, default=str)
    print(f"\n💾 Resultados guardados en: {output_path}")


if __name__ == "__main__":
    asyncio.run(main())
