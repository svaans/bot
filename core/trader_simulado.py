import os
import time
import threading
import json
import pandas as pd
import numpy as np
import asyncio
from datetime import datetime

from core.logger import configurar_logger
from core.utils import validar_dataframe, segundos_transcurridos
from core.pesos import cargar_pesos_estrategias
from core.tendencia import detectar_tendencia, señales_repetidas
from core.modo import MODO_REAL
from core.adaptador_umbral import calcular_umbral_adaptativo, cargar_umbral_optimo, calcular_tp_sl_adaptativos
from estrategias_entrada.gestor_entradas import evaluar_estrategias, entrada_permitida
from estrategias_salida.reajuste_tp_sl import calcular_promedios_sl_tp
from estrategias_salida.salida_trailing_stop import verificar_trailing_stop
from estrategias_salida.salida_por_tendencia import verificar_reversion_tendencia
from estrategias_salida.gestor_salidas import evaluar_salidas, verificar_filtro_tecnico
from estrategias_salida.salida_stoploss import salida_stoploss
from filtros.validador_entradas import evaluar_validez_estrategica
from filtros.filtro_salidas import validar_necesidad_de_salida
from binance_api.websocket import escuchar_velas
from aprendizaje.aprendizaje_en_linea import registrar_resultado_trade
from aprendizaje.entrenador_estrategias import actualizar_pesos_estrategias_symbol
from core.configuracion import cargar_configuracion_simbolo
from core.monitor_estado_bot import monitorear_estado_periodicamente
from indicadores.rsi import calcular_rsi
from indicadores.momentum import calcular_momentum
from indicadores.slope import calcular_slope
from config.config import INTERVALO_VELAS

log = configurar_logger("trader_simulado", modo_silencioso=True)


class TraderSimulado:
    def __init__(self, symbols, configuraciones=None, pesos_personalizados=None, modo_optimizacion=False):
        self.symbols = symbols
        self.modo_optimizacion = modo_optimizacion
        self.estado = {s: "esperando" for s in symbols}
        self.buffer = {s: [] for s in symbols}
        self.ordenes_abiertas = {}
        self.historial_cierres = {}
        self.ultimo_timestamp = {s: None for s in symbols}
        self.resultados = {s: [] for s in symbols}
        self.capital_simulado = {s: 1000.0 for s in symbols}
        self.historial_ordenes = []
        self.ultimo_umbral = {}
        self.ultima_tendencia = {}
        self.ultimas_estrategias = {}
        self.locks = {s: asyncio.Lock() for s in symbols}
        self.ultimo_log_cooldown = {}
        self.lock_archivo = threading.Lock()

        self.configuraciones = configuraciones or {}
        self.pesos_personalizados = pesos_personalizados or {}
        self.pesos_por_simbolo = {}
        self.config_por_simbolo = {}

        for symbol in self.symbols:
            self.config_por_simbolo[symbol] = self.configuraciones.get(symbol) or cargar_configuracion_simbolo(symbol)
            self.pesos_por_simbolo[symbol] = self.pesos_personalizados.get(symbol) or cargar_pesos_estrategias().get(symbol, {})

            try:
                archivo = f"datos/{symbol.replace('/', '_').lower()}_1m.parquet"
                df = pd.read_parquet(archivo)
                for _, row in df.tail(30).iterrows():
                    self.buffer[symbol].append({
                        "symbol": symbol,
                        "timestamp": row["timestamp"],
                        "open": row["open"],
                        "high": row["high"],
                        "low": row["low"],
                        "close": row["close"],
                        "volume": row["volume"]
                    })
                log.info(f"✅ Precargadas 30 velas para {symbol}.")
            except Exception as e:
                log.error(f"❌ Error precargando velas para {symbol}: {e}")

    def cooldown_activo(self, symbol, cierre, cooldown_segundos):
        try:
            ts = cierre["timestamp"]
            ts = datetime.fromisoformat(ts) if isinstance(ts, str) else datetime.utcfromtimestamp(ts)
            return (datetime.utcnow() - ts).total_seconds() < cooldown_segundos
        except:
            return False

    def riesgo_superado_simulado(self, symbol, config):
        riesgo_maximo = config.get("riesgo_maximo_diario", 0.3)
        capital_actual = self.capital_simulado.get(symbol, 1000)
        return (1000 - capital_actual) / 1000 > riesgo_maximo

    async def ejecutar(self):
        self.tarea_estado = asyncio.create_task(monitorear_estado_periodicamente(self))

        async def callback(vela):
            await self.procesar_vela(vela)

        tareas = [escuchar_velas(s, INTERVALO_VELAS, callback) for s in self.symbols]
        await asyncio.gather(*tareas)

    async def procesar_vela(self, vela):
        symbol = vela["symbol"]
        async with self.locks[symbol]:
            self.buffer[symbol].append(vela)
            self.buffer[symbol] = self.buffer[symbol][-50:]
            if vela["timestamp"] == self.ultimo_timestamp.get(symbol):
                return
            self.ultimo_timestamp[symbol] = vela["timestamp"]

            if len(self.buffer[symbol]) < 30:
                log.info(f"📉 [{symbol}] Buffer insuficiente ({len(self.buffer[symbol])} velas)")
                return

            df = pd.DataFrame(self.buffer[symbol])
            if not validar_dataframe(df, ["high", "low", "close"]):
                log.warning(f"⚠️ [{symbol}] DataFrame inválido")
                return

            log.info(f"🔍 [{symbol}] Procesando vela — {vela['timestamp']}")

            tendencia = detectar_tendencia(symbol, df)
            log.info(f"🔁 [{symbol}] Tendencia detectada: {tendencia}")

            evaluacion = evaluar_estrategias(symbol, df, tendencia)
            if evaluacion is None:
                log.warning(f"⚠️ [{symbol}] Evaluación de estrategias nula")
                return

            puntaje = evaluacion["puntaje_total"]
            estrategias_detectadas = evaluacion["estrategias_activas"]
            log.info(f"📊 [{symbol}] Puntaje total: {puntaje:.2f} — Estrategias activas: {estrategias_detectadas}")

            pesos_symbol = self.pesos_por_simbolo.get(symbol, {})

            condiciones_iguales = (
                symbol in self.ultima_tendencia and
                symbol in self.ultimas_estrategias and
                self.ultima_tendencia[symbol] == tendencia and
                self.ultimas_estrategias[symbol] == estrategias_detectadas
            )

            config_actual = self.config_por_simbolo.get(symbol, {})
            umbral = calcular_umbral_adaptativo(symbol, df, estrategias_detectadas, pesos_symbol, config_actual)
            self.ultimo_umbral[symbol] = umbral
            self.ultimas_estrategias[symbol] = estrategias_detectadas
            self.ultima_tendencia[symbol] = tendencia

            capital_symbol = self.capital_simulado.get(symbol, 0.0)
            log.info(
                f"📈 [{symbol}] Tendencia: {tendencia} | Precio: {df['close'].iloc[-1]:.2f} | "
                f"Capital: {capital_symbol:.2f}€ | Estrategias activas: {len(estrategias_detectadas)} | "
                f"Puntaje: {puntaje:.2f} | Umbral: {umbral:.2f}"
            )

            if symbol in self.ordenes_abiertas:
                await self.verificar_cierres_simulados(vela)
                return

            config = self.config_por_simbolo.get(symbol, {})
            peso_min_total = config.get("peso_minimo_total", 0.5)
            diversidad_min = config.get("diversidad_minima", 2)
            cooldown_perdida = int(config.get("cooldown_tras_perdida", 5)) * 60

            cierre = self.historial_cierres.get(symbol)
            if cierre and self.cooldown_activo(symbol, cierre, cooldown_perdida):
                tiempo = int((datetime.utcnow() - datetime.fromisoformat(cierre["timestamp"])).total_seconds())
                if self.ultimo_log_cooldown.get(symbol) != tiempo:
                    self.ultimo_log_cooldown[symbol] = tiempo
                    log.info(f"🕒 [{symbol}] Cooldown activo tras pérdida anterior ({tiempo}s)")
                return

            estrategias_activas = [k for k, v in estrategias_detectadas.items() if v]
            peso_total = sum(pesos_symbol.get(k, 0) for k in estrategias_activas)
            diversidad = len(estrategias_activas)

            log.info(f"⚖️ [{symbol}] Peso total: {peso_total:.2f} / {peso_min_total} — Diversidad: {diversidad} / {diversidad_min}")

            if puntaje < umbral:
                log.warning(f"🚫 [{symbol}] Entrada bloqueada: puntaje {puntaje:.2f} < umbral {umbral:.2f}")
                return

            if diversidad < diversidad_min or peso_total < peso_min_total:
                log.warning(f"🚫 [{symbol}] Entrada bloqueada por peso o diversidad insuficiente")
                return

            if not evaluar_validez_estrategica(symbol, df, estrategias_detectadas):
                log.warning(f"❌ [{symbol}] Rechazada por filtro estratégico")
                return

            ventana_close = df["close"].tail(10)
            media_close = np.mean(ventana_close)
            volatilidad = np.std(ventana_close) / media_close if media_close else 0

            repetidas = señales_repetidas(self.buffer[symbol], pesos_symbol, tendencia, volatilidad, ventanas=3)
            if repetidas < 1 and puntaje < 1.2 * umbral:
                log.warning(f"🚫 [{symbol}] Entrada rechazada por persistencia insuficiente y puntaje débil")
                return
            elif repetidas < 1:
                log.info(f"⚠️ [{symbol}] Entrada débil permitida sin persistencia, puntaje alto")

            rsi = calcular_rsi(df)
            momentum = calcular_momentum(df)
            slope = calcular_slope(df)

            if not entrada_permitida(symbol, puntaje, umbral, estrategias_detectadas, rsi, slope, momentum):
                log.warning(f"🚫 [{symbol}] Rechazada por entrada_permitida()")
                return

            if self.riesgo_superado_simulado(symbol, config):
                log.warning(f"🔒 [{symbol}] Riesgo diario superado, entrada bloqueada")
                return

            log.info(f"✅ [{symbol}] Entrada CONFIRMADA — Puntaje: {puntaje:.2f}, Peso: {peso_total:.2f}, Diversidad: {diversidad}, Persistencia: {repetidas}/5")

            precio = float(vela["close"])
            sl, tp = calcular_tp_sl_adaptativos(df, precio, config)

            if not estrategias_detectadas or not any(estrategias_detectadas.values()):
                log.warning(f"⚠️ [{symbol}] Entrada ignorada — sin estrategias activas válidas")
                return

            self.ordenes_abiertas[symbol] = {
                "symbol": symbol,
                "precio_entrada": precio,
                "stop_loss": sl,
                "take_profit": tp,
                "timestamp": str(vela["timestamp"]),
                "estrategias_activas": estrategias_detectadas,
                "max_price": precio,
                "tendencia": tendencia
            }

            if not self.modo_optimizacion:
                self.guardar_orden_simulada(symbol, self.ordenes_abiertas[symbol].copy())
                actualizar_pesos_estrategias_symbol(symbol)
                self.pesos_por_simbolo[symbol] = cargar_pesos_estrategias().get(symbol, {})
                log.warning(f"🟢 ORDEN SIMULADA {symbol} COMPRA a {precio} | SL: {sl} | TP: {tp}")


    async def verificar_cierres_simulados(self, vela):
        symbol = vela["symbol"]
        if symbol not in self.ordenes_abiertas:
            log.info(f"🔍 [{symbol}] No hay orden abierta que verificar.")
            return

        orden = self.ordenes_abiertas[symbol]
        df = pd.DataFrame(self.buffer[symbol])
        if not validar_dataframe(df, ["high", "low", "close"]):
            log.warning(f"⚠️ [{symbol}] DataFrame inválido al verificar cierres")
            return

        precio_min = float(vela["low"])
        precio_max = float(vela["high"])
        precio_cierre = float(vela["close"])
        stop_loss = orden["stop_loss"]
        take_profit = orden["take_profit"]
        entrada = orden["precio_entrada"]

        log.info(f"🔎 [{symbol}] Verificando cierre — Precio actual: {precio_cierre:.2f}, SL: {stop_loss:.2f}, TP: {take_profit:.2f}")

        # ⏱️ Tiempo máximo abierto (6h), pero solo cerrar si está en ganancia
        timestamp_orden = orden.get("timestamp")
        if timestamp_orden and segundos_transcurridos(timestamp_orden) > 6 * 3600:
            retorno_actual = (precio_cierre - orden["precio_entrada"]) / orden["precio_entrada"]
            if retorno_actual > 0:
                log.warning(f"⌛ [{symbol}] Orden expirada en ganancia — Retorno: {retorno_actual:.4f}")
                self.cerrar_orden_simulada(symbol, precio_cierre, exito=True, motivo="Expirada")
                return
            else:
                log.info(f"🕒 [{symbol}] Orden vencida pero aún en pérdida (retorno {retorno_actual:.4f}), se mantiene abierta")


        # 🛑 Stop Loss con validación
        config_actual = self.config_por_simbolo.get(symbol, {})
        if precio_min <= stop_loss:
            log.info(f"🛑 [{symbol}] Posible SL activado — Precio mínimo: {precio_min:.2f}")
            resultado = salida_stoploss(orden, df, config=config_actual)

            if resultado.get("cerrar", False):
                log.info(f"🟥 [{symbol}] SL confirmado — {resultado.get('razon', '')}")
                self.cerrar_orden_simulada(symbol, stop_loss, exito=False, motivo="Stop Loss")
            else:
                log.warning(f"🛡️ [{symbol}] SL evitado — {resultado.get('razon', 'sin razón')}")
            return

        # 🎯 Take Profit
        if precio_max >= take_profit:
            log.info(f"🎯 [{symbol}] TP alcanzado — Cerrando por Take Profit")
            self.cerrar_orden_simulada(symbol, take_profit, exito=True, motivo="Take Profit")
            return

        # 🔃 Trailing Stop
        cerrar, motivo = verificar_trailing_stop(orden, precio_cierre, config=config_actual)
        if cerrar:
            log.info(f"🔃 [{symbol}] Trailing Stop activado — {motivo}")
            self.cerrar_orden_simulada(symbol, precio_cierre, exito=True, motivo=motivo)
            return

        # 📉 Reversión de tendencia
        if verificar_reversion_tendencia(symbol, df, self.ultima_tendencia.get(symbol)):
            if not verificar_filtro_tecnico(self, symbol):
                log.warning(f"📉 [{symbol}] Cambio de tendencia detectado — Cierre forzado")
                self.cerrar_orden_simulada(symbol, precio_cierre, exito=False, motivo="Cambio de tendencia")
                return
            else:
                log.warning(f"🧠 [{symbol}] Cambio de tendencia detectado, pero filtros técnicos evitan cierre")
        
        # 🧠 Estrategia de salida activada
        resultado_salida = evaluar_salidas(orden, df, config=config_actual)
        if resultado_salida.get("cerrar", False):
            razon = resultado_salida.get("razon", "Estrategia desconocida")

            # Reevaluación técnica actualizada
            tendencia_actual = detectar_tendencia(symbol, df)
            evaluacion = evaluar_estrategias(symbol, df, tendencia_actual)
            estrategias_activas = evaluacion.get("estrategias_activas", {})
            puntaje = evaluacion.get("puntaje_total", 0)
            pesos_symbol = self.pesos_por_simbolo.get(symbol, {})
            umbral = calcular_umbral_adaptativo(symbol, df, estrategias_activas, pesos_symbol, config=config_actual)

            if not validar_necesidad_de_salida(df, orden, estrategias_activas, puntaje=puntaje, umbral=umbral, config=config_actual):
                log.warning(f"❌ [{symbol}] Cierre por '{razon}' evitado: condiciones técnicas aún válidas.")
                return

            log.info(f"🚪 [{symbol}] Estrategia de salida activada — {razon}")
            self.cerrar_orden_simulada(symbol, precio_cierre, exito="profit" in razon.lower(), motivo=f"Estrategia: {razon}")

    def cerrar_orden_simulada(self, symbol, precio_salida, exito, motivo):
        orden = self.ordenes_abiertas.pop(symbol, None)
        if not orden:
            return

        precio_entrada = orden["precio_entrada"]
        retorno_total = round((precio_salida - precio_entrada) / precio_entrada, 6)
        self.capital_simulado[symbol] *= (1 + retorno_total)

        ganancia = self.capital_simulado[symbol] * retorno_total
        self.resultados[symbol].append(ganancia)

        self.historial_cierres[symbol] = {
            "timestamp": datetime.utcnow().isoformat(),
            "motivo": motivo
        }

        orden.update({
            "precio_cierre": precio_salida,
            "retorno_total": retorno_total,
            "fecha_cierre": datetime.utcnow().isoformat(),
            "motivo_cierre": motivo
        })
        if not self.modo_optimizacion:
            self.guardar_orden_simulada(symbol, orden)
            self.historial_ordenes.append(orden)
            registrar_resultado_trade(symbol, orden, retorno_total)
            log.warning(f"📤 ORDEN SIMULADA CERRADA {symbol} a {precio_salida:.2f} | Motivo: {motivo}")

        if motivo.lower() in ["cambio de tendencia", "estrategia: cambio de tendencia"]:
            nueva_tendencia = detectar_tendencia(symbol, pd.DataFrame(self.buffer.get(symbol, [])))
            self.ultima_tendencia[symbol] = nueva_tendencia

        actualizar_pesos_estrategias_symbol(symbol)
        self.pesos_por_simbolo[symbol] = cargar_pesos_estrategias().get(symbol, {})

    def guardar_orden_simulada(self, symbol: str, nueva_orden: dict):
        archivo = f"ordenes_simuladas/{symbol.replace('/', '_').lower()}.json"
        temp_archivo = archivo + ".tmp"

        for intento in range(3):
            try:
                with self.lock_archivo:
                    ordenes = []
                    if os.path.exists(archivo):
                        with open(archivo, "r", encoding="utf-8") as f:
                            contenido = f.read().strip()
                            if contenido:
                                try:
                                    ordenes = json.loads(contenido)
                                    if not isinstance(ordenes, list):
                                        raise ValueError("El archivo debe contener una lista JSON.")
                                except Exception as e:
                                    raise ValueError(f"Archivo dañado: {e}")

                    # ✅ Evitar guardar duplicados exactos
                    if ordenes and nueva_orden == ordenes[-1]:
                        log.info(f"🔁 Orden duplicada ignorada en {archivo}")
                        return

                    ordenes.append(nueva_orden)

                    with open(temp_archivo, "w", encoding="utf-8") as f:
                        json.dump(ordenes, f, indent=2)

                    os.replace(temp_archivo, archivo)
                    log.info(f"💾 Orden simulada registrada en {archivo}")
                    return

            except (json.JSONDecodeError, ValueError) as e:
                try:
                    timestamp = int(time.time())
                    corrupto = archivo.replace(".json", f"_corrupto_{timestamp}.json")
                    os.rename(archivo, corrupto)
                    log.warning(f"⚠️ Archivo corrupto renombrado: {archivo} → {corrupto} — Error: {e}")
                except Exception as err:
                    log.error(f"❌ Error al renombrar archivo corrupto: {err}")
                ordenes = [nueva_orden]

            except PermissionError:
                log.error(f"⏳ Archivo en uso ({archivo}) — intento {intento + 1}/3")
                time.sleep(0.5)

            except Exception as e:
                log.error(f"❌ Error inesperado guardando orden simulada: {e}")
                return

        log.error(f"❌ No se pudo guardar la orden simulada para {symbol} tras 3 intentos.")

    def resumen_final(self):
        print("\n📈 RESUMEN FINAL DEL SIMULADOR")
        total_ganancia = 0
        total_ordenes = 0
        total_ganadoras = 0
        for symbol, operaciones in self.resultados.items():
            ganancia = sum(operaciones)
            ganadoras = len([x for x in operaciones if x > 0])
            total = len(operaciones)
            total_ordenes += total
            total_ganancia += ganancia
            total_ganadoras += ganadoras
            capital_final = self.capital_simulado[symbol]
            print(f"{symbol}: {total} operaciones | Winrate: {(ganadoras/total*100 if total else 0):.2f}% | Ganancia: {ganancia:.2f} | Capital final: {capital_final:.2f}€")

        print(f"\n🏁 TOTAL: {total_ordenes} operaciones | Winrate: {(total_ganadoras/total_ordenes*100 if total_ordenes else 0):.2f}% | Ganancia neta: {total_ganancia:.2f}€")

    def guardar_resultados(self):
        for symbol, ganancias in self.resultados.items():
            df = pd.DataFrame({"ganancia": ganancias})
            archivo = f"resultados/resultados_{symbol.replace('/', '_')}.csv"
            os.makedirs("resultados", exist_ok=True)
            df.to_csv(archivo, index=False)
            log.info(f"💾 Resultados guardados en {archivo}")

        ruta_historial = "ordenes_simuladas/ordenes_simuladas.json"
        if not os.path.exists(ruta_historial):
            with open(ruta_historial, "w", encoding="utf-8") as f:
                json.dump(self.historial_ordenes, f, indent=4)
            log.info(f"💾 Historial de órdenes guardado en {ruta_historial}")

    async def cerrar(self):
        self.resumen_final()
        self.guardar_resultados()

