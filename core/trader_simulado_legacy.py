import os
import time
import threading
import json
import pandas as pd
import numpy as np
import asyncio
from datetime import datetime

from core.logger import configurar_logger
from core.utils import validar_dataframe, segundos_transcurridos, distancia_minima_valida
from core.pesos import gestor_pesos
from core.tendencia import detectar_tendencia, señales_repetidas
from core.modo import MODO_REAL
from core.config_manager import Config
from core.adaptador_dinamico import calcular_umbral_adaptativo, calcular_tp_sl_adaptativos
from estrategias_entrada.gestor_entradas import evaluar_estrategias
from estrategias_salida.reajuste_tp_sl import calcular_promedios_sl_tp
from estrategias_salida.salida_trailing_stop import verificar_trailing_stop
from estrategias_salida.salida_por_tendencia import verificar_reversion_tendencia
from estrategias_salida.gestor_salidas import evaluar_salidas, verificar_filtro_tecnico
from estrategias_salida.salida_stoploss import verificar_salida_stoploss
from filtros.validador_entradas import evaluar_validez_estrategica
from filtros.filtro_salidas import validar_necesidad_de_salida
from binance_api.websocket import escuchar_velas
from aprendizaje.aprendizaje_en_linea import registrar_resultado_trade
from aprendizaje.entrenador_estrategias import actualizar_pesos_estrategias_symbol
from core.configuracion import cargar_configuracion_simbolo
from core.monitor_estado_bot import monitorear_estado_periodicamente
from core.ordenes_model import Orden
from core.kelly import calcular_fraccion_kelly
from indicadores.rsi import calcular_rsi
from indicadores.momentum import calcular_momentum
from indicadores.slope import calcular_slope
from config.config import INTERVALO_VELAS

log = configurar_logger("trader_simulado", modo_silencioso=True)


class TraderSimulado:
    def __init__(self, symbols, configuraciones=None, pesos_personalizados=None, modo_optimizacion=False):
        if isinstance(symbols, Config):
            self.config = symbols
            symbols = self.config.symbols
        else:
            self.config = None

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
        self.fecha_actual = datetime.utcnow().date()
        self.capital_inicial_diario = self.capital_simulado.copy()
        self.fraccion_kelly = calcular_fraccion_kelly()
        log.info(f"⚖️ Fracción Kelly: {self.fraccion_kelly:.4f}")

        self.configuraciones = configuraciones or {}
        self.pesos_personalizados = pesos_personalizados or {}
        self.pesos_por_simbolo = {}
        self.config_por_simbolo = {}

        for symbol in self.symbols:
            try:
                self.config_por_simbolo[symbol] = (
                    self.configuraciones.get(symbol) or cargar_configuracion_simbolo(symbol)
                )
            except ValueError as e:
                log.error(f"❌ {e}")
                raise

            self.pesos_por_simbolo[symbol] = (
                self.pesos_personalizados.get(symbol) or gestor_pesos.obtener_pesos_symbol(symbol)
            )
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
                raise

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
    
    def _tendencia_persistente(self, symbol, df, tendencia, velas=3):
        if len(df) < 30 + velas:
            return False
        for i in range(velas):
            sub_df = df.iloc[: -(velas - 1 - i)] if velas - 1 - i > 0 else df
            t, _ = detectar_tendencia(symbol, sub_df)
            if t != tendencia:
                return False
        return True

    def _validar_reentrada_tendencia(self, symbol, df, cierre, precio):
        if cierre.get("motivo") != "cambio de tendencia":
            return True
        tendencia = cierre.get("tendencia")
        if not tendencia:
            return False
        cierre_dt = pd.to_datetime(cierre.get("timestamp"))
        df_post = df[pd.to_datetime(df["timestamp"]) > cierre_dt]
        if len(df_post) < 3:
            log.info(f"⏳ [{symbol}] Esperando confirmación de tendencia")
            return False
        if not self._tendencia_persistente(symbol, df_post, tendencia, velas=3):
            log.info(f"⏳ [{symbol}] Tendencia {tendencia} no persistente tras cierre")
            return False
        precio_salida = cierre.get("precio")
        if precio_salida is not None and abs(precio - precio_salida) <= precio * 0.001:
            log.info(f"🚫 [{symbol}] Precio de entrada similar al de salida anterior")
            return False
        return True
    
    def ajustar_capital_diario(self, factor: float = 0.2, limite: float = 0.3) -> None:
        """Redistribuye el capital entre símbolos según el rendimiento diario."""
        total = sum(self.capital_simulado.values())
        pesos = {}
        for symbol in self.symbols:
            inicio = self.capital_inicial_diario.get(symbol, self.capital_simulado[symbol])
            final = self.capital_simulado[symbol]
            rendimiento = (final - inicio) / inicio if inicio else 0
            peso = 1 + factor * rendimiento
            peso = max(1 - limite, min(1 + limite, peso))
            pesos[symbol] = peso

        suma = sum(pesos.values()) or 1
        for symbol in self.symbols:
            self.capital_simulado[symbol] = round(total * pesos[symbol] / suma, 2)

        self.capital_inicial_diario = self.capital_simulado.copy()
        self.fecha_actual = datetime.utcnow().date()
        log.info(f"💰 Capital redistribuido: {self.capital_simulado}")

    async def ejecutar(self):
        self.tarea_estado = asyncio.create_task(monitorear_estado_periodicamente(self))

        async def callback(vela):
            await self.procesar_vela(vela)

        tareas = [escuchar_velas(s, INTERVALO_VELAS, callback) for s in self.symbols]
        await asyncio.gather(*tareas)

    async def procesar_vela(self, vela):
        symbol = vela["symbol"]
        async with self.locks[symbol]:
            if datetime.utcnow().date() != self.fecha_actual:
                self.ajustar_capital_diario()
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

            tendencia, _ = await asyncio.to_thread(detectar_tendencia, symbol, df)
            log.info(f"🔁 [{symbol}] Tendencia detectada: {tendencia}")
            direccion = "short" if tendencia == "bajista" else "long"

            evaluacion = await asyncio.to_thread(evaluar_estrategias, symbol, df, tendencia)
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
            repetidas = señales_repetidas(
                self.buffer[symbol], pesos_symbol, tendencia, volatilidad, ventanas=3
            )
            umbral = calcular_umbral_adaptativo(
                symbol,
                df,
                estrategias_detectadas,
                pesos_symbol,
                persistencia=repetidas,
                config=config_actual,
            )
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
            
            # Validación de reentrada por cambio de tendencia
            cierre = self.historial_cierres.get(symbol)
            if cierre and cierre.get("motivo") == "cambio de tendencia":
                precio_actual = float(df["close"].iloc[-1])
                if not self._validar_reentrada_tendencia(symbol, df, cierre, precio_actual):
                    self.historial_cierres[symbol]["velas"] = cierre.get("velas", 0) + 1
                    return
                else:
                    self.historial_cierres.pop(symbol, None)

            estrategias_activas = [k for k, v in estrategias_detectadas.items() if v]
            peso_total = sum(pesos_symbol.get(k, 0) for k in estrategias_activas)
            diversidad = len(estrategias_activas)

            log.info(f"⚖️ [{symbol}] Peso total: {peso_total:.2f} / {peso_min_total} — Diversidad: {diversidad} / {diversidad_min}")

            if puntaje < umbral:
                log.debug(f"🚫 [{symbol}] Entrada bloqueada: puntaje {puntaje:.2f} < umbral {umbral:.2f}")
                return

            if diversidad < diversidad_min or peso_total < peso_min_total:
                log.debug(f"🚫 [{symbol}] Entrada bloqueada por peso o diversidad insuficiente")
                return

            if not evaluar_validez_estrategica(symbol, df, estrategias_detectadas):
                log.debug(f"❌ [{symbol}] Rechazada por filtro estratégico")
                return

            ventana_close = df["close"].tail(10)
            media_close = np.mean(ventana_close)
            volatilidad = np.std(ventana_close) / media_close if media_close else 0

            repetidas = señales_repetidas(self.buffer[symbol], pesos_symbol, tendencia, volatilidad, ventanas=3)
            if repetidas < 2 and puntaje < 1.2 * umbral:
                log.debug(f"🚫 [{symbol}] Entrada rechazada por persistencia insuficiente y puntaje débil")
                return
            elif repetidas < 2:
                log.info(f"⚠️ [{symbol}] Entrada débil permitida con persistencia {repetidas}/5 insuficiente, puntaje alto")
            rsi = await asyncio.to_thread(calcular_rsi, df)
            momentum = await asyncio.to_thread(calcular_momentum, df)
            slope = await asyncio.to_thread(calcular_slope, df)

            if puntaje < umbral or not any(estrategias_detectadas.values()):
                log.debug(f"🚫 [{symbol}] Rechazada por filtros básicos")
                return

            if self.riesgo_superado_simulado(symbol, config):
                log.warning(f"🔒 [{symbol}] Riesgo diario superado, entrada bloqueada")
                return

            log.info(f"✅ [{symbol}] Entrada CONFIRMADA — Puntaje: {puntaje:.2f}, Peso: {peso_total:.2f}, Diversidad: {diversidad}, Persistencia: {repetidas}/5")

            precio = float(vela["close"])
            sl, tp = calcular_tp_sl_adaptativos(
                symbol,
                df,
                config,
                self.capital_simulado.get(symbol, 0),
                precio,
            )
            
            if not distancia_minima_valida(precio, sl, tp):
                log.warning(
                    f"📏 [{symbol}] Distancia SL/TP insuficiente. SL: {sl:.2f} TP: {tp:.2f}"
                )
                return

            if not estrategias_detectadas or not any(estrategias_detectadas.values()):
                log.warning(f"⚠️ [{symbol}] Entrada ignorada — sin estrategias activas válidas")
                return

            self.ordenes_abiertas[symbol] = Orden(
                symbol=symbol,
                precio_entrada=precio,
                stop_loss=sl,
                take_profit=tp,
                timestamp=datetime.utcnow().isoformat(),
                estrategias_activas=estrategias_detectadas,
                max_price=precio,
                tendencia=tendencia,
                cantidad=0.0,
                direccion=direccion,
            )

            if not self.modo_optimizacion:
                self.guardar_orden_simulada(symbol, self.ordenes_abiertas[symbol].__dict__.copy())
                actualizar_pesos_estrategias_symbol(symbol)
                self.pesos_por_simbolo[symbol] = gestor_pesos.obtener_pesos_symbol(symbol)
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
        stop_loss = orden.stop_loss
        take_profit = orden.take_profit
        entrada = orden.precio_entrada

        log.info(f"🔎 [{symbol}] Verificando cierre — Precio actual: {precio_cierre:.2f}, SL: {stop_loss:.2f}, TP: {take_profit:.2f}")

        # ⏱️ Tiempo máximo abierto (6h), pero solo cerrar si está en ganancia
        timestamp_orden = orden.timestamp
        if timestamp_orden and segundos_transcurridos(timestamp_orden) > 6 * 3600:
            retorno_actual = (precio_cierre - orden.precio_entrada) / orden.precio_entrada
            if retorno_actual > 0:
                log.warning(f"⌛ [{symbol}] Orden expirada en ganancia — Retorno: {retorno_actual:.4f}")
                await self.cerrar_orden_simulada(symbol, precio_cierre, exito=True, motivo="Expirada")
                return
            else:
                log.info(f"🕒 [{symbol}] Orden vencida pero aún en pérdida (retorno {retorno_actual:.4f}), se mantiene abierta")


        # 🛑 Stop Loss con validación
        config_actual = self.config_por_simbolo.get(symbol, {})
        if precio_min <= stop_loss:
            log.info(f"🛑 [{symbol}] Posible SL activado — Precio mínimo: {precio_min:.2f}")
            resultado = verificar_salida_stoploss(orden.__dict__, df, config=config_actual)

            if resultado.get("cerrar", False):
                log.info(f"🟥 [{symbol}] SL confirmado — {resultado.get('motivo', '')}")
                await self.cerrar_orden_simulada(symbol, stop_loss, exito=False, motivo="Stop Loss")
            else:
                if resultado.get("evitado", False):
                    log.debug("SL evitado correctamente, no se notificará por Telegram")
                    log.warning(f"🛡️ [{symbol}] SL evitado — {resultado.get('motivo', 'sin razón')}")
                else:
                    log.info(f"ℹ️ [{symbol}] {resultado.get('motivo', '')}")
            return

        # 🎯 Take Profit
        if precio_max >= take_profit:
            log.info(f"🎯 [{symbol}] TP alcanzado — Cerrando por Take Profit")
            await self.cerrar_orden_simulada(symbol, take_profit, exito=True, motivo="Take Profit")
            return

        # 🔃 Trailing Stop
        cerrar, motivo = verificar_trailing_stop(orden.__dict__, precio_cierre, config=config_actual)
        if cerrar:
            log.info(f"🔃 [{symbol}] Trailing Stop activado — {motivo}")
            await self.cerrar_orden_simulada(symbol, precio_cierre, exito=True, motivo=motivo)
            return

        # 📉 Reversión de tendencia
        if verificar_reversion_tendencia(symbol, df, self.ultima_tendencia.get(symbol)):
            pesos_symbol = self.pesos_por_simbolo.get(symbol, {})
            if not verificar_filtro_tecnico(
                symbol,
                df,
                orden.estrategias_activas,
                pesos_symbol,
                config=config_actual,
            ):
                log.warning(f"📉 [{symbol}] Cambio de tendencia detectado — Cierre forzado")
                await self.cerrar_orden_simulada(symbol, precio_cierre, exito=False, motivo="Cambio de tendencia")
                return
            else:
                log.warning(f"🧠 [{symbol}] Cambio de tendencia detectado, pero filtros técnicos evitan cierre")

        # 🧠 Estrategia de salida activada
        resultado_salida = evaluar_salidas(orden.__dict__, df, config=config_actual)
        if resultado_salida.get("cerrar", False):
            razon = resultado_salida.get("razon", "Estrategia desconocida")

            # Reevaluación técnica actualizada
            tendencia_actual, _ = await asyncio.to_thread(detectar_tendencia, symbol, df)
            evaluacion = await asyncio.to_thread(evaluar_estrategias, symbol, df, tendencia_actual)
            estrategias_activas = evaluacion.get("estrategias_activas", {})
            puntaje = evaluacion.get("puntaje_total", 0)
            pesos_symbol = self.pesos_por_simbolo.get(symbol, {})
            umbral = calcular_umbral_adaptativo(
                symbol,
                df,
                estrategias_activas,
                pesos_symbol,
                persistencia=0.0,
                config=config_actual,
            )

            if not validar_necesidad_de_salida(df, orden.__dict__, estrategias_activas, puntaje=puntaje, umbral=umbral, config=config_actual):
                log.warning(f"❌ [{symbol}] Cierre por '{razon}' evitado: condiciones técnicas aún válidas.")
                return

            log.info(f"🚪 [{symbol}] Estrategia de salida activada — {razon}")
            await self.cerrar_orden_simulada(symbol, precio_cierre, exito="profit" in razon.lower(), motivo=f"Estrategia: {razon}")

    async def cerrar_orden_simulada(self, symbol, precio_salida, exito, motivo):
        orden = self.ordenes_abiertas.pop(symbol, None)
        if not orden:
            return

        precio_entrada = orden.precio_entrada
        retorno_total = round((precio_salida - precio_entrada) / precio_entrada, 6)
        capital_inicial = self.capital_simulado[symbol]
        invertido = capital_inicial * self.fraccion_kelly
        ganancia = invertido * retorno_total
        self.capital_simulado[symbol] = capital_inicial + ganancia
        self.resultados[symbol].append(ganancia)

        self.historial_cierres[symbol] = {
            "timestamp": datetime.utcnow().isoformat(),
            "motivo": motivo.lower().strip(),
            "precio": precio_salida,
            "tendencia": None,
        }

        orden.precio_cierre = precio_salida
        orden.retorno_total = retorno_total
        orden.fecha_cierre = datetime.utcnow().isoformat()
        orden.motivo_cierre = motivo
        if not self.modo_optimizacion:
            self.guardar_orden_simulada(symbol, orden.__dict__)
            self.historial_ordenes.append(orden.__dict__) 
            registrar_resultado_trade(symbol, orden.__dict__, retorno_total)
            log.warning(f"📤 ORDEN SIMULADA CERRADA {symbol} a {precio_salida:.2f} | Motivo: {motivo}")

        if motivo.lower() in ["cambio de tendencia", "estrategia: cambio de tendencia"]:
            nueva_tendencia, _ = await asyncio.to_thread(
                detectar_tendencia,
                symbol,
                pd.DataFrame(self.buffer.get(symbol, []))
            )
            self.ultima_tendencia[symbol] = nueva_tendencia
            if symbol in self.historial_cierres:
                self.historial_cierres[symbol]["tendencia"] = nueva_tendencia   


        actualizar_pesos_estrategias_symbol(symbol)
        self.pesos_por_simbolo[symbol] = gestor_pesos.obtener_pesos_symbol(symbol)

    def guardar_orden_simulada(self, symbol: str, nueva_orden: dict):
        archivo = f"ordenes_simuladas/{symbol.replace('/', '_').lower()}.parquet"
        temp_archivo = archivo + ".tmp"

        for intento in range(3):
            try:
                with self.lock_archivo:
                    ordenes = []
                    if os.path.exists(archivo):
                        try:
                            df = pd.read_parquet(archivo)
                            ordenes = df.to_dict("records")
                        except Exception as e:
                            log.error(f"❌ Error leyendo {archivo}: {e}")
                            raise ValueError(f"Archivo dañado: {e}")

                    # ✅ Evitar guardar duplicados exactos
                    if ordenes and nueva_orden == ordenes[-1]:
                        log.info(f"🔁 Orden duplicada ignorada en {archivo}")
                        return

                    ordenes.append(nueva_orden)

                    df_guardar = pd.DataFrame(ordenes)
                    if "estrategias_activas" in df_guardar.columns:
                        df_guardar["estrategias_activas"] = df_guardar["estrategias_activas"].apply(
                            lambda v: json.dumps(v) if isinstance(v, dict) else v
                        )
                    if "tendencia" in df_guardar.columns:
                        df_guardar["tendencia"] = df_guardar["tendencia"].apply(
                            lambda v: v[0] if isinstance(v, tuple) else v
                        )

                    df_guardar.to_parquet(temp_archivo, index=False)

                    os.replace(temp_archivo, archivo)
                    log.info(f"💾 Orden simulada registrada en {archivo}")
                    return

            except ValueError as e:
                try:
                    timestamp = int(time.time())
                    corrupto = archivo.replace(".parquet", f"_corrupto_{timestamp}.parquet")
                    os.rename(archivo, corrupto)
                    log.warning(f"⚠️ Archivo corrupto renombrado: {archivo} → {corrupto} — Error: {e}")
                except OSError as err:
                    log.error(f"❌ Error al renombrar archivo corrupto: {err}")
                    raise
                ordenes = [nueva_orden]

            except PermissionError:
                log.error(f"⏳ Archivo en uso ({archivo}) — intento {intento + 1}/3")
                time.sleep(0.5)

            except Exception as e:
                log.error(f"❌ Error inesperado guardando orden simulada: {e}")
                raise

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

        ruta_historial = "ordenes_simuladas/ordenes_simuladas.parquet"
        if not os.path.exists(ruta_historial):
            pd.DataFrame(self.historial_ordenes).to_parquet(ruta_historial, index=False)
            log.info(f"💾 Historial de órdenes guardado en {ruta_historial}")

    async def cerrar(self):
        self.resumen_final()
        self.guardar_resultados()
