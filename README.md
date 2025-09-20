# Documentación Completa del Bot de Trading

## 1. Introducción General
1. Este bot de trading está diseñado para operar de forma autónoma en mercados de criptomonedas.
2. El objetivo principal es ejecutar estrategias cuantitativas con gestión de riesgo adaptable.
3. El proyecto abarca backend, frontend, motor de trading, módulos de aprendizaje y backtesting.
4. La arquitectura modular permite extender componentes sin afectar el núcleo.
5. Se prioriza la transparencia para que cualquier desarrollador pueda entender el sistema.

## 2. Visión de Arquitectura
6. El **núcleo** (`core/`) contiene el motor de estrategias, alimentación de datos y gestión de órdenes.
7. El **backend** (`backend/`) ofrece APIs y panel administrativo basados en Django.
8. El **frontend** (`frontend/`) es una aplicación React con TailwindCSS para visualizar métricas y estado.
9. El **aprendizaje** (`learning/`) incluye scripts de calibración y entrenamiento continuo.
10. El **backtesting** (`backtesting/`) permite validar estrategias con datos históricos.
11. Los **indicadores** (`indicators/` y `core/indicadores/`) calculan señales técnicas.
12. Las **configuraciones** (`config/`) controlan parámetros globales y pesos dinámicos.
13. El **directorio de estado** (`estado/`) persiste capital y cierres para continuidad operacional.
14. El **directorio de scripts** (`scripts/`) alberga utilidades administrativas.
15. Los **tests** (`tests/`) cubren integración, reconexiones, capital y watchdog.

## 3. Estructura de Carpetas
16. `backtesting/` ejecuta simulaciones históricas.
17. `backend/` contiene el servidor web y tareas programadas.
18. `binance_api/` implementa comunicación con Binance: `cliente.py`, `filters.py` y `websocket.py`.
19. `config/` gestiona configuraciones y pesos con archivos JSON y Python.
20. `core/` implementa el motor de trading modular.
21. `docs/` almacena documentación auxiliar.
22. `estado/` mantiene el capital y cierre de operaciones entre ejecuciones.
23. `frontend/` provee el panel de control interactivo.
24. `indicators/` define indicadores técnicos reutilizables.
25. `learning/` orquesta procesos de aprendizaje automático.
26. `scripts/` aloja utilidades como auditorías y migraciones.
27. `tests/` incluye pruebas automatizadas.
28. `main.py` inicia el bot unificando todos los componentes.
29. `requirements.txt` lista dependencias Python globales.
30. `SECURITY.md` detalla políticas de seguridad.

## 4. Backend (Django)
31. El backend sigue el patrón estándar de Django para crear APIs seguras.
32. `backend/manage.py` expone comandos administrativos.
33. `backend/backend/` aloja la configuración del proyecto Django.
34. Dentro del proyecto se definen middlewares y settings por ambiente.
35. El submódulo `tareas/` implementa jobs periódicos.
36. El submódulo `users/` gestiona autenticación y modelos de usuario.
37. Las dependencias específicas se listan en `backend/requirements.txt`.
38. Las APIs permiten consultar estrategias, métricas y configuración.
39. El panel administrativo facilita la gestión manual de datos.
40. Las tareas programadas interactúan con el motor central mediante colas o HTTP.

## 5. Frontend (React + Tailwind)
41. El frontend ofrece un dashboard para monitorear el bot.
42. Se inicia con `npm start --prefix frontend`.
43. `frontend/src/` contiene componentes React organizados por vistas.
44. `frontend/public/` aloja archivos estáticos y `index.html`.
45. `package.json` define scripts de desarrollo y build.
46. `tailwind.config.js` configura estilos utilitarios.
47. `postcss.config.js` habilita procesamiento CSS.
48. El entorno incluye pruebas básicas ejecutadas con `npm test --prefix frontend`.
49. El panel muestra capital, posiciones, registros y alertas.
50. Las llamadas al backend usan `fetch` o bibliotecas HTTP.
51. El frontend permite ajustar parámetros y visualizar logs.

## 6. Motor de Trading (core/)
52. `core/trader_modular.py` implementa la clase `Trader` que orquesta la sesión.
53. `core/trader_simulado.py` permite ejecutar el bot en modo simulado.
54. `data_feed/lite.py` gestiona la suscripción a datos de mercado en tiempo real.
55. `core/event_bus.py` distribuye eventos entre componentes desacoplados.
56. `core/ordenes_reales.py` es un stub utilizado en pruebas para simular la ejecución de órdenes reales.
57. `core/orders/` contiene estructuras y helpers para manejar órdenes.
58. `core/estrategias.py` administra el registro y ejecución de estrategias.
59. `core/strategies/` incluye implementaciones concretas de estrategias.
60. `core/risk/risk_manager.py` calcula límites de exposición y gestiona stop-loss.
61. `core/risk/` agrupa políticas de riesgo reutilizables.
62. `core/adaptador_dinamico.py` ajusta parámetros en caliente según condiciones.
63. `core/adaptador_umbral.py` define reglas umbral para activar señales.
64. `core/adaptador_configuracion_dinamica.py` sincroniza cambios de configuración externos.
65. `core/ajustador_riesgo.py` recalibra el riesgo basándose en métricas recientes.
66. `core/capital_manager.py` controla el saldo disponible y distribución por estrategia.
67. `core/position_manager.py` sigue las posiciones abiertas y su PnL.
68. `core/registro_metrico.py` guarda métricas operativas en persistencia.
69. `core/reporting.py` genera reportes de desempeño.
70. `core/supervisor.py` vigila la salud del bot y gestiona reinicios.
71. `core/monitor_estado_bot.py` ofrece un endpoint para monitorear vida del proceso.
72. `core/notificador.py` envía mensajes a sistemas externos (Telegram, correo).
73. `core/notification_manager.py` centraliza diferentes canales de notificación.
74. `core/auditoria.py` almacena bitácoras detalladas de cada operación.
75. `core/rejection_handler.py` maneja órdenes rechazadas y reintentos.
76. `core/evaluacion_tecnica.py` calcula puntuaciones basadas en indicadores.
77. `core/score_tecnico.py` produce un score global a partir de señales.
78. `core/scoring.py` combina resultados de múltiples estrategias.
79. `core/metricas_semanales.py` resume estadísticas semanales.
80. `core/procesar_vela.py` transforma velas brutas en eventos de análisis.
81. `core/hot_reload.py` permite recargar módulos sin detener el bot.
82. `core/contexto_externo.py` provee datos externos para estrategias.
83. `core/mode.py` selecciona entre modos real y simulado.
84. `core/market_regime.py` detecta el régimen del mercado actual.
85. `core/persistencia_tecnica.py` guarda resultados de indicadores en almacenamiento.
86. `core/data/` aloja datos auxiliares utilizados por el motor.
87. `core/utils/` contiene funciones generales de apoyo.
88. `core/config_manager/` centraliza lectura de parámetros en ejecución.
89. La comunicación interna se basa en `asyncio` para operación concurrente.
77. `core/scoring.py` produce un score global a partir de señales.
78. `core/metricas_semanales.py` resume estadísticas semanales.
79. `core/procesar_vela.py` transforma velas brutas en eventos de análisis.
80. `core/hot_reload.py` permite recargar módulos sin detener el bot.
81. `core/contexto_externo.py` provee datos externos para estrategias.
82. `core/mode.py` selecciona entre modos real y simulado.
83. `core/market_regime.py` detecta el régimen del mercado actual.
84. `core/persistencia_tecnica.py` guarda resultados de indicadores en almacenamiento.
85. `core/data/` aloja datos auxiliares utilizados por el motor.
86. `core/utils/` contiene funciones generales de apoyo.
101. `ichimoku.py` implementa el sistema de Ichimoku Kinko Hyo.
102. `incremental.py` gestiona cálculos incrementales eficientes.
103. `macd.py` produce la convergencia y divergencia de medias móviles.
104. `medias.py` ofrece funciones para promedios simples, exponenciales y ponderados.
105. `momentum.py` evalúa la velocidad de cambio de precios.
106. `rango.py` mide rangos de volatilidad.
107. `rsi.py` calcula el índice de fuerza relativa.
108. `slope.py` obtiene pendientes de series para analizar tendencias.
109. `sma.py` y `sma_bajista.py` aplican medias móviles simples.
110. `volatilidad.py` deriva medidas de variación estándar.
111. `volumen.py` analiza volumen negociado para detectar entradas.
112. `retornos_volatilidad.py` correlaciona retornos con variaciones de precio.
113. `vwap.py` calcula el precio promedio ponderado por volumen.

## 8. Gestión de Configuración (config/)
114. `config/config_manager.py` expone la clase `ConfigManager` para leer parámetros desde `.env`.
115. `config/config.py` define estructuras de configuración.
116. `config/configuracion.py` centraliza valores por defecto.
117. `development.py` y `production.py` establecen ajustes por entorno.
118. `claves.env` almacena claves de API cifradas.
119. `configuraciones_base.json` recopila valores iniciales.
120. `configuraciones_optimas.json` guarda resultados de calibraciones.
121. `estrategias_pesos.json` define pesos por estrategia.
122. `estrategias_pesos_base.json` proporciona pesos iniciales para nuevas calibraciones.
123. `pesos_tecnicos.json` determina la importancia de cada indicador.
124. `pesos_salidas.json` controla parámetros de salida.
125. `riesgo.json` establece límites de exposición.
126. `exit_defaults.py` define valores por defecto de stop-loss y take-profit.
127. Los archivos `*_fecha.txt` marcan fechas de reseteos diarios.
128. Los locks como `riesgo.json.lock` previenen concurrencia simultánea.
129. La configuración se recarga dinámicamente gracias a adaptadores del núcleo.

## 9. Persistencia y Estado
130. `estado/capital.json` rastrea el balance disponible del bot.
131. `estado/historial_cierres.json` registra operaciones cerradas para análisis.
132. `core/persistencia_tecnica.py` escribe resultados de indicadores en el disco.
133. `docs/storage_consistency.md` documenta la consistencia del almacenamiento.
134. Se recomienda ejecutar auditorías periódicas mediante `scripts/auditor.py`.
135. La persistencia de datos históricos se realiza en formato Parquet o bases de datos.

## 10. Módulos de Aprendizaje (learning/)
136. `aprendizaje_continuo.py` implementa reentrenamiento periódico con datos recientes.
137. `aprendizaje_en_linea.py` ajusta modelos en tiempo real mientras el bot opera.
138. `analisis_resultados.py` genera reportes de desempeño post-entrenamiento.
139. `entrenador_estrategias.py` optimiza parámetros de cada estrategia.
140. `entrenar_sl_tp_rl.py` entrena niveles de stop-loss y take-profit con aprendizaje reforzado.
141. `recalibrar_semana.py` ajusta pesos semanalmente según resultados.
142. `reset_configuracion.py` restaura configuraciones predeterminadas.
143. `reset_pesos.py` limpia ponderaciones de indicadores bajo ciertas condiciones.
144. `async_learning_manager.py` y `trade_results_manager.py` coordinan procesos de aprendizaje y registro.
145. `calibrar_parametros_adaptativos.py` busca valores óptimos para adaptadores.
146. Los módulos de aprendizaje se integran con `core` mediante APIs internas.
147. Los resultados se guardan en `configuraciones_optimas.json` para uso posterior.

## 11. Backtesting
148. `backtesting/backtest.py` ejecuta simulaciones completas de estrategias.
149. `backtesting/backtest_ventanas.py` prueba estrategias en ventanas deslizantes.
150. `backtesting/descargar_velas_binance.py` obtiene datos históricos desde Binance.
151. `backtesting/engine.py` provee utilidades compartidas para simulaciones.
152. `backtesting/op.py` modela operaciones hipotéticas durante el backtest.
153. Las simulaciones permiten evaluar rendimiento y ajustar parámetros antes de producción.
154. Los resultados alimentan los archivos de configuración óptima.

## 12. Scripts Utilitarios
155. `scripts/benchmark_flush.py` mide desempeño de operaciones de escritura.
156. `scripts/migrate_parquet_to_db.py` migra datos Parquet a bases de datos.
157. `scripts/refactor_imports.py` reorganiza importaciones automáticamente.
158. `scripts/parche.py` aplica correcciones rápidas sobre datos o config.
159. `scripts/reporte_semanal.py` genera un informe semanal de métricas clave.
160. `scripts/auditor.py` revisa integridad de datos y estados.
161. `scripts/cancell.py` cancela órdenes pendientes en masa si es necesario.

## 13. Pruebas Automatizadas
162. `tests/conftest.py` define fixtures compartidas para pytest.
163. `tests/test_capital.py` valida cálculos de capital y riesgo.
164. `tests/test_e2e.py` cubre un flujo de extremo a extremo del bot.
165. `tests/test_reconexiones.py` verifica la reconexión al exchange tras fallos.
166. `tests/test_tendencia.py` comprueba la detección de tendencias.
167. `tests/test_watchdog.py` asegura que el bot responde a caídas de procesos.
168. Las pruebas se ejecutan con `pytest` desde la raíz del proyecto.
169. Se recomienda ejecutar `python backend/manage.py test` para validar el backend.
170. El frontend se prueba con `npm test --prefix frontend`.

## 14. Ejecución del Bot
171. Clonar el repositorio y crear un entorno virtual con Python 3.10 o superior.
172. Instalar dependencias con `pip install -r requirements.txt`.
173. Configurar variables de entorno en `config/claves.env` y archivos JSON.
174. Opcional: instalar dependencias del backend y frontend.
175. Ejecutar `python main.py` para iniciar el bot.
176. El script inicializa supervisión, hot reload y carga de configuración.
177. Al iniciar, se muestran mensajes indicando si el modo es real o simulado.
178. El bot escucha señales `SIGINT` y `SIGTERM` para detenerse limpiamente.
179. Se pueden reiniciar módulos sin detener el proceso gracias a `hot_reload`.
180. Logs detallados se imprimen en consola y se persisten en archivos de auditoría.

## 15. Despliegue y Operación
181. Para producción se recomienda ejecutar el bot como servicio `systemd`.
182. Configurar reinicios automáticos en caso de fallo.
183. Mantener copias de seguridad de `estado/` y `config/`.
184. Monitorizar el proceso mediante `core/monitor_estado_bot.py`.
185. Integrar alertas con `core/notificador.py` para recibir mensajes de error.
186. Actualizar dependencias de manera controlada en entornos aislados.

## 16. Seguridad
187. Las claves de API se guardan en `claves.env` y nunca se versionan.
188. `SECURITY.md` define políticas de manejo de credenciales.
189. Limitar permisos de la cuenta de exchange a lo estrictamente necesario.
190. Evitar ejecutar el bot con privilegios de superusuario.
191. Revisar periódicamente dependencias con herramientas de análisis de vulnerabilidades.
192. Cifrar respaldos que contengan información sensible.

## 17. Buenas Prácticas de Desarrollo
193. Seguir PEP8 y convenciones de estilo para Python.
194. Escribir docstrings claros explicando responsabilidades de cada módulo.
195. Añadir pruebas unitarias al introducir nuevas funcionalidades.
196. Ejecutar `pytest` antes de cada commit.
197. Documentar cambios relevantes en este README y en `docs/`.
198. Mantener sincronizadas las configuraciones de desarrollo y producción.
199. Usar ramas descriptivas y Pull Requests para revisar código.

## 18. Flujo de Datos Interno
200. El `data_feed` recibe velas o ticks del exchange.
201. Los datos se envían al `event_bus` para distribución.
202. Estrategias suscritas procesan los eventos y generan señales.
203. El `risk_manager` evalúa las señales y determina tamaño de posición.
204. `ordenes_reales` o el simulador ejecutan las órdenes.
205. El `position_manager` actualiza estados de posiciones abiertas.
206. `registro_metrico` registra métricas en persistencia.
207. `notificador` envía resúmenes o alertas.
208. El `supervisor` verifica que los componentes respondan adecuadamente.
209. `monitor_estado_bot` expone un endpoint de salud para sistemas externos.

## 19. Gestión de Errores
210. `rejection_handler.py` reintenta órdenes rechazadas con backoff.
211. Excepciones críticas se registran en `auditoria.py`.
212. Hot reload se detiene automáticamente ante errores persistentes.
213. `supervisor.py` reinicia el bot si detecta bloqueos.
214. Las estrategias deben manejar sus propias excepciones para no detener el flujo global.
215. Los logs incluyen trazas completas para facilitar debugging.

## 20. Integración con Exchanges
216. `binance_api/cliente.py` encapsula la API REST de Binance.
217. `binance_api/websocket.py` gestiona conexiones de streaming.
218. `binance_api/filters.py` valida límites y parámetros de órdenes.
219. Los adaptadores pueden modificarse para soportar otros exchanges.
220. La arquitectura separa la lógica del exchange para facilitar pruebas.

## 21. Métricas y Reporting
221. `core/metrics.py` define métricas básicas de rendimiento.
222. `metricas_semanales.py` genera resúmenes por semana.
223. `reporting.py` produce informes con estadísticas clave.
224. `scripts/reporte_semanal.py` automatiza la generación y envío de reportes.
225. Los informes se integran con el frontend y el backend.

## 22. Hot Reload y Supervisión
226. `hot_reload.py` observa cambios en archivos Python.
227. Al detectar modificaciones, recarga módulos sin reiniciar el proceso.
228. `supervisor.py` crea tareas supervisadas y reinicia componentes caídos.
229. `monitor_estado_bot.py` expone el estado a través de un puerto configurable.
230. El flujo de supervisión está pensado para alta disponibilidad.

## 23. Extensibilidad de Estrategias
231. Nuevas estrategias se agregan en `core/strategies/` siguiendo una interfaz común.
232. Las estrategias reciben datos del `event_bus` y devuelven señales normalizadas.
233. Se pueden combinar varias estrategias mediante `scoring.py`.
234. Los pesos de cada estrategia se ajustan en `config/estrategias_pesos.json`.
235. El bot soporta activación o desactivación dinámica de estrategias.
236. Los resultados de backtesting alimentan la selección de estrategias activas.

## 24. Gestión de Riesgo Avanzada
237. `ajustador_riesgo.py` analiza métricas recientes para reducir exposición en mercados volátiles.
238. `adaptador_umbral.py` bloquea entradas si los indicadores superan límites críticos.
239. `capital_manager.py` distribuye capital entre estrategias según su rendimiento.
240. `risk/risk_manager.py` establece niveles de stop-loss y take-profit por operación.
241. `position_manager.py` verifica que el tamaño de posición no exceda el capital disponible.
242. `market_regime.py` detecta ambientes alcistas, bajistas o laterales para ajustar el riesgo.
243. Los módulos de riesgo trabajan coordinados con configuraciones externas.

## 25. Indicadores Internos del Núcleo
244. `core/indicadores/` incluye versiones optimizadas para el motor en tiempo real.
245. Estos indicadores se calculan de forma incremental para eficiencia.
246. Los resultados se integran con `evaluacion_tecnica.py`.
247. `indicadores` externos pueden ser adaptados para uso interno.

## 26. Config Manager Interno
248. `core/config_manager/` escucha cambios de archivos y los aplica en caliente.
249. Permite modificar pesos o límites sin reiniciar el bot.
250. Se integra con `adaptador_configuracion_dinamica.py`.
251. Los cambios se registran para auditoría y análisis.

## 27. Orquestación de Órdenes
252. `orders/` define estructuras de orden, resultados y estados.
253. `ordenes_reales.py` convierte señales en órdenes firmadas para el exchange.
254. `rejection_handler.py` monitorea respuestas y reintenta según políticas.
255. `trader_modular.py` coordina el envío de órdenes y el seguimiento de fills.
256. `trader_simulado.py` emula fills usando datos históricos.

## 28. Administración de Sesión
257. `main.py` es el punto de entrada del sistema.
258. Inicializa hot reload, supervisor y configuración.
259. Llama a `Trader.ejecutar()` para iniciar el ciclo principal.
260. Maneja señales del sistema para apagarlo ordenadamente.
261. Después de cada parada se limpia el estado y se cierran conexiones.
262. Los banner y mensajes de inicio ayudan a identificar modo y versión.

## 29. Utilidades Generales
263. `core/utils/` alberga funciones auxiliares como manejo de fechas y formateo.
264. `scripts/refactor_imports.py` ayuda a mantener un estilo coherente.
265. `docs/debugging.py` ofrece herramientas para depurar.
266. `docs/exit_flow.md` describe el flujo de salida de operaciones.
267. `docs/calibracion_parametros.md` explica el proceso de calibración de parámetros.
268. `binance_api/filters.py` garantiza que las órdenes cumplan reglas del exchange.

## 30. Ciclo de Vida de Estrategias
269. Las estrategias se inician al cargar el `Trader`.
270. Cada estrategia suscrita recibe eventos de mercado.
271. Generan señales positivas, negativas o neutras.
272. Los scores se combinan para decidir operaciones.
273. `ajustador_riesgo.py` puede reducir el score si el riesgo es alto.
274. Si el score supera el umbral, se crea una orden.
275. El estado de la operación se rastrea hasta su cierre.
276. Los resultados impactan en los pesos para futuras decisiones.

## 31. Extensión y Contribuciones
277. Se aceptan Pull Requests que sigan estándares de estilo y pruebas.
278. Documentar cualquier nueva carpeta o módulo agregado.
279. Mantener este README actualizado con nuevas funcionalidades.
280. Añadir indicadores personalizados en `indicators/` o `core/indicadores/`.
281. Extender el frontend con vistas adicionales para nuevas métricas.
282. Crear endpoints en el backend para integraciones externas.
283. Incluir pruebas que cubran todos los caminos críticos.
284. Ejecutar linters y formateadores antes de contribuir.

## 32. Roadmap Futuro
285. Integrar más exchanges mediante adaptadores genéricos.
286. Añadir soporte para futuros perpetuos y margin trading.
287. Incorporar aprendizaje por refuerzo profundo para decisiones complejas.
288. Mejorar la interfaz del dashboard con gráficos en tiempo real.
289. Implementar despliegues automáticos con contenedores.
290. Añadir monitoreo de latencia y métricas de rendimiento.
291. Crear un módulo de simulación en papel para estrategias manuales.
292. Publicar un paquete pip con componentes reutilizables.
293. Documentar tutoriales paso a paso en `docs/`.
294. Establecer un sistema de plugins para estrategias de terceros.

## 33. Créditos y Licencia
295. Proyecto desarrollado por un equipo de entusiastas del trading algorítmico.
296. Se distribuye bajo una licencia que permite modificación con atribución.
297. Las contribuciones externas son bienvenidas bajo el código de conducta del proyecto.
298. Agradecimientos a la comunidad open source por librerías y herramientas.
299. Revise `SECURITY.md` para lineamientos de divulgación responsable.
300. Para consultas o soporte, utilice los canales definidos en el repositorio.
301. Mantenga siempre copias de seguridad y pruebe en entornos controlados.
302. ¡Feliz trading y contribuciones seguras!
