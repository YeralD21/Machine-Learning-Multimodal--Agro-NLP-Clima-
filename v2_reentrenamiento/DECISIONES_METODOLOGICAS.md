## Coordenadas climáticas — granularidad provincial (decisión aprobada, evidencia completa)

**Proceso (3 pasos de investigación previos a la decisión):**
1. Búsqueda de fuente de coordenadas distritales con UBIGEO 6 dígitos.
2. Análisis de heterogeneidad geográfica de distritos productores (top-10
   provincias de Limón Sutil).
3. Estimación de costo del clima a nivel distrito.

**Fuente distrital — SÍ existe (la investigación la encontró, no se asumió su ausencia):**
- `jmcastagnetto/ubigeo-peru-aumentado` → `ubigeo_distrito.csv`
  (`https://raw.githubusercontent.com/jmcastagnetto/ubigeo-peru-aumentado/main/ubigeo_distrito.csv`,
  409,857 bytes, HTTP 200 verificado). Columnas clave: `inei` (UBIGEO INEI
  6 dígitos), `latitude`, `lon`, `altitude`, `capital`, `superficie`. Cruza
  directo con `COD_UBIGEO` de MIDAGRI. No es un archivo oficial de INEI
  (tabulación derivada de CEPLAN/INEI/MINSA, DOI Zenodo, MIT), pero es la
  única fuente con lat/lon ya calculadas listas para cruce.
- Ruta 100% INEI: capa oficial de geometría distrital
  `https://ide.inei.gob.pe/files/Distrito.rar` (GeoPackage, actualizado 2023,
  ~12.4 MB, HTTP 200) con UBIGEO en atributos; el centroide debe computarse
  (requiere geopandas/GDAL). No existe centroide distrital precalculado y
  descargable del INEI; los códigos oficiales (sin coordenadas) están en
  SISCONCODE (`https://webapp.inei.gob.pe:8443/sisconcode/main.htm`).

**Heterogeneidad de distritos productores (Paso 2) — evidencia:**
criterio: distritos con PRODUCCION acumulada 2016-2025 > 0
(`limon_sutil_distrito_dedup.csv`).

| Provincias (top-10) | N distritos | Distrito líder (% de la producción provincial) |
|---|---|---|
| PIURA (#1) | 9 | TAMBO GRANDE 85.4% |
| SULLANA (#2) | 6 | SULLANA 99.3% |
| LAMBAYEQUE (#3) | 6 | OLMOS 52.9% |
| ZARUMILLA (#4) | 4 | MATAPALO 59.4% |
| MORROPON (#5) | 7 | CHULUCANAS 66.7% |
| TUMBES (#6) | 6 | PAMPAS DE HOSPITAL 66.3% |
| CORONEL PORTILLO (#7) | 7 | CAMPOVERDE 27.7% |
| MAYNAS (#8) | 11 | BELEN 36.9% |
| UTCUBAMBA (#9) | 6 | CUMBA 41.2% |
| PADRE ABAD (#10) | 5 | CURIMANA 25.2% |

La producción por provincia está concentrada: 7 de 10 provincias tienen un
distrito líder con ≥53% de su producción; solo CORONEL PORTILLO y PADRE ABAD
muestran reparto relativamente uniforme (líder ≤28%, ≤7 distritos).

**Costo estimado de la opción distrital (Paso 3):**
- Distritos productores únicos (PRODUCCION>0): Sutil 481 + Dulce 72 =
  494 únicos (59 compartidos entre ambos cultivos).
- Llamadas NASA POWER: 494 vs 109 actuales → **385 adicionales**.
- Tiempo estimado (0.7 s delay + ~1.3 s latencia, sin retries): **~16.5 min**
  vs ~3.6 min actuales.
- Limitación del grid: NASA POWER es ~0.5°×0.5° (~55×55 km). Entre las 109
  coordenadas provinciales actuales hay solo 88 celdas distintas (21
  provincias comparten celda); a nivel distrito la cota real de información
  serían las celdas únicas tocadas (no 494).

**Decisión:** mantener la granularidad provincial
(`data/interim/coordenadas_provincias_completo.csv`) como fuente de clima
para el reentrenamiento v2. No se ejecuta descarga distrital.

**Estado de aprobación:** esta decisión fue presentada al usuario con la
evidencia completa de los 3 pasos y fue **APROBADA explícitamente el
30-08-2026** tras su revisión (no fue tomada unilateralmente).

## 2026-XX-XX — Split train/val/test — Limón Sutil y Limón Dulce

**Decisión:** Split cronológico train=2016-2023 (96 meses), val=2024 (12 meses), test=2025 (12 meses). 2026 excluido por cobertura geográfica incompleta (año en curso).

**Verificación de representación de shocks (previa a la decisión):**
- Train: 28% meses de shock (Sutil) / 25% (Dulce)
- Val: 0% (Sutil) ⚠️ / 25% (Dulce)
- Test: 25% (Sutil) / 25% (Dulce)

**Justificación estadística:** Test de chi2 train vs. val+test compatible (Sutil p=0.11, Dulce p=1.00) — no hay sesgo estructural entre particiones.

**Justificación de dominio:** Los shocks detectados (umbral P75 de variación mensual) correlacionan con eventos reales documentados: El Niño 2015-16, El Niño Costero 2017, cuarentena COVID-19 2020, Ciclón Yaku 2023, crisis de precios del limón 2023-24 — no son ruido estadístico.

**Limitación declarada:** VAL-Sutil no contiene meses de shock (2024 fue año de producción récord con baja volatilidad relativa). La validación de hiperparámetros para Sutil mide capacidad de extrapolación a nivel superior, no robustez ante shocks. Las métricas de resiliencia (Δs) se reportan exclusivamente sobre el conjunto de test (2025), que sí tiene representación adecuada (25%).

**Alternativa descartada:** Split train=2016-2022/val=2023/test=2025 con 2024 como buffer no scoreado — descartada por "quemar" un año completo de datos sin ganancia proporcional, dado que el riesgo central (train sin shocks) no se confirmó.

### Actualización — ajuste de train por dropna de lags (features temporales)

Tras generar lags t-6, los primeros 6 meses (2016-01 a 2016-06) se
eliminan por no tener historial completo dentro del dataset (dropna,
mismo criterio que v1). Train pasa de 96 a 90 meses (2016-07 a
2023-12).

**Verificación de shocks re-confirmada:** train mantiene 27.8% Sutil /
24.4% Dulce (vs. 28%/25% original) — cambio marginal, split sigue
sano (chi² train vs val+test: p=0.20).

**Meses eliminados con shock:** 2016-02 (Sutil +26.5%, Dulce +38.3%)
y 2016-06 (Sutil +25.6%) — ambos apenas superan el umbral P75, no
corresponden a eventos climáticos mayores documentados (año de cierre
de El Niño 2015-16, distinto de El Niño Costero 2017). Pérdida de
1-2 shocks marginales de 90 meses, sin comprometer robustez del split.

## 2026-XX-XX — Deduplicación Ayacucho (Limón Sutil)

**Hallazgo:** 1,061 claves duplicadas (AÑO+MES+COD_UBIGEO+COD_PRODUCTO),
exclusivamente en Ayacucho (0 en otros 23 departamentos, 0 en Limón Dulce).
Hasta 7 filas por clave.

**Causa identificada:** 55% son copias casi exactas (mismo PRODUCCION y
PRECIO_CHACRA, solo varía VERDE_ACTUAL) — consistente con
re-declaraciones/capturas repetidas del mismo registro mensual.

**Decisión:** Agregación por MEDIA (no suma, no máximo) para claves
duplicadas de Ayacucho antes de la agregación distrito→provincia.

**Justificación:** Suma infla Ayacucho ~1.75x (artefacto de captura,
no producción real). Media es neutral ante el ruido.

**Impacto en serie nacional:** Ayacucho = 0.11-0.40% de producción
nacional anual → impacto <0.2% en el total agregado, independiente
de la regla elegida. No afecta el modelo principal (serie nacional).

## 2026-XX-XX — Agregación distrito→provincia: VERDE_ACTUAL y PRECIO_CHACRA

**VERDE_ACTUAL — decisión: incluir como feature de contexto (Canal A)**

Justificación: aunque SIEMBRA y COSECHA son siempre 0 (cultivo permanente,
no aplican mes a mes), VERDE_ACTUAL sí varía y mide superficie en
producción activa (hectáreas). Aporta señal no redundante con PRODUCCION:
permite separar "cuánta tierra está en producción" de "cuánto rinde esa
tierra dado el clima" — exactamente la distinción que el modelo necesita
aprender. Una caída abrupta de VERDE_ACTUAL en una provincia (ej. por
desastre que destruye plantaciones) puede funcionar como señal
anticipatoria de caída de producción futura, similar en espíritu a las
variables climáticas exógenas. Se decide incluirla en el dataset de
features y dejar que el análisis SHAP determine empíricamente su aporte
real al modelo, en lugar de descartarla sin haberla probado.

Agregación distrito→provincia: SUMA (es una medida de superficie,
se agrega igual que PRODUCCION).

**PRECIO_CHACRA — decisión: media ponderada por producción**

Opciones evaluadas:
(A) Media simple de los precios de todos los distritos de la provincia.
(B) Media ponderada por producción de cada distrito. [ELEGIDA]

Justificación: la media ponderada es económicamente más representativa
del precio real que enfrenta la cadena de valor de la provincia. Un
distrito que produce 5,000 t no debe pesar lo mismo en el precio
provincial que uno que produce 1 t — la media simple distorsionaría el
precio hacia distritos marginales con baja producción, mientras que la
ponderada refleja el precio efectivo asociado al grueso de la producción
real de la provincia (relevante para simulaciones de impacto económico
tipo PYME, donde el precio que importa es el de donde se concentra el
volumen).

Fórmula: precio_provincia = Σ(precio_distrito × producción_distrito) / Σ(producción_distrito)

Aplica igual para Limón Sutil y Limón Dulce.

## Observación — Lambayeque-Chiclayo (Limón Sutil)

Chiclayo registra VERDE_ACTUAL>0 en los 120 meses (2016-2025) pero
produccion_t=0 en todos ellos. No distorsiona agregados (peso 0 en
media ponderada), pero se documenta como observación de calidad de
datos MIDAGRI para referencia futura — no requiere acción correctiva.

## Coordenadas climáticas — fallback departamental

**Hallazgo:** 8/107 provincias de Sutil y 3/28 de Dulce carecen de
coordenadas exactas en la fuente base (clima_dataset_final.csv, v1).

**Verificación de impacto:** suma de producción de las provincias
faltantes = 0.252% del total nacional (Sutil), 2.819% (Dulce).
4 de las 8 provincias faltantes de Sutil (Chiclayo, Cañete, Lima,
Talara) tienen producción = 0 en todo el periodo 2016-2025.

**Decisión:** usar centroide del departamento como fallback de
coordenadas para estas provincias marginales, en lugar de descartarlas
o buscar una fuente exacta adicional (no se justifica el esfuerzo
dado el impacto <3% en el peor caso).

**Trazabilidad:** columna `fuente` en
`data/interim/coordenadas_provincias_completo.csv` distingue
`provincia_exacta` (99 filas) vs `departamento_fallback` (10 filas).

## Fuente INDECI — reemplazo por dataset consolidado oficial

**Hallazgo:** las URLs originales de Emergencias2016/2017/2018.xls
(datosabiertos.gob.pe) están rotas desde su publicación (confirmado
vía Wayback Machine). La plataforma rediseñada publica un dataset
consolidado `BD_2003-2025_EMERGENCIAS.csv` (142,139 filas, 49 columnas,
actualizado 2026-08-26).

**Decisión:** usar este único archivo como fuente completa de INDECI
2016-2025, reemplazando los .dbf de 2019-2020, el CSV procesado
2021-2023, y descartando el plan de scraping SINPAD para 2024-2025.

**Verificación de cobertura:** 107/107 provincias productoras de
limón tienen registros de emergencia en el rango completo 2016-2025.
Conteos por año dentro de provincias limoneras: 2016=2,862 · 2017=3,764 ·
2018=2,996 · 2019=2,396 · 2020=2,769 · 2021=4,678 · 2022=4,226 ·
2023=7,487 · 2024=8,260 · 2025=9,319 (tendencia creciente, consistente
con mejor reporte reciente, no necesariamente más emergencias reales).

**Adaptaciones aplicadas al integrar:**
1. Mapeo de nombres de columna (esquema .dbf truncado → esquema CSV moderno)
2. Vocabulario de peligros: filtro final de 10 categorías climáticas
   (LLUVIA INTENSA, VIENTOS FUERTES, BAJAS TEMPERATURAS, INUNDACION,
   DESLIZAMIENTO, HUAYCO, SEQUIA, EROSION, TORMENTA ELECTRICA, MAREJADA).
   Las 12 restantes de las 22 únicas se excluyen por no ser climáticas
   (incendios urbanos/forestales, sismos, derrumbes, actividad
   volcánica, biológicas: plagas/epidemias, y humano-inducidas:
   derrames, contaminación, explosiones). Nota: la columna usa "LLUVIA INTENSA"
   (singular), no "LLUVIAS INTENSAS" como el scraper original.
3. Normalización de LIMA: LIMA METROPOLITANA + LIMA PROVINCIAS → LIMA
4. Parseo de doble formato de fecha

## Scraping de noticias — Agraria.pe (categoría agronegocios, 2016-2025)

**Diagnóstico previo:** scraper v1 obsoleto (slugs cambiados, selectores
de listado rotos, fecha ya no en listado). Corregido en
`src/scraping/agraria_scraper_v2.py`.

**Bug crítico descubierto y corregido:** artículos de 2016 usan
`<div style="text-align:justify">` en vez de `<p>` para el cuerpo —
el extractor original devolvía vacío para todo el año 2016 sin dar
error. Se agregó fallback dual `<p>` + `<div>`, verificado sin cambios
en 3 puntos del rango histórico (2016, ~2020, ~2024) — solo existen
esos 2 formatos, no hay un tercero.

**Filtro de relevancia — 2 niveles:**
- Nivel 1 (match directo): limón, limon, piura, sullana, tumbes, niño,
  senamhi, midagri, lluvias, sequía, helada
- Nivel 2 (contexto agrario + mención de fruta/cítrico/región):
  agroexportación, cosecha, campaña agrícola, producción agrícola,
  sector agrario, clima, cambio climático, irrigación, inundación

**Justificación del filtro de 2 niveles:** el filtro original (solo
keywords directas de limón/clima) producía 0 resultados en páginas
históricas de la categoría `agronegocios` porque esas noticias son
mayoritariamente de comercio/industria general. Se amplió para
capturar también el contexto agrario amplio, que puede correlacionar
con sentimiento de mercado sin mencionar limón explícitamente.

**Robustez del scraper:** se agregó backoff exponencial (5 reintentos,
2s→32s) tras 3 crashes por error DNS transitorio durante la corrida
completa. El checkpoint garantizó cero pérdida de datos en cada
interrupción (manual o por crash).

**Resultado final — categoría `agronegocios`, 1900 páginas, 2016-2025:**
- 2,883 artículos guardados, 0 duplicados reales
- Cobertura: 118/120 meses (98.3%) — huecos únicamente en 2016-01 y
  2016-02
- Composición: 12.7% mención directa de limón (Nivel 1), 87.3% contexto
  agrario amplio (Nivel 2)
- Longitud media del cuerpo: 2,872 caracteres
- Distribución por año: 2016=219, 2017=404, 2018=274, 2019=223,
  2020=161, 2021=210, 2022=228, 2023=338, 2024=416, 2025=410

**Decisión:** el corpus de `agronegocios` se considera SUFICIENTE para
la señal NLP mensual. No se agregan categorías o fuentes adicionales
por ahora (decisión pendiente de confirmación explícita del usuario;
ver nota abajo).

**Limitación declarada:** meses 2016-01 y 2016-02 sin cobertura de
noticias — tendrán `avg_sentiment=0`/`n_noticias=0` en el dataset
maestro, igual que se documentó para el corpus v1.

**Archivo final:** `v2_reentrenamiento/data/interim/noticias/corpus_agronegocios_2016_2025.csv`
(2,883 filas)

## Validación cruzada del índice NLP — correlación con eventos reales

**Resultado del análisis de sentimiento (2,883 noticias, RoBERTuito
zero-shot):**
- Distribución: NEU 67.1%, POS 19.7%, NEG 13.2%
- Media global 2016-2025: +0.0923

**Validación cruzada con eventos climáticos documentados:**
- El Niño Costero 2017 (ene-mar 2017): media trimestral -0.1452,
  con enero 2017 como MÍNIMO ABSOLUTO de toda la serie 2016-2025
  (-0.2464) — detectado sin haber diseñado el índice para ese fin.
- Crisis de precios del limón 2023-24 (jul2023-jun2024): enero 2024
  registra el SEGUNDO mínimo global (-0.2176), coincidiendo
  exactamente con el shock ya identificado independientemente en el
  análisis de producción MIDAGRI (percentil 75 de variación mensual).
- COVID-19 (mar-jun 2020): caída atenuada (mínimo -0.0336 en abril
  2020), notablemente menor que los dos eventos climáticos. Se
  atribuye a que el corpus (categoría agronegocios) contiene
  principalmente noticias factuales/logísticas sobre COVID sin
  carga emocional marcada, a diferencia de la cobertura de desastres
  climáticos que sí genera lenguaje evaluativo negativo.

**Significancia:** esta doble confirmación cruzada (señal NLP
independiente vs. shocks detectados en datos de producción) respalda
la validez del índice de sentimiento como proxy de shocks del sector,
más allá de la simple correlación esperada por diseño.


## 2026-09-03 — Bug de horizonte en GC1 (SARIMA): test replicaba val

**Hallazgo:** en `02_gc1_sarima_prophet.ipynb`, el ajuste final de SARIMA
llamaba dos veces a `forecast(steps=12)` sobre el mismo objeto ajustado.
Como `forecast()` de statsmodels siempre proyecta desde el final de la
serie de entrenamiento y no avanza estado entre llamadas, ambas llamadas
devolvían **el mismo vector**. Las "predicciones de test" (2025) eran en
realidad las de val (2024) duplicadas, evaluadas contra los valores reales
de 2025.

**Corrección:** un único forecast de 24 pasos desde el fin de train; val
toma los pasos 1..12 y test los pasos 13..24.

    f_val_test = np.asarray(fit.forecast(steps=N_VAL + N_TEST))
    pred_val   = f_val_test[:N_VAL]
    pred_test  = f_val_test[N_VAL:]

Esto implica que **test se evalúa a horizonte 13-24 meses**, no 1-12. Es
la lectura honesta del diseño: el modelo no ve 2024 en ningún momento, así
que 2025 está a 13-24 pasos del último dato observado. No se re-ajusta con
val (eso sería fuga de información).

**Alcance del bug — confirmación cruzada:** el grid de selección de
candidatos (576 combinaciones) usa `forecast(steps=N_VAL)` y **no estaba
afectado**: solo evalúa val, que es el paso 1..12 y siempre fue correcto.
La selección de hiperparámetros no se contaminó.

**Confirmación cruzada de que el bug era exclusivo de SARIMA:** tras
re-ejecutar el pipeline completo, `exp_003_prophet_sutil` y
`exp_003_prophet_dulce` regeneraron `metricas.json` y `predicciones.csv`
**byte a byte idénticos** a los previos (solo cambió `fecha_ejecucion` en
`config.yaml`). Prophet nunca tuvo el bug porque predice con
`.predict(future_df)` sobre un DataFrame de fechas explícito, no con un
contador de pasos con estado. Los dos experimentos SARIMA sí cambiaron
predicciones y métricas. Esta asimetría es la verificación de que el
diagnóstico fue correcto y la corrección quedó acotada.

**Impacto:** las métricas de test de SARIMA anteriores al 2026-09-03 son
inválidas y fueron reemplazadas. Naive (exp_001) no usa `forecast()` y no
requirió re-ejecución.

## 2026-09-03 — Sobreajuste por AIC en SARIMA-Sutil — exp_002b como representativo

**Hallazgo:** con el horizonte ya corregido, el SARIMA-Sutil seleccionado
por el procedimiento automático (top-5 por AIC en train, desempate por MAE
en val) generaliza mal:

| | orden | AIC train | MAE val | MAE test | R² test |
|---|---|---:|---:|---:|---:|
| exp_002 (AIC) | (1,1,3)(2,1,0,12) | **1874.93** | 8439.79 | 12031.69 | **−1.11** |
| exp_002b (parsimonia) | (1,1,1)(1,1,0,12) | 1886.52 | 4824.13 | **3638.14** | **+0.755** |

El orden con **mejor** AIC produce el **peor** test: R² negativo significa
que predecir la media de test habría sido mejor, y su MAE_test (12031.69)
es 2.6× el del Naive (4704.29). El AIC premia ajuste en muestra; con 90
observaciones y 6 parámetros estacionales el criterio no penaliza lo
suficiente para una serie con cambio de nivel (ver sección siguiente).

**Decisión:** la comparativa oficial GC1 representa SARIMA-Sutil con
**`exp_002b_sarima_sutil_simple`**. `exp_002_sarima_sutil` se conserva en
`experimentos/` como registro auditable del hallazgo, y NO se borra. El
mapeo está explícito en el dict `EXP_COMPARATIVA` del notebook, de modo que
una re-ejecución completa reproduce la comparativa correcta.

**Cómo se eligió exp_002b (importante para la validez del test):** el orden
(1,1,1)(1,1,0,12) se fijó **a priori** como el SARIMA estacional mínimo
razonable — un término AR, una diferencia, un MA, más su contraparte
estacional. NO hubo búsqueda sobre val ni sobre test, ni se probaron
variantes eligiendo la de mejor MAE_test. Es selección por parsimonia, un
criterio declarado antes de mirar el resultado. De haber barrido órdenes
optimizando test, el MAE_test reportado no sería un estimador honesto.

**Diagnóstico de residuos (exp_002b):** Ljung-Box sin autocorrelación
residual (p>0.05 en lags 6, 12, 18, 24); todas las raíces AR y MA fuera del
círculo unitario (AR mín |r|=1.0643, MA |r|=4.4561); sesgo del MSE en test
1.5%. El modelo parsimonioso es estadísticamente adecuado, no solo más
afortunado.

**SARIMA-Dulce no se modifica:** `exp_002_sarima_dulce` (selección por AIC)
da R²_test = 0.885 y MAE_test = 53.40 (−36.9% vs Naive). El problema no
apareció en esa serie, cuya tendencia es mucho más suave (+2.5%/año vs
+4.0%/año, y salto train→test de +18.2% vs +35.5%).

**Efecto sobre la conclusión de GC1:** con exp_002b, los cuatro pares
cultivo×modelo de GC1 superan al Naive en MAE_test (Sutil: SARIMA −22.7%,
Prophet −21.7%; Dulce: SARIMA −36.9%, Prophet −23.8%). Con exp_002, la
celda SARIMA-Sutil era +155.8% PEOR que el Naive. El cambio de experimento
representativo altera la conclusión sustantiva, no solo una cifra.

**Limitación declarada:** el procedimiento de selección automática por AIC
queda invalidado como criterio único para GC1-Sutil. En modelos posteriores
(GC2, GE, GM) la selección debe evaluarse en val con métrica de error, no
por criterios de información en train.

## 2026-09-03 — Salto de nivel: tendencia secular y sus dos consecuencias

**Hallazgo:** la producción de Limón Sutil tiene una tendencia creciente
sostenida que el split cronológico convierte en un problema de
extrapolación, no de interpolación.

Media anual de `produccion_t_sutil` (t/mes):

    2016  22393    2021  27981
    2017  13584    2022  30558
    2018  21509    2023  23067
    2019  24205    2024  32209
    2020  25132    2025  31908

- CAGR 2016→2025: **+4.01%/año** (Dulce: +2.50%/año)
- Pendiente OLS: +1327 t/año ≈ +5.25%/año sobre la media
- Media train (≤2023) = 23554 · val (2024) = 32209 · test (2025) = 31908
- **Salto de nivel train→test: +35.5%** (Dulce: +18.2%)

Todo modelo entrenado hasta 2023-12 debe proyectar a un régimen ~35% más
alto que el que observó. Esto explica el R² negativo de exp_002: sus
predicciones quedan sistemáticamente por debajo del nivel real de 2025
(errores de ~12000 t con signo consistente), y no es un fallo de
implementación sino la respuesta esperada de un modelo sin término de
tendencia adecuado ante un cambio de nivel de esa magnitud.

**Descomposición de la tendencia (área vs. rendimiento):** usando
`verde_actual_ha` de `data/interim/limon_sutil_provincia.csv`, el
crecimiento de producción se descompone en dos factores independientes:

| | 2016 | 2025 | CAGR |
|---|---:|---:|---:|
| Área en producción (ha) | 260.6 | 317.5 | **+2.22%/año** |
| Rendimiento (t/ha) | 1031.3 | 1205.9 | **+1.75%/año** |
| Producción (t) | 268 722 | 382 899 | **+4.01%/año** |

La descomposición cierra: 1.0222 × 1.0175 = 1.0401. La tendencia secular
NO es un artefacto estadístico ni un efecto de precios — responde a
expansión física de superficie más mejora de productividad, ambas
sostenidas. Esto respalda modelar la tendencia explícitamente (t_index)
en vez de tratarla como deriva a diferenciar.

**Nota de interpretación:** parte del salto 2023→2024 es recuperación tras
la caída de 2023 (crisis de precios del limón y Ciclón Yaku, ya
documentados en la sección de shocks), no solo crecimiento estructural. La
tendencia de +4%/año es real pero está superpuesta a volatilidad de nivel
importante; no debe presentarse como crecimiento suave.

### Decisión (a) — feature `t_index`

Se añade `t_index`, índice temporal lineal 0..113, a
`master_dataset_{sutil,dulce}_v2_features.csv` y su versión escalada
(32 → 33 columnas), para dar a los modelos multivariados un canal explícito
de tendencia secular en vez de esperar que la infieran de los lags.

**Escalado sin fuga:** el StandardScaler se ajusta **solo sobre train**
(90 filas, t_index 0..89) y se aplica a las 114. Verificable en el dataset
escalado: `t_index` va de −1.7129 a +2.6367 con media 0.4619 — la media no
es 0 y el máximo excede +2σ precisamente porque val y test caen fuera del
rango de ajuste. Si el scaler se hubiera ajustado sobre las 114 filas, la
media sería 0. Scalers en
`resultados_v2_final/scalers/scaler_{sutil,dulce}_v2b_tindex.joblib`.

**Limitación declarada:** `t_index` extrapola linealmente fuera de train.
Es una ayuda para captar la deriva de nivel, no un modelo de la tendencia;
a horizontes largos su aporte se degrada y puede sobre-proyectar si el
crecimiento se aplana.

**Alcance actual:** GC1 (SARIMA, Prophet) es **univariado** y corre sobre
`master_dataset_*_v2.csv` crudo — NO consume `t_index`. La feature queda
disponible para GC2/GE/GM. Prophet, por su parte, ya modela tendencia
internamente con su término de trend, lo que explica que sea el único
modelo GC1 que sobrevive bien al salto de nivel en Sutil sin ajuste manual
(R²_test 0.792).

### Decisión (b) — lags one-step-ahead

Se evalúan los modelos LSTM (GE, GM) alimentando los lags con valores
REALES del mes anterior (no con la propia predicción recursiva del
modelo), como decisión de diseño experimental para aislar la capacidad
predictiva del modelo del efecto de acumulación de error recursivo.

**Consecuencia:** esto hace que los modelos multivariados (GC2, GE, GM) NO
sean directamente comparables con GC1 (SARIMA, Prophet) en la tabla de
MAE_test, ya que GC1 se evalúa en modo puramente recursivo (forecast puro
sin retroalimentación de datos reales) mientras que los modelos LSTM
reciben información real del mes previo en cada paso.

**Limitación declarada:** no se ha verificado el rezago real de
publicación de datos de MIDAGRI en un escenario operativo — esta decisión
prioriza la evaluación aislada de capacidad predictiva sobre el realismo
del escenario de despliegue. Validar el rezago real de publicación queda
como trabajo futuro antes de cualquier despliegue productivo del modelo.

## 2026-09-03 — Δs con n_shock=3: la cifra es real, la inferencia no

**Contexto:** Δs = (MAE_shock − MAE_global)/MAE_global × 100 se calcula
sobre los meses de test que superan el umbral P75 de variación mensual
(23.2% para Sutil). En test 2025 eso son **3 meses: 2025-01, 2025-07 y
2025-11**.

**Hallazgo:** SARIMA-Sutil (exp_002b) reporta Δs = −79.25%, es decir MAE en
meses de shock (755.01) muy inferior al MAE global (3638.14). La cifra es
aritméticamente correcta pero **no es evidencia de resiliencia ante
shocks**. Desglose de los errores absolutos en test:

- Meses de shock:   2025-01 → 14.1 · 2025-07 → 822.6 · 2025-11 → 1428.3
- Mayores errores:  2025-04 → 7170.8 · 2025-06 → 7094.3 · 2025-05 → 6153.3
  (los tres **fuera** de meses de shock)

El promedio de shock lo domina 2025-01, con un error de 14.1 t sobre 38325 t
reales (**0.04%**) — una coincidencia numérica, no capacidad predictiva.
Excluyendo ese único mes, MAE_shock sube a 1125 y Δs pasa a ≈ −69%: un
tercio del efecto depende de una sola observación.

**Contraste que lo confirma:** exp_002 (el modelo descartado) reporta
MAE_shock = 12066.7 vs MAE_no-shock = 12020.0, Δs = +0.29%. Un modelo
uniformemente malo produce Δs ≈ 0. Es decir, Δs con n=3 refleja
principalmente **sobre qué tres meses cayó el azar**, no la robustez del
modelo ante perturbaciones.

**Decisión:** Δs se sigue reportando (es la métrica de resiliencia definida
para el proyecto) pero **siempre acompañado de n_shock**, y nunca se
presenta un Δs de Sutil como resultado principal ni como mejora
cuantificada. Redacción prohibida: "el modelo mejora 79% ante shocks".
Redacción aceptable: "Δs = −79.25% sobre n_shock = 3, dominado por un único
mes; no interpretable como resiliencia".

**Limitación declarada:** con 12 meses de test y umbral P75, n_shock ≈ 3 es
el máximo estructuralmente posible en este diseño. Δs no admite contraste
de hipótesis con esta muestra. Una evaluación robusta de resiliencia
requeriría walk-forward validation sobre múltiples ventanas de test, lo que
extendería el número de meses de shock evaluados sin alterar el split
declarado. Queda registrado como trabajo pendiente, no ejecutado.
