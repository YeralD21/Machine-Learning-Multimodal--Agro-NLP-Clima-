# CONTEXTO DE SESIÓN — 2026-09-07

**Documento de continuidad autocontenido.** Escrito al cierre de la sesión de
investigación del 2026-09-07 (21:17 hora local). Está pensado para que una
sesión futura de Claude Code pueda retomar el trabajo **sin acceso al historial
de la conversación que lo generó**.

- **Repositorio:** `C:\Machine-learming\Machine-Learning-Multimodal--Agro-NLP-Clima-`
- **Branch:** `v2-reentrenamiento` (branch principal del repo: `main`)
- **HEAD al cierre:** `bfd19a7c9de7fd5f1acbc8144429169dc3c4000e` (`bfd19a7`, 2026-09-04 08:17:01 -0500)
- **¿Se hizo commit en esta sesión?** **NO.** Hay cambios sin commit (§3).
- **¿Se entrenó algún modelo en esta sesión?** **NO.** Ningún `.fit()` de GC2, GE,
  GM, XGBoost ni TCN fue ejecutado. Lo único ejecutado fue el pipeline de Fase 2
  (agregación, features, escalado), que no entrena modelos predictivos.

---

## 1. IDENTIFICACIÓN DE LA INVESTIGACIÓN

### Título de la tesis

> **«Resiliencia ante Shocks Climáticos de un Modelo Híbrido DualLSTM-Attention
> Multimodal y Explicabilidad SHAP para el Pronóstico de Demanda Agroindustrial
> Peruana en Entornos Volátiles y Small Data»**

Investigador: Fabrizio Sánchez Saravia — UPeU (Universidad Peruana Unión), Juliaca.

### Objetivo general vigente

Evaluar si un modelo híbrido **DualLSTM-Attention multimodal** —que integra
producción agrícola, clima, desastres y señal de noticias (NLP)— pronostica la
**demanda/producción agroindustrial mensual** de limón peruano con mayor
**resiliencia ante shocks climáticos** que los baselines clásicos y que un modelo
estructural equivalente sin NLP, en un régimen de **small data** y alta
volatilidad; y explicar sus predicciones mediante **SHAP**.

> ⚠️ **Nota de trazabilidad:** el texto formal del PPI **no está en el
> repositorio**. Este objetivo general está reconstruido a partir del título, del
> `README.md`, de `CLAUDE.md` y de `docs/pipeline_fases_234.md`. Si el PPI oficial
> difiere, **prevalece el PPI**, y este párrafo debe corregirse.

### Cultivares estudiados

| Cultivar | Serie objetivo | Provincias con peso (v2) |
|---|---|---|
| **Limón Sutil** | `produccion_t_sutil` (t/mes, nacional) | 106 |
| **Limón Dulce** | `produccion_t_dulce` (t/mes, nacional) | 28 |

Ambos se modelan **por separado**, con datasets, pesos espaciales y scalers
independientes.

### Periodo de datos

- **Serie cruda:** 2016-01 … 2025-12 (120 meses).
- **Serie utilizable tras el `dropna` de lag6:** 2016-07 … 2025-12 (114 meses).
- **2026 excluido** de la evaluación histórica (año en curso, cobertura
  geográfica incompleta).

### Fuentes de datos

| Fuente | Contenido | Nivel original | Ruta principal |
|---|---|---|---|
| **MIDAGRI** (Sisagri) | producción, superficie (`VERDE_ACTUAL`), precio en chacra | distrito-mes | `v2_reentrenamiento/data/raw/midagri/sisagri_2015_2026.xlsx` (229 MB) |
| **NASA POWER** | `T2M`, `T2M_MAX`, `WS2M`, `PRECTOTCORR`, `RH2M` | provincia-mes (109 prov.) | `data/raw/nasa_power/por_provincia/clima_nasa_power_2016_2025.csv` |
| **INDECI** (SINPAD) | 6 variables de emergencia climática | provincia-mes (107 prov.) | `data/interim/indeci_limpio_2016_2025.csv` |
| **Agraria.pe → NLP** | `avg_sentiment`, `n_noticias` (RoBERTuito) | nacional-mes | `data/interim/noticias/sentimiento_mensual_2016_2025.csv` |

### Modelos / grupos experimentales

| Grupo | Descripción | Estado en v2 |
|---|---|---|
| **Naive** | Baseline de persistencia | ✅ cerrado (`exp_001_naive_{sutil,dulce}`) |
| **GC1** | SARIMA + Prophet (univariados) | ✅ cerrado y commiteado (`exp_002*`, `exp_002b`, `exp_003*`) |
| **GC2** | SARIMAX + LSTM residual (híbrido estadístico-neuronal con exógenas) | ⬜ **NO INICIADO** — siguiente modelo |
| **XGBoost** | Competidor (gradient boosting con lags y rolling stats) | ⬜ no iniciado en v2 |
| **GE** | Grupo Estructural: Dual-LSTM + Bahdanau Attention, **sin NLP** | ⬜ no iniciado en v2 |
| **GM** | Grupo Multimodal: arquitectura de GE **+ NLP** | ⬜ no iniciado en v2 |

### Rol de SHAP

Explicabilidad post-hoc del modelo recomendado: ranking global de importancia de
features y análisis del comportamiento en meses de shock. En v1 se aplicó con
`KernelExplainer` sobre GE (`notebooks/fase4/actividad_16_shap.ipynb`). **En v2
todavía no se ha ejecutado**; corresponde a una fase posterior a GC2/GE/GM.

### Métrica de shocks Δs

```
Δs = (MAE_shock − MAE_global) / MAE_global × 100
```

- Meses de shock = aquellos cuya `|100·(y_t − y_{t−1})/y_{t−1}|` supera el
  **percentil 75** de la variación mensual, calculado **siempre sobre la serie
  real en toneladas, nunca en z-score**.
- Umbrales P75 ya validados: **Sutil 23.2 %**, **Dulce 33.9 %**.
- Se evalúa **solo sobre el conjunto de test**.
- **Limitación declarada y vigente:** en el test de 12 meses, `n_shock ≈ 3`, que
  es el máximo estructuralmente posible. Δs **debe reportarse siempre acompañado
  de `n_shock`** y **nunca presentarse como resultado principal ni como mejora
  cuantificada**. Redacción prohibida: *«el modelo mejora 79 % ante shocks»*.
  Redacción aceptable: *«Δs = −79.25 % sobre n_shock = 3, dominado por un único
  mes; no interpretable como resiliencia»*.

### Finalidad de la simulación económica

Traducir el error de pronóstico y los escenarios de shock (p. ej. El Niño) a
impacto económico para una PYME agroexportadora, de modo que el aporte del modelo
se exprese en unidades de decisión y no solo en MAE. En v1 existe como
`notebooks/fase4/impacto_economico_nino_2026.ipynb` y `simulacion_impacto_economico.ipynb`.
**En v2 aún no se ha abordado.**

---

## 2. ROLES DE TRABAJO

| Rol | Responsabilidades |
|---|---|
| **ChatGPT** | Supervisor metodológico. Revisión científica y bibliográfica. Toma de decisiones metodológicas junto con el investigador. Genera las instrucciones/prompts que se pasan a Claude Code. |
| **Claude Code** | Auditor técnico del repositorio. Implementador. Ejecuta código **únicamente después de aprobación explícita**. **No debe tomar unilateralmente decisiones metodológicas abiertas.** |
| **Investigador** (Fabrizio Sánchez S.) | Aprobación final de todos los cambios y decisiones. |

### Regla permanente

> **Ante cualquier ambigüedad metodológica, Claude Code debe DETENERSE, reportar
> el problema y esperar decisión.** No debe resolverla por su cuenta, aunque exista
> una opción que parezca razonable.

Precedente de esta sesión: al regenerar Fase 2, las instrucciones fijaban la regla
categórica *«ninguna variable exógena del mes objetivo puede entrar»* pero al
detallar solo nombraban clima e INDECI, sin mencionar el NLP. Claude aplicó la
regla de forma uniforme (rezagó también el NLP) **y lo reportó explícitamente
como interpretación pendiente de confirmación**. El investigador la confirmó
después. Ese es el patrón esperado.

---

## 3. ESTADO DEL REPOSITORIO AL CIERRE

- **Branch:** `v2-reentrenamiento`
- **HEAD:** `bfd19a7c9de7fd5f1acbc8144429169dc3c4000e`
- **Último commit:** `bfd19a7` — *"fix(gc1): Corregir bug de horizonte SARIMA y
  establecer SARIMA-Sutil representativo…"* (2026-09-04 08:17:01 -0500)
- **¿Hay cambios sin commit?** **SÍ.** Ninguno fue commiteado en esta sesión.

### 3.1 · Modificados (` M`) — 13 archivos, todos de v2

```
v2_reentrenamiento/DECISIONES_METODOLOGICAS.md                       (524 -> 625 líneas)
v2_reentrenamiento/data/processed/master_dataset_sutil_v2.csv          (120,18) -> (120,16)
v2_reentrenamiento/data/processed/master_dataset_sutil_v2_features.csv (114,33) -> (114,48)
v2_reentrenamiento/data/processed/master_dataset_sutil_v2_escalado.csv (114,33) -> (114,48)
v2_reentrenamiento/data/processed/master_dataset_dulce_v2.csv          (120,18) -> (120,16)
v2_reentrenamiento/data/processed/master_dataset_dulce_v2_features.csv (114,33) -> (114,48)
v2_reentrenamiento/data/processed/master_dataset_dulce_v2_escalado.csv (114,33) -> (114,48)
v2_reentrenamiento/notebooks/fase2_features/02_fusion_dataset_maestro.ipynb
v2_reentrenamiento/notebooks/fase2_features/02_fusion_dataset_maestro_ejecutado.ipynb
v2_reentrenamiento/notebooks/fase2_features/03_features_temporales.ipynb
v2_reentrenamiento/notebooks/fase2_features/03_features_temporales_ejecutado.ipynb
v2_reentrenamiento/notebooks/fase2_features/04_escalado.ipynb
v2_reentrenamiento/notebooks/fase2_features/04_escalado_ejecutado.ipynb
```

### 3.2 · Nuevos sin trackear (`??`) — creados en esta sesión

```
v2_reentrenamiento/src/features/__init__.py
v2_reentrenamiento/src/features/hashing.py
v2_reentrenamiento/src/features/pesos_geograficos.py
v2_reentrenamiento/src/features/agregacion_espacial.py
v2_reentrenamiento/src/features/features_temporales.py
v2_reentrenamiento/src/features/escalado.py
v2_reentrenamiento/notebooks/fase2_features/00_pesos_geograficos.ipynb
v2_reentrenamiento/notebooks/fase2_features/00_pesos_geograficos_ejecutado.ipynb
v2_reentrenamiento/resultados_v2_final/pesos/            (directorio completo)
  ├─ pesos_geograficos_train.csv
  ├─ pesos_geograficos_train_meta.json
  ├─ fase2_v2_regenerada_meta.json
  └─ REGISTRO_PRE_REGENERACION.json
v2_reentrenamiento/resultados_v2_final/scalers/scaler_sutil_v2c.joblib
v2_reentrenamiento/resultados_v2_final/scalers/scaler_sutil_v2c_parametros.csv
v2_reentrenamiento/resultados_v2_final/scalers/scaler_dulce_v2c.joblib
v2_reentrenamiento/resultados_v2_final/scalers/scaler_dulce_v2c_parametros.csv
v2_reentrenamiento/resultados_v2_final/scalers/obsoletos/  (directorio completo)
```

### 3.3 · Nuevos sin trackear, **anteriores** a esta sesión (arrastre)

```
v2_reentrenamiento/ENTORNO_COMPUTO.md          (creado 2026-09-04, sin commitear)
v2_reentrenamiento/HANDOFF_CONTEXTO_ACTUAL.md  (creado 2026-09-04, sin commitear)
```
Y archivos de **v1** pendientes de un commit separado, ajenos a esta sesión:
`data/interim/indeci/…`, `data/interim/nasa/…`, `sources/agraria-pe/sin-unificar/…`,
`sources/noticias-ampliado/`, 8 scrapers en `src/scraping/`, `src/weather/nasa_power_downloader.py`.

### 3.4 · Movidos (aparecen como ` D` + `??`)

Los 4 scalers obsoletos se **movieron** (no se borraron) a
`v2_reentrenamiento/resultados_v2_final/scalers/obsoletos/`, junto con un
`README.md` que explica por qué no deben usarse:

```
scaler_sutil.joblib             (28 features, esquema pre-t_index)
scaler_dulce.joblib             (28 features, esquema pre-t_index)
scaler_sutil_v2b_tindex.joblib  (29 features, esquema con ponderación contemporánea)
scaler_dulce_v2b_tindex.joblib  (29 features, esquema con ponderación contemporánea)
```

### 3.5 · NO tocados en esta sesión

- **Todo el proyecto v1:** `notebooks/`, `src/` (raíz), `pipeline/`, `resultados/`,
  `data/`, `sources/`, `dashboard/`, `docs/`.
- **Todo `v2_reentrenamiento/experimentos/`** — verificado por `git status`: cero
  cambios en `exp_001*`, `exp_002*`, `exp_002b*`, `exp_003*` y `REGISTRO_MAESTRO.csv`.
- **Todo `v2_reentrenamiento/notebooks/fase3_modelado/`** — GC1, Naive y el
  diagnóstico `02b` intactos.
- **`v2_reentrenamiento/notebooks/fase2_features/01_nlp_sentimiento.ipynb`** — intacto.
- **`v2_reentrenamiento/data/interim/` y `data/raw/`** — intactos (inputs congelados, D21).

---

## 4. FASE 2 — ESTADO CONGELADO

**Fase 2 fue regenerada por completo el 2026-09-07 y queda APROBADA
PROVISIONALMENTE** por el investigador para continuar con el diseño experimental.

### 4.1 · Split congelado

Dataset tras el `dropna` de lag6: **TOTAL = 114 meses (2016-07 … 2025-12)**.

| Partición | Rango | n filas |
|---|---|---:|
| **TRAIN** | 2016-07 … 2023-12 | **90** |
| **VALIDATION** | 2024-01 … 2024-12 | **12** |
| **TEST** | 2025-01 … 2025-12 | **12** |

### 4.2 · Secuencias con `lookback = 6`

| Partición | Meses objetivo | n secuencias |
|---|---|---:|
| **TRAIN** | 2017-01 … 2023-12 | **84** |
| **VALIDATION** | 2024-01 … 2024-12 | **12** |
| **TEST** | 2025-01 … 2025-12 | **12** |
| | **TOTAL** | **108** |

### 4.3 · Cómo deben construirse las secuencias (regla obligatoria)

> Las secuencias se construyen **sobre el array temporal completo de 114 filas**,
> y **después** se asignan a partición **según la fecha del TARGET**, nunca según
> la fecha del contexto.

Contexto de una secuencia = las **6 filas estrictamente anteriores** al mes
objetivo. Con índices posicionales sobre las 114 filas (`t_index == 0..113`):

```
fila   0 ..  89   2016-07 .. 2023-12   TRAIN
fila  90 .. 101   2024-01 .. 2024-12   VALIDATION
fila 102 .. 113   2025-01 .. 2025-12   TEST

objetivo válido si j >= 6   ->   j = 6..113   ->   108 secuencias
  TRAIN  j =   6.. 89  ->  84    (pierde 6: los objetivos 2016-07..2016-12
                                  exigirían filas anteriores a 2016-07, eliminadas
                                  por el dropna de lag6)
  VAL    j =  90..101  ->  12    (no pierde ninguna)
  TEST   j = 102..113  ->  12    (no pierde ninguna)
```

**Ejemplos obligatorios que deben cumplirse:**

- Objetivo **2024-01** (fila 90) → contexto filas 84–89 = **2023-07 … 2023-12**
  (filas de TRAIN).
- Objetivo **2025-01** (fila 102) → contexto filas 96–101 = **2024-07 … 2024-12**
  (filas de VALIDATION).

En ambos casos el contexto son observaciones **estrictamente pasadas** respecto
del mes objetivo, por lo que no hay fuga de información. **Enero–diciembre 2025
se evalúa completo (12 meses), sin pérdida por el lookback y sin ningún relleno
artificial (padding).**

> **Error a no repetir:** en v1 las secuencias de test se construían *dentro* de
> la partición de test, lo que consumía sus 6 primeros meses. En GM_v2 y GM_v3
> eso redujo el test evaluado de 12 a **6** meses.

### 4.4 · Esquema resultante

| Artefacto | Antes | Después |
|---|---|---|
| `master_dataset_{c}_v2.csv` | (120, 18) | **(120, 16)** |
| `master_dataset_{c}_v2_features.csv` | (114, 33) | **(114, 48)** |
| `master_dataset_{c}_v2_escalado.csv` | (114, 33) | **(114, 48)** — 44 escaladas |

Las 48 columnas de `features`/`escalado`:

```
 0 año                    1 mes                    2 produccion_t_{c}   <- TARGET
 3 mes_sin                4 mes_cos                5 t_index
 6..8    produccion_t_{c}_lag{1,3,6}
 9..23   T2M / T2M_MAX / WS2M / PRECTOTCORR / RH2M      x lag{1,3,6}   (15)
24..41   num_emergencias / personas_afectadas / personas_damnificadas /
         total_afectados / hectareas_cultivo_perdidas /
         hectareas_cultivo_afectadas                     x lag{1,3,6}   (18)
42..47   avg_sentiment / n_noticias                      x lag{1,3,6}   (6)
```

**Features predictivas disponibles = 45** (48 − `año` − `mes` − target).
Columnas **no escaladas**: `año`, `mes`, `mes_sin`, `mes_cos`.

### 4.5 · Verificaciones que pasaron

- NaN = 0 e Inf = 0 en los 6 archivos.
- Anti-fuga celda a celda: 4 788 comprobaciones por cultivar (114 × 42), **0 fallos**.
- Cero exógenas contemporáneas; 42 columnas `_lag` = (13 exógenas + 1 target) × 3.
- Scaler: `n_samples_seen_ = 90`, `n_features_in_ = 44`; `|media_train| máx`
  4.93e-15 (Sutil) / 5.67e-15 (Dulce); `std_train = 1.0000000000`;
  `|media_val| máx` 1.9631 / 2.2481 y `|media_test| máx` 2.4250 / 2.4250 (>0 ⇒
  solo `transform`, nunca `fit`).

---

## 5. CORRECCIONES METODOLÓGICAS CERRADAS HOY

Todas están documentadas en `v2_reentrenamiento/DECISIONES_METODOLOGICAS.md`,
sección **«2026-09-07 — Corrección de look-ahead y regeneración completa de Fase 2»**.

### A. Ponderación espacial — **CERRADA (D14, D15, D16, D17)**

**Problema.** La agregación provincia→nacional de NASA e INDECI ponderaba por la
producción provincial del **mismo mes t**:

```
ANTES (look-ahead):  C_t = Σ_p( C_{p,t} · Y_{p,t} ) / Σ_p( Y_{p,t} )
```

Calcular `T2M[t]` exigía conocer la producción provincial de *t*, que es lo que se
quiere predecir. Magnitud medida del artefacto: la ponderación por producción
elevaba la correlación clima–producción de 0.24 a 0.66 (T2M) y de −0.07 a +0.65
(T2M_MAX) frente a la media espacial simple.

**Nueva regla aprobada.**

```
area_media_train_p = mean( verde_actual_ha[p, t] ),   t ∈ 2016-01 .. 2023-12
w_p                = area_media_train_p / Σ_j( area_media_train_j )
C_{cultivar,t}     = Σ_p( w_{cultivar,p} · C_{p,t} )
```

Los pesos son **fijos**, **independientes por cultivar**, calculados
**exclusivamente con TRAIN 2016-01..2023-12**, y **congelados** para validation y test.

- **Por qué `verde_actual_ha`:** 99.97 % de observaciones >0 (vs 74.9 % Sutil /
  56.0 % Dulce de la producción), 0.00–0.01 % de missing, 0 negativos, 0 outliers,
  unidad consistente, CV interanual mediano 0.133 (Sutil) / 0.000 (Dulce).
  Su baja variación intraanual **no es un problema** porque **no se usa como
  predictor dinámico**, solo como estructura espacial histórica fija.
- **Por qué pesos por cultivar (D16):** el vector combinado anterior daba a Dulce
  solo el **1.65 %** del peso (2 261 139 t Sutil vs 38 001 t Dulce en train), de
  modo que la serie climática del maestro de Limón Dulce estaba determinada al
  ~98 % por las zonas productoras de Sutil (Piura-Tumbes), un régimen climático
  distinto de la selva central de Junín. **El vector combinado queda PROHIBIDO.**
- **D17:** ninguna provincia excluida, sin piso mínimo, sin suavizado. Resultado:
  **106/106 (Sutil)** y **28/28 (Dulce)** provincias con peso estrictamente positivo.

**Concentración resultante:** Sutil HHI 0.1910, N_efectivo 5.24, top-5 0.7319
(PIURA 0.3892, SULLANA 0.1441, LAMBAYEQUE 0.0796, ZARUMILLA 0.0676,
CORONEL PORTILLO 0.0514). Dulce HHI 0.1723, N_efectivo 5.80, top-5 0.7072
(CHANCHAMAYO 0.3311, SATIPO 0.2174, UTCUBAMBA 0.0591, REQUENA 0.0545,
MAYNAS 0.0451).

**Cobertura de la masa de peso:** NASA 100.0000 % en ambos cultivares; INDECI
100.0000 % en Sutil y **97.8881 %** en Dulce (`ANCASH|YUNGAY` 0.011264 y
`ANCASH|CARHUAZ` 0.009856 no tienen registros INDECI). El agregador renormaliza
sobre la masa realmente presente cada mes.

**Artefactos generados:**
`v2_reentrenamiento/resultados_v2_final/pesos/pesos_geograficos_train.csv` (134 filas ×
15 columnas = 106 Sutil + 28 Dulce) y su metadata
`pesos_geograficos_train_meta.json`.

### B. `n_provincias` — **ELIMINADA (D20-a) · CERRADA**

Verificado empíricamente que era exactamente `#{p : produccion[p,t] > 0}`, con
**coincidencia 120/120 meses en ambos cultivares**. Es decir, una transformación
contemporánea de la distribución provincial del target → **look-ahead**.

Evidencia adicional de que medía cobertura del reporte MIDAGRI y no un fenómeno
agronómico: oscilaba entre **60 y 94** provincias mes a mes sobre un universo de
107 (Sutil), y entre 8 y 20 sobre 28 (Dulce); correlación con el target solo 0.2846.

**NO reincorporarla.** La exposición espacial ya queda representada por los pesos
`verde_actual_ha`.

### C. `precio_chacra_kg` — **ELIMINADA del conjunto predictivo (D20-b/c) · CERRADA**

Razones:
1. Se agregaba ponderando por `produccion_t` **en los dos niveles**
   (distrito→provincia y provincia→nacional), verificado numéricamente.
2. Viaja en el **mismo registro MIDAGRI** que la producción objetivo (columnas
   `SIEMBRA, COSECHA, PRODUCCION, VERDE_ACTUAL, PRECIO_CHACRA` de una misma fila).
3. **Ceros estructurales:** `precio = 0` ⟺ `produccion = 0` en el **100 %** de los
   casos (3 118/3 118 filas Sutil, 1 377/1 377 Dulce). El 0 significa
   «no observado», no un precio real.
4. La disponibilidad operacional de `precio_lag1` **no está verificada**.
5. No es una modalidad necesaria para responder el objetivo predictivo principal.

> ### ⚠️ ACLARACIÓN IMPORTANTE — el target NO cambió
>
> **EL TARGET SIGUE SIENDO LA DEMANDA/PRODUCCIÓN MENSUAL**
> (`produccion_t_sutil` / `produccion_t_dulce`, en toneladas).
>
> Eliminar el precio **como predictor** NO significa pronosticar precio ni
> cambiar el target. El precio **puede utilizarse posteriormente como dato o
> supuesto de la simulación económica**, pero **no como predictor contemporáneo
> del forecast**.

Permanece disponible en `data/interim/limon_{sutil,dulce}_provincia.csv` y
`limon_{sutil,dulce}_nacional_mensual.csv` para trazabilidad.

### D. Protocolo temporal — **CERRADA (D6)**

**ROLLING ONE-STEP-AHEAD.**

```
información disponible hasta el cierre de t   →   predicción de y[t+1]
```

Ejemplo del test: datos hasta 2024-12 → y[2025-01]; datos hasta 2025-01 →
y[2025-02]; … ; datos hasta 2025-11 → y[2025-12]. Los 12 meses de test 2025 se
preservan.

**Esto NO es nowcasting.** **No se usan exógenas del mes objetivo.**

### E. Lags — **CERRADA**

Respecto del target se mantienen **lag1, lag3, lag6** para todas las exógenas
aprobadas.

> **Orden obligatorio: la agregación espacial se realiza ANTES de generar los
> lags.** Primero `C_{cultivar,t} = Σ_p( w_p · C_{p,t} )` sobre la serie
> provincial; después los rezagos sobre la serie **nacional ya agregada**. Nunca
> se rezaga a nivel provincial antes de agregar. Los pesos espaciales permanecen
> fijos; los lags solo cambian el instante temporal.

### F. NLP — **CERRADA**

| Modelo | ¿Consume NLP? |
|---|---|
| **GC2** | **NO** |
| **GE** | **NO** (es la definición del grupo) |
| **GM** | **SÍ**, pero **únicamente rezagado** |

En GM solo se admiten:

```
avg_sentiment_lag1   avg_sentiment_lag3   avg_sentiment_lag6
n_noticias_lag1      n_noticias_lag3      n_noticias_lag6
```

**NO reincorporar NLP contemporáneo respecto del target.**

> **Cambio metodológico respecto de v1:** en v1, GM usaba `avg_sentiment` y
> `n_noticias_beto` **contemporáneos** dentro del Canal B. En v2 solo se usan
> rezagados. Esto altera la hipótesis sustantiva: pasa de *«el sentimiento del mes
> explica la producción del mes»* a *«el sentimiento de meses previos anticipa la
> producción»*. Debe declararse en el PPI (ver **D27**, abierta).

### G. `t_index` — **CERRADA**

Definición **calendario** aprobada e implementada:

```
t_index = 12·(año − 2016) + (mes − 1) − 6
```

- Rango final: **0 … 113** (`int64`).
- Checks verificados en ambos cultivares:
  **2016-07 = 0 · 2023-12 = 89 · 2024-01 = 90 · 2025-12 = 113**.
- Se genera **después** del `dropna` de lags y **antes** del split y del scaler.
- `t_posicional = np.arange(len(df))` se calcula **solo como CHECK**, nunca como
  definición; hay `assert t_index == t_posicional`.
- Valores escalados resultantes: min **−1.7129**, máx **+2.6367**, media **+0.4619**.

**Contexto histórico:** `t_index` existía en los CSV desde el 2026-09-03 pero
**ningún código lo generaba** (búsqueda repo-wide: cero coincidencias en celdas de
código). Su definición se recuperó por reconstrucción aritmética verificada contra
los tres valores publicados, y ahora está implementada de forma reproducible en
`src/features/features_temporales.py`.

---

## 6. REPRODUCIBILIDAD / ARTEFACTOS

**SHA-256 se introdujo en esta sesión como convención de reproducibilidad de v2.**
No existía en v1 ni en la Fase 2 v2 original.

### 6.1 · Módulos de lógica núcleo (nuevos)

```
v2_reentrenamiento/src/features/__init__.py
v2_reentrenamiento/src/features/hashing.py               clase Hasher
v2_reentrenamiento/src/features/pesos_geograficos.py     clases PesosGeograficos, VentanaTrain
v2_reentrenamiento/src/features/agregacion_espacial.py   clase AgregadorEspacial
v2_reentrenamiento/src/features/features_temporales.py   clase FeaturesTemporales
v2_reentrenamiento/src/features/escalado.py              clase EscaladorTrain
```

### 6.2 · Notebooks de Fase 2 (orquestadores delgados sobre `src/`)

```
v2_reentrenamiento/notebooks/fase2_features/00_pesos_geograficos.ipynb        (NUEVO)
v2_reentrenamiento/notebooks/fase2_features/02_fusion_dataset_maestro.ipynb   (REESCRITO)
v2_reentrenamiento/notebooks/fase2_features/03_features_temporales.ipynb      (REESCRITO)
v2_reentrenamiento/notebooks/fase2_features/04_escalado.ipynb                 (REESCRITO)
```
Cada uno con su contraparte `_ejecutado.ipynb`. Orden de ejecución: **00 → 02 → 03 → 04**.
(`01_nlp_sentimiento.ipynb` no se tocó.)

### 6.3 · Artefactos de pesos y metadata

```
v2_reentrenamiento/resultados_v2_final/pesos/pesos_geograficos_train.csv
v2_reentrenamiento/resultados_v2_final/pesos/pesos_geograficos_train_meta.json
v2_reentrenamiento/resultados_v2_final/pesos/fase2_v2_regenerada_meta.json
v2_reentrenamiento/resultados_v2_final/pesos/REGISTRO_PRE_REGENERACION.json
```
`REGISTRO_PRE_REGENERACION.json` contiene el estado **previo** a la regeneración
(13 artefactos afectados con su SHA-256 y shape anteriores + 8 inputs congelados).

### 6.4 · Scalers

**Vigentes** (44 features, `n_samples_seen_ = 90`):
```
v2_reentrenamiento/resultados_v2_final/scalers/scaler_sutil_v2c.joblib
v2_reentrenamiento/resultados_v2_final/scalers/scaler_sutil_v2c_parametros.csv
v2_reentrenamiento/resultados_v2_final/scalers/scaler_dulce_v2c.joblib
v2_reentrenamiento/resultados_v2_final/scalers/scaler_dulce_v2c_parametros.csv
```
Parámetros del target: Sutil `mean_train = 23431.9954`, `scale_train = 8041.5165`;
Dulce `mean_train = 391.6001`, `scale_train = 144.2033`.

**Obsoletos — NO USAR** (movidos, no borrados):
```
v2_reentrenamiento/resultados_v2_final/scalers/obsoletos/README.md
v2_reentrenamiento/resultados_v2_final/scalers/obsoletos/scaler_sutil.joblib             (28 feat.)
v2_reentrenamiento/resultados_v2_final/scalers/obsoletos/scaler_dulce.joblib             (28 feat.)
v2_reentrenamiento/resultados_v2_final/scalers/obsoletos/scaler_sutil_v2b_tindex.joblib  (29 feat.)
v2_reentrenamiento/resultados_v2_final/scalers/obsoletos/scaler_dulce_v2b_tindex.joblib  (29 feat.)
```

### 6.5 · SHA-256 de los artefactos regenerados

Leídos del disco el 2026-09-07 21:17.

| Archivo (relativo a `v2_reentrenamiento/`) | SHA-256 |
|---|---|
| `data/processed/master_dataset_sutil_v2.csv` | `220d9a9aabf6e41a31ee5950223eab3a74ddeadba18b8a8e281021b2af84b3f8` |
| `data/processed/master_dataset_sutil_v2_features.csv` | `a6f23d0eb3178514c75f6851f3c2039837a192acc5f2b008bc445dda35968cc9` |
| `data/processed/master_dataset_sutil_v2_escalado.csv` | `d664fd728798d52e5c1bbe702745d40d49b8761513cef148e0a56f2481ca7941` |
| `data/processed/master_dataset_dulce_v2.csv` | `547443d1a117217b8f01f723f345749d530597b45210922a7e609469521934d0` |
| `data/processed/master_dataset_dulce_v2_features.csv` | `c0e7cbe99fdf514cf3bde3c3f735b7d14fc63b27c7f3f163ec665ae25696a188` |
| `data/processed/master_dataset_dulce_v2_escalado.csv` | `38466a4f9581f7d8058962248161d7cb1d467a82e7c2092de73364920dd84f80` |
| `resultados_v2_final/pesos/pesos_geograficos_train.csv` | `6751f5af82b38193dfcf03260165346b4db1fe1d1458eaeff0432aeac2fa7c4f` |
| `resultados_v2_final/scalers/scaler_sutil_v2c.joblib` | `ba51666f00ded716c63698dd04e29068d58d3cab064202384bfbd4435fbf350e` |
| `resultados_v2_final/scalers/scaler_dulce_v2c.joblib` | `1bf919b4c00d236801f12df420b79a16f47c4fe4df3b711472cdc36428eb962b` |

### 6.6 · Inputs interim CONGELADOS (D21)

**No deben modificarse.** La cadena
`sisagri_2015_2026.xlsx → distrito → provincia → nacional` **no es reproducible**:
no existe código en el repositorio que la genere (verificado repo-wide; los únicos
4 archivos que la mencionan son consumidores). Se tratan como inputs congelados,
registrados por hash. La reconstrucción de Fase 1 **queda fuera del alcance**.

| Archivo (relativo a `v2_reentrenamiento/`) | SHA-256 |
|---|---|
| `data/interim/limon_sutil_provincia.csv` | `74566d396566122ce24780a960d77bdf797de00b0ee6d7fe5916ab8dfbe301d2` |
| `data/interim/limon_dulce_provincia.csv` | `06581dd806c5ffd2f3fd8d1286e0bd34e99955c7c8716d4009a48cc711b4d81c` |
| `data/interim/indeci_limpio_2016_2025.csv` | `75f021ad693a5ff154f659fff025a239e5b8f25e99bd4b14d4290bff13388be9` |
| `data/raw/nasa_power/por_provincia/clima_nasa_power_2016_2025.csv` | `fddd0291afd9cbe79dc87be18ad6597bce20d1d9c2d4cb7d35d94b5f9b1d867e` |
| `data/interim/noticias/sentimiento_mensual_2016_2025.csv` | `c3424fc11ef77ba9d33157ab0510aec795f8848c18ea5511b7d649e4c134cb7b` |

Las fórmulas de agregación quedaron **reconstruidas y verificadas numéricamente**
en esta sesión (aunque el código no exista):
`produccion_t` provincia = SUMA distrital · `verde_actual_ha` = SUMA distrital ·
`precio_chacra_kg` = media ponderada por producción (ambos niveles) ·
`produccion_t_nacional` = SUMA provincial · `n_provincias_reportando` =
`#{p : produccion > 0}`.

---

## 7. GC1 Y NAIVE — INTACTOS

- **GC1 y el baseline Naive NO fueron reentrenados ni modificados en esta sesión.**
  `git status` confirma cero cambios en `v2_reentrenamiento/experimentos/` y en
  `v2_reentrenamiento/notebooks/fase3_modelado/`.
- **Verificación de la serie objetivo:** `produccion_t_{cultivar}` es la SUMA
  provincial y **no depende de los pesos espaciales**. Comprobado
  `max|dif| = 0.000000000000` contra `limon_{c}_nacional_mensual.csv` en ambos
  cultivares.
- Contra la serie `real_t` registrada por GC1 en sus `predicciones.csv`:
  `exp_002b_sarima_sutil_simple` `max|dif| = 0.005` y `exp_003_prophet_dulce`
  `max|dif| = 0.003`. **Ambas se explican únicamente por el `.round(2)`** con que
  GC1 escribió esos CSV: contra `round(produccion, 2)` la diferencia es
  **0.0000000000**. La serie subyacente es idéntica.

### Estado vigente de GC1 (no reinterpretar ni cambiar)

| Experimento | Cultivar | Orden | MAE test | R² test |
|---|---|---|---:|---:|
| `exp_001_naive_sutil` | Sutil | — | 4 704.29 | 0.4746 |
| `exp_001_naive_dulce` | Dulce | — | 84.70 | 0.6686 |
| **`exp_002b_sarima_sutil_simple`** ⭐ | Sutil | (1,1,1)(1,1,0,12) | **3 638.14** | **0.7552** |
| `exp_002_sarima_sutil` (descartado, se conserva) | Sutil | (1,1,3)(2,1,0,12) | 12 031.69 | −1.1115 |
| `exp_002_sarima_dulce` | Dulce | (1,0,0)(0,1,0,12) | 53.40 | 0.8846 |
| `exp_003_prophet_sutil` | Sutil | — | 3 682.03 | 0.7925 |
| `exp_003_prophet_dulce` | Dulce | — | 64.58 | 0.8094 |

Decisiones de GC1 ya cerradas y documentadas (no reabrir):
- **Bug de horizonte corregido:** un único `forecast(steps=24)` desde el fin de
  train; val = pasos 1–12, test = pasos 13–24. **El test de GC1 está a horizonte
  13–24 meses**, no 1–12.
- **`exp_002b` es el SARIMA-Sutil representativo**, elegido *a priori* por
  parsimonia (no por AIC ni mirando test). `exp_002` se conserva como registro
  auditable del hallazgo de sobreajuste por AIC.
- **Limitación declarada:** el procedimiento de selección automática por AIC queda
  invalidado como criterio único; en GC2/GE/GM la selección debe evaluarse en val
  con métrica de error.
- **Determinismo verificado:** la re-ejecución completa reprodujo las métricas
  hasta el último dígito (SARIMAX por MLE y Prophet con `mcmc_samples=0` son
  deterministas en esta configuración).

---

## 8. AUDITORÍA PREVIA A GC2 REALIZADA HOY

### 8.1 · GC2 v1 — especificación exacta

Fuente: `notebooks/fase3/actividad_13_gc2_sarimax_lstm.ipynb` (**sin outputs
guardados y sin contraparte `_ejecutado`**) + `resultados/gc2/gc2_metricas.json`.

**SARIMAX**
- Endógena: `produccion_t`, media provincial mensual, **en z-score de Fase 2**.
- Exógenas (8): `T2M`, `PRECTOTCORR`, `QV2M`, `RH2M`, `ALLSKY_SFC_SW_DWN`,
  `WS2M`, `num_emergencias`, `hectareas_cultivo_perdidas`.
- `order = (1, 0, 0)` · `seasonal_order = (2, 1, 0, 12)` — **heredados de GC1 v1**
  (`auto_arima`, criterio AIC, solo train). No se evaluaron.
- `n_train = 44`, `n_test = 12`, corte 2024-09-01, split 80/20 **sin validation**.
- `StandardScaler` de exógenas con `fit` solo sobre `X_train`.
- `enforce_stationarity=False`, `enforce_invertibility=False`, `fit(disp=False, maxiter=500)`.
- 12 parámetros estimados (1 AR + 2 SAR + 8 exóg + σ²).

**LSTM residual**
- Target: residuo escalado en `t+6`; residuos = `y_train − sarimax_res.fittedvalues`
  (**one-step in-sample**).
- Features: **9** = 1 canal de residuos + 8 exógenas. Lookback **6**.
  Formas `X (38, 6, 9)`, `y (38,)`.
- Arquitectura: `LSTM(32)` → `Dropout(0.30)` → `Dense(16, relu)` → `Dense(1)`.
  **Sin Attention.** ≈**5 921** parámetros entrenables (deducido; nunca se imprimió).
- Optimizer `Adam`, learning rate **1e-3**, loss `mse`, métrica `mae`.
- Batch **4**. Max epochs **300**; ejecutadas **26**.
- `EarlyStopping(val_loss, patience=25, restore_best_weights=True)`.
- `ReduceLROnPlateau(factor=0.5, patience=12, min_lr=1e-6)`. **Sin ModelCheckpoint.**
- `shuffle`: **no especificado → default `True`** de Keras.
- Seed: `tf.random.set_seed(42)`, `np.random.seed(42)`, `TF_ENABLE_ONEDNN_OPTS='0'`.
  Sin `PYTHONHASHSEED`, sin `TF_DETERMINISTIC_OPS`.
- Validation: `validation_split=0.15` sobre las 38 secuencias (≈32 train / 6 val).
  **No había conjunto de validación declarado.**
- Protocolo de inferencia: `sarimax_res.forecast(steps=12, exog=X_test_sc)`
  **+** corrección LSTM **recursiva** (semilla = últimos 6 residuos de train,
  cada predicción se realimenta).
- Resultado: MAE **0.1969**, RMSE 0.2926, R² **−53.63**. Ablación SARIMAX-solo:
  MAE 0.1798 → **el híbrido empeoró respecto de su propia componente estadística**.

**Problemas detectados en GC2 v1**
1. **Padding artificial:** los primeros pasos de test rellenaban el contexto de
   exógenas repitiendo la **última fila de train** en vez de usar los meses reales
   anteriores.
2. **Desajuste de horizonte del LSTM:** se entrena sobre residuos *one-step
   in-sample* y se aplica de forma recursiva a residuos *multi-step*, cuya
   estructura de error es distinta. Probable causa principal del R² −53.6.
3. **Orden SARIMAX heredado sin evaluación** (AIC de GC1).
4. **Sin validation declarada.**
5. **Métricas en z-score**, no comparables con la escala en toneladas de v2.
6. **Hiperparámetros distintos de GE/GM v1** sin justificación: batch 4 vs 8,
   patience 25 vs 15, ReduceLROnPlateau 12 vs 8.
7. **Entorno de ejecución indeterminable** (el notebook no guardó outputs).
8. `QV2M` y `ALLSKY_SFC_SW_DWN` **no existen en v2** → el conjunto exógeno de v1
   es irreproducible (6 de 8 disponibles).

**Clasificación de los hiperparámetros de GC2 v1**

| Categoría | Elementos |
|---|---|
| FIJO POR DISEÑO | endógena, escalado solo-train de exógenas, lookback 6, split 80/20, Adam, estructura híbrida, seed 42, oneDNN off |
| HEREDADO DE GC1 | `order`, `seasonal_order` |
| ELEGIDO ARBITRARIAMENTE | las 8 exógenas, 32 unidades, dropout 0.30, Dense 16, lr 1e-3, batch 4, 300 epochs, patience 25, RLROP 12, `validation_split` 0.15, `enforce_*=False` |
| AJUSTADO CON VALIDATION | **ninguno** (no hubo búsqueda) |
| AJUSTADO MIRANDO TEST | **ninguno en GC2** (se ejecutó una sola vez, sin iteración) |
| NO DOCUMENTADO | `shuffle`, nº de parámetros, ausencia de ModelCheckpoint, doble normalización, entorno de ejecución |

### 8.2 · GE v1

- **Intención metodológica:** «Grupo Estructural» — la arquitectura propuesta de
  la tesis, **sin NLP**. Referencia declarada: Gu et al. (2022), dual-channel
  LSTM-Attention.
- **Arquitectura v1:** dos canales. Canal A = `produccion_t` (1 feature);
  Canal B = 23 exógenas. Cada canal: `LSTM(64, return_sequences=True)` →
  `Dropout(0.30)` → `BahdanauAttention(64)`. Fusión: `Concatenate` (128) →
  `Dense(64, relu)` → `Dropout(0.15)` → `Dense(16, relu)` → `Dense(1)`.
  L2 = 0.001 en ambos LSTM y en `dense_1`. **65 249 parámetros** (verificado).
  Adam 1e-3, batch 8, max 300 epochs (ejecutadas 20, mejor época 5),
  EarlyStopping patience 15, ReduceLROnPlateau patience 8, ModelCheckpoint activo.
  `n_train = 40`, `n_test = 10`. Predicción **recursiva multi-step**.
- **Ausencia de NLP:** confirmada en `CANAL_B_COLS` (23 columnas, ninguna de NLP),
  en `README.md` («GE: Dual-LSTM sin NLP») y en `docs/pipeline_fases_234.md`.
- MAE test v1: **0.0673**.
- **Cobertura del Canal B de GE v1 en v2: 11 de 23 (48 %)** — 3 NASA inexistentes
  (`T2M_MIN`, `QV2M`, `ALLSKY_SFC_SW_DWN`), 4 temporales no generadas
  (`mes_num`, `trimestre_num/sin/cos`), 2 geográficas no aplicables (`lat`, `lon`,
  sin sentido en serie nacional), 1 precio eliminado por decisión.

### 8.3 · GM v1

- **Intención metodológica:** «Grupo Multimodal» — GE **+ NLP**. Es el peldaño que
  aísla el aporte de la señal de noticias.
- **GM original** (`actividad_15`): arquitectura **idéntica a GE**, Canal B = 25
  (23 + `avg_sentiment` + `n_noticias_beto`, **contemporáneos**).
  **65 761 parámetros.** Mismos hiperparámetros, mismo split (40/10), misma
  predicción recursiva. MAE test **0.0981**.
- **GM_v2 / GM_v3** (`actividad_15v2`, `actividad_15v5`): **arquitectura
  completamente distinta** — `PCA(0.95)` → 8 componentes → `LSTM(64)` +
  self-attention aditiva de 1 cabeza (`Dense(1,tanh)` → `Softmax` → `Multiply` →
  `Lambda(reduce_sum)`, **no Bahdanau**) ‖ rama NLP `LSTM(16)` → `Dropout(0.5)`.
  **23 106 parámetros.** `shuffle=False`, `validation_split=0.2`, max 200 epochs.
  MAE test 0.0646 / **0.0645**. GM_v3 difiere de GM_v2 **solo en el fichero de
  corpus NLP**.

### 8.4 · Hallazgo central de la auditoría — REGISTRAR

> **La comparación GE vs GM de v1 NO constituye una ablación limpia del aporte del
> NLP**, porque entre ambos cambiaban simultáneamente:
>
> 1. **Arquitectura** — GE: Dual-LSTM + Bahdanau ×2, 65 249 params.
>    GM_v3: PCA + LSTM + self-attention 1 cabeza, 23 106 params.
> 2. **Protocolo de forecasting** — GE: recursivo multi-step. GM_v3: one-step-ahead.
> 3. **Conjunto de test** — GE: 10 meses (desde 2024-11). GM_v3: **6 meses**
>    (desde 2025-03), porque sus secuencias de test se construían dentro de la
>    partición de test.
> 4. **Entorno numérico** — GE: Windows/CPU, Keras 3.14.0, NumPy 2.4.4, oneDNN
>    desactivado. GM_v3: WSL2/GPU (RTX 3060), Keras 3.14.1, NumPy 2.4.6, oneDNN
>    activo.
>
> El único par realmente comparable de v1 es **GE (0.0673) vs GM original
> (0.0981)** — misma arquitectura, mismos hiperparámetros, mismo split, mismo
> protocolo, difiriendo solo en las 2 columnas de NLP — y ese par indica que el
> NLP **empeoró** el resultado.
>
> **Consecuencia:** la afirmación «el NLP aporta», sostenida en v1 sobre GE vs
> GM_v3, no está respaldada por una ablación válida. v2 debe rehacerla limpiamente.

**Hallazgo adicional — el peldaño GC3 nunca existió.**
`resultados/gc1/reporte_ejecutivo_gc1.md:165` lista «14 — LSTM Multivariado |
GC3 con variables exógenas | Pendiente», y el markdown de `actividad_13` dice
«Sin NLP / sin Attention: diferencia respecto a GC3». En el diseño original había
**dos** peldaños entre GC2 y GM; en la implementación se colapsaron en uno (GE).
Por eso el salto **GC2→GE mezcla tres cambios a la vez** (attention +
dual-channel + conjunto exógeno ampliado).

---

## 9. ESQUEMAS C1 / C2 / C3 — **D25 = ABIERTA**

Tres alternativas auditadas para el diseño de comparabilidad. **Ninguna elegida.**

### C1 — Mismo conjunto informativo para GC2/GE, NLP exclusivo de GM

| | |
|---|---|
| **Ventaja** | Dos ablaciones limpias encadenadas: **GC2→GE aísla solo la ARQUITECTURA**; **GE→GM aísla solo el NLP**. |
| **Problema** | Con 84 secuencias de train y hasta 45 features, GC2 recibiría un vector exógeno mucho mayor del que un SARIMAX puede estimar con 90 observaciones. |
| **Compat. hipótesis** | Alta — lectura literal de «aislar el aporte de cada componente». |
| **Compat. PPI** | Alta. |
| **Compat. v1** | Baja (v1 usaba 8 / 23 / 25 exógenas distintas). |
| **Interpretabilidad** | GC2→GE máxima · GE→GM máxima. |

### C2 — Cada grupo conserva su conjunto propio de exógenas

| | |
|---|---|
| **Ventaja** | Compara configuraciones completas; es lo más parecido a v1. |
| **Problema** | **GC2→GE confunde arquitectura con información** — exactamente el defecto de v1. |
| **Compat. hipótesis** | Baja para aislar componentes; alta si la pregunta es «qué configuración funciona mejor». |
| **Compat. PPI** | Media — sirve para la tabla comparativa, no para la afirmación causal. |
| **Compat. v1** | Alta. |
| **Interpretabilidad** | GC2→GE baja · GE→GM alta (si GM = GE + NLP y nada más). |

### C3 — Reintroducir GC3 y construir una escalera de ablaciones más extensa

```
GC1  univariado
GC2  SARIMAX + LSTM residual                            (híbrido estadístico)
GC3  LSTM multivariado SIN attention, mismas exógenas   ← peldaño vacante
GE   Dual-LSTM + Bahdanau Attention, mismas exógenas
GM   GE + NLP
```

| | |
|---|---|
| **Ventaja** | Tres ablaciones limpias: GC2→GC3 (estadístico vs neuronal), GC3→GE (la attention), GE→GM (el NLP). Es el diseño que la documentación original describe. |
| **Problema** | Añade un modelo al alcance: +1 modelo × 2 cultivares × nº de seeds. |
| **Compat. hipótesis** | La más alta. |
| **Compat. PPI** | Alta, pero amplía el alcance. |
| **Compat. v1** | Nula — GC3 nunca existió. |
| **Interpretabilidad** | Máxima en los tres saltos. |

> **D25 = ABIERTA.** Ninguna alternativa fue elegida. Es la decisión de mayor
> alcance porque fija el conjunto exógeno de los tres (o cuatro) modelos a la vez.

---

## 10. DECISIONES

Estados: **CERRADA** · **ABIERTA** · **SUBORDINADA** · **NO BLOQUEANTE** ·
**RESUELTA PENDIENTE DE DOCUMENTACIÓN**.

### 10.1 · Cerradas en sesiones previas o en esta

| ID | Decisión | Estado |
|---|---|---|
| **D3** | Validation = 2024-01..2024-12 cronológica explícita; sin `validation_split` interno del train | **CERRADA** |
| **D5** | Protocolo principal = rolling one-step-ahead; recursive reservado como análisis secundario | **CERRADA** |
| **D6** | Protocolo temporal = FORECAST ROLLING ONE-STEP-AHEAD; no nowcasting; sin exógenas del mes objetivo | **CERRADA** |
| **D14** | Ponderador espacial = `verde_actual_ha` | **CERRADA** |
| **D15** | Resumen temporal = media sobre TRAIN 2016-01..2023-12; pesos congelados | **CERRADA** |
| **D16** | Vectores de peso independientes por cultivar; combinado PROHIBIDO | **CERRADA** |
| **D17** | Sin exclusión de provincias, sin piso mínimo, sin suavizado | **CERRADA** |
| **D18** | Fase 2 se corrige antes de GC2/GE/GM, en una sola pasada junto con `t_index` | **CERRADA — ejecutada** |
| **D19** | SHA-256 como convención de reproducibilidad de v2 | **CERRADA — implementada** |
| **D20-a** | `n_provincias` ELIMINADA | **CERRADA** |
| **D20-b/c** | `precio_chacra_kg` ELIMINADA del conjunto predictivo | **CERRADA** |
| **D21** | Interim MIDAGRI como inputs congelados; no se reconstruye Fase 1 | **CERRADA** |
| **D23** | Etiquetado forecast vs nowcast | **CERRADA** — resuelta al aprobar D6: es **forecast** |
| **NLP** | GC2 sin NLP · GE sin NLP · GM con NLP **solo rezagado** | **CERRADA** |
| **`t_index`** | Definición calendario `12·(año−2016)+(mes−1)−6`, rango 0..113 | **CERRADA — implementada** |

### 10.2 · Abiertas

| ID | Decisión | Estado | Dependencia | Siguiente acción |
|---|---|---|---|---|
| **D25** | Esquema de comparabilidad: **C1 / C2 / C3** | **ABIERTA — BLOQUEANTE PRIORITARIA** | ninguna | Decidir §9. Fija el conjunto exógeno de todos los modelos a la vez |
| **D1 resto** | Conjunto exógeno final de GC2 sobre las 45 features disponibles (qué lags climáticos, qué INDECI, `t_index` en SARIMAX o solo en LSTM, lags del target en el LSTM residual) | **SUBORDINADA a D25** | D25 | Resolver tras D25 |
| **D2.1** | Orden SARIMAX: heredar de v1 / heredar de GC1 v2 / rejilla en val / parsimonia a priori | **ABIERTA — bloqueante** | D1 resto | Nota: heredar de v1 incumpliría la regla ya aprobada (orden por AIC) |
| **D2.2** | SARIMAX con **90** obs. (2016-07..2023-12) vs **96** obs. (2016-01..2023-12) | **ABIERTA — bloqueante** | ninguna | El LSTM residual no gana ninguna secuencia con 96; solo mejora la estimación del SARIMAX |
| **D2.3** | Actualización en el rolling: **A** parámetros congelados + estado actualizado · **B** re-fit mensual · **C** recursivo sin actualizar | **ABIERTA — bloqueante** | D6 (cerrada) | A y B satisfacen D6 y corrigen el desajuste de horizonte de v1; C lo contradice |
| **D0** | Determinismo: `TF_ENABLE_ONEDNN_OPTS=0` + `TF_DETERMINISTIC_OPS=1` vs default | **ABIERTA — bloqueante antes del primer `fit`** | ninguna | GC2 v1 ya usaba el primero. Fijarlo antes es gratis; después obliga a reentrenar |
| **D4** | Hiperparámetros del LSTM y de los callbacks (`patience`, `factor`, `min_lr`, batch, unidades, epochs) | **ABIERTA — bloqueante para ejecutar** | D25 | v1 GC2 difería de v1 GE/GM sin justificación |
| **D3-bis** | Con **12 secuencias** de validation, `val_loss` será muy ruidosa para el early stopping. ¿Se acepta, o se añade un criterio/tolerancia? | **ABIERTA — no bloqueante para escribir** | D3 (cerrada) | Consecuencia directa de D3 |
| **D7** | `shuffle` en `model.fit`: `True` (GC2/GE v1) o `False` (GM_v2/v3 v1) | **ABIERTA — NO BLOQUEANTE** | — | Decidir antes de entrenar |
| **D8** | Estructura de resultados y esquema de `REGISTRO_MAESTRO.csv` con múltiples seeds (¿media? ¿`_std`? ¿`_n_seeds`? ¿ruta `exp_004_gc2_<cultivar>/seeds/seed_N/`?) | **ABIERTA — NO BLOQUEANTE** | D10 | El registro actual solo admite escalares |
| **D10** | Número de seeds | **ABIERTA — bloqueante para ejecutar, no para escribir** | — | Reservada explícitamente por el investigador |
| **D11** | Destino final de los 4 scalers obsoletos (hoy en `scalers/obsoletos/`) | **ABIERTA — NO BLOQUEANTE** | — | ¿Se borran o se mantienen archivados? |
| **D12** | Documentar la consecuencia 90 → 84 secuencias | **RESUELTA PENDIENTE DE DOCUMENTACIÓN** → **YA DOCUMENTADA** en `DECISIONES_METODOLOGICAS.md` (sección 2026-09-07) y en §4.2 de este archivo | — | Cerrar formalmente |
| **D13** | ¿Conjunto exógeno compartido GC2/GE/GM? | **SUBORDINADA — absorbida por D25** | D25 | No decidir por separado |
| **D22** | Alcance del hash de D19: el «origen» auditable son los interinos, no el Excel | **RESUELTA PENDIENTE DE DOCUMENTACIÓN** → los 5 hashes de inputs congelados están en §6.6 y en `pesos_geograficos_train_meta.json` | — | Confirmar aceptación del punto de corte |
| **D24** | Verificación externa de los rezagos reales de publicación de MIDAGRI, NASA POWER e INDECI | **ABIERTA — NO BLOQUEANTE bajo D6** | — | Los tres siguen **NO VERIFICADOS**. D6 evita el problema por diseño al usar solo rezagos, pero la afirmación de despliegue operativo sigue sin sustento |
| **D26** | ¿Se replican los módulos NLP M1–M4 de GM_v2/v3 en v2? Si sí, ¿dónde se calculan `nlp_index` y `nlp_index_lag1`? (no están entre las 48 columnas) | **ABIERTA — NO BLOQUEANTE para GC2** | D25 | Afecta a GM, no a GC2 |
| **D27** | Declarar en el PPI el cambio de NLP contemporáneo (v1) a NLP rezagado (v2), porque modifica la hipótesis sustantiva | **ABIERTA — NO BLOQUEANTE** | — | Decisión de redacción del investigador |

---

## 11. LO QUE NO DEBE CAMBIARSE

- **El target:** `produccion_t_sutil` / `produccion_t_dulce` en toneladas. No se
  pronostica precio.
- **El split:** 90 / 12 / 12 sobre 2016-07..2025-12. 2026 fuera.
- **El protocolo:** rolling one-step-ahead (D6).
- **Los pesos espaciales:** fijos, por cultivar, de TRAIN. El vector combinado está
  PROHIBIDO.
- **`n_provincias` y `precio_chacra_kg`:** no reincorporar como predictores.
- **NLP contemporáneo:** no reincorporar.
- **`t_index`:** definición calendario, rango 0..113.
- **Los inputs interim y raw:** congelados (D21).
- **GC1, `exp_002b` como representativo, el Naive y todo `experimentos/`:** no
  reinterpretar ni reentrenar.
- **Todo el proyecto v1** (`notebooks/`, `src/` raíz, `pipeline/`, `resultados/`).
- **La regla de Δs:** siempre acompañada de `n_shock`; nunca como resultado principal.

---

# PUNTO EXACTO DE REANUDACIÓN

## ⛔ NO ENTRENAR TODAVÍA.

**La próxima sesión debe comenzar resolviendo D25: el esquema
experimental/comparabilidad — C1 vs C2 vs C3** (§9 de este documento).

D25 va primero porque fija el conjunto exógeno de GC2, GE y GM **a la vez**, y de
ella dependen D1-resto y D13. Resolverla después obligaría a rehacer el diseño de
los tres modelos.

### Cadena bloqueante hasta poder escribir GC2

Verificada contra el estado actual del repositorio al cierre de esta sesión:

```
D25  esquema de comparabilidad (C1 / C2 / C3)
  └─> D1 resto   conjunto exógeno final de GC2 (sobre las 45 features disponibles)
        └─> D2.1  orden SARIMAX
        └─> D2.2  90 vs 96 observaciones
        └─> D2.3  estrategia de actualización en el rolling (A / B / C)
              └─> D0   determinismo (antes del primer fit)
                    └─> D4   hiperparámetros del LSTM y de los callbacks
                          └─> ESCRIBIR GC2
                                └─> D10 (nº de seeds) + D7 + D8  antes de EJECUTAR
```

Decisiones que **no** bloquean escribir GC2: D3-bis, D7, D8, D10, D11, D24, D26, D27.

### Archivos que se crearían al implementar GC2 (nada de esto existe aún)

```
v2_reentrenamiento/src/models/arquitecturas/gc2_sarimax_lstm.py
v2_reentrenamiento/src/models/entrenamiento/secuenciador.py
v2_reentrenamiento/src/models/entrenamiento/protocolos.py
v2_reentrenamiento/src/models/entrenamiento/callbacks.py
v2_reentrenamiento/src/models/entrenamiento/runner.py
v2_reentrenamiento/src/evaluation/metricas_v2.py
v2_reentrenamiento/notebooks/fase3_modelado/03_gc2_sarimax_lstm.ipynb
v2_reentrenamiento/experimentos/exp_004_gc2_{sutil,dulce}/...
```
(`v2_reentrenamiento/src/models/` y `src/evaluation/` contienen hoy solo `.gitkeep`.)

---

# INSTRUCCIÓN DE REANUDACIÓN PARA CLAUDE CODE

Al recibir este archivo en una sesión futura, **antes de hacer cualquier otra cosa**:

1. **Lee COMPLETO este archivo** (`CONTEXTO_SESION_2026-09-07.md`).
2. **Lee `v2_reentrenamiento/DECISIONES_METODOLOGICAS.md`** completo — al cierre de
   esta sesión tenía **625 líneas**; la última sección es
   *«2026-09-07 — Corrección de look-ahead y regeneración completa de Fase 2»*.
3. **Comprueba `git status`** y el HEAD. Al cierre: branch `v2-reentrenamiento`,
   HEAD `bfd19a7`, **con cambios sin commit** (§3).
4. **Comprueba que los hashes/artefactos críticos siguen coincidiendo** con §6.5 y
   §6.6. Si alguno difiere, **DETENTE y repórtalo**: significa que los datasets o
   los inputs congelados cambiaron fuera de esta cadena de decisiones.
5. **NO entrenes** ningún modelo.
6. **NO modifiques la metodología** ni ninguna decisión marcada CERRADA en §10.1.
7. **Reporta cualquier contradicción** que encuentres entre este documento, la
   documentación metodológica y el estado real del repositorio.
8. **Continúa desde D25 únicamente después de recibir instrucciones del
   investigador.** No elijas C1, C2 ni C3 por tu cuenta.

**Recordatorio permanente:** ante cualquier ambigüedad metodológica, DETENTE,
repórtala y espera decisión. No la resuelvas unilateralmente aunque exista una
opción que parezca razonable.
