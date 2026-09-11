# CONTEXTO DE SESIÓN — 2026-09-08 · MÉTRICAS Y ESTADO PRE-ENTRENAMIENTO

**Documento de continuidad autocontenido.** Escrito al cierre de la sesión del
2026-09-08. Está pensado para que una sesión futura de Claude Code retome el
trabajo **sin acceso al historial de la conversación que lo generó**.

- **Repositorio:** `C:\Machine-learming\Machine-Learning-Multimodal--Agro-NLP-Clima-`
- **Documento hermano (NO sustituido):** `CONTEXTO_SESION_2026-09-07.md` (51 166 bytes, 956 líneas)
  — sigue siendo válido para Fase 2, ponderación espacial, GC1 y genealogía previa.
  **Este archivo lo COMPLEMENTA; no lo reemplaza.**
- **Qué añade este documento:** taxonomía experimental v2 rediseñada, auditoría
  completa de INDECI (D29–D34), revisión bibliográfica de métricas, panel
  provisional de métricas y el punto pendiente D35.

> ⚠️ **AVISO DE SUPERSESIÓN.** El marco **C1 / C2 / C3** descrito en
> `CONTEXTO_SESION_2026-09-07.md` §9 y la afirmación de su §8.4 de que
> «el peldaño GC3 nunca existió» **quedan OBSOLETOS**. Ver §4 y §16 de este
> documento. Ambos se construyeron sobre una premisa que la auditoría de
> genealogía del 2026-09-08 demostró falsa.

---

## 1. ESTADO GENERAL DEL PROYECTO

### 1.1 · Git

| Concepto | Valor |
|---|---|
| **Branch** | `v2-reentrenamiento` (branch principal del repo: `main`) |
| **HEAD** | `bfd19a7c9de7fd5f1acbc8144429169dc3c4000e` (`bfd19a7`) |
| **Fecha del HEAD** | 2026-09-04 08:17:01 -0500 |
| **Commits en esta sesión** | **NINGUNO** |
| **Cambios sin commit** | **SÍ** — 13 modificados (` M`), 4 borrados/movidos (` D`), 31 sin trackear (`??`) |

Es **exactamente el mismo estado** que al cierre del 2026-09-07. Ni un solo
archivo del repositorio fue modificado en la sesión del 2026-09-08.

### 1.2 · Archivos modificados (` M`) — 13, todos de v2

```
v2_reentrenamiento/DECISIONES_METODOLOGICAS.md                       (625 líneas; 524 en HEAD)
v2_reentrenamiento/data/processed/master_dataset_sutil_v2.csv
v2_reentrenamiento/data/processed/master_dataset_sutil_v2_features.csv
v2_reentrenamiento/data/processed/master_dataset_sutil_v2_escalado.csv
v2_reentrenamiento/data/processed/master_dataset_dulce_v2.csv
v2_reentrenamiento/data/processed/master_dataset_dulce_v2_features.csv
v2_reentrenamiento/data/processed/master_dataset_dulce_v2_escalado.csv
v2_reentrenamiento/notebooks/fase2_features/02_fusion_dataset_maestro.ipynb
v2_reentrenamiento/notebooks/fase2_features/02_fusion_dataset_maestro_ejecutado.ipynb
v2_reentrenamiento/notebooks/fase2_features/03_features_temporales.ipynb
v2_reentrenamiento/notebooks/fase2_features/03_features_temporales_ejecutado.ipynb
v2_reentrenamiento/notebooks/fase2_features/04_escalado.ipynb
v2_reentrenamiento/notebooks/fase2_features/04_escalado_ejecutado.ipynb
```

### 1.3 · Movidos (` D` + `??`) — 4 scalers obsoletos

Movidos, **no borrados**, a `v2_reentrenamiento/resultados_v2_final/scalers/obsoletos/`
junto con un `README.md` que explica por qué no deben usarse:
`scaler_{sutil,dulce}.joblib` (28 features) y `scaler_{sutil,dulce}_v2b_tindex.joblib` (29 features).

### 1.4 · Sin trackear (`??`) — 31 entradas

Creados en la sesión del 2026-09-07: los 6 módulos de `v2_reentrenamiento/src/features/`,
los notebooks `00_pesos_geograficos*.ipynb`, el directorio `resultados_v2_final/pesos/`,
los 4 artefactos `scaler_*_v2c*`, y `scalers/obsoletos/`.
Arrastre anterior: `ENTORNO_COMPUTO.md`, `HANDOFF_CONTEXTO_ACTUAL.md`.
Ajenos a v2 (pendientes de commit separado): `data/interim/indeci/`, `data/interim/nasa/`,
`sources/agraria-pe/sin-unificar/`, `sources/noticias-ampliado/`, 8 scrapers en
`src/scraping/`, `src/weather/nasa_power_downloader.py`, y
`CONTEXTO_SESION_2026-09-07.md`.

**Este archivo (`CONTEXTO_SESION_2026-09-08_METRICAS.md`) se añade como entrada `??` número 32.**

### 1.5 · Drift desde el último handoff

**NINGUNO.** Verificado por dos vías independientes:

1. **Hashes:** los **14** SHA-256 críticos (9 artefactos regenerados de §6.5 del
   handoff anterior + 5 inputs congelados de §6.6) **coinciden exactamente**.
   Ver §14 de este documento.
2. **Marcas de tiempo:** `find . -newermt "2026-09-08 00:00"` sobre el repositorio
   (excluyendo `.git/`, `venv/`, `__pycache__`) devuelve **cero archivos**.

### 1.6 · Confirmaciones obligatorias de esta sesión

- ✅ **NO se entrenó ningún modelo.** Cero llamadas a `.fit()` de cualquier modelo
  predictivo. `v2_reentrenamiento/experimentos/` sigue conteniendo únicamente los
  7 experimentos de Naive y GC1, sin cambios.
- ✅ **NO se regeneró Fase 2.** Los 6 CSV maestros/features/escalado y los 2 scalers
  vigentes conservan sus hashes originales del 2026-09-07 20:58–20:59.
- ✅ **NO se usó test 2025 para ninguna decisión metodológica.**
- ✅ **NO se hizo commit.**
- ✅ **NO se modificó ningún archivo del repositorio.** Todo el trabajo de la sesión
  fue lectura, cómputo de diagnóstico en directorio temporal, y análisis.

---

## 2. REGLA METODOLÓGICA RECTORA

> **El PPI es un documento EVOLUTIVO, NO una especificación rígida.**

### Orden de trabajo aprobado

```
1. Cerrar y auditar la metodología experimental v2.
2. CONGELAR las decisiones ANTES de mirar el test 2025.
3. Entrenar y evaluar.
4. Reportar los resultados REALES, aunque contradigan las hipótesis iniciales.
5. Actualizar el PPI DESPUÉS, según la metodología efectivamente ejecutada
   y los hallazgos reales.
```

### Uso legítimo del PPI actual

- Referencia de la **pregunta científica original**.
- Referencia **conceptual** de la tesis.
- Fuente de **trazabilidad** sobre decisiones anteriores.

### Uso ILEGÍTIMO del PPI actual

- **NO** es autoridad para conservar una decisión metodológica que la auditoría v2
  haya demostrado débil, contaminada, innecesaria o incompatible con el protocolo actual.

### Prohibición explícita

> **NO adaptar el experimento para favorecer al DualLSTM-Attention.**

Ninguna elección de features, métrica, benchmark, escalador, semilla,
hiperparámetro o esquema de comparación puede tomarse porque mejore el resultado
del modelo experimental. Toda elección debe justificarse *a priori* y por diseño.

### Regla permanente de operación (vigente desde 2026-09-07)

> Ante cualquier ambigüedad metodológica, Claude Code debe **DETENERSE**, reportar
> el problema y esperar decisión. No debe resolverla por su cuenta, aunque exista
> una opción que parezca razonable.

### Roles

| Rol | Responsabilidad |
|---|---|
| **ChatGPT** | Supervisor metodológico. Revisión científica y bibliográfica. Genera las instrucciones que se pasan a Claude Code |
| **Claude Code** | Auditor técnico e implementador. Ejecuta **solo** tras aprobación explícita. **No toma decisiones metodológicas abiertas** |
| **Investigador** (Fabrizio Sánchez S., UPeU Juliaca) | Aprobación final de todo cambio y decisión |

---

## 3. PREGUNTA CIENTÍFICA ACTUAL

> **El objetivo experimental YA NO debe formularse como «hacer que DualLSTM gane».**

### Formulación conceptual vigente

> ¿Puede una arquitectura **DualLSTM con atención de Bahdanau y entradas
> multimodales** justificar su mayor complejidad frente a modelos estadísticos y
> XGBoost bajo **Small Data**, tanto en **precisión predictiva global** como en
> **menor deterioro condicionado a shocks climáticos**, y aporta la **modalidad
> NLP** información predictiva adicional?

### Tres dimensiones a separar SIEMPRE

| Dim. | Nombre | Qué mide | Comparación que la responde |
|---|---|---|---|
| **A** | **Precisión predictiva global** | Error sobre los 12 targets de 2025 | GE frente a Naive, GC1, GC2 (XGBoost), GC3 |
| **B** | **Deterioro condicionado a shocks** | Diferencia de error entre meses de shock y meses normales | MAE_shock / MAE_no_shock por modelo; Δs como descriptor |
| **C** | **Valor incremental de NLP** | Aporte de la modalidad de noticias rezagada | **GC3 → GE**, única ablación controlada |

**Un resultado negativo en cualquiera de las tres dimensiones es un resultado
válido y debe reportarse.** La tesis responde una pregunta; no defiende una tesis previa.

---

## 4. TAXONOMÍA CONCEPTUAL ACTUAL

> **ESTADO: propuesta conceptual acordada, NO formalizada todavía en
> `DECISIONES_METODOLOGICAS.md`. NO renumerar ni modificar código todavía.**

### 4.1 · Estructura propuesta

| Etiqueta | Modelo | Rol | Información |
|---|---|---|---|
| **BASELINE** | Naive `ŷ[t] = y[t−1]` | Piso de referencia | 1 (la propia serie) |
| **GC1** | SARIMA | Control univariado | 1 (univariado) |
| **GC2** | **XGBoost** | Control de familia | **36 features no-NLP** |
| **GC3** | DualLSTM + Bahdanau Attention **SIN NLP** | Control estructural / base de ablación | **36 features no-NLP** |
| **GE** | DualLSTM + Bahdanau Attention **CON NLP rezagado** | **GRUPO EXPERIMENTAL (único)** | **42 features** |
| **SECUNDARIO** | Prophet | Benchmark estadístico histórico | 1 (univariado) |
| **FUERA del experimento principal** | SARIMAX + LSTM residual | — | — |

### 4.2 · Fundamento de la nomenclatura

`GC` = **Grupo de Control**; `GE` = **Grupo Experimental**. Confirmado
documentalmente en `resultados/gc1/reporte_ejecutivo_gc1.md`, fechado
**25 de mayo de 2026**, cuyo título es «Fase 3: **Grupo de Control GC1**».

**Definición operativa de Grupo de Control adoptada:**

> Una **configuración de modelo única y completamente especificada** —familia,
> conjunto informativo, protocolo temporal y partición— declarada *a priori* como
> referencia frente a la cual se evalúa el GE. **Un GC es un modelo, una
> configuración, una cifra por cultivar** — no una familia de modelos.

Esto elimina la ambigüedad histórica de `GC1 = {SARIMA, Prophet}`, que ya causó
un problema real (hubo que designar un «SARIMA representativo», `exp_002b`).

### 4.3 · Justificación de cada cambio respecto de v1

| Cambio | Razón (a priori, NO por rendimiento) |
|---|---|
| **XGBoost pasa de «competidor externo» a GC2** | Es el único control que aísla la **familia** manteniendo la matriz de entrada **idéntica** a la de GC3. Declararlo control *a priori* **compromete a reportarlo pase lo que pase**: salvaguarda contra reporte selectivo |
| **Prophet baja a secundario** | Su pregunta propia (tendencia explícita con *changepoints* vs tendencia diferenciada) es real pero **no necesaria** para evaluar el GE. **NO se elimina** y **NO se degrada por su rendimiento** |
| **SARIMAX-LSTM sale del principal** | Su pregunta (hibridación estadístico-neuronal) no es la de la tesis; su rol de puente lo cubre mejor XGBoost, que recibe información **idéntica** a GC3. Además concentraba toda la deuda de decisión abierta (D1, D2.1, D2.2, D2.3) |
| **Un solo GE** | La auditoría de genealogía (§16) demostró que «GE vs GM» como dos grupos de diseño es **etiquetado a posteriori**, no diseño original |

### 4.4 · Advertencia sobre la numeración

> **El índice de `GC_k` es un IDENTIFICADOR, no un ordinal de complejidad ni de
> similitud con el GE.** La progresión Naive → GC1 → GC2 → GC3 → GE mezcla **dos
> ejes** (información y familia de modelo) y no es monótona. Esto debe declararse
> explícitamente en la tesis.

```
                    INFORMACIÓN  ──────────────────────────────────►
                    univariada        + exógenas (36)       + NLP (42)
   F  persistencia │  BASELINE
   A               │
   M  estadística  │  GC1 SARIMA
   I               │  [Prophet, secundario]
   L  ML tabular   │                    GC2 XGBoost
   I               │
   A  DL recurrente│                    GC3 DualLSTM  ──────►  GE
      + atención   │                                  (ablación NLP)
```

### 4.5 · Tabla de equivalencias v1 ↔ v2 (para no perder trazabilidad)

| v1 | v2 propuesto |
|---|---|
| GC1-SARIMA | **GC1** |
| GC1-Prophet | **Secundario** |
| GC2 (SARIMAX+LSTM) | **Fuera del principal** |
| XGBoost (competidor, `act.15v3`) | **GC2** |
| **«GE»** (`act.14`, sin NLP) | **GC3** |
| **«GM»** (`act.15`, con NLP) | **GE** |
| TCN, GM_v2 / GM_v3 / GM_v4 | Fuera del diseño |

### 4.6 · Clasificación de las comparaciones

| Comparación | Tipo | Qué PERMITE concluir | Qué **NO** permite concluir |
|---|---|---|---|
| BASELINE → GC1 | Baseline → control | Si la estructura temporal univariada supera la persistencia | Nada sobre exógenas |
| GC1 → GC2 | Entre familias | Poco: cruza familia **e** información | Cualquier atribución a un solo factor |
| **GC2 → GC3** | **Entre familias, información controlada** | Que, con **matriz idéntica (36)**, la arquitectura recurrente con atención rinde X frente a boosting | Que «la recurrencia aporta X» ni «la atención aporta X» por separado |
| **GC3 → GE** | **ABLACIÓN CONTROLADA** | Que incorporar la **modalidad de noticias rezagada** (6 columnas, +1 536 parámetros), con todo lo demás constante, modifica el error en X (media ± sd sobre S semillas) | Efecto causal del sentimiento. Generalización poblacional (n_test = 12). Separar «señal» de «capacidad» sin control placebo |
| **GC2 → GE** | **Compuesta** | **Solo vía la descomposición GC2 → GC3 → GE** | Nada directamente: cruza familia **e** información |
| GC1 → Prophet | Benchmark secundario | Si la tendencia explícita cambia el resultado en la familia clásica | Nada sobre el GE |

### 4.7 · Condiciones EXACTAS para que GC3 → GE sea ablación válida

Las 19 deben cumplirse simultáneamente. Cada una corrige un defecto real de v1:

| # | Elemento | Estado exigido |
|---|---|---|
| 1 | Arquitectura estructural (nº canales, capas, orden) | IDÉNTICA |
| 2 | Bloques Bahdanau Attention (nº y unidades) | IDÉNTICOS |
| 3 | Unidades LSTM por canal | IDÉNTICAS |
| 4 | Capas Dense (fusión, tamaños, activaciones) | IDÉNTICAS |
| 5 | Regularización (L2, dropout y tasas) | IDÉNTICA |
| 6 | Optimizer | IDÉNTICO |
| 7 | Learning rate (y schedule) | IDÉNTICO |
| 8 | Batch size | IDÉNTICO |
| 9 | Callbacks (métrica, patience, factor, min_lr, restore_best) | IDÉNTICOS |
| 10 | Lookback (6) | IDÉNTICO |
| 11 | Train (84 secuencias) | IDÉNTICO |
| 12 | Validation (12, 2024) | IDÉNTICO |
| 13 | Test (12, 2025-01..2025-12) | IDÉNTICO |
| 14 | Scaler de features comunes (`scaler_{c}_v2c`, sin re-fit) | IDÉNTICO |
| 15 | Semillas (mismo conjunto, mismo nº S) | IDÉNTICAS |
| 16 | Entorno (máquina, SO, versiones, flags de determinismo) | IDÉNTICO |
| 17 | Protocolo rolling one-step-ahead | IDÉNTICO |
| 18 | Features no-NLP (36) | IDÉNTICAS |
| 19 | **Única diferencia permitida** | Canal B: **32 → 38** columnas (entran `avg_sentiment_lag{1,3,6}`, `n_noticias_lag{1,3,6}`) |

**Asimetría de parámetros — formulación obligatoria de la conclusión.**
GE tendrá **+1 536 parámetros** respecto de GC3. Esa diferencia es exactamente
`4 × 6 × 64` = los pesos de entrada de las 6 columnas nuevas en el LSTM del
Canal B; **nada más cambia**. No existe forma de añadir una modalidad sin
parametrizarla. Redacción correcta:

> «Manteniendo constantes arquitectura, hiperparámetros, particiones, escalador,
> protocolo, semillas y entorno, la **incorporación de la modalidad de noticias
> rezagada** al canal exógeno (6 columnas, +1 536 parámetros, ≈ +2.3 %) modifica
> el error de test en X (media ± sd sobre S semillas, n_test = 12).»

**Redacción PROHIBIDA:** «el sentimiento de las noticias mejora el pronóstico en X %».

**Opción de refuerzo NO adoptada (decisión del investigador):** un control
**GE-placebo** con las 6 columnas NLP **permutadas en el tiempo** — mismo recuento
de parámetros, señal destruida. Separaría «capacidad añadida» de «señal añadida».
Coste: 2 cultivares × S semillas.

### 4.8 · Consecuencia para D26 (módulos NLP M1–M4)

Los módulos M1–M4 de GM_v2/GM_v3 (`nlp_index`, `nlp_index_lag1`, dropout 0.5 en
rama NLP, PCA 95 %) **quedan EXCLUIDOS del GE principal**: violarían las
condiciones 1, 5 y 18. **D26 queda resuelta en negativo.**

---

## 5. SPLIT Y PROTOCOLO TEMPORAL

### 5.1 · Particiones (CONGELADAS — no modificar)

| Concepto | Valor |
|---|---|
| Serie cruda | 2016-01 … 2025-12 = **120 meses** |
| **Train original** | 2016-01 … 2023-12 = **96 meses** |
| **Validation** | 2024-01 … 2024-12 = **12 meses** |
| **Test final** | 2025-01 … 2025-12 = **12 meses** |
| **2026** | **EXCLUIDO** (año en curso, cobertura geográfica incompleta) |
| Serie utilizable tras `dropna` de lag6 | 2016-07 … 2025-12 = **114 meses** |
| **Train multivariado efectivo** | 2016-07 … 2023-12 = **90 filas** |
| **Secuencias con lookback = 6** | **84 train / 12 val / 12 test = 108** |

### 5.2 · Las DOS ventanas TRAIN (no es un error — C-6)

| Ventana | Rango | n | Uso |
|---|---|---:|---|
| **TRAIN PREPROCESSING ESPACIAL** | 2016-01 … 2023-12 | **96** | Estimar los pesos espaciales estructurales históricos (D15) |
| **TRAIN MODELABLE** | 2016-07 … 2023-12 | **90** | Ajuste del scaler y de todos los modelos |

Esto explica que la media de train de la sección de tendencia (23 554 t) no
coincida con la del scaler (23 431.9954 t): son ventanas distintas, no un fallo.

### 5.3 · Construcción de secuencias (regla obligatoria)

> Las secuencias se construyen **sobre el array temporal completo de 114 filas**,
> y **después** se asignan a partición **según la fecha del TARGET**, nunca según
> la fecha del contexto.

```
fila   0 ..  89   2016-07 .. 2023-12   TRAIN
fila  90 .. 101   2024-01 .. 2024-12   VALIDATION
fila 102 .. 113   2025-01 .. 2025-12   TEST

objetivo válido si j >= 6  ->  j = 6..113  ->  108 secuencias
  TRAIN  j =   6.. 89  ->  84   (pierde 6: los objetivos 2016-07..2016-12
                                 exigirían filas anteriores al dropna de lag6)
  VAL    j =  90..101  ->  12   (no pierde ninguna)
  TEST   j = 102..113  ->  12   (no pierde ninguna)
```

**Ejemplos obligatorios que DEBEN cumplirse:**

- Objetivo **2024-01** (fila 90) → contexto filas 84–89 = **2023-07 … 2023-12** (filas de TRAIN).
- Objetivo **2025-01** (fila 102) → contexto filas 96–101 = **2024-07 … 2024-12** (filas de VALIDATION).

En ambos casos el contexto son observaciones **estrictamente pasadas** respecto
del mes objetivo → **no hay fuga**. **Enero–diciembre 2025 se evalúa completo
(12 meses), sin pérdida por el lookback y sin ningún relleno artificial (padding).**

> **ERROR A NO REPETIR:** en v1 las secuencias de test se construían *dentro* de
> la partición de test, lo que consumía sus 6 primeros meses. En GM_v2 y GM_v3
> eso redujo el test evaluado de 12 a **6** meses.

### 5.4 · Regla temporal (D6 — CERRADA)

```
información disponible hasta el cierre de t   →   predecir y[t+1]
```

- **FORECAST ROLLING ONE-STEP-AHEAD.** No es nowcasting.
- **Ninguna exógena del mes objetivo entra en la matriz predictiva.** Todas
  aparecen solo como `lag1` / `lag3` / `lag6`.
- D5: el protocolo recursivo queda reservado como **análisis secundario**.

### 5.5 · Aislamiento del TEST 2025 — regla dura

> **El test 2025 debe quedar TOTALMENTE fuera de:**
> selección de features · selección de hiperparámetros · arquitectura ·
> elección de semillas · definición de métricas · tuning · elección del
> denominador de escalado · elección del benchmark.

Cualquier selección se hace **solo con TRAIN**, o con **TRAIN + VALIDATION**
cuando corresponda metodológicamente, y el criterio se declara **por escrito
antes** de ejecutar.

**Precedente que obliga a esta regla:** el hallazgo `exp_002` vs `exp_002b`
demostró en este mismo proyecto que la selección automática por AIC produce
conclusiones sustantivas erróneas (el mejor AIC dio el peor test, R² = −1.11).
Limitación ya CERRADA: *en GC2/GC3/GE la selección debe evaluarse en validation
con métrica de error, no por criterios de información en train*.

### 5.6 · Comparabilidad temporal de GC1 (asunto abierto — «E2»)

**Estado actual:** GC1 está evaluado **fixed-origin, a horizonte 13–24 meses**
(consecuencia del fix de `bfd19a7`: un único `forecast(steps=24)` desde el fin de
train; val = pasos 1–12, test = pasos 13–24). Los modelos multivariados irán en
**rolling one-step-ahead**. `DECISIONES_METODOLOGICAS.md` ya declara por escrito
que **no son directamente comparables en MAE_test**.

**Propuesta E2 (recomendada, NO ejecutada):** conservar intacto el GC1
fixed-origin y **añadir** una evaluación rolling one-step-ahead de SARIMA/Prophet
sobre exactamente los 12 targets de 2025, **sin volver a seleccionar modelo ni
hiperparámetros**:

- *SARIMA*: parámetros congelados, `append(..., refit=False)` mes a mes, `forecast(steps=1)`.
- *Prophet*: no tiene API de estado incremental → **re-ajuste mensual con
  hiperparámetros congelados**; cada ajuste del mes *m* usa solo datos ≤ *m−1*.
- **Asimetría a resolver:** para simetría, ambos deberían ir por re-fit mensual.
- **Nomenclatura obligatoria:** experimentos NUEVOS (`exp_002c_*_rolling`,
  `exp_003b_*_rolling`). `exp_001`, `exp_002`, `exp_002b`, `exp_003` **INTACTOS**.
- Coste: minutos. No requiere GPU ni reentrenamiento neuronal.

**El Naive NO necesita trabajo adicional.** Verificado: `ŷ[t] == y[t−1]` en toda
la serie de ambos cultivares, incluido `2025-01 ← 2024-12` (26 555.14). Ya es
rolling one-step-ahead puro sobre los 12 targets correctos.

---

## 6. ESTADO INDECI — DECISIONES CERRADAS CONCEPTUALMENTE

Auditoría completa realizada el 2026-09-08. **Ningún dato fue modificado.**

### 6.0 · Flujo auditado

```
BD_2003-2025_EMERGENCIAS.csv        142 139 × 49   sep=';'  latin-1   snapshot 2026-08-30
   fila = 1 emergencia × 1 DISTRITO
   ID   = CODIGO DE EMERGENCIA-SINPAD
   fecha = SOLO ocurrencia (FECHA DE LA EMER)
        │  filtro AÑO 2016-2025             -> 85 421 filas
        │  filtro PELIGRO en 10 climáticos  -> 65 129 filas
        │  LIMA METROPOLITANA/PROVINCIAS -> LIMA ; MES texto -> entero
        ▼
   GROUP BY (anio, mes, departamento, provincia)
        num_emergencias = COUNT(*)   ·   resto = SUM(...)         <- EXTENSIVO, correcto
        total_afectados = afectados + damnificados                <- DERIVADA (excluida, D20-d)
        ▼
indeci_limpio_2016_2025.csv         12 840 × 10 = 107 prov × 120 meses, balanceado, 0 dup
        │  AgregadorEspacial.agregar(pesos = w_cultivar de verde_actual_ha, TRAIN 2016-01..2023-12)
        │      X_cult,t = SUM_p (w_p * X_p,t) / SUM_p w_p
        │      SUM w = 1.000000 (Sutil) · 0.978881 (Dulce)
        ▼
master_dataset_{c}_v2.csv           120 × 16   (6 columnas INDECI, round 4)
        │  dropna lag6 -> 114 filas ; lags {1,3,6} sobre la serie NACIONAL ya agregada
        ▼
master_dataset_{c}_v2_features.csv  114 × 48   (18 columnas INDECI_lag)
        │  StandardScaler fit TRAIN(90)  <- neutraliza toda constante multiplicativa
        ▼
master_dataset_{c}_v2_escalado.csv   114 × 48  ->  15 features INDECI tras D20-d
```

### 6.1 · D29 — Operador de agregación espacial para variables extensivas

**APROBADA.** Conservar la **ponderación espacial por área histórica TRAIN-only
del cultivar**.

- **NO interpretar** las variables INDECI ponderadas como **totales físicos**.
- **Describirlas como:** *indicadores/proxies de **exposición territorial** a
  emergencias, ponderados por la distribución histórica del cultivar.*

**Evidencia que la sustenta.** El operador `X_cult,t = SUM_p (w_p·X_p,t)/SUM_p w_p`
es una **media ponderada**: correcto para variables **intensivas** (NASA: T2M,
RH2M…), dimensionalmente impreciso para variables **extensivas** (conteos,
personas, hectáreas). Sin embargo:

1. **El z-score de Fase 2 neutraliza las unidades.** Cualquier alternativa que
   sea un reescalado constante produce un **input numéricamente idéntico**.
2. Verificado analítica y numéricamente: la «intensidad normalizada»
   (`SUM X_p / SUM a_p`) es **matemáticamente idéntica a la suma** tras z-score,
   porque `w_p` es proporcional a `a_p`. Solo existen **DOS** series distinguibles:
   **ponderación por ÁREA** (actual) y **ponderación UNIFORME** (suma).
3. Y difieren de forma **material**:

| Variable | r(z) ACTUAL vs SUMA — Sutil | — Dulce |
|---|---:|---:|
| `num_emergencias` | 0.7971 | 0.8935 |
| `personas_afectadas` | 0.8156 | 0.8183 |
| `personas_damnificadas` | 0.8488 | 0.9134 |
| `hectareas_cultivo_perdidas` | **0.5694** | 0.7864 |
| `hectareas_cultivo_afectadas` | **0.2796** | 0.7826 |

Causa de la divergencia: concentración del peso (PIURA = **38.92 %** en Sutil,
top-5 = 73.19 %). La media ponderada es casi «lo que pasó en Piura»; la suma
recoge el total nacional.

**Por qué se conserva la ponderación por área:** es la más específica del
cultivar, es consecuencia directa de D14–D17 (ya aprobadas por razones
independientes: eliminación del look-ahead y especificidad por cultivar), y
cambiar a suma uniforme sería **revertir** esas decisiones solo para INDECI y no
para NASA. **Efecto sobre el modelo: CERO.** La corrección es **documental**.

**Nota adicional:** `hectareas_cultivo_perdidas/afectadas` son hectáreas de
**cualquier cultivo**, no de limón. Debe declararse.

### 6.2 · D30 — Disponibilidad temporal (antes D24)

**APROBADA.** INDECI es un **snapshot consolidado ex-post**.

| Semántica temporal | ¿Existe en el raw? |
|---|---|
| Fecha de **ocurrencia** | ✅ `FECHA DE LA EMER` (+ `AÑO`, `MES`) |
| Fecha de **registro** | ❌ **NO EXISTE** |
| Fecha de **actualización** | ❌ **NO EXISTE** |
| Fecha de **publicación** | ❌ **NO EXISTE** |

- **Mantener lags 1/3/6.** No se introduce lag adicional (sería un margen
  arbitrario sin evidencia que lo calibre).
- **Declarar explícitamente** que **no puede garantizarse disponibilidad
  operacional histórica exacta**. La evaluación se realiza bajo **información
  consolidada**; **no constituye simulación de despliegue operativo**.
- Evidencia indirecta de consolidación retrospectiva: los conteos anuales de
  registros climáticos en provincias limoneras crecen de 2016 = 2 200 a
  2025 = 7 813, atribuido a mejora del reporte, no a más desastres reales.

### 6.3 · D31 — INDECI SÍ es reproducible

**APROBADA.** La reconstrucción raw → provincia/mes fue **auditada
satisfactoriamente**:

| Variable | Coincidencia | max abs dif |
|---|---:|---:|
| `num_emergencias` | **12 840 / 12 840 = 100.00 %** | 0.00 |
| `personas_afectadas` | **12 840 / 12 840 = 100.00 %** | 0.00 |
| `personas_damnificadas` | **12 840 / 12 840 = 100.00 %** | 0.00 |
| `hectareas_cultivo_perdidas` | 12 780 / 12 840 = 99.53 % | 0.01 *(redondeo)* |
| `hectareas_cultivo_afectadas` | 12 796 / 12 840 = 99.66 % | 0.01 *(redondeo)* |

**Consecuencia: D21 (inputs congelados no reproducibles) queda restringido a
MIDAGRI.** INDECI sale de esa lista. *(Pendiente: versionar el script de
reconstrucción — hoy NO existe código en el repositorio que genere
`indeci_limpio_2016_2025.csv`.)*

**Confirmación adicional de D20-d en origen:** en el archivo oficial,
`total_afectados − (personas_afectadas + personas_damnificadas)` = **0.0 EXACTO**.

### 6.4 · D32 — NO incorporar nuevas variables INDECI a v2

**APROBADA.** Existen en el raw pero **no se incorporan**:

| Columna raw disponible y NO usada | Suma 2016-2025 en provincias limoneras |
|---|---:|
| `VIVIENDAS AFECTADAS` | 951 078 |
| `VIVIENDAS DESTRUIDAS` | 173 631 |
| `PERDIDA VACUNO` | 95 353 |
| `CANAL DE REGADIO AFECTADO` | 13 352.4 |
| `LESIONADOS` | 4 356 |
| `CANAL DE REGADIO COLAPSADO` | 3 617.2 |
| `FALLECIDOS` | 431 |
| `DESAPARECIDOS` | 73 |

Incorporar cualquiera **exigiría regenerar Fase 2**. Queda registrado como
trabajo futuro. *(Nota: `CANAL DE REGADIO AFECTADO/COLAPSADO` es,
agronómicamente, la más directamente ligada a la producción bajo riego.)*

### 6.5 · D33 — Filtro de peligros

**APROBADA.** Mantener el filtro actual de **10 categorías climáticas** sobre las
22 existentes: `LLUVIA INTENSA` · `VIENTOS FUERTES` · `BAJAS TEMPERATURAS` ·
`INUNDACION` · `DESLIZAMIENTO` · `HUAYCO` · `SEQUIA` · `EROSION` ·
`TORMENTA ELECTRICA` · `MAREJADA`.

**No reabrir `DERRUMBE DE CERRO` (1 612 registros) ni `ALUD` (46) salvo evidencia
metodológica nueva.** Queda registrada la observación de que son movimientos en
masa frecuentemente desencadenados por lluvia, de la misma familia que
`DESLIZAMIENTO` y `HUAYCO`, que sí se incluyen.

### 6.6 · D34 — Definición de `num_emergencias`

**APROBADA.** Mantener `COUNT(*)`.

Evidencia (2016-2025): **85 421 filas / 85 414 IDs SINPAD únicos**; solo **7**
IDs repetidos (**0.01 %**), y los 7 son **reutilización de código entre años
distintos**, no actualizaciones del mismo evento. Ejemplo:

```
ID 97558 | 22/12/2018 | HUANCAVELICA/CONAYCA | SEQUIA                  | 0 afectados
ID 97558 | 13/01/2019 | LIMA/RIMAC           | INCENDIO URB. E INDUST. | 6 afectados
```

Documentar que `COUNT(DISTINCT CODIGO-SINPAD)` cambiaría ≈ **0.01 %** y **no
justifica regeneración**. El riesgo de duplicación por actualizaciones queda
**descartado empíricamente**.

### 6.7 · CONFIRMACIÓN

> ✅ **NO regenerar Fase 2 debido a estas decisiones.** Impacto sobre Fase 2:
> **categoría B — solo cambia la selección de columnas**. Los 9 hashes de
> artefactos regenerados siguen **VÁLIDOS**.

### 6.8 · Discrepancia documental menor (NO bloqueante)

`DECISIONES_METODOLOGICAS.md` documenta conteos anuales INDECI
(2016 = 2 862 … 2025 = 9 319) que **no reproducen** desde el archivo congelado.
Los valores en disco son: **todos los peligros** 3 069 … 9 838; **solo climáticos**
2 200 … 7 813. Los documentados caen **entre ambos** y no coinciden con ninguno.
Probable diferencia de conjunto de provincias o de normalización de LIMA.
**Deuda de trazabilidad, no bug.**

### 6.9 · NASA — NO TOCADO

Revisado únicamente para descartar un bug de implementación: **no lo hay**.
`AgregadorEspacial` aplica media ponderada, **el operador correcto para variables
intensivas**. Cobertura de masa 100.0000 % en ambos cultivares, panel balanceado.
**Decisión NASA CERRADA, no reabierta.**

---

## 7. SHOCKS — ESTADO ACTUAL

### 7.1 · Definición vigente

```
mes i es SHOCK  <=>  | delta_y_i / y_(i-1) |  >  P75
```

- El umbral P75 se calcula **siempre sobre la serie real en TONELADAS, nunca en z-score**.
- Umbrales congelados: **Sutil 23.2 %** · **Dulce 33.9 %**.
- Se evalúa **solo sobre el conjunto de test**.

### 7.2 · Meses de shock en test 2025 (verificados)

| Cultivar | Meses de shock | n_shock |
|---|---|---:|
| **Sutil** | 2025-01, 2025-07, 2025-11 | **3** |
| **Dulce** | 2025-01, 2025-02, 2025-03 | **3** |

**Propiedad clave:** los meses de shock dependen **solo de la serie real**, no del
modelo → son **idénticos para todos los modelos** dentro de un cultivar. La
definición es aplicable idénticamente a BASELINE, GC1 rolling, GC2, GC3 y GE
sobre los mismos 12 targets.

### 7.3 · Índice Δs

```
Delta_s = (MAE_shock − MAE_global) / MAE_global * 100
```

### 7.4 · ⚠️ ADVERTENCIA OBLIGATORIA SOBRE Δs

> **Δs todavía NO debe presentarse como una métrica estándar validada de
> resiliencia. Debe considerarse PROVISIONALMENTE un indicador DESCRIPTIVO de
> deterioro condicionado.**

Con **n_shock = 3** por cultivar:

- **NO afirmar resiliencia estadísticamente demostrada.**
- **Lenguaje obligatorio:** *«menor deterioro observado en los episodios
  clasificados como shock»*.
- **Δs debe reportarse SIEMPRE acompañado de `n_shock`.**
- **Redacción PROHIBIDA:** *«el modelo mejora 79 % ante shocks»*.
- **Redacción ACEPTABLE:** *«Δs = −79.25 % sobre n_shock = 3, dominado por un
  único mes; no interpretable como resiliencia»*.

**Evidencia que obliga a esta advertencia** (ya documentada): en `exp_002b`,
Δs = −79.25 % con errores absolutos de 14.1 / 822.6 / 1428.3 t en los tres meses
de shock, mientras los tres mayores errores del test (7170.8 / 7094.3 / 6153.3 t)
caen **fuera** de meses de shock. El promedio lo domina 2025-01, con un error de
14.1 t sobre 38 325 t reales (**0.04 %**) — una coincidencia numérica, no
capacidad predictiva. Excluyendo ese único mes, Δs pasa a ≈ −69 %.
Contraste confirmatorio: `exp_002` (modelo descartado, uniformemente malo)
reporta Δs = +0.29 %.

Con 12 meses de test y umbral P75, **n_shock ≈ 3 es el máximo estructuralmente
posible en este diseño**. Δs **no admite contraste de hipótesis** con esta
muestra. Una evaluación robusta requeriría *walk-forward validation* sobre
múltiples ventanas de test — registrado como trabajo pendiente, no ejecutado.

### 7.5 · Requisito operativo para el cálculo

> **Todas las métricas condicionadas a shock deben calcularse en TONELADAS.**
> BASELINE y GC1 ya lo están. GC2/GC3/GE trabajan en z-score → **desnormalizar con
> `scaler_{c}_v2c` (`target_mean`, `target_scale`) antes** de calcular MAE,
> MAE_shock y Δs. Es el bug de desnormalización ya corregido en el commit
> `4147616`; **no debe reaparecer**.

Parámetros del target: **Sutil** `mean_train = 23431.9954`, `scale_train = 8041.5165`
· **Dulce** `mean_train = 391.6001`, `scale_train = 144.2033`.

---

## 8. REVISIÓN BIBLIOGRÁFICA DE MÉTRICAS REALIZADA

> **Nota de trazabilidad:** las referencias siguientes y su lectura fueron
> aportadas por el investigador y su supervisor metodológico. **NO fueron
> verificadas de forma independiente por Claude Code en esta sesión**, y **no
> existen copias ni notas de estas referencias en el repositorio** (NO VERIFICADO
> en repo). Se registran fielmente como base de la decisión D35.

### 8.1 · Hyndman & Koehler — «Another look at measures of forecast accuracy»

Uso:
- fundamento de **MASE**;
- **scaled errors**;
- comparación frente a **Naive**;
- problemas de **MAPE / sMAPE**;
- definición conceptual de **RMSSE**.

### 8.2 · Kim & Kim — «A new metric of absolute percentage error for intermittent demand forecasts»

Uso:
- problemas de **MAPE**;
- **MAAPE**;
- **advertencia:** MAAPE **reduce la influencia de errores extremos**, por lo cual
  **NO parece ideal como métrica principal** cuando los extremos son **eventos
  reales relevantes** — que es exactamente el caso de esta tesis (shocks climáticos).

### 8.3 · Koutsandreas et al. (2022) — «On the selection of forecasting accuracy measures»

Uso:
- evidencia empírica **a gran escala**;
- selección de métricas;
- **RelMAE** y **RelRMSE**;
- **sMAE / sRMSE** existen, pero **pueden ser problemáticos con no estacionariedad**
  — relevante aquí: la serie de Sutil tiene tendencia secular +4.01 %/año y salto
  de nivel train→test de +35.5 %;
- **no existe una métrica universal perfecta**;
- **diferentes métricas pueden representar objetivos distintos**.

### 8.4 · Makridakis et al. / M5 Accuracy Competition

Uso:
- evidencia aplicada **masiva** en forecasting de demanda;
- **RMSSE**;
- **scaled errors**;
- **denominador basado en el error Naive in-sample**;
- **RMSSE como medida principal en M5**.

### 8.5 · Petropoulos et al. (2022) — «Forecasting: theory and practice»

Uso:
- referencia metodológica moderna;
- evaluación **out-of-sample**;
- **validación temporal**;
- **problemas especiales cuando hay pocos datos** — directamente aplicable al
  régimen small-data de esta tesis (84 secuencias de train, 12 de test).

### 8.6 · Pasche et al. (2025) — «Validating Deep Learning Weather Forecast Models on Recent High-Impact Extreme Events»

Uso:
- **evidencia fuerte de que buenos scores globales pueden ocultar errores en
  eventos extremos**;
- **justificación para evaluación complementaria específica bajo shocks** — es el
  respaldo bibliográfico de la dimensión **B** de §3;
- *case-study / impact-centric evaluation*.

### 8.7 · Nikraftar et al. (2024) — «Impact-Based Skill Evaluation of Seasonal Precipitation Forecasts»

Uso:
- fundamento conceptual de la **evaluación orientada a impactos/extremos**;
- **NO copiar directamente sus métricas**, porque su objetivo es **climatológico**,
  no producción agroindustrial mensual.

---

## 9. PANEL PROVISIONAL DE MÉTRICAS

> ⚠️ **NO implementar todavía como decisión final. Es un CANDIDATO.**
> La decisión formal es **D35** (§10 y §11).

### 9.1 · Panel candidato

| Rol | Métrica | Nota |
|---|---|---|
| **Primaria global** | **MAE** | En toneladas |
| **Complementaria global** | **RMSE** | Penaliza errores grandes; relevante ante shocks |
| **Escalada** | **MASE** | Denominador pendiente — ver §10 |
| **Escalada cuadrática** | **RMSSE** | Denominador pendiente — ver §10 |
| **Secundaria / descriptiva** | **R²** | Solo descriptivo |
| **Condicionada** | **MAE_shock**, **MAE_no_shock** | Obligatorias, siempre con `n_shock` |
| **Condicionada (posible)** | RMSE_shock, RMSE_no_shock | A evaluar |
| **Condicionada (descriptiva)** | **Δs** | Indicador de deterioro, NO de resiliencia (§7.4) |

### 9.2 · NO priorizar

- **MAPE** — problemas documentados (§8.1, §8.2); además la serie tiene valores
  bajos en Dulce que inflan el porcentaje.
- **sMAPE** — ídem.
- **MAAPE** — **reduce la influencia de los extremos**, contrario al objetivo de
  esta tesis (§8.2).
- **sMAE**, **sRMSE** — problemáticos bajo no estacionariedad (§8.3), y esta serie
  no es estacionaria.

### 9.3 · Métrica relativa registrada, NO aprobada

```
RelMAE = MAE_modelo / MAE_benchmark
```

Puede utilizarse para **comunicar mejora relativa frente al benchmark**, pero
**todavía NO está aprobada como métrica final**. Decisión dentro de D35.

### 9.4 · Requisitos transversales del panel

1. **Todas las métricas en TONELADAS** (desnormalizar con `scaler_{c}_v2c`).
2. **Los mismos 12 targets** (2025-01…2025-12) para todos los modelos.
3. **Agregación multi-semilla**: media ± sd sobre S semillas; nunca una ejecución única.
4. **Nunca comparar** el bloque rolling con el bloque fixed-origin de GC1.

---

## 10. PUNTO METODOLÓGICO PENDIENTE MÁS IMPORTANTE

> **NO cerrar D35 todavía.**

Antes hay que decidir el **denominador exacto de MASE / RMSSE**.

### 10.1 · Las dos alternativas

| | **A · Naive no estacional** | **B · Seasonal Naive** |
|---|---|---|
| Fórmula del error de referencia | `y[t] − y[t−1]` | `y[t] − y[t−12]` |
| Qué considera «trivial» | Repetir el mes anterior | Repetir el mismo mes del año anterior |
| Coherencia con el BASELINE del diseño | **Alta** — el BASELINE de §4 es exactamente `ŷ[t]=y[t−1]` | Media — introduce un segundo benchmark implícito distinto del BASELINE reportado |
| Sensibilidad a la estacionalidad | Baja: si la serie es fuertemente estacional, el denominador es grande y MASE parece optimista | Alta: descuenta la estacionalidad, exige al modelo batir el ciclo anual |
| Sensibilidad a la tendencia | Alta | El salto de nivel +35.5 % train→test infla el error estacional |

### 10.2 · Qué hay que determinar con evidencia metodológica

Las series son **mensuales y agroindustriales** y **pueden mostrar estacionalidad**
(la producción de limón tiene ciclo de cosecha marcado; los rezagos elegidos
[1, 3, 6] se alinearon a la periodicidad trimestral/semestral). Hay que decidir
cuál de los dos debe usarse como **escalador**, con argumento metodológico —
no con resultados.

### 10.3 · REGLAS DURAS

- El denominador debe calcularse **SOLO con TRAIN**.
- **Nunca** usar validation ni test para construir el denominador.
- **NO elegir el benchmark que produzca mejores resultados para GE.**
- La decisión debe ser **metodológica** y quedar **fijada ANTES del entrenamiento final**.

### 10.4 · Consideraciones adicionales a resolver dentro de D35

- Con **Seasonal Naive** y TRAIN = 90 filas (2016-07…2023-12), el denominador usa
  `n − 12 = 78` diferencias. Con **Naive**, usa `n − 1 = 89`. Ambos son viables.
- Decidir si el denominador es **único por cultivar** (recomendable) o por partición.
- Decidir si MASE y RMSSE comparten denominador o cada uno usa el suyo
  (MAE del error de referencia vs RMSE del error de referencia).

> **D35 = protocolo definitivo de métricas.** Será probablemente la última
> investigación antes de poder entrenar.

---

## 11. DECISIONES TODAVÍA ABIERTAS

### 11.1 · Bloque de métricas

| ID | Decisión | Estado |
|---|---|---|
| **D35** | **Panel final de métricas y fórmulas exactas** | **ABIERTA — bloqueante prioritaria** |
| D35-a | Benchmark de escalado MASE/RMSSE: **Naive t−1 vs Seasonal Naive t−12** | ABIERTA — subordinada a D35 |
| D35-b | ¿RMSSE aporta suficiente valor adicional a RMSE + MASE? | ABIERTA — subordinada a D35 |
| D35-c | ¿RelMAE se reportará como métrica secundaria/comunicacional? | ABIERTA — subordinada a D35 |
| D35-d | Definición final de la evaluación shock / no-shock (¿RMSE condicionado?) | ABIERTA — subordinada a D35 |
| D35-e | **Destino final de Δs** (¿se mantiene? ¿solo como descriptor?) | ABIERTA — subordinada a D35 |

### 11.2 · Bloque de entrenamiento

| ID | Decisión | Estado |
|---|---|---|
| **D0** | **Determinismo**: `TF_ENABLE_ONEDNN_OPTS=0` + `TF_DETERMINISTIC_OPS=1` vs default; `PYTHONHASHSEED`; semillas de Python/NumPy/TF | **ABIERTA — bloqueante ANTES del primer `fit`** |
| **D10** | **Número de semillas S** | ABIERTA — reservada explícitamente por el investigador |
| **D4** | **Hiperparámetros** de LSTM y callbacks (unidades, dropout, L2, lr, batch, epochs, patience, factor, min_lr) | ABIERTA — bloqueante para ejecutar |
| D4-a | **Early stopping**: métrica, patience, `restore_best_weights` | ABIERTA — parte de D4 |
| D4-b | **Batch size** | ABIERTA — parte de D4 |
| D4-c | **Loss de las redes** (MSE estándar; ¿alguna ponderación? — recordar que la shock-weighted loss de GM_v4 quedó fuera) | ABIERTA — parte de D4 |
| **D7** | **`shuffle`** en `model.fit`: `True` (GC2/GE v1) o `False` (GM_v2/v3 v1) | ABIERTA — debe ser idéntico en GC3 y GE |
| **D3-bis** | Con **12 secuencias** de validation, `val_loss` será muy ruidosa para el early stopping. ¿Se acepta o se añade criterio/tolerancia? | ABIERTA |
| **D3-op** | **Estrategia exacta de uso de validation 2024** (early stopping, selección de modelo, ambas) | ABIERTA |
| **D8** | **Agregación de resultados entre semillas** y esquema de `REGISTRO_MAESTRO.csv` (¿media? ¿`_std`? ¿`_n_seeds`? ¿rutas `seeds/seed_N/`?) + campo de métricas en toneladas | ABIERTA |
| **D2.3'** | **Protocolo rolling one-step final**: parámetros congelados + estado actualizado (A) vs re-fit mensual (B) | ABIERTA |
| **E2** | Añadir GC1 rolling (SARIMA + Prophet) sobre 2025-01..2025-12 | ABIERTA — recomendada |

### 11.3 · Bloque de diseño

| ID | Decisión | Estado |
|---|---|---|
| **D25** | **Formalización final de la taxonomía** en `DECISIONES_METODOLOGICAS.md` | ABIERTA — acordada conceptualmente (§4), **NO escrita** |
| D25-a | ¿Se adopta el control **GE-placebo** (NLP permutado)? | ABIERTA — opcional |
| D25-b | ¿El índice `GC_k` se declara identificador, no ordinal? | ABIERTA — recomendado |

### 11.4 · Deuda documental (NO bloqueante)

| ID | Punto |
|---|---|
| **D20-d** | `total_afectados` eliminada — aprobada, **pendiente de documentar** |
| **D29–D34** | Aprobadas en esta sesión (§6), **pendientes de documentar** |
| **C-1** | `verde_actual_ha` **supersedida** como predictor — aprobada, pendiente de documentar |
| **C-6** | Las dos ventanas TRAIN (96 vs 90) — pendiente de documentar |
| **C-3** | Marcar como SUPERSEDIDA la sección antigua de `t_index` en `DECISIONES_METODOLOGICAS.md` (aún nombra los scalers `v2b_tindex`, hoy obsoletos) |
| **C-4** | Corregir la procedencia registrada de GC1/Naive en `REGISTRO_MAESTRO.csv` (dice `120x18`, el archivo es `120x16`) **sin reentrenarlos ni alterar sus métricas** |
| **C-5** | Inconsistencia `anio` (metadata) vs `año` (CSV) en `fase2_v2_regenerada_meta.json` |
| **§8.4** | Corregir la afirmación «el peldaño GC3 nunca existió» en `CONTEXTO_SESION_2026-09-07.md` (ver §16) |
| **§6.8** | Conteos anuales INDECI documentados que no reproducen |
| **D11** | Destino final de los 4 scalers obsoletos |
| **D24'** | Rezagos reales de publicación de MIDAGRI y NASA POWER: siguen **NO VERIFICADOS** |
| **D27** | Declarar en el PPI el cambio de NLP contemporáneo (v1) a NLP rezagado (v2) |
| **D31'** | Versionar el script de reconstrucción raw → provincia de INDECI |

### 11.5 · Backlog para el PPI (aplicar DESPUÉS de experimentar)

Periodo temporal · split 90/12/12 y 84/12/12 · dos ventanas TRAIN · protocolo
rolling one-step · definición de shocks y prohibición de leer Δs como resiliencia ·
semillas · variables eliminadas (`n_provincias`, `precio_chacra_kg`,
`total_afectados`, `verde_actual_ha` como predictor) · ponderación espacial
crop-aware · NLP rezagado (cambia la hipótesis sustantiva) · conjunto exógeno
36/42 · grupos de comparación y nomenclatura control/experimental + tabla de
equivalencias v1↔v2 · arquitectura final · resultados v1 escritos prematuramente
(están en z-score, con splits y protocolos distintos: **no comparables con v2**) ·
hipótesis redactadas como confirmadas antes del experimento · retirar la
afirmación «el NLP aporta» sostenida en v1 sobre una ablación inválida ·
explicabilidad SHAP · simulación económica (el precio ya solo puede ser supuesto,
no predictor) · convención SHA-256 e inputs congelados.

---

## 12. PRÓXIMA TAREA PARA CLAUDE EN NUEVA SESIÓN

> ### ⛔ NO EMPEZAR ENTRENANDO.

### Primero: **AUDITORÍA PRE-ENTRENAMIENTO SIN CAMBIOS**

Revisar y reportar los 23 puntos siguientes. **Donde no exista implementación
todavía, decirlo explícitamente** («NO EXISTE») en lugar de describir lo que
debería haber.

| # | Punto a auditar |
|---|---|
| 1 | Implementación existente/propuesta de **XGBoost** (GC2) |
| 2 | Implementación de **DualLSTM-Attention SIN NLP** (GC3) |
| 3 | Implementación de **GE con NLP** |
| 4 | **Diferencias exactas entre GC3 y GE** (deben ser SOLO las 6 columnas NLP) |
| 5 | **Número de features por modelo** |
| 6 | **Número de parámetros** por modelo |
| 7 | **Lookback** |
| 8 | **Loss** |
| 9 | **Optimizer** |
| 10 | **Learning rate** |
| 11 | **Batch size** |
| 12 | **Early stopping** |
| 13 | **Callbacks** |
| 14 | **`shuffle`** |
| 15 | **Semillas** |
| 16 | **Determinismo** TensorFlow / Python / NumPy |
| 17 | Cómo se usa **validation 2024** |
| 18 | Cómo se **preserva el contexto temporal** al pasar train → val → test |
| 19 | **Rolling one-step** |
| 20 | **Evitar leakage** |
| 21 | **Cálculo de métricas** |
| 22 | **Agregación multi-semilla** |
| 23 | **Confirmar que el test 2025 NO participa en ninguna selección** |

### Estado de partida conocido (para no perder tiempo)

- `v2_reentrenamiento/src/models/arquitecturas/` → **solo `.gitkeep`**
- `v2_reentrenamiento/src/models/entrenamiento/` → **solo `.gitkeep`**
- `v2_reentrenamiento/src/evaluation/` → **solo `.gitkeep`**
- `v2_reentrenamiento/notebooks/fase3_modelado/` → solo `01_baseline_naive*`,
  `02_gc1_sarima_prophet*`, `02b_diagnostico_sarima_sutil.ipynb`
- **GC2 (XGBoost), GC3 y GE NO EXISTEN todavía en v2.**

### Archivos que se crearían al implementar (nada de esto existe aún)

```
v2_reentrenamiento/src/models/arquitecturas/dual_lstm_attention.py
v2_reentrenamiento/src/models/arquitecturas/xgboost_model.py
v2_reentrenamiento/src/models/entrenamiento/secuenciador.py
v2_reentrenamiento/src/models/entrenamiento/protocolos.py
v2_reentrenamiento/src/models/entrenamiento/callbacks.py
v2_reentrenamiento/src/models/entrenamiento/runner.py
v2_reentrenamiento/src/evaluation/metricas_v2.py
v2_reentrenamiento/notebooks/fase3_modelado/03_gc2_xgboost.ipynb
v2_reentrenamiento/notebooks/fase3_modelado/04_gc3_dual_lstm.ipynb
v2_reentrenamiento/notebooks/fase3_modelado/05_ge_dual_lstm_nlp.ipynb
v2_reentrenamiento/experimentos/exp_00X_{gc2,gc3,ge}_{sutil,dulce}/...
```

### Después: **DETENERSE y esperar aprobación.**

---

## 13. RESTRICCIONES

**NO:**

- entrenar;
- ejecutar HPO;
- probar muchas configuraciones;
- **mirar test para seleccionar**;
- regenerar Fase 2;
- añadir nuevas features;
- reincorporar `total_afectados`;
- reincorporar NLP contemporáneo;
- cambiar el target;
- editar el PPI;
- revivir SARIMAX-LSTM;
- **hacer cambios metodológicos silenciosos**.

> **Toda ambigüedad debe reportarse ANTES de implementar.**

### Lo que NO debe cambiarse (heredado y vigente)

- **El target:** `produccion_t_sutil` / `produccion_t_dulce` **en toneladas**. No se pronostica precio.
- **El split:** 90 / 12 / 12 sobre 2016-07…2025-12. 2026 fuera.
- **El protocolo:** rolling one-step-ahead (D6).
- **Los pesos espaciales:** fijos, por cultivar, de TRAIN. **El vector combinado está PROHIBIDO** (D16).
- **`n_provincias` y `precio_chacra_kg`:** no reincorporar como predictores.
- **`verde_actual_ha`:** no reincorporar como predictor (solo base estructural de los pesos).
- **`t_index`:** definición calendario `12·(año−2016)+(mes−1)−6`, rango 0..113.
- **Los inputs interim y raw:** congelados (D21, ahora restringido a MIDAGRI por D31).
- **GC1, `exp_002b` como representativo, el Naive y todo `experimentos/`:** no reinterpretar ni reentrenar.
- **Todo el proyecto v1** (`notebooks/`, `src/` raíz, `pipeline/`, `resultados/`, `dashboard/`, `docs/`).
- **La regla de Δs:** siempre acompañada de `n_shock`; nunca como resultado principal.

---

## 14. EVIDENCIA Y TRAZABILIDAD

### 14.1 · Estado exacto de git (verificado 2026-09-08)

```
branch : v2-reentrenamiento
HEAD   : bfd19a7c9de7fd5f1acbc8144429169dc3c4000e   (bfd19a7)
fecha  : 2026-09-04 08:17:01 -0500
status : 13 modificados ( M) · 4 borrados/movidos ( D) · 31 sin trackear (??)
commits en esta sesión : NINGUNO
drift desde 2026-09-07 : NINGUNO (14/14 hashes coinciden; 0 archivos con mtime de hoy)
```

### 14.2 · Hashes SHA-256 — artefactos regenerados de Fase 2 (**9/9 VERIFICADOS**)

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

### 14.3 · Hashes SHA-256 — inputs congelados (**5/5 VERIFICADOS**)

| Archivo (relativo a `v2_reentrenamiento/`) | SHA-256 |
|---|---|
| `data/interim/limon_sutil_provincia.csv` | `74566d396566122ce24780a960d77bdf797de00b0ee6d7fe5916ab8dfbe301d2` |
| `data/interim/limon_dulce_provincia.csv` | `06581dd806c5ffd2f3fd8d1286e0bd34e99955c7c8716d4009a48cc711b4d81c` |
| `data/interim/indeci_limpio_2016_2025.csv` | `75f021ad693a5ff154f659fff025a239e5b8f25e99bd4b14d4290bff13388be9` |
| `data/raw/nasa_power/por_provincia/clima_nasa_power_2016_2025.csv` | `fddd0291afd9cbe79dc87be18ad6597bce20d1d9c2d4cb7d35d94b5f9b1d867e` |
| `data/interim/noticias/sentimiento_mensual_2016_2025.csv` | `c3424fc11ef77ba9d33157ab0510aec795f8848c18ea5511b7d649e4c134cb7b` |

### 14.4 · Hash NUEVO registrado en esta sesión (raw INDECI)

| Archivo (relativo a `v2_reentrenamiento/`) | SHA-256 |
|---|---|
| `data/raw/indeci/oficial_2016_2018/BD_2003-2025_EMERGENCIAS.csv` | `a1fe63f0a7c836b8ff7dd1c87271d3bd3144f8444e41f93b7db205c85fef2687` |

*(27 040 446 bytes · 142 139 × 49 · `sep=';'` · `encoding=latin-1` · snapshot 2026-08-30.
Nota: la carpeta se llama `oficial_2016_2018` pero el contenido cubre 2003-2025 — nombre engañoso.)*

### 14.5 · Rutas de datasets v2

```
v2_reentrenamiento/data/processed/master_dataset_{sutil,dulce}_v2.csv           (120 × 16)
v2_reentrenamiento/data/processed/master_dataset_{sutil,dulce}_v2_features.csv  (114 × 48)
v2_reentrenamiento/data/processed/master_dataset_{sutil,dulce}_v2_escalado.csv  (114 × 48, 44 escaladas)
```

**Las 48 columnas de `features` / `escalado`:**

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

**Recuento de features tras D20-d** (excluir `total_afectados_lag{1,3,6}`):

| Bloque | n |
|---|---:|
| AR — lags del target | 3 |
| Calendario — `mes_sin`, `mes_cos` | 2 |
| Tendencia — `t_index` | 1 |
| NASA — 5 × lag{1,3,6} | 15 |
| INDECI — 5 × lag{1,3,6} | 15 |
| NLP — 2 × lag{1,3,6} | 6 |
| **TOTAL no-NLP (GC2, GC3)** | **36** |
| **TOTAL con NLP (GE)** | **42** |

Canales de GC3/GE: **Canal A = 4** (`produccion_t_{c}` + 3 lags) ·
**Canal B = 32** (GC3) / **38** (GE).

Parámetros analíticos (fórmula calibrada: reproduce **exactamente** los 65 249 de
GE v1 con Canal A=1, Canal B=23; los bloques Bahdanau de v1 usan Dense **sin bias**,
8 256 parámetros por bloque):

| Config | Canal A | Canal B | Parámetros |
|---|---:|---:|---:|
| GC3 | 4 | 32 | **67 809** |
| GE | 4 | 38 | **69 345** |
| **Δ (GE − GC3)** | | +6 | **+1 536 (≈ +2.3 %)** |

El delta absoluto **+1 536** es invariante a la configuración de canales elegida
(es `4 × 6 × 64`); el porcentaje relativo varía ligeramente. La configuración
final de canales se fija en **D4**.

Columnas **no escaladas**: `año`, `mes`, `mes_sin`, `mes_cos`.

### 14.6 · Rutas de scalers

**VIGENTES** (44 features, `n_samples_seen_ = 90`, ajustados solo en TRAIN 2016-07…2023-12):

```
v2_reentrenamiento/resultados_v2_final/scalers/scaler_sutil_v2c.joblib
v2_reentrenamiento/resultados_v2_final/scalers/scaler_sutil_v2c_parametros.csv
v2_reentrenamiento/resultados_v2_final/scalers/scaler_dulce_v2c.joblib
v2_reentrenamiento/resultados_v2_final/scalers/scaler_dulce_v2c_parametros.csv
```

Parámetros del target: **Sutil** `mean = 23431.9954`, `scale = 8041.5165` ·
**Dulce** `mean = 391.6001`, `scale = 144.2033`.

**OBSOLETOS — NO USAR** (movidos, no borrados):

```
v2_reentrenamiento/resultados_v2_final/scalers/obsoletos/README.md
v2_reentrenamiento/resultados_v2_final/scalers/obsoletos/scaler_{sutil,dulce}.joblib             (28 feat.)
v2_reentrenamiento/resultados_v2_final/scalers/obsoletos/scaler_{sutil,dulce}_v2b_tindex.joblib  (29 feat.)
```

### 14.7 · Pesos espaciales

```
v2_reentrenamiento/resultados_v2_final/pesos/pesos_geograficos_train.csv        (134 × 15 = 106 Sutil + 28 Dulce)
v2_reentrenamiento/resultados_v2_final/pesos/pesos_geograficos_train_meta.json
v2_reentrenamiento/resultados_v2_final/pesos/fase2_v2_regenerada_meta.json
v2_reentrenamiento/resultados_v2_final/pesos/REGISTRO_PRE_REGENERACION.json
```

Base `verde_actual_ha`, media sobre **TRAIN 2016-01…2023-12**, **vectores
independientes por cultivar** (combinado PROHIBIDO), sin exclusiones ni piso mínimo.
Concentración: **Sutil** HHI 0.1910, N_efectivo 5.24, top-1 PIURA 0.3892, top-5 0.7319 ·
**Dulce** HHI 0.1723, N_efectivo 5.80, top-1 CHANCHAMAYO 0.3311, top-5 0.7072.
Cobertura de masa: NASA 100.0000 % en ambos; INDECI 100.0000 % Sutil y **97.8881 %** Dulce
(`ANCASH|YUNGAY` 0.011264 y `ANCASH|CARHUAZ` 0.009856 sin registros INDECI).

### 14.8 · Archivos de decisiones metodológicas

```
v2_reentrenamiento/DECISIONES_METODOLOGICAS.md    625 líneas (524 en HEAD)
CONTEXTO_SESION_2026-09-07.md                     956 líneas · 51 166 bytes
CONTEXTO_SESION_2026-09-08_METRICAS.md            este archivo
v2_reentrenamiento/ENTORNO_COMPUTO.md             sin trackear, creado 2026-09-04
v2_reentrenamiento/HANDOFF_CONTEXTO_ACTUAL.md     sin trackear, creado 2026-09-04
```

### 14.9 · Notebooks y scripts que controlarán el entrenamiento

**Existentes (Fase 2 — orquestadores delgados sobre `src/`):**

```
v2_reentrenamiento/src/features/__init__.py
v2_reentrenamiento/src/features/hashing.py                clase Hasher
v2_reentrenamiento/src/features/pesos_geograficos.py      clases PesosGeograficos, VentanaTrain
v2_reentrenamiento/src/features/agregacion_espacial.py    clase AgregadorEspacial
v2_reentrenamiento/src/features/features_temporales.py    clase FeaturesTemporales
v2_reentrenamiento/src/features/escalado.py               clase EscaladorTrain

v2_reentrenamiento/notebooks/fase2_features/00_pesos_geograficos.ipynb
v2_reentrenamiento/notebooks/fase2_features/01_nlp_sentimiento.ipynb   (no tocado)
v2_reentrenamiento/notebooks/fase2_features/02_fusion_dataset_maestro.ipynb
v2_reentrenamiento/notebooks/fase2_features/03_features_temporales.ipynb
v2_reentrenamiento/notebooks/fase2_features/04_escalado.ipynb
   (orden de ejecución: 00 -> 02 -> 03 -> 04; cada uno con su `_ejecutado.ipynb`)

v2_reentrenamiento/notebooks/fase3_modelado/01_baseline_naive.ipynb
v2_reentrenamiento/notebooks/fase3_modelado/02_gc1_sarima_prophet.ipynb
v2_reentrenamiento/notebooks/fase3_modelado/02b_diagnostico_sarima_sutil.ipynb
```

**Para el entrenamiento: NO EXISTEN todavía.** Ver §12.

### 14.10 · Experimentos registrados (INTACTOS — no reinterpretar ni reentrenar)

```
v2_reentrenamiento/experimentos/REGISTRO_MAESTRO.csv
v2_reentrenamiento/experimentos/comparativas/
v2_reentrenamiento/experimentos/exp_001_naive_{sutil,dulce}/
v2_reentrenamiento/experimentos/exp_002_sarima_{sutil,dulce}/
v2_reentrenamiento/experimentos/exp_002b_sarima_sutil_simple/
v2_reentrenamiento/experimentos/exp_003_prophet_{sutil,dulce}/
```

| Experimento | Cultivar | Orden | MAE test | R² test |
|---|---|---|---:|---:|
| `exp_001_naive_sutil` | Sutil | — | 4 704.29 | 0.4746 |
| `exp_001_naive_dulce` | Dulce | — | 84.70 | 0.6686 |
| **`exp_002b_sarima_sutil_simple`** ⭐ | Sutil | (1,1,1)(1,1,0,12) | **3 638.14** | **0.7552** |
| `exp_002_sarima_sutil` *(descartado, se conserva)* | Sutil | (1,1,3)(2,1,0,12) | 12 031.69 | −1.1115 |
| `exp_002_sarima_dulce` | Dulce | (1,0,0)(0,1,0,12) | 53.40 | 0.8846 |
| `exp_003_prophet_sutil` | Sutil | — | 3 682.03 | 0.7925 |
| `exp_003_prophet_dulce` | Dulce | — | 64.58 | 0.8094 |

*(Métricas en toneladas. **El test de GC1 está a horizonte 13–24 meses**, no 1–12.
Estas cifras son de referencia histórica y **no deben usarse para seleccionar
metodología** — ver §2.)*

### 14.11 · Entorno

- **Python 3.11 estricto** (TensorFlow incompatible con 3.13+).
- Ejecutar siempre con el venv: `venv\Scripts\python.exe` (Windows) o
  `.\venv\Scripts\Activate.ps1`.
- Detalles de máquina/versiones: `v2_reentrenamiento/ENTORNO_COMPUTO.md` — **NO
  VERIFICADO en esta sesión** (no se abrió).

### 14.12 · Elementos NO VERIFICADOS

| Elemento | Motivo |
|---|---|
| Texto formal del **PPI** (objetivo general, objetivos específicos, hipótesis) | **NO EXISTE EN EL REPOSITORIO.** Búsqueda repo-wide: «objetivo general» → 1 solo archivo (`CONTEXTO_SESION_2026-09-07.md`, que se declara reconstrucción); «objetivos específicos» → 0; «hipótesis general/específicas» → 0 |
| Las **7 referencias bibliográficas** de §8 | Aportadas por el investigador; no hay copias ni notas en el repositorio |
| **Rezagos reales de publicación** de MIDAGRI y NASA POWER | Siguen sin verificar (D24') |
| Contenido de `ENTORNO_COMPUTO.md` y `HANDOFF_CONTEXTO_ACTUAL.md` | No abiertos en esta sesión |
| Cadena `sisagri_2015_2026.xlsx → distrito → provincia → nacional` | **No reproducible**: no existe código que la genere (D21) |
| Script generador de `indeci_limpio_2016_2025.csv` | **No existe en el repositorio**, aunque la transformación **sí fue reconstruida y verificada** (D31) |

---

## 15. HALLAZGOS TÉCNICOS DE ESTA SESIÓN QUE DEBEN SOBREVIVIR

### 15.1 · D20-d — `total_afectados` (APROBADA, pendiente de documentar)

```
total_afectados  ==  personas_afectadas + personas_damnificadas
```

| Comprobación | Resultado |
|---|---|
| En el archivo INDECI oficial | **dif = 0.0 EXACTO** |
| En las columnas `_lag` de features | max abs dif = **0.0001** (redondeo del CSV a 4 decimales) |
| Escala de la variable | 1.99 … 58 813.83 (media 1 749.15) |

**Efecto sobre la matriz de diseño de TRAIN (90 filas, estandarizada):**

| Conjunto exógeno no-NLP | k | rank | **número de condición** | max abs r |
|---|---:|---:|---:|---:|
| **CON** `total_afectados_lag{1,3,6}` | 36 | 36 | **1 007 580 912** | 0.984 |
| **SIN** `total_afectados_lag{1,3,6}` | 33 | 33 | **49.0** | 0.956 |

Mejora del condicionamiento en **7 órdenes de magnitud**.

**Implementación:** `StandardScaler` es columna a columna → excluir esas 3
columnas al construir la matriz de entrada es **numéricamente idéntico** a no
haberlas ajustado nunca. **NO requiere regenerar Fase 2.**

**Variables INDECI candidatas tras D20-d (5 × 3 lags = 15):**
`num_emergencias` · `personas_afectadas` · `personas_damnificadas` ·
`hectareas_cultivo_perdidas` · `hectareas_cultivo_afectadas`.

### 15.2 · Degeneración de calendario y tendencia bajo diferenciación

Comprobación aritmética (sin ajustar ningún modelo), aplicando el operador
`(1−B)(1−B^12)` del orden `exp_002b` (1,1,1)(1,1,0,12):

| Columna | max abs tras `(1−B)(1−B^12)` | Resultado |
|---|---:|---|
| `t_index` | **0.000000e+00** | **columna nula** |
| `mes_sin` | **0.000000e+00** | **columna nula** |
| `mes_cos` | **0.000000e+00** | **columna nula** |
| `T2M_lag1` | 1.8057e+00 | no degenerada |
| `num_emergencias_lag1` | 2.7449e+01 | no degenerada |

**Relevante si alguna vez se reactiva un SARIMAX:** con `simple_differencing=True`
esas tres columnas serían literalmente ceros (matriz singular); con el default
`simple_differencing=False` la regresión va sobre niveles y no revienta, pero su
efecto queda absorbido por la diferenciación. Calendario y tendencia pertenecen
al canal neuronal, no al componente SARIMAX. *(Hoy no bloquea nada: SARIMAX-LSTM
está fuera del experimento principal.)*

### 15.3 · Restricción estructural sobre «90 vs 96 observaciones»

El CSV de features **empieza en 2016-07** y no contiene filas 2016-01…2016-06.
Por tanto, bajo Fase 2 congelada, **96 observaciones solo son alcanzables para un
modelo sin exógenas rezagadas** (leyendo el master crudo, como hizo GC1).
Cualquier modelo **con** exógenas `_lag` está limitado a **90**. Ventanas
intermedias exigirían regenerar Fase 2 → prohibido.
Dato de consistencia: **GC1 ya usó train = 2016-07…2023-12 (90)** pese a ser univariado.

### 15.4 · Verificaciones de Fase 2 que siguen pasando

- NaN = 0 e Inf = 0 en los 6 archivos.
- Anti-fuga celda a celda: **4 788** comprobaciones por cultivar (114 × 42), **0 fallos**.
- Cero exógenas contemporáneas; 42 columnas `_lag` = (13 exógenas + 1 target) × 3.
- Scaler: `n_samples_seen_ = 90`, `n_features_in_ = 44`; `|media_train| máx`
  4.93e-15 (Sutil) / 5.67e-15 (Dulce); `std_train = 1.0000000000`;
  `|media_val| máx` 1.9631 / 2.2481 y `|media_test| máx` 2.4250 / 2.4250
  (>0 ⇒ solo `transform`, nunca `fit`).
- `t_index` `int64`, 0…113, con los 4 checks calendario correctos
  (2016-07 = 0 · 2023-12 = 89 · 2024-01 = 90 · 2025-12 = 113).

---

## 16. CORRECCIÓN A `CONTEXTO_SESION_2026-09-07.md`

### 16.1 · «El peldaño GC3 nunca existió» — **AFIRMACIÓN FALSA**

`CONTEXTO_SESION_2026-09-07.md:760` afirma que el peldaño GC3 nunca existió.
La auditoría de genealogía del 2026-09-08 demostró lo contrario.

**Evidencia:** `resultados/gc1/reporte_ejecutivo_gc1.md`, **fechado 25 de mayo de
2026** —anterior a toda implementación de Fases 3–4— contiene el plan original:

| Actividad | Modelo | Estado (al 25-05-2026) |
|---|---|---|
| 11 — SARIMA | GC1 baseline univariado | Completado |
| 12 — Prophet | GC1 baseline univariado | Completado |
| 13 — LSTM-Attention | **GC2** univariado | Pausado |
| 14 — LSTM Multivariado | **GC3** con variables exógenas | Pendiente |
| 15 — Modelo Multimodal | **Agro + Clima + NLP (BETO)** | Pendiente |
| 16 — Análisis SHAP | Explicabilidad XAI | Pendiente |

Y su §1: «**Propósito del GC1:** fijar el piso de rendimiento que los modelos
multivariados (GC2, GC3, **modelos multimodales con NLP/BETO**) deberán superar.»

**En el diseño original NO existen las etiquetas «GE» ni «GM».**

**GC3 sí se implementó: es la Actividad 14, renombrada «GE».** El markdown de
`actividad_13` define GC2 como «sin NLP / **sin Attention**: diferencia respecto a
GC3», lo que implica que GC3 debía **tener** Attention y **no tener** NLP — que es
exactamente lo que «GE» es. **No hubo un peldaño colapsado: hubo un renombramiento.**

### 16.2 · Genealogía de las etiquetas

| Momento | Artefacto | Qué ocurrió |
|---|---|---|
| 25-05-2026 | `reporte_ejecutivo_gc1.md` | Plan: GC1/GC2/GC3 + «modelos multimodales con NLP». **Ni «GE» ni «GM»** |
| 02-06-2026 (`16bab43`) | `notebooks/fase3/actividad_14_ge_lstm_attention.ipynb` | **Nace «GE»**, autodenominado «**Fase 3 — Modelo Experimental (GE)**» (no «Grupo Estructural»). Ocupa el hueco de GC3. Canal A=1, Canal B=23, sin NLP, 65 249 par. |
| 02-06-2026 | `resultados/generar_reporte_final_fase3.py` | Fase 3 cierra con 4 modelos y rotula GE «MEJOR MODELO» |
| 02-06-2026 | `notebooks/fase4/actividad_15_multimodal_nlp.ipynb` | **Nace «GM»** para el modelo multimodal **ya planificado**. Motivación escrita: «Añadirlo al Canal B debería mejorar **el R² negativo del modelo GE**». Arquitectura **idéntica a GE** + 2 columnas NLP contemporáneas |
| 02-06-2026 | **`resultados/gm_v2/gm_v2_metricas.json`** | **Primera aparición de la clave `"GE_sin_NLP"`** — la taxonomía «GE = sin NLP / GM = con NLP» se cristaliza **DESPUÉS** de ejecutar ambos modelos |
| 02–24-06-2026 | **20 archivos** heredan `GE_sin_NLP` | Propagación automática |
| posterior | `CLAUDE.md:57`, `docs/pipeline_fases_234.md:56`, `CONTEXTO_SESION:76,702` | **Renombran GE como «Grupo Estructural»** y consolidan «GE vs GM» como si fueran dos grupos de diseño |

**Conclusión:** la interpretación «GE = sin NLP / GM = con NLP» es **reconstruida a
posteriori**, no parte del diseño experimental original. **Hubo un solo grupo
experimental**, y el modelo multimodal con NLP **estaba planificado desde el
25-05-2026** como Actividad 15 — no se inventó tras el fallo de GE. Lo que se
desplazó de su rol fue **GE**, que era GC3 (un control) y pasó a presentarse como
«Modelo Experimental».

### 16.3 · El marco C1 / C2 / C3 queda OBSOLETO

`CONTEXTO_SESION_2026-09-07.md` §9 plantea tres alternativas de comparabilidad
(C1/C2/C3) construidas sobre la premisa de que GC3 era **un modelo a añadir**.
Como GC3 ya existe bajo otro nombre, ese marco pierde sentido.
**Sustituido por la taxonomía de §4 de este documento.**

### 16.4 · Genealogía de las variantes GM (todas FUERA del diseño v2)

```
GE (act. 14)  -- R2 -2.34 -----------------------------+
                                                       |  ablación limpia
GM original (act. 15) = GE + 2 cols NLP contemporáneas -+  R2 -5.96  <- EMPEORA
        |
        |  la ablación limpia no dio el resultado esperado
        v
GM_v2 (act. 15v2)  arquitectura NUEVA (PCA 95%->8 comp + LSTM + self-attention
                   1 cabeza, 23 106 par.) + split NUEVO (44/12) + módulos M1-M4
        +--> GM_v3 (act. 15v5)  = GM_v2 con corpus NLP ampliado (multi-fuente)
        +--> GM_v4 (act. 15v6)  = GM_v3 + shock-weighted loss + canales redefinidos (A=20, B=2)
```

GM_v2/v3/v4 son **variantes de rescate**, no grupos formales: cambian
arquitectura, split y módulos a la vez. **Violarían las condiciones de ablación de
§4.7 y quedan fuera del diseño v2.**

---

# PUNTO EXACTO DE REANUDACIÓN

## ⛔ NO ENTRENAR. NO IMPLEMENTAR. NO REGENERAR.

**La próxima sesión debe:**

1. **Leer COMPLETO este archivo** y, para el contexto de Fase 2, ponderación
   espacial y GC1, también `CONTEXTO_SESION_2026-09-07.md` (teniendo presente el
   aviso de supersesión del encabezado y §16).
2. **Leer `v2_reentrenamiento/DECISIONES_METODOLOGICAS.md`** (625 líneas).
3. **Comprobar `git status` y HEAD.** Esperado: `v2-reentrenamiento`, `bfd19a7`,
   13 ` M` / 4 ` D` / 32 `??`.
4. **Verificar los 14 hashes de §14.2 y §14.3.** Si alguno difiere, **DETENERSE y
   reportarlo**: significaría que los datasets o los inputs cambiaron fuera de esta
   cadena de decisiones.
5. **Ejecutar la AUDITORÍA PRE-ENTRENAMIENTO de §12** (23 puntos), sin cambios.
6. **DETENERSE y esperar aprobación.**

**No cerrar D35 por cuenta propia. No elegir el denominador de MASE/RMSSE sin
instrucción. No formalizar la taxonomía sin instrucción.**

**Recordatorio permanente:** ante cualquier ambigüedad metodológica, DETENERSE,
reportarla y esperar decisión. No resolverla unilateralmente aunque exista una
opción que parezca razonable.

---

*Documento generado el 2026-09-08 por Claude Code (auditoría, sin modificación de
datos ni entrenamiento). Complementa —no sustituye— a `CONTEXTO_SESION_2026-09-07.md`.*
