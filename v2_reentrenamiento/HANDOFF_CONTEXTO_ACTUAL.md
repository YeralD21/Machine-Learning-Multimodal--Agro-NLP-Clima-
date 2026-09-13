# HANDOFF — Contexto operativo actual (v2 Reentrenamiento)

---

## ACTUALIZACION 2026-09-12 -- EVALUACION TEST GC2/XGBOOST CONGELADA

Esta seccion prevalece sobre las actualizaciones anteriores para el estado de
GC2/XGBoost.

Se ejecuto la apertura unica y final de TEST 2025 para GC2/XGBoost v2, segun
autorizacion explicita del usuario.

Alcance:

```text
GC2 = XGBoost
modelos evaluados = 20
cultivares = sutil, dulce
seeds = 0..9
predicciones TEST = 240
TEST target = 2025-01..2025-12
representacion = X_t -> y_(t+1)
primer forecast = 2024-12 -> 2025-01
```

Restricciones verificadas:

```text
no retraining
no HPO
no CV
no early stopping
no best seed
no descarte de seeds
no cambios de features
no cambios de hiperparametros
no refit de scaler
no uso de TEST para modificar modelos
```

Resultado de auditoria:

```text
v2_reentrenamiento/auditorias/AUDITORIA_EVALUACION_TEST_GC2_XGBOOST.md
status = approved
controles = 458
fallos = 0
```

Artefactos oficiales:

```text
v2_reentrenamiento/resultados_v2_final/evaluacion_test_gc2_xgboost/predicciones_test_gc2_xgboost.csv
v2_reentrenamiento/resultados_v2_final/evaluacion_test_gc2_xgboost/metricas_test_por_seed_gc2_xgboost.csv
v2_reentrenamiento/resultados_v2_final/evaluacion_test_gc2_xgboost/resumen_test_multiseed_gc2_xgboost.csv
v2_reentrenamiento/resultados_v2_final/evaluacion_test_gc2_xgboost/metricas_shock_por_seed_gc2_xgboost.csv
v2_reentrenamiento/resultados_v2_final/evaluacion_test_gc2_xgboost/resumen_shock_multiseed_gc2_xgboost.csv
v2_reentrenamiento/resultados_v2_final/evaluacion_test_gc2_xgboost/manifest_evaluacion_test_gc2_xgboost.json
v2_reentrenamiento/auditorias/AUDITORIA_EVALUACION_TEST_GC2_XGBOOST.md
```

Figuras diagnosticas oficiales:

```text
v2_reentrenamiento/resultados_v2_final/evaluacion_test_gc2_xgboost/figuras/
```

Metricas multi-seed TEST principales:

```text
sutil MAE mean=5902.375361, median=5895.677050, SD=140.128108, min=5626.916107, max=6068.807395
sutil RMSE mean=7010.785262, median=7043.292203, SD=96.968340, min=6834.980366, max=7105.380628
sutil RelMAE_N1 mean=1.254679, MASE_1 mean=1.749074, RMSSE_1 mean=1.619089, R2 mean=0.352848

dulce MAE mean=59.378883, median=59.703676, SD=2.428516, min=56.048322, max=64.173183
dulce RMSE mean=77.205668, median=77.210505, SD=3.394146, min=72.921016, max=84.933167
dulce RelMAE_N1 mean=0.701042, MASE_1 mean=0.809661, RMSSE_1 mean=0.897207, R2 mean=0.792714
```

Metricas shock multi-seed:

```text
sutil n_shock=3, n_nonshock=9, MAE_shock mean=8296.896516, MAE_nonshock mean=5104.201642, Delta_s mean=40.615361
dulce n_shock=3, n_nonshock=9, MAE_shock mean=90.104501, MAE_nonshock mean=49.137010, Delta_s mean=51.774420
```

Incidencia operativa:

```text
La primera ejecucion calculo y escribio CSV/manifest/figuras, pero fallo al
redactar la auditoria por ausencia del paquete opcional tabulate usado por
pandas.to_markdown(). No se repitio la evaluacion TEST. Se corrigio el script
para generar Markdown sin dependencia externa y se ejecuto solo --audit-only
leyendo los artefactos ya producidos.
```

No se hizo commit ni push.

### Siguiente paso exacto

```text
Interpretacion posterior de resultados GC2 ya congelados, o integracion en
tabla conjunta cuando los demas modelos esten igualmente congelados.
```

No realizar ajustes de GC2 despues de TEST.

---

## ACTUALIZACION 2026-09-12 -- CIERRE ENTRENAMIENTO OFICIAL GC2/XGBOOST

Esta seccion prevalece sobre la actualizacion 2026-09-11 para el estado de
ejecucion de GC2/XGBoost. La taxonomia vigente se mantiene:

```text
GC2 = XGBoost
```

Se ejecutaron las 20 corridas oficiales de GC2/XGBoost:

```text
2 cultivares x 10 seeds
cultivares = sutil, dulce
seeds = 0..9
```

Restricciones respetadas:

```text
no HPO
no CV
no early stopping
no best seed
no cambios de features
no cambios de hiperparametros
no apertura de TEST 2025
artefactos completos por corrida
```

Artefactos nuevos principales:

```text
v2_reentrenamiento/auditorias/ejecutar_entrenamiento_oficial_gc2_xgboost.py
v2_reentrenamiento/auditorias/entrenamiento_oficial_gc2_xgboost_manifest.json
v2_reentrenamiento/auditorias/auditar_post_run_gc2_xgboost.py
v2_reentrenamiento/auditorias/AUDITORIA_POST_RUN_GC2_XGBOOST.md
v2_reentrenamiento/auditorias/resumen_post_run_gc2_xgboost.csv
v2_reentrenamiento/auditorias/resumen_post_run_gc2_xgboost.json
v2_reentrenamiento/resultados_v2_final/official_gc2_xgboost/
```

Por cada corrida se guardo:

```text
model.xgboost.json
model.joblib
metricas.json
predicciones_train_val.csv
feature_importance.csv
config.json
metadata.json
logs/subprocess_stdout.log
logs/subprocess_stderr.log
```

Auditoria post-run:

```text
v2_reentrenamiento/auditorias/AUDITORIA_POST_RUN_GC2_XGBOOST.md
status = approved
83/83 controles OK
20 manifest records
20 corridas auditadas
TRAIN shape por corrida: X=(89,37), y=(89,)
VAL shape por corrida: X=(12,37), y=(12,)
TEST 2025 abierto = NO
```

Metricas de validacion registradas en:

```text
v2_reentrenamiento/auditorias/resumen_post_run_gc2_xgboost.csv
```

Rango observado de MAE de validacion:

```text
sutil: 4174.339290 .. 4719.800022
dulce: 45.888951 .. 55.458889
```

No se hizo commit ni push.

### Siguiente paso exacto

```text
Solicitar autorizacion explicita para apertura unica de TEST GC2/XGBoost.
```

Hasta que esa autorizacion exista:

```text
no abrir TEST 2025
no evaluar TEST GC2
no elegir best seed
no comparar contra modelos finales usando TEST
no commit/push salvo orden explicita del usuario
```

---

## ACTUALIZACION 2026-09-11 -- CIERRE PREFLIGHT GC2/XGBOOST

Esta seccion prevalece sobre las secciones historicas inferiores donde GC2
aparece como no iniciado o como SARIMAX+LSTM. La taxonomia vigente es:

```text
GC2 = XGBoost
```

No se entreno GC2/XGBoost, no se ejecuto model.fit(), no se hizo HPO, no se
abrio TEST 2025, no se hizo commit y no se hizo push.

### Estado metodologico alcanzado

D37-D45 quedaron **CERRADAS** en DECISIONES_METODOLOGICAS.md.

Correccion metodologica aprobada y registrada:

```text
min_child_weight oficial = 1
```

No usar min_child_weight=3.

GC2/XGBoost quedo implementado con logica tabular separada de GC3/GE. La
representacion oficial es:

```text
X_t -> y_(t+1)
```

XGBoost es tabular. No se construyen ventanas LSTM.

### Archivos creados/modificados en el cierre GC2

```text
v2_reentrenamiento/src/models/features_gc2_xgboost.py
v2_reentrenamiento/src/training/config_gc2_xgboost.py
v2_reentrenamiento/src/training/tabular_gc2_xgboost.py
v2_reentrenamiento/src/training/official_runner_gc2_xgboost.py
v2_reentrenamiento/auditorias/AUDITORIA_PREFLIGHT_GC2_XGBOOST.md
v2_reentrenamiento/auditorias/gc2_xgboost_preflight_dry_run.json
v2_reentrenamiento/DECISIONES_METODOLOGICAS.md
v2_reentrenamiento/HANDOFF_CONTEXTO_ACTUAL.md
```

### Resultado del pre-flight GC2/XGBoost

Informe:

```text
v2_reentrenamiento/auditorias/AUDITORIA_PREFLIGHT_GC2_XGBOOST.md
```

JSON estructurado:

```text
v2_reentrenamiento/auditorias/gc2_xgboost_preflight_dry_run.json
```

Resultado:

```text
35/35 controles OK
20 futuras corridas esperadas
seeds = 0..9
features = 37
Sutil TRAIN X=(89,37), y=(89,)
Dulce TRAIN X=(89,37), y=(89,)
TRAIN temporal: X 2016-07..2023-11, y 2016-08..2023-12
VAL X=(12,37), y=(12,)
VAL temporal: X 2023-12..2024-11, y 2024-01..2024-12
min_child_weight=1 verificado
model.fit() = NO
TEST cargado = NO
HPO = NO
CV/TimeSeriesSplit = NO
early stopping = NO
commit/push = NO
```

### Configuracion oficial congelada GC2/XGBoost

```text
objective = reg:squarederror
eval_metric = mae
booster = gbtree
max_depth = 2
min_child_weight = 1
learning_rate = 0.05
n_estimators = 200
subsample = 0.8
colsample_bytree = 0.8
reg_alpha = 0.0
reg_lambda = 1.0
gamma = 0.0
tree_method = hist
n_jobs = 1
random_state = seed
seeds = 0..9
```

### Siguiente paso exacto

```text
Autorizar y ejecutar las 20 corridas oficiales de GC2/XGBoost:
2 cultivares x 10 seeds.
```

Restricciones para esa siguiente etapa:

```text
no HPO
no CV
no early stopping
no best seed
no cambios de features
no cambios de hiperparametros
no abrir TEST 2025
guardar artefactos completos por corrida
```

Despues de las 20 corridas:

```text
1. Auditoria post-run GC2.
2. Recien despues, autorizacion explicita de apertura unica TEST GC2.
```

### Pendientes posteriores

Mantener como pendientes posteriores:

```text
evaluacion TEST GC2
SARIMA rolling one-step
comparacion final Naive/SARIMA/XGBoost/GC3/GE
Prophet secundario
SHAP
actualizacion final del PPI
```

### Nota para reanudacion

Las secciones antiguas de este handoff reflejan el estado del 2026-09-04 y
quedan como trazabilidad historica. Para continuar GC2, usar como fuentes
vigentes:

```text
v2_reentrenamiento/DECISIONES_METODOLOGICAS.md
v2_reentrenamiento/auditorias/AUDITORIA_METODOLOGICA_GC2_XGBOOST.md
v2_reentrenamiento/auditorias/AUDITORIA_PREFLIGHT_GC2_XGBOOST.md
v2_reentrenamiento/auditorias/gc2_xgboost_preflight_dry_run.json
v2_reentrenamiento/src/training/official_runner_gc2_xgboost.py
```


**Generado:** 2026-09-04
**Destinatario:** ChatGPT, que ya leyó `DECISIONES_METODOLOGICAS.md` y el short paper.
**Alcance:** SOLO contexto operativo que no se deduce de esos documentos.
No re-explica decisiones ya documentadas, no rediseña metodología, no propone
modelos ni hiperparámetros, no toma decisiones nuevas.

> ⚠️ **Advertencia de versión — NO CONFIRMADO.** No consta en qué momento
> ChatGPT leyó `DECISIONES_METODOLOGICAS.md`. Ese archivo pasó de 294 a 524
> líneas el 2026-09-04 (commit `bfd19a7`), añadiendo 4 secciones nuevas:
> bug de horizonte SARIMA, sobreajuste por AIC en SARIMA-Sutil, salto de nivel
> / tendencia secular (con descomposición área+rendimiento) y Δs con n_shock=3.
> **Si la lectura fue anterior a ese commit, esas 4 secciones son
> desconocidas para ChatGPT y deben leerse antes de continuar.**

---

## 1. Punto exacto donde quedamos

**Fase actual:** Fase 3 — Modelado (v2).
**Estado de la fase:** GC1 **cerrado y commiteado**. GC2 **no iniciado**.

**"Gate Check": NO CONFIRMADO.** No existe ningún artefacto con ese nombre en
el repositorio (`grep -rniE "gate.?check"` sin resultados). Si es un concepto
de la planificación externa (PPI/asesoría), no está reflejado en el repo y no
puede mapearse desde aquí.

**Lo último que se hizo:**

1. Se cerró GC1 por completo: corrección del bug de horizonte, establecimiento
   de `exp_002b` como SARIMA-Sutil representativo, comparativa oficial
   regenerada, documentación en `DECISIONES_METODOLOGICAS.md`, y **commit
   `bfd19a7`** con 24 archivos.
2. Se documentó el entorno de cómputo en `ENTORNO_COMPUTO.md` (nuevo, **sin
   commitear**).
3. Se investigó por qué v1 pudo usar GPU y v2 no, con hallazgo forense sobre
   el entorno dual de v1 (detalle en §3 y en `ENTORNO_COMPUTO.md`).

**Último archivo/notebook trabajado:**
`v2_reentrenamiento/notebooks/fase3_modelado/02_gc1_sarima_prophet.ipynb`
(celda 20 parcheada + re-ejecución completa con `nbconvert`), y después
`v2_reentrenamiento/ENTORNO_COMPUTO.md`.

**Siguiente acción antes de detenernos:**
diseñar **GC2 (SARIMAX + LSTM)** — pero está **bloqueada** por una decisión de
determinismo pendiente de aprobación del usuario (ver §4).

---

## 2. Trabajo posterior a la última entrada de DECISIONES_METODOLOGICAS.md

Las 4 secciones nuevas de ese documento ya cubren: bug de horizonte,
sobreajuste por AIC, tendencia secular / `t_index` / lags one-step-ahead, y
Δs con n_shock=3. **Aquí solo va lo que NO está allí.**

| # | Qué se hizo | Por qué | Resultado | Estado |
|---|---|---|---|---|
| 2.1 | Parche de la celda 20 de `02_gc1_sarima_prophet.ipynb`: se sustituyó el mapeo hardcodeado `f'exp_002_sarima_{cultivo}'` por un dict explícito `EXP_COMPARATIVA` + guard `FileNotFoundError` | Editar solo el CSV de salida se perdía en la siguiente ejecución del notebook; la corrección tenía que ser durable | La comparativa se regenera correctamente desde cero; si falta `exp_002b` falla ruidosamente en vez de caer en el experimento equivocado | **VERIFICADO** |
| 2.2 | Columna `experimento` añadida a `gc1_comparativa_naive_sarima_prophet.csv` | Procedencia legible por máquina en cada fila | 18 filas, cada una declara su experimento fuente | **VERIFICADO** |
| 2.3 | Creado `resultados_v2_final/README.md` | Documentar la procedencia de SARIMA-Sutil, la dependencia de ejecución de `exp_002b` y la advertencia de Δs junto a los datos, no solo en el doc metodológico | 39 líneas | **IMPLEMENTADO** |
| 2.4 | Re-ejecución completa de `02_gc1_sarima_prophet.ipynb` vía `nbconvert --execute` | El `_ejecutado.ipynb` conservaba código viejo y una conclusión errónea guardada en sus outputs | Exit 0, ~15 min, sin celdas en error. El output ahora muestra `SUTIL SARIMA ... SUPERA al Naive` | **VERIFICADO** |
| 2.5 | Verificación de determinismo del re-run | Comprobar que la re-ejecución no alteraba métricas ya registradas | Idénticas hasta el último dígito (`exp_002_sarima_sutil` MAE_test = 12031.692040368267 en ambas corridas). Solo cambió `fecha_ejecucion` | **VERIFICADO** |
| 2.6 | Verificación de integridad de `REGISTRO_MAESTRO.csv` tras el re-run | El re-run podía sobrescribir la fila de `exp_002b` (que genera otro notebook) | Las 7 filas intactas; el notebook hace upsert por id, no reescribe el archivo | **VERIFICADO** |
| 2.7 | Commit `bfd19a7` con 24 archivos | Cierre de GC1 | Hecho manualmente por el usuario. Ningún archivo de v1 incluido | **IMPLEMENTADO** |
| 2.8 | Creado `ENTORNO_COMPUTO.md` (359 líneas) | Documentar hardware/software antes de entrenar cualquier modelo de deep learning | Ver §3 | **IMPLEMENTADO — sin commitear** |
| 2.9 | Hallazgo forense: v1 usó DOS entornos de cómputo distintos | Investigar por qué v1 pudo usar GPU y v2 no | GE se entrenó en CPU/Windows y GM en GPU/WSL2, con versiones de patch distintas. Detalle en §3 | **VERIFICADO** |
| 2.10 | Sección "Limitación metodológica de v1" añadida a `ENTORNO_COMPUTO.md` | Dejar registro de la limitación identificada y de la corrección que v2 aplica por diseño | Redacción aprobada por el usuario, incorporada literalmente | **IMPLEMENTADO — sin commitear** |

**Nota sobre 2.9:** este hallazgo NO está en `DECISIONES_METODOLOGICAS.md`.
Vive solo en `ENTORNO_COMPUTO.md`, que aún no está commiteado.

---

## 3. ENTORNO_COMPUTO actual

Resumen fiel de `v2_reentrenamiento/ENTORNO_COMPUTO.md` (359 líneas).

**Hardware:** AMD Ryzen 7 5800X (8 físicos / 16 lógicos @ 3801 MHz), 31.93 GB
RAM, NVIDIA GeForce RTX 3060 con 12 288 MiB (12 GB) VRAM, driver **591.86**,
modo WDDM, límite 170 W. `nvidia-smi` reporta "CUDA Version: 13.1", que es la
**máxima soportada por el driver**, no un toolkit instalado.
SO: Windows 11 Pro build 26200 (64 bits).

### Windows (entorno PRIMARIO de v2)

| | |
|---|---|
| venv | `C:\Machine-learming\Machine-Learning-Multimodal--Agro-NLP-Clima-\venv` |
| Python | **3.11.9** |
| TensorFlow | **2.21.0 — build CPU** (`is_cuda_build: False`) |
| Keras | **3.14.0** (backend `tensorflow`, `KERAS_BACKEND` no definida) |
| NumPy / Pandas | **2.4.4 / 3.0.2** |
| Otros | scikit-learn 1.8.0, statsmodels 0.14.6, prophet 1.3.0 (cmdstanpy 1.3.0), pmdarima 2.1.1, xgboost 3.2.0, shap 0.51.0, scipy 1.17.1, joblib 1.5.3, transformers 5.5.4, pysentimiento 0.7.3 |
| torch | 2.11.0**+cpu** (instalado pero NO usado por el proyecto) |
| CPU/GPU | **CPU.** `tf.config.list_physical_devices('GPU')` → `[]` |
| Paquetes | 240 |
| Estado v2 | **Todo v2 corre aquí.** Naive, GC1 (SARIMA, Prophet) ya entrenados y commiteados en este entorno |

No hay Python en el PATH del sistema (alias de Microsoft Store). No hay CUDA
Toolkit ni cuDNN en Windows (`nvcc` ausente del PATH; no existe
`C:\Program Files\NVIDIA GPU Computing Toolkit\CUDA\`).

### WSL2 (entorno usado en v1 — NO usado en v2)

| | |
|---|---|
| Distribución | **Ubuntu 26.04 LTS** sobre WSL2 (estado habitual `Stopped`, se levanta bajo demanda) |
| venv | `/home/yerald/tf-gpu-311` (existe también `/home/yerald/tf-gpu`) |
| Python del venv | **3.11.15** |
| Python del sistema WSL | 3.14.4 — demasiado nuevo para el proyecto, NO usar |
| TensorFlow | **2.21.0 — build CUDA** (`is_built_with_cuda: True`) |
| Keras | **3.14.1** |
| NumPy / Pandas | **2.4.6 / 3.0.3** |
| CUDA | **12.5.1** |
| cuDNN | **9** |
| GPU detectada | **`PhysicalDevice(name='/physical_device:GPU:0', device_type='GPU')`** |
| Paquetes NVIDIA | `nvidia-cudnn-cu12==9.22.0.52`, `nvidia-cublas-cu12==12.9.2.10` |
| Otros | keras-tcn 3.5.6 (ausente en Windows), scikit-learn 1.8.0, statsmodels 0.14.6, prophet 1.3.0, xgboost 3.2.0, shap 0.51.0 |
| Paquetes | 160 |

`nvidia-smi` dentro de WSL2 reporta `NVIDIA GeForce RTX 3060, 591.86`.
Verificado en vivo el 2026-09-04: el entorno **existe y funciona hoy**.

### Diferencias de patch entre entornos (la huella forense)

| Paquete | Windows | WSL2 | ¿Difiere? |
|---|---|---|---|
| tensorflow | 2.21.0 | 2.21.0 | — |
| keras | 3.14.0 | **3.14.1** | ✅ |
| numpy | 2.4.4 | **2.4.6** | ✅ |
| pandas | 3.0.2 | **3.0.3** | ✅ |

### Dónde se ejecutó v1

**En AMBOS entornos, según el notebook.** Determinado por las versiones de
patch impresas en los notebooks `_ejecutado`:

- `notebooks/fase3/actividad_14_ge_lstm_attention.ipynb` (**GE**) →
  Keras 3.14.0, NumPy 2.4.4, Pandas 3.0.2, `Dispositivo TF: CPU`
  → **Windows, CPU**
- `notebooks/fase4/actividad_15_ejecutado.ipynb` (**GM**) →
  Keras 3.14.1, NumPy 2.4.6, Pandas 3.0.3, `GPU detectada: 1 dispositivo(s)`,
  `Dispositivo TF: GPU`, rutas `/mnt/c/...` → **WSL2, GPU**

Triangulación adicional: `actividad_16_ejecutado.ipynb` y
`actividad_17_ejecutado.ipynb` imprimen rutas `/mnt/c/...`, y
`gen_nb_actividad17.py` tiene hardcodeado
`BASE_DIR = pathlib.Path('/mnt/c/Machine-learming/...')`.

**Alcance limitado:** el entorno de GM_v2, XGBoost, TCN, GM_v3 y GM_v4 **no
pudo determinarse** — esos notebooks no imprimen versión ni dispositivo.

### Dónde se está ejecutando v2

**Íntegramente en el venv de Windows, CPU.** Naive y GC1 ya están entrenados
y commiteados en ese entorno.

### Por qué v2 está en CPU

Dos causas independientes, ninguna de ellas "falta de GPU":

1. **TensorFlow ≥ 2.11 no soporta GPU en Windows nativo**, ni aunque
   CUDA/cuDNN estuvieran instalados. El propio TF lo advierte al importarse.
   Además el build instalado es CPU-only: `tf.sysconfig.get_build_info()` no
   expone `cuda_version` ni `cudnn_version` porque no existen en él.
2. **PyTorch está instalado como rueda `+cpu`** — no es limitación de
   plataforma (existe rueda CUDA para Windows), pero el proyecto no usa
   PyTorch de todos modos.

A esto se suma la razón **de diseño**: con 90 observaciones de entrenamiento y
arquitecturas LSTM pequeñas, la GPU no aporta ventaja apreciable, y un único
entorno numérico para toda la Fase 3 es precisamente lo que faltó en v1.

### Por qué WSL2 sí puede usar GPU

Porque es Linux. La restricción de TF ≥ 2.11 es específica de Windows nativo.
El driver de Windows (591.86) se expone a WSL2 sin instalar driver adicional
en Linux, y el stack CUDA se resuelve por paquetes pip dentro del venv
(`nvidia-cudnn-cu12`, `nvidia-cublas-cu12`), no por toolkit del sistema.

### Qué implicaría cambiar ahora de entorno

GC1 ya está entrenado y commiteado (`bfd19a7`) en el venv de Windows. Migrar
GC2/GE/GM a WSL2 **partiría la Fase 3 de v2 en dos entornos numéricos —
exactamente la limitación que se identificó en v1**. La única opción
metodológicamente limpia sería re-ejecutar **toda** la Fase 3 en el nuevo
entorno y re-registrar todos los experimentos, no solo los nuevos.

**No se toma ninguna decisión nueva sobre migrar en este documento.** El
estado registrado es: v2 en Windows/CPU, sin migración planificada.

---

## 4. Determinismo pendiente antes de GC2

### YA DECIDIDO (y en efecto)

- `seed = 42` global fijada antes de cada `fit` (criterio del proyecto, ya
  documentado en `DECISIONES_METODOLOGICAS.md`).
- Prophet con `mcmc_samples=0` (puntos MAP, sin MCMC).
- **Determinismo de GC1 verificado empíricamente (2026-09-04):** la
  re-ejecución completa reprodujo las métricas de `exp_002` y `exp_003` hasta
  el último dígito. SARIMAX (MLE de statsmodels) y Prophet MAP son
  deterministas en esta configuración. **Esto ya no requiere decisión.**

### PENDIENTE DE APROBACIÓN — bloquea el primer entrenamiento de GC2

**Hecho observado:** oneDNN está **ACTIVO** en el build de TensorFlow de
Windows. Al importar TF se emite:

    oneDNN custom operations are on. You may see slightly different numerical
    results due to floating-point round-off errors from different computation
    orders.

**Implicación:** `seed=42` fija inicialización y barajado, pero **no**
garantiza reproducibilidad bit-a-bit entre máquinas mientras oneDNN reordene
operaciones. Para GC1 no aplicaba (SARIMAX/Prophet). **Para el componente LSTM
de GC2, y después para GE y GM, sí aplica.**

**La decisión abierta es binaria:**

- **Opción A — determinismo estricto.** Fijar, ANTES de importar TensorFlow:

      import os
      os.environ['TF_ENABLE_ONEDNN_OPTS'] = '0'
      os.environ['TF_DETERMINISTIC_OPS'] = '1'

  Debe declararse en el notebook correspondiente porque cambia el resultado
  numérico respecto a cualquier entrenamiento previo.

- **Opción B — configuración por defecto.** Aceptar deriva en los últimos
  decimales entre ejecuciones/máquinas.

**Por qué bloquea:** fijarlo antes del primer entrenamiento de GC2 no cuesta
nada; hacerlo después obliga a reentrenar todo lo que ya se haya entrenado con
la configuración anterior.

**Estado: NO DECIDIDO. Corresponde al usuario.** Este documento no elige.

---

## 5. Decisión pendiente sobre PPI/tesis

**Qué se discutió:** si la diferencia de entornos entre v1 (GE en CPU/Windows,
GM en GPU/WSL2, con versiones de patch distintas) debe mencionarse en el
PPI/tesis final como limitación reconocida de v1.

**Estado del registro:** la limitación ya está **documentada por escrito** en
`ENTORNO_COMPUTO.md`, sección "Limitación metodológica de v1 — comparación GE
vs. GM en entornos distintos", con redacción aprobada por el usuario. Esa
redacción establece explícitamente que:

- no se afirma que el resultado de v1 sea inválido;
- con una diferencia relativa del ~4% (GE 0.0673 vs GM_v3 0.0645 MAE), el
  efecto del entorno no puede descartarse como *totalmente* despreciable,
  pero tampoco hay evidencia de que explique la diferencia completa;
- el diseño experimental de v1 simplemente no permite aislar el efecto del NLP
  del efecto del entorno de cómputo;
- v2 corrige esto por diseño (entorno único), y se documenta como mejora
  metodológica, no como corrección de un error de v1.

**Recomendación emitida (pendiente de aprobación del usuario):** mencionarlo,
pero **encuadrado como fortaleza de v2, no como confesión sobre v1**. En el
PPI encajaría en la sección de diseño experimental ("todos los modelos se
entrenan en un entorno de cómputo único, eliminando por diseño la variabilidad
numérica entre entornos"), con la limitación de v1 como nota al pie o en el
apartado de limitaciones. Argumento a favor: si un jurado pregunta por qué v2
reentrena todo desde cero, "unificar el entorno de cómputo" es una respuesta
concreta y verificable; sin ella, el reentrenamiento parece motivado solo por
los datos ampliados. Argumento de cautela: NO presentarlo como que el
resultado GE vs GM de v1 estaba comprometido — la evidencia no lo sostiene.

**Estado: PENDIENTE DE APROBACIÓN DEL USUARIO.** No bloquea GC2.

---

## 6. Estructura relevante actual del repositorio

Sin `.git`, venvs, caches ni dependencias.

```
Machine-Learning-Multimodal--Agro-NLP-Clima-/        ← RAÍZ = proyecto v1
│
├── CLAUDE.md                    guía del repo (describe v1)
├── README.md                    reproducibilidad v1; menciona "Windows + WSL2"
├── requirements.txt             v1, SIN versiones fijadas
│
├── notebooks/                   ── V1 ──
│   ├── fase2/                   NLP, cyclic encoding, lags, escalado
│   ├── fase3/
│   │   ├── actividad_11_gc1_sarima.ipynb
│   │   ├── actividad_12_gc1_prophet.ipynb
│   │   ├── actividad_13_gc2_sarimax_lstm.ipynb      ← REFERENCIA para GC2 v2
│   │   └── actividad_14_ge_lstm_attention.ipynb     ← GE v1 (CPU/Windows)
│   └── fase4/
│       ├── actividad_15_ejecutado.ipynb             ← GM v1 (GPU/WSL2)
│       ├── actividad_15v2..v6_*.ipynb               ← GM_v2, XGBoost, TCN, GM_v3, GM_v4
│       ├── actividad_16_*.ipynb                     SHAP, reentrenamiento extendido
│       └── actividad_17_*.ipynb                     validación final
├── src/                         ── V1 ── agro/, weather/, features/, models/, scraping/
├── pipeline/                    ── V1 ── ETL 10 actividades
├── data/, sources/              ── V1 ── crudos e interinos
├── resultados/                  ── V1 ── modelos entrenados, métricas, PDFs
├── dashboard/, docs/            ── V1 ──
│
└── v2_reentrenamiento/          ══ V2 — TRABAJO ACTUAL ══
    │
    ├── DECISIONES_METODOLOGICAS.md      524 líneas (commiteado)
    ├── ENTORNO_COMPUTO.md               359 líneas (NUEVO, sin commitear)
    ├── HANDOFF_CONTEXTO_ACTUAL.md       este archivo (sin commitear)
    │
    ├── notebooks/
    │   ├── fase1_recoleccion/  09_resumen_fase1[_ejecutado].ipynb
    │   ├── fase2_features/     01_nlp_sentimiento, 02_fusion_dataset_maestro,
    │   │                       03_features_temporales, 04_escalado (+ _ejecutado)
    │   ├── fase3_modelado/     ← FASE ACTUAL
    │   │   ├── 01_baseline_naive[_ejecutado].ipynb
    │   │   ├── 02_gc1_sarima_prophet[_ejecutado].ipynb   ← último trabajado
    │   │   ├── 02b_diagnostico_sarima_sutil.ipynb        (sin _ejecutado)
    │   │   └── (GC2 aún NO existe)
    │   └── fase4_evaluacion/   vacío
    │
    ├── src/                    ← CASI VACÍO (solo .gitkeep + 2 scripts)
    │   ├── data_collection/    agraria_scraper_v2.py, nasa_power_downloader_v2.py
    │   ├── features/           .gitkeep
    │   ├── models/
    │   │   ├── arquitecturas/  .gitkeep   ← aquí irían GC2/GE/GM
    │   │   └── entrenamiento/  .gitkeep
    │   └── evaluation/         .gitkeep
    │
    ├── data/
    │   ├── raw/                midagri, nasa_power, indeci, noticias
    │   ├── interim/            limon_{sutil,dulce}_provincia.csv,
    │   │                       limon_*_distrito_{crudo,dedup}.csv,
    │   │                       limon_*_nacional_mensual.csv,
    │   │                       indeci_limpio_2016_2025.csv,
    │   │                       coordenadas_provincias_completo.csv
    │   └── processed/          master_dataset_{sutil,dulce}_v2.csv          ← GC1 usa este (crudo)
    │                           master_dataset_{sutil,dulce}_v2_features.csv ← 33 cols, con t_index
    │                           master_dataset_{sutil,dulce}_v2_escalado.csv ← 33 cols, escalado
    │
    ├── experimentos/
    │   ├── REGISTRO_MAESTRO.csv          7 filas
    │   ├── exp_001_naive_{sutil,dulce}/
    │   ├── exp_002_sarima_{sutil,dulce}/ (sutil = descartado, se conserva)
    │   ├── exp_002b_sarima_sutil_simple/ ← SARIMA-Sutil representativo
    │   ├── exp_003_prophet_{sutil,dulce}/
    │   └── comparativas/                 vacío
    │       cada exp_*/ contiene: config.yaml, metricas.json, predicciones.csv
    │
    ├── resultados_v2_final/
    │   ├── README.md                                    procedencia + advertencia Δs
    │   ├── gc1_comparativa_naive_sarima_prophet.csv     18 filas, col. `experimento`
    │   ├── scalers/
    │   │   ├── scaler_{sutil,dulce}.joblib
    │   │   └── scaler_{sutil,dulce}_v2b_tindex.joblib   ← ajustados SOLO en train
    │   ├── ge/, ge_versiones/                           vacíos (.gitkeep)
    │   └── gm/, gm_versiones/                           vacíos (.gitkeep)
    │
    └── simulador_tiempo_real/   dashboard_vivo/, escenarios_historicos_opcionales/ (vacíos)
```

**Archivos por grupo de modelos:**

| Grupo | v2 — estado | Artefactos |
|---|---|---|
| Naive | ✅ cerrado | `exp_001_naive_{sutil,dulce}/`, `01_baseline_naive.ipynb` |
| GC1 (SARIMA/Prophet) | ✅ cerrado, commiteado | `exp_002*`, `exp_003*`, `02_gc1_*`, `02b_*`, comparativa |
| **GC2 (SARIMAX+LSTM)** | ⬜ **no iniciado** | ninguno. Referencia v1: `notebooks/fase3/actividad_13_gc2_sarimax_lstm.ipynb` |
| GE | ⬜ no iniciado | `resultados_v2_final/ge/` vacío. Referencia v1: `actividad_14_ge_lstm_attention.ipynb` |
| GM | ⬜ no iniciado | `resultados_v2_final/gm/` vacío. Referencia v1: `actividad_15*` |

---

## 7. Estado de Git

**Branch actual:** `v2-reentrenamiento` (branch principal del repo: `main`).

**`git status` (sin staging, sin commits — el índice está limpio):**

```
?? data/interim/indeci/indeci_temporal_2019_2025.csv
?? data/interim/nasa/clima_dataset_2019_2020.csv
?? sources/agraria-pe/sin-unificar/agro_news_2020.csv
?? sources/agraria-pe/sin-unificar/checkpoint_2019_2020.json
?? sources/noticias-ampliado/
?? src/scraping/agronoticias_scraper.py
?? src/scraping/andina_scraper.py
?? src/scraping/base_scraper.py
?? src/scraping/freshfruit_scraper.py
?? src/scraping/recalcular_sentimiento_v2.py
?? src/scraping/redagricola_scraper.py
?? src/scraping/run_expansion.py
?? src/scraping/unificar_corpus_v2.py
?? src/weather/nasa_power_downloader.py
?? v2_reentrenamiento/ENTORNO_COMPUTO.md
```

**Archivos modificados NO commiteados:** **ninguno.** Todo lo modificado entró
en `bfd19a7`. El árbol de trabajo solo tiene archivos untracked.

**Archivos untracked relevantes:**

| Archivo | Pertenece a | Nota |
|---|---|---|
| `v2_reentrenamiento/ENTORNO_COMPUTO.md` | **v2** | Documento nuevo de esta sesión, pendiente de commit |
| `v2_reentrenamiento/HANDOFF_CONTEXTO_ACTUAL.md` | **v2** | Este archivo (creado después del `git status` mostrado) |
| 8 scrapers en `src/scraping/`, `src/weather/nasa_power_downloader.py` | v1 | Arrastre de trabajo de ampliación de corpus de v1 |
| `sources/noticias-ampliado/`, `sources/agraria-pe/sin-unificar/*` | v1 | Corpus ampliado sin unificar |
| `data/interim/{indeci,nasa}/*.csv` | v1 | Datos interinos 2019-2020/2025 |

Los archivos de v1 quedaron deliberadamente fuera del commit `bfd19a7`; están
previstos para un commit separado posterior.

**Últimos 10 commits** (mensajes truncados a 110 caracteres):

```
bfd19a7  fix(gc1): Corregir bug de horizonte SARIMA y establecer SARIMA-Sutil representativo. Bug critico: forecast(...
ee5f9be  feat(v2): Fases 1-3 completas - recoleccion, features, modelado inicial. Fase 1: MIDAGRI, NASA POWER, INDEC...
e105b0b  feat(v2): Estructura de carpetas para reentrenamiento v2. Nueva estructura contenida en v2_reentrenamiento/...
4147616  fix(critical): Documentar y corregir bug de desnormalizacion en calculo de shocks
5e40cdc  docs: Diagnostico de estructura del proyecto — pre-reorganizacion
83efa21  Last ..
4f98815  docs: README reproducible + run_pipeline.py + requirements completo
7945128  feat: GitHub Pages — copiar dashboard a /docs para despliegue
88bf5d6  feat: iconos Lucide SVG con efecto glow en sidebar del dashboard
21ce925  feat: Dashboard profesional Grafana-style — CRISP-MLQ Fase 5
```

Los commits desde `e105b0b` (inclusive) hacia arriba son de v2; los anteriores
son de v1.

---

## 8. Pendientes reales (abiertos AHORA)

| # | Qué falta | Por qué importa | ¿Bloquea GC2? | Quién decide |
|---|---|---|---|---|
| 1 | **Decisión de determinismo:** `TF_ENABLE_ONEDNN_OPTS=0` + `TF_DETERMINISTIC_OPS=1`, o configuración por defecto (§4) | Fijarlo antes del primer entrenamiento LSTM es gratis; hacerlo después obliga a reentrenar GC2 y todo lo que siga | **SÍ — bloquea** | **Usuario** |
| 2 | **Diseño de GC2 (SARIMAX + LSTM)** | Es la siguiente actividad de Fase 3. No existe notebook ni código | Es GC2 | Usuario + asistente, tras resolver #1 |
| 3 | Mención de la diferencia de entornos v1/v2 en PPI/tesis (§5) | Define cómo se presenta la justificación del reentrenamiento ante el jurado | No | **Usuario** |
| 4 | Commit de `ENTORNO_COMPUTO.md` y `HANDOFF_CONTEXTO_ACTUAL.md` | El hallazgo forense del entorno dual de v1 solo existe en archivos untracked; se perdería si se limpia el árbol | No | Usuario |
| 5 | Commit separado de los archivos de v1 (8 scrapers, corpus ampliado, CSVs interinos) | Quedaron deliberadamente fuera de `bfd19a7`; siguen untracked | No | Usuario |
| 6 | `02b_diagnostico_sarima_sutil.ipynb` no tiene contraparte `_ejecutado` | Los demás notebooks de fase3 sí la tienen. **NO CONFIRMADO** si es omisión o intencional | No | Usuario |
| 7 | `v2_reentrenamiento/src/models/` está vacío (solo `.gitkeep`) | CLAUDE.md exige que toda lógica núcleo sea basada en clases y viva en `src/`; los notebooks son sandboxes. GC2/GE/GM necesitarán código allí | No, pero condiciona cómo se implementa GC2 | Usuario + asistente |
| 8 | `experimentos/comparativas/` está vacío | Directorio previsto pero sin uso. **NO CONFIRMADO** cuál era su propósito | No | Usuario |

**No se listan aquí** las decisiones ya cerradas en
`DECISIONES_METODOLOGICAS.md` (split, umbral de shock, bug de horizonte,
`exp_002b` como representativo, `t_index`, lags one-step-ahead, tratamiento
de Δs).

---

## 9. Punto exacto de reanudación

**ChatGPT, para continuar desde donde quedamos:**

**1. Qué debes revisar primero**

- Confirma que leíste `DECISIONES_METODOLOGICAS.md` en su versión de **524
  líneas** (posterior al commit `bfd19a7`). Si tu lectura fue de la versión de
  294 líneas, léela de nuevo: faltan las 4 secciones del 2026-09-03 (bug de
  horizonte, sobreajuste por AIC, tendencia secular con `t_index` y lags
  one-step-ahead, y Δs con n_shock=3).
- Lee `v2_reentrenamiento/ENTORNO_COMPUTO.md` **completo** — no está
  commiteado y su contenido no se deduce de ningún otro documento.
- Revisa `v2_reentrenamiento/resultados_v2_final/README.md` y
  `gc1_comparativa_naive_sarima_prophet.csv` para el estado de cierre de GC1.
- Nota de orientación: GC1 corre sobre `master_dataset_{sutil,dulce}_v2.csv`
  (crudo, univariado) y **no** consume `t_index`. Los datasets `_features` /
  `_escalado` (33 columnas, con `t_index`) están preparados para los modelos
  multivariados pero aún no los usa ningún experimento.

**2. Qué decisión viene inmediatamente después**

La **decisión de determinismo** (§4): activar
`TF_ENABLE_ONEDNN_OPTS=0` + `TF_DETERMINISTIC_OPS=1` antes del primer
entrenamiento LSTM de GC2, o quedarse con la configuración por defecto.
**Es del usuario. No la tomes tú.** Puedes exponer el trade-off si te lo pide,
pero no elijas.

**3. Qué NO debe ejecutarse todavía**

- No entrenar GC2 ni ningún modelo, hasta que la decisión de determinismo esté
  tomada.
- No crear ni modificar notebooks de GC2.
- No modificar `DECISIONES_METODOLOGICAS.md`.
- No modificar el código ni los notebooks de GC1 ya cerrados.
- No hacer `git add` ni `git commit`.
- No migrar a WSL2/GPU.
- No re-seleccionar hiperparámetros ni redefinir GC1/GC2/GE/GM.

**4. Qué puede ejecutarse una vez aprobada esa decisión**

- Diseñar GC2 (SARIMAX + LSTM) siguiendo el split y las convenciones ya
  fijadas, usando `notebooks/fase3/actividad_13_gc2_sarimax_lstm.ipynb` de v1
  como referencia estructural.
- Crear el notebook `v2_reentrenamiento/notebooks/fase3_modelado/03_gc2_*.ipynb`
  y el código de arquitectura correspondiente en
  `v2_reentrenamiento/src/models/arquitecturas/`.
- Registrar el experimento como `exp_004_*` siguiendo el formato de los
  existentes (`config.yaml`, `metricas.json`, `predicciones.csv`) y añadir su
  fila a `REGISTRO_MAESTRO.csv`.
- Si se aprueba la Opción A de determinismo, declarar las variables de entorno
  **al inicio del notebook, antes de importar TensorFlow**, y dejarlo escrito
  en el `config.yaml` del experimento.

---

**Fin del handoff.** Este documento no modifica código, no entrena, no hace
staging ni commit, y no cierra ninguna decisión pendiente.
