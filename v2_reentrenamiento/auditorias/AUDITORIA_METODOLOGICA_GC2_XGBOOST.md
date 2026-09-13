# AUDITORIA METODOLOGICA GC2/XGBOOST

Tarea: auditoria y diseno de protocolo para GC2/XGBoost v2. No se entreno
XGBoost, no se ejecuto `model.fit()`, no se hizo HPO, no se evaluo TEST 2025 y
no se modificaron datasets, scalers, GC3/GE ni resultados oficiales existentes.

Los resultados TEST 2025 congelados de GC3/GE no se usaron para definir ni
favorecer GC2/XGBoost.

## A. Estado del XGBoost existente

### Implementacion v2

No se encontro implementacion activa v2 de GC2/XGBoost en:

- `v2_reentrenamiento/src/`
- `v2_reentrenamiento/src/models/`
- `v2_reentrenamiento/src/training/`
- `v2_reentrenamiento/src/evaluation/`

Tampoco existe aun un runner oficial v2 de XGBoost. Los archivos v2 actuales
son de GC3/GE, Naive/SARIMA/Prophet, auditorias y resultados ya congelados.

En `CONTEXTO_SESION_2026-09-10_CIERRE_GC3_GE.md` consta que GC2/XGBoost
requiere protocolo propio antes de comparacion final y que la politica de
seeds para GC2 queda pendiente segun estocasticidad final.

### Implementaciones y resultados historicos v1

| Ruta | Finalidad | Clasificacion |
|---|---|---|
| `notebooks/fase4/actividad_15v3_xgboost_competidor.ipynb` | XGBoost competidor historico | v1 principal |
| `resultados/xgboost/xgb_metricas.json` | Metricas v1 principal | v1 principal |
| `resultados/xgboost/xgb_predicciones.csv` | Predicciones v1 principal | v1 principal |
| `notebooks/fase4/actividad_16_reentrenamiento_extendido.ipynb` | Reentrenamiento extendido 2019-2025 con XGBoost | v1/analisis extendido, no v2 |
| `resultados/xgboost_ext/metricas.json` | Metricas XGBoost extendido | v1/analisis extendido |
| `resultados/xgboost_final/metricas.json` | Variante final historica | v1/analisis historico |
| `CLAUDE.md` | Handoff historico, indica que el modelo XGBoost no fue guardado | v1/documentacion |
| `docs/pipeline_fases_234.md` | Documentacion del pipeline historico | v1/documentacion |
| `docs/*`, `dashboard/*`, scripts de visualizacion | Visualizaciones/relatos historicos sobre XGBoost | v1/documentacion |

`resultados/gc2/` no corresponde al GC2 actual de la taxonomia v2: contiene
`GC2_SARIMAX_LSTM`, no XGBoost. Esa denominacion queda historicamente
incompatible con la taxonomia actual, donde GC2 = XGBoost.

## B. Diferencias v1 -> v2

### XGBoost v1 principal auditado

Fuente: `notebooks/fase4/actividad_15v3_xgboost_competidor.ipynb` y
`resultados/xgboost/xgb_metricas.json`.

- Dataset: `data/processed/master_dataset_fase2_multivariado.csv`.
- Agregacion: `groupby('fecha_evento').mean(numeric_only=True)`.
- NLP: merge con `notebooks/fase2/output/01_nlp_sentimiento/sentimiento_mensual.csv`.
- Target: `produccion_t`.
- Features: `EXOG` completo del dataframe, mas `prod_lag1`, `prod_lag2`,
  `prod_lag3`, `prod_lag6`, `prod_roll3_mean`, `prod_roll6_mean`,
  `prod_roll3_std`.
- Numero guardado de features: 29.
- Split: 80/20 cronologico despues de `dropna`.
- Conteos guardados: `n_train=40`, `n_test=10`.
- Predicciones guardadas: 2024-11 a 2025-08.
- Grid: `max_depth=[2,3,4]`, `n_estimators=[50,100,200]`,
  `learning_rate=[0.05,0.1,0.2]`, `subsample=[0.8,1.0]`,
  `colsample_bytree=[0.8,1.0]`.
- CV: `TimeSeriesSplit(n_splits=3)` sobre `X_train`.
- Mejor configuracion historica: `max_depth=2`, `n_estimators=200`,
  `learning_rate=0.05`, `subsample=0.8`, `colsample_bytree=0.8`.
- Seed: `SEED=42`, `random_state=42`.
- Metricas: MAE, RMSE, R2, MAPE, sMAPE, MASE.
- MASE historico: `mae / naive_mae` usando `naive_mae` calculado sobre el
  propio periodo test del split 80/20.

### Incompatibilidades de v1 con v2

- Usa NLP (`nlp_index`, `nlp_index_lag1`), incompatible con GC2 v2 si se cierra
  como competidor tabular NO-NLP.
- Usa `prod_lag2` y rolling features no preespecificadas para v2.
- Usa EXOG completo historico, incluyendo variables no aprobadas para GC3 v2.
- Usa split 80/20, no split v2 TRAIN 2016-2023, VAL 2024, TEST 2025.
- Evalua un periodo historico de 10 meses que cruza 2024-11..2025-08, no TEST
  v2 Jan-Dec 2025.
- Calcula shock con umbral fijo historico `variacion_pct > 20`, no D35-b.
- MASE no usa denominadores TRAIN-only D35 congelados.
- No guarda modelo final XGBoost; `CLAUDE.md` indica que debe reentrenarse
  desde notebook para reconstruirlo.
- No hay trazabilidad v2 de hashes, scalers v2c, git hash, entorno y artefactos
  por cultivar/modelo/seed.

## C. Features propuestas

La hipotesis auditada es metodologicamente coherente: GC2/XGBoost debe ser un
competidor tabular NO-NLP con el mismo conjunto informativo aprobado para GC3,
pero con representacion tabular propia.

Features candidatas recomendadas para aprobacion:

Rama A GC3, usadas como features tabulares:

- `produccion_t_{cultivar}`
- `produccion_t_{cultivar}_lag1`
- `produccion_t_{cultivar}_lag3`
- `produccion_t_{cultivar}_lag6`

Rama B GC3, usadas como features tabulares:

- `mes_sin`
- `mes_cos`
- `t_index`
- `T2M_lag1`, `T2M_lag3`, `T2M_lag6`
- `T2M_MAX_lag1`, `T2M_MAX_lag3`, `T2M_MAX_lag6`
- `WS2M_lag1`, `WS2M_lag3`, `WS2M_lag6`
- `PRECTOTCORR_lag1`, `PRECTOTCORR_lag3`, `PRECTOTCORR_lag6`
- `RH2M_lag1`, `RH2M_lag3`, `RH2M_lag6`
- `num_emergencias_lag1`, `num_emergencias_lag3`, `num_emergencias_lag6`
- `personas_afectadas_lag1`, `personas_afectadas_lag3`,
  `personas_afectadas_lag6`
- `personas_damnificadas_lag1`, `personas_damnificadas_lag3`,
  `personas_damnificadas_lag6`
- `hectareas_cultivo_perdidas_lag1`, `hectareas_cultivo_perdidas_lag3`,
  `hectareas_cultivo_perdidas_lag6`
- `hectareas_cultivo_afectadas_lag1`, `hectareas_cultivo_afectadas_lag3`,
  `hectareas_cultivo_afectadas_lag6`

Total: 37 predictores.

Exclusiones propuestas:

- NLP.
- `precio_chacra_kg`.
- `n_provincias`.
- `total_afectados` y sus lags.
- Exogenas contemporaneas.
- Rolling features nuevas.
- `lag2` heredado de v1.

Justificacion: esto iguala la informacion disponible entre GC2 y GC3 sin copiar
la arquitectura secuencial de GC3. XGBoost recibe una fila tabular `X_t` con
variables ya rezagadas; GC3 recibe una ventana secuencial de seis filas. La
comparabilidad se basa en informacion y tarea predictiva, no en representacion.

## D. Representacion temporal

Regla comun:

```text
informacion disponible al cierre de t -> prediccion de y_(t+1)
```

Para XGBoost tabular, la representacion natural es:

```text
fila tabular X_t -> target y_(t+1)
```

No debe crearse una ventana LSTM `(lookback, features)` para XGBoost solo por
simetria formal. Las dependencias temporales ya estan representadas por:

- `produccion_t_{cultivar}`
- lags de produccion 1/3/6
- lags 1/3/6 de NASA e INDECI
- codificacion temporal y tendencia

## E. Numero esperado de observaciones

El dataset v2 con lags tiene 114 filas efectivas:

- 2016-07..2025-12.
- TRAIN feature rows: 90, 2016-07..2023-12.
- VAL feature rows: 12, 2024-01..2024-12.
- TEST feature rows: 12, 2025-01..2025-12.

### Alternativa A: tabular natural con desplazamiento `X_t -> y_(t+1)`

TRAIN:

- Primera fecha X: 2016-07.
- Primera fecha target: 2016-08.
- Ultima fecha X: 2023-11.
- Ultima fecha target: 2023-12.
- n TRAIN esperado: 89.

VAL:

- Primera fecha X: 2023-12.
- Primera fecha target: 2024-01.
- Ultima fecha X: 2024-11.
- Ultima fecha target: 2024-12.
- n VAL esperado: 12.

TEST futuro, no ejecutar en esta auditoria:

- Primera fecha X: 2024-12.
- Primera fecha target: 2025-01.
- Ultima fecha X: 2025-11.
- Ultima fecha target: 2025-12.
- n TEST esperado: 12.

Ventaja: maxima eficiencia de datos y representacion tabular correcta.

Riesgo: TRAIN usa 89 pares, mientras GC3 usa 84 secuencias. La diferencia es
metodologicamente defendible porque GC3 pierde cinco targets adicionales por
su necesidad arquitectonica de ventana de seis filas. No debe forzarse a
XGBoost a perder observaciones si no hay razon temporal.

### Alternativa B: restringir a los mismos targets que GC3

TRAIN:

- Primera fecha X: 2016-12.
- Primera fecha target: 2017-01.
- Ultima fecha X: 2023-11.
- Ultima fecha target: 2023-12.
- n TRAIN esperado: 84.

VAL:

- Primera fecha X: 2023-12.
- Primera fecha target: 2024-01.
- Ultima fecha X: 2024-11.
- Ultima fecha target: 2024-12.
- n VAL esperado: 12.

Ventaja: misma cantidad de targets TRAIN que GC3/GE.

Riesgo: descarta artificialmente cinco pares validos de entrenamiento para un
modelo tabular por imitar una restriccion arquitectonica ajena.

### Recomendacion para aprobacion

Adoptar Alternativa A: 89 pares TRAIN tabulares. La comparacion justa exige la
misma tarea predictiva, las mismas fuentes informativas aprobadas y el mismo
TEST final, no igualar la perdida de muestras inducida por una arquitectura
secuencial.

## F. Scaling

Opciones auditadas:

### A) Usar `_v2_escalado.csv`

Ventajas:

- Reutiliza artefactos v2c ya auditados.
- Evita crear un segundo pipeline de datos para GC2.
- Mantiene trazabilidad de hashes, splits y columnas.
- Target escalado puede invertirse con los mismos scalers v2c para evaluar en
  toneladas bajo D35.

Riesgos:

- XGBoost/arboles no necesitan escalado; el escalado no aporta beneficio
  teorico importante.
- Las importancias de variables quedan en escala estandarizada, aunque para
  arboles esto no afecta los cortes monotonicamente.

### B) Usar `_v2_features.csv` raw

Ventajas:

- Escala original mas interpretable para splits de arboles.
- Evita preprocesamiento innecesario para XGBoost.

Riesgos:

- Introduce una ruta operativa distinta a GC3/GE.
- Requiere reglas adicionales para target, metadata y reproducibilidad.
- Podria generar diferencias por manejo de variables no escaladas y aumentar
  superficie de auditoria.

### C) Combinacion controlada

Ventaja: permite target escalado y features raw, o viceversa.

Riesgo: complejidad innecesaria y mayor probabilidad de errores de pipeline.

Recomendacion para aprobacion: usar `master_dataset_{cultivar}_v2_escalado.csv`
sin volver a aplicar scaler. Registrar scaler v2c solo para metadata e inverse
transform. La eleccion prioriza reproducibilidad y consistencia operativa v2;
no se justifica como mejora de desempeno.

## G. Estrategias de hiperparametros

### A) Reutilizar configuracion parsimoniosa historica v1

Configuracion historica:

```text
max_depth=2
n_estimators=200
learning_rate=0.05
subsample=0.8
colsample_bytree=0.8
random_state=42
```

Ventajas:

- Preexistente y documentada antes del TEST v2 GC3/GE.
- Parsimoniosa en profundidad (`max_depth=2`).
- Evita nuevo HPO.
- Computacionalmente barata.

Riesgos:

- Fue seleccionada por grid v1 con datos, features y split distintos.
- Incluia NLP, rolling features y `lag2`, por tanto no es una seleccion directa
  para GC2 v2.
- `n_estimators=200` sin early stopping puede ser alto para ~89 observaciones,
  aunque `learning_rate=0.05` y regularizacion por subsampling moderan el riesgo.

### B) Preespecificar configuracion parsimoniosa v2

Ventajas:

- Independiente de los resultados TEST v2 GC3/GE.
- Puede fijar explicitamente regularizacion ausente en v1:
  `min_child_weight`, `reg_alpha`, `reg_lambda`, `gamma`.
- Reduce grados de libertad.
- Adecuada para Small Data.

Riesgos:

- Introduce una nueva decision metodologica que requiere aprobacion.
- Si se elige sin CV, no optimiza empiricamente para cada cultivar.

### C) Tuning exclusivamente dentro de TRAIN

Procedimiento posible:

- TimeSeriesSplit o expanding-window dentro de TRAIN tabular.
- Grid muy acotado.
- VAL 2024 reservado solo para diagnostico/early stopping si se aprueba.
- TEST 2025 prohibido.

Ventajas:

- Seleccion dentro de TRAIN, sin usar VAL 2024 ni TEST 2025.
- Puede adaptar complejidad al dataset v2.

Riesgos:

- Con ~89 observaciones, incluso un grid pequeno puede sobreajustar folds muy
  cortos.
- Aumenta grados de libertad y complejidad de auditoria.
- Puede generar una asimetria frente a GC3/GE, donde no se hizo busqueda de
  arquitectura/hyperparams sobre validation.

Recomendacion para aprobacion: Estrategia B. Preespecificar una configuracion
parsimoniosa v2 basada en Small Data y regularizacion, sin HPO. La configuracion
historica v1 puede servir como evidencia de rango razonable, no como resultado
automaticamente transferible.

## H. Regularizacion

Parametros que deben fijarse explicitamente en el futuro runner:

| Parametro | Valor actual v2 | Valor v1 observado | Candidato v2 | Justificacion |
|---|---:|---:|---:|---|
| `objective` | No existe | default no explicitado | `reg:squarederror` | Regresion continua; explicito y reproducible. |
| `eval_metric` | No existe | no explicitado | `mae` para diagnostico, o `rmse` si se usa early stopping sobre RMSE | D35 primaria MAE; entrenamiento de arboles permite metricas de eval. |
| `max_depth` | No existe | 2 | 2 | Arboles poco profundos por Small Data. |
| `min_child_weight` | No existe | default | 3 | Regularizacion adicional para evitar hojas pequenas. |
| `learning_rate` | No existe | 0.05 | 0.05 | Conservador; continuidad con v1 sin grid. |
| `n_estimators` | No existe | 200 | 200 fijo si no hay early stopping; 500 con early stopping solo si se aprueba | Con LR bajo requiere suficientes arboles; evitar HPO. |
| `subsample` | No existe | 0.8 | 0.8 o 1.0 segun decision de seeds | 0.8 regulariza pero introduce stochasticity. |
| `colsample_bytree` | No existe | 0.8 | 0.8 o 1.0 segun decision de seeds | 0.8 regulariza; stochasticity real. |
| `reg_alpha` | No existe | default | 0.0 | Mantener parsimonia salvo decision explicita. |
| `reg_lambda` | No existe | default | 1.0 | Regularizacion L2 default explicita. |
| `gamma` | No existe | default | 0.0 | No agregar umbral de split sin justificacion adicional. |
| `tree_method` | No existe | default | `hist` | Reproducibilidad y eficiencia CPU. |
| `n_jobs` | No existe | default | 1 | Reducir variacion por paralelismo; costo bajo con small data. |
| `random_state` / `seed` | No existe v2 | 42 | ver D43 | Depende de si se adopta multi-seed. |

La tabla es una propuesta tecnica pendiente; no queda aprobada por esta
auditoria.

## I. Early stopping

Opciones:

### n_estimators fijo

Ventajas:

- Simetria metodologica con un modelo preespecificado sin usar VAL para ajustar
  el numero efectivo de arboles.
- Menor dependencia de 12 meses de VAL.
- Facil reproducibilidad.

Riesgos:

- No controla dinamicamente sobreajuste.
- `n_estimators` queda como hiperparametro fijo sensible.

### Early stopping con VAL 2024

Ventajas:

- Analogo al uso de VAL 2024 en GC3/GE para control de optimizacion.
- Permite fijar un techo alto de arboles y restaurar el mejor numero.

Riesgos:

- En XGBoost, early stopping elige explicitamente `best_iteration`, que es un
  hiperparametro efectivo del modelo. Con solo 12 meses de VAL, puede sobreajustar
  al ano 2024.
- Si se usa VAL para seleccionar `best_iteration`, debe quedar estrictamente
  prohibido usar VAL para elegir otros parametros.

Recomendacion para aprobacion: preferir `n_estimators` fijo en la primera
version oficial GC2 v2. Si se aprueba early stopping, debe ser una decision
separada y preespecificada con `eval_set=VAL 2024`, `early_stopping_rounds`
fijo, sin grid y sin cambiar otras configuraciones.

## J. Seeds/reproducibilidad

XGBoost puede ser estocastico si:

- `subsample < 1`.
- `colsample_bytree < 1`.
- hay paralelismo o metodos no deterministas segun backend.

Alternativas:

### A) Configuracion determinista y seed fija

Usar `subsample=1.0`, `colsample_bytree=1.0`, `n_jobs=1` y una seed fija
preespecificada.

Ventaja: una corrida por cultivar/modelo; maxima simplicidad.

Riesgo: pierde regularizacion por subsampling y se aparta de la configuracion
historica v1.

### B) Multi-seed 0..9 si hay stochasticity real

Usar `subsample=0.8`, `colsample_bytree=0.8`, `n_jobs=1` y seeds 0..9.

Ventaja: caracteriza sensibilidad a la aleatoriedad inducida por subsampling,
emparejable nominalmente con GC3/GE sin seleccionar best seed.

Riesgo: aumenta costo y volumen de artefactos, aunque bajo para XGBoost.

Recomendacion para aprobacion: si se mantiene subsampling como regularizacion,
usar seeds 0..9. Esa variabilidad representa sensibilidad a inicializacion/
muestreo estocastico del boosting, no incertidumbre temporal ni intervalo de
confianza. Si se decide `subsample=colsample_bytree=1.0`, usar una seed fija
preespecificada y no repetir 10 corridas artificiales.

## K. Comparabilidad con GC3/GE

Comparaciones permitidas:

- Naive <-> XGBoost: comparacion contra baseline operacional t-1 mediante D35.
- SARIMA <-> XGBoost: familia estadistica clasica vs ML tabular.
- XGBoost <-> GC3: familias de modelo distintas con informacion NO-NLP
  comparable.
- XGBoost <-> GE: familia tabular NO-NLP vs familia neuronal con NLP; no es
  ablacion causal de NLP ni de arquitectura.

Aclaracion central:

XGBoost <-> GC3/GE es comparacion entre familias de modelos y representaciones.
La ablacion limpia del NLP sigue siendo GC3 <-> GE porque comparten arquitectura
y difieren solo en las seis variables NLP lagged.

## L. Proteccion contra adaptacion post-TEST

No se leyeron predicciones, metricas ni figuras TEST 2025 de GC3/GE para tomar
decisiones sobre GC2/XGBoost.

Fuentes de cada decision propuesta:

| Decision | Fuente |
|---|---|
| GC2 como XGBoost NO-NLP | Taxonomia actual y `CONTEXTO_SESION_2026-09-08_METRICAS.md`/handoffs metodologicos. |
| 37 features GC3 como informacion de GC2 | D36 cerrada para GC3/GE, no resultados TEST. |
| Exclusion NLP | Taxonomia GC2/GC3/GE y ablacion GC3->GE. |
| Exclusion `precio_chacra_kg`, `n_provincias`, `total_afectados`, contemporaneas | D36 y validadores de features GC3/GE. |
| Representacion tabular `X_t -> y_(t+1)` | Metodologia temporal v2 y naturaleza de XGBoost. |
| Usar `_v2_escalado.csv` sin refit | Pipeline v2c auditado y reproducibilidad, no desempeno observado. |
| Hiperparametros parsimoniosos | Small Data, regularizacion y evidencia historica v1 como rango, no TEST v2. |
| No grid grande sobre VAL 2024 | D3-op y tamano VAL=12. |
| Seeds dependientes de stochasticity | D10 deja GC2 pendiente y criterio tecnico de XGBoost. |

Ninguna decision se justifica por superar, aproximarse o compensar resultados
TEST observados de GC3/GE.

## M. Riesgos metodologicos

- XGBoost con 37 predictores y ~89 pares TRAIN sigue siendo Small Data; incluso
  arboles poco profundos pueden sobreajustar.
- Usar 89 pares TRAIN mejora eficiencia pero no iguala exactamente n TRAIN de
  GC3; debe aprobarse explicitamente.
- Usar `_v2_escalado.csv` es reproducible pero no necesario para arboles.
- Subsampling introduce stochasticity real; exige politica de seeds clara.
- Early stopping con 12 meses de VAL puede convertirse en seleccion indirecta
  de complejidad.
- No existe todavia runner v2 ni pre-flight oficial de GC2/XGBoost.
- La nomenclatura historica `GC2=SARIMAX+LSTM` puede inducir confusion; en v2
  debe quedar congelado que GC2=XGBoost.

## N. Propuesta recomendada

Recomendacion metodologica para someter a aprobacion:

- GC2/XGBoost v2 = competidor tabular NO-NLP.
- Cultivares: sutil y dulce.
- Features: exactamente las 37 features de GC3, aplanadas en una fila tabular.
- Representacion: `X_t -> y_(t+1)`.
- TRAIN tabular: 89 pares, 2016-07->2016-08 hasta 2023-11->2023-12.
- VAL: 12 pares, 2023-12->2024-01 hasta 2024-11->2024-12.
- TEST futuro: 12 pares, 2024-12->2025-01 hasta 2025-11->2025-12. No abrir en
  esta etapa.
- Datos: `master_dataset_{cultivar}_v2_escalado.csv`, sin volver a escalar.
- Scalers v2c: metadata e inverse transform futuro; no refit.
- Hiperparametros: preespecificados, parsimoniosos, sin HPO.
- Early stopping: no usar inicialmente; `n_estimators` fijo.
- Seeds: si `subsample=0.8` y `colsample_bytree=0.8`, usar seeds 0..9; si se
  cambia a 1.0/1.0, usar una seed fija.
- Artefactos futuros minimos por corrida: config, metadata, modelo serializado,
  predicciones VAL si se autorizan, logs, hashes de dataset/scaler/modelo,
  versiones, OS, CPU/GPU, git hash, estado final, feature list, split rows,
  parametros XGBoost completos y feature importance diagnostica.

## O. Decisiones que requieren aprobacion

| ID | Decision | Alternativas | Recomendacion | Justificacion | Estado |
|---|---|---|---|---|---|
| D37 | Definicion de features GC2 | 37 GC3 no-NLP; v1 29 features; incluir NLP; agregar rolling/lag2 | 37 GC3 no-NLP | Igualdad de informacion con GC3 sin contaminar ablacion NLP. | PENDIENTE DE APROBACION METODOLOGICA |
| D38 | Representacion temporal | Tabular `X_t -> y_(t+1)`; ventana LSTM; targets GC3 forzados | Tabular `X_t -> y_(t+1)` | XGBoost es tabular; la igualdad relevante es informacion/tarea, no arquitectura. | PENDIENTE DE APROBACION METODOLOGICA |
| D39 | Scaling | `_v2_escalado.csv`; raw `_v2_features.csv`; combinacion | `_v2_escalado.csv` sin refit | Reutiliza pipeline v2c auditado y simplifica trazabilidad. | PENDIENTE DE APROBACION METODOLOGICA |
| D40 | Estrategia de hiperparametros | Reusar v1; preespecificar v2; tuning TRAIN-only | Preespecificar v2 parsimonioso | Evita HPO y adaptacion post-TEST; adecuado a Small Data. | PENDIENTE DE APROBACION METODOLOGICA |
| D41 | Configuracion XGBoost | Historica v1; determinista 1.0/1.0; regularizada con subsampling | `objective=reg:squarederror`, `max_depth=2`, `learning_rate=0.05`, `n_estimators=200`, `min_child_weight=3`, `subsample=0.8`, `colsample_bytree=0.8`, `reg_alpha=0`, `reg_lambda=1`, `gamma=0`, `tree_method=hist`, `n_jobs=1` | Parsimonia, regularizacion y continuidad parcial con v1 sin aceptar su grid como protocolo v2. | PENDIENTE DE APROBACION METODOLOGICA |
| D42 | Early stopping | No usar; usar VAL 2024 con rounds fijo | No usar inicialmente | Evita convertir VAL=12 en seleccion de complejidad efectiva. | PENDIENTE DE APROBACION METODOLOGICA |
| D43 | Seeds/stochasticity | Seed fija; seeds 0..9 | Seeds 0..9 si subsampling < 1; seed fija si deterministicidad completa | Multi-seed solo si hay stochasticity real. | PENDIENTE DE APROBACION METODOLOGICA |
| D44 | Protocolo de validacion | VAL 2024; TRAIN-only CV; sin validacion | Sin HPO; VAL 2024 solo diagnostico/pre-flight. Si se exige seleccion, CV TRAIN-only muy acotado | Evita grid grande y no usa TEST. | PENDIENTE DE APROBACION METODOLOGICA |
| D45 | Reproducibilidad/artefactos | Minima; completa por corrida | Completa por cultivar/seed con hashes, versiones, config, modelo, logs, splits y feature list | Necesaria para comparabilidad con GC3/GE. | PENDIENTE DE APROBACION METODOLOGICA |

## Git status final

```text
warning: unable to access 'C:\Users\ADMIN/.config/git/ignore': Permission denied
warning: unable to access 'C:\Users\ADMIN/.config/git/ignore': Permission denied
## main...origin/main
?? .claude/
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
?? v2_reentrenamiento/HANDOFF_CONTEXTO_ACTUAL.md
?? v2_reentrenamiento/auditorias/AUDITORIA_ENTRENAMIENTO_OFICIAL_GC3_GE.md
?? v2_reentrenamiento/auditorias/AUDITORIA_EVALUACION_TEST_GC3_GE.md
?? v2_reentrenamiento/auditorias/AUDITORIA_METODOLOGICA_GC2_XGBOOST.md
?? v2_reentrenamiento/auditorias/AUDITORIA_POST_RUN_GC3_GE.md
?? v2_reentrenamiento/auditorias/AUDITORIA_RUNNER_OFICIAL_GC3_GE.md
?? v2_reentrenamiento/auditorias/auditar_entrenamiento_oficial_gc3_ge.py
?? v2_reentrenamiento/auditorias/auditar_post_run_final_gc3_ge.py
?? v2_reentrenamiento/auditorias/auditar_runner_oficial_gc3_ge.py
?? v2_reentrenamiento/auditorias/ejecutar_entrenamiento_oficial_gc3_ge.py
?? v2_reentrenamiento/auditorias/entrenamiento_oficial_gc3_ge_manifest.json
?? v2_reentrenamiento/auditorias/evaluar_test_gc3_ge.py
?? v2_reentrenamiento/auditorias/resumen_entrenamiento_oficial_gc3_ge.csv
?? v2_reentrenamiento/auditorias/resumen_entrenamiento_oficial_gc3_ge.json
?? v2_reentrenamiento/auditorias/resumen_post_run_gc3_ge.csv
?? v2_reentrenamiento/auditorias/resumen_post_run_gc3_ge.json
?? v2_reentrenamiento/auditorias/runner_oficial_gc3_ge_dry_run.json
?? v2_reentrenamiento/resultados_v2_final/evaluacion_test_gc3_ge/
?? v2_reentrenamiento/resultados_v2_final/official_gc3_ge/
?? v2_reentrenamiento/resultados_v2_final/technical_pilot/
?? v2_reentrenamiento/resultados_v2_final/technical_pilot_scaled/
?? v2_reentrenamiento/src/training/official_runner_gc3_ge.py
```

No commit. No push.

AUDITORÍA GC2/XGBOOST COMPLETADA Y PENDIENTE DE APROBACIÓN METODOLÓGICA
