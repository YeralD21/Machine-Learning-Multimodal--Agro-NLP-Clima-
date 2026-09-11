# AUDITORIA IMPLEMENTACION TECNICA GC3/GE

## 1. Alcance

Implementacion de infraestructura tecnica reutilizable para GC3/GE v2, sin
entrenamiento experimental oficial. No se ejecutaron las 10 seeds oficiales, no
se lanzaron las 40 corridas, no se entreno 300 epochs, no se evaluo test 2025,
no se calcularon metricas cientificas sobre test, no se hizo HPO y no se
modificaron decisiones metodologicas.

## 2. Archivos inspeccionados

- `v2_reentrenamiento/DECISIONES_METODOLOGICAS.md`
- `v2_reentrenamiento/auditorias/AUDITORIA_D4_D36_ARQUITECTURA_GC3_GE.md`
- `v2_reentrenamiento/auditorias/AUDITORIA_D0_D10_REPRODUCIBILIDAD_MULTISEED.md`
- `CONTEXTO_SESION_2026-09-08_METRICAS.md`
- `v2_reentrenamiento/ENTORNO_COMPUTO.md`
- `v2_reentrenamiento/data/processed/master_dataset_{sutil,dulce}_v2_features.csv`
- `v2_reentrenamiento/resultados_v2_final/scalers/scaler_{sutil,dulce}_v2c_parametros.csv`

## 3. Archivos creados/modificados

Archivos creados:

- `v2_reentrenamiento/src/models/__init__.py`
- `v2_reentrenamiento/src/models/dual_lstm_attention.py`
- `v2_reentrenamiento/src/models/features_gc3_ge.py`
- `v2_reentrenamiento/src/training/__init__.py`
- `v2_reentrenamiento/src/training/config_gc3_ge.py`
- `v2_reentrenamiento/src/training/determinism.py`
- `v2_reentrenamiento/src/training/callbacks_gc3_ge.py`
- `v2_reentrenamiento/src/training/sequences_gc3_ge.py`
- `v2_reentrenamiento/src/training/artifacts_gc3_ge.py`
- `v2_reentrenamiento/src/training/metadata_gc3_ge.py`
- `v2_reentrenamiento/src/training/history_gc3_ge.py`
- `v2_reentrenamiento/src/evaluation/__init__.py`
- `v2_reentrenamiento/src/evaluation/metrics_d35.py`
- `v2_reentrenamiento/src/evaluation/shocks_d35.py`
- `v2_reentrenamiento/src/evaluation/predictions_io.py`
- `v2_reentrenamiento/auditorias/verificar_implementacion_gc3_ge.ps1`
- `v2_reentrenamiento/auditorias/AUDITORIA_IMPLEMENTACION_TECNICA_GC3_GE.md`

Archivos modificados en esta tarea:

- ninguno preexistente.

## 4. Arquitectura implementada

Factory unica:

- `build_gc3_ge_model(model_name, n_features_a, n_features_b)`

Arquitectura:

```text
Rama A:
Input(6, 4)
-> LSTM(16, return_sequences=True)
-> BahdanauAttention(16)

Rama B:
Input(6, 33) para GC3
Input(6, 39) para GE
-> LSTM(16, return_sequences=True)
-> BahdanauAttention(16)

Concatenate(context_A, context_B)
-> Dense(16, relu)
-> Dropout(0.20)
-> Dense(8, relu)
-> Dense(1)
```

No se agregaron capas LSTM adicionales, Bidirectional, BatchNorm, LayerNorm,
nuevas regularizaciones, nuevas capas Dense ni nuevos mecanismos de attention.

## 5. Features GC3

Rama A, 4 features:

```text
produccion_t_{cultivar}
produccion_t_{cultivar}_lag1
produccion_t_{cultivar}_lag3
produccion_t_{cultivar}_lag6
```

Rama B GC3, 33 features:

```text
mes_sin
mes_cos
t_index
T2M_lag1
T2M_lag3
T2M_lag6
T2M_MAX_lag1
T2M_MAX_lag3
T2M_MAX_lag6
WS2M_lag1
WS2M_lag3
WS2M_lag6
PRECTOTCORR_lag1
PRECTOTCORR_lag3
PRECTOTCORR_lag6
RH2M_lag1
RH2M_lag3
RH2M_lag6
num_emergencias_lag1
num_emergencias_lag3
num_emergencias_lag6
personas_afectadas_lag1
personas_afectadas_lag3
personas_afectadas_lag6
personas_damnificadas_lag1
personas_damnificadas_lag3
personas_damnificadas_lag6
hectareas_cultivo_perdidas_lag1
hectareas_cultivo_perdidas_lag3
hectareas_cultivo_perdidas_lag6
hectareas_cultivo_afectadas_lag1
hectareas_cultivo_afectadas_lag3
hectareas_cultivo_afectadas_lag6
```

Total GC3: 37 inputs.

## 6. Features GE

GE usa la misma Rama A de GC3 y la misma Rama B GC3 mas:

```text
avg_sentiment_lag1
avg_sentiment_lag3
avg_sentiment_lag6
n_noticias_lag1
n_noticias_lag3
n_noticias_lag6
```

Rama B GE: 39 features. Total GE: 43 inputs.

## 7. Construccion de secuencias

Modulo:

- `v2_reentrenamiento/src/training/sequences_gc3_ge.py`

Regla implementada:

```text
contexto filas [j-lookback, j)
target fila j
```

Con `lookback = 6`, la secuencia termina en mes `t` y predice `t+1`. Todas las
fechas de contexto deben ser estrictamente anteriores a la fecha del target.

## 8. Alineamiento temporal

Auditoria estatica ejecutada:

```text
v2_reentrenamiento/auditorias/verificar_implementacion_gc3_ge.ps1
```

Resultado:

```text
OK_IMPLEMENTACION_GC3_GE
TRAIN_SEQUENCES=84
VAL_TARGETS=12
TEST_TARGETS=12
```

Verificaciones:

- feature rows: 114
- TRAIN feature rows: 90, 2016-07..2023-12
- TRAIN targets: 84, 2017-01..2023-12
- validation targets: 12, primera fecha 2024-01
- test targets: 12, primera fecha 2025-01
- 2026 ausente

## 9. Shapes

Shapes esperados/preparados:

| Modelo | Split | X_a | X_b | y |
|---|---|---:|---:|---:|
| GC3 | TRAIN | (84, 6, 4) | (84, 6, 33) | (84,) |
| GC3 | VAL | (12, 6, 4) | (12, 6, 33) | (12,) |
| GC3 | TEST | (12, 6, 4) | (12, 6, 33) | (12,) |
| GE | TRAIN | (84, 6, 4) | (84, 6, 39) | (84,) |
| GE | VAL | (12, 6, 4) | (12, 6, 39) | (12,) |
| GE | TEST | (12, 6, 4) | (12, 6, 39) | (12,) |

No se generaron predicciones test ni metricas test.

## 10. Conteo de parametros

La factory contiene un guard con `model.count_params()` y detiene si el conteo
no coincide con la auditoria.

Debido a que el entorno Python/TensorFlow oficial no esta disponible en esta
sesion, no se pudo ejecutar runtime `model.count_params()`. El venv Windows
apunta a un Python base inexistente y WSL devuelve acceso denegado. No se
reparo el entorno.

Verificacion matematica ejecutada por PowerShell:

| Modelo | Esperado | Obtenido por formula auditada |
|---|---:|---:|
| GC3 | 6,273 | 6,273 |
| GE | 6,657 | 6,657 |

Implementacion exacta de BahdanauAttention:

```text
W_query: Dense(16, use_bias=False)
W_values: Dense(16, use_bias=False)
V: Dense(1, use_bias=False)
softmax sobre eje temporal
context = sum(alpha * values, axis=1)
```

## 11. Loss/optimizer

Implementado conforme a D4-loss:

```text
optimizer = Adam(learning_rate=0.001)
loss = "mse"
metrics = ["mae"]
```

MAE permanece como metrica primaria de evaluacion fuera de muestra segun D35-a.

## 12. Callbacks

Modulo:

- `v2_reentrenamiento/src/training/callbacks_gc3_ge.py`

Callbacks preparados:

- EarlyStopping: `monitor="val_loss"`, `patience=15`,
  `restore_best_weights=True`
- ReduceLROnPlateau: `monitor="val_loss"`, `factor=0.5`, `patience=8`,
  `min_lr=1e-6`
- ModelCheckpoint: `monitor="val_loss"`, `save_best_only=True`, ruta dentro del
  directorio de la corrida/seed

`val_loss` corresponde a MSE sobre validation 2024.

## 13. Determinismo

Modulo:

- `v2_reentrenamiento/src/training/determinism.py`

Funcion:

```text
configure_determinism(seed)
```

Controla:

- `PYTHONHASHSEED`
- `TF_ENABLE_ONEDNN_OPTS=0` como medida conservadora CPU v2
- `TF_DETERMINISTIC_OPS=1` cuando corresponde
- Python `random`
- NumPy
- TensorFlow seed
- `keras.utils.set_random_seed(seed)` si esta disponible
- `tf.config.experimental.enable_op_determinism()` si esta disponible

El reporte marca si TensorFlow ya estaba importado antes de configurar las
variables de entorno.

## 14. Multi-seed preparado

Modulo:

- `v2_reentrenamiento/src/training/config_gc3_ge.py`

Constante:

```text
OFFICIAL_SEEDS = tuple(range(10))
```

No se ejecutaron seeds.

## 15. Metadata

Modulo:

- `v2_reentrenamiento/src/training/metadata_gc3_ge.py`

Prepara registro de:

- cultivar, modelo, seed
- versiones Python/TensorFlow/Keras/NumPy cuando el entorno este disponible
- OS, CPU/GPU
- determinism settings y oneDNN setting
- timestamp UTC
- git commit/hash
- hashes de dataset y scaler
- hash de configuracion del modelo
- lookback, batch size, learning rate, max epochs
- best_epoch y stopped_epoch cuando existan

## 16. Historial TRAIN/VAL

Modulo:

- `v2_reentrenamiento/src/training/history_gc3_ge.py`

Columnas preparadas:

```text
epoch
loss
val_loss
mae
val_mae
learning_rate
```

Tambien se contemplan `best_epoch` y `stopped_epoch` en metadata.

## 17. Predicciones temporales

Modulo:

- `v2_reentrenamiento/src/evaluation/predictions_io.py`

Columnas preparadas:

```text
fecha
y_real
y_pred
error
abs_error
es_shock
```

No se generaron predicciones test oficiales.

## 18. Metricas preparadas

Modulo:

- `v2_reentrenamiento/src/evaluation/metrics_d35.py`

Funciones preparadas:

- MAE
- RMSE
- RelMAE_N1
- MASE_1
- RMSSE_1
- R2
- MAE_global
- MAE_shock
- MAE_nonshock
- n_shock
- n_nonshock
- Delta_s

No se calcularon metricas sobre test.

## 19. Shock protocol

Modulo:

- `v2_reentrenamiento/src/evaluation/shocks_d35.py`

P75 TRAIN-only oficial:

| Cultivar | P75 |
|---|---:|
| Sutil | 0.240834900212216 |
| Dulce | 0.320281354618397 |

Regla implementada:

```text
shock si abs((y_t - y_(t-1)) / y_(t-1)) > P75_c
```

La comparacion es estricta `>`.

## 20. Tests anti-leakage

Script ejecutado:

```text
powershell -NoProfile -ExecutionPolicy Bypass -File v2_reentrenamiento/auditorias/verificar_implementacion_gc3_ge.ps1
```

Checks incluidos:

1. Rama A = 4.
2. Rama B GC3 = 33.
3. Rama B GE = 39.
4. GC3 total = 37.
5. GE total = 43.
6. Columnas requeridas existen.
7. `precio_chacra_kg` ausente de inputs.
8. `n_provincias` ausente de inputs.
9. `total_afectados` ausente de inputs.
10. 2026 ausente.
11. TRAIN rows features = 90.
12. TRAIN sequences = 84.
13. VAL targets = 12.
14. TEST targets = 12.
15. Primer target TRAIN = 2017-01.
16. Ultimo target TRAIN = 2023-12.
17. Primer target VAL = 2024-01.
18. Primer target TEST = 2025-01.
19. Scaler vigente registra 44 features escaladas.
20. Parametros GC3 = 6,273 por formula.
21. Parametros GE = 6,657 por formula.

Adicionalmente, busqueda estatica confirmo que no hay llamadas a `.fit()`,
`.evaluate()` ni `.predict()` en los nuevos modulos.

## 21. Graficos futuros

El pipeline queda preparado para generar posteriormente, sin fabricar datos
ilustrativos:

- serie historica mensual 2016-2025 con TRAIN/VAL/TEST;
- TRAIN MSE vs VALIDATION MSE;
- TRAIN MAE vs VALIDATION MAE;
- Real vs Predicho por modelo;
- Real + SARIMA + XGBoost + GC3 + GE;
- Real vs Predicho con meses shock resaltados;
- distribucion del MAE entre seeds GC3/GE;
- SHAP en su fase correspondiente.

Sutil y Dulce deben graficarse por separado cuando compartir escala dificulte
la interpretacion.

## 22. Limitaciones/ambiguedades

No quedan ambiguedades metodologicas nuevas.

Limitacion tecnica: no se pudo ejecutar import de Python/TensorFlow ni
`model.summary()` ni `model.count_params()` runtime porque:

- `python` no existe en PATH;
- `py -0p` no encuentra instalaciones;
- `venv/Scripts/python.exe` apunta a un Python base inexistente;
- WSL devuelve acceso denegado.

No se reparo el entorno, conforme a las restricciones de la tarea.

## 23. Estado final

Infraestructura implementada y auditoria estatica superada. La ejecucion
oficial multi-seed sigue pendiente. Antes del entrenamiento oficial debe
repararse/recapturarse el entorno Python/TensorFlow v2 y ejecutar la
verificacion runtime de modelos, incluyendo `model.count_params()`.
