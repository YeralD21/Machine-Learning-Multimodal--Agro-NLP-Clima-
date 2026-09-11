# CONTEXTO SESION 2026-09-10 CIERRE GC3/GE

Este handoff consolida el estado real actual para continuar en una nueva sesion
de Codex. No se debe reinterpretar metodologia ya cerrada ni convertir pilotos
tecnicos en resultados experimentales oficiales.

## 1. Git actual

Branch actual:

```text
v2-reentrenamiento
```

HEAD actual:

```text
bfd19a7c9de7fd5f1acbc8144429169dc3c4000e
```

Resumen de estado:

```text
Hay cambios tracked preexistentes en DECISIONES_METODOLOGICAS.md, datasets v2,
notebooks fase2 y eliminaciones de scalers antiguos.
Hay multiples archivos/directorios untracked, incluyendo auditorias, src v2,
scalers v2c, technical_pilot y technical_pilot_scaled.
No se hizo commit.
```

`git status --short` reporto advertencias de permiso al leer
`C:\Users\ADMIN/.config/git/ignore`, pero el estado fue obtenido.

`git diff --stat` tracked actual:

```text
17 files changed, 2233 insertions(+), 3962 deletions(-)
```

Nota: los artefactos untracked no aparecen en `git diff --stat`.

## 2. CERRADO - Decisiones metodologicas GC3/GE

Fuente normativa vigente:

```text
v2_reentrenamiento/DECISIONES_METODOLOGICAS.md
```

Decisiones cerradas relevantes:

```text
D0      reproducibilidad controlada
D3-op   uso de validation 2024 solo para control del entrenamiento
D4      arquitectura neuronal GC3/GE
D4-loss loss de entrenamiento GC3/GE
D7      shuffle=False
D10     multi-seed GC3/GE
D35-a   metricas globales
D35-b   shocks
D36     diseno de ramas GC3/GE y ablacion NLP
```

No reabrir ni optimizar estas decisiones sin autorizacion externa explicita.

## 3. CERRADO - Arquitectura oficial

GC3 y GE usan la misma arquitectura:

```text
Rama A:
Input A
-> LSTM(16, return_sequences=True)
-> BahdanauAttention(16)

Rama B:
Input B
-> LSTM(16, return_sequences=True)
-> BahdanauAttention(16)

Fusion:
Concatenate(context_A, context_B)
-> Dense(16, activation="relu")
-> Dropout(0.20)
-> Dense(8, activation="relu")
-> Dense(1)
```

Restricciones cerradas:

```text
L2 = 0
Adam learning_rate = 0.001
batch_size = 8
max_epochs = 300
shuffle = False
No stacked LSTM
No bidirectional LSTM
No BatchNorm/LayerNorm agregado
No nuevos hiperparametros
```

Conteos oficiales:

```text
GC3 = 6273 parametros
GE  = 6657 parametros
```

## 4. CERRADO - Features exactas

Rama A para GC3 y GE, 4 inputs:

```text
produccion_t_{cultivar}
produccion_t_{cultivar}_lag1
produccion_t_{cultivar}_lag3
produccion_t_{cultivar}_lag6
```

Rama B GC3, 33 inputs:

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

Rama B GE = Rama B GC3 + 6 NLP rezagadas:

```text
avg_sentiment_lag1
avg_sentiment_lag3
avg_sentiment_lag6
n_noticias_lag1
n_noticias_lag3
n_noticias_lag6
```

Totales:

```text
GC3 = A4 + B33 = 37 inputs
GE  = A4 + B39 = 43 inputs
```

## 5. CERRADO - Split y alineamiento temporal

Split oficial:

```text
TRAIN      2016-01..2023-12 raw
VALIDATION 2024-01..2024-12
TEST       2025-01..2025-12
2026 excluido
```

Luego de lags:

```text
TRAIN feature rows efectivas = 90, 2016-07..2023-12
lookback = 6
TRAIN sequences = 84
VAL targets = 12
TEST targets = 12
```

Regla predictiva:

```text
secuencia que termina en mes t -> predice y_{t+1}
```

Targets esperados:

```text
Primer target TRAIN = 2017-01
Ultimo target TRAIN = 2023-12
Primer target VAL   = 2024-01
Primer target TEST  = 2025-01
```

La fila t puede contener `produccion_t_{cultivar}` porque se conoce al origen
del pronostico. No debe entrar `produccion_t+1` como input.

## 6. CERRADO - D0 reproducibilidad

Definicion cerrada:

```text
reproducibilidad controlada dentro de un entorno software/hardware congelado
```

No se promete identidad bit-a-bit universal entre hardware, sistemas
operativos, drivers o versiones distintas.

Infraestructura D0:

```text
configure_determinism(seed)
PYTHONHASHSEED
Python random
NumPy
TensorFlow/Keras
keras.utils.set_random_seed(seed)
tf.config.experimental.enable_op_determinism()
TF_DETERMINISTIC_OPS=1 cuando corresponde
TF_ENABLE_ONEDNN_OPTS=0 como medida conservadora CPU v2
```

Las variables que deben actuar antes de inicializar TensorFlow deben fijarse
antes del import/inicializacion de TensorFlow.

## 7. CERRADO - D10 multi-seed

Seeds oficiales GC3/GE:

```text
0,1,2,3,4,5,6,7,8,9
```

Total:

```text
10 seeds
```

Regla:

```text
GC3(seed=s) <-> GE(seed=s), para s=0..9
```

No seleccionar seed ganadora. No descartar seeds por bajo rendimiento salvo
fallo tecnico verificable y documentado. Las mismas seeds no implican
inicializaciones numericamente identicas entre GC3 y GE porque sus dimensiones
de entrada difieren.

## 8. CERRADO - D4-loss

Compilacion oficial:

```text
optimizer = Adam(learning_rate=0.001)
loss = "mse"
metrics = ["mae"]
```

La loss MSE optimiza pesos durante entrenamiento. MAE continua como metrica
primaria de evaluacion fuera de muestra segun D35-a.

## 9. CERRADO - Callbacks y shuffle

EarlyStopping:

```text
monitor = "val_loss"
patience = 15
restore_best_weights = True
```

ReduceLROnPlateau:

```text
monitor = "val_loss"
factor = 0.5
patience = 8
min_lr = 1e-6
```

`val_loss` = MSE sobre validation 2024. `shuffle=False`.

Validation 2024 se usa solo para EarlyStopping, ReduceLROnPlateau,
restauracion del mejor estado y diagnostico de convergencia/generalizacion.
No se usa para elegir arquitectura, loss, features, seeds o hiperparametros.

## 10. VERIFICADO RUNTIME - Entorno

Entorno runtime auditado:

```text
Python      3.11.9
TensorFlow  2.21.0
Keras       3.14.0
NumPy       2.4.4
Pandas      3.0.2
Dispositivo CPU
GPU TensorFlow no disponible en Windows nativo para esta instalacion
```

Ruta de ejecucion usada:

```text
venv\Scripts\python.exe
```

## 11. VERIFICADO RUNTIME - Auditoria de modelos

Informe:

```text
v2_reentrenamiento/auditorias/AUDITORIA_ENTORNO_RUNTIME_GC3_GE.md
```

Resultado:

```text
GC3 count_params runtime = 6273
GE  count_params runtime = 6657
Forward pass sintetico GC3 correcto
Forward pass sintetico GE correcto
Compile MSE/MAE correcto
Callbacks construidos correctamente
No se ejecuto entrenamiento en la auditoria runtime
No se uso TEST
```

## 12. DESCARTADO - Primer piloto raw

Primer piloto tecnico:

```text
TECHNICAL_PILOT / Sutil / GC3 / seed_00
```

Artefactos:

```text
v2_reentrenamiento/resultados_v2_final/technical_pilot/sutil/GC3/seed_00/
v2_reentrenamiento/auditorias/AUDITORIA_PILOTO_TECNICO_GC3_SUTIL_SEED0.md
v2_reentrenamiento/auditorias/AUDITORIA_POST_PILOTO_ESCALA_TARGET.md
```

Fue tecnicamente util para verificar end-to-end `model.fit`, callbacks,
history, checkpoint y metadata. Sin embargo, queda DESCARTADO como corrida
oficial porque uso `master_dataset_sutil_v2_features.csv` raw y no aplico el
escalado previsto para inputs/target.

Resumen tecnico del piloto raw:

```text
epochs = 294
best_epoch = 279
stopped_epoch = 294
ReduceLROnPlateau epoch = 287
best val_loss raw = 94221704.0
fit_seconds = 33.079141
```

No reutilizar pesos, checkpoint, history ni metricas del piloto raw.

## 13. VERIFICADO RUNTIME - Auditoria de scalers

Informe:

```text
v2_reentrenamiento/auditorias/AUDITORIA_SCALERS_PIPELINE_GC3_GE.md
```

Scalers v2c:

```text
scaler_sutil_v2c.joblib
scaler_dulce_v2c.joblib
```

Clase:

```text
sklearn.preprocessing._data.StandardScaler
```

Estado auditado:

```text
n_features_in_ = 44
n_samples_seen_ = 90
fit exclusivamente TRAIN efectivo 2016-07..2023-12
VAL 2024 no participa en fit
TEST 2025 no participa en fit
```

Columnas no escaladas:

```text
año
mes
mes_sin
mes_cos
```

El target futuro `y_{t+1}` se deriva de la misma columna
`produccion_t_{cultivar}` en la fila target posterior a la ventana. No existe
un target futuro explicito separado.

## 14. CERRADO - Pipeline oficial de datos GC3/GE

Fuente oficial para entrenamiento neuronal:

```text
v2_reentrenamiento/data/processed/master_dataset_{cultivar}_v2_escalado.csv
```

Reglas:

```text
NO volver a aplicar scaler sobre `_escalado.csv`
NO recalcular scalers
NO fit de scaler por seed
NO fit de scaler por modelo
```

Uso de scaler joblib/parametros:

```text
metadata
hashes
inverse transform futuro de y_pred/y_real a toneladas
auditoria de escala
```

Inverse transform del target:

```text
y_toneladas = y_scaled * scale_train(produccion_t_{cultivar}) + mean_train(produccion_t_{cultivar})
```

## 15. VERIFICADO RUNTIME - Segundo TECHNICAL_PILOT escalado

Piloto ejecutado:

```text
TECHNICAL_PILOT_SCALED / Sutil / GC3 / seed_00
```

Ruta:

```text
v2_reentrenamiento/resultados_v2_final/technical_pilot_scaled/sutil/GC3/seed_00/
```

Informe:

```text
v2_reentrenamiento/auditorias/AUDITORIA_PILOTO_TECNICO_ESCALADO_GC3_SUTIL_SEED0.md
```

Runner:

```text
v2_reentrenamiento/auditorias/ejecutar_piloto_tecnico_escalado_gc3_sutil_seed0.py
```

Pre-flight verificado:

```text
GC3 params = 6273
TRAIN sequences = 84
VAL targets = 12
Xa_train = (84, 6, 4)
Xb_train = (84, 6, 33)
y_train = (84,)
target y_train/y_val en escala StandardScaler
produccion_t_sutil de Rama A escalada
mes_sin/mes_cos raw
resto de Rama B escalada
no se aplico scaler nuevamente
```

Resultado tecnico:

```text
epochs ejecutadas = 85
best_epoch = 70
stopped_epoch = 85
ReduceLROnPlateau epoch = 78
best val_loss = 0.5792437195777893
final val_loss tras restore_best_weights = 0.5792436599731445
restore_best_weights verificado = True
fit_seconds = 11.020983799999158
seconds_per_epoch = 0.12965863294116656
```

Rangos target:

```text
y_train escalado = -2.310547..2.292568
y_val escalado   =  0.047167..2.577853
```

Comparacion tecnica con raw:

```text
raw y_train = 4851.691000..41867.719000
raw y_val   = 23811.288000..44161.843000
raw best val_loss = 94221704.0
scaled best val_loss = 0.5792437195777893
raw epochs = 294
scaled epochs = 85
```

Esta comparacion es solo tecnica de escala, magnitud de loss, tiempo y
callbacks. No es comparacion cientifica de rendimiento.

## 16. VERIFICADO - TEST no usado

En el piloto escalado:

```text
TEST cargado en objeto de secuencias = False
TEST usado = False
No se generaron predicciones TEST
No se calcularon metricas TEST
```

TEST 2025 permanece sin evaluacion neuronal oficial.

## 17. Artefactos creados hoy

Principales artefactos de la etapa GC3/GE reciente:

```text
v2_reentrenamiento/src/models/dual_lstm_attention.py
v2_reentrenamiento/src/models/features_gc3_ge.py
v2_reentrenamiento/src/training/
v2_reentrenamiento/src/evaluation/
v2_reentrenamiento/auditorias/AUDITORIA_IMPLEMENTACION_TECNICA_GC3_GE.md
v2_reentrenamiento/auditorias/AUDITORIA_ENTORNO_RUNTIME_GC3_GE.md
v2_reentrenamiento/auditorias/verificar_runtime_gc3_ge.py
v2_reentrenamiento/auditorias/AUDITORIA_PILOTO_TECNICO_GC3_SUTIL_SEED0.md
v2_reentrenamiento/auditorias/AUDITORIA_POST_PILOTO_ESCALA_TARGET.md
v2_reentrenamiento/auditorias/AUDITORIA_SCALERS_PIPELINE_GC3_GE.md
v2_reentrenamiento/auditorias/ejecutar_piloto_tecnico_gc3_sutil_seed0.py
v2_reentrenamiento/auditorias/ejecutar_piloto_tecnico_escalado_gc3_sutil_seed0.py
v2_reentrenamiento/auditorias/AUDITORIA_PILOTO_TECNICO_ESCALADO_GC3_SUTIL_SEED0.md
v2_reentrenamiento/resultados_v2_final/technical_pilot/
v2_reentrenamiento/resultados_v2_final/technical_pilot_scaled/
```

El piloto raw anterior permanece separado y descartado. El piloto escalado es
tecnico y no constituye resultado experimental oficial.

## 18. CERRADO - D35 para evaluacion final

D35-a:

```text
MAE primaria
RMSE complementaria
RelMAE_N1 comparacion directa contra Naive t-1
MASE_1 complementaria escalada
RMSSE_1 complementaria escalada
R2 descriptiva secundaria
```

La evaluacion final debe hacerse en toneladas despues de inverse transform
cuando corresponda.

D35-b:

```text
P75 Sutil = 0.240834900212216
P75 Dulce = 0.320281354618397
shock_t = 1 si r_t > P75_c
```

Meses shock oficiales TEST 2025:

```text
Sutil: 2025-01, 2025-07, 2025-11
Dulce: 2025-01, 2025-02, 2025-03
n_shock = 3 por cultivar
```

Metricas condicionales previstas:

```text
MAE_global
MAE_shock
MAE_nonshock
n_shock
n_nonshock
Delta_s
```

No calcular metricas TEST hasta ejecutar las corridas oficiales y reconstruir
predicciones oficiales en toneladas.

## 19. VERIFICADO RUNTIME - Estado GC3/GE

Estado actual:

```text
GC3/GE tecnicamente listo para preparar la etapa de entrenamiento oficial.
Infraestructura creada.
Runtime TensorFlow verificado.
Conteos runtime verificados.
Pipeline escalado corregido para piloto tecnico.
Piloto escalado completo end-to-end sin usar TEST.
```

Importante:

```text
No se ejecutaron las 40 corridas oficiales.
No existe aun resultado experimental oficial GC3/GE.
El seed 0 de Sutil/GC3 debera reentrenarse desde cero en la corrida oficial.
No reutilizar checkpoints/history/metricas de pilotos.
```

## 20. PENDIENTE

Pendientes principales:

```text
Preparar ejecucion oficial 40 corridas:
  Sutil GC3 seeds 0..9
  Sutil GE seeds 0..9
  Dulce GC3 seeds 0..9
  Dulce GE seeds 0..9

Consolidar resultados multi-seed:
  resultados individuales por seed
  media
  mediana
  desviacion estandar
  minimo
  maximo

Generar predicciones oficiales y evaluar D35:
  inverse transform a toneladas
  metricas globales
  metricas shock/nonshock
  Delta_s
  graficos de entrenamiento y evaluacion

GC2/XGBoost:
  todavia requiere protocolo propio antes de comparacion final.
  decidir si aplica multi-seed segun configuracion final estocastica.

PPI:
  actualizar despues de cerrar experimentacion.
```

No inventar ni extrapolar resultados oficiales todavia.

## 21. Comandos de control ejecutados

Al cierre de esta sesion se ejecuto:

```text
git status --short
git diff --stat
```

No se hizo commit.
