# AUDITORIA SCALERS PIPELINE GC3/GE

## 1. Alcance

Auditoria de los scalers v2c y del pipeline de escalado requerido para GC3/GE.
No se entrenaron modelos, no se ejecuto `model.fit()`, no se ejecutaron seeds,
no se uso test 2025 para evaluacion, no se hizo HPO y no se modificaron
decisiones metodologicas.

Archivos inspeccionados:

```text
v2_reentrenamiento/resultados_v2_final/scalers/scaler_sutil_v2c.joblib
v2_reentrenamiento/resultados_v2_final/scalers/scaler_sutil_v2c_parametros.csv
v2_reentrenamiento/resultados_v2_final/scalers/scaler_dulce_v2c.joblib
v2_reentrenamiento/resultados_v2_final/scalers/scaler_dulce_v2c_parametros.csv
v2_reentrenamiento/data/processed/master_dataset_sutil_v2_features.csv
v2_reentrenamiento/data/processed/master_dataset_sutil_v2_escalado.csv
v2_reentrenamiento/data/processed/master_dataset_dulce_v2_features.csv
v2_reentrenamiento/data/processed/master_dataset_dulce_v2_escalado.csv
v2_reentrenamiento/src/features/escalado.py
```

## 2. Origen del escalado v2c

El modulo `v2_reentrenamiento/src/features/escalado.py` define:

```text
EscaladorTrain
StandardScaler
TRAIN 2016-07..2023-12, n=90
VAL   2024-01..2024-12, n=12
TEST  2025-01..2025-12, n=12
```

Columnas excluidas del StandardScaler:

```text
año
mes
mes_sin
mes_cos
```

El codigo ajusta:

```text
self.scaler.fit(partes["train"][self.columnas])
```

y luego aplica:

```text
self.scaler.transform(...)
```

a TRAIN, VAL y TEST. El codigo contiene un assert:

```text
n_samples_seen_ == 90
```

## 3. Scaler Sutil v2c

Clase exacta:

```text
sklearn.preprocessing._data.StandardScaler
```

Atributos auditados:

| Atributo | Valor |
|---|---:|
| `n_features_in_` | 44 |
| `n_samples_seen_` | 90 |
| `with_mean` | True |
| `with_std` | True |
| `feature_names_in_` coincide con CSV parametros | True |

El scaler incluye `produccion_t_sutil`.

No existe una columna target futura explicita. El target `y_{t+1}` se deriva
de la misma columna `produccion_t_sutil`, tomada de la fila target posterior a
la ventana de contexto.

Parametros de `produccion_t_sutil`:

| mean_ | scale_ | var_ |
|---:|---:|---:|
| 23431.995422222222 | 8041.516494927326 | 64665987.53818826 |

Pruebas de fit TRAIN-only:

| Verificacion | Resultado |
|---|---:|
| Filas raw/escalado | 114 / 114 |
| TRAIN/VAL/TEST | 90 / 12 / 12 |
| Rango TRAIN | 2016-07-01..2023-12-01 |
| Rango VAL | 2024-01-01..2024-12-01 |
| Rango TEST | 2025-01-01..2025-12-01 |
| max abs(mean_train_raw - mean_param) | 2.27e-13 |
| max abs(var_train_raw - var_param) | 7.45e-09 |
| max abs(mean_all_rows_raw - mean_param) | 1816.110288 |
| max abs(escalado_csv - formula) | 3.55e-15 |
| max abs mean TRAIN escalado | 4.93e-15 |
| std TRAIN escalado min/max | 1.0 / 1.0 |

Conclusion Sutil: el scaler fue ajustado sobre TRAIN efectivo 2016-07..2023-12
(90 filas) y no sobre las 114 filas completas. VAL y TEST fueron solo
transformados.

## 4. Scaler Dulce v2c

Clase exacta:

```text
sklearn.preprocessing._data.StandardScaler
```

Atributos auditados:

| Atributo | Valor |
|---|---:|
| `n_features_in_` | 44 |
| `n_samples_seen_` | 90 |
| `with_mean` | True |
| `with_std` | True |
| `feature_names_in_` coincide con CSV parametros | True |

El scaler incluye `produccion_t_dulce`.

No existe una columna target futura explicita. El target `y_{t+1}` se deriva
de la misma columna `produccion_t_dulce`, tomada de la fila target posterior a
la ventana de contexto.

Parametros de `produccion_t_dulce`:

| mean_ | scale_ | var_ |
|---:|---:|---:|
| 391.6001111111111 | 144.20332113377236 | 20794.59782600988 |

Pruebas de fit TRAIN-only:

| Verificacion | Resultado |
|---|---:|
| Filas raw/escalado | 114 / 114 |
| TRAIN/VAL/TEST | 90 / 12 / 12 |
| Rango TRAIN | 2016-07-01..2023-12-01 |
| Rango VAL | 2024-01-01..2024-12-01 |
| Rango TEST | 2025-01-01..2025-12-01 |
| max abs(mean_train_raw - mean_param) | 3.55e-15 |
| max abs(var_train_raw - var_param) | 3.55e-15 |
| max abs(mean_all_rows_raw - mean_param) | 66.948229 |
| max abs(escalado_csv - formula) | 4.00e-15 |
| max abs mean TRAIN escalado | 5.66e-15 |
| std TRAIN escalado min/max | 1.0 / 1.0 |

Conclusion Dulce: el scaler fue ajustado sobre TRAIN efectivo
2016-07..2023-12 (90 filas) y no sobre las 114 filas completas. VAL y TEST
fueron solo transformados.

## 5. Orden exacto de columnas escaladas

Sutil:

```text
produccion_t_sutil
t_index
produccion_t_sutil_lag1
produccion_t_sutil_lag3
produccion_t_sutil_lag6
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
total_afectados_lag1
total_afectados_lag3
total_afectados_lag6
hectareas_cultivo_perdidas_lag1
hectareas_cultivo_perdidas_lag3
hectareas_cultivo_perdidas_lag6
hectareas_cultivo_afectadas_lag1
hectareas_cultivo_afectadas_lag3
hectareas_cultivo_afectadas_lag6
avg_sentiment_lag1
avg_sentiment_lag3
avg_sentiment_lag6
n_noticias_lag1
n_noticias_lag3
n_noticias_lag6
```

Dulce usa el mismo orden, reemplazando las columnas de produccion por:

```text
produccion_t_dulce
produccion_t_dulce_lag1
produccion_t_dulce_lag3
produccion_t_dulce_lag6
```

Nota: el scaler incluye `total_afectados_lag1/lag3/lag6` porque esas columnas
existen en el dataset escalado general, pero GC3/GE no las usan por exclusion
metodologica.

## 6. Auditoria de `_escalado.csv`

Columnas no escaladas:

```text
año
mes
mes_sin
mes_cos
```

Todas las demas 44 columnas fueron escaladas por `StandardScaler`.

Los archivos `_escalado.csv` de Sutil y Dulce:

- tienen las mismas 114 fechas que los `_features.csv`;
- conservan TRAIN/VAL/TEST como 90/12/12;
- reproducen exactamente la formula `(raw - mean_train) / scale_train`;
- no muestran evidencia de ajuste con 2024 o 2025.

## 7. Target operacional

Para una secuencia que termina en mes `t`:

```text
Rama A: produccion_t_{cultivar} y lags, todos de la ventana hasta t.
Target: produccion_t_{cultivar} de la fila t+1.
```

El target operativo correcto debe ser el valor estandarizado de
`produccion_t_{cultivar}` en la fila `t+1`, usando el scaler v2c del cultivar.
No hay que crear un scaler nuevo.

Inverse transform del target:

```text
y_toneladas = y_scaled * scale_train(produccion_t_c) + mean_train(produccion_t_c)
```

Para Sutil:

```text
y_toneladas = y_scaled * 8041.516494927326 + 23431.995422222222
```

Para Dulce:

```text
y_toneladas = y_scaled * 144.20332113377236 + 391.6001111111111
```

## 8. Secuencias escaladas sin fit

Se reconstruyeron secuencias desde `_escalado.csv`, sin `model.fit()`.

Sutil GC3:

| Split | Xa | Xb | y | y mean | y std pop | y min | y max |
|---|---:|---:|---:|---:|---:|---:|---:|
| TRAIN | (84,6,4) | (84,6,33) | (84,) | 0.035721 | 1.024248 | -2.310547 | 2.292568 |
| VAL | (12,6,4) | (12,6,33) | (12,) | 1.091432 | 1.017956 | 0.047167 | 2.577853 |
| TEST | (12,6,4) | (12,6,33) | (12,) | 1.054064 | 1.083834 | -0.673358 | 2.323909 |

Sutil GE:

```text
TRAIN Xa=(84,6,4), Xb=(84,6,39), y=(84,)
VAL   Xa=(12,6,4), Xb=(12,6,39), y=(12,)
TEST  Xa=(12,6,4), Xb=(12,6,39), y=(12,)
```

Dulce GC3:

| Split | Xa | Xb | y | y mean | y std pop | y min | y max |
|---|---:|---:|---:|---:|---:|---:|---:|
| TRAIN | (84,6,4) | (84,6,33) | (84,) | 0.050502 | 0.996875 | -1.572087 | 1.883104 |
| VAL | (12,6,4) | (12,6,33) | (12,) | 0.393127 | 1.071434 | -1.124801 | 1.970814 |
| TEST | (12,6,4) | (12,6,33) | (12,) | 0.528420 | 1.176974 | -1.130002 | 2.349668 |

Dulce GE:

```text
TRAIN Xa=(84,6,4), Xb=(84,6,39), y=(84,)
VAL   Xa=(12,6,4), Xb=(12,6,39), y=(12,)
TEST  Xa=(12,6,4), Xb=(12,6,39), y=(12,)
```

## 9. Ejemplos `t -> t+1` e inverse transform

Sutil:

| Caso | fin secuencia | prod_t raw | prod_t scaled | target fecha | target raw | target scaled | inverse error |
|---|---|---:|---:|---|---:|---:|---:|
| Primer TRAIN | 2016-12-01 | 16714.142 | -0.835396 | 2017-01-01 | 18255.799 | -0.643684 | 0.0 |
| Intermedio | 2020-06-01 | 26948.624 | 0.437309 | 2020-07-01 | 15387.758 | -1.000338 | 0.0 |
| Ultimo TRAIN | 2023-11-01 | 24158.060 | 0.090290 | 2023-12-01 | 30799.027 | 0.916125 | 0.0 |

Dulce:

| Caso | fin secuencia | prod_t raw | prod_t scaled | target fecha | target raw | target scaled | inverse error |
|---|---|---:|---:|---|---:|---:|---:|
| Primer TRAIN | 2016-12-01 | 135.700 | -1.774578 | 2017-01-01 | 256.050 | -0.939993 | 0.0 |
| Intermedio | 2020-06-01 | 541.168 | 1.037201 | 2020-07-01 | 426.159 | 0.239654 | 0.0 |
| Ultimo TRAIN | 2023-11-01 | 206.380 | -1.284437 | 2023-12-01 | 224.300 | -1.160168 | 2.84e-14 |

## 10. Rama A / Rama B

GC3 Sutil:

- Rama A: 4/4 SCALED.
- Rama B: `mes_sin` y `mes_cos` RAW; las otras 31 features SCALED.

GC3 Dulce:

- Rama A: 4/4 SCALED.
- Rama B: `mes_sin` y `mes_cos` RAW; las otras 31 features SCALED.

GE Sutil:

- Rama A: 4/4 SCALED.
- Rama B: `mes_sin` y `mes_cos` RAW; las otras 37 features SCALED, incluidas
  las 6 NLP lagged.

GE Dulce:

- Rama A: 4/4 SCALED.
- Rama B: `mes_sin` y `mes_cos` RAW; las otras 37 features SCALED, incluidas
  las 6 NLP lagged.

En este contexto, `RAW` para `mes_sin`/`mes_cos` no indica fuga ni mezcla
accidental: son variables ciclicas ya acotadas y excluidas intencionalmente del
StandardScaler.

## 11. Leakage

Verificaciones:

| Riesgo | Resultado |
|---|---|
| Scaler fit con VAL | NO |
| Scaler fit con TEST | NO |
| `n_samples_seen_` | 90 |
| VAL solo transform | SI |
| TEST solo transform | SI |
| Recalculo por seed | NO |
| Recalculo por modelo | NO |
| Mismo scaler por cultivar para GC3/GE | SI |

El scaler congelado por cultivar debe reutilizarse en todas las seeds GC3/GE.

## 12. Correccion minima propuesta

Correccion minima recomendada:

```text
A) leer directamente master_dataset_{cultivar}_v2_escalado.csv
```

Motivo:

- `_escalado.csv` ya es la fuente materializada y auditada del scaler v2c;
- evita doble escalado accidental;
- conserva `mes_sin` y `mes_cos` sin escalar como fue definido;
- permite construir `X_a`, `X_b` e `y` directamente en la escala que espera la
  red;
- el `.joblib` o `*_parametros.csv` quedan para metadata e inverse transform
  del target.

No se implemento esta correccion en esta auditoria. La siguiente repeticion de
`TECHNICAL_PILOT` deberia hacerse despues de modificar el runner para leer
`_escalado.csv` o una funcion central equivalente.

Opcion B, leer raw y aplicar scaler en runtime, tambien es tecnicamente valida,
pero es mas propensa a doble escalado o a diferencias de orden de columnas. No
se recomienda mientras exista `_escalado.csv` ya auditado.

## 13. Respuestas finales

1. **Que scaler usa Sutil?**
   `scaler_sutil_v2c.joblib`, clase `sklearn.preprocessing._data.StandardScaler`,
   44 features, `n_samples_seen_=90`.

2. **Fue fit solo con TRAIN?**
   Si. Los parametros coinciden con TRAIN 2016-07..2023-12 y no con las 114
   filas completas.

3. **Que scaler usa Dulce?**
   `scaler_dulce_v2c.joblib`, clase `sklearn.preprocessing._data.StandardScaler`,
   44 features, `n_samples_seen_=90`.

4. **Fue fit solo con TRAIN?**
   Si. Los parametros coinciden con TRAIN 2016-07..2023-12 y no con las 114
   filas completas.

5. **`produccion_t` se escala?**
   Si. `produccion_t_sutil` y `produccion_t_dulce` estan incluidos en sus
   respectivos scalers.

6. **`y_{t+1}` se escala?**
   Debe escalarse usando el valor ya escalado de `produccion_t_{cultivar}` en
   la fila target `t+1`.

7. **Como se hace inverse_transform?**
   Para predicciones del target: `y_toneladas = y_scaled * scale_train +
   mean_train` usando los parametros de `produccion_t_{cultivar}`.

8. **Rama A queda completamente en la escala prevista?**
   Si, si se lee `_escalado.csv`: las 4 features de Rama A quedan SCALED.

9. **Rama B queda completamente en la escala prevista?**
   Si: `mes_sin`/`mes_cos` quedan RAW por diseno; el resto queda SCALED.

10. **VAL participa en fit del scaler?**
    NO.

11. **TEST participa en fit del scaler?**
    NO.

12. **El piloto anterior queda invalidado como corrida oficial?**
    SI. Entreno con raw `features.csv`, no con la escala v2c prevista.

13. **El pipeline esta listo para repetir TECHNICAL_PILOT?**
    Conceptualmente si, pero el runner debe corregirse primero para leer
    `_escalado.csv` o aplicar el scaler congelado exactamente una vez. No debe
    repetirse el piloto oficial-tecnico hasta aplicar esa correccion.
