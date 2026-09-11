# AUDITORIA PILOTO TECNICO ESCALADO GC3 SUTIL SEED0

## 1. Alcance

Auditoria del segundo piloto tecnico GC3/Sutil/seed0, ejecutado como
`TECHNICAL_PILOT_SCALED` y separado de resultados oficiales. La unica
correccion aplicada fue leer `master_dataset_sutil_v2_escalado.csv` como
fuente de entrenamiento. No se modificaron arquitectura, features, split,
loss, callbacks, seeds, scalers ni decisiones metodologicas.

## 2. Fuente de datos

- Dataset usado por model.fit: `C:\Machine-learming\Machine-Learning-Multimodal--Agro-NLP-Clima-\v2_reentrenamiento\data\processed\master_dataset_sutil_v2_escalado.csv`
- Scaler usado solo para metadata/inverse transform: `C:\Machine-learming\Machine-Learning-Multimodal--Agro-NLP-Clima-\v2_reentrenamiento\resultados_v2_final\scalers\scaler_sutil_v2c.joblib`
- Se aplico scaler nuevamente sobre `_escalado.csv`: False
- TEST cargado en el objeto de secuencias: False
- TEST usado: False

## 3. Pre-flight

- Parametros GC3 esperados/obtenidos: 6273 / 6273
- Loss: mse
- Optimizer: Adam
- Learning rate: 0.0010000000474974513
- Shuffle: False
- Callbacks: EarlyStopping, ReduceLROnPlateau, ModelCheckpoint y logger tecnico de learning rate.

## 4. Shapes

| Tensor | Shape |
|---|---:|
| Xa_train | (84, 6, 4) |
| Xb_train | (84, 6, 33) |
| y_train | (84,) |
| Xa_val | (12, 6, 4) |
| Xb_val | (12, 6, 33) |
| y_val | (12,) |

## 5. Escala del target

| Split | min | max | mean | std pop |
|---|---:|---:|---:|---:|
| TRAIN y escalado | -2.310547324493 | 2.292568023632 | 0.035721106047 | 1.024248432865 |
| VAL y escalado | 0.047166797210 | 2.577853019496 | 1.091432329128 | 1.017955523744 |

El target entregado a `model.fit()` esta en escala StandardScaler y se deriva
de `produccion_t_sutil` en la fila t+1.

## 6. Verificaciones de escalado

- `produccion_t_sutil` de Rama A escalada: True
- `mes_sin`/`mes_cos` permanecen raw: True
- Resto de Rama B escalada: True
- max abs diff `_escalado.csv` vs formula del scaler congelado: 3.553e-15

## 7. Ejemplos t -> t+1

| fecha_final_secuencia | produccion_t raw | produccion_t scaled | fecha_target | target raw | target scaled | inverse(target) |
|---|---:|---:|---|---:|---:|---:|
| 2016-12-01 | 16714.142000 | -0.835396336805 | 2017-01-01 | 18255.799000 | -0.643684114245 | 18255.799000 |
| 2020-06-01 | 26948.624000 | 0.437309129440 | 2020-07-01 | 15387.758000 | -1.000338359972 | 15387.758000 |
| 2023-11-01 | 24158.060000 | 0.090289509228 | 2023-12-01 | 30799.027000 | 0.916124661614 | 30799.027000 |

## 8. Ejecucion model.fit

- Epochs ejecutadas: 85
- Best epoch: 70
- Stopped epoch: 85
- Best val_loss: 0.5792437195777893
- Final val_loss tras restore_best_weights: 0.5792436599731445
- Restore best weights verificado: True
- ReduceLROnPlateau epochs: [78]

## 9. History

- Ruta: `C:\Machine-learming\Machine-Learning-Multimodal--Agro-NLP-Clima-\v2_reentrenamiento\resultados_v2_final\technical_pilot_scaled\sutil\GC3\seed_00\history.csv`
- Filas: 85
- NaN en columnas numericas: 0
- Inf en columnas numericas: 0
- Columnas: epoch, loss, val_loss, mae, val_mae, learning_rate

## 10. Graficos

- TRAIN vs VAL MSE: `C:\Machine-learming\Machine-Learning-Multimodal--Agro-NLP-Clima-\v2_reentrenamiento\resultados_v2_final\technical_pilot_scaled\sutil\GC3\seed_00\figures\pilot_loss_mse.png`
- TRAIN vs VAL MAE: `C:\Machine-learming\Machine-Learning-Multimodal--Agro-NLP-Clima-\v2_reentrenamiento\resultados_v2_final\technical_pilot_scaled\sutil\GC3\seed_00\figures\pilot_metric_mae.png`

Las figuras usan solo el history del piloto escalado e incluyen marcas de
`best_epoch` y `stopped_epoch`.

## 11. Checkpoint y metadata

- Checkpoint: `C:\Machine-learming\Machine-Learning-Multimodal--Agro-NLP-Clima-\v2_reentrenamiento\resultados_v2_final\technical_pilot_scaled\sutil\GC3\seed_00\checkpoint_best.keras`
- Metadata: `C:\Machine-learming\Machine-Learning-Multimodal--Agro-NLP-Clima-\v2_reentrenamiento\resultados_v2_final\technical_pilot_scaled\sutil\GC3\seed_00\metadata.json`
- Config: `C:\Machine-learming\Machine-Learning-Multimodal--Agro-NLP-Clima-\v2_reentrenamiento\resultados_v2_final\technical_pilot_scaled\sutil\GC3\seed_00\config.json`
- Log: `C:\Machine-learming\Machine-Learning-Multimodal--Agro-NLP-Clima-\v2_reentrenamiento\resultados_v2_final\technical_pilot_scaled\sutil\GC3\seed_00\logs\pilot_run_summary.json`

Estos artefactos pertenecen al piloto tecnico y no deben reutilizarse en las
corridas oficiales.

## 12. Tiempo

- Tiempo total fit: 11.020984 segundos
- Segundos/epoch: 0.129659
- Estimacion operativa 40 corridas: 7.347 minutos

La estimacion es solo una extrapolacion operativa del piloto. EarlyStopping
puede producir duraciones distintas entre modelos/seeds.

## 13. Comparacion tecnica con piloto raw

| Aspecto | Piloto raw | Piloto escalado |
|---|---:|---:|
| y_train min/max | 4851.691000..41867.719000 | -2.310547..2.292568 |
| y_val min/max | 23811.288000..44161.843000 | 0.047167..2.577853 |
| best val_loss | 94221704.0 | 0.5792437195777893 |
| tiempo fit segundos | 33.07914100000016 | 11.020983799999158 |
| epochs | 294 | 85 |
| ReduceLR epochs | [287] | [78] |

Comparacion estrictamente tecnica de escala, magnitud de loss, tiempo y
callbacks. No constituye comparacion de rendimiento cientifico.

## 14. Estado final

El piloto escalado completo datos -> secuencias -> factory -> compile -> fit
-> validation -> callbacks -> history -> checkpoint -> metadata usando
`master_dataset_sutil_v2_escalado.csv`. TEST 2025 no fue usado.
