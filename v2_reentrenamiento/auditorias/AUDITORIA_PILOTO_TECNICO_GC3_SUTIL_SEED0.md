# AUDITORIA PILOTO TECNICO GC3 SUTIL SEED0

## 1. Alcance

Se ejecuto una unica corrida tecnica piloto:

```text
cultivar = sutil
modelo = GC3
seed = 0
etiqueta = TECHNICAL_PILOT
```

Esta corrida no constituye resultado experimental oficial. Su objetivo fue
verificar el flujo tecnico:

```text
datos -> secuencias -> model factory -> compile -> fit -> validation
-> callbacks -> history -> checkpoint -> metadata
```

No se ejecuto GE, no se ejecutaron las 10 seeds, no se lanzaron las 40
corridas oficiales, no se evaluo test 2025, no se generaron predicciones test,
no se calcularon metricas test, no se hizo HPO y no se modificaron decisiones
metodologicas.

Script usado:

```text
v2_reentrenamiento/auditorias/ejecutar_piloto_tecnico_gc3_sutil_seed0.py
```

## 2. Pre-flight

Pre-flight verificado antes de `model.fit()`:

| Verificacion | Resultado |
|---|---|
| Modelo | GC3 |
| Cultivar | Sutil |
| Seed | 0 |
| Etiqueta | `TECHNICAL_PILOT` |
| `count_params()` | 6,273 |
| Loss | `mse` |
| Optimizer | Adam |
| Learning rate | 0.0010000000474974513 |
| `shuffle` | `False` |
| EarlyStopping monitor | `val_loss` |
| ReduceLROnPlateau monitor | `val_loss` |
| TensorFlow device | CPU |
| TEST usado | NO |

## 3. Shapes

El runner cargo solamente filas hasta validation 2024 para construir los
objetos entregados a `model.fit()`. No se construyeron secuencias test.

| Objeto | Shape |
|---|---:|
| `Xa_train` | `(84, 6, 4)` |
| `Xb_train` | `(84, 6, 33)` |
| `y_train` | `(84,)` |
| `Xa_val` | `(12, 6, 4)` |
| `Xb_val` | `(12, 6, 33)` |
| `y_val` | `(12,)` |

Fechas:

| Split | Inicio target | Fin target |
|---|---|---|
| TRAIN | 2017-01-01 | 2023-12-01 |
| VALIDATION | 2024-01-01 | 2024-12-01 |

## 4. Entorno

| Componente | Valor |
|---|---|
| Python | 3.11.9 |
| TensorFlow | 2.21.0 |
| Keras | 3.14.0 |
| NumPy | 2.4.4 |
| pandas | 3.0.2 |
| OS | Windows-10-10.0.26200-SP0 |
| CPU | AMD64 Family 25 Model 33 Stepping 2, AuthenticAMD |
| GPU TensorFlow | `[]` |
| Dispositivo TensorFlow | CPU |

Determinismo D0:

| Campo | Valor |
|---|---|
| `PYTHONHASHSEED` | `0` |
| `TF_ENABLE_ONEDNN_OPTS` | `0` |
| `TF_DETERMINISTIC_OPS` | `1` |
| `keras.utils.set_random_seed` | usado |
| `enable_op_determinism` | usado |
| TensorFlow cargado previamente | `False` |

## 5. Configuracion

Configuracion usada sin cambios:

| Elemento | Valor |
|---|---|
| lookback | 6 |
| LSTM por rama | 16 |
| BahdanauAttention por rama | 16 |
| Head | Dense16 -> Dropout0.20 -> Dense8 -> Dense1 |
| loss | MSE (`mse`) |
| metrica fit | MAE (`mae`) |
| optimizer | Adam |
| learning rate | 0.001 |
| batch size | 8 |
| max epochs | 300 |
| shuffle | `False` |
| EarlyStopping | `monitor=val_loss`, `patience=15`, `restore_best_weights=True` |
| ReduceLROnPlateau | `monitor=val_loss`, `factor=0.5`, `patience=8`, `min_lr=1e-6` |

## 6. Ejecucion model.fit

`model.fit()` se ejecuto una sola vez para:

```text
TECHNICAL_PILOT / sutil / GC3 / seed_00
```

Datos entregados a `fit`:

```text
train = 84 secuencias
validation = 12 targets de 2024
test = no entregado
```

No se ejecuto `model.predict()` sobre test ni se calcularon metricas test.

## 7. Epochs

| Campo | Valor |
|---|---:|
| Epochs maximas | 300 |
| Epochs ejecutadas | 294 |
| Batch size | 8 |
| Pasos por epoch | 11 |

La corrida se detuvo por EarlyStopping antes de completar las 300 epochs.

## 8. EarlyStopping

EarlyStopping funciono.

| Campo | Valor |
|---|---|
| Monitor | `val_loss` |
| Patience | 15 |
| `restore_best_weights` | `True` |
| Best epoch | 279 |
| Stopped epoch | 294 |

La diferencia `294 - 279 = 15` es consistente con `patience=15`.

## 9. ReduceLROnPlateau

ReduceLROnPlateau estuvo disponible y se activo.

| Campo | Valor |
|---|---|
| Monitor | `val_loss` |
| Factor | 0.5 |
| Patience | 8 |
| min_lr | 1e-6 |
| Epochs con reduccion LR | 287 |

El learning rate paso de aproximadamente `0.001` a `0.0005`.

## 10. Best epoch

| Campo | Valor |
|---|---:|
| Best epoch | 279 |
| Best `val_loss` | 94,221,704.0 |
| `val_loss` despues de restaurar pesos | 94,221,704.0 |
| `restore_best_weights` verificado | `True` |

La verificacion se hizo evaluando validation despues de finalizar el fit. Esto
no usa test 2025 y no se interpreta como resultado cientifico.

## 11. History

History guardado en:

```text
v2_reentrenamiento/resultados_v2_final/technical_pilot/sutil/GC3/seed_00/history.csv
```

Columnas:

```text
epoch, loss, val_loss, mae, val_mae, learning_rate
```

Filas: 294.

## 12. Graficos

Graficos diagnosticos generados:

```text
v2_reentrenamiento/resultados_v2_final/technical_pilot/sutil/GC3/seed_00/figures/pilot_loss_mse.png
v2_reentrenamiento/resultados_v2_final/technical_pilot/sutil/GC3/seed_00/figures/pilot_metric_mae.png
```

Ambos estan etiquetados:

```text
TECHNICAL PILOT - NOT OFFICIAL RESULT
```

No se generaron graficos test.

## 13. Checkpoint

Checkpoint tecnico:

```text
v2_reentrenamiento/resultados_v2_final/technical_pilot/sutil/GC3/seed_00/checkpoint_best.keras
```

Tamano: 152,871 bytes.

El checkpoint esta en una ruta separada de cualquier futura corrida oficial.
No debe reutilizarse en resultados oficiales.

## 14. Tiempo

| Campo | Valor |
|---|---:|
| Tiempo total `fit` | 33.079141 s |
| Segundos/epoch | 0.112514 s |
| Tiempo total script | 37.544554 s |
| TensorFlow device | CPU |

La primera epoch incluyo sobrecosto de grafo/inicializacion; esta medicion es
operativa, no cientifica.

## 15. Estimacion operativa 40 corridas

Extrapolacion simple del piloto:

| Campo | Valor |
|---|---:|
| Segundos estimados para 40 corridas | 1,323.165640 s |
| Minutos estimados para 40 corridas | 22.052761 min |

Esta estimacion asume costo igual al piloto. En corridas reales, EarlyStopping
puede detener en epochs distintos segun modelo, cultivar y seed.

## 16. Separacion piloto/oficial

Ruta raiz del piloto:

```text
v2_reentrenamiento/resultados_v2_final/technical_pilot/sutil/GC3/seed_00/
```

Archivos principales:

```text
config.json
metadata.json
history.csv
checkpoint_best.keras
logs/pilot_run_summary.json
figures/pilot_loss_mse.png
figures/pilot_metric_mae.png
```

El archivo `config.json` marca:

```text
official_result = false
must_retrain_official_seed0 = true
```

Cuando se ejecute el experimento oficial, GC3/Sutil/seed0 debera entrenarse de
nuevo desde cero. No deben reutilizarse pesos, checkpoint, history ni metricas
del piloto.

## 17. Incidencias

Incidencia pre-fit:

- El primer intento se detuvo antes de `model.fit()` por incompatibilidad de
  API en el runner piloto: `pandas.to_numeric(errors="ignore")` no es valido
  en pandas 3.0.2.
- No hubo entrenamiento en ese intento.
- Solo quedaron directorios vacios `figures/` y `logs/`.
- Se corrigio el runner para convertir columnas numericas sin `errors="ignore"`
  y para no sobrescribir una carpeta piloto que contenga archivos previos.

Incidencias runtime no bloqueantes:

- TensorFlow emitio la advertencia esperada de Windows nativo: GPU no
  disponible para TensorFlow >= 2.11 en Windows.
- TensorFlow emitio mensajes informativos sobre CPU y `MapDataset`; no
  impidieron la ejecucion.

## 18. Estado final

| Criterio | Estado |
|---|---|
| GC3 = 6273 params | OK |
| Shapes TRAIN correctos | OK |
| Validation = 2024 | OK |
| TEST no utilizado | OK |
| `model.fit` completo | OK |
| `shuffle=False` | OK |
| EarlyStopping funciono | OK |
| `restore_best_weights` funciono | OK |
| ReduceLROnPlateau funciono | OK |
| History completo | OK |
| Learning rate registrado | OK |
| Checkpoint separado | OK |
| Graficos generados | OK |
| Metadata guardada | OK |
| Tiempo registrado | OK |
| Piloto separado de resultados oficiales | OK |

Estado final: piloto tecnico completado. No se deriva ninguna conclusion
cientifica sobre desempeno predictivo.
