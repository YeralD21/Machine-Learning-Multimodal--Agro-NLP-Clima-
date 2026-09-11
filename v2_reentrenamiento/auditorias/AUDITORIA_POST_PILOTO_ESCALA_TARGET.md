# AUDITORIA POST-PILOTO - ESCALA DEL TARGET

## 1. Alcance

Auditoria post-piloto del entrenamiento tecnico:

```text
TECHNICAL_PILOT / sutil / GC3 / seed_00
```

No se ejecuto nuevo entrenamiento, no se ejecuto `model.fit()`, no se
ejecutaron nuevas seeds, no se ejecuto GE, no se uso test 2025, no se
calcularon metricas cientificas test y no se modificaron decisiones
metodologicas.

Fuentes inspeccionadas:

- `v2_reentrenamiento/DECISIONES_METODOLOGICAS.md`
- `v2_reentrenamiento/auditorias/AUDITORIA_PILOTO_TECNICO_GC3_SUTIL_SEED0.md`
- `v2_reentrenamiento/auditorias/AUDITORIA_IMPLEMENTACION_TECNICA_GC3_GE.md`
- `v2_reentrenamiento/src/`
- `v2_reentrenamiento/resultados_v2_final/technical_pilot/sutil/GC3/seed_00/`

## 2. Target recibido por `model.fit`

El piloto uso como fuente:

```text
v2_reentrenamiento/data/processed/master_dataset_sutil_v2_features.csv
```

El runner del piloto define:

```text
dataset_path = .../master_dataset_sutil_v2_features.csv
target = produccion_t_sutil
values_y = df.loc[:, spec.target].to_numpy(dtype=float)
```

Por tanto:

| Pregunta | Respuesta |
|---|---|
| `y_train` esta en toneladas originales? | SI |
| `y_train` esta estandarizado? | NO |
| `y_val` esta en toneladas originales? | SI |
| `y_val` esta estandarizado? | NO |
| Columna target exacta | `produccion_t_sutil` |
| Scaler aplicado al target en el piloto | Ninguno |
| Scaler disponible registrado | `scaler_sutil_v2c.joblib` |
| Ese scaler fue aplicado antes de `fit` | NO |

El scaler disponible contiene parametros TRAIN-only para el target:

| Feature | mean_train | scale_train | var_train |
|---|---:|---:|---:|
| `produccion_t_sutil` | 23431.995422222222 | 8041.516494927326 | 64665987.53818826 |

Ese scaler parece ajustado sobre TRAIN segun el archivo de parametros y la
documentacion metodologica, pero no transformo el target usado por el piloto.

## 3. Estadisticos de `y_train` y `y_val`

Valores entregados efectivamente al modelo:

| Split | n | min | max | mean | std poblacional | std muestral |
|---|---:|---:|---:|---:|---:|---:|
| `y_train` | 84 | 4851.691 | 41867.719 | 23719.247286 | 8236.510668 | 8285.979645 |
| `y_val` | 12 | 23811.288 | 44161.843 | 32208.766500 | 8185.906135 | 8549.900111 |

Para comparacion, si se hubiera usado la version escalada disponible:

| Split | n | min | max | mean | std poblacional | std muestral |
|---|---:|---:|---:|---:|---:|---:|
| `y_train` escalado equivalente | 84 | -2.310547 | 2.292568 | 0.035721 | 1.024248 | 1.030400 |
| `y_val` escalado equivalente | 12 | 0.047167 | 2.577853 | 1.091432 | 1.017956 | 1.063220 |

## 4. Pipeline real del target

Pipeline observado en el piloto:

```text
master_dataset_sutil_v2_features.csv
   ->
target = produccion_t_sutil
   ->
sin transformacion / sin escalado
   ->
creacion de secuencias con lookback=6
   ->
y_train raw en toneladas / y_val raw en toneladas
   ->
model.fit(..., loss="mse", metrics=["mae"])
   ->
salida de la red en la misma escala de entrenamiento
   ->
sin inverse transform en el piloto
   ->
toneladas finales si se interpretara directamente la salida
```

Pipeline escalado disponible, pero no usado por el piloto:

```text
master_dataset_sutil_v2_escalado.csv
   ->
produccion_t_sutil estandarizado con scaler_sutil_v2c
   ->
entrenamiento sobre target escalado
   ->
inverse transform futuro con scaler_sutil_v2c para volver a toneladas
```

## 5. Auditoria de `val_loss = 94,221,704`

El piloto reporto:

```text
best_val_loss = 94,221,704.0
loss = MSE
```

Diagnostico:

```text
sqrt(best_val_loss) = 9706.786492
```

Esto equivale a un error cuadratico medio en escala de toneladas, no a escala
estandarizada. La magnitud es matematicamente coherente con un target raw en
toneladas cuyos valores de validation estan entre aproximadamente 23,811 y
44,162 toneladas.

Clasificacion solicitada:

```text
A) magnitud matematicamente coherente con target en toneladas.
```

Si el diseno operativo esperado para GC3/GE oficial era entrenar con target
escalado, entonces esta magnitud tambien evidencia que el piloto no siguio ese
contrato de escala.

## 6. Inputs escalados

Rama A del piloto:

```text
produccion_t_sutil
produccion_t_sutil_lag1
produccion_t_sutil_lag3
produccion_t_sutil_lag6
```

Rama B del piloto:

```text
mes_sin, mes_cos, t_index
NASA lag1/lag3/lag6
INDECI lag1/lag3/lag6
```

El piloto leyo `master_dataset_sutil_v2_features.csv`, no
`master_dataset_sutil_v2_escalado.csv`. Por tanto, los inputs usados por
`fit` fueron raw/no estandarizados, salvo `mes_sin` y `mes_cos`, que por
construccion ya estan en escala trigonometrica [-1, 1].

El archivo `scaler_sutil_v2c_parametros.csv` contiene parametros para 35 de los
37 inputs GC3. Los dos no incluidos son:

```text
mes_sin
mes_cos
```

Comparacion de algunas columnas TRAIN:

| Columna | raw min | raw max | raw mean | escalado min | escalado max | escalado mean |
|---|---:|---:|---:|---:|---:|---:|
| `produccion_t_sutil` | 4851.691 | 41867.719 | 23431.995422 | -2.310547 | 2.292568 | ~0 |
| `produccion_t_sutil_lag1` | 4851.691 | 41867.719 | 23307.152911 | -2.303091 | 2.316207 | ~0 |
| `t_index` | 0 | 89 | 44.5 | -1.712912 | 1.712912 | ~0 |
| `T2M_lag1` | 22.429 | 26.4831 | 24.412726 | -1.764328 | 1.841393 | ~0 |
| `num_emergencias_lag1` | 0.1042 | 33.4069 | 2.717584 | -0.582326 | 6.838326 | ~0 |
| `mes_sin` | -1 | 1 | -0.041467 | -1 | 1 | -0.041467 |
| `mes_cos` | -1 | 1 | 0.011111 | -1 | 1 | 0.011111 |

Conclusiones de inputs:

- Rama A llego raw/no estandarizada.
- Rama B llego raw/no estandarizada para `t_index`, NASA e INDECI.
- `mes_sin` y `mes_cos` no requieren StandardScaler y aparecen iguales en raw
  y escalado.
- El scaler v2c existe y parece TRAIN-only, pero no fue aplicado en el piloto.

## 7. `produccion_t_sutil` input vs target

La construccion del piloto usa:

```text
contexto filas [j-lookback, j)
target fila j
```

Por tanto, la fila final de la secuencia es `j-1` y el target es `j`. Esto
corresponde a `t -> t+1`; no son la misma fila temporal.

Ejemplos auditables, sin usar test:

| Caso | fecha_final_secuencia | `produccion_t_sutil` al final | fecha_target | target |
|---|---|---:|---|---:|
| Primer TRAIN | 2016-12-01 | 16714.142 | 2017-01-01 | 18255.799 |
| TRAIN intermedio | 2020-06-01 | 26948.624 | 2020-07-01 | 15387.758 |
| Ultimo TRAIN | 2023-11-01 | 24158.060 | 2023-12-01 | 30799.027 |

Resultado: el target `t+1` esta alineado correctamente respecto al ultimo mes
de la secuencia.

## 8. Incidente pandas

Incidente observado en el primer intento del piloto:

```text
ValueError: invalid error value specified
```

Archivo:

```text
v2_reentrenamiento/auditorias/ejecutar_piloto_tecnico_gc3_sutil_seed0.py
```

Linea original segun traceback:

```text
df[column] = pd.to_numeric(df[column], errors="ignore")
```

Motivo: en pandas 3.0.2, `errors="ignore"` ya no es un valor valido para
`pd.to_numeric`.

Cambio aplicado en el runner:

```text
try:
    df[column] = pd.to_numeric(df[column])
except (TypeError, ValueError):
    pass
```

Equivalencia tecnica:

- Las columnas numericas convertibles se convierten igual a tipos numericos.
- Las columnas no convertibles permanecen sin cambios al capturar la excepcion.
- En el CSV usado, las columnas requeridas para features/target/fechas son
  convertibles o se procesan explicitamente como enteros para fecha.

Impacto del cambio:

| Elemento | Alterado? |
|---|---|
| Features | NO |
| Valores numericos | NO |
| Fechas | NO |
| Target | NO |
| Split | NO |
| Scalers | NO |

No hubo entrenamiento en el intento fallido; la falla ocurrio antes de
`model.fit()`.

## 9. EarlyStopping y ReduceLROnPlateau

History auditado:

| Campo | Valor |
|---|---:|
| best_epoch | 279 |
| stopped_epoch | 294 |
| diferencia | 15 |
| EarlyStopping patience | 15 |

La relacion:

```text
294 - 279 = 15
```

es consistente con `EarlyStopping(patience=15)`.

ReduceLROnPlateau:

| Campo | Valor |
|---|---|
| patience | 8 |
| factor | 0.5 |
| epoch de reduccion observado | 287 |
| LR antes | ~0.001 |
| LR despues | ~0.0005 |

La reduccion aparece en `history.csv` en la columna `learning_rate` y coincide
con `logs/pilot_run_summary.json`.

## 10. History

Archivo:

```text
v2_reentrenamiento/resultados_v2_final/technical_pilot/sutil/GC3/seed_00/history.csv
```

Columnas presentes:

```text
epoch
loss
val_loss
mae
val_mae
learning_rate
```

Verificaciones:

| Verificacion | Resultado |
|---|---:|
| Filas | 294 |
| NaN en columnas numericas | 0 |
| Inf en columnas numericas | 0 |
| best_epoch por minimo `val_loss` | 279 |
| stopped_epoch por ultima fila | 294 |
| reducciones LR detectadas | 287 |

## 11. Checkpoint

Checkpoint:

```text
v2_reentrenamiento/resultados_v2_final/technical_pilot/sutil/GC3/seed_00/checkpoint_best.keras
```

Verificacion:

| Campo | Valor |
|---|---|
| Existe | SI |
| Tamano | 152,871 bytes |
| Header | ZIP/Keras (`50-4B-03-04`) |

Vinculacion del checkpoint:

- `config.json`: `run_label = TECHNICAL_PILOT`, `cultivar = sutil`,
  `model_name = GC3`, `seed = 0`, `official_result = false`.
- `metadata.json`: `cultivar = sutil`, `model_name = GC3`, `seed = 0`,
  `best_epoch = 279`, `stopped_epoch = 294`.
- `pilot_run_summary.json`: registra `test_used = false` y la ruta del
  checkpoint.

Este checkpoint no debe reutilizarse en el experimento oficial.

## 12. Figuras

Figuras verificadas:

```text
pilot_loss_mse.png
pilot_metric_mae.png
```

Ambas existen y tienen header PNG valido (`89-50-4E-47-0D-0A-1A-0A`).

Segun el codigo del runner:

- se generan exclusivamente desde `history_frame`;
- grafican TRAIN y VALIDATION;
- marcan `best_epoch`;
- marcan `stopped_epoch`;
- incluyen el titulo `TECHNICAL PILOT - NOT OFFICIAL RESULT`.

No se uso test para generarlas.

## 13. Evaluacion de autorizacion tecnica

El piloto verifica correctamente:

- integridad de `model.fit`;
- alineamiento temporal `t -> t+1`;
- callbacks;
- checkpoint;
- history;
- logging;
- separacion piloto/oficial;
- no uso de test.

Sin embargo, la auditoria detecta una discrepancia tecnica de escala:

```text
El piloto entreno con target e inputs raw desde master_dataset_sutil_v2_features.csv.
No aplico master_dataset_sutil_v2_escalado.csv ni scaler_sutil_v2c antes de fit.
```

Dado que la infraestructura metodologica previa habia requerido usar los
scalers v2 aprobados/congelados y registrar inverse scaling cuando
corresponda, esta discrepancia debe resolverse antes de autorizar las 40
corridas oficiales.

Esta auditoria no corrige el problema. Solo lo documenta.

## 14. Respuestas obligatorias

1. **El target del entrenamiento esta raw o escalado?**
   Raw, en toneladas originales.

2. **Eso coincide con el diseno vigente?**
   No queda alineado con el contrato operativo esperado de usar scalers v2
   congelados para GC3/GE. El piloto uso el CSV raw y no aplico el scaler,
   aunque registro `scaler_sutil_v2c.joblib` en metadata.

3. **94,221,704 de `val_loss` es coherente con esa escala?**
   Si. Es coherente con MSE sobre toneladas originales. Su raiz cuadrada es
   `9706.786492`, una magnitud en toneladas.

4. **El target `t+1` esta correctamente alineado?**
   Si. La secuencia termina en `t` y el target corresponde a `t+1`.

5. **El fix pandas cambio algun dato?**
   No. Solo reemplazo una opcion de API incompatible por una conversion
   equivalente con `try/except`; no altero features, valores, fechas, target,
   split ni scalers.

6. **History tiene 294 filas limpias?**
   Si. Tiene 294 filas, columnas requeridas, 0 NaN y 0 Inf.

7. **EarlyStopping se comporto conforme a `patience=15`?**
   Si. `best_epoch=279`, `stopped_epoch=294`, diferencia 15.

8. **ReduceLROnPlateau se comporto conforme a `patience=8`?**
   Si. La reduccion de learning rate se registro en epoch 287 y paso de
   aproximadamente 0.001 a 0.0005.

9. **Existe alguna razon tecnica para NO autorizar despues las corridas oficiales?**
   Si. Antes de las corridas oficiales debe resolverse la discrepancia de
   escala: el piloto entreno raw, mientras que el pipeline v2 dispone de
   dataset escalado y scaler TRAIN-only. No conviene autorizar las 40 corridas
   hasta fijar/implementar explicitamente si GC3/GE oficial entrenara con datos
   escalados o raw, y como se hara el inverse transform si corresponde.
