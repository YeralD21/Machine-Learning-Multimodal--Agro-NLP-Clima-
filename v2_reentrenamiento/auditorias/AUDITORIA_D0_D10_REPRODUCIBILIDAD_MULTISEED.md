# AUDITORIA D0/D10 - REPRODUCIBILIDAD Y MULTI-SEED GC3/GE

## 1. Alcance

Auditoria previa al cierre de D0 y D10 para GC3/GE v2. Esta tarea es
documental y de inspeccion: no se entrenaron modelos, no se ejecuto ninguna
seed, no se uso validation 2024, no se uso test 2025, no se ejecuto HPO, no se
modificaron datasets, scalers, features, notebooks, modelos ni
`DECISIONES_METODOLOGICAS.md`.

Fuentes leidas:

- `v2_reentrenamiento/DECISIONES_METODOLOGICAS.md`
- `v2_reentrenamiento/auditorias/AUDITORIA_D4_D36_ARQUITECTURA_GC3_GE.md`
- `CONTEXTO_SESION_2026-09-08_METRICAS.md`
- `v2_reentrenamiento/ENTORNO_COMPUTO.md`
- `notebooks/fase3/actividad_14_ge_lstm_attention.ipynb`
- `notebooks/fase4/actividad_15_multimodal_nlp.ipynb`
- `notebooks/fase4/actividad_15_ejecutado.ipynb`
- notebooks historicos XGBoost/GM_v2-v6 por busqueda de texto
- `requirements.txt`

Se respetan como cerrados D4, D36, D7, D3-op, D35-a y D35-b. Esta auditoria no
los reabre.

Configuracion oficial vigente de GC3/GE v2:

| Elemento | GC3 | GE |
|---|---:|---:|
| Inputs totales | 37 | 43 |
| Rama A | 4 | 4 |
| Rama B | 33 | 39 |
| LSTM por rama | 16 | 16 |
| BahdanauAttention por rama | 16 | 16 |
| Head | Dense16 -> Dropout0.20 -> Dense8 -> Dense1 | Igual |
| L2 | 0 | 0 |
| Optimizer | Adam lr=0.001 | Igual |
| Batch | 8 | Igual |
| Max epochs | 300 | Igual |
| EarlyStopping | patience 15, restore_best_weights=True | Igual |
| ReduceLROnPlateau | factor 0.5, patience 8, min_lr 1e-6 | Igual |
| Shuffle | False | Igual |
| TRAIN sequences | 84 | 84 |

## 2. Estado historico v1

Notebook principal GC3/GE historico:

```text
notebooks/fase3/actividad_14_ge_lstm_attention.ipynb
```

Notebook multimodal historico:

```text
notebooks/fase4/actividad_15_multimodal_nlp.ipynb
notebooks/fase4/actividad_15_ejecutado.ipynb
```

Hallazgo observado en GE v1:

| Elemento | Evidencia historica |
|---|---|
| Seed TensorFlow | `tf.random.set_seed(RANDOM_STATE)` con `RANDOM_STATE = 42` |
| Seed NumPy | `np.random.seed(RANDOM_STATE)` con `RANDOM_STATE = 42` |
| Python `random.seed` | No observado en GE/GM original |
| `keras.utils.set_random_seed` | No observado |
| `PYTHONHASHSEED` | No observado en GE/GM original; si aparece en variantes GM_v2/v3/v4/TCN |
| `TF_ENABLE_ONEDNN_OPTS` | `0`, fijado antes de importar TensorFlow |
| `TF_DETERMINISTIC_OPS` | No observado |
| `tf.config.experimental.enable_op_determinism()` | No observado |
| Threads CPU | No observado |
| `random_state` sklearn | No aplica al GE/GM original; si aparece en PCA/XGBoost de variantes |
| GPU memory growth | Se activaba si habia GPU detectada |
| `model.fit(..., shuffle=...)` | No especificado en GE/GM original; Keras usa default `shuffle=True` para arrays |

La arquitectura v1 fija una sola seed (`42`) y controla parte de la
aleatoriedad de inicializacion/barajado. No fija todas las fuentes relevantes
de no determinismo. Por tanto, `seed=42` historica debe considerarse
reproducibilidad parcial, no reproducibilidad completa garantizada.

## 3. Fuentes de aleatoriedad

Fuentes relevantes para GC3/GE:

| Fuente | Riesgo | Control propuesto |
|---|---|---|
| Inicializacion de pesos Keras | Cambia trayectoria de entrenamiento | Seed global por corrida |
| Dropout | Mascara aleatoria por batch/epoch | Seed global por corrida |
| Orden de ventanas durante SGD | Cambia trayectoria si `shuffle=True` | D7 fija `shuffle=False` |
| EarlyStopping | Depende de trayectoria y ruido numerico | Mismos callbacks y seed |
| ReduceLROnPlateau | Depende de trayectoria y `val_loss` | Mismos callbacks y seed |
| Operaciones TensorFlow no deterministas | Diferencias dentro/fuera de GPU | Activar determinismo si la API lo permite |
| oneDNN/BLAS CPU | Orden de reduccion puede variar | Evaluar `TF_ENABLE_ONEDNN_OPTS=0` |
| GPU kernels/cuDNN | Algunas operaciones pueden no ser bit-a-bit | Preferir CPU unico o activar determinismo |
| Versiones de librerias | Cambian kernels y defaults | Registrar versiones exactas |
| Hashing de Python | Puede afectar iteracion de estructuras no ordenadas | `PYTHONHASHSEED` antes del proceso |
| Threads | Paralelismo puede cambiar orden numerico | Registrar o fijar si se exige determinismo estricto |

Con D7 cerrado (`shuffle=False`) se elimina una fuente de variacion por orden de
secuencias durante `fit`, pero no se elimina la aleatoriedad de inicializacion,
dropout ni operaciones internas.

## 4. Entorno actual

Fuente principal: `v2_reentrenamiento/ENTORNO_COMPUTO.md`.

Entorno primario v2 documentado:

| Elemento | Valor documentado |
|---|---|
| SO | Windows 11 Pro, build 26200, 64 bits |
| CPU | AMD Ryzen 7 5800X 8-Core, 8 fisicos / 16 logicos |
| RAM | 31.93 GB |
| GPU fisica | NVIDIA GeForce RTX 3060, 12 GB |
| Driver NVIDIA | 591.86 |
| Python v2 previsto | 3.11.9 |
| TensorFlow | 2.21.0 CPU build |
| Keras | 3.14.0 |
| NumPy | 2.4.4 |
| Pandas | 3.0.2 |
| scikit-learn | 1.8.0 |
| XGBoost | 3.2.0 |
| GPU visible para TF Windows | `[]` |
| Decision entorno v2 | Windows venv CPU |

Inspeccion actual adicional:

| Comando / fuente | Resultado |
|---|---|
| `cmd /c ver` | Microsoft Windows 10.0.26200.9445 |
| `nvidia-smi` | NVIDIA GeForce RTX 3060, driver 591.86, 12288 MiB |
| `git rev-parse --short HEAD` | `bfd19a7` |
| Branch | `v2-reentrenamiento` |
| `venv/pyvenv.cfg` | Apunta a Python 3.11.9 en `C:\Users\ADMIN\AppData\Local\Programs\Python\Python311` |
| Ejecucion de `venv/Scripts/python.exe` | Falla: Python base no encontrado |

Estado del entorno: el venv de Windows esta roto en la inspeccion actual porque
su `pyvenv.cfg` referencia un ejecutable base inexistente. No se reparo ni se
modifico el entorno. Para futuras ejecuciones oficiales, D0 debe exigir un
entorno funcional recapturado antes de entrenar.

Entorno alternativo WSL2 documentado:

| Elemento | Valor |
|---|---|
| Python | 3.11.15 |
| TensorFlow | 2.21.0 CUDA build |
| Keras | 3.14.1 |
| NumPy | 2.4.6 |
| GPU visible | RTX 3060 |

No se recomienda mezclar Windows CPU y WSL2 GPU dentro de la misma Fase 3: esa
mezcla reintroduciria una limitacion ya identificada en v1.

## 5. Determinismo TensorFlow/Keras

APIs y controles a considerar para D0:

```python
import os
os.environ["PYTHONHASHSEED"] = str(seed)
os.environ["TF_ENABLE_ONEDNN_OPTS"] = "0"
os.environ["TF_DETERMINISTIC_OPS"] = "1"

import random
import numpy as np
import tensorflow as tf
from tensorflow import keras

random.seed(seed)
np.random.seed(seed)
tf.random.set_seed(seed)
keras.utils.set_random_seed(seed)
tf.config.experimental.enable_op_determinism()
```

Notas tecnicas:

- `keras.utils.set_random_seed(seed)` es preferible como llamada unificada en
  Keras 3/TensorFlow reciente porque sincroniza Python, NumPy y TensorFlow segun
  la API vigente. Puede conservarse junto con llamadas explicitas si se
  documenta el orden.
- `tf.config.experimental.enable_op_determinism()` o su API vigente debe
  evaluarse en el entorno finalmente instalado. Si no existe en la version
  efectiva, debe registrarse la ausencia.
- `PYTHONHASHSEED` debe fijarse antes de iniciar el proceso Python para tener
  efecto completo.
- `TF_ENABLE_ONEDNN_OPTS=0` debe fijarse antes de importar TensorFlow si se
  decide desactivar oneDNN.
- `TF_DETERMINISTIC_OPS=1` tambien debe fijarse antes de importar TensorFlow.

Recomendacion tecnica para futura implementacion: centralizar estas acciones en
una funcion de setup ejecutada al inicio del notebook/script, y registrar en
metadata tanto el valor solicitado como la confirmacion de APIs disponibles.

## 6. Limites de reproducibilidad

Se deben distinguir dos niveles:

| Nivel | Alcance | Expectativa razonable |
|---|---|---|
| Mismo hardware/software | Misma maquina, SO, CPU/GPU, Python, TensorFlow/Keras, NumPy, flags, datasets y scalers | Reproducibilidad muy alta; potencialmente bit-a-bit si todas las operaciones usadas son deterministas |
| Hardware/versiones diferentes | Otra maquina, otro SO, otra CPU/GPU, otros parches de librerias | No prometer bit-a-bit; esperar reproducibilidad metodologica y diferencias numericas pequenas |

D0 no debe prometer reproducibilidad bit-a-bit universal. Debe prometer un
protocolo de captura y control suficiente para replicar corridas dentro del
mismo entorno y para explicar desviaciones en entornos diferentes.

## 7. Alternativas 5/10/20 seeds

| Opcion | Ventajas | Desventajas | Costo relativo | Estabilidad descriptiva esperable |
|---|---|---|---:|---|
| 5 seeds | Bajo costo; permite detectar varianza extrema inicial | Estimadores de media/sd muy fragiles; min/max poco informativos | 1x | Baja-moderada |
| 10 seeds | Buen balance; suficiente para media/mediana/sd descriptivas iniciales; costo manejable | Aun no es inferencia temporal; colas poco caracterizadas | 2x vs 5 | Moderada |
| 20 seeds | Mejor descripcion de variabilidad por inicializacion/dropout | Mayor costo; puede parecer precision excesiva con solo 84 secuencias TRAIN | 4x vs 5 | Moderada-alta para variabilidad de entrenamiento |

Con solo 84 secuencias TRAIN, el objetivo multi-seed no es aumentar el tamano
temporal efectivo, sino describir sensibilidad a inicializacion y trayectoria de
entrenamiento. Mas seeds reducen el ruido de la estimacion descriptiva entre
corridas, pero no crean nuevos meses de validation/test.

## 8. Propuesta D10 no vinculante

**PROPUESTA D10 NO VINCULANTE:** usar 10 seeds preespecificadas:

```text
0, 1, 2, 3, 4, 5, 6, 7, 8, 9
```

Justificacion:

- lista simple, reproducible y sin seleccion por desempeno;
- evita conservar `42` por costumbre o porque haya sido historicamente
  favorable;
- ofrece mejor estabilidad descriptiva que 5 seeds;
- mantiene costo mas razonable que 20 seeds para 2 cultivares x 2 modelos;
- es coherente con 84 secuencias TRAIN y con la necesidad de reportar
  sensibilidad sin convertir validation 2024 en concurso de seeds.

Esta propuesta no cierra D10. La lista debe quedar fijada antes del primer
entrenamiento oficial y no puede cambiarse despues de ver resultados.

## 9. Estadisticos a reportar

Por cultivar, modelo y metrica:

- resultados individuales por seed;
- media;
- mediana;
- desviacion estandar;
- minimo;
- maximo.

Metrica primaria:

- MAE, conforme a D35-a.

Metricas adicionales a reportar cuando aplique:

- RMSE;
- RelMAE_N1;
- MASE_1;
- RMSSE_1;
- R2 como descriptiva secundaria;
- metricas condicionales de D35-b: MAE_global, MAE_shock, MAE_nonshock,
  n_shock, n_nonshock, Delta_s.

Advertencia obligatoria: la dispersion entre seeds no es intervalo de confianza
temporal y no aumenta `n=12` ni `n_shock=3`. Describe sensibilidad del
entrenamiento estocastico bajo un mismo protocolo.

## 10. Emparejamiento GC3/GE

Cada seed debe ejecutarse emparejada:

```text
GC3 seed s
GE  seed s
```

Condiciones identicas por par:

- mismo split;
- mismo lookback;
- misma arquitectura salvo +6 NLP en Rama B de GE;
- misma inicializacion controlada por seed;
- mismos flags de determinismo;
- mismos callbacks;
- mismo batch;
- mismo max_epochs;
- mismo `shuffle=False`;
- mismo scaler;
- mismo protocolo de evaluacion;
- mismo entorno.

El emparejamiento por seed mejora la limpieza descriptiva de la ablacion
GC3 -> GE porque reduce variacion atribuible a elecciones arbitrarias de seed.
No convierte el contraste en causal ni elimina las limitaciones de n_test=12.

## 11. Aplicabilidad a XGBoost

XGBoost puede ser determinista o estocastico segun configuracion.

Fuentes de estocasticidad relevantes:

| Hiperparametro / condicion | Efecto |
|---|---|
| `subsample < 1` | Muestreo aleatorio de filas por arbol/iteracion |
| `colsample_bytree < 1` | Muestreo aleatorio de columnas por arbol |
| `colsample_bylevel`, `colsample_bynode` | Muestreo adicional de columnas si se usan |
| `random_state` / `seed` | Controla muestreos internos |
| `tree_method` / paralelismo | Puede introducir diferencias numericas menores segun backend |

El XGBoost historico (`actividad_15v3_xgboost_competidor`) uso una grilla con
`subsample` en `{0.8, 1.0}` y `colsample_bytree` en `{0.8, 1.0}`. Por tanto, si
GC2 v2 adopta configuraciones con muestreo menor que 1, metodologicamente
conviene usar las mismas seeds oficiales que GC3/GE y reportar sensibilidad si
la estocasticidad esta activa.

Si GC2 queda con `subsample=1.0`, `colsample_bytree=1.0` y sin otros muestreos,
multi-seed puede no cambiar resultados de forma material; aun asi debe fijarse
`random_state` para trazabilidad.

## 12. Metadata por corrida

Cada corrida futura deberia almacenar, como minimo:

| Campo | Motivo |
|---|---|
| `run_id` | Identificador unico |
| cultivar | Sutil/Dulce |
| modelo | GC3/GE/GC2 si aplica |
| seed | Reproducibilidad de inicializacion |
| lista oficial de seeds | Verificacion D10 |
| timestamp inicio/fin | Trazabilidad |
| git commit/hash | Estado del codigo |
| git status resumido | Detectar dirty worktree |
| Python version | Entorno |
| TensorFlow version | Entorno |
| Keras version | Entorno |
| NumPy version | Entorno |
| pandas/scikit-learn/XGBoost | Entorno |
| OS | Entorno |
| CPU | Entorno |
| GPU visible | Entorno |
| oneDNN setting | Determinismo |
| `TF_DETERMINISTIC_OPS` | Determinismo |
| `PYTHONHASHSEED` | Determinismo |
| `enable_op_determinism` disponible/activado | Determinismo |
| threads si se fijan | Determinismo |
| dataset hashes | Integridad |
| scaler hashes | Integridad |
| model config hash | Integridad de arquitectura |
| feature list hash | Integridad de inputs |
| callbacks config | Reproducibilidad |
| epochs run / best epoch | Entrenamiento |
| checkpoint path | Recuperabilidad |

## 13. Estructura de artefactos

Esquema propuesto para futuras corridas, sin crear archivos ahora:

```text
v2_reentrenamiento/experimentos/
  gc3_ge_multiseed/
    {cultivar}/
      {modelo}/
        seed_{seed}/
          config.json
          metadata.json
          history.csv
          predicciones_validation.csv
          predicciones_test.csv
          metricas.json
          checkpoint_best.keras
          model_final.keras
```

Reglas:

- cada seed tiene su propio directorio;
- nunca sobrescribir una seed con otra;
- GC3 y GE deben compartir estructura;
- las comparativas agregadas deben construirse leyendo resultados individuales;
- los checkpoints no deben guardarse en una ruta comun que pueda ser pisada por
  otra seed.

## 14. Riesgos

| Riesgo | Impacto | Mitigacion propuesta |
|---|---|---|
| Venv actual roto | Bloquea ejecucion oficial | Reparar solo despues de cerrar D0/D10 y recapturar entorno |
| Mezclar Windows CPU y WSL2 GPU | Confunde comparabilidad GC3/GE | Entorno unico por toda Fase 3 |
| oneDNN activo | Diferencias numericas por orden de operaciones | Decidir `TF_ENABLE_ONEDNN_OPTS=0` antes de entrenar |
| Determinismo API no disponible | Control incompleto | Registrar disponibilidad real |
| Seleccionar seed ganadora | Sesgo metodologico | Lista de seeds fija a priori |
| Usar validation como concurso de seeds | Sobreajuste a 12 meses | Validation solo controla entrenamiento preespecificado |
| Interpretar sd entre seeds como IC temporal | Inferencia indebida | Etiquetar como variabilidad de entrenamiento |
| Checkpoints sobrescritos | Perdida de trazabilidad | Directorio por seed |

## 15. Decisiones pendientes

Para cerrar D0:

1. Confirmar entorno oficial funcional: Windows CPU o alternativa unica.
2. Decidir si se desactiva oneDNN (`TF_ENABLE_ONEDNN_OPTS=0`).
3. Decidir si se exige `TF_DETERMINISTIC_OPS=1`.
4. Confirmar uso de `keras.utils.set_random_seed(seed)`.
5. Confirmar uso de `tf.config.experimental.enable_op_determinism()` o API
   vigente.
6. Definir si se fijan threads CPU.
7. Definir metadata obligatoria por corrida.

Para cerrar D10:

1. Elegir numero de seeds.
2. Fijar lista exacta de seeds antes de entrenar.
3. Confirmar emparejamiento GC3/GE por seed.
4. Definir estadisticos agregados a reportar.
5. Definir si GC2 XGBoost entra al mismo protocolo multi-seed cuando haya
   estocasticidad.

## 16. Recomendacion tecnica NO vinculante

**RECOMENDACION TECNICA NO VINCULANTE:**

Para D0, usar un unico entorno oficial Windows CPU recapturado y funcional,
fijando antes de importar TensorFlow:

```text
PYTHONHASHSEED={seed}
TF_ENABLE_ONEDNN_OPTS=0
TF_DETERMINISTIC_OPS=1
```

y dentro del proceso:

```text
random.seed(seed)
np.random.seed(seed)
tf.random.set_seed(seed)
keras.utils.set_random_seed(seed)
tf.config.experimental.enable_op_determinism() si esta disponible
```

Para D10, usar 10 seeds preespecificadas:

```text
0, 1, 2, 3, 4, 5, 6, 7, 8, 9
```

Ejecutar GC3 y GE emparejados por seed, reportando resultados individuales y
estadisticos descriptivos. Si GC2 XGBoost usa `subsample < 1` o
`colsample_bytree < 1`, aplicar las mismas seeds oficiales tambien a GC2.

Esta recomendacion no cierra D0 ni D10. La decision final debe tomarse
externamente antes de cualquier entrenamiento oficial.
