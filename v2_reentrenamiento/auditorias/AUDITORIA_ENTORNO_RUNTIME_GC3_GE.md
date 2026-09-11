# AUDITORIA ENTORNO RUNTIME GC3/GE

## 1. Estado inicial

Objetivo de esta auditoria: diagnosticar el entorno Python/TensorFlow necesario
para ejecutar verificaciones runtime de la infraestructura GC3/GE v2 ya
implementada. No se entrenaron modelos, no se ejecuto `model.fit()`, no se
ejecutaron las seeds oficiales, no se uso test 2025 y no se modificaron
decisiones metodologicas.

El diagnostico inicial dentro del sandbox mostro:

| Componente | Estado | Version/ruta | Problema | Accion propuesta/realizada |
|---|---|---|---|---|
| `python` en PATH | No disponible | n/a | Comando no reconocido | Usar interprete del venv oficial |
| `py` launcher | No disponible para Python instalado | n/a | `No installed Python found!` | Usar interprete del venv oficial |
| `pip` en PATH | No disponible | n/a | Comando no reconocido | No usar pip global |
| venv Windows | Funcional fuera del sandbox | `venv/Scripts/python.exe` | En sandbox no accedia al Python base | Ejecutar venv con permiso de sistema |
| Python base del venv | Accesible fuera del sandbox | `C:/Users/ADMIN/AppData/Local/Programs/Python/Python311/python.exe` | El sandbox no permite inspeccion directa de `AppData` | No reinstalar |
| TensorFlow en venv | Presente y funcional | `tensorflow 2.21.0` | No verificado en auditoria previa | Verificado runtime |
| Keras en venv | Presente y funcional | `keras 3.14.0` | No verificado en auditoria previa | Verificado runtime |
| WSL | No accesible | `wsl -l -v` | `E_ACCESSDENIED` | No usar WSL para v2 |
| GPU NVIDIA | Detectada por driver | RTX 3060, driver 591.86 | TensorFlow Windows >=2.11 no usa GPU nativa | Mantener CPU v2 |

Conclusion: no fue necesario reconstruir el entorno. El problema observado en
sesiones anteriores se debia a acceso restringido desde el sandbox al Python
base del venv. Con permiso de ejecucion para `venv/Scripts/python.exe`, el
entorno oficial Windows CPU funciona.

## 2. Python encontrado

Comando ejecutado:

```text
venv/Scripts/python.exe --version
```

Resultado:

```text
Python 3.11.9
```

Version completa capturada por runtime:

```text
3.11.9 (tags/v3.11.9:de54cf5, Apr  2 2024, 10:12:12) [MSC v.1938 64 bit (AMD64)]
```

Sistema reportado por Python:

```text
Windows-10-10.0.26200-SP0
```

## 3. Entorno roto

`venv/pyvenv.cfg` contiene:

```text
home = C:\Users\ADMIN\AppData\Local\Programs\Python\Python311
version = 3.11.9
executable = C:\Users\ADMIN\AppData\Local\Programs\Python\Python311\python.exe
```

Dentro del sandbox, `venv/Scripts/python.exe --version` fallaba con:

```text
No Python at '"C:\Users\ADMIN\AppData\Local\Programs\Python\Python311\python.exe'
```

Fuera del sandbox, con permiso explicito de ejecucion, el mismo comando devuelve
`Python 3.11.9`. Por tanto, el venv no estaba irrecuperable a nivel de sistema.
No se borro ni se reemplazo el entorno viejo.

## 4. CPU/GPU

Fuente documental vigente:

- `v2_reentrenamiento/ENTORNO_COMPUTO.md`

Hardware documentado:

| Componente | Valor |
|---|---|
| CPU | AMD Ryzen 7 5800X 8-Core |
| RAM | 31.93 GB |
| GPU fisica | NVIDIA GeForce RTX 3060 |
| VRAM | 12 GB |
| Driver NVIDIA | 591.86 |

Verificacion directa disponible:

```text
nvidia-smi --query-gpu=name,driver_version,memory.total --format=csv,noheader
NVIDIA GeForce RTX 3060, 591.86, 12288 MiB
```

`Get-CimInstance` devolvio acceso denegado para SO/CPU/GPU desde esta sesion,
por lo que esos detalles se toman de `ENTORNO_COMPUTO.md` y de `nvidia-smi`.

## 5. TensorFlow/Keras

Import runtime verificado:

| Paquete | Version |
|---|---:|
| TensorFlow | 2.21.0 |
| Keras | 3.14.0 |
| NumPy | 2.4.4 |
| pandas | 3.0.2 |
| scikit-learn | 1.8.0 |
| joblib | 1.5.3 |

TensorFlow reporto:

```text
CUDA_BUILD=False
CPUS=[PhysicalDevice(name='/physical_device:CPU:0', device_type='CPU')]
GPUS=[]
BUILD_INFO={'is_cuda_build': False, 'is_rocm_build': False, 'is_tensorrt_build': False, ...}
```

TensorFlow tambien emitio la advertencia esperada de Windows nativo:

```text
TensorFlow GPU support is not available on native Windows for TensorFlow >= 2.11.
```

Esto confirma que el entorno runtime de v2 es CPU, consistente con
`ENTORNO_COMPUTO.md`.

## 6. Entorno oficial v2 creado/reparado

No se creo un entorno nuevo y no se instalo ningun paquete.

Entorno usado:

```text
venv/Scripts/python.exe
```

Razon: el venv oficial ya funcionaba al ejecutarse con acceso al Python base.
Crear `.venv_v2` o reinstalar Python/TensorFlow habria introducido cambios
innecesarios.

## 7. Dependencias finales

Dependencias finales efectivas para esta verificacion:

| Componente | Version/ruta |
|---|---|
| Python | 3.11.9 |
| Interprete | `venv/Scripts/python.exe` |
| TensorFlow | 2.21.0 |
| Keras | 3.14.0 |
| NumPy | 2.4.4 |
| pandas | 3.0.2 |
| scikit-learn | 1.8.0 |
| joblib | 1.5.3 |
| Dispositivo TensorFlow | CPU |
| GPU TensorFlow | ninguna |

## 8. Determinismo D0

Script creado:

```text
v2_reentrenamiento/auditorias/verificar_runtime_gc3_ge.py
```

El script llama `configure_determinism(0)` antes de importar los modulos que
construyen GC3/GE. Resultado:

| Campo | Valor |
|---|---|
| `PYTHONHASHSEED` | `0` |
| `TF_ENABLE_ONEDNN_OPTS` | `0` |
| `TF_DETERMINISTIC_OPS` | `1` |
| TensorFlow cargado previamente | `False` |
| `keras.utils.set_random_seed` usado | `True` |
| `tf.config.experimental.enable_op_determinism()` usado | `True` |

Nota: una importacion aislada de TensorFlow sin D0 mostro que oneDNN queda
activo por defecto. La verificacion runtime oficial de esta auditoria uso D0 y
fijo `TF_ENABLE_ONEDNN_OPTS=0` antes de cargar TensorFlow.

## 9. model.summary GC3

Modelo:

```text
GC3_DualLSTM_BahdanauAttention_v2
```

Resumen runtime:

| Capa | Output shape | Parametros |
|---|---:|---:|
| rama_a | `(None, 6, 4)` | 0 |
| rama_b | `(None, 6, 33)` | 0 |
| lstm_a | `(None, 6, 16)` | 1,344 |
| lstm_b | `(None, 6, 16)` | 3,200 |
| attention_a | `(None, 16)` | 528 |
| attention_b | `(None, 16)` | 528 |
| concatenate | `(None, 32)` | 0 |
| dense_16 | `(None, 16)` | 528 |
| dropout_020 | `(None, 16)` | 0 |
| dense_8 | `(None, 8)` | 136 |
| output | `(None, 1)` | 9 |

Total runtime:

```text
Total params: 6,273
Trainable params: 6,273
Non-trainable params: 0
```

## 10. model.summary GE

Modelo:

```text
GE_DualLSTM_BahdanauAttention_v2
```

Resumen runtime:

| Capa | Output shape | Parametros |
|---|---:|---:|
| rama_a | `(None, 6, 4)` | 0 |
| rama_b | `(None, 6, 39)` | 0 |
| lstm_a | `(None, 6, 16)` | 1,344 |
| lstm_b | `(None, 6, 16)` | 3,584 |
| attention_a | `(None, 16)` | 528 |
| attention_b | `(None, 16)` | 528 |
| concatenate | `(None, 32)` | 0 |
| dense_16 | `(None, 16)` | 528 |
| dropout_020 | `(None, 16)` | 0 |
| dense_8 | `(None, 8)` | 136 |
| output | `(None, 1)` | 9 |

Total runtime:

```text
Total params: 6,657
Trainable params: 6,657
Non-trainable params: 0
```

## 11. count_params GC3

| Valor | Parametros |
|---|---:|
| Esperado por auditoria | 6,273 |
| Obtenido por `model.count_params()` | 6,273 |

Resultado: coincide exactamente.

## 12. count_params GE

| Valor | Parametros |
|---|---:|
| Esperado por auditoria | 6,657 |
| Obtenido por `model.count_params()` | 6,657 |

Resultado: coincide exactamente.

## 13. Forward pass sintetico

No se usaron datos oficiales. No se uso validation. No se uso test.

Forward pass realizado mediante llamada directa al modelo, no mediante
`model.predict()`.

| Modelo | Input A sintetico | Input B sintetico | Output |
|---|---:|---:|---:|
| GC3 | `(2, 6, 4)` | `(2, 6, 33)` | `(2, 1)` |
| GE | `(2, 6, 4)` | `(2, 6, 39)` | `(2, 1)` |

Resultado: shapes correctos.

## 14. Compile/callbacks

Compilacion runtime verificada:

| Elemento | Valor |
|---|---|
| Optimizer | Adam |
| Learning rate | 0.0010000000474974513 |
| Loss | `mse` |
| Metrica auxiliar | `mae` |

Callbacks construidos sin ejecutar entrenamiento:

| Callback | Monitor | Parametros |
|---|---|---|
| EarlyStopping | `val_loss` | `patience=15`, `restore_best_weights=True` |
| ReduceLROnPlateau | `val_loss` | `factor=0.5`, `patience=8`, `min_lr=1e-6` |
| ModelCheckpoint | `val_loss` | `save_best_only=True`, ruta por corrida |

No se ejecuto `fit`; por tanto no se crearon pesos ni checkpoints reales.

## 15. Dispositivos TensorFlow

TensorFlow detecto:

```text
physical_cpu = ["PhysicalDevice(name='/physical_device:CPU:0', device_type='CPU')"]
physical_gpu = []
logical_devices = ["LogicalDevice(name='/device:CPU:0', device_type='CPU')"]
```

Dispositivo efectivo: CPU.

GPU fisica disponible segun `nvidia-smi`: NVIDIA GeForce RTX 3060, pero no
visible para TensorFlow Windows CPU build. No se forzo GPU.

## 16. Limitaciones

- `Get-CimInstance` devolvio acceso denegado; CPU/SO detallados se apoyan en
  `ENTORNO_COMPUTO.md` y en Python runtime.
- `wsl --status` y `wsl -l -v` devolvieron `E_ACCESSDENIED`; WSL no fue usado.
- La verificacion confirma runtime de arquitectura, conteos, callbacks,
  determinismo y forward sintetico. No estima duracion de entrenamiento.
- No se hizo benchmark con epochs.
- No se ejecutaron seeds oficiales.

## 17. Estado para entrenamiento

Criterio de exito:

| Criterio | Estado |
|---|---|
| Python funcional | OK |
| TensorFlow importa | OK |
| Keras importa | OK |
| NumPy compatible | OK |
| Modulos GC3/GE importan | OK |
| GC3 runtime = 6273 parametros | OK |
| GE runtime = 6657 parametros | OK |
| GC3 forward pass correcto | OK |
| GE forward pass correcto | OK |
| Output shape correcto | OK |
| Compile MSE/MAE correcto | OK |
| Callbacks correctos | OK |
| Determinismo D0 compatible | OK |
| No se ejecuto entrenamiento | OK |
| No se utilizo test | OK |

Estado final: entorno runtime tecnicamente listo para una siguiente etapa
autorizada. Esta auditoria no inicia entrenamiento experimental.
