# Entorno de cómputo — v2 Reentrenamiento

**Fecha de captura:** 2026-09-04 08:20
**Actualizado:** 2026-09-04 (hallazgo del entorno WSL2 de v1)
**Capturado antes de:** diseño y entrenamiento de GC2 (SARIMAX + LSTM)
**Propósito:** trazabilidad y reproducibilidad de los experimentos de Fase 3.

---

## Resumen ejecutivo — LEER ANTES DE ENTRENAR

**v2 entrena en CPU, en el venv de Windows. La GPU no se usa — por decisión,
no por imposibilidad.**

Existen DOS entornos funcionales en esta máquina, ambos con TensorFlow
2.21.0. La diferencia entre ellos es el sistema operativo, no la versión del
framework:

| | **Windows venv** (v2 usa este) | **WSL2 `tf-gpu-311`** (v1 usó este) |
|---|---|---|
| Python | 3.11.9 | 3.11.15 |
| TensorFlow | 2.21.0 — build **CPU** | 2.21.0 — build **CUDA** |
| `is_built_with_cuda` | `False` | `True` |
| CUDA / cuDNN | no aplica | **12.5.1 / 9** |
| GPU visible | `[]` | `/physical_device:GPU:0` |
| Keras | 3.14.0 | 3.14.1 |
| NumPy | 2.4.4 | 2.4.6 |
| Pandas | 3.0.2 | 3.0.3 |

**Por qué el venv de Windows no ve la GPU** — dos causas independientes:

1. **TensorFlow ≥ 2.11 no soporta GPU en Windows nativo**, ni aunque
   CUDA/cuDNN estuvieran instalados. El propio TF lo advierte al importarse:
   *"TensorFlow GPU support is not available on native Windows for
   TensorFlow >= 2.11. Even if CUDA/cuDNN are installed, GPU will not be
   used. Please use WSL2 or the TensorFlow-DirectML plugin."*
   Además, este build concreto es CPU-only: `tf.sysconfig.get_build_info()`
   no expone `cuda_version` ni `cudnn_version` porque no existen en él.
2. **PyTorch está instalado como rueda `+cpu`.** No es limitación de
   plataforma — existe rueda CUDA para Windows; se instaló la variante CPU.
   (PyTorch no lo usa este proyecto; se documenta para descartar ambigüedad.)

**No hay CUDA Toolkit ni cuDNN instalados en Windows** (`nvcc` no está en
PATH, no existe `C:\Program Files\NVIDIA GPU Computing Toolkit\CUDA\`). La
"CUDA Version: 13.1" que reporta `nvidia-smi` es la versión **máxima
soportada por el driver**, no un toolkit instalado. El stack CUDA real vive
dentro del venv de WSL2, como paquetes pip (`nvidia-cudnn-cu12`, etc.).

**Decisión para v2: entrenar TODO en el venv de Windows (CPU).** Con 90
observaciones de entrenamiento y arquitecturas LSTM pequeñas, la GPU no
aporta ventaja apreciable — el sobrecoste de transferencia host↔device
domina. Lo que sí aporta es **un único entorno numérico para toda la Fase
3**, que es precisamente lo que faltó en v1 (ver la sección de limitación
metodológica más abajo).

Consecuencia práctica: **ninguna métrica de tiempo de entrenamiento de v2
debe atribuirse a aceleración por GPU.**

---

## Hardware

| Componente | Detalle |
|---|---|
| CPU | AMD Ryzen 7 5800X 8-Core |
| Núcleos | 8 físicos / 16 lógicos @ 3801 MHz |
| RAM | 31.93 GB |
| GPU | NVIDIA GeForce RTX 3060 |
| VRAM | 12 288 MiB (12 GB) |
| Driver NVIDIA | 591.86 |
| CUDA máx. soportada por driver | 13.1 |
| Modo del driver | WDDM |
| Límite de potencia GPU | 170 W |

Nota: la GPU está ocupada por procesos de escritorio (~1.3 GB de VRAM en
uso por Chrome, Explorer, WebView2, etc.). No es un entorno de cómputo
dedicado.

## Sistema operativo

| | |
|---|---|
| SO | Microsoft Windows 11 Pro |
| Build | 26200 (64 bits) |
| WSL | instalado — `Ubuntu` (WSL2), `docker-desktop` (WSL2) |

---

## Entorno PRIMARIO de v2 — venv de Windows

| | |
|---|---|
| Python | **3.11.9** |
| Intérprete | `C:\Machine-learming\Machine-Learning-Multimodal--Agro-NLP-Clima-\venv\Scripts\python.exe` |
| Paquetes instalados | 240 |

No hay Python en el PATH del sistema (el alias de ejecución de Windows
apunta a Microsoft Store). **Todo debe invocarse con el intérprete del
venv**, como ya indica CLAUDE.md.

### TensorFlow / Keras — framework en uso

    TensorFlow: 2.21.0
    CUDA disponible: False
    GPUs detectadas: []
    Version CUDA usada por TF: NO PRESENTE en build_info
    Version cuDNN usada por TF: NO PRESENTE en build_info
    --- build_info completo ---
       is_cuda_build: False
       is_rocm_build: False
       is_tensorrt_build: False
       msvcp_dll_names: msvcp140.dll,msvcp140_1.dll

Backend de Keras: `tensorflow` (`KERAS_BACKEND` no definida — default).

### PyTorch — instalado, NO usado por el proyecto

    PyTorch: 2.11.0+cpu
    CUDA disponible: False
    Version CUDA: None
    GPU: N/A

### Librerías clave (Windows venv)

| Paquete | Versión |
|---|---|
| tensorflow | 2.21.0 |
| keras | 3.14.0 |
| torch | 2.11.0+cpu |
| statsmodels | 0.14.6 |
| pmdarima | 2.1.1 |
| prophet | 1.3.0 |
| cmdstanpy | 1.3.0 |
| holidays | 0.97 |
| scikit-learn | 1.8.0 |
| xgboost | 3.2.0 |
| shap | 0.51.0 |
| pandas | 3.0.2 |
| numpy | 2.4.4 |
| scipy | 1.17.1 |
| joblib | 1.5.3 |
| transformers | 5.5.4 |
| pysentimiento | 0.7.3 |
| matplotlib | 3.10.8 |
| seaborn | 0.13.2 |
| plotly | 6.7.0 |
| streamlit | 1.56.0 |
| nbconvert | 7.17.1 |

`statsmodels 0.14.6` produjo los SARIMA de GC1 y producirá los SARIMAX de
GC2. `prophet 1.3.0` usa backend `cmdstanpy 1.3.0`.

---

## Entorno alternativo WSL2 (usado en v1)

Verificado en vivo el 2026-09-04. **Existe, está operativo y tiene acceso
real a la GPU.** No se usa en v2 (ver decisión en el resumen ejecutivo).

| | |
|---|---|
| Distro | **Ubuntu 26.04 LTS** sobre WSL2 |
| Estado habitual | `Stopped` — se levanta bajo demanda |
| venv | `/home/yerald/tf-gpu-311` (existe también `/home/yerald/tf-gpu`) |
| Python del venv | **3.11.15** |
| Python del sistema WSL | 3.14.4 (demasiado nuevo para el proyecto; NO usar) |
| Paquetes instalados | 160 |

### Verificación de GPU dentro de WSL2

    Python 3.11.15
    TensorFlow: 2.21.0
    CUDA build: True
    GPUs: [PhysicalDevice(name='/physical_device:GPU:0', device_type='GPU')]
    CUDA: 12.5.1
    cuDNN: 9

`nvidia-smi` dentro de WSL2 reporta `NVIDIA GeForce RTX 3060, 591.86` — el
driver de Windows se expone a WSL2 sin instalar driver adicional en Linux.
El stack CUDA se resuelve por paquetes pip dentro del venv:

| Paquete CUDA | Versión |
|---|---|
| nvidia-cudnn-cu12 | 9.22.0.52 |
| nvidia-cublas-cu12 | 12.9.2.10 |

### Librerías clave (WSL2 `tf-gpu-311`) — nótense las diferencias de patch

| Paquete | **WSL2** | Windows venv | ¿Difiere? |
|---|---|---|---|
| tensorflow | 2.21.0 | 2.21.0 | — |
| keras | **3.14.1** | 3.14.0 | ✅ |
| numpy | **2.4.6** | 2.4.4 | ✅ |
| pandas | **3.0.3** | 3.0.2 | ✅ |
| scikit-learn | 1.8.0 | 1.8.0 | — |
| statsmodels | 0.14.6 | 0.14.6 | — |
| prophet | 1.3.0 | 1.3.0 | — |
| xgboost | 3.2.0 | 3.2.0 | — |
| shap | 0.51.0 | 0.51.0 | — |
| keras-tcn | 3.5.6 | (ausente) | ✅ |

Estas diferencias de patch son las que permitieron identificar por huella
qué notebook de v1 corrió en cada entorno (ver sección siguiente).

### Cómo activarlo (solo si alguna vez se necesita)

    wsl -d Ubuntu -- bash -lc 'source $HOME/tf-gpu-311/bin/activate && python ...'

Las rutas del proyecto se ven desde WSL2 como
`/mnt/c/Machine-learming/Machine-Learning-Multimodal--Agro-NLP-Clima-`.

---

## Limitación metodológica de v1 — comparación GE vs. GM en entornos distintos

**Hallazgo (2026-09-04):** durante la documentación del entorno de
cómputo de v2, se determinó por evidencia forense (versiones de
patch impresas en los notebooks `_ejecutado` de v1) que GE
(`actividad_14_ge_lstm_attention.ipynb`) se entrenó en CPU sobre
Windows nativo, mientras que GM
(`actividad_15_ejecutado.ipynb`) se entrenó en GPU sobre WSL2 Ubuntu
— con versiones de patch distintas de TensorFlow/Keras/NumPy/Pandas
entre ambos entornos.

**Implicación:** la comparación central de v1 (¿aporta valor el canal
NLP? GE=0.0673 vs GM\_v3=0.0645 MAE, diferencia relativa ~4%) se
realizó cruzando dos entornos numéricos distintos. Diferencias de
orden de operaciones en punto flotante (oneDNN, backend BLAS) pueden
introducir variación en la última cifra de métricas — el README de
v1 ya señalaba esto de forma genérica, sin identificar qué modelos
específicos corrieron en cada entorno.

**Alcance de la limitación:** confirmado para GE (CPU) vs. GM (GPU).
El entorno de GM\_v2, XGBoost, TCN, GM\_v3 y GM\_v4 no pudo
determinarse con la evidencia disponible (esos notebooks no
imprimen versión/dispositivo).

**No se afirma que el resultado de v1 sea inválido** — con una
diferencia relativa del ~4%, el efecto del entorno no puede
descartarse como *totalmente* despreciable, pero tampoco hay
evidencia de que explique la diferencia completa. El diseño
experimental de v1 simplemente no permite aislar el efecto del NLP
del efecto del entorno de cómputo.

**Corrección aplicada en v2:** todos los experimentos de Fase 3
(Naive, GC1, GC2, GE, GM) se entrenan en el mismo entorno único
(Windows venv, CPU, TensorFlow 2.21.0), eliminando esta fuente de
confusión por diseño. Esto se documenta como mejora metodológica
explícita respecto a v1, no como corrección de un error de v1 (la
comparación de v1 sigue siendo la que se presentó y defendió en la
jornada científica).

### Evidencia que sustenta el hallazgo

Salidas literales guardadas en los notebooks `_ejecutado` de v1:

**`notebooks/fase4/actividad_15_ejecutado.ipynb` (GM multimodal) → GPU/WSL2**

    TensorFlow  : 2.21.0
    Keras       : 3.14.1
    NumPy       : 2.4.6
    Pandas      : 3.0.3
    GPU detectada: 1 dispositivo(s)
    Dispositivo TF: GPU
    Proyecto    : /mnt/c/Machine-learming/Machine-Learning-Multimodal--Agro-NLP-Clima-

**`notebooks/fase3/actividad_14_ge_lstm_attention.ipynb` (GE) → CPU/Windows**

    TensorFlow  : 2.21.0
    Keras       : 3.14.0
    NumPy       : 2.4.4
    Pandas      : 3.0.2
    Dispositivo TF: CPU

Las ternas Keras/NumPy/Pandas coinciden exactamente con las de cada entorno
verificado hoy. Como triangulación adicional:
`actividad_16_ejecutado.ipynb` y `actividad_17_ejecutado.ipynb` imprimen
rutas `/mnt/c/...`, y `gen_nb_actividad17.py` tiene hardcodeado
`BASE_DIR = pathlib.Path('/mnt/c/Machine-learming/...')`.

El `README.md` de v1 ya declaraba el hecho de forma genérica —
*"el entorno se desarrolló en Windows + WSL2"* y *"GPU/CPU: pequeñas
diferencias numéricas (orden de reducción en GPU) pueden producir
variaciones marginales en la última cifra del MAE respecto a las tablas"*—
pero sin decir qué modelo corrió dónde. Esta sección cierra ese vacío.

---

## Determinismo y reproducibilidad

**oneDNN está ACTIVO** en el build de TensorFlow de Windows. Al importar TF
se emite:

    oneDNN custom operations are on. You may see slightly different
    numerical results due to floating-point round-off errors from different
    computation orders.

`seed=42` fija la inicialización y el barajado, pero **no garantiza
reproducibilidad bit-a-bit entre máquinas** mientras oneDNN reordene las
operaciones. Para GC1 (SARIMAX por MLE y Prophet MAP) no aplica. Para los
modelos LSTM de GC2/GE/GM sí puede haber deriva en los últimos decimales.

Si se requiere determinismo estricto en los LSTM, fijar **antes** de
importar TensorFlow:

    import os
    os.environ['TF_ENABLE_ONEDNN_OPTS'] = '0'
    os.environ['TF_DETERMINISTIC_OPS'] = '1'

Debe declararse en el notebook correspondiente si se activa, porque cambia
el resultado numérico respecto a los entrenamientos ya registrados.
**Decisión pendiente antes de GC2.**

**Determinismo ya verificado (GC1, 2026-09-04):** el re-run completo de
`02_gc1_sarima_prophet.ipynb` reprodujo las métricas de exp_002 y exp_003
byte a byte (p. ej. exp_002_sarima_sutil MAE_test = 12031.692040368267 en
ambas ejecuciones). SARIMAX y Prophet con `mcmc_samples=0` son
deterministas en esta configuración.

---

## Si en el futuro se necesitara GPU

El camino ya está construido: **el venv `tf-gpu-311` de WSL2 funciona hoy**
(ver sección correspondiente). No requiere instalar nada.

Pero cambiar de entorno a mitad de Fase 3 **reintroduciría exactamente la
limitación metodológica que v2 corrige**: GC1 ya está entrenado y
commiteado (`bfd19a7`) en el venv de Windows. Migrar GC2/GE/GM a WSL2
partiría la Fase 3 en dos entornos numéricos, igual que en v1.

Por tanto, si alguna vez se decide usar GPU, la única opción metodológicamente
limpia es **re-ejecutar TODA la Fase 3 en el nuevo entorno** y re-registrar
todos los experimentos, no solo los nuevos.

Otras vías, documentadas para descartarlas:

- **PyTorch con rueda CUDA en Windows nativo** — exigiría portar los modelos
  de Keras a PyTorch. No previsto.
- **TensorFlow-DirectML plugin** — funciona en Windows nativo pero va
  rezagado en versiones de TF y no cubre todas las operaciones.

---

## Comandos de captura

Para regenerar este documento:

    # Windows
    nvidia-smi
    venv\Scripts\python.exe --version
    venv\Scripts\python.exe -m pip freeze
    venv\Scripts\python.exe -c "import tensorflow as tf; print(tf.__version__, tf.test.is_built_with_cuda(), tf.config.list_physical_devices('GPU'), tf.sysconfig.get_build_info())"
    venv\Scripts\python.exe -c "import torch; print(torch.__version__, torch.cuda.is_available(), torch.version.cuda)"

    # WSL2
    wsl -l -v
    wsl -d Ubuntu -- bash -lc 'source $HOME/tf-gpu-311/bin/activate && python --version && python -c "import tensorflow as tf; bi=tf.sysconfig.get_build_info(); print(tf.__version__, tf.config.list_physical_devices(\"GPU\"), bi[\"cuda_version\"], bi[\"cudnn_version\"])"'
    wsl -d Ubuntu -- bash -lc 'source $HOME/tf-gpu-311/bin/activate && pip freeze'
