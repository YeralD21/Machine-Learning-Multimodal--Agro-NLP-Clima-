# AUDITORIA BENCHMARK EXPLAINER SHAP V2

Estado: COMPLETADO. D52 Y D55 PENDIENTES DE APROBACION FINAL.

## Alcance y controles

Ejecucion UTC: 2026-09-13T02:15:29.031340+00:00 a 2026-09-13T02:18:31.920069+00:00; 182.88 s de benchmark.
Cuatro checkpoints oficiales: GC3/GE, Sutil/Dulce, seed 0 como centinela tecnico. Inventario verificado de 40 checkpoints neuronales y 20 XGBoost. No seleccion por desempeno.
Mismas dos muestras por metodo: objetivos VAL 2024-01 y 2024-07 (indices 0 y 6). Contextos respectivos: 2023-07..12 y 2024-01..06. Regla por calendario, sin examinar resultados.
156 corridas neuronales = 52 configuraciones x 3 repeticiones = 312 explicaciones tecnicas de muestras VAL repetidas. TreeExplainer: 12 corridas, 24 explicaciones tecnicas VAL. Cero explicaciones TEST.
Lectura acotada con csv.DictReader + islice(..., 102): se parsean solo 2016-07..2024-12, calendario exacto validado. El hashing binario del CSV completo solo verifica integridad; no materializa ni explica TEST.
No entrenamiento/reentrenamiento, HPO, CV, cambio de features/scalers/pesos ni seleccion de seeds. Hashes antes/despues iguales y pesos en memoria invariantes. No commit ni push.

## Correccion del antecedente

El pre-flight anterior usaba pd.read_csv del CSV completo y filtraba despues. Por ello su test_loaded=False no demostraba ausencia de carga de TEST. Este benchmark no llama a ese lector; reutiliza solo su constructor puro de secuencias TRAIN/VAL. No se atribuyen explicaciones oficiales TEST al antecedente. El hallazgo queda registrado sin reescribir sus evidencias.

## Configuracion comun

Entorno: Python 3.11.9; SHAP 0.51.0; TensorFlow 2.21.0; Keras 3.14.0; NumPy 2.4.4; XGBoost 3.2.0.
Equipo: AMD64 Family 25 Model 33 Stepping 2, AuthenticAMD; 16 CPUs logicas; RAM 31.93 GiB. Inferencia CPU, TensorFlow intra/inter=1, oneDNN=0, ops deterministas=1.
Wrapper comun: concat(flatten(A), flatten(B)); tf.function llama al mismo modelo con training=False y lotes de hasta 1024. Prediccion verificada contra llamada directa; sin recompilar/entrenar ni guardar modelos.
Shapes SHAP: GC3 flat=(2,222), elemental=(2,6,37); GE flat=(2,258), elemental=(2,6,43); Tree=(2,37). Valores firmados, base y predicciones en unidades del target escalado. No se aplica inverse_transform al benchmark.
Semillas del explainer: 1729 referencia, 1729 repeticion exacta, 1730 sensibilidad Monte Carlo. Son distintas de las seeds de entrenamiento. Se reinicializa el explainer en cada corrida y se mantiene el mismo orden de muestras.
Kernel: nsamples=2048; ampliacion=4096 en N=32; l1_reg=0.0, sin seleccion de features. Sampling: nsamples=16384; ampliacion=65536 en N=32; min_samples_per_feature=100. Permutation: max_evals=4*(2*P+1); ampliacion=16*(2*P+1) en N=32; Independent(max_samples=N), batch_size=1024. P es 222/258. Presupuestos no equivalen al mismo numero de filas de inferencia; el JSON registra las filas realmente evaluadas.
Sampling ademas probo TRAIN completo N=84 con nsamples=16384. No se repitieron Deep/Gradient: permanecen descartados por el StagingError registrado previamente.

## Background

84 ventanas neuronales TRAIN, objetivos 2017-01..2023-12; cada ventana conserva sus seis filas cronologicas. Indices cero-based = rint(linspace(0,83,N)). Seleccion por cultivar; GC3/GE comparten fechas y las 37 variables comunes, verificadas iguales. No primeros N, clustering ni busqueda de muestras.

| N | Indices exactos (o rango completo) | Cobertura de meses objetivo |
|---:|---|---|
| 8 | [0, 12, 24, 36, 47, 59, 71, 83] | 2/12 |
| 16 | [0, 6, 11, 17, 22, 28, 33, 39, 44, 50, 55, 61, 66, 72, 77, 83] | 12/12 |
| 32 | [0, 3, 5, 8, 11, 13, 16, 19, 21, 24, 27, 29, 32, 35, 37, 40, 43, 46, 48, 51, 54, 56, 59, 62, 64, 67, 70, 72, 75, 78, 80, 83] | 12/12 |
| 84 | 0..83, todos | 12/12 |

N=8 concentra objetivos en enero/diciembre y sus contextos no cubren todos los meses al mismo lag. N=16 y N=32 cubren los doce meses objetivo; no son muestras estacionalmente balanceadas. N=32 amplia la cobertura de ventanas, manteniendo coste viable. Cambiar background cambia la referencia matematica: no se exige identidad de atribuciones.

## Resultados completos por configuracion

Tiempo/muestra: media de tres corridas, dos muestras cada una. rho E/F: Spearman del ranking mean absolute SHAP elemental/por feature entre RNG 1729 y 1730. No son p-values ni comparaciones de importancia cientifica. Alto = presupuesto ampliado; base = presupuesto comun. Memoria es pico RSS del proceso, no memoria aislada del explainer.

| Cultivar | Modelo | Metodo | N | Presupuesto | s/muestra | s total 3 corridas | RSS MiB | Error reconstruccion max | rho E | rho F | Warnings |
|---|---|---|---:|---|---:|---:|---:|---:|---:|---:|---:|
| sutil | GC3 | Kernel | 8 | base | 0.1706 | 1.041 | 500.3 | 1.192e-07 | 0.8968 | 0.9734 | 0 |
| sutil | GC3 | Sampling | 8 | base | 0.4986 | 3.006 | 474.2 | 1.248e-07 | 0.9845 | 0.9900 | 0 |
| sutil | GC3 | Permutation | 8 | base | 0.8682 | 5.221 | 580.2 | 1.304e-07 | 0.9830 | 0.9813 | 0 |
| sutil | GC3 | Kernel | 16 | base | 0.2008 | 1.220 | 610.9 | 1.192e-07 | 0.8843 | 0.9704 | 0 |
| sutil | GC3 | Sampling | 16 | base | 0.5025 | 3.028 | 570.9 | 1.266e-07 | 0.9676 | 0.9817 | 0 |
| sutil | GC3 | Permutation | 16 | base | 0.1451 | 0.883 | 600.9 | 1.248e-07 | 0.9780 | 0.9844 | 0 |
| sutil | GC3 | Kernel | 32 | base | 0.3282 | 1.984 | 639.7 | 1.192e-07 | 0.9029 | 0.9540 | 0 |
| sutil | GC3 | Kernel | 32 | high | 0.6380 | 3.845 | 809.3 | 1.192e-07 | 0.9503 | 0.9910 | 0 |
| sutil | GC3 | Sampling | 32 | base | 0.5058 | 3.049 | 578.5 | 1.159e-07 | 0.9546 | 0.9839 | 0 |
| sutil | GC3 | Sampling | 32 | high | 1.7841 | 10.719 | 602.4 | 1.203e-07 | 0.9769 | 0.9964 | 0 |
| sutil | GC3 | Permutation | 32 | base | 0.2722 | 1.647 | 638.6 | 1.211e-07 | 0.9743 | 0.9841 | 0 |
| sutil | GC3 | Permutation | 32 | high | 1.0727 | 6.448 | 638.6 | 1.211e-07 | 0.9918 | 0.9908 | 0 |
| sutil | GC3 | Sampling | 84 | base | 0.5069 | 3.058 | 578.6 | 1.284e-07 | 0.9413 | 0.9772 | 0 |
| sutil | GE | Kernel | 8 | base | 0.1909 | 1.162 | 609.0 | 5.215e-08 | 0.8002 | 0.9619 | 0 |
| sutil | GE | Sampling | 8 | base | 0.5443 | 3.280 | 580.8 | 5.341e-08 | 0.9511 | 0.9799 | 0 |
| sutil | GE | Permutation | 8 | base | 0.0870 | 0.534 | 603.2 | 5.215e-08 | 0.9541 | 0.9840 | 0 |
| sutil | GE | Kernel | 16 | base | 0.2175 | 1.320 | 626.4 | 5.215e-08 | 0.7807 | 0.9500 | 0 |
| sutil | GE | Sampling | 16 | base | 0.5358 | 3.230 | 582.1 | 5.479e-08 | 0.9134 | 0.9722 | 0 |
| sutil | GE | Permutation | 16 | base | 0.1719 | 1.045 | 623.5 | 5.076e-08 | 0.9494 | 0.9656 | 0 |
| sutil | GE | Kernel | 32 | base | 0.3537 | 2.139 | 651.6 | 5.215e-08 | 0.7457 | 0.9354 | 0 |
| sutil | GE | Kernel | 32 | high | 0.6933 | 4.178 | 849.3 | 5.215e-08 | 0.8574 | 0.9669 | 0 |
| sutil | GE | Sampling | 32 | base | 0.5382 | 3.245 | 584.0 | 6.051e-08 | 0.9089 | 0.9734 | 0 |
| sutil | GE | Sampling | 32 | high | 1.8821 | 11.309 | 610.2 | 5.218e-08 | 0.9592 | 0.9909 | 0 |
| sutil | GE | Permutation | 32 | base | 0.3437 | 2.077 | 669.4 | 5.215e-08 | 0.9472 | 0.9666 | 0 |
| sutil | GE | Permutation | 32 | high | 1.3609 | 8.179 | 669.4 | 5.215e-08 | 0.9835 | 0.9914 | 0 |
| sutil | GE | Sampling | 84 | base | 0.5365 | 3.235 | 587.3 | 5.233e-08 | 0.8853 | 0.9603 | 0 |
| dulce | GC3 | Kernel | 8 | base | 0.1433 | 0.877 | 715.2 | 8.882e-16 | 0.8445 | 0.9711 | 0 |
| dulce | GC3 | Sampling | 8 | base | 0.5065 | 3.054 | 691.5 | 4.400e-09 | 0.9754 | 0.9881 | 0 |
| dulce | GC3 | Permutation | 8 | base | 0.0689 | 0.425 | 706.0 | 1.490e-08 | 0.9553 | 0.9796 | 0 |
| dulce | GC3 | Kernel | 16 | base | 0.2039 | 1.240 | 729.1 | 2.776e-16 | 0.9155 | 0.9791 | 0 |
| dulce | GC3 | Sampling | 16 | base | 0.5104 | 3.078 | 690.9 | 3.522e-09 | 0.9640 | 0.9886 | 0 |
| dulce | GC3 | Permutation | 16 | base | 0.1347 | 0.820 | 720.9 | 3.725e-09 | 0.9716 | 0.9865 | 0 |
| dulce | GC3 | Kernel | 32 | base | 0.3261 | 1.974 | 753.5 | 7.772e-16 | 0.9219 | 0.9749 | 0 |
| dulce | GC3 | Kernel | 32 | high | 0.6482 | 3.907 | 824.7 | 7.772e-16 | 0.9566 | 0.9927 | 0 |
| dulce | GC3 | Sampling | 32 | base | 0.5024 | 3.030 | 692.0 | 2.615e-09 | 0.9543 | 0.9810 | 0 |
| dulce | GC3 | Sampling | 32 | high | 1.7659 | 10.611 | 707.0 | 2.214e-10 | 0.9815 | 0.9950 | 0 |
| dulce | GC3 | Permutation | 32 | base | 0.2673 | 1.618 | 752.0 | 9.313e-10 | 0.9702 | 0.9874 | 0 |
| dulce | GC3 | Permutation | 32 | high | 1.0768 | 6.474 | 752.0 | 9.895e-10 | 0.9920 | 0.9972 | 0 |
| dulce | GC3 | Sampling | 84 | base | 0.5030 | 3.035 | 692.0 | 6.104e-09 | 0.9478 | 0.9877 | 0 |
| dulce | GE | Kernel | 8 | base | 0.1464 | 0.895 | 720.2 | 5.960e-08 | 0.8358 | 0.9555 | 0 |
| dulce | GE | Sampling | 8 | base | 0.5364 | 3.234 | 692.0 | 6.049e-08 | 0.9692 | 0.9811 | 0 |
| dulce | GE | Permutation | 8 | base | 0.0876 | 0.538 | 713.3 | 6.706e-08 | 0.9594 | 0.9737 | 0 |
| dulce | GE | Kernel | 16 | base | 0.2272 | 1.381 | 736.3 | 5.960e-08 | 0.8554 | 0.9636 | 0 |
| dulce | GE | Sampling | 16 | base | 0.5426 | 3.271 | 692.0 | 6.400e-08 | 0.9317 | 0.9784 | 0 |
| dulce | GE | Permutation | 16 | base | 0.1717 | 1.043 | 733.5 | 5.960e-08 | 0.9656 | 0.9870 | 0 |
| dulce | GE | Kernel | 32 | base | 0.4156 | 2.512 | 768.5 | 5.960e-08 | 0.8619 | 0.9751 | 0 |
| dulce | GE | Kernel | 32 | high | 0.6599 | 3.977 | 846.0 | 5.960e-08 | 0.9470 | 0.9852 | 0 |
| dulce | GE | Sampling | 32 | base | 0.5341 | 3.221 | 691.9 | 7.177e-08 | 0.9472 | 0.9773 | 0 |
| dulce | GE | Sampling | 32 | high | 1.8762 | 11.273 | 713.2 | 5.965e-08 | 0.9772 | 0.9959 | 0 |
| dulce | GE | Permutation | 32 | base | 0.3372 | 2.038 | 774.9 | 5.960e-08 | 0.9739 | 0.9888 | 0 |
| dulce | GE | Permutation | 32 | high | 1.3546 | 8.142 | 774.9 | 5.960e-08 | 0.9909 | 0.9959 | 0 |
| dulce | GE | Sampling | 84 | base | 0.5372 | 3.240 | 692.8 | 6.094e-08 | 0.9344 | 0.9814 | 0 |

Todas las 156 corridas tuvieron shapes correctos, salidas finitas y reconstruccion <1e-5 escalado. Mismo RNG/config: diferencia maxima firmada SHAP=0 en las 52 parejas; Tree tambien identico. No warnings de explainers ni excepciones. La reconstruccion es un control de implementacion, no prueba de convergencia del ranking.

El primer Permutation incluye compilacion JIT de SHAP/Numba; sus repeticiones calientes se registran separadas en JSON. Warnings de carga Keras sobre build de BahdanauAttention y mensajes de TensorFlow sobre GPU nativa/placeholder se conservan como incidencias de entorno; no se modifico la capa. Memoria RSS muestreada cada 20 ms incluye TensorFlow y retencion del allocator; puede omitir picos mas breves.

## Sensibilidad al background

Comparaciones al mismo presupuesto base y RNG. Diferencia L1 = sum(abs(I_N1-I_N2))/sum(abs(I_N1)), sobre importancia elemental. Incluye sensibilidad Monte Carlo y cambio de referencia; no estima un error contra SHAP exacto.

| Cultivar | Modelo | Metodo | N1 -> N2 | rho E | rho F | L1 relativa | Cambio base max |
|---|---|---|---|---:|---:|---:|---:|
| sutil | GC3 | Kernel | 8 -> 16 | 0.8166 | 0.8551 | 0.3624 | 0.33027 |
| sutil | GC3 | Sampling | 8 -> 16 | 0.8659 | 0.8983 | 0.3856 | 0.33027 |
| sutil | GC3 | Permutation | 8 -> 16 | 0.8701 | 0.8959 | 0.3859 | 0.33027 |
| sutil | GC3 | Kernel | 16 -> 32 | 0.9594 | 0.9870 | 0.1235 | 0.08143 |
| sutil | GC3 | Sampling | 16 -> 32 | 0.9302 | 0.9798 | 0.2196 | 0.08143 |
| sutil | GC3 | Permutation | 16 -> 32 | 0.9619 | 0.9891 | 0.1293 | 0.08143 |
| sutil | GC3 | Sampling | 32 -> 84 | 0.9181 | 0.9813 | 0.2083 | 0.03280 |
| sutil | GE | Kernel | 8 -> 16 | 0.7297 | 0.8744 | 0.3801 | 0.01238 |
| sutil | GE | Sampling | 8 -> 16 | 0.8209 | 0.8963 | 0.3931 | 0.01238 |
| sutil | GE | Permutation | 8 -> 16 | 0.8402 | 0.9210 | 0.3838 | 0.01238 |
| sutil | GE | Kernel | 16 -> 32 | 0.9288 | 0.9857 | 0.1545 | 0.01567 |
| sutil | GE | Sampling | 16 -> 32 | 0.8711 | 0.9683 | 0.2858 | 0.01567 |
| sutil | GE | Permutation | 16 -> 32 | 0.9438 | 0.9857 | 0.1605 | 0.01567 |
| sutil | GE | Sampling | 32 -> 84 | 0.8798 | 0.9819 | 0.2486 | 0.00374 |
| dulce | GC3 | Kernel | 8 -> 16 | 0.7501 | 0.8326 | 0.4477 | 1.07007 |
| dulce | GC3 | Sampling | 8 -> 16 | 0.8348 | 0.8331 | 0.4495 | 1.07007 |
| dulce | GC3 | Permutation | 8 -> 16 | 0.8385 | 0.8336 | 0.4543 | 1.07007 |
| dulce | GC3 | Kernel | 16 -> 32 | 0.9587 | 0.9889 | 0.1110 | 0.02272 |
| dulce | GC3 | Sampling | 16 -> 32 | 0.9315 | 0.9780 | 0.1916 | 0.02272 |
| dulce | GC3 | Permutation | 16 -> 32 | 0.9563 | 0.9865 | 0.1110 | 0.02272 |
| dulce | GC3 | Sampling | 32 -> 84 | 0.9322 | 0.9772 | 0.1873 | 0.04855 |
| dulce | GE | Kernel | 8 -> 16 | 0.7414 | 0.8460 | 0.4153 | 1.07231 |
| dulce | GE | Sampling | 8 -> 16 | 0.8185 | 0.8658 | 0.4364 | 1.07231 |
| dulce | GE | Permutation | 8 -> 16 | 0.8384 | 0.8620 | 0.4224 | 1.07231 |
| dulce | GE | Kernel | 16 -> 32 | 0.9427 | 0.9867 | 0.1173 | 0.02125 |
| dulce | GE | Sampling | 16 -> 32 | 0.9197 | 0.9816 | 0.2158 | 0.02125 |
| dulce | GE | Permutation | 16 -> 32 | 0.9480 | 0.9894 | 0.1111 | 0.02125 |
| dulce | GE | Sampling | 32 -> 84 | 0.9331 | 0.9802 | 0.2049 | 0.04919 |

Las comparaciones de presupuesto base->alto a N=32, todas las parejas de backgrounds, top-10 overlap y diferencias firmadas completas estan en comparisons del JSON. Solo se comparan indices de rankings, sin decidir por los nombres de las variables.

## D52 propuesta

Recomendar UN metodo oficial: PermutationExplainer, Independent(background, max_samples=32), link identity, RNG=1729, max_evals=16*(2*P+1): GC3=7120; GE=8272; batch_size=1024. Wrapper probado, sin alterar pesos ni inputs del modelo. Mantener el presupuesto para todos los meses/cultivares/seeds, sin ajustes por narrativas SHAP.
Motivo: compatible en los cuatro centinelas, reproducibilidad exacta, reconstruccion finita y mayor estabilidad elemental entre RNG que Kernel/Sampling con los presupuestos altos ensayados. D56 requiere conservar precisamente esa dimension elemental. Kernel es mas rapido pero menos estable en el ranking elemental; no se lo considera incompatible ni singular en este ensayo. Sampling es viable y competitivo, incluyendo TRAIN completo, pero el presupuesto alto consume mas tiempo que Permutation en esta prueba.
Fallback propuesto: SamplingExplainer, mismo background N=32 y wrapper, nsamples=65536, min_samples_per_feature=100, link identity, RNG=1729. Activacion solo por excepcion tecnica, valores no finitos/shapes incorrectos o reconstruccion >1e-5 escalado. Detener y documentar antes de cambiar el metodo oficial; no mezclar silenciosamente explainers entre seeds ni usar preferencias interpretativas.

## D55 propuesta

Background oficial propuesto: N=32 TRAIN-only por cultivar, indices equiespaciados con la regla exacta anterior; mismas ventanas para GC3/GE y las diez seeds. XGBoost: N=32 filas TRAIN de su bundle oficial de 89 pares, regla rint(linspace(0,88,32)), reutilizada en todas sus seeds; no son ventanas LSTM. No VAL/TEST para background, sin semilla de muestreo.
Justificacion: N=16->32 mantiene rankings por feature cercanos con Permutation y N=32 incorpora el doble de ventanas a coste operativo viable. N=8 tiene cobertura estacional insuficiente. No se afirma que N=32 sea optimo ni equivalente a TRAIN completo. Sampling N=84 confirma viabilidad tecnica de todo TRAIN, pero el fallback conserva N=32 para no cambiar la referencia al cambiar de estimador.

## Coste futuro estimado

480 explicaciones neuronales = 2 cultivares x 2 modelos x 10 seeds x 12 meses. Extrapolacion con presupuesto alto y N=32; incluye carga/calentamiento medidos por modelo y setup por seed. No incluye informes, escritura masiva, imports frios ni margen por otros procesos. No se ejecutaron esas explicaciones.

| Metodo | Explicaciones | Segundos estimados | Minutos |
|---|---:|---:|---:|
| KernelExplainer | 480 | 335.82 | 5.60 |
| SamplingExplainer | 480 | 896.08 | 14.93 |
| PermutationExplainer | 480 | 602.84 | 10.05 |

XGBoost TreeExplainer N=32: 240 explicaciones, computo SHAP estimado 0.090 s + setup 0.286 s; carga de los 20 modelos/imports/IO adicionales no medidos en esa extrapolacion. No promesa de tiempo total subsegundo.
Reservar aproximadamente 10-15 minutos para la opcion Permutation en este equipo con el wrapper medido; el tiempo real puede variar entre seeds, muestras y estado del sistema. Ninguna medicion procede de TEST.

## Limites metodologicos

Dos muestras VAL y una seed de entrenamiento por modelo solo permiten una recomendacion tecnica. No certifican convergencia para todos los meses/seeds. Correlacion alta puede coexistir con diferencias en magnitud; consultar L1 relativa y valores firmados guardados. La estabilidad de background se midio al presupuesto base, no al presupuesto alto para todos los N.
Los tres metodos son model-agnostic y usan perturbaciones marginales. Pueden romper dependencias entre lags duplicados y secuencias, generando combinaciones fuera de la distribucion observada. Las atribuciones responden a ese juego de enmascaramiento, no a una distribucion condicional temporal ni a causalidad. Las bases dependen del cultivar/modelo/background.
Kernel/Sampling incorporan restricciones/correcciones de suma; Permutation acumula diferencias telescopicas. Aditividad pequena por si sola no demuestra una estimacion precisa de cada Shapley value.
Para el futuro: explicar las 10 seeds, congelar primero D52/D55, preservar sample x timestep x feature y signed SHAP. mean|SHAP| por feature = media sobre muestras de la suma de absolutos sobre tiempo; por timestep = media de suma sobre features; por grupo = suma de importancia de sus features. Esto difiere de abs(sum(SHAP)); no confundir cancelacion local con importancia global. NLP/attention/shocks siguen las decisiones ya cerradas, sin interpretacion oficial en este benchmark.

## Fuentes y reproducibilidad

- Codigo ejecutado: `benchmark_explainers_shap_v2.py`; reporte/verificacion: `resumir_benchmark_shap_v2.py`.
- Fuente de ventanas: constructor puro TRAIN/VAL del pre-flight, contrastado con `src/training/sequences_gc3_ge.py`; nombres y orden reutilizados del builder oficial y cotejados con 40 configs. Ningun feature recalculado.
- `shap_v2_explainer_benchmark.json`: versiones, git hash, hashes de datasets/scalers/checkpoints/configs/codigo instalado SHAP, indices/fechas/hashes de backgrounds, warnings, errores, tiempos, RSS, parametros, bases, predicciones, reconstruccion y comparaciones por corrida.
- `shap_v2_benchmark_arrays/`: arrays tecnicos TRAIN/VAL NPZ con hashes; SHAP firmado flat/elemental, base, prediction, residual, backgrounds y muestras. No son resultados oficiales ni se escriben en resultados_v2_final/shap/.
- Git HEAD observado: `44fe6eafca927ba22d97bf8e46d6ad530cc1a47a`; integridad de 144 archivos congelados verificada.
- SHA256 codigo benchmark: `155ef53968b4908020e003ba4abddc82ac5f1fbc4e7ed4b3abca5a85870bb65a`.
- SHA256 JSON final: `c29b4ffa1fa7b03daa15db52a770214ba92587fd4d152cbaa3cd82cea7f18641`.
- [KernelExplainer oficial](https://shap.readthedocs.io/en/stable/generated/shap.KernelExplainer.html): regresion ponderada; contrastada con _kernel.py local SHAP 0.51.0.
- [SamplingExplainer oficial](https://shap.readthedocs.io/en/stable/generated/shap.SamplingExplainer.html): muestreo model-agnostic del background. Se fijan nsamples explicitamente porque explain() local usa auto=1000*M, distinto de la descripcion heredada de shap_values.
- [PermutationExplainer oficial](https://shap.readthedocs.io/en/stable/generated/shap.PermutationExplainer.html): recorridos antiteticos y seed; se usa __call__(max_evals=...), no la interfaz legacy shap_values.

## Decisiones para aprobacion

| ID | Decision | Alternativas | Recomendacion | Justificacion | Estado |
|---|---|---|---|---|---|
| D52 | Explainer GC3/GE | Kernel / Sampling / Permutation | Permutation, 16*(2*P+1); fallback Sampling 65536 | Estabilidad elemental, fidelidad y coste medidos en cuatro centinelas | PENDIENTE DE APROBACION FINAL |
| D55 | Background | 8 / 16 / 32 TRAIN; Sampling 84 | 32 TRAIN equiespaciados por cultivar | Cobertura, sensibilidad y coste; misma referencia en fallback | PENDIENTE DE APROBACION FINAL |

D51, D53, D54 y D56-D61 estan CERRADAS por autorizacion expresa del usuario. Esta recomendacion no cierra D52/D55 ni autoriza SHAP oficial TEST.
