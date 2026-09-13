# HANDOFF DE SESION V2

Cierre: 2026-09-12, America/Lima. Repositorio:
`C:\Machine-learming\Machine-Learning-Multimodal--Agro-NLP-Clima-`.

**LEER ESTE ARCHIVO AL REANUDAR SIN CONTEXTO.** Sustituye el punto de
reanudacion obsoleto de `HANDOFF_CONTEXTO_ACTUAL.md`, que se conserva como
antecedente. Fuente normativa vigente: `DECISIONES_METODOLOGICAS.md`, en
particular el cierre D51-D61 al final. No regresar a diseno/entrenamiento GC2.

## A. Estado general

Naive, SARIMA rolling one-step, XGBoost, GC3 y GE estan completamente
cerrados, evaluados y congelados. Los modelos que requieren entrenamiento
ya fueron entrenados. Comparacion final TEST consolidada para ambos
cultivares, 12 meses 2025-01..2025-12, horizonte t -> t+1.

**NO entrenar, reentrenar, tunear, ejecutar HPO/CV ni modificar arquitectura,
features, scalers, hiperparametros, semillas, modelos o predicciones.** No
repetir evaluaciones predictivas TEST. SHAP sera explicabilidad post-hoc,
no una oportunidad de mejora posterior de modelos.

SARIMA principal es rolling, parametros congelados, append(refit=False),
fit historico TRAIN 2016-07..2023-12; VAL 2024 solo actualiza estado:

- Sutil: exp_002b_sarima_sutil_simple, (1,1,1)(1,1,0,12).
- Dulce: exp_002_sarima_dulce, (1,0,0)(0,1,0,12).

El SARIMA fixed-origin anterior se conserva como complemento de horizonte
largo, no sustituye al rolling de la tabla principal. No repetir el bug
historico forecast(steps=12) dos veces que duplicaba VAL como TEST.

### Fuentes congeladas (rutas relativas a v2_reentrenamiento)

| Bloque | Fuente oficial |
|---|---|
| Naive | experimentos/exp_001_naive_{sutil,dulce}/metricas.json y predicciones.csv |
| SARIMA rolling | resultados_v2_final/evaluacion_test_sarima_rolling/ |
| XGBoost | resultados_v2_final/evaluacion_test_gc2_xgboost/ |
| GC3/GE | resultados_v2_final/evaluacion_test_gc3_ge/ |
| Comparacion | resultados_v2_final/comparacion_final/ |
| Checkpoints GC3/GE | resultados_v2_final/official_gc3_ge/{cultivar}/{GC3,GE}/seed_XX/checkpoint_best.keras |
| Modelos XGBoost | resultados_v2_final/official_gc2_xgboost/{cultivar}/GC2_XGBoost/seed_XX/model.joblib |
| Scalers congelados | resultados_v2_final/scalers/scaler_{cultivar}_v2c.joblib |
| Datos escalados | data/processed/master_dataset_{cultivar}_v2_escalado.csv |

Consultar manifests de cada evaluacion y
`auditorias/AUDITORIA_COMPARACION_FINAL_MODELOS.md` para fuentes/hashes y
comparabilidad ya auditadas. Los checkpoints oficiales llamados
checkpoint_best.keras son los aprobados del entrenamiento previo; no implica
elegir una best seed. No usar v1, pilotos ni technical_pilot.

## B. Resultados principales guardados

Transcripcion redondeada a seis decimales de las tres tablas oficiales de
comparacion_final, sin recalcular metricas ni predicciones. XGBoost/GC3/GE:
medias de metricas de 10 seeds, **no metricas de una prediccion ensemble**.
Naive/SARIMA: una corrida determinista, sin SD artificial. Las columnas SD
completas siguen en los CSV; representan sensibilidad entre seeds, no
intervalos de confianza temporal.

### Globales D35

Fuente: `resultados_v2_final/comparacion_final/tabla_maestra_modelos.csv`.
MAE/RMSE en toneladas; orden D35 preservado.

| Cultivar | Modelo | MAE | RMSE | RelMAE_N1 | MASE_1 | RMSSE_1 | R2 |
|---|---|---:|---:|---:|---:|---:|---:|
| sutil | Naive | 4704.292917 | 6317.411052 | 1.000000 | 1.394042 | 1.458959 | 0.474616 |
| sutil | SARIMA rolling | 3985.016474 | 4894.896368 | 0.847102 | 1.180896 | 1.130440 | 0.684583 |
| sutil | XGBoost | 5902.375361 | 7010.785262 | 1.254679 | 1.749074 | 1.619089 | 0.352848 |
| sutil | GC3 | 7746.000211 | 8684.061051 | 1.646581 | 2.295403 | 2.005519 | -0.034742 |
| sutil | GE | 9426.458661 | 10830.150126 | 2.003799 | 2.793379 | 2.501143 | -0.584054 |
| dulce | Naive | 84.700833 | 97.712081 | 1.000000 | 1.154938 | 1.135512 | 0.668554 |
| dulce | SARIMA rolling | 21.766759 | 31.616495 | 0.256984 | 0.296801 | 0.367415 | 0.965299 |
| dulce | XGBoost | 59.378883 | 77.205668 | 0.701042 | 0.809661 | 0.897207 | 0.792714 |
| dulce | GC3 | 103.413025 | 125.318120 | 1.220921 | 1.410088 | 1.456322 | 0.436190 |
| dulce | GE | 95.584556 | 116.899722 | 1.128496 | 1.303343 | 1.358492 | 0.507517 |

### Shocks D35-b

Fuente: `resultados_v2_final/comparacion_final/tabla_shocks_modelos.csv`.
Mascaras fijas: Sutil Jan/Jul/Nov; Dulce Jan/Feb/Mar 2025. Todas las filas
tienen n_shock=3 y n_nonshock=9. Delta_s en porcentaje, indice descriptivo
de deterioro condicional ante shocks, sin causalidad ni prueba estadistica
de resiliencia.

| Cultivar | Modelo | MAE_global | MAE_shock | MAE_nonshock | Delta_s |
|---|---|---:|---:|---:|---:|
| sutil | Naive | 4704.291667 | 11660.153333 | 2385.671111 | 147.862041 |
| sutil | SARIMA rolling | 3985.016474 | 7824.402904 | 2705.220997 | 96.345560 |
| sutil | XGBoost | 5902.375361 | 8296.896516 | 5104.201642 | 40.615361 |
| sutil | GC3 | 7746.000211 | 5594.109032 | 8463.297270 | -28.104799 |
| sutil | GE | 9426.458661 | 7908.487889 | 9932.448918 | -16.601239 |
| dulce | Naive | 84.700833 | 138.256667 | 66.848889 | 63.229405 |
| dulce | SARIMA rolling | 21.766759 | 11.603702 | 25.154444 | -46.690723 |
| dulce | XGBoost | 59.378883 | 90.104501 | 49.137010 | 51.774420 |
| dulce | GC3 | 103.413025 | 118.944194 | 98.235969 | 13.262077 |
| dulce | GE | 95.584556 | 99.907589 | 94.143546 | 1.903821 |

Conservar los valores guardados: el MAE Naive Sutil de la tabla global
(4704.292917) y el MAE_global de su tabla shock (4704.291667) tienen una
pequena diferencia preexistente entre artefactos. La consolidacion documento
tolerancia de redondeo y_true de 0.01 t. No corregir ni recalcular estas
tablas durante el handoff. Para multi-seed, Delta_s es la media oficial de
los Delta_s por seed; no reconstruirlo desde el cociente de medias.

### Diferencia descriptiva de MAE en meses shock (toneladas)

Fuente: `resultados_v2_final/comparacion_final/tabla_diferencia_error_shock_ton.csv`.
Signo: MAE_shock_modelo - MAE_shock_benchmark; negativo = menor error.

| Cultivar | Modelo | Diferencia vs SARIMA | Diferencia vs XGBoost |
|---|---|---:|---:|
| sutil | GC3 | -2230.293872 | -2702.787484 |
| sutil | GE | 84.084984 | -388.408628 |
| dulce | GC3 | 107.340492 | 28.839693 |
| dulce | GE | 88.303887 | 9.803088 |

Observaciones ya registradas: SARIMA tiene menor MAE global en ambos
cultivares. En Sutil, GC3 tiene menor MAE global y shock que GE; el menor
MAE shock entre los cinco modelos es GC3. En Dulce, GE tiene menor MAE
global y shock que GC3, pero SARIMA tiene el menor MAE shock. No construir
un ganador absoluto ni narrativa favorable a GE por obligacion.

No interpretar diferencias como toneladas ahorradas, beneficio economico
o reduccion de perdidas reales. n_shock=3 limita las descripciones. Ya existe
`comparacion_final/RESUMEN_SHORT_PAPER_RESULTADOS.md`; la discusion cientifica
y el short paper integrado con SHAP siguen pendientes.

## C. SHAP aprobado, todavia sin ejecucion oficial TEST

**SHAP TEST OFICIAL TODAVIA NO EJECUTADO. D51-D61 CERRADAS.**

| ID | Protocolo vigente |
|---|---|
| D51 | GC3, GE, XGBoost; no SHAP forzado Naive/SARIMA |
| D52 | GC3/GE PermutationExplainer, RNG=1729; max_evals GC3=7120, GE=8272. SamplingExplainer nsamples=65536 solo por fallo tecnico real, mismo background |
| D53 | XGBoost TreeExplainer |
| D54 | Todas las seeds 0..9; sin best seed ni seed representativa |
| D55 | 32 referencias TRAIN por cultivar, indices equiespaciados deterministas, mismos entre seeds, guardar indices/hashes |
| D56 | SHAP elemental firmado sample x timestep x feature antes de agregar |
| D57 | Produccion historica, temporalidad, NASA/clima, INDECI, NLP solo GE |
| D58 | Shock/nonshock descriptivo; 3 shocks/9 nonshocks por cultivar |
| D59 | NLP mean absolute SHAP, participacion relativa, ranking de seis variables, shock/nonshock; sin causalidad |
| D60 | Attention solo diagnostico separado si se decide implementarlo mas adelante |
| D61 | Versiones/hashes/background/parametros/shapes/tiempos/seeds/outputs/reconstruccion completos |

Detalles tecnicos aprobados: wrapper flatten(A) seguido de flatten(B),
unflatten exacto; training=False; Permutation con Independent max_samples=32,
link identity, batch_size=1024. XGBoost TreeExplainer con background explicito,
interventional y output raw. Fallback Sampling: min_samples_per_feature=100,
RNG=1729, mismo wrapper/background. Registrar fallo antes de activar el
fallback; no cambiar por rankings, resultados cientificos ni figuras.

Lookback=6: GC3 A=(6,4), B=(6,33), P=222; GE A=(6,4), B=(6,39), P=258.
Guardar elemental GC3=(12,6,37), GE=(12,6,43) por seed; XGBoost=(12,37).
Features desde `src/models/features_gc3_ge.py` y `features_gc2_xgboost.py`.
Las 6 NLP lagged de GE: avg_sentiment_lag1/lag3/lag6 y
n_noticias_lag1/lag3/lag6; ninguna NLP contemporanea.

Background: indices = rint(linspace(0,n_train-1,32)). Para GC3/GE n_train=84
ventanas con objetivos 2017-01..2023-12; para XGBoost n_train=89 pares con
objetivos 2016-08..2023-12. **Indices exactos y seis hashes aprobados estan
en DECISIONES_METODOLOGICAS.md, D55**, y en el JSON del benchmark. No tomar
primeros 32 ni usar VAL/TEST como referencias. No reescalar/refittear.

### Evidencia y justificacion tecnica anterior al TEST SHAP

Benchmark solo TRAIN+VAL, seed 0 centinela, ambos cultivares, enero/julio
2024 como dos muestras VAL por modelo. No uso TEST para generar SHAP ni
para seleccionar el metodo. 156 corridas neuronales, 12 TreeExplainer:
error maximo neuronal 1.30e-7 escalado; mismo RNG da SHAP identico;
rho elemental Permutation entre RNG=0.9835..0.9920. Sin warnings/excepciones
de explainers. Coste extrapolado de 480 explicaciones neuronales ~10.05 min
mas IO/informes; reservar ~10-15 min en el mismo equipo. Tree 240 muestras:
~0.09 s computo + ~0.29 s setup, carga/imports/IO adicionales.

Se selecciono Permutation por compatibilidad, reproducibilidad, estabilidad,
fidelidad/reconstruccion y coste, ANTES de SHAP oficial TEST. Dos muestras
VAL por centinela no certifican estabilidad para todos los meses/seeds.
Deep/Gradient descartados por incompatibilidad observada; no repetirlos ni
alterar modelos para adaptarlos. Kernel no es el explainer oficial.

### Incidencia del lector y evidencias historicas

El lector anterior podia cargar el CSV completo antes de filtrar TEST;
`test_loaded=False` era una afirmacion demasiado fuerte sobre la carga.
El benchmark final usa csv.DictReader + islice(...,102), lectura acotada a
102 registros TRAIN+VAL, fechas exactas 2016-07..2024-12; no solicita la fila
103. **Mantener esta restriccion en futuros scripts de preflight.** El hash
binario completo es solo control de integridad, no analisis de datos TEST.

Esto no produjo SHAP TEST oficial, no modifico modelos ni predicciones y
no intervino en seleccion por resultados TEST. No volver al lector previo.

Evidencias en `auditorias/`:

- AUDITORIA_METODOLOGICA_SHAP_V2.md: protocolo vigente cerrado.
- AUDITORIA_BENCHMARK_EXPLAINER_SHAP_V2.md y shap_v2_explainer_benchmark.json:
  evidencia historica previa a la aprobacion; su etiqueta pendiente se
  conserva por trazabilidad y queda superada por este cierre D51-D61.
- shap_v2_benchmark_arrays/: NPZ tecnicos TRAIN/VAL, no resultados oficiales.
- benchmark_explainers_shap_v2.py: referencia del wrapper/lector/probes.
- resumir_benchmark_shap_v2.py y auditar_shap_v2_preflight.py: generadores
  historicos; **no reejecutarlos para regenerar el cierre**, pues escriben
  documentos o metadatos con el estado metodologico anterior.

Entorno observado: Windows, Python 3.11.9, shap 0.51.0, TensorFlow 2.21.0,
Keras 3.14.0, XGBoost 3.2.0. Usar venv\Scripts\python.exe. Inference CPU:
TF_ENABLE_ONEDNN_OPTS=0, TF_DETERMINISTIC_OPS=1, intra/inter threads=1.
Warnings de carga Keras/BahdanauAttention y GPU TensorFlow nativa quedaron
documentados; no justifican modificar capas/pesos.

## D. Siguiente paso exacto

La proxima sesion debe comenzar con:

**AUDITORIA/PREFLIGHT DEL SCRIPT DE EJECUCION SHAP OFICIAL**

1. Leer este handoff, D51-D61 y la auditoria SHAP vigente. No reabrir la
   seleccion del explainer/background ni de modelos. El runner oficial aun
   debe prepararse/auditarse; el benchmark no es el runner de produccion.
2. Preparar y auditar el runner con preflight exclusivamente TRAIN+VAL:
   lector acotado, 60 modelos oficiales, seeds completas, shapes/nombres,
   wrapper, background TRAIN y sus indices/hashes, RNG/parametros,
   reconstruccion y hashes inmutables. Distinguir prueba tecnica de outputs
   oficiales; impedir sobrescrituras o una ejecucion duplicada silenciosa.
3. Solo tras verificar ese preflight, ejecutar SHAP post-hoc del TEST 2025
   congelado bajo D51-D61. Esta secuencia es la continuacion indicada por el
   usuario, para la proxima sesion; no ejecutar durante el cierre de hoy.

| Modelo | Cultivares | Seeds por cultivar | Meses TEST | Explicaciones |
|---|---:|---:|---:|---:|
| GC3 | 2 | 10 | 12 | 240 |
| GE | 2 | 10 | 12 | 240 |
| XGBoost | 2 | 10 | 12 | 240 |
| Total | | | | 720 |

Son 480 explicaciones neuronales y 240 de arboles. Pronosticos a explicar:
objetivos Jan-Dec 2025, origen Dec2024..Nov2025. Background siempre TRAIN.
La inferencia necesaria para SHAP debe explicar la funcion congelada y ser
consistente con las predicciones oficiales; no reemplazar archivos de
prediccion ni producir una nueva evaluacion predictiva para seleccion.

Salida prevista: `resultados_v2_final/shap/{cultivar}/{model}/seed_XX/`,
mas manifest/auditoria oficiales; registrar entradas, bases, SHAP firmado,
shapes, fechas, unidades, parametros y reconstruccion. En el cierre actual
no existe la carpeta oficial shap. No confundir arrays del benchmark con
los resultados oficiales ni explicar solo seed 0.

SHAP nunca habilitara modificar posteriormente arquitectura, features,
hiperparametros, seeds, modelos ni predicciones. Ante fallo critico detener
y documentar; fallback solo por fallo tecnico real, sin reinterpretacion
o ajuste para favorecer NLP/clima/shocks.

## E. Despues de SHAP

Pendiente: agregacion por feature, timestep/lag, grupo, NLP y shock/nonshock;
figuras con proposito cientifico; interpretacion cientifica; short paper
integrado; actualizacion posterior del PPI; simulacion economica solo si
se mantiene en el alcance final. No escribir ahora conclusiones finales
de tesis. Attention opcional exige decision posterior de implementacion.

Conservar primero valores elementales firmados. Agregados documentados:
feature = mean_sample(sum_timestep(abs(phi))); timestep =
mean_sample(sum_feature(abs(phi))); grupo = suma de importancia de features
del grupo. No intercambiar sum(abs(phi)) con abs(sum(phi)). No confundir lag
nominal de cada feature con posicion temporal del lookback.

D35 y D35-b siguen congeladas. Denominadores TRAIN-only: Sutil D_MASE1=3374.5715,
D_RMSSE1=18749601.6694; Dulce D_MASE1=73.3380, D_RMSSE1=7404.7927.
Delta_s=((MAE_shock-MAE_global)/MAE_global)*100, calculado por seed antes
de resumir. No recalcular para este handoff ni modificar mascaras.

## F. Git y entorno

Rama: `main...origin/main`. HEAD observado:
`44fe6eafca927ba22d97bf8e46d6ad530cc1a47a`. No se hizo commit ni push.
Resumen de `git status -sb`: DECISIONES_METODOLOGICAS.md modificado;
HANDOFF_SESION.md y auditorias/artefactos de trabajo sin seguimiento,
junto a numerosos archivos preexistentes. AUDITORIA_METODOLOGICA_SHAP_V2.md
tambien esta untracked aunque se actualizo su contenido.

Archivos untracked ajenos al experimento incluyen `.claude/`,
`data/interim/indeci/`, `data/interim/nasa/`, `sources/noticias-ampliado/`,
`sources/agraria-pe/sin-unificar/`, scripts en `src/scraping/` y
`src/weather/nasa_power_downloader.py`. No incluirlos accidentalmente en
un futuro commit; no hacer git add -A ni limpiar/revertir cambios del usuario.
Auditorias/evaluaciones/modelos oficiales tambien estan untracked: no
eliminarlos. Un futuro staging debe revisarse archivo por archivo y contar
con autorizacion para commit/push.

Warning conocido no bloqueante:

```text
warning: unable to access 'C:\Users\ADMIN/.config/git/ignore': Permission denied
```

Incidencia de entorno, no de los modelos. No cambiar configuracion Git ni
permisos para resolverla durante este cierre. Mostrar de nuevo git status -sb
al terminar la proxima tarea; preservar cambios ajenos.

## G. Verificacion del cierre

Cierre exclusivamente documental: DECISIONES_METODOLOGICAS.md,
auditorias/AUDITORIA_METODOLOGICA_SHAP_V2.md y este HANDOFF_SESION.md.
No SHAP TEST, entrenamiento, inferencia de modelos, modificacion de
predicciones ni commit/push. Las metricas se leyeron de tablas oficiales,
no se recalcularon. D51-D61 documentadas como CERRADAS; siguiente paso exacto
incluido arriba. Verificacion de integridad mediante SHA256 antes/despues
de 830 archivos de resultados, datasets, experimentos, codigo de modelos/
training y evidencias tecnicas SHAP. Los informes historicos conservan sus
hashes y su estado de aprobacion de aquel momento.
