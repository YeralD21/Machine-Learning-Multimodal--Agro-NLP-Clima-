# AUDITORIA METODOLOGICA SHAP V2

Estado: **D51-D61 CERRADAS**, por aprobacion expresa del usuario al cierre de la sesion del 2026-09-12 (America/Lima).

Revision posterior al micro-benchmark: `AUDITORIA_BENCHMARK_EXPLAINER_SHAP_V2.md` y `shap_v2_explainer_benchmark.json`. Esos dos artefactos conservan su estado historico pendiente y sus hashes; la aprobacion posterior queda registrada aqui y en `../DECISIONES_METODOLOGICAS.md`, seccion D51-D61. **SHAP TEST OFICIAL TODAVIA NO EJECUTADO.**

## Alcance

Esta auditoria disena explicabilidad post-hoc para modelos v2 cerrados. No entrena, no reentrena, no modifica pesos/checkpoints, no cambia features, no cambia scalers, no hace HPO/CV y no selecciona seeds por desempeno.

SHAP se interpretara como atribucion del modelo a sus predicciones, no como causalidad de las variables sobre la produccion.

## Hechos Verificados En Codigo

- Entorno: shap 0.51.0; TensorFlow 2.21.0; Keras 3.14.0; XGBoost 3.2.0.
- GC3/GE: arquitectura Dual-LSTM + Bahdanau Attention en `v2_reentrenamiento/src/models/dual_lstm_attention.py`; dos inputs `rama_a` y `rama_b`; lookback=6; capa custom `BahdanauAttention`.
- Features GC3/GE: `features_gc3_ge.py` define rama A con 4 variables de produccion; rama B GC3=33; rama B GE=39.
- NLP GE: exactamente `avg_sentiment_lag1`, `avg_sentiment_lag3`, `avg_sentiment_lag6`, `n_noticias_lag1`, `n_noticias_lag3`, `n_noticias_lag6`.
- XGBoost: `features_gc2_xgboost.py` define 37 predictores no NLP y `tabular_gc2_xgboost.py` construye X_t -> y_(t+1).
- Artefactos oficiales: 10 seeds completos para GC3, GE y XGBoost en Sutil y Dulce.

## Antecedente Historico v1

- `notebooks/fase4/actividad_16_shap.ipynb` y `gen_nb_actividad16.py` documentan `KernelExplainer` con `shap.kmeans(X_train, 10)` sobre un wrapper dual-input v1. Es antecedente, no protocolo v2 aprobado.
- `generar_shap_shocks.py` y `visualizar_shocks_comparativo.py` consumen resultados SHAP legacy para figuras/dashboard; no constituyen implementacion SHAP v2.

## Comparacion De Explainers

- GC3/GE: `DeepExplainer` y `GradientExplainer` permanecen descartados por `StagingError` observado en el pre-flight con TF 2.21/Keras 3.14/capa custom. No se repitieron ni se modificaron modelos.
- Benchmark final: Kernel, Sampling y Permutation fueron compatibles en GC3/GE seed 0 de ambos cultivares, sobre exactamente enero y julio VAL 2024. Seed 0 es centinela tecnico, no seed seleccionada para interpretacion. Se completaron 156 corridas neuronales, sin warnings del explainer ni excepciones.
- Kernel se ensayo con `nsamples=2048/4096`, `l1_reg=0.0` para conservar todas las dimensiones. Ya no hubo singularidad, pero presento menor estabilidad del ranking elemental entre RNG que Permutation al presupuesto alto probado.
- Sampling se ensayo con `nsamples=16384/65536`, `min_samples_per_feature=100`. Fue compatible y tambien permitio background TRAIN completo de 84 ventanas, sin coste proporcional a evaluar todo el background por coalicion.
- D52 CERRADA: `PermutationExplainer` con wrapper flatten/unflatten, `Independent(background, max_samples=32)`, link identity, `seed=1729`, `batch_size=1024`, `max_evals=16*(2*P+1)`: 7120 para GC3 (P=222), 8272 para GE (P=258). P cuenta posiciones temporales x variables, no solo nombres de features.
- Fallback aprobado: `SamplingExplainer`, mismo wrapper/background, `nsamples=65536`, `min_samples_per_feature=100`, RNG 1729, link identity. Solo ante fallo tecnico real (excepcion, shapes/no-finitos o reconstruccion >1e-5 escalado): detener y documentar antes de sustituir el metodo oficial. No mezclar silenciosamente metodos entre seeds ni cambiar por resultados cientificos, rankings de variables o apariencia de figuras.
- XGBoost: D53 CERRADA, `TreeExplainer`. Conservar el baseline tecnico probado: `feature_perturbation="interventional"`, `model_output="raw"`, background D55; probado en las dos seeds centinela, 37 features. Las atribuciones de los tres modelos estan en unidades del target escalado.

Con N=32 y presupuesto alto, la correlacion Spearman elemental entre RNG 1729/1730 fue 0.9835..0.9920 para Permutation, 0.9592..0.9815 para Sampling y 0.8574..0.9566 para Kernel. Repetir el mismo RNG/config produjo arrays identicos. Estas son comprobaciones tecnicas de estabilidad, no pruebas de significancia ni precision certificada de cada Shapley value.

La seleccion de Permutation se hizo ANTES del SHAP oficial TEST, exclusivamente por compatibilidad, reproducibilidad, estabilidad numerica, fidelidad/reconstruccion, ausencia de warnings/excepciones de explainers y coste viable. Error maximo neuronal observado: 1.30385160446167e-7 (aproximadamente 1.30e-7) escalado. TEST no se uso para generar SHAP ni para elegir el metodo. Dos muestras VAL por centinela no certifican estabilidad para todos los meses o seeds futuros.

## Background

D55 CERRADA: **32 referencias exclusivamente TRAIN por cultivar**, indices cero-based `np.rint(np.linspace(0, n_train-1, 32)).astype(int)`, sin muestreo aleatorio. Para GC3/GE hay 84 ventanas TRAIN (objetivos 2017-01..2023-12), con inputs historicos desde 2016-07. Mismas ventanas para ambos modelos y todas las seeds; cada cultivar aporta sus propios valores congelados. La seleccion ofrece cobertura temporal mas representativa que primeros-N, es determinista, reproducible, de coste viable e independiente de resultados TEST. No VAL ni TEST para el background.

Indices neuronales: `[0, 3, 5, 8, 11, 13, 16, 19, 21, 24, 27, 29, 32, 35, 37, 40, 43, 46, 48, 51, 54, 56, 59, 62, 64, 67, 70, 72, 75, 78, 80, 83]`.

Para XGBoost aplicar la misma regla N=32 a sus 89 filas predictoras TRAIN oficiales (objetivos 2016-08..2023-12), sin transformarlas en ventanas ni forzar igualdad con la representacion neuronal. Fechas, indices exactos, arrays y hashes por cultivar/modelo estan en el JSON del benchmark. No se usan VAL/TEST ni se refittea scaler.

Se probaron N=8/16/32 y Sampling N=84. N=8 equiespaciado concentra meses objetivo en enero/diciembre; N=16/32 cubren los doce meses, aunque no quedan estacionalmente balanceados. Con Permutation al presupuesto base, la correlacion de ranking por feature entre N=16 y N=32 fue 0.9857..0.9894. N=32 amplia la cobertura de ventanas a coste viable; no se afirma que sea un optimo ni equivalente a TRAIN completo. El fallback mantendra las mismas 32 referencias para preservar el baseline.

## Agregacion Temporal

Conservar SHAP firmado `sample x timestep x feature`: GC3 `(n,6,37)`, GE `(n,6,43)`. El wrapper concatena rama A aplanada y luego rama B aplanada; la reconstruccion elemental reordena ambas a su eje feature original.

Operacion documentada para los agregados: `I_feature[j] = mean_sample(sum_timestep(abs(phi)))`; `I_timestep[l] = mean_sample(sum_feature(abs(phi)))`; `I_group[g] = sum_feature_in_group(I_feature)`. La importancia elemental global es `mean_sample(abs(phi[l,j]))`. No sustituir suma de absolutos por absoluto de la suma; los valores firmados se conservan para analisis local y reconstruccion.

Posiciones: contexto de seis meses, del mas antiguo al origen t (t-5..t), prediccion t+1. Una feature llamada lag3 en la posicion t-5 referencia una observacion mas antigua: no confundir el lag nominal de la feature con el timestep. Los grupos se derivan de las constantes oficiales, sin alterar features.

D54 CERRADA: explicar separadamente las 10 seeds oficiales por cultivar/modelo y resumir atribuciones entre seeds. No explicar solo una seed ni reemplazarlo por una trayectoria ensemble. SD entre seeds refleja sensibilidad a inicializacion/estocasticidad, no incertidumbre temporal ni intervalo de confianza.

## Shocks

D58 CERRADA: mean absolute SHAP shock/nonshock, diferencia descriptiva, ranking por grupo y explicaciones locales. Mascaras congeladas: Sutil Jan/Jul/Nov 2025; Dulce Jan/Feb/Mar 2025; n_shock=3 y n_nonshock=9. Con n_shock=3 no usar pruebas fuertes de significancia, causalidad ni lenguaje de variable responsable. No se ejecutaron estos analisis en el benchmark.

D59 CERRADA: GE, importancia absoluta de las seis NLP lagged, participacion en la suma total de |SHAP|, ranking dentro del bloque NLP y comparacion shock/nonshock. GC3 carece de ese bloque; diferencias GC3/GE no son efectos causales de incorporar NLP. Ante denominador cero, participacion no definida, no division silenciosa.

## Attention

Los pesos de atencion pueden guardarse como diagnostico interno separado **solo si posteriormente se decide implementarlo**. Su implementacion sigue pendiente; D60 cierra su interpretacion, no obliga a generarlos. No sustituyen SHAP y no deben interpretarse como explicacion causal.

## Reproducibilidad y limites

D61 CERRADA: conservar versiones, hashes de modelos/datasets/scalers/codigo, git HEAD y estado de trabajo, arrays firmados, bases, nombres/orden, posiciones/fechas, indices y valores del background, parametros de explainer, seeds de modelo y de estimador diferenciadas, shapes, tiempos, warnings/excepciones y errores de reconstruccion. El benchmark incluye NPZ tecnicos y verificacion posterior independiente de sus hashes/shapes/reconstruccion.

La futura salida propuesta sigue en `resultados_v2_final/shap/{cultivar}/{model}/seed_XX/`, con resumenes de importancia feature/grupo/timestep, NLP y shocks. Esta carpeta no fue creada para resultados oficiales. Las pruebas actuales se guardaron exclusivamente bajo auditorias.

Correccion de alcance del antecedente: el lector neuronal del pre-flight anterior cargaba el CSV completo antes de filtrar. Su `test_loaded=False` no acreditaba ausencia de carga. El nuevo benchmark usa lectura acotada a 102 filas con `islice`, valida calendario 2016-07..2024-12 y no parsea registros TEST. El hashing binario completo se usa solo para integridad.

La incidencia no produjo SHAP TEST oficial, no modifico modelos ni predicciones y no intervino en seleccion mediante resultados TEST. Mantener la lectura acotada en futuros preflights: no pedir el registro 103 ni cargar todo el CSV para filtrarlo despues. No volver a ejecutar generadores historicos que sobrescriben esta auditoria con decisiones antiguas.

Los tres explainers comparados usan perturbaciones marginales que pueden romper dependencias entre lags/ventanas. SHAP explica ese juego de enmascaramiento y la funcion congelada, no causalidad ni una distribucion condicional temporal. La aditividad pequena puede resultar de restricciones/correcciones del estimador, no demuestra convergencia. Dos muestras VAL/seed centinela por modelo no certifican todos los meses ni las diez seeds.

Coste extrapolado para 480 explicaciones neuronales con la propuesta: 602.84 s (10.05 min) con carga/calentamiento medidos, mas imports/IO/informes; reservar aproximadamente 10-15 minutos en este equipo. Sampling fallback: 14.93 min medidos por extrapolacion. Tree: 240 explicaciones, ~0.09 s de computo SHAP + ~0.29 s de setup, sin incluir carga de todos los modelos/imports/IO. No se ejecutaron explicaciones TEST para estimar estos tiempos.

## Tabla De Decisiones

| ID | Decision | Alternativas | Recomendacion | Justificacion | Estado |
|---|---|---|---|---|---|
| D51 | modelos incluidos en SHAP | GC3/GE/XGBoost vs solo GC3/GE | Incluir GC3, GE y XGBoost; excluir Naive/SARIMA | Foco arquitectonico y complemento TreeSHAP | CERRADA |
| D52 | explainer GC3/GE | Kernel, Sampling, Permutation | Permutation, max_evals=16*(2*P+1), RNG 1729; fallback Sampling 65536 solo por fallo tecnico | Compatibilidad, estabilidad elemental y coste medidos antes de SHAP TEST | CERRADA |
| D53 | explainer XGBoost | TreeExplainer, PermutationExplainer | TreeExplainer | Explica el modelo de arboles congelado | CERRADA |
| D54 | estrategia multi-seed | 10 seeds, seed mediana, ensemble | Todas las 10 seeds oficiales | Sin best seed ni seed representativa | CERRADA |
| D55 | background | 8,16,32 TRAIN; Sampling 84 | 32 referencias TRAIN equiespaciadas por cultivar; indices y hashes guardados | Cobertura temporal, reproducibilidad y coste; independencia de TEST | CERRADA |
| D56 | agregacion temporal | elemental, feature, timestep, grupo | Conservar sample x timestep x feature y derivar agregados | Preserva atribucion elemental y operaciones explicitas | CERRADA |
| D57 | agrupacion de variables | manual vs constantes oficiales | Produccion historica, temporalidad, NASA/clima, INDECI, NLP solo GE | Nombres y orden de builders oficiales | CERRADA |
| D58 | analisis shock | shock, nonshock, locales | Descriptivo; n_shock=3 | Sin causalidad ni significancia fuerte | CERRADA |
| D59 | tratamiento NLP | absoluto, relativo, ranking, shocks | Seis variables NLP lagged de GE | Sin NLP contemporaneo ni interpretacion causal | CERRADA |
| D60 | attention complementaria | omitir, diagnostico, sustituto | Diagnostico separado | Attention no equivale a explicacion | CERRADA |
| D61 | reproducibilidad/artefactos | arrays, JSON, tablas | Versiones/hashes/background/parametros/shapes/tiempos/seeds/outputs/reconstruccion | Auditoria completa sin reentrenar | CERRADA |

## Protocolo Cerrado y Siguiente Paso

Protocolo CERRADO: GC3/GE con **PermutationExplainer**, GC3 max_evals=7120, GE max_evals=8272, RNG=1729, background **32 referencias TRAIN equiespaciadas por cultivar**; Sampling nsamples=65536 solo como fallback ante fallo tecnico real. XGBoost con TreeExplainer. Todas las diez seeds, SHAP elemental conservado y agregaciones explicitas, shocks/NLP descriptivos y attention separada si se decide implementarla. **D51-D61 CERRADAS.**

Proxima sesion: **AUDITORIA/PREFLIGHT DEL SCRIPT DE EJECUCION SHAP OFICIAL**; solo tras verificarlo, ejecutar SHAP post-hoc TEST 2025 congelado (240 explicaciones GC3 + 240 GE + 240 XGBoost). Esta secuencia fue indicada expresamente por el usuario; no reabrir la seleccion de D51-D61. Hoy no se implementa ni ejecuta el runner oficial. SHAP no habilita modificar arquitectura, features, hiperparametros, seeds, modelos ni predicciones. Continuar desde `../HANDOFF_SESION.md`.
