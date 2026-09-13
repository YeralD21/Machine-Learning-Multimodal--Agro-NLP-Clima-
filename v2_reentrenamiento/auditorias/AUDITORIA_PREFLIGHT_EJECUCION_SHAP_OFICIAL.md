# AUDITORIA PREFLIGHT EJECUCION SHAP OFICIAL

Estado: **LISTO PARA EJECUCIÓN SHAP TEST OFICIAL**.

**SHAP oficial TEST no ejecutado. Esta tarea termina en preflight.**

## Evidencia y alcance

Se leyeron el HANDOFF_SESION, las tres auditorias SHAP requeridas, el JSON completo del benchmark y las decisiones vigentes. D51-D61 cerradas prevalecen sobre las etiquetas historicas pendientes. No se modificaron esas fuentes. La aprobacion futura de ejecucion es distinta de esta aprobacion tecnica.

60 modelos cargados: 40 GC3/GE y 20 XGBoost; 720 predicciones comparadas solo por integridad. 60 explicaciones tecnicas sobre enero VAL 2024, una por modelo/seed; cero explicaciones TEST. 893 archivos congelados con SHA256 antes/despues y nueva comprobacion independiente.

No entrenamiento, HPO, seleccion de seeds, cambios de features/scalers/shocks, metricas predictivas nuevas, figuras ni interpretacion cientifica. No commit ni push. Naive y SARIMA se verifican desde sus artefactos congelados sin ejecutarlos.

## Alineacion temporal y lecturas TEST

La busqueda de NPZ/NPY en los directorios oficiales no encontro tensores TEST serializados reutilizables. GC3/GE reutiliza exactamente load_feature_dataframe + build_sequences de la evaluacion original. Se verifica cada una de las 12 ventanas contra las filas del mismo CSV escalado, sin recalcular features. El wrapper convierte a float32 igual que la entrada Keras y conserva flatten(A) seguido de flatten(B). Se cotejan tanto model.predict sobre los tensores originales float64 como el wrapper contra el CSV congelado. XGBoost reutiliza la funcion pura _build_test_arrays del evaluador original mediante AST; no ejecuta el evaluador.

Objetivos Jan-Dec 2025; origen Dec2024-Nov2025; contexto neuronal enero Jul-Dec2024, diciembre Jun-Nov2025. Informacion hasta t -> y_(t+1). Las variables lagged no se confunden con la posicion del timestep.

Tolerancia fijada antes de inferencia: atol=1e-6 escalado, rtol=0; maximo observado wrapper=2.88948059035e-07, evaluador original=2.88948059035e-07. Toneladas: atol=scale_train*1e-6+1e-8; scaler joblib cotejado con CSV. Reconstruccion explainer: atol=1e-5 escalado; maximo VAL=6.15068262544e-07. Ninguna tolerancia se ajusto a resultados.

El lector TRAIN/VAL sigue usando DictReader + islice(...,102), sin solicitar el registro 103. La excepcion autorizada de integridad se registra separadamente:

- `C:\Machine-learming\Machine-Learning-Multimodal--Agro-NLP-Clima-\v2_reentrenamiento\resultados_v2_final\evaluacion_test_gc3_ge\predicciones_test_gc3_ge.csv`: Read frozen predictions, dates, masks and model hashes; 480 filas.
- `C:\Machine-learming\Machine-Learning-Multimodal--Agro-NLP-Clima-\v2_reentrenamiento\resultados_v2_final\evaluacion_test_gc2_xgboost\predicciones_test_gc2_xgboost.csv`: Read frozen predictions, dates and masks; 240 filas.
- `C:\Machine-learming\Machine-Learning-Multimodal--Agro-NLP-Clima-\v2_reentrenamiento\data\processed\master_dataset_sutil_v2_escalado.csv`: Integrity only: original TEST builder, dates/shapes/predictions; 114 filas.
- `C:\Machine-learming\Machine-Learning-Multimodal--Agro-NLP-Clima-\v2_reentrenamiento\data\processed\master_dataset_dulce_v2_escalado.csv`: Integrity only: original TEST builder, dates/shapes/predictions; 114 filas.

El hashing binario completo no constituye analisis TEST. Se conservaron las mascaras del CSV y se cotejaron contra shocks_d35; no se recalcularon P75.

## Background y protocolo

32 referencias TRAIN por cultivar; los seis NPZ/hashes aprobados se reutilizan y contrastan contra TRAIN. GC3/GE: 84 ventanas TRAIN; XGBoost: 89 pares. Indices rint(linspace(0,n-1,32)), compartidos entre seeds. Indices, fechas y seis hashes completos estan en backgrounds del JSON adjunto.

Permutation: Independent(max_samples=32), identity, RNG=1729, batch_size=1024; GC3 max_evals=7120, GE=8272. Tree: background explicito, interventional, raw; entradas/target escalados. Sampling nsamples=65536, min_samples_per_feature=100, RNG=1729, mismo background: exclusivamente tras fallo tecnico real registrado. La corrida se detiene; reanudar exige el registro del fallo y su SHA256. No existe seleccion libre de explainer; timeout/lentitud, discrepancias de integridad o preferencias interpretativas no habilitan fallback. No se activo fallback en este preflight.

## Almacenamiento y agregacion

Runner: `../scripts/ejecutar_shap_v2_oficial.py`. Probes y manifest sellado bajo `../resultados_v2_final/shap/_preflight/`; temporales bajo `shap/_runtime_tmp/`. La futura corrida usa `shap/official_test_2025/{cultivar}/{GC3,GE,XGBoost}/seed_XX/`, con creacion exclusiva.

`raw.npz` conserva flat_shap firmado, elemental_shap GC3=(12,6,37), GE=(12,6,43), XGBoost=(12,37), bases, reconstruccion, residuos, prediccion del modelo, predicciones congeladas escaladas/toneladas, entradas escaladas, fechas target/contexto, nombres/indices de features, rama/timestep, background/indices y mascara shock. `metadata.json` conserva versiones, seeds, RNG, budgets, tiempos, warnings, hashes, unidades y mapeo completo. El NPZ se relee sin pickle y se compara exactamente antes de generar agregados.

A: mean_sample(sum_timestep(abs(phi))); B: mean_sample(sum_feature(abs(phi))); C: suma de A por grupo; XGBoost usa mean_sample(abs(phi)). D: seis NLP GE y participacion en el total absoluto (denominador cero -> null, sin imputacion). E: mismas operaciones sobre shock/nonshock, diferencias descriptivas. F: media, mediana, SD muestral, minimo y maximo de cada agregado A-E calculado previamente por seed. Nunca se promedian SHAP firmados entre seeds antes del absoluto. Valores firmados locales quedan intactos. Las pruebas con signos opuestos y datos sinteticos ejercitan todos estos caminos sin rankings TEST.

## Guardas PASS/FAIL

| Guarda | Estado | Evidencia |
|---|---|---|
| Lectura integral y evidencia historica del benchmark | PASS | JSON completo: 56329 hojas revisadas; 156 corridas neuronales y 12 Tree; todos los NPZ historicos coinciden con sus hashes |
| Preflight completo | PASS | C:\Machine-learming\Machine-Learning-Multimodal--Agro-NLP-Clima-\v2_reentrenamiento\resultados_v2_final\shap\_preflight\20260913T190939018411Z\preflight.json |
| Script definitivo sellado | PASS | 4a8a78c19b8c254c740179efe3b17e969479b7f026c3252fbc937a123b2af8ac |
| Modelos/datos/scalers/predicciones/shocks/features/codigo inmutables | PASS | 893 SHA256 verificados; cambios: [] |
| Naive congelado | PASS | Manifest de comparacion aprobado y hashes historicos verificados |
| SARIMA_rolling congelado | PASS | Manifest de comparacion aprobado y hashes historicos verificados |
| XGBoost congelado | PASS | Manifest de comparacion aprobado y hashes historicos verificados |
| GC3 congelado | PASS | Manifest de comparacion aprobado y hashes historicos verificados |
| GE congelado | PASS | Manifest de comparacion aprobado y hashes historicos verificados |
| 40 redes + 20 XGBoost; seeds completas 0..9 | PASS | Inventario, config y carga de cada modelo |
| D51 alcance GC3/GE/XGBoost | PASS | Sin SHAP Naive/SARIMA |
| D52 Permutation/RNG/presupuesto | PASS | {'GC3': 7120, 'GE': 8272} |
| D53 Tree interventional/raw | PASS | Pipeline escalado; no transform/fit del scaler |
| Sello utilizable por el arranque oficial futuro | PASS | Igualdad del inventario completo, incluyendo el script; informes nuevos de esta tarea excluidos explicitamente |
| D54 no seleccion de seed ni presupuesto configurable | PASS | CLI sin selectores; prueba negativa de seeds incompletas |
| D55 background sutil/GC3 | PASS | array SHA256 4d95df53dfeaa745e0752465ead086efced9947d7af4d83ee64d6fcc14d8c5a3; indices/valores contra TRAIN y benchmark aprobado |
| D55 background sutil/GE | PASS | array SHA256 a8a25c6720e2e3d1a8c9dd5ca111cade33260403d4ea92637d3072a8ade36818; indices/valores contra TRAIN y benchmark aprobado |
| D55 background sutil/XGBoost | PASS | array SHA256 71d3dbb62dc1fa60e8362b73296b6be423f13fa03f78e57af898c0114ead9980; indices/valores contra TRAIN y benchmark aprobado |
| D55 background dulce/GC3 | PASS | array SHA256 d3c84a671da84253c3e5fb0a8edb7af3d7ac3f8c8dc4bd96c3fcd216d20bd32c; indices/valores contra TRAIN y benchmark aprobado |
| D55 background dulce/GE | PASS | array SHA256 01895908a544ead3ab1dcb359b3de99cb0e057d64e09d52c87800248846de69e; indices/valores contra TRAIN y benchmark aprobado |
| D55 background dulce/XGBoost | PASS | array SHA256 41021ad468e00e8b23dcfc710e6e2922c2539349b84ab616bdbec3546b774be2; indices/valores contra TRAIN y benchmark aprobado |
| D56 shape y mapeo sutil/GC3 | PASS | input SHA256 f6fae2a61ffe09684a12b4c4a3a900c5b1ba3f14e1faac429de088c86ec9e5a1; flat->rama/timestep/feature reversible |
| Calendario y horizonte sutil/GC3 | PASS | Contexto enero=['2024-07-01', '2024-08-01', '2024-09-01', '2024-10-01', '2024-11-01', '2024-12-01']; contexto diciembre=['2025-06-01', '2025-07-01', '2025-08-01', '2025-09-01', '2025-10-01', '2025-11-01'] |
| D57 grupos sutil/GC3 | PASS | Constantes de features oficiales |
| D56 shape y mapeo sutil/GE | PASS | input SHA256 ff594e8dbe3970b10869bf6150be2602cff66fcff42416ab457920ca8aef0730; flat->rama/timestep/feature reversible |
| Calendario y horizonte sutil/GE | PASS | Contexto enero=['2024-07-01', '2024-08-01', '2024-09-01', '2024-10-01', '2024-11-01', '2024-12-01']; contexto diciembre=['2025-06-01', '2025-07-01', '2025-08-01', '2025-09-01', '2025-10-01', '2025-11-01'] |
| D57 grupos sutil/GE | PASS | Constantes de features oficiales |
| D56 shape y mapeo sutil/XGBoost | PASS | input SHA256 35191e1d9759180dab8d38375a0f9bad1bf81209b39cb03f130e80c0449ab5ac; flat->rama/timestep/feature reversible |
| Calendario y horizonte sutil/XGBoost | PASS | Contexto enero=['2024-12-01']; contexto diciembre=['2025-11-01'] |
| D57 grupos sutil/XGBoost | PASS | Constantes de features oficiales |
| D56 shape y mapeo dulce/GC3 | PASS | input SHA256 f79fdd8819112dce0a7c0a6af9191077417b138f0061b89f6bf38c65fa5e806f; flat->rama/timestep/feature reversible |
| Calendario y horizonte dulce/GC3 | PASS | Contexto enero=['2024-07-01', '2024-08-01', '2024-09-01', '2024-10-01', '2024-11-01', '2024-12-01']; contexto diciembre=['2025-06-01', '2025-07-01', '2025-08-01', '2025-09-01', '2025-10-01', '2025-11-01'] |
| D57 grupos dulce/GC3 | PASS | Constantes de features oficiales |
| D56 shape y mapeo dulce/GE | PASS | input SHA256 102facde491a21ec2c5bd261611667dc74df9ac2b5da84506ecde016aec61d3f; flat->rama/timestep/feature reversible |
| Calendario y horizonte dulce/GE | PASS | Contexto enero=['2024-07-01', '2024-08-01', '2024-09-01', '2024-10-01', '2024-11-01', '2024-12-01']; contexto diciembre=['2025-06-01', '2025-07-01', '2025-08-01', '2025-09-01', '2025-10-01', '2025-11-01'] |
| D57 grupos dulce/GE | PASS | Constantes de features oficiales |
| D56 shape y mapeo dulce/XGBoost | PASS | input SHA256 1e86e3806eb4b2b3facc584d89f738636721cffc24cda672bdd8485ec7457d9c; flat->rama/timestep/feature reversible |
| Calendario y horizonte dulce/XGBoost | PASS | Contexto enero=['2024-12-01']; contexto diciembre=['2025-11-01'] |
| D57 grupos dulce/XGBoost | PASS | Constantes de features oficiales |
| 720 predicciones congeladas reproducidas | PASS | rtol=0, atol=1e-6 escalado; max wrapper=2.88948059035e-07; max evaluador original=2.88948059035e-07 |
| Scalers e inversion a toneladas | PASS | StandardScaler TRAIN=90; joblib/CSV consistentes; tolerancia toneladas=scale_train*1e-6+1e-8 |
| D58 mascaras fijas | PASS | CSV congelados: Sutil Jan/Jul/Nov; Dulce Jan/Feb/Mar; 3/9, sin recalcular umbrales |
| Pruebas VAL de los 60 explainers/NPZ/aditividad | PASS | Verificacion independiente de 60/60 NPZ; max residuo=6.15068262544e-07 escalado |
| Pesos en memoria sin cambios | PASS | Hash pesos/booster antes y despues de probes |
| Cero SHAP TEST | PASS | Solo enero VAL 2024 para cada una de las 60 corridas; directorio oficial ausente |
| Lecturas TEST explicitadas | PASS | Dos CSV de predicciones (480/240 filas), dos datasets (114 filas cada uno, 12 de 2025); solo integridad |
| D56-D59 agregaciones A-E y serializacion completa | PASS | Arrays SINTETICOS: roundtrip NPZ/JSON, conservacion absoluta, NLP denominador cero, shock/nonshock; no rankings TEST |
| Variabilidad entre 10 seeds sin cancelacion firmada | PASS | Seeds de signos opuestos conservan importancia; media/mediana/SD(ddof=1)/min/max de agregados por seed |
| D60 attention separado | PASS | No se genera ni se necesita attention |
| Sin entrenar/reentrenar/modificar modelos | PASS | AST sin llamadas ['fit', 'fit_transform', 'partial_fit', 'save_model', 'set_weights', 'train_on_batch']; hashes inmutables |
| Fallback tecnico documentado con parada | PASS | No automatico: error Permutation -> registro/STOP; reanudacion exige hash del fallo y mismo preflight; no por lentitud ni integridad |
| Guardas de autorizacion/hash/calendario/escritura | PASS | Pruebas adversariales rechazan entradas invalidas antes del explainer |
| Sin sobrescritura silenciosa | PASS | Creacion exclusiva x/xb; directorio oficial unico; temporales tambien dentro de SHAP |
| D61 entorno y RNG reproducibles | PASS | {"python": "3.11.9", "numpy": "2.4.4", "shap": "0.51.0", "tensorflow": "2.21.0", "keras": "3.14.0", "xgboost": "3.2.0", "joblib": "1.5.3", "pandas": "3.0.2", "os": "Windows-10-10.0.26200-SP0", "cpu": "AMD64 Family 25 Model 33 Stepping 2, AuthenticAMD", "devices": ["PhysicalDevice(name='/physical_device:CPU:0', device_type='CPU')"], "TF_ENABLE_ONEDNN_OPTS": "0", "TF_DETERMINISTIC_OPS": "1", "intra_threads": 1, "inter_threads": 1, "pythonhashseed": "1729"} |
| Temporales sin violar confinamiento | PASS | AutoGraph y cache en shap/_runtime_tmp; stdout/stderr y warnings conservados |

## Entorno, incidencias y limites

Se uso venv Python 3.11.9 fuera del sandbox porque este no podia acceder al interprete base. Las primeras pruebas detectaron restricciones del dispositivo NUL de Windows y temporales AutoGraph. Se corrigio el runner para reconocer NUL y confinar los temporales a SHAP. Las corridas previas se conservan como evidencia de desarrollo; solo el manifest/hash indicado abajo corresponde al script final. Las lecturas de integridad y probes VAL se repitieron al verificar la version corregida.

Warnings conocidos de BahdanauAttention/Keras, GPU nativa y trazado TensorFlow se registran; no se modificaron capas. El audit hook protege escrituras Python, rutas resueltas y sobrescrituras; el control SHA256 antes/despues cubre artefactos congelados. No se afirma que un hook Python sea un sandbox de sistema para bibliotecas nativas. Una muestra VAL por cada seed prueba operacion/serializacion; no certifica convergencia de atribuciones TEST. El benchmark previo aporta la evidencia tecnica adicional. NLP y shocks permanecen descriptivos, sin causalidad; attention no implementada.

## Sello y ejecucion futura

- Script SHA256: `4a8a78c19b8c254c740179efe3b17e969479b7f026c3252fbc937a123b2af8ac`.
- Preflight aprobado: `C:\Machine-learming\Machine-Learning-Multimodal--Agro-NLP-Clima-\v2_reentrenamiento\resultados_v2_final\shap\_preflight\20260913T190939018411Z\preflight.json`.
- Preflight SHA256: `1cbf7a91a5c00bc61b8b1d8b9dedabb3aee93028a483a3a1bee3d45d69beeccc`.
- Auditoria JSON: `shap_v2_preflight_ejecucion_oficial.json`.

Ejecutar sin argumentos solo corre preflight. La corrida oficial requiere NUEVA autorizacion del usuario y `--execute-test --approved-preflight <ruta anterior> --preflight-sha256 <hash anterior>`. Esos argumentos no se usaron en esta tarea. Antes del primer SHAP TEST, el runner vuelve a verificar hashes, versiones, inventario y las 720 predicciones; falla ante cambios. No ejecutar los generadores historicos.

Estado de salida: **LISTO PARA EJECUCIÓN SHAP TEST OFICIAL**.
