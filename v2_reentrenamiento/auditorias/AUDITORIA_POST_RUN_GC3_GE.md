# AUDITORIA POST-RUN GC3/GE

Auditoria post-run final de las 40 corridas oficiales GC3/GE v2 antes de
autorizar apertura unica de TEST 2025. No se entrenaron modelos, no se
cargo TEST 2025, no se generaron predicciones TEST, no se modifico
metodologia, y no se hizo commit ni push.

## A. Integridad experimental

- Corridas esperadas: 40.
- Corridas auditadas: 40.
- Matriz esperada: cultivares `sutil`, `dulce`; modelos `GC3`, `GE`; seeds `0..9`.
- Rutas independientes: OK.
- Seeds 0..9 por combinacion: OK.
- Inicio UTC batch: 2026-09-11T16:28:28.446811+00:00.
- Fin UTC batch: 2026-09-11T16:35:50.591390+00:00.
- Duracion batch segundos: 442.143851.
- Resumen CSV: `v2_reentrenamiento\auditorias\resumen_post_run_gc3_ge.csv`.
- Resumen JSON: `v2_reentrenamiento\auditorias\resumen_post_run_gc3_ge.json`.

## B. Consistencia metodologica

- Protocolo comun verificado: lookback=6, loss=MSE, metric=MAE, Adam lr=0.001, batch=8, max_epochs=300, shuffle=False.
- EarlyStopping verificado: monitor=val_loss, patience=15, restore_best_weights=True.
- ReduceLROnPlateau verificado: monitor=val_loss, factor=0.5, patience=8, min_lr=1e-6.
- ModelCheckpoint verificado: monitor=val_loss, save_best_only=True.
- GC3: Rama A=4, Rama B=33, total=37, params=6273.
- GE: Rama A=4, Rama B=39, total=43, params=6657.
- Diferencia predictiva GC3->GE: las 6 variables NLP lagged aprobadas.
- Predictores prohibidos/contemporaneos indebidos: no detectados.
- No HPO y no seleccion de mejor seed: verificado por configuracion fija y ausencia de artefactos de busqueda.

## C. Reproducibilidad

- Metadata D0 presente: seed, Python, NumPy, TensorFlow, Keras, OS, CPU/GPU, deterministic ops, PYTHONHASHSEED, TF_DETERMINISTIC_OPS, TF_ENABLE_ONEDNN_OPTS, git hash, timestamp, hashes dataset/scaler.
- Versiones registradas: Python=['3.11.9'], TensorFlow=['2.21.0'], Keras=['3.14.0'], NumPy=['2.4.4'].
- Git hash registrado: ['44fe6eafca927ba22d97bf8e46d6ad530cc1a47a'].
- No se exige reproducibilidad bit-a-bit fuera del entorno congelado.
- Histories identicos entre corridas distintas: NO.
- Checkpoints identicos entre corridas distintas: NO.

## D. Diagnostico de entrenamiento

Diagnostico agregado de optimizacion sobre VALIDATION 2024. Estos valores
no se usan para elegir seed, arquitectura, hiperparametros ni ganador.

| Cultivar | Modelo | n | epochs_ran | best_epoch | best_val_loss | best_val_mae | training_seconds |
|---|---|---:|---|---|---|---|---|
| dulce | GC3 | 10 | media=42.400000; mediana=41.500000; min=28.000000; max=65.000000 | media=27.400000; mediana=26.500000; min=13.000000; max=50.000000 | media=0.329916; mediana=0.283770; SD=0.112578; min=0.222888; max=0.536507 | media=0.482225; mediana=0.453791; SD=0.081900; min=0.400036; max=0.621091 | media=5.900789; mediana=5.857856; min=4.664994; max=7.975634 |
| dulce | GE | 10 | media=53.000000; mediana=50.000000; min=29.000000; max=93.000000 | media=38.000000; mediana=35.000000; min=14.000000; max=78.000000 | media=0.250152; mediana=0.240663; SD=0.073957; min=0.144479; max=0.386852 | media=0.421799; mediana=0.412998; SD=0.073821; min=0.302423; max=0.539460 | media=6.825406; mediana=6.530529; min=4.776952; max=10.569382 |
| sutil | GC3 | 10 | media=57.600000; mediana=58.500000; min=16.000000; max=116.000000 | media=42.600000; mediana=43.500000; min=1.000000; max=101.000000 | media=1.357384; mediana=1.147735; SD=0.668360; min=0.523427; max=2.461803 | media=0.875296; mediana=0.860489; SD=0.196890; min=0.582172; max=1.177642 | media=7.349364; mediana=7.411497; min=3.487688; max=12.329330 |
| sutil | GE | 10 | media=34.800000; mediana=25.500000; min=16.000000; max=82.000000 | media=19.800000; mediana=10.500000; min=1.000000; max=67.000000 | media=2.042818; mediana=2.149676; SD=0.473083; min=0.971999; max=2.667714 | media=1.128990; mediana=1.121367; SD=0.136735; min=0.842592; max=1.306978 | media=5.148610; mediana=4.430875; min=3.490226; max=9.012547 |

- Histories no vacios, epochs consecutivos, best_epoch<=stopped_epoch<=300 y learning rates validos.
- No se detectaron NaN/Inf en loss, val_loss, mae, val_mae ni learning_rate.
- best_epoch coincide con el minimo val_loss registrado usando indexacion 1-based.
- No se generaron figuras para evitar cherry-picking visual; el diagnostico agregado queda en CSV/JSON.

## E. Evidencia de uso/no uso de TEST

- Configs oficiales: `test_loaded_for_fit=false` y `test_used_for_training=false` en las 40 corridas.
- No existen archivos de predicciones ni metricas TEST en los directorios oficiales.
- Logs/configs/metadata/rutas no muestran evidencia de carga, evaluacion o prediccion sobre TEST 2025.
- Auditoria estatica del runner: la ruta oficial de entrenamiento usa TRAIN/VALIDATION y no contiene prediccion TEST.
- Conclusion: no existe evidencia de uso de TEST 2025.

## F. Incidencias

- No se detectaron incidencias bloqueantes.
- No se detectaron archivos extra inesperados ni tamanos cero.

## G. Veredicto

| Area | Estado |
|---|---|
| estructura_40_corridas | OK |
| seeds_0_9_por_combo | OK |
| rutas_independientes | OK |
| config_identica_salvo_identidad_features | OK |
| features_ok | OK |
| histories_ok | OK |
| checkpoints_ok | OK |
| params_ok | OK |
| d0_metadata_ok | OK |
| seed_independence_ok | OK |
| pilots_not_used | OK |
| no_test_evidence | OK |
| files_integrity_ok | OK |

## Git

```text
## main...origin/main
?? data/interim/indeci/indeci_temporal_2019_2025.csv
?? data/interim/nasa/clima_dataset_2019_2020.csv
?? sources/agraria-pe/sin-unificar/agro_news_2020.csv
?? sources/agraria-pe/sin-unificar/checkpoint_2019_2020.json
?? sources/noticias-ampliado/
?? src/scraping/agronoticias_scraper.py
?? src/scraping/andina_scraper.py
?? src/scraping/base_scraper.py
?? src/scraping/freshfruit_scraper.py
?? src/scraping/recalcular_sentimiento_v2.py
?? src/scraping/redagricola_scraper.py
?? src/scraping/run_expansion.py
?? src/scraping/unificar_corpus_v2.py
?? src/weather/nasa_power_downloader.py
?? v2_reentrenamiento/HANDOFF_CONTEXTO_ACTUAL.md
?? v2_reentrenamiento/auditorias/AUDITORIA_ENTRENAMIENTO_OFICIAL_GC3_GE.md
?? v2_reentrenamiento/auditorias/AUDITORIA_POST_RUN_GC3_GE.md
?? v2_reentrenamiento/auditorias/AUDITORIA_RUNNER_OFICIAL_GC3_GE.md
?? v2_reentrenamiento/auditorias/auditar_entrenamiento_oficial_gc3_ge.py
?? v2_reentrenamiento/auditorias/auditar_post_run_final_gc3_ge.py
?? v2_reentrenamiento/auditorias/auditar_runner_oficial_gc3_ge.py
?? v2_reentrenamiento/auditorias/ejecutar_entrenamiento_oficial_gc3_ge.py
?? v2_reentrenamiento/auditorias/entrenamiento_oficial_gc3_ge_manifest.json
?? v2_reentrenamiento/auditorias/resumen_entrenamiento_oficial_gc3_ge.csv
?? v2_reentrenamiento/auditorias/resumen_entrenamiento_oficial_gc3_ge.json
?? v2_reentrenamiento/auditorias/resumen_post_run_gc3_ge.csv
?? v2_reentrenamiento/auditorias/resumen_post_run_gc3_ge.json
?? v2_reentrenamiento/auditorias/runner_oficial_gc3_ge_dry_run.json
?? v2_reentrenamiento/resultados_v2_final/official_gc3_ge/
?? v2_reentrenamiento/resultados_v2_final/technical_pilot/
?? v2_reentrenamiento/resultados_v2_final/technical_pilot_scaled/
?? v2_reentrenamiento/src/training/official_runner_gc3_ge.py
```

NO commit.
NO push.

POST-RUN GC3/GE APROBADO PARA APERTURA ÚNICA DE TEST 2025
