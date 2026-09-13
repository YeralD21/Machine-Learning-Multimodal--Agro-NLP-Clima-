# AUDITORIA PREFLIGHT GC2/XGBOOST

Dry-run/pre-flight del runner oficial GC2/XGBoost v2. No se entreno XGBoost, no se ejecuto `model.fit()`, no se hizo HPO, no se ejecuto TimeSeriesSplit para seleccion y no se cargo TEST 2025 para entrenamiento ni validacion.

## Archivos creados/modificados

- `v2_reentrenamiento/DECISIONES_METODOLOGICAS.md`
- `v2_reentrenamiento/src/models/features_gc2_xgboost.py`
- `v2_reentrenamiento/src/training/config_gc2_xgboost.py`
- `v2_reentrenamiento/src/training/tabular_gc2_xgboost.py`
- `v2_reentrenamiento/src/training/official_runner_gc2_xgboost.py`
- `v2_reentrenamiento/auditorias/AUDITORIA_PREFLIGHT_GC2_XGBOOST.md`
- `v2_reentrenamiento/auditorias/gc2_xgboost_preflight_dry_run.json`

## Componentes reutilizados

- `v2_reentrenamiento.src.models.features_gc3_ge constants`
- `v2_reentrenamiento.src.training.metadata_gc3_ge hash/git helpers`

## Config oficial

```json
{
  "best_seed_selection": false,
  "booster": "gbtree",
  "colsample_bytree": 0.8,
  "cv_performed": false,
  "early_stopping": false,
  "eval_metric": "mae",
  "gamma": 0.0,
  "hpo_performed": false,
  "learning_rate": 0.05,
  "max_depth": 2,
  "min_child_weight": 1,
  "n_estimators": 200,
  "n_jobs": 1,
  "objective": "reg:squarederror",
  "reg_alpha": 0.0,
  "reg_lambda": 1.0,
  "subsample": 0.8,
  "tree_method": "hist"
}
```

## Shapes y fechas

### sutil

- Features: 37
- TRAIN X: 2016-07..2023-11
- TRAIN y: 2016-08..2023-12
- TRAIN shape: X=[89, 37], y=[89]
- VAL X: 2023-12..2024-11
- VAL y: 2024-01..2024-12
- VAL shape: X=[12, 37], y=[12]
- TEST loaded: false

### dulce

- Features: 37
- TRAIN X: 2016-07..2023-11
- TRAIN y: 2016-08..2023-12
- TRAIN shape: X=[89, 37], y=[89]
- VAL X: 2023-12..2024-11
- VAL y: 2024-01..2024-12
- VAL shape: X=[12, 37], y=[12]
- TEST loaded: false

## Features GC2

GC2 usa exactamente las 37 features NO-NLP de GC3. Ausentes: NLP, precio_chacra_kg, n_provincias, total_afectados, contemporaneas exogenas, lag2 y rolling mean/std.

```text
produccion_t_sutil
produccion_t_sutil_lag1
produccion_t_sutil_lag3
produccion_t_sutil_lag6
mes_sin
mes_cos
t_index
T2M_lag1
T2M_lag3
T2M_lag6
T2M_MAX_lag1
T2M_MAX_lag3
T2M_MAX_lag6
WS2M_lag1
WS2M_lag3
WS2M_lag6
PRECTOTCORR_lag1
PRECTOTCORR_lag3
PRECTOTCORR_lag6
RH2M_lag1
RH2M_lag3
RH2M_lag6
num_emergencias_lag1
num_emergencias_lag3
num_emergencias_lag6
personas_afectadas_lag1
personas_afectadas_lag3
personas_afectadas_lag6
personas_damnificadas_lag1
personas_damnificadas_lag3
personas_damnificadas_lag6
hectareas_cultivo_perdidas_lag1
hectareas_cultivo_perdidas_lag3
hectareas_cultivo_perdidas_lag6
hectareas_cultivo_afectadas_lag1
hectareas_cultivo_afectadas_lag3
hectareas_cultivo_afectadas_lag6
```

## Controles pre-flight

| ID | Control | Estado | Detalle |
|---:|---|---|---|
| 1 | cultivares = sutil/dulce | OK | ('sutil', 'dulce') |
| 2 | seeds = 0..9 | OK | (0, 1, 2, 3, 4, 5, 6, 7, 8, 9) |
| 3 | total futuro = 20 corridas | OK | 2 x 10 |
| 4 | features = 37 | OK | {'sutil': 37, 'dulce': 37} |
| 5 | NLP ausente | OK | sin avg_sentiment/n_noticias |
| 6 | precio ausente | OK | precio_chacra_kg no presente |
| 7 | n_provincias ausente | OK | n_provincias no presente |
| 8 | total_afectados ausente | OK | total_afectados no presente |
| 9 | contemporaneas exogenas ausentes | OK | solo exogenas lagged |
| 10 | lag2 ausente | OK | lag2 no presente |
| 11 | rolling features ausentes | OK | rolling/mean/std no presentes |
| 12 | TRAIN n=89 | OK | {'sutil': 89, 'dulce': 89} |
| 13 | TRAIN X 2016-07..2023-11 | OK | origenes TRAIN |
| 14 | TRAIN y 2016-08..2023-12 | OK | targets TRAIN |
| 15 | VAL n=12 | OK | {'sutil': 12, 'dulce': 12} |
| 16 | VAL X 2023-12..2024-11 | OK | origenes VAL |
| 17 | VAL y 2024-01..2024-12 | OK | targets VAL |
| 18 | no TEST load | OK | solo filas <=2024 cargadas |
| 19 | no HPO | OK | hpo_performed=false |
| 20 | no CV | OK | cv_performed=false |
| 21 | no early stopping | OK | early_stopping=false |
| 22 | n_estimators=200 | OK | 200 |
| 23 | max_depth=2 | OK | 2 |
| 24 | min_child_weight=1 | OK | 1 |
| 25 | learning_rate=.05 | OK | 0.05 |
| 26 | subsample=.8 | OK | 0.8 |
| 27 | colsample_bytree=.8 | OK | 0.8 |
| 28 | reg_alpha=0 | OK | 0.0 |
| 29 | reg_lambda=1 | OK | 1.0 |
| 30 | gamma=0 | OK | 0.0 |
| 31 | tree_method=hist | OK | hist |
| 32 | n_jobs=1 | OK | 1 |
| 33 | objective reg:squarederror | OK | reg:squarederror |
| 34 | eval_metric mae | OK | mae |
| 35 | booster gbtree | OK | gbtree |

## Confirmaciones

- `model.fit()` ejecutado: NO.
- TEST 2025 cargado: NO.
- HPO ejecutado: NO.
- TimeSeriesSplit/CV ejecutado: NO.
- Early stopping usado: NO.
- Best seed selection: NO.

## Git

- Git commit/hash: `44fe6eafca927ba22d97bf8e46d6ad530cc1a47a`

```text
## main...origin/main
 M v2_reentrenamiento/DECISIONES_METODOLOGICAS.md
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
?? v2_reentrenamiento/auditorias/AUDITORIA_EVALUACION_TEST_GC3_GE.md
?? v2_reentrenamiento/auditorias/AUDITORIA_METODOLOGICA_GC2_XGBOOST.md
?? v2_reentrenamiento/auditorias/AUDITORIA_POST_RUN_GC3_GE.md
?? v2_reentrenamiento/auditorias/AUDITORIA_PREFLIGHT_GC2_XGBOOST.md
?? v2_reentrenamiento/auditorias/AUDITORIA_RUNNER_OFICIAL_GC3_GE.md
?? v2_reentrenamiento/auditorias/auditar_entrenamiento_oficial_gc3_ge.py
?? v2_reentrenamiento/auditorias/auditar_post_run_final_gc3_ge.py
?? v2_reentrenamiento/auditorias/auditar_runner_oficial_gc3_ge.py
?? v2_reentrenamiento/auditorias/ejecutar_entrenamiento_oficial_gc3_ge.py
?? v2_reentrenamiento/auditorias/entrenamiento_oficial_gc3_ge_manifest.json
?? v2_reentrenamiento/auditorias/evaluar_test_gc3_ge.py
?? v2_reentrenamiento/auditorias/gc2_xgboost_preflight_dry_run.json
?? v2_reentrenamiento/auditorias/resumen_entrenamiento_oficial_gc3_ge.csv
?? v2_reentrenamiento/auditorias/resumen_entrenamiento_oficial_gc3_ge.json
?? v2_reentrenamiento/auditorias/resumen_post_run_gc3_ge.csv
?? v2_reentrenamiento/auditorias/resumen_post_run_gc3_ge.json
?? v2_reentrenamiento/auditorias/runner_oficial_gc3_ge_dry_run.json
?? v2_reentrenamiento/resultados_v2_final/evaluacion_test_gc3_ge/
?? v2_reentrenamiento/resultados_v2_final/official_gc3_ge/
?? v2_reentrenamiento/resultados_v2_final/technical_pilot/
?? v2_reentrenamiento/resultados_v2_final/technical_pilot_scaled/
?? v2_reentrenamiento/src/models/features_gc2_xgboost.py
?? v2_reentrenamiento/src/training/config_gc2_xgboost.py
?? v2_reentrenamiento/src/training/official_runner_gc2_xgboost.py
?? v2_reentrenamiento/src/training/official_runner_gc3_ge.py
?? v2_reentrenamiento/src/training/tabular_gc2_xgboost.py
```

- No commit.
- No push.

GC2/XGBOOST PRE-FLIGHT APROBADO Y LISTO PARA AUTORIZACIÓN DE ENTRENAMIENTO
