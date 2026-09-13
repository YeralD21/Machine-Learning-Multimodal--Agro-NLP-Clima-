# AUDITORIA RUNNER OFICIAL GC3/GE

## Alcance

Se audito y preparo el runner oficial GC3/GE para las 40 corridas
preespecificadas. En esta tarea se ejecuto solo dry-run: no se llamo
`model.fit()`, no se entreno ninguna red, no se generaron checkpoints
oficiales y no se hizo commit.

## Decision de implementacion

- Decision: B) reutilizar componentes existentes y crear un orquestador oficial.
- Motivo: ya existian componentes auditados para features, arquitectura,
  configuracion, callbacks, determinismo, history, metadata y artefactos;
  faltaba un orquestador oficial con pre-flight integral y guardas contra
  uso de TEST/sobrescritura.

## Archivos creados/modificados

- Creado: `v2_reentrenamiento/src/training/official_runner_gc3_ge.py`.
- Creado: `v2_reentrenamiento/auditorias/auditar_runner_oficial_gc3_ge.py`.
- Creado: `v2_reentrenamiento/auditorias/runner_oficial_gc3_ge_dry_run.json`.
- Creado: `v2_reentrenamiento/auditorias/AUDITORIA_RUNNER_OFICIAL_GC3_GE.md`.
- No se modificaron datasets, scalers, notebooks ni decisiones metodologicas.

## Diseno del runner

- Destino oficial previsto: `v2_reentrenamiento/resultados_v2_final/official_gc3_ge/`.
- Estructura por corrida: `cultivar/modelo/seed_XX`.
- Corridas esperadas: 2 cultivares x 2 modelos x 10 seeds = 40.
- Seeds oficiales: `0,1,2,3,4,5,6,7,8,9`.
- El dry-run no crea directorios oficiales de corrida.
- La funcion futura `run_official_training()` rechaza configuraciones no oficiales
  y directorios existentes con archivos.

## Pre-flight checks

- Dataset correcto `master_dataset_{cultivar}_v2_escalado.csv`.
- Scaler correcto `scaler_{cultivar}_v2c.joblib` como StandardScaler.
- Hash SHA256 de dataset, scaler y parametros del scaler registrado.
- Fechas 2016-07..2025-12, sin 2026.
- Split estructural: TRAIN efectivo 90 filas, VAL 12, TEST 12.
- Secuencias de fit construidas solo con filas <= 2024.
- TRAIN sequences = 84 y VAL targets = 12.
- TEST no se materializa como arrays de fit ni callbacks.
- Lookback = 6.
- Features exactas GC3/GE y ausencia de exogenas contemporaneas indebidas.
- Shapes exactos por cultivar/modelo.
- Parametros exactos GC3=6273 y GE=6657.
- Seeds exactas 0..9.
- `shuffle=False`.
- Loss/optimizer/callbacks oficiales.
- Directorio oficial diferenciado de `technical_pilot*`.
- Bloqueo de sobrescritura silenciosa de corridas oficiales existentes.
- Git hash y hashes de inputs congelados registrados.

## Resultado del dry-run

- Estado dry-run: READY.
- `model.fit()` ejecutado: False.
- TEST usado para entrenamiento: False.
- TEST cargado para fit: False.
- Corridas oficiales planificadas: 40.
- Seeds verificadas: 0, 1, 2, 3, 4, 5, 6, 7, 8, 9.
- Shuffle: False.
- Batch size: 8.
- Max epochs: 300.
- Git hash: `44fe6eafca927ba22d97bf8e46d6ad530cc1a47a`.

## Parametros del modelo

| Modelo | Params esperados | Params dry-run | Loss | Optimizer | LR |
|---|---:|---:|---|---|---:|
| GC3 | 6273 | 6273 | mse | Adam | 0.0010000000474974513 |
| GE | 6657 | 6657 | mse | Adam | 0.0010000000474974513 |

## Shapes verificados

| Cultivar | Modelo | Rama A | Rama B | Inputs | TRAIN seq | VAL targets | Xa_train | Xb_train | Xa_val | Xb_val |
|---|---|---:|---:|---:|---:|---:|---|---|---|---|
| sutil | GC3 | 4 | 33 | 37 | 84 | 12 | (84, 6, 4) | (84, 6, 33) | (12, 6, 4) | (12, 6, 33) |
| sutil | GE | 4 | 39 | 43 | 84 | 12 | (84, 6, 4) | (84, 6, 39) | (12, 6, 4) | (12, 6, 39) |
| dulce | GC3 | 4 | 33 | 37 | 84 | 12 | (84, 6, 4) | (84, 6, 33) | (12, 6, 4) | (12, 6, 33) |
| dulce | GE | 4 | 39 | 43 | 84 | 12 | (84, 6, 4) | (84, 6, 39) | (12, 6, 4) | (12, 6, 39) |

## Confirmaciones explicitas

- TEST 2025 no fue usado para entrenamiento.
- TEST 2025 no fue pasado a callbacks.
- TEST 2025 no fue materializado como arrays de fit en el runner oficial.
- No hubo llamada a `model.fit()` durante esta auditoria.
- GC3 confirma 6273 parametros.
- GE confirma 6657 parametros.
- TRAIN sequences confirma 84 por cultivar/modelo.
- VAL targets confirma 12 por cultivar/modelo.
- Seeds oficiales confirmadas: 0..9.

## Riesgos o bloqueos

- No se encontraron bloqueos metodologicos.
- Riesgo operativo pendiente: el entrenamiento oficial todavia no fue autorizado
  ni ejecutado; los directorios oficiales deben permanecer sin archivos previos
  para evitar sobrescritura.
- `git status` sigue mostrando archivos untracked preexistentes y los nuevos
  artefactos de auditoria; no se hizo commit.

## Git status final

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
?? v2_reentrenamiento/auditorias/AUDITORIA_RUNNER_OFICIAL_GC3_GE.md
?? v2_reentrenamiento/auditorias/auditar_runner_oficial_gc3_ge.py
?? v2_reentrenamiento/auditorias/runner_oficial_gc3_ge_dry_run.json
?? v2_reentrenamiento/resultados_v2_final/technical_pilot/
?? v2_reentrenamiento/resultados_v2_final/technical_pilot_scaled/
?? v2_reentrenamiento/src/training/official_runner_gc3_ge.py
```

No commit.

RUNNER OFICIAL GC3/GE LISTO PARA AUTORIZACIÓN DE ENTRENAMIENTO
