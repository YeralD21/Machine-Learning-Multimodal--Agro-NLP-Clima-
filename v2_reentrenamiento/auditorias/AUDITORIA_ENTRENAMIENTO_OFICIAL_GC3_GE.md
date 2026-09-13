# AUDITORIA ENTRENAMIENTO OFICIAL GC3/GE

## Alcance

Auditoria post-run de las 40 corridas oficiales GC3/GE v2. No incluye
predicciones TEST, metricas TEST, inverse transform final, D35, Delta_s,
SHAP ni seleccion de modelo/seed.

## Tiempo

- Inicio UTC: 2026-09-11T16:28:28.446811+00:00
- Fin UTC: 2026-09-11T16:35:50.591390+00:00
- Duracion total segundos: 442.143851

## Pre-flight

- Pre-flight oficial ejecutado antes del entrenamiento: READY.
- Runner autorizado: `v2_reentrenamiento/src/training/official_runner_gc3_ge.py`.
- Cada corrida se ejecuto en un proceso Python separado para aplicar D0 antes de inicializar TensorFlow.

## Corridas

- Esperadas: 40
- Reales auditadas: 40
- Exitosas: 40
- Fallidas: 0
- Reintentadas: 0
- Resumen CSV: `v2_reentrenamiento\auditorias\resumen_entrenamiento_oficial_gc3_ge.csv`
- Resumen JSON: `v2_reentrenamiento\auditorias\resumen_entrenamiento_oficial_gc3_ge.json`

## Controles post-run

| # | Control | Estado |
|---:|---|---|
| 1 | exactamente 40 corridas oficiales completas | OK |
| 2 | exactamente 10 seeds por cultivar/modelo | OK |
| 3 | seeds 0..9 sin duplicados ni faltantes | OK |
| 4 | ninguna corrida reutilizo artefactos pilot | OK |
| 5 | ninguna corrida sobreescribio otra | OK |
| 6 | ninguna corrida cargo TEST | OK |
| 7 | todas usan TRAIN=84 sequences | OK |
| 8 | todas usan VAL=12 targets | OK |
| 9 | GC3 siempre 6273 params | OK |
| 10 | GE siempre 6657 params | OK |
| 11 | shuffle=False en todas | OK |
| 12 | misma configuracion cerrada salvo cultivar/modelo/seed | OK |
| 13 | history.csv contiene columnas requeridas | OK |
| 14 | best_epoch y stopped_epoch presentes | OK |
| 15 | checkpoint_best.keras presente | OK |
| 16 | sin NaN/Inf en loss o val_loss | OK |
| 17 | corridas fallidas registradas | OK |

## Confirmaciones metodologicas

- TEST 2025 no fue cargado ni evaluado.
- No se generaron predicciones TEST.
- No se calcularon metricas TEST.
- No hubo HPO.
- No se modificaron arquitectura, hiperparametros ni features.
- No se selecciono mejor seed.
- No se interpreto que modelo gano.
- No se reutilizaron checkpoints de `technical_pilot/` ni `technical_pilot_scaled/`.

## Archivos nuevos/modificados

- `v2_reentrenamiento/resultados_v2_final/official_gc3_ge/` con 40 directorios de corrida.
- `v2_reentrenamiento/auditorias/ejecutar_entrenamiento_oficial_gc3_ge.py`.
- `v2_reentrenamiento/auditorias/entrenamiento_oficial_gc3_ge_manifest.json`.
- `v2_reentrenamiento/auditorias/auditar_entrenamiento_oficial_gc3_ge.py`.
- `v2_reentrenamiento/auditorias/resumen_entrenamiento_oficial_gc3_ge.csv`.
- `v2_reentrenamiento/auditorias/resumen_entrenamiento_oficial_gc3_ge.json`.
- `v2_reentrenamiento/auditorias/AUDITORIA_ENTRENAMIENTO_OFICIAL_GC3_GE.md`.

## Fallos o reintentos

- No se registraron fallos ni reintentos.

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
?? v2_reentrenamiento/auditorias/AUDITORIA_ENTRENAMIENTO_OFICIAL_GC3_GE.md
?? v2_reentrenamiento/auditorias/AUDITORIA_RUNNER_OFICIAL_GC3_GE.md
?? v2_reentrenamiento/auditorias/auditar_entrenamiento_oficial_gc3_ge.py
?? v2_reentrenamiento/auditorias/auditar_runner_oficial_gc3_ge.py
?? v2_reentrenamiento/auditorias/ejecutar_entrenamiento_oficial_gc3_ge.py
?? v2_reentrenamiento/auditorias/entrenamiento_oficial_gc3_ge_manifest.json
?? v2_reentrenamiento/auditorias/resumen_entrenamiento_oficial_gc3_ge.csv
?? v2_reentrenamiento/auditorias/resumen_entrenamiento_oficial_gc3_ge.json
?? v2_reentrenamiento/auditorias/runner_oficial_gc3_ge_dry_run.json
?? v2_reentrenamiento/resultados_v2_final/official_gc3_ge/
?? v2_reentrenamiento/resultados_v2_final/technical_pilot/
?? v2_reentrenamiento/resultados_v2_final/technical_pilot_scaled/
?? v2_reentrenamiento/src/training/official_runner_gc3_ge.py
```

NO commit.
NO push.

ENTRENAMIENTO OFICIAL GC3/GE COMPLETADO Y LISTO PARA AUDITORÍA POST-RUN
