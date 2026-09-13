# AUDITORIA EVALUACION TEST GC3/GE

Apertura unica y evaluacion final de TEST 2025 para las 40 corridas oficiales GC3/GE v2.
No se reentreno ningun modelo, no hubo HPO, no se modificaron arquitectura, hiperparametros, features, scalers, shocks ni metricas, y no se selecciono mejor seed.

## Alcance y conteos

- Checkpoints evaluados: 40
- Predicciones generadas: 480
- Matriz: 2 cultivares x 2 modelos x 10 seeds.
- Cada corrida produjo exactamente 12 predicciones TEST: enero 2025 a diciembre 2025.
- Inicio UTC: 2026-09-11T17:01:31.142238+00:00
- Fin UTC: 2026-09-11T17:01:45.375706+00:00
- Duracion segundos: 14.233468

## Alineacion temporal

- Protocolo verificado: secuencia termina en t y predice y_(t+1).
- Primera prediccion TEST 2025-01 usa contexto historico que termina en 2024-12.
- y_true es identico entre seeds del mismo cultivar y entre GC3/GE dentro de cada cultivar.
- Fechas TEST exactas Jan-Dec 2025, sin duplicados ni meses faltantes.

## Inverse transform

- y_true_scaled e y_pred_scaled se convirtieron a toneladas usando scaler v2c TRAIN-only por cultivar.
- No se refitteo ni recalculo ningun scaler.
- Reconstruccion de y_true contra dataset raw/features verificada con max abs diff <= 1e-8.

## Metricas D35

- Metricas globales calculadas en toneladas: MAE, RMSE, RelMAE_N1, MASE_1, RMSSE_1, R2.
- RelMAE_N1 = MAE_modelo / MAE_Naive_t-1 sobre los mismos 12 meses TEST.
- Denominadores congelados: Sutil D_MASE1=3374.5715, D_RMSSE1=18749601.6694; Dulce D_MASE1=73.3380, D_RMSSE1=7404.7927.

## Shocks D35

- Mascara oficial congelada usada sin redefinicion post hoc.
- Sutil shock TEST: 2025-01, 2025-07, 2025-11.
- Dulce shock TEST: 2025-01, 2025-02, 2025-03.
- n_shock=3 y n_nonshock=9 en todas las corridas.
- Delta_s se reporta como indice descriptivo de deterioro condicional ante shocks definido en este estudio; no es prueba causal ni metrica universal de resiliencia.

## Resumen multi-seed TEST

Valores descriptivos por cultivar/modelo, n=10 seeds. No constituyen seleccion de seed ni ranking cientifico final.

| cultivar | modelo | n | MAE_mean | MAE_median | MAE_sd | MAE_min | MAE_max | RMSE_mean | RelMAE_N1_mean | MASE_1_mean | RMSSE_1_mean | R2_mean | MAE_shock_mean | MAE_nonshock_mean | Delta_s_mean |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| dulce | GC3 | 10 | 103.413025 | 104.161885 | 20.635199 | 57.581582 | 128.958173 | 125.318120 | 1.220921 | 1.410088 | 1.456322 | 0.436190 | 118.944194 | 98.235969 | 13.262077 |
| dulce | GE | 10 | 95.584556 | 103.564856 | 20.000875 | 53.721550 | 117.683182 | 116.899722 | 1.128496 | 1.303343 | 1.358492 | 0.507517 | 99.907589 | 94.143546 | 1.903821 |
| sutil | GC3 | 10 | 7746.000211 | 7800.597771 | 1528.989957 | 5760.589144 | 10247.144676 | 8684.061051 | 1.646581 | 2.295403 | 2.005519 | -0.034742 | 5594.109032 | 8463.297270 | -28.104799 |
| sutil | GE | 10 | 9426.458661 | 9601.156008 | 1481.729376 | 6129.185075 | 11131.093933 | 10830.150126 | 2.003799 | 2.793379 | 2.501143 | -0.584054 | 7908.487889 | 9932.448918 | -16.601239 |

## Comparacion pareada GC3 -> GE

- Tabla pareada creada por cultivar + seed.
- Incluye delta_MAE_GE_minus_GC3, delta_RMSE_GE_minus_GC3, delta_Delta_s_GE_minus_GC3 y delta_MAE_shock_GE_minus_GC3.
- No se ejecuto ningun test de significancia y no se descarto ningun par desfavorable.

Resumen descriptivo de deltas pareados:

| cultivar | n | delta_MAE_GE_minus_GC3_mean | delta_MAE_GE_minus_GC3_median | delta_MAE_GE_minus_GC3_sd | delta_MAE_GE_minus_GC3_min | delta_MAE_GE_minus_GC3_max | delta_RMSE_GE_minus_GC3_mean | delta_RMSE_GE_minus_GC3_median | delta_RMSE_GE_minus_GC3_sd | delta_RMSE_GE_minus_GC3_min | delta_RMSE_GE_minus_GC3_max | delta_Delta_s_GE_minus_GC3_mean | delta_Delta_s_GE_minus_GC3_median | delta_Delta_s_GE_minus_GC3_sd | delta_Delta_s_GE_minus_GC3_min | delta_Delta_s_GE_minus_GC3_max | delta_MAE_shock_GE_minus_GC3_mean | delta_MAE_shock_GE_minus_GC3_median | delta_MAE_shock_GE_minus_GC3_sd | delta_MAE_shock_GE_minus_GC3_min | delta_MAE_shock_GE_minus_GC3_max |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| dulce | 10 | -7.828469 | -6.329370 | 10.171667 | -25.445003 | 9.237517 | -8.418398 | -7.816441 | 13.608502 | -27.906599 | 16.007380 | -11.358255 | -7.345558 | 9.062219 | -24.643683 | -1.326452 | -19.036605 | -21.378709 | 13.178816 | -32.870790 | 10.026370 |
| sutil | 10 | 1680.458450 | 1527.367325 | 1321.539086 | -101.112312 | 4189.169386 | 2146.089075 | 1681.917401 | 1478.458446 | 185.472645 | 4925.890830 | 11.503560 | 11.305600 | 14.803366 | -7.053484 | 37.893193 | 2314.378856 | 2390.848988 | 1715.875117 | -503.600872 | 4731.399804 |

## Figuras

- `v2_reentrenamiento\resultados_v2_final\evaluacion_test_gc3_ge\figures\real_vs_pred_test_2025_sutil.png`
- `v2_reentrenamiento\resultados_v2_final\evaluacion_test_gc3_ge\figures\real_vs_pred_test_2025_dulce.png`
- `v2_reentrenamiento\resultados_v2_final\evaluacion_test_gc3_ge\figures\distribucion_mae_test_2025_sutil.png`
- `v2_reentrenamiento\resultados_v2_final\evaluacion_test_gc3_ge\figures\distribucion_mae_test_2025_dulce.png`
- `v2_reentrenamiento\resultados_v2_final\evaluacion_test_gc3_ge\figures\shock_diagnostico_test_2025_sutil.png`
- `v2_reentrenamiento\resultados_v2_final\evaluacion_test_gc3_ge\figures\shock_diagnostico_test_2025_dulce.png`

## Sanity checks

| Check | Estado |
|---|---|
| 40_corridas_evaluadas | OK |
| 480_predicciones_totales | OK |
| 12_meses_por_corrida | OK |
| jan_dec_2025 | OK |
| 10_seeds_por_cultivar_modelo | OK |
| sin_nan_inf_predicciones | OK |
| sin_nan_inf_metricas | OK |
| y_true_identico_entre_seeds | OK |
| y_true_identico_gc3_ge | OK |
| n_shock_3_siempre | OK |
| n_nonshock_9_siempre | OK |
| params_6273_6657 | OK |
| scaler_correcto_por_cultivar | OK |
| inverse_transform_correcto | OK |
| denominadores_mase_rmsse_correctos | OK |
| mascara_shock_exacta | OK |
| checkpoint_oficial_correcto | OK |
| ninguna_ruta_pilot_usada | OK |
| ninguna_corrida_reentrenada | OK |
| ningun_checkpoint_modificado | OK |

## Archivos creados

- `v2_reentrenamiento\resultados_v2_final\evaluacion_test_gc3_ge\predicciones_test_gc3_ge.csv`
- `v2_reentrenamiento\resultados_v2_final\evaluacion_test_gc3_ge\metricas_test_por_seed_gc3_ge.csv`
- `v2_reentrenamiento\resultados_v2_final\evaluacion_test_gc3_ge\resumen_test_multiseed_gc3_ge.csv`
- `v2_reentrenamiento\resultados_v2_final\evaluacion_test_gc3_ge\comparacion_pareada_gc3_vs_ge.csv`
- `v2_reentrenamiento\resultados_v2_final\evaluacion_test_gc3_ge\metricas_shock_por_seed_gc3_ge.csv`
- `v2_reentrenamiento\resultados_v2_final\evaluacion_test_gc3_ge\manifest_evaluacion_test_gc3_ge.json`
- `v2_reentrenamiento\auditorias\AUDITORIA_EVALUACION_TEST_GC3_GE.md`
- `v2_reentrenamiento\resultados_v2_final\evaluacion_test_gc3_ge\figures/`

## Incidencias

- No se detectaron incidencias en los sanity checks.
- Los resultados se conservan sin modificar, independientemente de su direccion o magnitud.

## Git status

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
?? v2_reentrenamiento/auditorias/evaluar_test_gc3_ge.py
?? v2_reentrenamiento/auditorias/resumen_entrenamiento_oficial_gc3_ge.csv
?? v2_reentrenamiento/auditorias/resumen_entrenamiento_oficial_gc3_ge.json
?? v2_reentrenamiento/auditorias/resumen_post_run_gc3_ge.csv
?? v2_reentrenamiento/auditorias/resumen_post_run_gc3_ge.json
?? v2_reentrenamiento/auditorias/runner_oficial_gc3_ge_dry_run.json
?? v2_reentrenamiento/resultados_v2_final/evaluacion_test_gc3_ge/
?? v2_reentrenamiento/resultados_v2_final/official_gc3_ge/
?? v2_reentrenamiento/resultados_v2_final/technical_pilot/
?? v2_reentrenamiento/resultados_v2_final/technical_pilot_scaled/
?? v2_reentrenamiento/src/training/official_runner_gc3_ge.py
```

NO commit.
NO push.

EVALUACIÓN TEST GC3/GE COMPLETADA Y CONGELADA PARA INTERPRETACIÓN
