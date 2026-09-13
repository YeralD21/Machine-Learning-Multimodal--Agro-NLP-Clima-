# AUDITORIA PREFLIGHT FINAL SARIMA ROLLING

Pre-flight final corregido SOLO con TRAIN+VAL. No se cargo TEST 2025, no se generaron forecasts TEST y no se calcularon metricas TEST.

## Decisiones cerradas

- D46: rolling one-step con `forecast(steps=1)` seguido de `append([y_real], refit=False)`.
- D47: estimacion de parametros en TRAIN historico GC1 `2016-07..2023-12`.
- D48: VAL 2024 se usa exclusivamente como flujo observado para actualizar estado sin refit.
- D49: especificacion por cultivar conservada desde GC1 congelado.
- D50: reproducibilidad mediante metadata, parametros, hashes, git hash y log de actualizaciones.

## Especificaciones finales verificadas

| Cultivar | Experimento GC1 | order | seasonal_order |
|---|---|---|---|
| sutil | exp_002b_sarima_sutil_simple | (1, 1, 1) | (1, 1, 0, 12) |
| dulce | exp_002_sarima_dulce | (1, 0, 0) | (0, 1, 0, 12) |

## Resultado

- Estado: `approved`
- Controles: 38
- Statsmodels: 0.14.6
- TEST 2025 cargado: NO.
- Forecasts TEST: NO.
- Metricas TEST: NO.

## Controles

| Control | Estado | Detalle |
|---|---|---|
| sutil especificacion respaldada en GC1 | OK | (1, 1, 1)(1, 1, 0, 12) |
| sutil especificacion correcta por cultivar | OK | (1, 1, 1)(1, 1, 0, 12) |
| sutil TRAIN inicia 2016-07 | OK | 2016-07 |
| sutil TRAIN termina 2023-12 | OK | 2023-12 |
| sutil no TEST cargado | OK | False |
| sutil fit inicial solo TRAIN | OK | train=90, fit.nobs=90 |
| sutil 12 observaciones VAL | OK | 12 |
| sutil VAL Jan-Dec 2024 | OK | ['2024-01', '2024-02', '2024-03', '2024-04', '2024-05', '2024-06', '2024-07', '2024-08', '2024-09', '2024-10', '2024-11', '2024-12'] |
| sutil 12 append | OK | 12 |
| sutil todos append refit=False | OK | false |
| sutil ningun fit/refit durante VAL | OK | 0 |
| sutil forecast steps=1 soportado | OK | 12 one-step |
| sutil parametros invariantes despues de append | OK | [True, True, True, True, True, True, True, True, True, True, True, True] |
| sutil estado final llega a Dec-2024 | OK | 2024-12, nobs=102 |
| sutil no HPO | OK | sin grid/auto_arima/AIC/BIC |
| sutil no CV | OK | sin CV |
| sutil no seleccion de ordenes | OK | orden leido de config GC1 congelada |
| sutil no metricas TEST | OK | no se calcula TEST |
| sutil no forecasts TEST | OK | no se recorre 2025 |
| dulce especificacion respaldada en GC1 | OK | (1, 0, 0)(0, 1, 0, 12) |
| dulce especificacion correcta por cultivar | OK | (1, 0, 0)(0, 1, 0, 12) |
| dulce TRAIN inicia 2016-07 | OK | 2016-07 |
| dulce TRAIN termina 2023-12 | OK | 2023-12 |
| dulce no TEST cargado | OK | False |
| dulce fit inicial solo TRAIN | OK | train=90, fit.nobs=90 |
| dulce 12 observaciones VAL | OK | 12 |
| dulce VAL Jan-Dec 2024 | OK | ['2024-01', '2024-02', '2024-03', '2024-04', '2024-05', '2024-06', '2024-07', '2024-08', '2024-09', '2024-10', '2024-11', '2024-12'] |
| dulce 12 append | OK | 12 |
| dulce todos append refit=False | OK | false |
| dulce ningun fit/refit durante VAL | OK | 0 |
| dulce forecast steps=1 soportado | OK | 12 one-step |
| dulce parametros invariantes despues de append | OK | [True, True, True, True, True, True, True, True, True, True, True, True] |
| dulce estado final llega a Dec-2024 | OK | 2024-12, nobs=102 |
| dulce no HPO | OK | sin grid/auto_arima/AIC/BIC |
| dulce no CV | OK | sin CV |
| dulce no seleccion de ordenes | OK | orden leido de config GC1 congelada |
| dulce no metricas TEST | OK | no se calcula TEST |
| dulce no forecasts TEST | OK | no se recorre 2025 |

## Metadata D50

```json
{
  "dulce": {
    "code_hash": "d427a58a80aa237b2e7ffff8e3ca0f3e15210eca8514571e1fa4093c54539360",
    "cultivar": "dulce",
    "dataset_hash": "547443d1a117217b8f01f723f345749d530597b45210922a7e609469521934d0",
    "experiment": "exp_002_sarima_dulce",
    "final_nobs": 102,
    "final_state_last_observed": "2024-12",
    "fit_params_after_val": [
      1.7449298972217306,
      0.5806836224222154,
      555.587410313313
    ],
    "fit_params_initial": [
      1.7449298972217306,
      0.5806836224222154,
      555.587410313313
    ],
    "git_hash": "44fe6eafca927ba22d97bf8e46d6ad530cc1a47a",
    "order": [
      1,
      0,
      0
    ],
    "seasonal_order": [
      0,
      1,
      0,
      12
    ],
    "statsmodels_version": "0.14.6",
    "test_loaded": false,
    "timestamp": "2026-09-12T23:15:48.761732+00:00",
    "train_first": "2016-07",
    "train_last": "2023-12",
    "train_n": 90,
    "val_first": "2024-01",
    "val_last": "2024-12",
    "val_n": 12
  },
  "sutil": {
    "code_hash": "d427a58a80aa237b2e7ffff8e3ca0f3e15210eca8514571e1fa4093c54539360",
    "cultivar": "sutil",
    "dataset_hash": "220d9a9aabf6e41a31ee5950223eab3a74ddeadba18b8a8e281021b2af84b3f8",
    "experiment": "exp_002b_sarima_sutil_simple",
    "final_nobs": 102,
    "final_state_last_observed": "2024-12",
    "fit_params_after_val": [
      207.9869971004901,
      -0.05483771744209054,
      0.22441134712817645,
      -0.47318291608103935,
      20258571.196003255
    ],
    "fit_params_initial": [
      207.9869971004901,
      -0.05483771744209054,
      0.22441134712817645,
      -0.47318291608103935,
      20258571.196003255
    ],
    "git_hash": "44fe6eafca927ba22d97bf8e46d6ad530cc1a47a",
    "order": [
      1,
      1,
      1
    ],
    "seasonal_order": [
      1,
      1,
      0,
      12
    ],
    "statsmodels_version": "0.14.6",
    "test_loaded": false,
    "timestamp": "2026-09-12T23:15:48.648992+00:00",
    "train_first": "2016-07",
    "train_last": "2023-12",
    "train_n": 90,
    "val_first": "2024-01",
    "val_last": "2024-12",
    "val_n": 12
  }
}
```

## Actualizaciones VAL

Cada fila `updates` del JSON registra: observed_date, observed_y, forecast_origin, forecast_target, append_refit=false y verificacion de invariancia de parametros.
