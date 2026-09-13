# AUDITORIA EVALUACION TEST SARIMA ROLLING

Apertura unica y final de TEST 2025 para GC1/SARIMA rolling one-step.

## Especificaciones por cultivar

| Cultivar | Experimento | order | seasonal_order |
|---|---|---|---|
| sutil | exp_002b_sarima_sutil_simple | (1, 1, 1) | (1, 1, 0, 12) |
| dulce | exp_002_sarima_dulce | (1, 0, 0) | (0, 1, 0, 12) |

## Protocolo

- TRAIN: 2016-07..2023-12.
- VAL: 2024-01..2024-12 incorporado con append(refit=False).
- TEST: 2025-01..2025-12 rolling one-step.
- Serie cruda en toneladas; no hay escalado ni inverse-transform.
- No refit, no reseleccion, no HPO, no CV, no auto_arima.

## Metricas D35

| cultivar | model | MAE | RMSE | RelMAE_N1 | MASE_1 | RMSSE_1 | R2 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| sutil | SARIMA_rolling_one_step | 3985.016474 | 4894.896368 | 0.847102 | 1.180896 | 1.130440 | 0.684583 |
| dulce | SARIMA_rolling_one_step | 21.766759 | 31.616495 | 0.256984 | 0.296801 | 0.367415 | 0.965299 |

## Shocks D35-b

| cultivar | model | MAE_global | MAE_shock | MAE_nonshock | Delta_s | n_shock | n_nonshock |
| --- | --- | --- | --- | --- | --- | --- | --- |
| sutil | SARIMA_rolling_one_step | 3985.016474 | 7824.402904 | 2705.220997 | 96.345560 | 3 | 9 |
| dulce | SARIMA_rolling_one_step | 21.766759 | 11.603702 | 25.154444 | -46.690723 | 3 | 9 |

Delta_s es un indice descriptivo de deterioro condicional ante shocks; no es prueba causal ni prueba estadistica de resiliencia.

## Hashes y parametros

```json
{
  "dulce": {
    "code_hash": "91181d5a5e3de596eb0c0ee50fcb325d7aed0e4d469edd4d53722ee5576d6b65",
    "cultivar": "dulce",
    "dataset_hash": "547443d1a117217b8f01f723f345749d530597b45210922a7e609469521934d0",
    "experiment": "exp_002_sarima_dulce",
    "final_state_last_observed": "2025-12",
    "fit_kwargs": {
      "enforce_invertibility": false,
      "enforce_stationarity": false,
      "initialization": "approximate_diffuse",
      "trend": "c"
    },
    "fit_params_after_test": [
      1.7449298972217306,
      0.5806836224222154,
      555.587410313313
    ],
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
    "initial_test_state_last_observed": "2024-12",
    "no_scaling_inverse_transform": true,
    "order": [
      1,
      0,
      0
    ],
    "scaling_note": "SARIMA GC1 was fit directly on raw toneladas from master_dataset_{cultivar}_v2.csv.",
    "seasonal_order": [
      0,
      1,
      0,
      12
    ],
    "statsmodels_version": "0.14.6",
    "test_first": "2025-01",
    "test_last": "2025-12",
    "test_n": 12,
    "timestamp": "2026-09-12T23:22:22.280460+00:00",
    "train_first": "2016-07",
    "train_last": "2023-12",
    "train_n": 90,
    "val_first": "2024-01",
    "val_last": "2024-12",
    "val_n": 12
  },
  "sutil": {
    "code_hash": "91181d5a5e3de596eb0c0ee50fcb325d7aed0e4d469edd4d53722ee5576d6b65",
    "cultivar": "sutil",
    "dataset_hash": "220d9a9aabf6e41a31ee5950223eab3a74ddeadba18b8a8e281021b2af84b3f8",
    "experiment": "exp_002b_sarima_sutil_simple",
    "final_state_last_observed": "2025-12",
    "fit_kwargs": {
      "enforce_invertibility": false,
      "enforce_stationarity": false,
      "initialization": "approximate_diffuse",
      "trend": "c"
    },
    "fit_params_after_test": [
      207.9869971004901,
      -0.05483771744209054,
      0.22441134712817645,
      -0.47318291608103935,
      20258571.196003255
    ],
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
    "initial_test_state_last_observed": "2024-12",
    "no_scaling_inverse_transform": true,
    "order": [
      1,
      1,
      1
    ],
    "scaling_note": "SARIMA GC1 was fit directly on raw toneladas from master_dataset_{cultivar}_v2.csv.",
    "seasonal_order": [
      1,
      1,
      0,
      12
    ],
    "statsmodels_version": "0.14.6",
    "test_first": "2025-01",
    "test_last": "2025-12",
    "test_n": 12,
    "timestamp": "2026-09-12T23:22:22.157167+00:00",
    "train_first": "2016-07",
    "train_last": "2023-12",
    "train_n": 90,
    "val_first": "2024-01",
    "val_last": "2024-12",
    "val_n": 12
  }
}
```

## Artefactos

```json
{
  "audit": "C:\\Machine-learming\\Machine-Learning-Multimodal--Agro-NLP-Clima-\\v2_reentrenamiento\\auditorias\\AUDITORIA_EVALUACION_TEST_SARIMA_ROLLING.md",
  "figures": [
    "C:\\Machine-learming\\Machine-Learning-Multimodal--Agro-NLP-Clima-\\v2_reentrenamiento\\resultados_v2_final\\evaluacion_test_sarima_rolling\\figuras\\sutil_real_vs_predicted.png",
    "C:\\Machine-learming\\Machine-Learning-Multimodal--Agro-NLP-Clima-\\v2_reentrenamiento\\resultados_v2_final\\evaluacion_test_sarima_rolling\\figuras\\dulce_real_vs_predicted.png",
    "C:\\Machine-learming\\Machine-Learning-Multimodal--Agro-NLP-Clima-\\v2_reentrenamiento\\resultados_v2_final\\evaluacion_test_sarima_rolling\\figuras\\shock_vs_nonshock_abs_error.png"
  ],
  "manifest": "C:\\Machine-learming\\Machine-Learning-Multimodal--Agro-NLP-Clima-\\v2_reentrenamiento\\resultados_v2_final\\evaluacion_test_sarima_rolling\\manifest_evaluacion_test_sarima_rolling.json",
  "metrics": "C:\\Machine-learming\\Machine-Learning-Multimodal--Agro-NLP-Clima-\\v2_reentrenamiento\\resultados_v2_final\\evaluacion_test_sarima_rolling\\metricas_test_sarima_rolling.csv",
  "predictions": "C:\\Machine-learming\\Machine-Learning-Multimodal--Agro-NLP-Clima-\\v2_reentrenamiento\\resultados_v2_final\\evaluacion_test_sarima_rolling\\predicciones_test_sarima_rolling.csv",
  "shock_metrics": "C:\\Machine-learming\\Machine-Learning-Multimodal--Agro-NLP-Clima-\\v2_reentrenamiento\\resultados_v2_final\\evaluacion_test_sarima_rolling\\metricas_shock_sarima_rolling.csv"
}
```

## Controles

| Control | Estado | Detalle |
|---|---|---|
| statsmodels=0.14.6 | OK | 0.14.6 |
| sutil especificacion correcta | OK | (1, 1, 1)(1, 1, 0, 12) |
| sutil TRAIN original correcto | OK | 2016-07..2023-12, n=90 |
| sutil parametros iniciales = despues de VAL | OK | allclose atol=1e-10 |
| sutil estado inicial TEST llega a Dec-2024 | OK | 2024-12 |
| sutil TEST Jan-Dec 2025 | OK | ['2025-01', '2025-02', '2025-03', '2025-04', '2025-05', '2025-06', '2025-07', '2025-08', '2025-09', '2025-10', '2025-11', '2025-12'] |
| sutil 12 predicciones | OK | 12 |
| sutil primer forecast Dec2024 -> Jan2025 | OK | 2024-12->2025-01 |
| sutil ultimo forecast Nov2025 -> Dec2025 | OK | 2025-11->2025-12 |
| sutil forecast steps=1 siempre | OK | all 1 |
| sutil append refit=False siempre | OK | all false |
| sutil ningun fit/refit durante TEST | OK | solo append(refit=False) |
| sutil parametros invariantes tras cada append TEST | OK | [True, True, True, True, True, True, True, True, True, True, True, True] |
| sutil no HPO | OK | sin HPO |
| sutil no CV | OK | sin CV |
| sutil no seleccion de ordenes | OK | orden verificado contra config GC1 |
| sutil no auto_arima | OK | no import/call |
| sutil no modificacion dataset | OK | lectura solamente |
| sutil no modificacion D35 | OK | denominadores constantes |
| sutil no modificacion shocks | OK | mascaras congeladas |
| sutil n_shock=3 | OK | 3 |
| sutil n_nonshock=9 | OK | 9 |
| dulce especificacion correcta | OK | (1, 0, 0)(0, 1, 0, 12) |
| dulce TRAIN original correcto | OK | 2016-07..2023-12, n=90 |
| dulce parametros iniciales = despues de VAL | OK | allclose atol=1e-10 |
| dulce estado inicial TEST llega a Dec-2024 | OK | 2024-12 |
| dulce TEST Jan-Dec 2025 | OK | ['2025-01', '2025-02', '2025-03', '2025-04', '2025-05', '2025-06', '2025-07', '2025-08', '2025-09', '2025-10', '2025-11', '2025-12'] |
| dulce 12 predicciones | OK | 12 |
| dulce primer forecast Dec2024 -> Jan2025 | OK | 2024-12->2025-01 |
| dulce ultimo forecast Nov2025 -> Dec2025 | OK | 2025-11->2025-12 |
| dulce forecast steps=1 siempre | OK | all 1 |
| dulce append refit=False siempre | OK | all false |
| dulce ningun fit/refit durante TEST | OK | solo append(refit=False) |
| dulce parametros invariantes tras cada append TEST | OK | [True, True, True, True, True, True, True, True, True, True, True, True] |
| dulce no HPO | OK | sin HPO |
| dulce no CV | OK | sin CV |
| dulce no seleccion de ordenes | OK | orden verificado contra config GC1 |
| dulce no auto_arima | OK | no import/call |
| dulce no modificacion dataset | OK | lectura solamente |
| dulce no modificacion D35 | OK | denominadores constantes |
| dulce no modificacion shocks | OK | mascaras congeladas |
| dulce n_shock=3 | OK | 3 |
| dulce n_nonshock=9 | OK | 9 |
| 2 modelos oficiales encontrados | OK | 2 |
| 12 predicciones Sutil | OK | 12 |
| 12 predicciones Dulce | OK | 12 |
| 24 predicciones totales | OK | 24 |
| no segunda evaluacion TEST | OK | manifest no existia antes de iniciar |

## Confirmacion

- Apertura unica TEST SARIMA rolling: SI.
- Resultados congelados para interpretacion posterior: SI.
