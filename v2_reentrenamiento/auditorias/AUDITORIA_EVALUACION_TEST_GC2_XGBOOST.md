# AUDITORIA EVALUACION TEST GC2/XGBOOST

Apertura unica y final de TEST 2025 para los 20 modelos oficiales GC2/XGBoost v2.

## Alcance

- Modelos evaluados: 20.
- Seeds: 0..9 por cultivar.
- Cultivares: sutil, dulce.
- Fechas TEST: 2025-01..2025-12.
- Predicciones: 240.
- Representacion: X_t -> y_(t+1).
- Primer forecast: 2024-12 -> 2025-01.
- No retraining, no HPO, no CV, no early stopping, no best seed.

## Metricas TEST por seed

| cultivar | model | seed | MAE | RMSE | RelMAE_N1 | MASE_1 | RMSSE_1 | R2 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| sutil | GC2_XGBoost | 0 | 6065.325456 | 7087.433503 | 1.289317 | 1.797362 | 1.636790 | 0.338734 |
| sutil | GC2_XGBoost | 1 | 5848.795984 | 6994.461575 | 1.243289 | 1.733197 | 1.615319 | 0.355969 |
| sutil | GC2_XGBoost | 2 | 5919.309847 | 7080.660732 | 1.258278 | 1.754092 | 1.635226 | 0.339997 |
| sutil | GC2_XGBoost | 3 | 5872.044253 | 7068.904496 | 1.248231 | 1.740086 | 1.632511 | 0.342187 |
| sutil | GC2_XGBoost | 4 | 5775.315395 | 6923.503496 | 1.227669 | 1.711422 | 1.598932 | 0.368970 |
| sutil | GC2_XGBoost | 5 | 5919.914015 | 6891.722605 | 1.258407 | 1.754271 | 1.591592 | 0.374750 |
| sutil | GC2_XGBoost | 6 | 5626.916107 | 6834.980366 | 1.196124 | 1.667446 | 1.578488 | 0.385003 |
| sutil | GC2_XGBoost | 7 | 5867.352570 | 7017.679911 | 1.247234 | 1.738696 | 1.620681 | 0.351686 |
| sutil | GC2_XGBoost | 8 | 6059.972587 | 7103.125307 | 1.288179 | 1.795775 | 1.640414 | 0.335802 |
| sutil | GC2_XGBoost | 9 | 6068.807395 | 7105.380628 | 1.290057 | 1.798393 | 1.640935 | 0.335380 |
| dulce | GC2_XGBoost | 0 | 56.048322 | 72.921016 | 0.661721 | 0.764247 | 0.847415 | 0.815404 |
| dulce | GC2_XGBoost | 1 | 58.837185 | 74.744090 | 0.694647 | 0.802274 | 0.868601 | 0.806059 |
| dulce | GC2_XGBoost | 2 | 60.938236 | 79.450275 | 0.719453 | 0.830923 | 0.923292 | 0.780867 |
| dulce | GC2_XGBoost | 3 | 60.482078 | 77.204060 | 0.714067 | 0.824703 | 0.897188 | 0.793083 |
| dulce | GC2_XGBoost | 4 | 56.987896 | 75.735393 | 0.672814 | 0.777058 | 0.880121 | 0.800880 |
| dulce | GC2_XGBoost | 5 | 64.173183 | 84.933167 | 0.757645 | 0.875033 | 0.987008 | 0.749579 |
| dulce | GC2_XGBoost | 6 | 60.374094 | 78.527913 | 0.712792 | 0.823231 | 0.912573 | 0.785926 |
| dulce | GC2_XGBoost | 7 | 59.498169 | 77.370041 | 0.702451 | 0.811287 | 0.899117 | 0.792192 |
| dulce | GC2_XGBoost | 8 | 59.909183 | 77.216951 | 0.707303 | 0.816891 | 0.897338 | 0.793014 |
| dulce | GC2_XGBoost | 9 | 56.540485 | 73.953779 | 0.667532 | 0.770958 | 0.859417 | 0.810139 |

## Resumen multi-seed

| cultivar | MAE_mean | MAE_median | MAE_SD | MAE_min | MAE_max | RMSE_mean | RMSE_median | RMSE_SD | RMSE_min | RMSE_max | RelMAE_N1_mean | RelMAE_N1_median | RelMAE_N1_SD | RelMAE_N1_min | RelMAE_N1_max | MASE_1_mean | MASE_1_median | MASE_1_SD | MASE_1_min | MASE_1_max | RMSSE_1_mean | RMSSE_1_median | RMSSE_1_SD | RMSSE_1_min | RMSSE_1_max | R2_mean | R2_median | R2_SD | R2_min | R2_max |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| dulce | 59.378883 | 59.703676 | 2.428516 | 56.048322 | 64.173183 | 77.205668 | 77.210505 | 3.394146 | 72.921016 | 84.933167 | 0.701042 | 0.704877 | 0.028672 | 0.661721 | 0.757645 | 0.809661 | 0.814089 | 0.033114 | 0.764247 | 0.875033 | 0.897207 | 0.897263 | 0.039443 | 0.847415 | 0.987008 | 0.792714 | 0.793048 | 0.018591 | 0.749579 | 0.815404 |
| sutil | 5902.375361 | 5895.677050 | 140.128108 | 5626.916107 | 6068.807395 | 7010.785262 | 7043.292203 | 96.968340 | 6834.980366 | 7105.380628 | 1.254679 | 1.253255 | 0.029787 | 1.196124 | 1.290057 | 1.749074 | 1.747089 | 0.041525 | 1.667446 | 1.798393 | 1.619089 | 1.626596 | 0.022394 | 1.578488 | 1.640935 | 0.352848 | 0.346936 | 0.017821 | 0.335380 | 0.385003 |

## Metricas shock por seed

| cultivar | model | seed | MAE_global | MAE_shock | MAE_nonshock | Delta_s | n_shock | n_nonshock |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| sutil | GC2_XGBoost | 0 | 6065.325456 | 8314.114791 | 5315.729011 | 37.076153 | 3 | 9 |
| sutil | GC2_XGBoost | 1 | 5848.795984 | 8250.025234 | 5048.386234 | 41.055104 | 3 | 9 |
| sutil | GC2_XGBoost | 2 | 5919.309847 | 8112.141934 | 5188.365818 | 37.045401 | 3 | 9 |
| sutil | GC2_XGBoost | 3 | 5872.044253 | 8543.033656 | 4981.714451 | 45.486534 | 3 | 9 |
| sutil | GC2_XGBoost | 4 | 5775.315395 | 8407.400598 | 4897.953661 | 45.574744 | 3 | 9 |
| sutil | GC2_XGBoost | 5 | 5919.914015 | 7830.156834 | 5283.166409 | 32.268084 | 3 | 9 |
| sutil | GC2_XGBoost | 6 | 5626.916107 | 8214.790451 | 4764.291326 | 45.990989 | 3 | 9 |
| sutil | GC2_XGBoost | 7 | 5867.352570 | 8147.649966 | 5107.253438 | 38.864162 | 3 | 9 |
| sutil | GC2_XGBoost | 8 | 6059.972587 | 8557.917719 | 5227.324209 | 41.220403 | 3 | 9 |
| sutil | GC2_XGBoost | 9 | 6068.807395 | 8591.733981 | 5227.831867 | 41.572033 | 3 | 9 |
| dulce | GC2_XGBoost | 0 | 56.048322 | 86.541528 | 45.883920 | 54.405208 | 3 | 9 |
| dulce | GC2_XGBoost | 1 | 58.837185 | 83.427846 | 50.640297 | 41.794423 | 3 | 9 |
| dulce | GC2_XGBoost | 2 | 60.938236 | 94.271906 | 49.827013 | 54.700745 | 3 | 9 |
| dulce | GC2_XGBoost | 3 | 60.482078 | 91.550238 | 50.126025 | 51.367546 | 3 | 9 |
| dulce | GC2_XGBoost | 4 | 56.987896 | 85.538833 | 47.470917 | 50.100003 | 3 | 9 |
| dulce | GC2_XGBoost | 5 | 64.173183 | 97.824893 | 52.955946 | 52.438898 | 3 | 9 |
| dulce | GC2_XGBoost | 6 | 60.374094 | 91.478593 | 50.005927 | 51.519613 | 3 | 9 |
| dulce | GC2_XGBoost | 7 | 59.498169 | 94.767315 | 47.741787 | 59.277701 | 3 | 9 |
| dulce | GC2_XGBoost | 8 | 59.909183 | 85.584092 | 51.350880 | 42.856383 | 3 | 9 |
| dulce | GC2_XGBoost | 9 | 56.540485 | 90.059764 | 45.367392 | 59.283678 | 3 | 9 |

## Resumen shock multi-seed

| cultivar | MAE_global_mean | MAE_global_median | MAE_global_SD | MAE_global_min | MAE_global_max | MAE_shock_mean | MAE_shock_median | MAE_shock_SD | MAE_shock_min | MAE_shock_max | MAE_nonshock_mean | MAE_nonshock_median | MAE_nonshock_SD | MAE_nonshock_min | MAE_nonshock_max | Delta_s_mean | Delta_s_median | Delta_s_SD | Delta_s_min | Delta_s_max | n_shock_mean | n_shock_median | n_shock_SD | n_shock_min | n_shock_max | n_nonshock_mean | n_nonshock_median | n_nonshock_SD | n_nonshock_min | n_nonshock_max |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| dulce | 59.378883 | 59.703676 | 2.428516 | 56.048322 | 64.173183 | 90.104501 | 90.769178 | 4.728704 | 83.427846 | 97.824893 | 49.137010 | 49.916470 | 2.437434 | 45.367392 | 52.955946 | 51.774420 | 51.979255 | 5.876318 | 41.794423 | 59.283678 | 3.000000 | 3.000000 | 0.000000 | 3.000000 | 3.000000 | 9.000000 | 9.000000 | 0.000000 | 9.000000 | 9.000000 |
| sutil | 5902.375361 | 5895.677050 | 140.128108 | 5626.916107 | 6068.807395 | 8296.896516 | 8282.070012 | 238.361386 | 7830.156834 | 8591.733981 | 5104.201642 | 5147.809628 | 179.661553 | 4764.291326 | 5315.729011 | 40.615361 | 41.137753 | 4.431079 | 32.268084 | 45.990989 | 3.000000 | 3.000000 | 0.000000 | 3.000000 | 3.000000 | 9.000000 | 9.000000 | 0.000000 | 9.000000 | 9.000000 |

Delta_s se conserva como indice descriptivo de deterioro condicional ante shocks definido en este estudio; no es una prueba causal ni una prueba estadistica de resiliencia.

## Denominadores y mascaras congeladas

```json
{
  "D_MASE1": {
    "dulce": 73.338,
    "sutil": 3374.5715
  },
  "D_RMSSE1": {
    "dulce": 7404.7927,
    "sutil": 18749601.6694
  },
  "NAIVE_TEST_MAE": {
    "dulce": 84.70083333333332,
    "sutil": 4704.292916666665
  },
  "P75_SHOCK": {
    "dulce": 0.320281354618397,
    "sutil": 0.240834900212216
  },
  "SHOCK_MONTHS": {
    "dulce": [
      "2025-01",
      "2025-02",
      "2025-03"
    ],
    "sutil": [
      "2025-01",
      "2025-07",
      "2025-11"
    ]
  }
}
```

## Hashes

```json
{
  "datasets": {
    "dulce": {
      "raw_features_for_inverse_check": "c0e7cbe99fdf514cf3bde3c3f735b7d14fc63b27c7f3f163ec665ae25696a188",
      "scaled": "38466a4f9581f7d8058962248161d7cb1d467a82e7c2092de73364920dd84f80"
    },
    "sutil": {
      "raw_features_for_inverse_check": "a6f23d0eb3178514c75f6851f3c2039837a192acc5f2b008bc445dda35968cc9",
      "scaled": "d664fd728798d52e5c1bbe702745d40d49b8761513cef148e0a56f2481ca7941"
    }
  },
  "models": {
    "dulce_seed_00": "d71f71406fc98f1f52bd73eba9f17a7af01ea9ef1e67e099da20cc94a2e34d20",
    "dulce_seed_01": "6450745f2864468bba570c6bdf5adf05d950cca3e2e5f377867b48ee8a323206",
    "dulce_seed_02": "8694ef4c908b5ae13e7892516d04b1bf2c1aa60d5b6676ddda2ca6f0b5a3f5c3",
    "dulce_seed_03": "67b9c56acd70ddb67c5ca16379dfeeb1b07f1c3a2a3f4afa21415d4cb0685be5",
    "dulce_seed_04": "33acd07923b0d1a93ea9c0c0ab56dd5d5efd8eb881e32c6080b248c28ba269c8",
    "dulce_seed_05": "06ba01afd7d718008c3362b8fa4568939472c1eefcda09103dc7701fb986330f",
    "dulce_seed_06": "409389419182e439caa8a726ab964c4fec31d0c38411a37fca2fe83bdda1800e",
    "dulce_seed_07": "eff56ad3dda7a6fb40dbe18fc15057cfb4e88b87f7e0032a13b69d3ad1249e7f",
    "dulce_seed_08": "6dde157f7fe3061ad89c57dd402adf61691f9c93a32d9272f78449deca584474",
    "dulce_seed_09": "d15cc99e6e49aaefa2d4845cb5af013d809eb76985ff4c00dbd4025e92cfc43f",
    "sutil_seed_00": "acaf1d44edc01823febf6aa95ce4b3ea89a7cea694bdc7cfbe8365304e443e8b",
    "sutil_seed_01": "ef322b4cc6e8689d067c8b3a07a175730a2a8ba006f2cf6e45a59230d279b225",
    "sutil_seed_02": "4d18262bf29c94df9944893eede19ab5b20e67275cdb152301ae568038d31426",
    "sutil_seed_03": "6a94eb25eb1e09ceb70de96475ef9aa695ebedd9973526da835e6621e2bd530f",
    "sutil_seed_04": "7997fae68e6f97370c41b24ceaeef2416a72c523240fde9074d63788f9bd9b28",
    "sutil_seed_05": "0134f39afa9d167dcb7392f35c80d494bd33cb35dc601a24731ff539bd3a207f",
    "sutil_seed_06": "d66ba0cdaa5571f3c41daa4c056a824d636505eb024a377c0e4c5ef2c2cc9c5d",
    "sutil_seed_07": "61f29a78afb76e9de9fbfb8cbfea1ef3d7a2a6e57e76ac4dd5136d0e58c96e8a",
    "sutil_seed_08": "b3e892a273a4a3648f75b9e3b3c422daa32c08f889e9b8200d8a6d21ef12d2c6",
    "sutil_seed_09": "17dd9848c14e3d186792c4d7bf83b524257ebe9997c6fbe3440fdd931d4a988f"
  },
  "scalers": {
    "dulce": "1bf919b4c00d236801f12df420b79a16f47c4fe4df3b711472cdc36428eb962b",
    "sutil": "ba51666f00ded716c63698dd04e29068d58d3cab064202384bfbd4435fbf350e"
  }
}
```

## Artefactos

```json
{
  "audit": "C:\\Machine-learming\\Machine-Learning-Multimodal--Agro-NLP-Clima-\\v2_reentrenamiento\\auditorias\\AUDITORIA_EVALUACION_TEST_GC2_XGBOOST.md",
  "figures": [
    "C:\\Machine-learming\\Machine-Learning-Multimodal--Agro-NLP-Clima-\\v2_reentrenamiento\\resultados_v2_final\\evaluacion_test_gc2_xgboost\\figuras\\sutil_real_vs_pred_seed_mediana_mae.png",
    "C:\\Machine-learming\\Machine-Learning-Multimodal--Agro-NLP-Clima-\\v2_reentrenamiento\\resultados_v2_final\\evaluacion_test_gc2_xgboost\\figuras\\sutil_distribucion_mae_seeds.png",
    "C:\\Machine-learming\\Machine-Learning-Multimodal--Agro-NLP-Clima-\\v2_reentrenamiento\\resultados_v2_final\\evaluacion_test_gc2_xgboost\\figuras\\sutil_error_shock_vs_nonshock.png",
    "C:\\Machine-learming\\Machine-Learning-Multimodal--Agro-NLP-Clima-\\v2_reentrenamiento\\resultados_v2_final\\evaluacion_test_gc2_xgboost\\figuras\\dulce_real_vs_pred_seed_mediana_mae.png",
    "C:\\Machine-learming\\Machine-Learning-Multimodal--Agro-NLP-Clima-\\v2_reentrenamiento\\resultados_v2_final\\evaluacion_test_gc2_xgboost\\figuras\\dulce_distribucion_mae_seeds.png",
    "C:\\Machine-learming\\Machine-Learning-Multimodal--Agro-NLP-Clima-\\v2_reentrenamiento\\resultados_v2_final\\evaluacion_test_gc2_xgboost\\figuras\\dulce_error_shock_vs_nonshock.png"
  ],
  "metrics_by_seed": "C:\\Machine-learming\\Machine-Learning-Multimodal--Agro-NLP-Clima-\\v2_reentrenamiento\\resultados_v2_final\\evaluacion_test_gc2_xgboost\\metricas_test_por_seed_gc2_xgboost.csv",
  "predictions": "C:\\Machine-learming\\Machine-Learning-Multimodal--Agro-NLP-Clima-\\v2_reentrenamiento\\resultados_v2_final\\evaluacion_test_gc2_xgboost\\predicciones_test_gc2_xgboost.csv",
  "shock_by_seed": "C:\\Machine-learming\\Machine-Learning-Multimodal--Agro-NLP-Clima-\\v2_reentrenamiento\\resultados_v2_final\\evaluacion_test_gc2_xgboost\\metricas_shock_por_seed_gc2_xgboost.csv",
  "shock_summary": "C:\\Machine-learming\\Machine-Learning-Multimodal--Agro-NLP-Clima-\\v2_reentrenamiento\\resultados_v2_final\\evaluacion_test_gc2_xgboost\\resumen_shock_multiseed_gc2_xgboost.csv",
  "summary_multiseed": "C:\\Machine-learming\\Machine-Learning-Multimodal--Agro-NLP-Clima-\\v2_reentrenamiento\\resultados_v2_final\\evaluacion_test_gc2_xgboost\\resumen_test_multiseed_gc2_xgboost.csv"
}
```

## Controles

| Control | Estado | Detalle |
|---|---|---|
| 20 modelos oficiales encontrados | OK | 20 |
| seeds 0..9 completas para Sutil | OK | [0, 1, 2, 3, 4, 5, 6, 7, 8, 9] |
| seeds 0..9 completas para Dulce | OK | [0, 1, 2, 3, 4, 5, 6, 7, 8, 9] |
| solo pares cultivar/seed oficiales | OK | [('dulce', 0), ('dulce', 1), ('dulce', 2), ('dulce', 3), ('dulce', 4), ('dulce', 5), ('dulce', 6), ('dulce', 7), ('dulce', 8), ('dulce', 9), ('sutil', 0), ('sutil', 1), ('sutil', 2), ('sutil', 3), ('sutil', 4), ('sutil', 5), ('sutil', 6), ('sutil', 7), ('sutil', 8), ('sutil', 9)] |
| sutil 37 features exactas | OK | 37 |
| sutil variables prohibidas ausentes | OK | [] |
| sutil TEST n=12 | OK | 12 |
| sutil fechas Jan-Dec 2025 exactas | OK | ['2025-01', '2025-02', '2025-03', '2025-04', '2025-05', '2025-06', '2025-07', '2025-08', '2025-09', '2025-10', '2025-11', '2025-12'] |
| sutil primer forecast = Dec2024 -> Jan2025 | OK | 2024-12 -> 2025-01 |
| sutil scaler TRAIN-only congelado | OK | scaler_sutil_v2c.joblib |
| sutil no refit scaler | OK | scaler cargado desde joblib |
| sutil y_true inverse transform correcto | OK | comparado contra master_dataset_v2_features |
| sutil n_shock=3 | OK | 3 |
| sutil n_nonshock=9 | OK | 9 |
| sutil mascara shock congelada | OK | [np.str_('2025-01'), np.str_('2025-07'), np.str_('2025-11')] |
| sutil seed_00 config congelada | OK | {'best_seed_selection': False, 'booster': 'gbtree', 'colsample_bytree': 0.8, 'cv_performed': False, 'early_stopping': False, 'eval_metric': 'mae', 'gamma': 0.0, 'hpo_performed': False, 'learning_rate': 0.05, 'max_depth': 2, 'min_child_weight': 1, 'n_estimators': 200, 'n_jobs': 1, 'objective': 'reg:squarederror', 'reg_alpha': 0.0, 'reg_lambda': 1.0, 'subsample': 0.8, 'tree_method': 'hist'} |
| sutil seed_00 random_state = seed | OK | 0 |
| sutil seed_00 features = 37 | OK | 37 |
| sutil seed_00 objective | OK | reg:squarederror |
| sutil seed_00 eval_metric | OK | mae |
| sutil seed_00 booster | OK | gbtree |
| sutil seed_00 n_estimators | OK | 200 |
| sutil seed_00 max_depth | OK | 2 |
| sutil seed_00 min_child_weight | OK | 1 |
| sutil seed_00 learning_rate | OK | 0.05 |
| sutil seed_00 subsample | OK | 0.8 |
| sutil seed_00 colsample_bytree | OK | 0.8 |
| sutil seed_00 reg_alpha | OK | 0.0 |
| sutil seed_00 reg_lambda | OK | 1.0 |
| sutil seed_00 gamma | OK | 0.0 |
| sutil seed_00 tree_method | OK | hist |
| sutil seed_00 n_jobs | OK | 1 |
| sutil seed_00 metadata no TEST pre-run | OK | False |
| sutil seed_00 no HPO/CV/early/best seed | OK | false/false/false/false |
| sutil seed_00 modelo no v1/piloto | OK | XGBRegressor |
| sutil seed_00 modelo random_state | OK | 0 |
| sutil seed_01 config congelada | OK | {'best_seed_selection': False, 'booster': 'gbtree', 'colsample_bytree': 0.8, 'cv_performed': False, 'early_stopping': False, 'eval_metric': 'mae', 'gamma': 0.0, 'hpo_performed': False, 'learning_rate': 0.05, 'max_depth': 2, 'min_child_weight': 1, 'n_estimators': 200, 'n_jobs': 1, 'objective': 'reg:squarederror', 'reg_alpha': 0.0, 'reg_lambda': 1.0, 'subsample': 0.8, 'tree_method': 'hist'} |
| sutil seed_01 random_state = seed | OK | 1 |
| sutil seed_01 features = 37 | OK | 37 |
| sutil seed_01 objective | OK | reg:squarederror |
| sutil seed_01 eval_metric | OK | mae |
| sutil seed_01 booster | OK | gbtree |
| sutil seed_01 n_estimators | OK | 200 |
| sutil seed_01 max_depth | OK | 2 |
| sutil seed_01 min_child_weight | OK | 1 |
| sutil seed_01 learning_rate | OK | 0.05 |
| sutil seed_01 subsample | OK | 0.8 |
| sutil seed_01 colsample_bytree | OK | 0.8 |
| sutil seed_01 reg_alpha | OK | 0.0 |
| sutil seed_01 reg_lambda | OK | 1.0 |
| sutil seed_01 gamma | OK | 0.0 |
| sutil seed_01 tree_method | OK | hist |
| sutil seed_01 n_jobs | OK | 1 |
| sutil seed_01 metadata no TEST pre-run | OK | False |
| sutil seed_01 no HPO/CV/early/best seed | OK | false/false/false/false |
| sutil seed_01 modelo no v1/piloto | OK | XGBRegressor |
| sutil seed_01 modelo random_state | OK | 1 |
| sutil seed_02 config congelada | OK | {'best_seed_selection': False, 'booster': 'gbtree', 'colsample_bytree': 0.8, 'cv_performed': False, 'early_stopping': False, 'eval_metric': 'mae', 'gamma': 0.0, 'hpo_performed': False, 'learning_rate': 0.05, 'max_depth': 2, 'min_child_weight': 1, 'n_estimators': 200, 'n_jobs': 1, 'objective': 'reg:squarederror', 'reg_alpha': 0.0, 'reg_lambda': 1.0, 'subsample': 0.8, 'tree_method': 'hist'} |
| sutil seed_02 random_state = seed | OK | 2 |
| sutil seed_02 features = 37 | OK | 37 |
| sutil seed_02 objective | OK | reg:squarederror |
| sutil seed_02 eval_metric | OK | mae |
| sutil seed_02 booster | OK | gbtree |
| sutil seed_02 n_estimators | OK | 200 |
| sutil seed_02 max_depth | OK | 2 |
| sutil seed_02 min_child_weight | OK | 1 |
| sutil seed_02 learning_rate | OK | 0.05 |
| sutil seed_02 subsample | OK | 0.8 |
| sutil seed_02 colsample_bytree | OK | 0.8 |
| sutil seed_02 reg_alpha | OK | 0.0 |
| sutil seed_02 reg_lambda | OK | 1.0 |
| sutil seed_02 gamma | OK | 0.0 |
| sutil seed_02 tree_method | OK | hist |
| sutil seed_02 n_jobs | OK | 1 |
| sutil seed_02 metadata no TEST pre-run | OK | False |
| sutil seed_02 no HPO/CV/early/best seed | OK | false/false/false/false |
| sutil seed_02 modelo no v1/piloto | OK | XGBRegressor |
| sutil seed_02 modelo random_state | OK | 2 |
| sutil seed_03 config congelada | OK | {'best_seed_selection': False, 'booster': 'gbtree', 'colsample_bytree': 0.8, 'cv_performed': False, 'early_stopping': False, 'eval_metric': 'mae', 'gamma': 0.0, 'hpo_performed': False, 'learning_rate': 0.05, 'max_depth': 2, 'min_child_weight': 1, 'n_estimators': 200, 'n_jobs': 1, 'objective': 'reg:squarederror', 'reg_alpha': 0.0, 'reg_lambda': 1.0, 'subsample': 0.8, 'tree_method': 'hist'} |
| sutil seed_03 random_state = seed | OK | 3 |
| sutil seed_03 features = 37 | OK | 37 |
| sutil seed_03 objective | OK | reg:squarederror |
| sutil seed_03 eval_metric | OK | mae |
| sutil seed_03 booster | OK | gbtree |
| sutil seed_03 n_estimators | OK | 200 |
| sutil seed_03 max_depth | OK | 2 |
| sutil seed_03 min_child_weight | OK | 1 |
| sutil seed_03 learning_rate | OK | 0.05 |
| sutil seed_03 subsample | OK | 0.8 |
| sutil seed_03 colsample_bytree | OK | 0.8 |
| sutil seed_03 reg_alpha | OK | 0.0 |
| sutil seed_03 reg_lambda | OK | 1.0 |
| sutil seed_03 gamma | OK | 0.0 |
| sutil seed_03 tree_method | OK | hist |
| sutil seed_03 n_jobs | OK | 1 |
| sutil seed_03 metadata no TEST pre-run | OK | False |
| sutil seed_03 no HPO/CV/early/best seed | OK | false/false/false/false |
| sutil seed_03 modelo no v1/piloto | OK | XGBRegressor |
| sutil seed_03 modelo random_state | OK | 3 |
| sutil seed_04 config congelada | OK | {'best_seed_selection': False, 'booster': 'gbtree', 'colsample_bytree': 0.8, 'cv_performed': False, 'early_stopping': False, 'eval_metric': 'mae', 'gamma': 0.0, 'hpo_performed': False, 'learning_rate': 0.05, 'max_depth': 2, 'min_child_weight': 1, 'n_estimators': 200, 'n_jobs': 1, 'objective': 'reg:squarederror', 'reg_alpha': 0.0, 'reg_lambda': 1.0, 'subsample': 0.8, 'tree_method': 'hist'} |
| sutil seed_04 random_state = seed | OK | 4 |
| sutil seed_04 features = 37 | OK | 37 |
| sutil seed_04 objective | OK | reg:squarederror |
| sutil seed_04 eval_metric | OK | mae |
| sutil seed_04 booster | OK | gbtree |
| sutil seed_04 n_estimators | OK | 200 |
| sutil seed_04 max_depth | OK | 2 |
| sutil seed_04 min_child_weight | OK | 1 |
| sutil seed_04 learning_rate | OK | 0.05 |
| sutil seed_04 subsample | OK | 0.8 |
| sutil seed_04 colsample_bytree | OK | 0.8 |
| sutil seed_04 reg_alpha | OK | 0.0 |
| sutil seed_04 reg_lambda | OK | 1.0 |
| sutil seed_04 gamma | OK | 0.0 |
| sutil seed_04 tree_method | OK | hist |
| sutil seed_04 n_jobs | OK | 1 |
| sutil seed_04 metadata no TEST pre-run | OK | False |
| sutil seed_04 no HPO/CV/early/best seed | OK | false/false/false/false |
| sutil seed_04 modelo no v1/piloto | OK | XGBRegressor |
| sutil seed_04 modelo random_state | OK | 4 |
| sutil seed_05 config congelada | OK | {'best_seed_selection': False, 'booster': 'gbtree', 'colsample_bytree': 0.8, 'cv_performed': False, 'early_stopping': False, 'eval_metric': 'mae', 'gamma': 0.0, 'hpo_performed': False, 'learning_rate': 0.05, 'max_depth': 2, 'min_child_weight': 1, 'n_estimators': 200, 'n_jobs': 1, 'objective': 'reg:squarederror', 'reg_alpha': 0.0, 'reg_lambda': 1.0, 'subsample': 0.8, 'tree_method': 'hist'} |
| sutil seed_05 random_state = seed | OK | 5 |
| sutil seed_05 features = 37 | OK | 37 |
| sutil seed_05 objective | OK | reg:squarederror |
| sutil seed_05 eval_metric | OK | mae |
| sutil seed_05 booster | OK | gbtree |
| sutil seed_05 n_estimators | OK | 200 |
| sutil seed_05 max_depth | OK | 2 |
| sutil seed_05 min_child_weight | OK | 1 |
| sutil seed_05 learning_rate | OK | 0.05 |
| sutil seed_05 subsample | OK | 0.8 |
| sutil seed_05 colsample_bytree | OK | 0.8 |
| sutil seed_05 reg_alpha | OK | 0.0 |
| sutil seed_05 reg_lambda | OK | 1.0 |
| sutil seed_05 gamma | OK | 0.0 |
| sutil seed_05 tree_method | OK | hist |
| sutil seed_05 n_jobs | OK | 1 |
| sutil seed_05 metadata no TEST pre-run | OK | False |
| sutil seed_05 no HPO/CV/early/best seed | OK | false/false/false/false |
| sutil seed_05 modelo no v1/piloto | OK | XGBRegressor |
| sutil seed_05 modelo random_state | OK | 5 |
| sutil seed_06 config congelada | OK | {'best_seed_selection': False, 'booster': 'gbtree', 'colsample_bytree': 0.8, 'cv_performed': False, 'early_stopping': False, 'eval_metric': 'mae', 'gamma': 0.0, 'hpo_performed': False, 'learning_rate': 0.05, 'max_depth': 2, 'min_child_weight': 1, 'n_estimators': 200, 'n_jobs': 1, 'objective': 'reg:squarederror', 'reg_alpha': 0.0, 'reg_lambda': 1.0, 'subsample': 0.8, 'tree_method': 'hist'} |
| sutil seed_06 random_state = seed | OK | 6 |
| sutil seed_06 features = 37 | OK | 37 |
| sutil seed_06 objective | OK | reg:squarederror |
| sutil seed_06 eval_metric | OK | mae |
| sutil seed_06 booster | OK | gbtree |
| sutil seed_06 n_estimators | OK | 200 |
| sutil seed_06 max_depth | OK | 2 |
| sutil seed_06 min_child_weight | OK | 1 |
| sutil seed_06 learning_rate | OK | 0.05 |
| sutil seed_06 subsample | OK | 0.8 |
| sutil seed_06 colsample_bytree | OK | 0.8 |
| sutil seed_06 reg_alpha | OK | 0.0 |
| sutil seed_06 reg_lambda | OK | 1.0 |
| sutil seed_06 gamma | OK | 0.0 |
| sutil seed_06 tree_method | OK | hist |
| sutil seed_06 n_jobs | OK | 1 |
| sutil seed_06 metadata no TEST pre-run | OK | False |
| sutil seed_06 no HPO/CV/early/best seed | OK | false/false/false/false |
| sutil seed_06 modelo no v1/piloto | OK | XGBRegressor |
| sutil seed_06 modelo random_state | OK | 6 |
| sutil seed_07 config congelada | OK | {'best_seed_selection': False, 'booster': 'gbtree', 'colsample_bytree': 0.8, 'cv_performed': False, 'early_stopping': False, 'eval_metric': 'mae', 'gamma': 0.0, 'hpo_performed': False, 'learning_rate': 0.05, 'max_depth': 2, 'min_child_weight': 1, 'n_estimators': 200, 'n_jobs': 1, 'objective': 'reg:squarederror', 'reg_alpha': 0.0, 'reg_lambda': 1.0, 'subsample': 0.8, 'tree_method': 'hist'} |
| sutil seed_07 random_state = seed | OK | 7 |
| sutil seed_07 features = 37 | OK | 37 |
| sutil seed_07 objective | OK | reg:squarederror |
| sutil seed_07 eval_metric | OK | mae |
| sutil seed_07 booster | OK | gbtree |
| sutil seed_07 n_estimators | OK | 200 |
| sutil seed_07 max_depth | OK | 2 |
| sutil seed_07 min_child_weight | OK | 1 |
| sutil seed_07 learning_rate | OK | 0.05 |
| sutil seed_07 subsample | OK | 0.8 |
| sutil seed_07 colsample_bytree | OK | 0.8 |
| sutil seed_07 reg_alpha | OK | 0.0 |
| sutil seed_07 reg_lambda | OK | 1.0 |
| sutil seed_07 gamma | OK | 0.0 |
| sutil seed_07 tree_method | OK | hist |
| sutil seed_07 n_jobs | OK | 1 |
| sutil seed_07 metadata no TEST pre-run | OK | False |
| sutil seed_07 no HPO/CV/early/best seed | OK | false/false/false/false |
| sutil seed_07 modelo no v1/piloto | OK | XGBRegressor |
| sutil seed_07 modelo random_state | OK | 7 |
| sutil seed_08 config congelada | OK | {'best_seed_selection': False, 'booster': 'gbtree', 'colsample_bytree': 0.8, 'cv_performed': False, 'early_stopping': False, 'eval_metric': 'mae', 'gamma': 0.0, 'hpo_performed': False, 'learning_rate': 0.05, 'max_depth': 2, 'min_child_weight': 1, 'n_estimators': 200, 'n_jobs': 1, 'objective': 'reg:squarederror', 'reg_alpha': 0.0, 'reg_lambda': 1.0, 'subsample': 0.8, 'tree_method': 'hist'} |
| sutil seed_08 random_state = seed | OK | 8 |
| sutil seed_08 features = 37 | OK | 37 |
| sutil seed_08 objective | OK | reg:squarederror |
| sutil seed_08 eval_metric | OK | mae |
| sutil seed_08 booster | OK | gbtree |
| sutil seed_08 n_estimators | OK | 200 |
| sutil seed_08 max_depth | OK | 2 |
| sutil seed_08 min_child_weight | OK | 1 |
| sutil seed_08 learning_rate | OK | 0.05 |
| sutil seed_08 subsample | OK | 0.8 |
| sutil seed_08 colsample_bytree | OK | 0.8 |
| sutil seed_08 reg_alpha | OK | 0.0 |
| sutil seed_08 reg_lambda | OK | 1.0 |
| sutil seed_08 gamma | OK | 0.0 |
| sutil seed_08 tree_method | OK | hist |
| sutil seed_08 n_jobs | OK | 1 |
| sutil seed_08 metadata no TEST pre-run | OK | False |
| sutil seed_08 no HPO/CV/early/best seed | OK | false/false/false/false |
| sutil seed_08 modelo no v1/piloto | OK | XGBRegressor |
| sutil seed_08 modelo random_state | OK | 8 |
| sutil seed_09 config congelada | OK | {'best_seed_selection': False, 'booster': 'gbtree', 'colsample_bytree': 0.8, 'cv_performed': False, 'early_stopping': False, 'eval_metric': 'mae', 'gamma': 0.0, 'hpo_performed': False, 'learning_rate': 0.05, 'max_depth': 2, 'min_child_weight': 1, 'n_estimators': 200, 'n_jobs': 1, 'objective': 'reg:squarederror', 'reg_alpha': 0.0, 'reg_lambda': 1.0, 'subsample': 0.8, 'tree_method': 'hist'} |
| sutil seed_09 random_state = seed | OK | 9 |
| sutil seed_09 features = 37 | OK | 37 |
| sutil seed_09 objective | OK | reg:squarederror |
| sutil seed_09 eval_metric | OK | mae |
| sutil seed_09 booster | OK | gbtree |
| sutil seed_09 n_estimators | OK | 200 |
| sutil seed_09 max_depth | OK | 2 |
| sutil seed_09 min_child_weight | OK | 1 |
| sutil seed_09 learning_rate | OK | 0.05 |
| sutil seed_09 subsample | OK | 0.8 |
| sutil seed_09 colsample_bytree | OK | 0.8 |
| sutil seed_09 reg_alpha | OK | 0.0 |
| sutil seed_09 reg_lambda | OK | 1.0 |
| sutil seed_09 gamma | OK | 0.0 |
| sutil seed_09 tree_method | OK | hist |
| sutil seed_09 n_jobs | OK | 1 |
| sutil seed_09 metadata no TEST pre-run | OK | False |
| sutil seed_09 no HPO/CV/early/best seed | OK | false/false/false/false |
| sutil seed_09 modelo no v1/piloto | OK | XGBRegressor |
| sutil seed_09 modelo random_state | OK | 9 |
| dulce 37 features exactas | OK | 37 |
| dulce variables prohibidas ausentes | OK | [] |
| dulce TEST n=12 | OK | 12 |
| dulce fechas Jan-Dec 2025 exactas | OK | ['2025-01', '2025-02', '2025-03', '2025-04', '2025-05', '2025-06', '2025-07', '2025-08', '2025-09', '2025-10', '2025-11', '2025-12'] |
| dulce primer forecast = Dec2024 -> Jan2025 | OK | 2024-12 -> 2025-01 |
| dulce scaler TRAIN-only congelado | OK | scaler_dulce_v2c.joblib |
| dulce no refit scaler | OK | scaler cargado desde joblib |
| dulce y_true inverse transform correcto | OK | comparado contra master_dataset_v2_features |
| dulce n_shock=3 | OK | 3 |
| dulce n_nonshock=9 | OK | 9 |
| dulce mascara shock congelada | OK | [np.str_('2025-01'), np.str_('2025-02'), np.str_('2025-03')] |
| dulce seed_00 config congelada | OK | {'best_seed_selection': False, 'booster': 'gbtree', 'colsample_bytree': 0.8, 'cv_performed': False, 'early_stopping': False, 'eval_metric': 'mae', 'gamma': 0.0, 'hpo_performed': False, 'learning_rate': 0.05, 'max_depth': 2, 'min_child_weight': 1, 'n_estimators': 200, 'n_jobs': 1, 'objective': 'reg:squarederror', 'reg_alpha': 0.0, 'reg_lambda': 1.0, 'subsample': 0.8, 'tree_method': 'hist'} |
| dulce seed_00 random_state = seed | OK | 0 |
| dulce seed_00 features = 37 | OK | 37 |
| dulce seed_00 objective | OK | reg:squarederror |
| dulce seed_00 eval_metric | OK | mae |
| dulce seed_00 booster | OK | gbtree |
| dulce seed_00 n_estimators | OK | 200 |
| dulce seed_00 max_depth | OK | 2 |
| dulce seed_00 min_child_weight | OK | 1 |
| dulce seed_00 learning_rate | OK | 0.05 |
| dulce seed_00 subsample | OK | 0.8 |
| dulce seed_00 colsample_bytree | OK | 0.8 |
| dulce seed_00 reg_alpha | OK | 0.0 |
| dulce seed_00 reg_lambda | OK | 1.0 |
| dulce seed_00 gamma | OK | 0.0 |
| dulce seed_00 tree_method | OK | hist |
| dulce seed_00 n_jobs | OK | 1 |
| dulce seed_00 metadata no TEST pre-run | OK | False |
| dulce seed_00 no HPO/CV/early/best seed | OK | false/false/false/false |
| dulce seed_00 modelo no v1/piloto | OK | XGBRegressor |
| dulce seed_00 modelo random_state | OK | 0 |
| dulce seed_01 config congelada | OK | {'best_seed_selection': False, 'booster': 'gbtree', 'colsample_bytree': 0.8, 'cv_performed': False, 'early_stopping': False, 'eval_metric': 'mae', 'gamma': 0.0, 'hpo_performed': False, 'learning_rate': 0.05, 'max_depth': 2, 'min_child_weight': 1, 'n_estimators': 200, 'n_jobs': 1, 'objective': 'reg:squarederror', 'reg_alpha': 0.0, 'reg_lambda': 1.0, 'subsample': 0.8, 'tree_method': 'hist'} |
| dulce seed_01 random_state = seed | OK | 1 |
| dulce seed_01 features = 37 | OK | 37 |
| dulce seed_01 objective | OK | reg:squarederror |
| dulce seed_01 eval_metric | OK | mae |
| dulce seed_01 booster | OK | gbtree |
| dulce seed_01 n_estimators | OK | 200 |
| dulce seed_01 max_depth | OK | 2 |
| dulce seed_01 min_child_weight | OK | 1 |
| dulce seed_01 learning_rate | OK | 0.05 |
| dulce seed_01 subsample | OK | 0.8 |
| dulce seed_01 colsample_bytree | OK | 0.8 |
| dulce seed_01 reg_alpha | OK | 0.0 |
| dulce seed_01 reg_lambda | OK | 1.0 |
| dulce seed_01 gamma | OK | 0.0 |
| dulce seed_01 tree_method | OK | hist |
| dulce seed_01 n_jobs | OK | 1 |
| dulce seed_01 metadata no TEST pre-run | OK | False |
| dulce seed_01 no HPO/CV/early/best seed | OK | false/false/false/false |
| dulce seed_01 modelo no v1/piloto | OK | XGBRegressor |
| dulce seed_01 modelo random_state | OK | 1 |
| dulce seed_02 config congelada | OK | {'best_seed_selection': False, 'booster': 'gbtree', 'colsample_bytree': 0.8, 'cv_performed': False, 'early_stopping': False, 'eval_metric': 'mae', 'gamma': 0.0, 'hpo_performed': False, 'learning_rate': 0.05, 'max_depth': 2, 'min_child_weight': 1, 'n_estimators': 200, 'n_jobs': 1, 'objective': 'reg:squarederror', 'reg_alpha': 0.0, 'reg_lambda': 1.0, 'subsample': 0.8, 'tree_method': 'hist'} |
| dulce seed_02 random_state = seed | OK | 2 |
| dulce seed_02 features = 37 | OK | 37 |
| dulce seed_02 objective | OK | reg:squarederror |
| dulce seed_02 eval_metric | OK | mae |
| dulce seed_02 booster | OK | gbtree |
| dulce seed_02 n_estimators | OK | 200 |
| dulce seed_02 max_depth | OK | 2 |
| dulce seed_02 min_child_weight | OK | 1 |
| dulce seed_02 learning_rate | OK | 0.05 |
| dulce seed_02 subsample | OK | 0.8 |
| dulce seed_02 colsample_bytree | OK | 0.8 |
| dulce seed_02 reg_alpha | OK | 0.0 |
| dulce seed_02 reg_lambda | OK | 1.0 |
| dulce seed_02 gamma | OK | 0.0 |
| dulce seed_02 tree_method | OK | hist |
| dulce seed_02 n_jobs | OK | 1 |
| dulce seed_02 metadata no TEST pre-run | OK | False |
| dulce seed_02 no HPO/CV/early/best seed | OK | false/false/false/false |
| dulce seed_02 modelo no v1/piloto | OK | XGBRegressor |
| dulce seed_02 modelo random_state | OK | 2 |
| dulce seed_03 config congelada | OK | {'best_seed_selection': False, 'booster': 'gbtree', 'colsample_bytree': 0.8, 'cv_performed': False, 'early_stopping': False, 'eval_metric': 'mae', 'gamma': 0.0, 'hpo_performed': False, 'learning_rate': 0.05, 'max_depth': 2, 'min_child_weight': 1, 'n_estimators': 200, 'n_jobs': 1, 'objective': 'reg:squarederror', 'reg_alpha': 0.0, 'reg_lambda': 1.0, 'subsample': 0.8, 'tree_method': 'hist'} |
| dulce seed_03 random_state = seed | OK | 3 |
| dulce seed_03 features = 37 | OK | 37 |
| dulce seed_03 objective | OK | reg:squarederror |
| dulce seed_03 eval_metric | OK | mae |
| dulce seed_03 booster | OK | gbtree |
| dulce seed_03 n_estimators | OK | 200 |
| dulce seed_03 max_depth | OK | 2 |
| dulce seed_03 min_child_weight | OK | 1 |
| dulce seed_03 learning_rate | OK | 0.05 |
| dulce seed_03 subsample | OK | 0.8 |
| dulce seed_03 colsample_bytree | OK | 0.8 |
| dulce seed_03 reg_alpha | OK | 0.0 |
| dulce seed_03 reg_lambda | OK | 1.0 |
| dulce seed_03 gamma | OK | 0.0 |
| dulce seed_03 tree_method | OK | hist |
| dulce seed_03 n_jobs | OK | 1 |
| dulce seed_03 metadata no TEST pre-run | OK | False |
| dulce seed_03 no HPO/CV/early/best seed | OK | false/false/false/false |
| dulce seed_03 modelo no v1/piloto | OK | XGBRegressor |
| dulce seed_03 modelo random_state | OK | 3 |
| dulce seed_04 config congelada | OK | {'best_seed_selection': False, 'booster': 'gbtree', 'colsample_bytree': 0.8, 'cv_performed': False, 'early_stopping': False, 'eval_metric': 'mae', 'gamma': 0.0, 'hpo_performed': False, 'learning_rate': 0.05, 'max_depth': 2, 'min_child_weight': 1, 'n_estimators': 200, 'n_jobs': 1, 'objective': 'reg:squarederror', 'reg_alpha': 0.0, 'reg_lambda': 1.0, 'subsample': 0.8, 'tree_method': 'hist'} |
| dulce seed_04 random_state = seed | OK | 4 |
| dulce seed_04 features = 37 | OK | 37 |
| dulce seed_04 objective | OK | reg:squarederror |
| dulce seed_04 eval_metric | OK | mae |
| dulce seed_04 booster | OK | gbtree |
| dulce seed_04 n_estimators | OK | 200 |
| dulce seed_04 max_depth | OK | 2 |
| dulce seed_04 min_child_weight | OK | 1 |
| dulce seed_04 learning_rate | OK | 0.05 |
| dulce seed_04 subsample | OK | 0.8 |
| dulce seed_04 colsample_bytree | OK | 0.8 |
| dulce seed_04 reg_alpha | OK | 0.0 |
| dulce seed_04 reg_lambda | OK | 1.0 |
| dulce seed_04 gamma | OK | 0.0 |
| dulce seed_04 tree_method | OK | hist |
| dulce seed_04 n_jobs | OK | 1 |
| dulce seed_04 metadata no TEST pre-run | OK | False |
| dulce seed_04 no HPO/CV/early/best seed | OK | false/false/false/false |
| dulce seed_04 modelo no v1/piloto | OK | XGBRegressor |
| dulce seed_04 modelo random_state | OK | 4 |
| dulce seed_05 config congelada | OK | {'best_seed_selection': False, 'booster': 'gbtree', 'colsample_bytree': 0.8, 'cv_performed': False, 'early_stopping': False, 'eval_metric': 'mae', 'gamma': 0.0, 'hpo_performed': False, 'learning_rate': 0.05, 'max_depth': 2, 'min_child_weight': 1, 'n_estimators': 200, 'n_jobs': 1, 'objective': 'reg:squarederror', 'reg_alpha': 0.0, 'reg_lambda': 1.0, 'subsample': 0.8, 'tree_method': 'hist'} |
| dulce seed_05 random_state = seed | OK | 5 |
| dulce seed_05 features = 37 | OK | 37 |
| dulce seed_05 objective | OK | reg:squarederror |
| dulce seed_05 eval_metric | OK | mae |
| dulce seed_05 booster | OK | gbtree |
| dulce seed_05 n_estimators | OK | 200 |
| dulce seed_05 max_depth | OK | 2 |
| dulce seed_05 min_child_weight | OK | 1 |
| dulce seed_05 learning_rate | OK | 0.05 |
| dulce seed_05 subsample | OK | 0.8 |
| dulce seed_05 colsample_bytree | OK | 0.8 |
| dulce seed_05 reg_alpha | OK | 0.0 |
| dulce seed_05 reg_lambda | OK | 1.0 |
| dulce seed_05 gamma | OK | 0.0 |
| dulce seed_05 tree_method | OK | hist |
| dulce seed_05 n_jobs | OK | 1 |
| dulce seed_05 metadata no TEST pre-run | OK | False |
| dulce seed_05 no HPO/CV/early/best seed | OK | false/false/false/false |
| dulce seed_05 modelo no v1/piloto | OK | XGBRegressor |
| dulce seed_05 modelo random_state | OK | 5 |
| dulce seed_06 config congelada | OK | {'best_seed_selection': False, 'booster': 'gbtree', 'colsample_bytree': 0.8, 'cv_performed': False, 'early_stopping': False, 'eval_metric': 'mae', 'gamma': 0.0, 'hpo_performed': False, 'learning_rate': 0.05, 'max_depth': 2, 'min_child_weight': 1, 'n_estimators': 200, 'n_jobs': 1, 'objective': 'reg:squarederror', 'reg_alpha': 0.0, 'reg_lambda': 1.0, 'subsample': 0.8, 'tree_method': 'hist'} |
| dulce seed_06 random_state = seed | OK | 6 |
| dulce seed_06 features = 37 | OK | 37 |
| dulce seed_06 objective | OK | reg:squarederror |
| dulce seed_06 eval_metric | OK | mae |
| dulce seed_06 booster | OK | gbtree |
| dulce seed_06 n_estimators | OK | 200 |
| dulce seed_06 max_depth | OK | 2 |
| dulce seed_06 min_child_weight | OK | 1 |
| dulce seed_06 learning_rate | OK | 0.05 |
| dulce seed_06 subsample | OK | 0.8 |
| dulce seed_06 colsample_bytree | OK | 0.8 |
| dulce seed_06 reg_alpha | OK | 0.0 |
| dulce seed_06 reg_lambda | OK | 1.0 |
| dulce seed_06 gamma | OK | 0.0 |
| dulce seed_06 tree_method | OK | hist |
| dulce seed_06 n_jobs | OK | 1 |
| dulce seed_06 metadata no TEST pre-run | OK | False |
| dulce seed_06 no HPO/CV/early/best seed | OK | false/false/false/false |
| dulce seed_06 modelo no v1/piloto | OK | XGBRegressor |
| dulce seed_06 modelo random_state | OK | 6 |
| dulce seed_07 config congelada | OK | {'best_seed_selection': False, 'booster': 'gbtree', 'colsample_bytree': 0.8, 'cv_performed': False, 'early_stopping': False, 'eval_metric': 'mae', 'gamma': 0.0, 'hpo_performed': False, 'learning_rate': 0.05, 'max_depth': 2, 'min_child_weight': 1, 'n_estimators': 200, 'n_jobs': 1, 'objective': 'reg:squarederror', 'reg_alpha': 0.0, 'reg_lambda': 1.0, 'subsample': 0.8, 'tree_method': 'hist'} |
| dulce seed_07 random_state = seed | OK | 7 |
| dulce seed_07 features = 37 | OK | 37 |
| dulce seed_07 objective | OK | reg:squarederror |
| dulce seed_07 eval_metric | OK | mae |
| dulce seed_07 booster | OK | gbtree |
| dulce seed_07 n_estimators | OK | 200 |
| dulce seed_07 max_depth | OK | 2 |
| dulce seed_07 min_child_weight | OK | 1 |
| dulce seed_07 learning_rate | OK | 0.05 |
| dulce seed_07 subsample | OK | 0.8 |
| dulce seed_07 colsample_bytree | OK | 0.8 |
| dulce seed_07 reg_alpha | OK | 0.0 |
| dulce seed_07 reg_lambda | OK | 1.0 |
| dulce seed_07 gamma | OK | 0.0 |
| dulce seed_07 tree_method | OK | hist |
| dulce seed_07 n_jobs | OK | 1 |
| dulce seed_07 metadata no TEST pre-run | OK | False |
| dulce seed_07 no HPO/CV/early/best seed | OK | false/false/false/false |
| dulce seed_07 modelo no v1/piloto | OK | XGBRegressor |
| dulce seed_07 modelo random_state | OK | 7 |
| dulce seed_08 config congelada | OK | {'best_seed_selection': False, 'booster': 'gbtree', 'colsample_bytree': 0.8, 'cv_performed': False, 'early_stopping': False, 'eval_metric': 'mae', 'gamma': 0.0, 'hpo_performed': False, 'learning_rate': 0.05, 'max_depth': 2, 'min_child_weight': 1, 'n_estimators': 200, 'n_jobs': 1, 'objective': 'reg:squarederror', 'reg_alpha': 0.0, 'reg_lambda': 1.0, 'subsample': 0.8, 'tree_method': 'hist'} |
| dulce seed_08 random_state = seed | OK | 8 |
| dulce seed_08 features = 37 | OK | 37 |
| dulce seed_08 objective | OK | reg:squarederror |
| dulce seed_08 eval_metric | OK | mae |
| dulce seed_08 booster | OK | gbtree |
| dulce seed_08 n_estimators | OK | 200 |
| dulce seed_08 max_depth | OK | 2 |
| dulce seed_08 min_child_weight | OK | 1 |
| dulce seed_08 learning_rate | OK | 0.05 |
| dulce seed_08 subsample | OK | 0.8 |
| dulce seed_08 colsample_bytree | OK | 0.8 |
| dulce seed_08 reg_alpha | OK | 0.0 |
| dulce seed_08 reg_lambda | OK | 1.0 |
| dulce seed_08 gamma | OK | 0.0 |
| dulce seed_08 tree_method | OK | hist |
| dulce seed_08 n_jobs | OK | 1 |
| dulce seed_08 metadata no TEST pre-run | OK | False |
| dulce seed_08 no HPO/CV/early/best seed | OK | false/false/false/false |
| dulce seed_08 modelo no v1/piloto | OK | XGBRegressor |
| dulce seed_08 modelo random_state | OK | 8 |
| dulce seed_09 config congelada | OK | {'best_seed_selection': False, 'booster': 'gbtree', 'colsample_bytree': 0.8, 'cv_performed': False, 'early_stopping': False, 'eval_metric': 'mae', 'gamma': 0.0, 'hpo_performed': False, 'learning_rate': 0.05, 'max_depth': 2, 'min_child_weight': 1, 'n_estimators': 200, 'n_jobs': 1, 'objective': 'reg:squarederror', 'reg_alpha': 0.0, 'reg_lambda': 1.0, 'subsample': 0.8, 'tree_method': 'hist'} |
| dulce seed_09 random_state = seed | OK | 9 |
| dulce seed_09 features = 37 | OK | 37 |
| dulce seed_09 objective | OK | reg:squarederror |
| dulce seed_09 eval_metric | OK | mae |
| dulce seed_09 booster | OK | gbtree |
| dulce seed_09 n_estimators | OK | 200 |
| dulce seed_09 max_depth | OK | 2 |
| dulce seed_09 min_child_weight | OK | 1 |
| dulce seed_09 learning_rate | OK | 0.05 |
| dulce seed_09 subsample | OK | 0.8 |
| dulce seed_09 colsample_bytree | OK | 0.8 |
| dulce seed_09 reg_alpha | OK | 0.0 |
| dulce seed_09 reg_lambda | OK | 1.0 |
| dulce seed_09 gamma | OK | 0.0 |
| dulce seed_09 tree_method | OK | hist |
| dulce seed_09 n_jobs | OK | 1 |
| dulce seed_09 metadata no TEST pre-run | OK | False |
| dulce seed_09 no HPO/CV/early/best seed | OK | false/false/false/false |
| dulce seed_09 modelo no v1/piloto | OK | XGBRegressor |
| dulce seed_09 modelo random_state | OK | 9 |
| 240 predicciones totales | OK | 240 |
| 12 predicciones por modelo/seed | OK | group sizes all 12 |
| no modelo v1/piloto | OK | solo official_gc2_xgboost |
| no retraining | OK | solo joblib.load + predict |
| no HPO | OK | config hpo_performed=false verificado |
| no CV | OK | config cv_performed=false verificado |
| no early stopping | OK | config early_stopping=false verificado |
| no best seed | OK | todas las seeds reportadas |
| no modificacion de modelos | OK | modelos cargados desde artefactos oficiales |
| no modificacion de datasets | OK | lectura solamente |
| no modificacion de features | OK | feature spec congelado |
| no modificacion de metricas/shocks | OK | D35/D35-b congelados |

## Confirmacion final

- Apertura unica TEST GC2/XGBoost: SI.
- Resultados congelados para interpretacion posterior: SI.
