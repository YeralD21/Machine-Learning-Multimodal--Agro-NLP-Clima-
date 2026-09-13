# AUDITORIA COMPARACION FINAL MODELOS

Consolidacion final v2 a partir de artefactos TEST ya congelados. No se entreno, no se reentreno, no se abrio nuevamente TEST y no se modificaron modelos, features, metricas ni mascaras de shock.

## Fuentes usadas

```json
{
  "gc3_ge_manifest": "C:\\Machine-learming\\Machine-Learning-Multimodal--Agro-NLP-Clima-\\v2_reentrenamiento\\resultados_v2_final\\evaluacion_test_gc3_ge\\manifest_evaluacion_test_gc3_ge.json",
  "gc3_ge_predictions": "C:\\Machine-learming\\Machine-Learning-Multimodal--Agro-NLP-Clima-\\v2_reentrenamiento\\resultados_v2_final\\evaluacion_test_gc3_ge\\predicciones_test_gc3_ge.csv",
  "gc3_ge_summary": "C:\\Machine-learming\\Machine-Learning-Multimodal--Agro-NLP-Clima-\\v2_reentrenamiento\\resultados_v2_final\\evaluacion_test_gc3_ge\\resumen_test_multiseed_gc3_ge.csv",
  "naive_dulce_metrics": "C:\\Machine-learming\\Machine-Learning-Multimodal--Agro-NLP-Clima-\\v2_reentrenamiento\\experimentos\\exp_001_naive_dulce\\metricas.json",
  "naive_dulce_predictions": "C:\\Machine-learming\\Machine-Learning-Multimodal--Agro-NLP-Clima-\\v2_reentrenamiento\\experimentos\\exp_001_naive_dulce\\predicciones.csv",
  "naive_sutil_metrics": "C:\\Machine-learming\\Machine-Learning-Multimodal--Agro-NLP-Clima-\\v2_reentrenamiento\\experimentos\\exp_001_naive_sutil\\metricas.json",
  "naive_sutil_predictions": "C:\\Machine-learming\\Machine-Learning-Multimodal--Agro-NLP-Clima-\\v2_reentrenamiento\\experimentos\\exp_001_naive_sutil\\predicciones.csv",
  "sarima_manifest": "C:\\Machine-learming\\Machine-Learning-Multimodal--Agro-NLP-Clima-\\v2_reentrenamiento\\resultados_v2_final\\evaluacion_test_sarima_rolling\\manifest_evaluacion_test_sarima_rolling.json",
  "sarima_metrics": "C:\\Machine-learming\\Machine-Learning-Multimodal--Agro-NLP-Clima-\\v2_reentrenamiento\\resultados_v2_final\\evaluacion_test_sarima_rolling\\metricas_test_sarima_rolling.csv",
  "sarima_predictions": "C:\\Machine-learming\\Machine-Learning-Multimodal--Agro-NLP-Clima-\\v2_reentrenamiento\\resultados_v2_final\\evaluacion_test_sarima_rolling\\predicciones_test_sarima_rolling.csv",
  "sarima_shocks": "C:\\Machine-learming\\Machine-Learning-Multimodal--Agro-NLP-Clima-\\v2_reentrenamiento\\resultados_v2_final\\evaluacion_test_sarima_rolling\\metricas_shock_sarima_rolling.csv",
  "xgb_manifest": "C:\\Machine-learming\\Machine-Learning-Multimodal--Agro-NLP-Clima-\\v2_reentrenamiento\\resultados_v2_final\\evaluacion_test_gc2_xgboost\\manifest_evaluacion_test_gc2_xgboost.json",
  "xgb_predictions": "C:\\Machine-learming\\Machine-Learning-Multimodal--Agro-NLP-Clima-\\v2_reentrenamiento\\resultados_v2_final\\evaluacion_test_gc2_xgboost\\predicciones_test_gc2_xgboost.csv",
  "xgb_shock_summary": "C:\\Machine-learming\\Machine-Learning-Multimodal--Agro-NLP-Clima-\\v2_reentrenamiento\\resultados_v2_final\\evaluacion_test_gc2_xgboost\\resumen_shock_multiseed_gc2_xgboost.csv",
  "xgb_summary": "C:\\Machine-learming\\Machine-Learning-Multimodal--Agro-NLP-Clima-\\v2_reentrenamiento\\resultados_v2_final\\evaluacion_test_gc2_xgboost\\resumen_test_multiseed_gc2_xgboost.csv"
}
```

## Hashes

```json
{
  "gc3_ge_manifest": "d4ee7a5217d468720b004bf2fd099c1cd09c5aeca4b5ffc1717d43cb926aacb7",
  "gc3_ge_predictions": "4124e4c24120bac199e1f623238181cc80c0c67d51755e57551c5ded2c95ab29",
  "gc3_ge_summary": "d816f2dc529f765813eb85ff469b21e6e32004178906034b271a346949952c52",
  "naive_dulce_metrics": "76e4b678ca09d21c7e041ec2a72ea0c4cc3028406cbfa891e34052aefdc194d9",
  "naive_dulce_predictions": "e2a9e6dc61a4cfbdd350c7ee0099c5feac84248ddc896d554be6e5f59f75f767",
  "naive_sutil_metrics": "3669bc8081109f37e539a95af983b2517b34f9f3ab632436cff41c8e53ea47a4",
  "naive_sutil_predictions": "3b19bcbdf005714fc525cc4942bdcd8849e06aca01068a67b08d50bc92140ac2",
  "sarima_manifest": "ece6345c30d78bb9e2ab4a34b02c46703091312d90f46caa320eb599ea719a60",
  "sarima_metrics": "aad8401d5fef21d42f140e969f9da27974f28458cc91d48d599f3962d7fb1057",
  "sarima_predictions": "561d0969ac05ad57f19464107e3857e84c553428a12712b1efdf8a645b1abc10",
  "sarima_shocks": "f258f325c58c41eeb386795821616c38e25175d9e944d17a35b7ce1b909939e3",
  "xgb_manifest": "c380471cc389ca76aa7bb2e5e8648ddd0303e7ac477ebc138032e2a7a68e228f",
  "xgb_predictions": "14c9d40716359d98926ae7e85743b4e2eaaf8ce89fea820261660442fa814bae",
  "xgb_shock_summary": "d69449b85d68ca55745b2fe19f6c4202fea20401a98e433a9fb32db8f51440c3",
  "xgb_summary": "9ba9198bf458dc53169d20f9ef677ef4ded07641d7b4e307bb63dfc650f20856"
}
```

## Comparabilidad

- Meses TEST: 2025-01..2025-12.
- Horizonte: t -> t+1 segun protocolos congelados.
- y_true validado por cultivar entre modelos.
- Mascaras shock: Sutil Jan/Jul/Nov; Dulce Jan/Feb/Mar.
- Multi-seed: XGBoost, GC3 y GE usan medias oficiales; SD reportada como sensibilidad entre seeds.
- Naive y SARIMA son deterministas; no se crea SD artificial.

## Tabla maestra

| cultivar | model | MAE | MAE_SD | RMSE | RMSE_SD | RelMAE_N1 | RelMAE_N1_SD | MASE_1 | MASE_1_SD | RMSSE_1 | RMSSE_1_SD | R2 | R2_SD | n_seeds | source |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| dulce | Naive | 84.700833 |  | 97.712081 |  | 1.000000 |  | 1.154938 |  | 1.135512 |  | 0.668554 |  | 1 | exp_001_naive_dulce |
| dulce | SARIMA_rolling | 21.766759 |  | 31.616495 |  | 0.256984 |  | 0.296801 |  | 0.367415 |  | 0.965299 |  | 1 | evaluacion_test_sarima_rolling |
| dulce | XGBoost | 59.378883 | 2.428516 | 77.205668 | 3.394146 | 0.701042 | 0.028672 | 0.809661 | 0.033114 | 0.897207 | 0.039443 | 0.792714 | 0.018591 | 10 | evaluacion_test_gc2_xgboost |
| dulce | GC3 | 103.413025 | 20.635199 | 125.318120 | 24.415534 | 1.220921 | 0.243625 | 1.410088 | 0.281371 | 1.456322 | 0.283733 | 0.436190 | 0.194636 | 10 | evaluacion_test_gc3_ge |
| dulce | GE | 95.584556 | 20.000875 | 116.899722 | 24.059130 | 1.128496 | 0.236136 | 1.303343 | 0.272722 | 1.358492 | 0.279591 | 0.507517 | 0.180869 | 10 | evaluacion_test_gc3_ge |
| sutil | Naive | 4704.292917 |  | 6317.411052 |  | 1.000000 |  | 1.394042 |  | 1.458959 |  | 0.474616 |  | 1 | exp_001_naive_sutil |
| sutil | SARIMA_rolling | 3985.016474 |  | 4894.896368 |  | 0.847102 |  | 1.180896 |  | 1.130440 |  | 0.684583 |  | 1 | evaluacion_test_sarima_rolling |
| sutil | XGBoost | 5902.375361 | 140.128108 | 7010.785262 | 96.968340 | 1.254679 | 0.029787 | 1.749074 | 0.041525 | 1.619089 | 0.022394 | 0.352848 | 0.017821 | 10 | evaluacion_test_gc2_xgboost |
| sutil | GC3 | 7746.000211 | 1528.989957 | 8684.061051 | 1882.404772 | 1.646581 | 0.325020 | 2.295403 | 0.453092 | 2.005519 | 0.434727 | -0.034742 | 0.437761 | 10 | evaluacion_test_gc3_ge |
| sutil | GE | 9426.458661 | 1481.729376 | 10830.150126 | 1837.034892 | 2.003799 | 0.314974 | 2.793379 | 0.439087 | 2.501143 | 0.424250 | -0.584054 | 0.478073 | 10 | evaluacion_test_gc3_ge |

## Tabla shocks

| cultivar | model | MAE_global | MAE_global_SD | MAE_shock | MAE_shock_SD | MAE_nonshock | MAE_nonshock_SD | Delta_s | Delta_s_SD | n_shock | n_nonshock | n_seeds |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| dulce | Naive | 84.700833 |  | 138.256667 |  | 66.848889 |  | 63.229405 |  | 3 | 9 | 1 |
| dulce | SARIMA_rolling | 21.766759 |  | 11.603702 |  | 25.154444 |  | -46.690723 |  | 3 | 9 | 1 |
| dulce | XGBoost | 59.378883 | 2.428516 | 90.104501 | 4.728704 | 49.137010 | 2.437434 | 51.774420 | 5.876318 | 3 | 9 | 10 |
| dulce | GC3 | 103.413025 | 20.635199 | 118.944194 | 34.592457 | 98.235969 | 18.361089 | 13.262077 | 19.225321 | 3 | 9 | 10 |
| dulce | GE | 95.584556 | 20.000875 | 99.907589 | 35.410418 | 94.143546 | 17.439509 | 1.903821 | 22.208278 | 3 | 9 | 10 |
| sutil | Naive | 4704.291667 |  | 11660.153333 |  | 2385.671111 |  | 147.862041 |  | 3 | 9 | 1 |
| sutil | SARIMA_rolling | 3985.016474 |  | 7824.402904 |  | 2705.220997 |  | 96.345560 |  | 3 | 9 | 1 |
| sutil | XGBoost | 5902.375361 | 140.128108 | 8296.896516 | 238.361386 | 5104.201642 | 179.661553 | 40.615361 | 4.431079 | 3 | 9 | 10 |
| sutil | GC3 | 7746.000211 | 1528.989957 | 5594.109032 | 1475.449355 | 8463.297270 | 1621.357746 | -28.104799 | 9.612913 | 3 | 9 | 10 |
| sutil | GE | 9426.458661 | 1481.729376 | 7908.487889 | 1569.095554 | 9932.448918 | 1526.747128 | -16.601239 | 9.289028 | 3 | 9 | 10 |

## Diferencia de MAE en meses shock (toneladas)

| cultivar | model | MAE_shock | shock_MAE_diff_vs_SARIMA | shock_MAE_diff_vs_XGB | shock_error_reduction_vs_SARIMA | shock_error_reduction_vs_XGB | interpretation_note |
| --- | --- | --- | --- | --- | --- | --- | --- |
| dulce | Naive | 138.256667 | 126.652965 | 48.152166 | -126.652965 | -48.152166 | diferencia/reduccion descriptiva del error absoluto medio; no ahorro economico |
| dulce | SARIMA_rolling | 11.603702 | 0.000000 | -78.500799 | -0.000000 | 78.500799 | diferencia/reduccion descriptiva del error absoluto medio; no ahorro economico |
| dulce | XGBoost | 90.104501 | 78.500799 | 0.000000 | -78.500799 | -0.000000 | diferencia/reduccion descriptiva del error absoluto medio; no ahorro economico |
| dulce | GC3 | 118.944194 | 107.340492 | 28.839693 | -107.340492 | -28.839693 | diferencia/reduccion descriptiva del error absoluto medio; no ahorro economico |
| dulce | GE | 99.907589 | 88.303887 | 9.803088 | -88.303887 | -9.803088 | diferencia/reduccion descriptiva del error absoluto medio; no ahorro economico |
| sutil | Naive | 11660.153333 | 3835.750429 | 3363.256817 | -3835.750429 | -3363.256817 | diferencia/reduccion descriptiva del error absoluto medio; no ahorro economico |
| sutil | SARIMA_rolling | 7824.402904 | 0.000000 | -472.493612 | -0.000000 | 472.493612 | diferencia/reduccion descriptiva del error absoluto medio; no ahorro economico |
| sutil | XGBoost | 8296.896516 | 472.493612 | 0.000000 | -472.493612 | -0.000000 | diferencia/reduccion descriptiva del error absoluto medio; no ahorro economico |
| sutil | GC3 | 5594.109032 | -2230.293872 | -2702.787484 | 2230.293872 | 2702.787484 | diferencia/reduccion descriptiva del error absoluto medio; no ahorro economico |
| sutil | GE | 7908.487889 | 84.084984 | -388.408628 | -84.084984 | 388.408628 | diferencia/reduccion descriptiva del error absoluto medio; no ahorro economico |

## Controles

| Control | Estado | Detalle |
|---|---|---|
| sutil Naive mismos 12 meses TEST | OK | ['2025-01', '2025-02', '2025-03', '2025-04', '2025-05', '2025-06', '2025-07', '2025-08', '2025-09', '2025-10', '2025-11', '2025-12'] |
| sutil Naive mascara shock oficial | OK | ['2025-01', '2025-07', '2025-11'] |
| sutil Naive n_shock=3 | OK | 3 |
| sutil Naive n_nonshock=9 | OK | 9 |
| sutil SARIMA_rolling mismos 12 meses TEST | OK | ['2025-01', '2025-02', '2025-03', '2025-04', '2025-05', '2025-06', '2025-07', '2025-08', '2025-09', '2025-10', '2025-11', '2025-12'] |
| sutil SARIMA_rolling mismos y_true | OK | comparado contra Naive; tolerancia=0.01 t por redondeo de artefactos CSV |
| sutil SARIMA_rolling mascara shock oficial | OK | ['2025-01', '2025-07', '2025-11'] |
| sutil SARIMA_rolling n_shock=3 | OK | 3 |
| sutil SARIMA_rolling n_nonshock=9 | OK | 9 |
| sutil XGBoost mismos 12 meses TEST | OK | ['2025-01', '2025-02', '2025-03', '2025-04', '2025-05', '2025-06', '2025-07', '2025-08', '2025-09', '2025-10', '2025-11', '2025-12'] |
| sutil XGBoost mismos y_true | OK | comparado contra Naive; tolerancia=0.01 t por redondeo de artefactos CSV |
| sutil XGBoost mascara shock oficial | OK | ['2025-01', '2025-07', '2025-11'] |
| sutil XGBoost n_shock=3 | OK | 3 |
| sutil XGBoost n_nonshock=9 | OK | 9 |
| sutil GC3 mismos 12 meses TEST | OK | ['2025-01', '2025-02', '2025-03', '2025-04', '2025-05', '2025-06', '2025-07', '2025-08', '2025-09', '2025-10', '2025-11', '2025-12'] |
| sutil GC3 mismos y_true | OK | comparado contra Naive; tolerancia=0.01 t por redondeo de artefactos CSV |
| sutil GC3 mascara shock oficial | OK | ['2025-01', '2025-07', '2025-11'] |
| sutil GC3 n_shock=3 | OK | 3 |
| sutil GC3 n_nonshock=9 | OK | 9 |
| sutil GE mismos 12 meses TEST | OK | ['2025-01', '2025-02', '2025-03', '2025-04', '2025-05', '2025-06', '2025-07', '2025-08', '2025-09', '2025-10', '2025-11', '2025-12'] |
| sutil GE mismos y_true | OK | comparado contra Naive; tolerancia=0.01 t por redondeo de artefactos CSV |
| sutil GE mascara shock oficial | OK | ['2025-01', '2025-07', '2025-11'] |
| sutil GE n_shock=3 | OK | 3 |
| sutil GE n_nonshock=9 | OK | 9 |
| dulce Naive mismos 12 meses TEST | OK | ['2025-01', '2025-02', '2025-03', '2025-04', '2025-05', '2025-06', '2025-07', '2025-08', '2025-09', '2025-10', '2025-11', '2025-12'] |
| dulce Naive mascara shock oficial | OK | ['2025-01', '2025-02', '2025-03'] |
| dulce Naive n_shock=3 | OK | 3 |
| dulce Naive n_nonshock=9 | OK | 9 |
| dulce SARIMA_rolling mismos 12 meses TEST | OK | ['2025-01', '2025-02', '2025-03', '2025-04', '2025-05', '2025-06', '2025-07', '2025-08', '2025-09', '2025-10', '2025-11', '2025-12'] |
| dulce SARIMA_rolling mismos y_true | OK | comparado contra Naive; tolerancia=0.01 t por redondeo de artefactos CSV |
| dulce SARIMA_rolling mascara shock oficial | OK | ['2025-01', '2025-02', '2025-03'] |
| dulce SARIMA_rolling n_shock=3 | OK | 3 |
| dulce SARIMA_rolling n_nonshock=9 | OK | 9 |
| dulce XGBoost mismos 12 meses TEST | OK | ['2025-01', '2025-02', '2025-03', '2025-04', '2025-05', '2025-06', '2025-07', '2025-08', '2025-09', '2025-10', '2025-11', '2025-12'] |
| dulce XGBoost mismos y_true | OK | comparado contra Naive; tolerancia=0.01 t por redondeo de artefactos CSV |
| dulce XGBoost mascara shock oficial | OK | ['2025-01', '2025-02', '2025-03'] |
| dulce XGBoost n_shock=3 | OK | 3 |
| dulce XGBoost n_nonshock=9 | OK | 9 |
| dulce GC3 mismos 12 meses TEST | OK | ['2025-01', '2025-02', '2025-03', '2025-04', '2025-05', '2025-06', '2025-07', '2025-08', '2025-09', '2025-10', '2025-11', '2025-12'] |
| dulce GC3 mismos y_true | OK | comparado contra Naive; tolerancia=0.01 t por redondeo de artefactos CSV |
| dulce GC3 mascara shock oficial | OK | ['2025-01', '2025-02', '2025-03'] |
| dulce GC3 n_shock=3 | OK | 3 |
| dulce GC3 n_nonshock=9 | OK | 9 |
| dulce GE mismos 12 meses TEST | OK | ['2025-01', '2025-02', '2025-03', '2025-04', '2025-05', '2025-06', '2025-07', '2025-08', '2025-09', '2025-10', '2025-11', '2025-12'] |
| dulce GE mismos y_true | OK | comparado contra Naive; tolerancia=0.01 t por redondeo de artefactos CSV |
| dulce GE mascara shock oficial | OK | ['2025-01', '2025-02', '2025-03'] |
| dulce GE n_shock=3 | OK | 3 |
| dulce GE n_nonshock=9 | OK | 9 |
| horizonte t -> t+1 verificado por artefactos oficiales | OK | SARIMA/XGB/GC3/GE manifests y predicciones congeladas |
| tabla maestra 10 filas | OK | 10 |
| tabla shocks 10 filas | OK | 10 |
| tabla diferencias shock 10 filas | OK | 10 |
| metricas D35 presentes | OK | ['cultivar', 'model', 'MAE', 'MAE_SD', 'RMSE', 'RMSE_SD', 'RelMAE_N1', 'RelMAE_N1_SD', 'MASE_1', 'MASE_1_SD', 'RMSSE_1', 'RMSSE_1_SD', 'R2', 'R2_SD', 'n_seeds', 'source'] |
| shocks D35-b presentes | OK | ['cultivar', 'model', 'MAE_global', 'MAE_global_SD', 'MAE_shock', 'MAE_shock_SD', 'MAE_nonshock', 'MAE_nonshock_SD', 'Delta_s', 'Delta_s_SD', 'n_shock', 'n_nonshock', 'n_seeds'] |
| chequeo especial Sutil SARIMA | OK | 7824.402904 |
| chequeo especial Sutil XGBoost | OK | 8296.896516 |
| chequeo especial Sutil GC3 | OK | 5594.109032 |
| chequeo especial Sutil GE | OK | 7908.487889 |
| no retraining/no nueva apertura TEST | OK | solo lectura de artefactos congelados |
