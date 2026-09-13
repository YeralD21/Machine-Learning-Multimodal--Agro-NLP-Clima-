# AUDITORIA INVENTARIO CIERRE EXPERIMENTAL V2

Fecha de auditoria: 2026-09-13

Alcance: inventario y trazabilidad documental de `v2_reentrenamiento/`. No se entreno, no se reentreno, no se ejecutaron modelos, no se ejecuto SHAP, no se recalcularon metricas, no se modificaron resultados congelados, no se generaron figuras y no se movieron/borraron/renombraron artefactos.

## 1. Resumen ejecutivo

Estado final propuesto: **B) EXPERIMENTO CERRADO CON DEUDA DOCUMENTAL MENOR**.

La fase experimental v2 esta cientificamente cerrada: existen artefactos congelados para Naive, SARIMA rolling, XGBoost, GC3, GE, analisis shock y SHAP oficial. La comparacion final contiene los cinco modelos, los dos cultivares, las metricas globales, las metricas shock, las mascaras oficiales y las fuentes de cada resultado. Los modelos multi-seed tienen artefactos por seed, predicciones TEST, metricas por seed, resumen multi-seed y manifiestos. SHAP oficial tiene 60 RAW NPZ, metadata por seed, agregados por seed, agregados multi-seed, manifiesto SHA256, auditoria post-run y tablas derivadas para interpretacion.

No se identifico una ausencia que bloquee reproducibilidad cientifica. La deuda menor es documental/presentacional:

- Naive esta incorporado y auditado en la comparacion final, pero no tiene carpeta final propia tipo `evaluacion_test_naive/` ni auditoria preflight/postrun exclusiva.
- Hay artefactos piloto y preflight SHAP en `resultados_v2_final/shap/_preflight/` y `technical_pilot*` que deben senalarse como no oficiales para evitar confusion.
- Ya existen figuras predictivas/comparativas, pero faltan figuras SHAP y una seleccion final de figuras de publicacion.

## 2. Inventario por categoria

| Categoria | Artefactos existentes | Estado |
| --- | --- | --- |
| A. decisiones metodologicas | `DECISIONES_METODOLOGICAS.md`; auditorias D0-D10, D4-D36, D35, D51-D61 SHAP | OK |
| B. auditorias | 30 `.md` en `v2_reentrenamiento/auditorias/`; relevantes oficiales: comparacion final, evaluaciones TEST, preflight/postrun, metodologia, interpretacion SHAP | OK |
| C. scripts | Scripts oficiales/evaluacion en `auditorias/` y SHAP oficial en `scripts/`; 7 scripts oficiales principales identificados | OK |
| D. datasets/features finales | `data/processed/master_dataset_*_v2.csv`, `*_features.csv`, `*_escalado.csv`; fuentes interim/raw trazables | OK |
| E. modelos congelados | XGBoost: 20 modelos `.joblib` + JSON; GC3/GE: 40 checkpoints `.keras`; SARIMA/Naive deterministas sin pesos persistidos equivalentes | OK |
| F. predicciones TEST congeladas | Naive en `experimentos/exp_001_*`; SARIMA, XGBoost, GC3/GE en `resultados_v2_final/evaluacion_test_*` | OK |
| G. metricas globales | `comparacion_final/tabla_maestra_modelos.csv`; metricas por familia en `evaluacion_test_*` | OK |
| H. analisis shock | `comparacion_final/tabla_shocks_modelos.csv`, `tabla_diferencia_error_shock_ton.csv`, metricas shock por familia | OK |
| I. SHAP RAW | `resultados_v2_final/shap/official_test_2025/{cultivar}/{modelo}/seed_XX/raw.npz` = 60 RAW | OK |
| J. SHAP agregado/interpretacion | `aggregates_descriptive.json`, `seed_variability_all_aggregates.json`, `shap/interpretacion/*.csv`, matriz de afirmaciones | OK |
| K. tablas finales | Comparacion final, evaluacion TEST, SHAP interpretacion, resumen short paper | OK |
| L. figuras/graficos | 31 figuras en `resultados_v2_final`; predictivas/comparativas/diagnosticas/piloto; no hay figuras SHAP oficiales | PARCIAL |
| M. metadata/manifiestos/hashes | Manifiestos por evaluacion, entrenamiento, comparacion final, SHAP official, metadata por seed | OK |
| N. documentacion/handoff | `HANDOFF_SESION.md`, `HANDOFF_CONTEXTO_ACTUAL.md`, `ENTORNO_COMPUTO.md`, `README.md` final | OK |
| O. otros artefactos relevantes | Notebooks historicos, experimentos GC1/Prophet/SARIMA antiguo, pilotos tecnicos | SOLO ANTECEDENTE/NO OFICIAL |

## 3. Matriz de trazabilidad de modelos

| modelo | cultivar | script oficial | configuracion documentada | modelo/pesos congelados si aplica | predicciones TEST | metricas globales | metricas shock | seeds | auditoria preflight | auditoria postrun | hash/integridad | estado |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Naive | sutil | Notebook/experimento `exp_001_naive_sutil`; consolidado por `consolidar_comparacion_final_modelos.py` | `DECISIONES_METODOLOGICAS.md`, `AUDITORIA_COMPARACION_FINAL_MODELOS.md` | NO APLICA | `experimentos/exp_001_naive_sutil/predicciones.csv` | `comparacion_final/tabla_maestra_modelos.csv` | `comparacion_final/tabla_shocks_modelos.csv` | 1 | NO APLICA/PARCIAL | `AUDITORIA_COMPARACION_FINAL_MODELOS.md` | hashes en manifest de comparacion final | OK con deuda documental menor |
| Naive | dulce | Notebook/experimento `exp_001_naive_dulce`; consolidado por `consolidar_comparacion_final_modelos.py` | idem | NO APLICA | `experimentos/exp_001_naive_dulce/predicciones.csv` | idem | idem | 1 | NO APLICA/PARCIAL | idem | idem | OK con deuda documental menor |
| SARIMA rolling | sutil | `evaluar_test_sarima_rolling.py` | `AUDITORIA_METODOLOGICA_SARIMA_ROLLING.md`, `AUDITORIA_PREFLIGHT_FINAL_SARIMA_ROLLING.md` | NO APLICA, rolling refit documentado | `evaluacion_test_sarima_rolling/predicciones_test_sarima_rolling.csv` | `metricas_test_sarima_rolling.csv`, tabla maestra | `metricas_shock_sarima_rolling.csv`, tabla shocks | 1 | OK | `AUDITORIA_EVALUACION_TEST_SARIMA_ROLLING.md` | `manifest_evaluacion_test_sarima_rolling.json` | OK |
| SARIMA rolling | dulce | `evaluar_test_sarima_rolling.py` | idem | NO APLICA | idem | idem | idem | 1 | OK | OK | OK | OK |
| XGBoost | sutil | `ejecutar_entrenamiento_oficial_gc2_xgboost.py`, `evaluar_test_gc2_xgboost.py` | `AUDITORIA_METODOLOGICA_GC2_XGBOOST.md`, `AUDITORIA_PREFLIGHT_GC2_XGBOOST.md` | `official_gc2_xgboost/sutil/GC2_XGBoost/seed_00..09/model.joblib` y `model.xgboost.json` | `evaluacion_test_gc2_xgboost/predicciones_test_gc2_xgboost.csv` | `resumen_test_multiseed_gc2_xgboost.csv`, tabla maestra | `resumen_shock_multiseed_gc2_xgboost.csv`, tabla shocks | 10 | OK | `AUDITORIA_POST_RUN_GC2_XGBOOST.md`, `AUDITORIA_EVALUACION_TEST_GC2_XGBOOST.md` | `manifest_evaluacion_test_gc2_xgboost.json`, metadata por seed | OK |
| XGBoost | dulce | idem | idem | `official_gc2_xgboost/dulce/GC2_XGBoost/seed_00..09/` | idem | idem | idem | 10 | OK | OK | OK | OK |
| GC3 | sutil | `ejecutar_entrenamiento_oficial_gc3_ge.py`, `evaluar_test_gc3_ge.py` | `AUDITORIA_D4_D36_ARQUITECTURA_GC3_GE.md`, `AUDITORIA_RUNNER_OFICIAL_GC3_GE.md` | `official_gc3_ge/sutil/GC3/seed_00..09/checkpoint_best.keras` | `evaluacion_test_gc3_ge/predicciones_test_gc3_ge.csv` y predicciones por corrida | `resumen_test_multiseed_gc3_ge.csv`, tabla maestra | `metricas_shock_por_seed_gc3_ge.csv`, tabla shocks | 10 | OK | `AUDITORIA_POST_RUN_GC3_GE.md`, `AUDITORIA_EVALUACION_TEST_GC3_GE.md` | `manifest_evaluacion_test_gc3_ge.json`, metadata por seed | OK |
| GC3 | dulce | idem | idem | `official_gc3_ge/dulce/GC3/seed_00..09/checkpoint_best.keras` | idem | idem | idem | 10 | OK | OK | OK | OK |
| GE | sutil | `ejecutar_entrenamiento_oficial_gc3_ge.py`, `evaluar_test_gc3_ge.py` | idem + NLP documentado | `official_gc3_ge/sutil/GE/seed_00..09/checkpoint_best.keras` | idem | idem | idem | 10 | OK | OK | OK | OK |
| GE | dulce | idem | idem | `official_gc3_ge/dulce/GE/seed_00..09/checkpoint_best.keras` | idem | idem | idem | 10 | OK | OK | OK | OK |

## 4. Cobertura de resultados principales

| Resultado paper | Archivo oficial | Cobertura |
| --- | --- | --- |
| MAE | `comparacion_final/tabla_maestra_modelos.csv` | 5 modelos x 2 cultivares |
| RMSE | `comparacion_final/tabla_maestra_modelos.csv` | 5 modelos x 2 cultivares |
| RelMAE_N1 | `comparacion_final/tabla_maestra_modelos.csv` | 5 modelos x 2 cultivares |
| MASE1 | `comparacion_final/tabla_maestra_modelos.csv` columna `MASE_1` | 5 modelos x 2 cultivares |
| RMSSE1 | `comparacion_final/tabla_maestra_modelos.csv` columna `RMSSE_1` | 5 modelos x 2 cultivares |
| R2 | `comparacion_final/tabla_maestra_modelos.csv` columna `R2` | 5 modelos x 2 cultivares |
| MAE_global | `comparacion_final/tabla_shocks_modelos.csv` | 5 modelos x 2 cultivares |
| MAE_shock | `comparacion_final/tabla_shocks_modelos.csv` | 5 modelos x 2 cultivares |
| MAE_nonshock | `comparacion_final/tabla_shocks_modelos.csv` | 5 modelos x 2 cultivares |
| n_shock/n_nonshock | `comparacion_final/tabla_shocks_modelos.csv` | Sutil 3/9; Dulce 3/9 |
| Delta_s | `comparacion_final/tabla_shocks_modelos.csv` | 5 modelos x 2 cultivares |
| Diferencias shock contra SARIMA/XGB | `comparacion_final/tabla_diferencia_error_shock_ton.csv` | 5 modelos x 2 cultivares |
| Multi-seed mean/SD global | `tabla_maestra_modelos.csv`, `resumen_test_multiseed_gc2_xgboost.csv`, `resumen_test_multiseed_gc3_ge.csv` | XGBoost, GC3, GE |
| Multi-seed por seed | `metricas_test_por_seed_gc2_xgboost.csv`, `metricas_test_por_seed_gc3_ge.csv` | XGBoost, GC3, GE |
| Multi-seed shock por seed | `metricas_shock_por_seed_gc2_xgboost.csv`, `metricas_shock_por_seed_gc3_ge.csv` | XGBoost, GC3, GE |

No se identifico falta de resultado cientifico esencial para reconstruir documentalmente las tablas de metricas del paper.

## 5. Cobertura SHAP

SHAP oficial se encuentra en `resultados_v2_final/shap/official_test_2025/`.

| Requisito SHAP | Artefacto | Estado |
| --- | --- | --- |
| SHAP elemental/raw | 60 `raw.npz` | OK |
| base values / expected values | `base_values` dentro de cada `raw.npz`; metadata por seed | OK |
| predicciones usadas por SHAP | `predictions_explainer`, `predictions_model_scaled` en RAW | OK |
| predicciones congeladas | `predictions_frozen_scaled`, `predictions_frozen_ton` en RAW | OK |
| fechas target | `target_dates` en RAW | OK |
| feature names | `feature_names`, `flat_feature_names` en RAW | OK |
| timestep | `flat_timestep`, `context_dates` en RAW | OK |
| branch/rama | `flat_branch` en RAW para redes | OK |
| cultivar/modelo/seed | ruta + `metadata.json` | OK |
| background | `background`, `background_indices`, `background.sha256_array` | OK |
| explainer y parametros | `metadata.json`, `complete.json` | OK |
| hashes | metadata por seed + `metadata/MANIFEST_SHA256.json` | OK |
| versiones | metadata por seed + `complete.json` | OK |
| tiempos | `metadata.json` por seed | OK |
| reconstruction checks | `reconstruction_abs_error` en RAW; `AUDITORIA_POSTRUN_SHAP_V2_OFICIAL.md` | OK |
| importancia feature/timestep/grupo | `aggregates_descriptive.json`, postrun JSON, interpretacion CSV | OK |
| NLP | `shap/interpretacion/tabla_nlp.csv` | OK |
| shock vs nonshock | `tabla_shock_nonshock_shap.csv` | OK |
| multi-seed SHAP | `seed_variability_all_aggregates.json`, `tabla_estabilidad_seeds_shap.csv` | OK |
| effective lag | `tabla_effective_lag.csv`; documentado en `AUDITORIA_INTERPRETACION_SHAP_V2.md` | OK |
| matriz de afirmaciones | `MATRIZ_AFIRMACIONES_SHAP.md` | OK |

No hay figuras SHAP generadas aun. Esto no bloquea cierre experimental; queda como presentacion.

## 6. Cobertura documental .md

Documentos humanos relevantes identificados:

| Documento | Que documenta |
| --- | --- |
| `DECISIONES_METODOLOGICAS.md` | Decisiones D0-D61, metodologia, restricciones interpretativas |
| `ENTORNO_COMPUTO.md` | Entorno de computo |
| `HANDOFF_SESION.md`, `HANDOFF_CONTEXTO_ACTUAL.md` | Estado congelado y traspaso operativo |
| `resultados_v2_final/README.md` | Notas de resultados finales, SARIMA-Sutil, Delta_s |
| `AUDITORIA_D0_D10_REPRODUCIBILIDAD_MULTISEED.md` | Reproducibilidad y multi-seed |
| `AUDITORIA_D4_D36_ARQUITECTURA_GC3_GE.md` | Arquitectura GC3/GE |
| `AUDITORIA_D35_ESTACIONALIDAD_TRAIN.md`, `AUDITORIA_D35_P75_SHOCKS.md`, `RECONSTRUCCION_D35_P75_TRAIN_ONLY.md` | Definicion y auditoria de shocks |
| `AUDITORIA_METODOLOGICA_GC2_XGBOOST.md` | Protocolo XGBoost |
| `AUDITORIA_METODOLOGICA_SARIMA_ROLLING.md` | Protocolo SARIMA rolling |
| `AUDITORIA_METODOLOGICA_SHAP_V2.md` | Protocolo SHAP D51-D61 |
| `AUDITORIA_PREFLIGHT_*` | Guardas preflight por familia |
| `AUDITORIA_EVALUACION_TEST_*` | Evaluacion TEST congelada por familia |
| `AUDITORIA_COMPARACION_FINAL_MODELOS.md` | Consolidacion final de metricas y shocks |
| `AUDITORIA_POSTRUN_SHAP_V2_OFICIAL.md` | Validacion post-run SHAP |
| `AUDITORIA_INTERPRETACION_SHAP_V2.md` | Preparacion interpretativa, effective lag, figuras candidatas |
| `MATRIZ_AFIRMACIONES_SHAP.md` | Frases permitidas/no permitidas para interpretacion SHAP |
| `RESUMEN_SHORT_PAPER_RESULTADOS.md` | Resumen corto de resultados finales |

Aspectos metodologicos relevantes cubiertos por `.md`:

- decisiones D0-D61: cubierto;
- split temporal y horizonte t -> t+1: cubierto en decisiones/auditorias/manifiestos;
- prevencion de leakage: cubierto en auditorias de datos/shock/SHAP;
- features finales: cubierto en auditorias y metadata; mas detalle operativo vive en codigo/metadata;
- arquitectura GC3/GE: cubierto;
- protocolo XGBoost: cubierto;
- protocolo SARIMA rolling: cubierto;
- definicion de shocks y Delta_s: cubierto;
- multi-seed: cubierto;
- metricas: cubierto;
- SHAP y effective lag: cubierto;
- limitaciones interpretativas: cubierto;
- resultados finales comparativos: cubierto.

Deuda menor: la trazabilidad de Naive esta suficientemente auditada en comparacion final, pero no tiene documento metodologico/preflight/postrun exclusivo del mismo nivel que SARIMA/XGB/GC3/GE.

## 7. Tablas existentes

### PAPER PRINCIPAL

| Tabla/archivo | Uso |
| --- | --- |
| `comparacion_final/tabla_maestra_modelos.csv` | Tabla global de modelos: MAE, RMSE, RelMAE_N1, MASE_1, RMSSE_1, R2 |
| `comparacion_final/tabla_shocks_modelos.csv` | Tabla shock: MAE_global, MAE_shock, MAE_nonshock, Delta_s, n_shock/n_nonshock |
| `comparacion_final/tabla_diferencia_error_shock_ton.csv` | Diferencias descriptivas en meses shock |
| `comparacion_final/RESUMEN_SHORT_PAPER_RESULTADOS.md` | Resumen narrativo corto para resultados |
| `shap/interpretacion/tabla_grupos_completa.csv` | SHAP por grupos GC3/GE |
| `shap/interpretacion/tabla_nlp.csv` | NLP en GE |
| `shap/interpretacion/tabla_effective_lag.csv` | Effective lag derivado |

### SUPLEMENTARIO

| Tabla/archivo | Uso |
| --- | --- |
| `evaluacion_test_gc2_xgboost/metricas_test_por_seed_gc2_xgboost.csv` | Metricas XGBoost por seed |
| `evaluacion_test_gc2_xgboost/metricas_shock_por_seed_gc2_xgboost.csv` | Shock XGBoost por seed |
| `evaluacion_test_gc2_xgboost/resumen_test_multiseed_gc2_xgboost.csv` | Resumen multi-seed XGBoost |
| `evaluacion_test_gc2_xgboost/resumen_shock_multiseed_gc2_xgboost.csv` | Shock multi-seed XGBoost |
| `evaluacion_test_gc3_ge/metricas_test_por_seed_gc3_ge.csv` | Metricas GC3/GE por seed |
| `evaluacion_test_gc3_ge/metricas_shock_por_seed_gc3_ge.csv` | Shock GC3/GE por seed |
| `evaluacion_test_gc3_ge/resumen_test_multiseed_gc3_ge.csv` | Resumen multi-seed GC3/GE |
| `evaluacion_test_gc3_ge/comparacion_pareada_gc3_vs_ge.csv` | Comparacion pareada GC3 vs GE |
| `shap/interpretacion/tabla_shock_nonshock_shap.csv` | SHAP shock/nonshock |
| `shap/interpretacion/tabla_estabilidad_seeds_shap.csv` | Estabilidad SHAP entre seeds |
| `shap/interpretacion/tabla_gc3_vs_ge.csv` | Contexto GC3 vs GE |
| `shap/interpretacion/tabla_xgboost_shap.csv` | XGBoost SHAP descriptivo |

### SOLO AUDITORIA/REPRODUCIBILIDAD

Manifiestos JSON, metadata por seed, `complete.json`, `shap_v2_postrun_oficial.json`, preflights SHAP, arrays de benchmark, predicciones por corrida y logs.

## 8. Figuras existentes

Se encontraron 31 figuras en `resultados_v2_final`.

### A. Figuras diagnosticas de entrenamiento / piloto

| Ruta | Representa | Oficialidad | Candidata paper |
| --- | --- | --- | --- |
| `technical_pilot*/sutil/GC3/seed_00/figures/pilot_loss_mse.png` | perdida piloto | piloto descartado | No |
| `technical_pilot*/sutil/GC3/seed_00/figures/pilot_metric_mae.png` | MAE piloto | piloto descartado | No |

### B. Figuras experimentales oficiales

| Ruta | Representa | Datos | Candidata paper | Comentario |
| --- | --- | --- | --- | --- |
| `comparacion_final/figuras/sutil_real_vs_predicted_modelos.png` | real vs predicho 2025 Sutil, modelos finales | TEST 2025 | Si | Publicable tras revision visual |
| `comparacion_final/figuras/dulce_real_vs_predicted_modelos.png` | real vs predicho 2025 Dulce, modelos finales | TEST 2025 | Si | Publicable tras revision visual |
| `comparacion_final/figuras/sutil_mae_global_por_modelo.png` | MAE global Sutil | TEST 2025 | Si | Publicable |
| `comparacion_final/figuras/dulce_mae_global_por_modelo.png` | MAE global Dulce | TEST 2025 | Si | Publicable |
| `comparacion_final/figuras/sutil_mae_shock_por_modelo.png` | MAE shock Sutil | TEST 2025 shock | Si | Publicable con nota n=3 |
| `comparacion_final/figuras/dulce_mae_shock_por_modelo.png` | MAE shock Dulce | TEST 2025 shock | Si | Publicable con nota n=3 |
| `comparacion_final/figuras/sutil_delta_s_por_modelo.png` | Delta_s Sutil | TEST 2025 | Suplemento | Usar con cautela |
| `comparacion_final/figuras/dulce_delta_s_por_modelo.png` | Delta_s Dulce | TEST 2025 | Suplemento | Usar con cautela |
| `evaluacion_test_sarima_rolling/figuras/*real_vs_predicted.png` | SARIMA real vs pred por cultivar | TEST 2025 | Suplemento | Oficial por familia |
| `evaluacion_test_gc2_xgboost/figuras/*real_vs_pred_seed_mediana_mae.png` | XGBoost seed mediana | TEST 2025 | Suplemento | Oficial por familia, no figura final integrada |
| `evaluacion_test_gc3_ge/figures/real_vs_pred_test_2025_*.png` | GC3/GE real vs pred | TEST 2025 | Suplemento | Oficial por familia |

### C. Figuras SHAP

No se encontraron figuras SHAP oficiales. SHAP esta cubierto numericamente por RAW, JSON y CSV derivados.

### D. Figuras antiguas/v1 o diagnosticas no publicables

| Ruta | Motivo |
| --- | --- |
| `auditoria_d35/acf_*.svg` | diagnostico de estacionalidad/autocorrelacion |
| `auditoria_d35/patron_mensual_*.svg` | diagnostico D35 |
| `evaluacion_test_gc2_xgboost/figuras/*distribucion_mae_seeds.png` | diagnostico multi-seed |
| `evaluacion_test_gc2_xgboost/figuras/*error_shock_vs_nonshock.png` | diagnostico por familia |
| `evaluacion_test_gc3_ge/figures/distribucion_mae_test_2025_*.png` | diagnostico multi-seed |
| `evaluacion_test_gc3_ge/figures/shock_diagnostico_test_2025_*.png` | diagnostico shock por familia |
| `evaluacion_test_sarima_rolling/figuras/shock_vs_nonshock_abs_error.png` | diagnostico SARIMA |

### E. Figuras candidatas para publicacion existentes

Publicables existentes: 8 figuras de `comparacion_final/figuras/` con prioridad sobre las figuras por familia. Las mas fuertes para paper principal son:

1. `sutil_real_vs_predicted_modelos.png`
2. `dulce_real_vs_predicted_modelos.png`
3. `sutil_mae_global_por_modelo.png`
4. `dulce_mae_global_por_modelo.png`
5. `sutil_mae_shock_por_modelo.png`
6. `dulce_mae_shock_por_modelo.png`

`delta_s` puede ir a suplemento o texto, por sensibilidad con n=3.

## 9. Figuras faltantes

Maximo 6 figuras recomendadas, basadas solo en resultados congelados:

| Figura propuesta | Objetivo cientifico | Fuente congelada | Principal/suplemento | Ya existe | Falta | Prioridad |
| --- | --- | --- | --- | --- | --- | --- |
| Real vs predicho 2025 por cultivar con 5 modelos | Mostrar ajuste temporal final | `comparacion_final/tabla_maestra_modelos.csv`, predicciones congeladas | Principal | Si, 2 figuras | No | ALTA |
| Comparacion global MAE/RMSE por cultivar | Comparar desempeno sin mezclar escalas entre cultivares | `tabla_maestra_modelos.csv` | Principal | MAE si; RMSE no integrado | Parcial | ALTA |
| Deterioro ante shocks | Mostrar MAE_global/MAE_shock/MAE_nonshock o Delta_s | `tabla_shocks_modelos.csv` | Principal/suplemento | MAE shock y Delta_s si; tripleta global/shock/nonshock no | Parcial | ALTA |
| SHAP por grupos GC3/GE | Interpretabilidad por bloques de variables | `tabla_grupos_completa.csv` | Principal | No | Si | ALTA |
| SHAP top features/NLP | Mostrar features y aporte NLP | `tabla_nlp.csv`, `shap_v2_postrun_oficial.json`, `tabla_xgboost_shap.csv` | Principal si se discute GE; suplemento si no | No | Si | MEDIA |
| Effective lag o estabilidad multi-seed | Evitar lectura erronea de timestep y mostrar robustez | `tabla_effective_lag.csv`, `tabla_estabilidad_seeds_shap.csv` | Suplemento o figura metodologica | No | Si | MEDIA |

## 10. Tablas de presentacion faltantes

| Elemento | Estado |
| --- | --- |
| Tabla global de modelos | Ya existe completamente en CSV; falta formatearla para paper |
| Tabla shock | Ya existe completamente en CSV; falta formatearla para paper |
| Tabla multi-seed | Resultado existe; falta decidir version compacta para suplemento |
| Tabla SHAP grupos | Ya existe en CSV; lista para formateo |
| Tabla NLP | Ya existe en CSV; lista para formateo |
| Tabla effective lag | Ya existe en CSV; probablemente suplemento por tamano |
| Tabla top features SHAP por modelo/cultivar | Existe en postrun JSON y reporte; si se quiere CSV dedicado, falta presentacion, no resultado |
| Tabla figuras/captions | Falta como material de redaccion, no como resultado cientifico |

No falta resultado cientifico esencial; faltan tablas de presentacion.

## 11. Artefactos huerfanos/ambiguos

| Situacion | Riesgo | Accion futura sugerida |
| --- | --- | --- |
| `technical_pilot/` y `technical_pilot_scaled/` dentro de `resultados_v2_final` | Pueden confundirse con corridas oficiales | Etiquetar en redaccion como pilotos no oficiales |
| `shap/_preflight/` con varios timestamps | Pueden confundirse con SHAP oficial TEST | Usar solo `shap/official_test_2025/` para resultados |
| `experimentos/exp_002*`, Prophet y notebooks GC1 | Antecedentes/descartados podrian mezclarse con oficiales | Mantener fuera de tablas finales salvo antecedente metodologico |
| Naive en `experimentos/exp_001_*`, no en `resultados_v2_final/evaluacion_test_naive/` | Trazabilidad asimetrica frente a otros modelos | Deuda documental menor; comparacion final lo audita |
| Figuras por familia y figuras consolidadas coexistentes | Puede haber duplicidad visual | Priorizar `comparacion_final/figuras/` para paper |
| SHAP top features en JSON/MD pero no en CSV dedicado | No bloquea, pero puede complicar armado de tablas | Opcional: tabla de presentacion futura |
| Resultados SHAP sin figuras | No bloquea experimento | Generar figuras en fase de presentacion |

No se detecto un resultado citado en auditorias oficiales sin CSV/JSON correspondiente. No se detecto script oficial clave sin auditoria asociada para XGB/GC3/GE/SARIMA/SHAP; Naive queda cubierto por comparacion final, no por auditoria exclusiva.

## 12. Estado Git

`git status -sb` reportado:

```text
## main...origin/main
 M v2_reentrenamiento/DECISIONES_METODOLOGICAS.md
?? .claude/
?? data/interim/indeci/indeci_temporal_2019_2025.csv
?? data/interim/nasa/clima_dataset_2019_2020.csv
?? sources/agraria-pe/sin-unificar/agro_news_2020.csv
?? sources/agraria-pe/sin-unificar/checkpoint_2019_2020.json
?? sources/noticias-ampliado/
?? src/scraping/*.py
?? src/weather/nasa_power_downloader.py
?? v2_reentrenamiento/HANDOFF_CONTEXTO_ACTUAL.md
?? v2_reentrenamiento/HANDOFF_SESION.md
?? v2_reentrenamiento/auditorias/*.md
?? v2_reentrenamiento/auditorias/*.py
?? v2_reentrenamiento/auditorias/*.json
?? v2_reentrenamiento/auditorias/*.csv
?? v2_reentrenamiento/auditorias/shap_v2_benchmark_arrays/
?? v2_reentrenamiento/resultados_v2_final/
?? v2_reentrenamiento/scripts/
?? v2_reentrenamiento/src/models/features_gc2_xgboost.py
?? v2_reentrenamiento/src/training/config_gc2_xgboost.py
?? v2_reentrenamiento/src/training/official_runner_gc2_xgboost.py
?? v2_reentrenamiento/src/training/official_runner_gc3_ge.py
?? v2_reentrenamiento/src/training/tabular_gc2_xgboost.py
```

Clasificacion de cambios/untracked:

- Experimento v2 oficial: `resultados_v2_final/official_*`, `evaluacion_test_*`, `comparacion_final/`, `src/training/*`, `src/models/features_gc2_xgboost.py`.
- SHAP: `scripts/ejecutar_shap_v2_oficial.py`, `resultados_v2_final/shap/`, auditorias SHAP, benchmark arrays.
- Interpretacion: `resultados_v2_final/shap/interpretacion/`, `AUDITORIA_INTERPRETACION_SHAP_V2.md`, `preparar_interpretacion_shap_v2.py`.
- Auditorias: multiples `.md`, `.py`, `.json`, `.csv` bajo `auditorias/`.
- Ajenos/no relacionados al cierre v2: `.claude/`, scraping, `sources/noticias-ampliado/`, algunos `data/interim/` y weather/scraping fuera del paquete final.

No se hizo `git add`, commit, push, checkout, reset ni clean.

## 13. Pendientes reales antes de redaccion

1. Elegir figuras definitivas del paper y suplemento.
2. Generar figuras SHAP desde tablas congeladas, sin recalcular SHAP.
3. Formatear tablas finales para paper: global, shock, SHAP grupos, NLP, effective lag/suplemento.
4. Escribir captions con advertencias: n_shock=3, Delta_s descriptivo, SHAP no causal, effective lag no igual a timestep.
5. Completar una nota documental breve para Naive si se desea simetria con las otras familias.
6. Separar explicitamente en la redaccion artefactos oficiales de pilotos/preflights/antecedentes.

## 14. Conclusion

El experimento v2 esta cerrado desde el punto de vista cientifico y reproducible. Los resultados esenciales existen, estan auditados, tienen predicciones/metricas/manifiestos y cubren los modelos solicitados. No se identifico bloqueo cientifico ni inconsistencia que impida redaccion.

Clasificacion: **B) EXPERIMENTO CERRADO CON DEUDA DOCUMENTAL MENOR**.

La deuda no exige nueva experimentacion; corresponde a presentacion, redaccion y clarificacion documental menor.
