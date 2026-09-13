# AUDITORIA INTERPRETACION SHAP V2

Estado: PASS

## Reconstruccion SHAP

- Error maximo documentado: 1.11217088117e-06
- Tolerancia oficial de reconstruccion: 1e-05
- Tolerancia oficial de prediccion congelada: 1e-06
- Las tolerancias son distintas: reconstruccion usa 1e-5 en escala del target escalado; prediccion usa 1e-6 en escala del target escalado.
- Criterio matematico: max(|prediction_scaled - (base_value + sum(SHAP_signed))|) <= reconstruction_atol_scaled.
- Unidad: target escalado, no toneladas.
- Maximo en: cultivar=sutil, modelo=XGBoost, seed=05, obs_index=2, fecha=2025-03-01.

## Salidas generadas

- C:\Machine-learming\Machine-Learning-Multimodal--Agro-NLP-Clima-\v2_reentrenamiento\resultados_v2_final\shap\interpretacion\tabla_grupos_completa.csv
- C:\Machine-learming\Machine-Learning-Multimodal--Agro-NLP-Clima-\v2_reentrenamiento\resultados_v2_final\shap\interpretacion\tabla_nlp.csv
- C:\Machine-learming\Machine-Learning-Multimodal--Agro-NLP-Clima-\v2_reentrenamiento\resultados_v2_final\shap\interpretacion\tabla_effective_lag.csv
- C:\Machine-learming\Machine-Learning-Multimodal--Agro-NLP-Clima-\v2_reentrenamiento\resultados_v2_final\shap\interpretacion\tabla_shock_nonshock_shap.csv
- C:\Machine-learming\Machine-Learning-Multimodal--Agro-NLP-Clima-\v2_reentrenamiento\resultados_v2_final\shap\interpretacion\tabla_estabilidad_seeds_shap.csv
- C:\Machine-learming\Machine-Learning-Multimodal--Agro-NLP-Clima-\v2_reentrenamiento\resultados_v2_final\shap\interpretacion\tabla_gc3_vs_ge.csv
- C:\Machine-learming\Machine-Learning-Multimodal--Agro-NLP-Clima-\v2_reentrenamiento\resultados_v2_final\shap\interpretacion\tabla_xgboost_shap.csv
- C:\Machine-learming\Machine-Learning-Multimodal--Agro-NLP-Clima-\v2_reentrenamiento\resultados_v2_final\shap\interpretacion\MATRIZ_AFIRMACIONES_SHAP.md

## Comparacion predictiva congelada

- Sutil: SARIMA es mejor global por MAE congelado; GC3 presenta menor MAE shock observado; GE queda peor que GC3 en MAE global y shock.
- Dulce: SARIMA es mejor global y shock; GE mejora descriptivamente frente a GC3, pero no supera SARIMA ni XGBoost globalmente.

## Effective lag

Para redes, effective_lag = -offset_secuencia_vs_origen + lag_interno_feature. Ejemplo: X_lag3 en t-5 equivale a t-8 respecto del origen.
Esta agregacion es derivada post-hoc y no reemplaza los SHAP elementales ni la agregacion oficial por timestep.

## Figuras candidatas

| Figura | Ubicacion sugerida | Nota |
| --- | --- | --- |
| Importancia SHAP por grupos GC3/GE | paper principal | Barras por cultivar/modelo usando tabla_grupos_completa.csv. |
| Top features por cultivar | paper principal o suplemento | Mostrar top 10 con SD entre seeds; separar familias de modelo. |
| NLP Sutil vs Dulce | paper principal si el texto discute GE; si no, suplemento | Variables NLP, participacion total y ranking. |
| Shock vs nonshock por grupos | suplemento | n=3 vs n=9, descriptivo sin inferencia. |
| Effective lag | paper principal metodologico o suplemento | Mostrar agregacion derivada junto a advertencia de lag efectivo. |
| Estabilidad multi-seed o XGBoost | suplemento | Rank/SD entre seeds; XGBoost como patron descriptivo separado. |

## Estabilidad interpretativa

Features relativamente estables destacadas:
- sutil/XGBoost produccion_t_sutil: mean=0.665813895499, SD=0.028165758145, rank_SD=0
- dulce/XGBoost mes_sin: mean=0.421590485777, SD=0.0411807711174, rank_SD=0
- dulce/XGBoost produccion_t_dulce: mean=0.210127697649, SD=0.0146268511165, rank_SD=0
- dulce/XGBoost produccion_t_dulce_lag6: mean=0.155306525641, SD=0.0177069054642, rank_SD=0.316227766017
- sutil/XGBoost WS2M_lag3: mean=0.143613429471, SD=0.0206561986062, rank_SD=0
- dulce/XGBoost RH2M_lag1: mean=0.121253301332, SD=0.0300927874008, rank_SD=0.316227766017
- sutil/XGBoost T2M_lag6: mean=0.100826953404, SD=0.0133965042155, rank_SD=0
- dulce/GE produccion_t_dulce_lag1: mean=0.0927013566616, SD=0.0160368324416, rank_SD=1.70293863659
- sutil/XGBoost RH2M_lag3: mean=0.0688375463427, SD=0.0084490293628, rank_SD=0.421637021356
- sutil/XGBoost WS2M_lag1: mean=0.0568089384128, SD=0.0118605142189, rank_SD=1.07496769977
- sutil/XGBoost mes_cos: mean=0.0504781255398, SD=0.00851511620381, rank_SD=0.918936583473
- dulce/XGBoost produccion_t_dulce_lag3: mean=0.0493597975819, SD=0.0155218870473, rank_SD=0.966091783079

Features altamente variables destacadas:
- sutil/GE personas_afectadas_lag3: mean=0.0579508989566, SD=0.0535479901836, rank_SD=10.4291258822
- sutil/GE num_emergencias_lag1: mean=0.0518933576996, SD=0.0486315260328, rank_SD=9.51957048518
- sutil/GC3 personas_afectadas_lag3: mean=0.0863823519644, SD=0.0443120389905, rank_SD=8.39378076647
- sutil/GE PRECTOTCORR_lag6: mean=0.0508115294755, SD=0.0437677698645, rank_SD=7.05533682951
- sutil/GE WS2M_lag1: mean=0.053195365484, SD=0.0437467971075, rank_SD=10.4695325164
- sutil/GE PRECTOTCORR_lag3: mean=0.0516227852864, SD=0.0427912655948, rank_SD=7.37864787373
- sutil/GE WS2M_lag3: mean=0.0506514894112, SD=0.0427719522968, rank_SD=5.1001089313
- sutil/GE T2M_lag6: mean=0.0522727061781, SD=0.0426032860291, rank_SD=10.2680734967
- sutil/GC3 num_emergencias_lag1: mean=0.0740470066374, SD=0.0377979514255, rank_SD=7.10242525089
- sutil/GE T2M_MAX_lag1: mean=0.0452742203036, SD=0.036584819074, rank_SD=7.78959419853
- sutil/GE produccion_t_sutil_lag6: mean=0.0528611539017, SD=0.0362554554692, rank_SD=8.38583461691
- sutil/GE T2M_lag1: mean=0.0523822975192, SD=0.0359234662636, rank_SD=8.33266664

## Riesgos de interpretacion

- No interpretar SHAP como causalidad.
- No comparar magnitudes SHAP brutas entre familias de modelos como una escala causal comun.
- No convertir diferencias shock/nonshock en significancia inferencial.
- No leer timestep como antiguedad efectiva cuando la feature contiene lag interno.
- No usar NLP como prueba de efecto causal de noticias.
- No afirmar resiliencia, beneficio economico ni toneladas ahorradas a partir de SHAP.

## Controles

| Control | Estado |
| --- | --- |
| No se ejecuto SHAP | PASS |
| No se modificaron RAW oficiales | PASS |
| Participaciones por grupo suman aproximadamente 1 | PASS |
| NLP extraido para Sutil GE y Dulce GE | PASS |
| Effective lag documentado | PASS |
| Shock/nonshock n=3/9 | PASS |
| Comparacion predictiva usa tabla congelada | PASS |
| Matriz de afirmaciones creada | PASS |
