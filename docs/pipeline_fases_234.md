# Pipeline Detallado — Fases 2, 3 y 4

> Sistema multi-cultivo de pronóstico de demanda agroindustrial (Perú, target: limón).
> Integra MIDAGRI + NASA POWER + INDECI + Agraria.pe → NLP (RoBERTuito) + LSTM-Attention + SHAP.
> Métricas reportadas en **escala z-score** (serie normalizada en Fase 2), salvo indicación contraria.

---

## Fase 2 — Ingeniería de Características

Genera el *master dataset* multivariado a partir del dataset limpio de Fase 1, añadiendo
señal de NLP (sentimiento RoBERTuito), codificación temporal cíclica, rezagos y escalado.
Configuración en `notebooks/fase2/config/fase2_config.json`.

### Notebooks en orden de ejecución

| N° | Archivo | Qué hace | Output generado |
|----|---------|----------|-----------------|
| 1 | `actividad_01_nlp_sentimiento.ipynb` | Análisis de sentimiento de noticias Agraria.pe con **RoBERTuito** (`pysentimiento/robertuito-sentiment-analysis`, vía `pysentimiento.create_analyzer`); agrega `avg_sentiment` y `n_noticias_beto` por mes | Columnas NLP añadidas al dataset base |
| 2 | `actividad_02_cyclic_time_encoding.ipynb` | Codificación temporal cíclica seno/coseno (`month_sin`, `month_cos`, `trimestre_sin/cos`) para capturar estacionalidad sin discontinuidad diciembre→enero | Features temporales cíclicas |
| 3 | `actividad_03_rezagos_temporales.ipynb` | Generación de rezagos (lags) **t-1, t-3, t-6** sobre variables de producción, clima y precio | Features de rezago |
| 4 | `actividad_04_normalizacion_escalado.ipynb` | Normalización z-score (`StandardScaler`) de features numéricas + `MinMaxScaler`; guarda scalers para desnormalizar en fases posteriores | `dataset_fase2_multivariado.csv` (2.6 MB), scalers en `notebooks/fase2/scalers/` |

> Scripts generadores (`_build_act0*.py`, `_run_beto.py`, `_prep_base_completo.py`) construyen
> los notebooks programáticamente vía `python gen_nb_fase2.py`.

### Decisiones técnicas clave

- **RoBERTuito por encima de léxicos de sentimiento**: transformer en español (con cabeza de sentimiento ya afinada, vía pysentimiento) captura contexto agroclimático (sequía, plagas, precios) que un diccionario no distingue.
- **Codificación cíclica sin/cos** en lugar de one-hot de mes: preserva continuidad temporal y reduce dimensionalidad; feature de alta importancia en SHAP (`month_cos`, `trimestre_num`).
- **Rezagos [1, 3, 6]** alineados a la periodicidad de cosecha del limón (ciclo trimestral / semestral).
- **z-score guardado en disco**: el master dataset queda z-scored para features numéricas y en crudo para geográficas/temporales (lat, lon, month_sin/cos), permitiendo desnormalizar a unidades físicas con `standard_scaler_fase2.joblib`.

### Tiempo estimado de ejecución
- Act. 01 (RoBERTuito, inferencia sobre corpus de noticias): **~10–20 min** (CPU) — cuello de botella de la fase.
- Act. 02–04 (vectorizado en pandas): **< 2 min** cada uno.
- **Total ≈ 15–25 min.**

---

## Fase 3 — Modelado Cuasiexperimental

Baselines clásicos y el modelo estructural de referencia (GE). Establece el marco
cuasiexperimental: un split temporal fijo (`2024-09-01` / `2024-11-01`) compara arquitecturas
crecientes en complejidad, aislando el aporte de cada componente.

### Notebooks en orden de ejecución

| N° | Archivo | Qué hace | Output generado |
|----|---------|----------|-----------------|
| 11 | `actividad_11_gc1_sarima.ipynb` | Baseline estadístico **SARIMA** `(1,0,0)(2,1,0)[12]`; selección por AIC | `resultados/gc1/gc1_sarima_metricas.json`, `gc1_sarima_predicciones.csv` |
| 12 | `actividad_12_gc1_prophet.ipynb` | Baseline **Prophet** (changepoint_prior=0.05, estacionalidad aditiva) | `resultados/gc1/gc1_prophet_metricas.json`, `gc1_prophet_predicciones.csv` |
| 13 | `actividad_13_gc2_sarimax_lstm.ipynb` | Híbrido **SARIMAX + LSTM(32)** con 8 exógenas (clima + INDECI); ablation SARIMAX-only | `resultados/gc2/gc2_metricas.json`, `gc2_predicciones.csv` |
| 14 | `actividad_14_ge_lstm_attention.ipynb` | **GE**: Dual-Input LSTM(64) + **Bahdanau Attention**(64), 23 features exógenas, sin NLP (ref. Gu et al. 2022) | `resultados/ge/ge_metricas.json`, `ge_predicciones.csv`, `ge_training_history.csv`, `ge_scalers.pkl` |

### Arquitectura implementada (GE — Grupo Estructural)

- **Dual-Input LSTM**: dos canales de entrada separados.
  - **Canal A** (target): serie histórica de `produccion_t` — `n_features_a = 1`.
  - **Canal B** (exógenas): 23 features (clima NASA, INDECI, temporales cíclicas) — `n_features_b = 23`.
- **Bahdanau Attention** (additive attention, 64 units): pondera los 6 timesteps de la secuencia; capas densas `W_query`, `W_values`, `V`. Requiere `custom_objects={'BahdanauAttention': ...}` al cargar.
- **Hiperparámetros**: `seq_len=6`, `lstm_units=64`, `attn_units=64`, `dropout=0.3`, `l2_reg=0.001`, `epochs=20` (best_val_loss=0.2638).
- **Split**: `n_train=40`, `n_test=10`, `split_date=2024-11-01`.
- **Modelos clásicos** (GC1/GC2): `n_train=44`, `n_test=12`, `split_date=2024-09-01`.

### Tiempo estimado de ejecución
- SARIMA / Prophet: **< 2 min** cada uno.
- GC2 (SARIMAX + LSTM, 26 épocas): **~3–5 min** (CPU).
- GE (Dual-LSTM + Attention, 20 épocas): **~5–10 min** (CPU).
- **Total ≈ 15–20 min.**

---

## Fase 4 — Evaluación y Explicabilidad

Grupo Multimodal (GM y sus versiones), competidores (XGBoost, TCN), explicabilidad SHAP,
reentrenamiento con datos extendidos 2019–2025, validación estadística y simulaciones
económicas (El Niño 2026).

### Notebooks en orden de ejecución

| N° | Archivo | Qué hace | Output generado |
|----|---------|----------|-----------------|
| 15 | `actividad_15_multimodal_nlp.ipynb` | **GM**: GE + NLP (`avg_sentiment`, `n_noticias_beto`), 25 features | `resultados/gm/gm_metricas.json`, `gm_predicciones.csv`, `gm_scalers.pkl` |
| 15v2 | `actividad_15v2_gm_nlp_mejorado.ipynb` | **GM_v2**: módulos NLP M1 (nlp_index), M2 (lag1), M3 (dropout 0.5), M4 (PCA 95%) | `resultados/gm_v2/gm_v2_metricas.json`, `gm_v2_predicciones.csv` |
| 15v3 | `actividad_15v3_xgboost_competidor.ipynb` | **XGBoost** competidor, 29 features (exog + lags + rolling) | `resultados/xgboost/xgb_metricas.json`, `xgb_predicciones.csv` |
| 15v4 | `actividad_15v4_tcn_competidor.ipynb` | **TCN** competidor (convoluciones causales) | `resultados/tcn/tcn_metricas.json`, `tcn_predicciones.csv` |
| 15v5 | `actividad_15v5_gm_nlp_v2.ipynb` | **GM_v3**: corpus NLP ampliado (600 noticias multi-fuente) + PCA | `resultados/gm_v3/gm_v3_metricas.json`, `gm_v3_predicciones.csv` |
| 15v6 | `actividad_15v6_gm_v4.ipynb` | **GM_v4**: + F3 shock-weighted loss | `resultados/gm_v4/gm_v4_metricas.json`, `gm_v4_predicciones.csv` |
| 16 | `actividad_16_shap.ipynb` | **SHAP** (KernelExplainer) sobre GE, ranking global de features | `resultados/shap/shap_resultados.json` |
| 16 | `actividad_16_reentrenamiento_extendido.ipynb` | Reentrenamiento con datos extendidos 2019–2025 (`_ext`, n_train=62/68) | `resultados/*_ext/`, `scaler_extendido*.json` |
| 17 | `actividad_17_reentrenamiento_final.ipynb` | Modelos finales (`_final`) sobre dataset reconstruido | `resultados/*_final/`, `ranking_final_44vs68.json` |
| 17 | `actividad_17_validacion.ipynb` | Validación estadística, RMSE condicionado a meses shock | `resultados/validacion/validacion_estadistica.json`, `resumen_validacion.csv` |
| — | `escenario_nino_2026.ipynb` | Simulación escenario El Niño 2026, sensibilidad climática | figuras / tablas de escenario |
| — | `impacto_economico_nino_2026.ipynb` | Impacto económico del shock climático | `impacto_economico_nino_2026_ejecutado.ipynb` |
| — | `preparar_dataset_extendido.ipynb`, `reconstruir_dataset_completo.ipynb`, `integracion_2019_2020.ipynb` | Preparación / reconstrucción del dataset 2019–2025 | `master_reconstruido_completo.csv` |

### Métricas obtenidas (test set, escala z-score)

Split principal DL: `n_train=40`, `n_test=10`. Clásicos y GM_v2+: `n_train=44`, `n_test=12`.
**Δs** = sensibilidad climática (de la simulación El Niño, ver CLAUDE.md).

| Modelo | MAE | RMSE | R² | Δs |
|--------|-----|------|----|----|
| Naive (baseline) | 0.0161 | — | — | — |
| **XGBoost** | **0.0471** | 0.0542 | -1.017 | 5% |
| GM_v4 (shock-loss) | 0.0640 | 0.0792 | -10.45 | — |
| **GM_v3** (corpus ampliado) | **0.0645** | 0.0709 | -8.18 | **22%** |
| GM_v2 | 0.0646 | 0.0771 | -9.86 | — |
| GE (sin NLP) | 0.0673 | 0.0698 | -2.345 | 11% |
| GC1-Prophet | 0.0919 | 0.1051 | -6.045 | — |
| GM (original) | 0.0981 | 0.1007 | -5.958 | — |
| GC1-SARIMA | 0.1006 | 0.1179 | -7.864 | — |
| TCN | 0.1860 | 0.1987 | -71.08 | — |
| GC2 (SARIMAX+LSTM) | 0.1969 | 0.2926 | -53.63 | — |

> **Lectura**: XGBoost lidera en MAE absoluto pero está dominado por lags y tiene la menor
> sensibilidad climática (5%). **GM_v3** es el modelo recomendado del sistema: mejor
> compromiso entre error y sensibilidad a shocks externos (22%) vía integración NLP + clima.
> Los R² negativos reflejan alta autocorrelación de la serie (el Naive es muy fuerte fuera de
> periodos de shock, MASE=1.0).

#### Reentrenamiento extendido (2019–2025, escala física, n_train≈62)

| Modelo | MAE_68 | RMSE_68 | R²_68 | n_shocks | MAE_shock | Deterioro |
|--------|--------|---------|-------|----------|-----------|-----------|
| XGBoost_final | 0.6253 | 0.7579 | **+0.282** | 4 | 0.8178 | +30.8% |
| GM_v3_final | 0.6353 | 0.7989 | -7.258 | 1 | 0.9493 | +49.4% |
| GE_final | 0.8833 | 0.9647 | -0.163 | 4 | 0.8444 | -4.4% |

### Archivos de resultados generados

**Métricas (JSON):**
- `resultados/gc1/gc1_sarima_metricas.json`
- `resultados/gc1/gc1_prophet_metricas.json`
- `resultados/gc2/gc2_metricas.json`
- `resultados/ge/ge_metricas.json`
- `resultados/gm/gm_metricas.json`
- `resultados/gm_v2/gm_v2_metricas.json`
- `resultados/gm_v3/gm_v3_metricas.json`
- `resultados/gm_v4/gm_v4_metricas.json`
- `resultados/xgboost/xgb_metricas.json`
- `resultados/tcn/tcn_metricas.json`
- `resultados/ge_ext/metricas.json`, `resultados/ge_final/metricas.json`
- `resultados/gm_v3_ext/metricas.json`, `resultados/gm_v3_final/metricas.json`
- `resultados/xgboost_ext/metricas.json`, `resultados/xgboost_final/metricas.json`
- `resultados/comparativa_extendido.json`
- `resultados/evaluacion_shocks_extendido.json`
- `resultados/ranking_final_44vs68.json`
- `resultados/shap/shap_resultados.json`
- `resultados/validacion/validacion_estadistica.json`

**Predicciones (CSV):**
- `resultados/gc1/gc1_sarima_predicciones.csv`, `gc1_prophet_predicciones.csv`
- `resultados/gc2/gc2_predicciones.csv`
- `resultados/ge/ge_predicciones.csv`, `ge/ge_training_history.csv`
- `resultados/gm/gm_predicciones.csv`, `gm/gm_training_history.csv`
- `resultados/gm_v2/gm_v2_predicciones.csv`
- `resultados/gm_v3/gm_v3_predicciones.csv`
- `resultados/gm_v4/gm_v4_predicciones.csv`
- `resultados/xgboost/xgb_predicciones.csv`
- `resultados/tcn/tcn_predicciones.csv`
- `resultados/ge_ext/predicciones.csv`, `resultados/ge_final/predicciones.csv`
- `resultados/gm_v3_ext/predicciones.csv`, `resultados/gm_v3_final/predicciones.csv`
- `resultados/xgboost_ext/predicciones.csv`, `resultados/xgboost_final/predicciones.csv`
- `resultados/validacion/resumen_validacion.csv`

**Scalers / artefactos:**
- `resultados/scaler_extendido.json`, `scaler_extendido_v2.json`, `scaler_reconstruido.json`

### Tiempo estimado de ejecución
- GM / GM_v2 / GM_v3 / GM_v4 (Dual-LSTM + NLP + PCA, ~15–33 épocas): **~5–10 min** cada uno.
- XGBoost (grid pequeño): **< 2 min.** TCN: **~2–3 min.**
- SHAP (KernelExplainer, nsamples=200): **~5–15 min** (cuello de botella).
- Reentrenamientos extendidos + validación: **~15–20 min.**
- Simulaciones El Niño / impacto económico: **~5–10 min.**
- **Total ≈ 45–75 min.**

---

### Notas de reproducibilidad

- Cada notebook `*_ejecutado.ipynb` contiene los outputs guardados de su fuente homónima.
- Modelos **no persistidos**: XGBoost (retrain desde `actividad_15v3`), GM_v3 (Lambda layer no deserializa → rebuild + `load_weights`).
- Scalers GM_v3 (scaler_s, scaler_n, scaler_y) y PCA no guardados → refit determinista con `random_state=42` sobre el mismo split.
- Todas las métricas z-score provienen de la serie normalizada de Fase 2 (media provincial).
