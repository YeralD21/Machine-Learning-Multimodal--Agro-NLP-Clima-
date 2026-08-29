# Verificación completa de Δ_s — recálculo del deterioro en meses de shock

## Hallazgo crítico

Las métricas de shock reportadas en el paper y en los notebooks
(`actividad_15v3_xgboost_competidor.ipynb`, `actividad_15v4_tcn_competidor.ipynb`,
`resultados/validacion/validacion_estadistica.json`) se calcularon con un **error
de escala**:

```python
# Notebook original (actividad_15v3, celda de análisis de shocks)
df_model['variacion_pct'] = df_model[TARGET].pct_change().abs() * 100
idx_shock = df_test_copy[df_test_copy['variacion_pct'] > 20].index.tolist()
```

`TARGET` es `produccion_t` en **escala z-score** (media 0, std 1). Aplicar
`pct_change()/` entre valores z cerca de cero genera variaciones sintéticas de
cientos de por ciento. Ejemplo comprobado en la ventana de test:

| Mes | Variación z-score (bug) | Variación real desnormalizada (ton) |
|---|---|---|
| 2025-01 | **+1021.4%** | **+10.19%** |
| 2024-09 | +229.9% | 1.27% |
| 2024-12 | +141.3% | 3.53% |

Con el umbral de shock del paper (`>20%`) la máscara marcaba **8 de 12** meses de
test como "shock", todos ficticios. Al desnormalizar la serie real a toneladas con
`notebooks/fase2/scalers/standard_scaler_fase2.joblib`, la variación mensual
absoluta máxima real en el periodo de test es **10.19% (enero 2025)**, por lo que
el mismo umbral `>20%` deja **0 meses de shock**.

## Método de recálculo

1. **Desnormalización.** La serie real de cada modelo (`real` en los CSVs de
   predicción) es la media mensual nacional de `produccion_t` en z-score. Se
   convierte a toneladas con `desnormalizar(z) = z·scale + mean` usando el
   `standard_scaler_fase2.joblib` (mean=16.3198, scale=27.9869 para
   `produccion_t`). La alineación se verificó: `max|real_guardada − serie_mestra| = 8.3e-17`.

2. **Distribución de variación real** (`|pct_change|` en toneladas):
   - **Ventana test** (sep-2024 → ago-2025, n=12): media 2.96%, std 2.68%,
     P50 2.17%, **P75 3.67%**, P90 4.99%, máx 10.19%.
   - **Serie completa** (2021-01 → 2025-08, n=55): media 4.56%, std 3.64%,
     P75 6.44%, P90 9.85%, máx 14.89%.

3. **Criterio de shock empírico.** Mes de shock = variación mensual absoluta
   (toneladas) **> P75 de la ventana de test (3.67%)**. Meses marcados
   **2025-01, 2025-05, 2025-06** (N=3): el cuartil más volátil del periodo de
   prueba. Este criterio ancla el umbral en la distribución real observada, no en
   la escala z-score. (Sensibilidad: P90 → 2025-01 y 2025-06; top-4 → +2024-12;
   P75 serie completa → solo 2025-01.)

4. **Δ_s** = `(MAE_shock − MAE_global) / MAE_global × 100`, con `MAE_global`
   sobre todo el test de cada modelo y `MAE_shock` sobre su intersección con la
   máscara de shock.

## Tabla 1 oficial (recalculada)

| Modelo | MAE_global | MAE_shock | Δ_s (%) | N_shock | N_test |
|---|---|---|---|---|---|
| Naive | 0.016065 | 0.035483 | **+120.87** | 3 | 12 |
| SARIMA (GC1) | 0.100639 | 0.145819 | **+44.89** | 3 | 12 |
| Prophet (GC1) | 0.091884 | 0.128605 | **+39.97** | 3 | 12 |
| SARIMAX+LSTM (GC2) | 0.196891 | 0.277364 | **+40.87** | 3 | 12 |
| XGBoost | 0.047104 | 0.079525 | **+68.83** | 3 | 10 |
| GE | 0.067272 | 0.078599 | **+16.84** | 3 | 10 |
| GM_v3 | 0.064500 | 0.059273 | **−8.10** | 2 | 6 |

Los `MAE_global` reproducen exactamente los valores publicados del paper
(Naive 0.0161, SARIMA 0.1006, Prophet 0.0919, GC2 0.1969, XGBoost 0.0471,
GE 0.0673, GM_v3 0.0645), lo que confirma que el recálculo opera sobre los
mismos artefactos oficiales.

## Comparación con los valores del paper (errados)

| Modelo | Paper (z-score) | Recalculado (toneladas) |
|---|---|---|
| GE | +2.3% | **+16.84%** |
| GM_v3 | +11.7% | **−8.10%** |
| XGBoost | +16.5% | **+68.83%** |

### Interpretación

- El deterioro en shocks reales es **mucho mayor** que el reportado para GE y
  XGBoost: la robustez del modelo estructural en volatilidad había sido
  sobreestimada por el umbral de 20% sobre z-score (que inflaba el denominador
  del MAE_global con shocks ficticios).
- Con el criterio empírico, **GE sigue siendo el dual-LSTM más robusto**
  (Δ_s +16.8%), frente a +68.8% de XGBoost: se mantiene la conclusión de que la
  arquitectura LSTM-Attention degrada menos en shocks, pero la magnitud de la
  ventaja es ~4× menor de lo publicado.
- **GM_v3 muestra Δ_s negativo (−8.1%)**: su MAE en los shocks (2025-05, 2025-06)
  es menor que su MAE global. Se interpreta con cautela: GM_v3 solo tiene
  N_test=6 (mar-2025 → ago-2025) porque `make_seq` descarta los primeros
  `TIMESTEPS=6` meses del test, por lo que su máscara queda con N_shock=2 y su
  ventana no incluye el epicentro de volatilidad (2025-01).

### Advertencias / limitaciones

1. **Ventanas de test heterogéneas.** GC1/GC2/Naive evalúan sep-2024 → ago-2025
   (12 m); XGBoost y GE nov-2024 → ago-2025 (10 m); GM_v3 mar-2025 → ago-2025
   (6 m). La columna `N_test` documenta esto en `tabla1_oficial.csv`.
2. El `validacion_estadistica.json` previo (RMSE, diebold-mariano) usó el mismo
   umbral erróneo (`shock_threshold: 0.2`, `eps_denominador: 0.05`) y 6 meses de
   shock ficticios; sus valores no son comparables con esta tabla.
3. Los meses de shock se definen ex-post sobre la serie real observada. El
   criterio P75 es un binning descriptivo de volatilidad, no un umbral causal.

## Archivos en este directorio

| Archivo | Contenido |
|---|---|
| `verificar_delta_s.py` | Script reproducible de la auditoría: carga el master reconstruido y el scaler, desnormaliza, calcula la distribución, define la máscara P75 y calcula MAE_global / MAE_shock / Δ_s por modelo. |
| `distribucion_variacion.csv` | Serie real desnormalizada (toneladas) y variación mensual por mes de test, con flag de shock P75. |
| `output_ejecucion.txt` | Output completo de la ejecución. |
| `tabla1_oficial.json` | Metadatos de la misma tabla en JSON (incluye máscara, umbrales y scripts fuente). |
| `../delta_s_recalculado/tabla1_oficial.csv` | **Tabla 1 oficial** para el paper: `[Modelo, MAE_global, MAE_shock, Delta_s_pct, N_meses_shock, N_test, shock_meses]`. |

## Fuente del dataset

`produccion_t` proviene de `master_dataset_fase2_multivariado_RECONSTRUIDO.csv`
(en `resultados/verificacion_simulacion_nino_2026/`), reconstruido de forma
determinística desde `data/processed/master_dataset_fase1.csv` siguiendo
`notebooks/fase2/actividad_02_cyclic_time_encoding.ipynb` (56 meses,
2021-01 → 2025-08, n_train=44/n_test=12 — igual que el output guardado de
`actividad_15v5_ejecutado.ipynb`). La columna `real` de los CSVs de predicción
coincide con la media nacional de esa serie (diferencia < 1e-16).

## Cómo reproducir

```powershell
$env:PYTHONIOENCODING='utf-8'
& "C:\Users\YERALD\AppData\Local\Programs\Python\Python311\python.exe" `
  resultados\verificacion_delta_s_completa\verificar_delta_s.py
```

Requiere Python 3.11 con `pandas`, `numpy`, `scikit-learn`, `joblib`
(todos ya presentes en el entorno). No modifica ningún archivo de
`resultados/*` existente ni archivos `.tex`.