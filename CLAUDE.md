# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Multi-crop demand forecasting system for Peru's agro-industrial sector (primary target: lime/limón). Integrates four heterogeneous data sources — MIDAGRI (crop statistics), NASA POWER (climate), INDECI (disasters), Agraria.pe (news) — through a 10-activity ETL pipeline into a Star Schema DWH, then applies NLP (RoBERTuito sentiment) and LSTM-Attention for demand/price prediction with SHAP explainability.

**Python version: strictly 3.11** (TensorFlow is incompatible with 3.13+). The system may have multiple Python versions installed; always use the venv.

## Running the Project

```powershell
# Activate virtualenv (Windows) — ALWAYS use this, not system Python
.\venv\Scripts\Activate.ps1

# Or invoke directly without activating
venv\Scripts\python.exe <script.py>

# Quick demo (Agro + Weather + Features + News scraping)
python main.py

# Full Phase 1 pipeline (all 10 ETL activities)
python main_fase1.py

# Run a specific activity (1–10)
python main_fase1.py --actividad 3

# NASA-specific pipeline
python main_nasa_pipeline.py
```

Notebooks in `pipeline/` are the primary execution environment. Run them sequentially: `actividad_01_*.ipynb` → `actividad_10_*.ipynb`.

## Architecture

### Four-Phase Design

**Phase 1 — Data Engineering** (`pipeline/`, `src/data_processing/`):
10 sequential activities: environment setup → data loading (4 sources) → EDA → quality audit → cleaning → DWH integration → Star Schema design → PostgreSQL creation → ETL → re-exploration.

**Phase 2 — Feature Engineering + NLP** (`notebooks/fase2/`, `src/`):
Sentiment analysis via pysentimiento's RoBERTuito (`pysentimiento/robertuito-sentiment-analysis`, sentiment head over RoBERTuito) → cyclic temporal encoding (sin/cos) → lag features (t-1, t-3, t-6) → normalization. Output: 24-column master dataset in `data/processed/master_dataset_fase2_multivariado.csv`. Configuration in `notebooks/fase2/config/fase2_config.json`.

**Phase 3 — Model Training** (`notebooks/fase4/actividad_15*.ipynb`):
Multiple model architectures trained and compared. Each produces metrics, predictions, and artifacts in `resultados/<model>/`.

**Phase 4 — Analysis & Simulation** (`notebooks/fase4/actividad_16*.ipynb`, `notebooks/fase4/actividad_17*.ipynb`, `notebooks/fase4/simulacion_*.ipynb`, `notebooks/fase4/escenario_*.ipynb`):
SHAP explainability, shock analysis, retraining with extended data, economic impact simulations, El Niño scenario analysis.

### Model Naming Convention

| Code | Full Name | Architecture | Key Feature |
|---|---|---|---|
| GC1 | Grupo Clásico 1 | SARIMA, Prophet | Statistical baselines |
| GC2 | Grupo Clásico 2 | SARIMAX+LSTM | Hybrid classical+DL |
| **GE** | Grupo Estructural | Dual-LSTM + BahdanauAttention | 23 exogenous features, no NLP |
| GM | Grupo Multimodal | Dual-LSTM + BahdanauAttention + NLP | 25 features (GE + avg_sentiment, n_noticias_beto) |
| **GM_v2** | GM improved | Same arch, NLP modules M1-M4 | nlp_index, lag1, dropout, PCA |
| **GM_v3** | GM corpus ampliado | LSTM + Attention + PCA + NLP | Multi-source NLP corpus (600 noticias) |
| XGBoost | Competidor | XGBoost regressor | 29 features (EXOG + lags + rolling stats) |
| TCN | Competidor | Temporal Convolutional Network | Causal convolutions |

Suffixes in `resultados/`: `_ext` = retrained on extended 2019-2025 data (n_train=68), `_final` = final version.

### Loading Trained Models

**GE model** requires custom layer definition:
```python
class BahdanauAttention(keras.layers.Layer):
    def __init__(self, units, **kwargs):
        super().__init__(**kwargs)
        self.units = units
        # ... W_query, W_values, V dense layers
model = keras.models.load_model(path, custom_objects={'BahdanauAttention': BahdanauAttention})
```

**GM_v3 model** uses a Lambda layer for `tf.reduce_sum` that fails to deserialize. Rebuild the architecture and load weights:
```python
gmv3_model = build_gm_v3(ss, ns)  # rebuild architecture
gmv3_model.load_weights('resultados/gm_v3/gm_v3_model.keras')
```

**XGBoost model** was NOT saved to disk. Retrain from `notebooks/fase4/actividad_15v3_xgboost_competidor.ipynb` using `best_params = {max_depth: 2, n_estimators: 200, learning_rate: 0.05, subsample: 0.8, colsample_bytree: 0.8}`.

### Scalers and Normalization

The master dataset contains z-scored values for numeric features (produccion_t, clima, INDECI, precio) and raw values for geographic/temporal features (lat, lon, month_sin, month_cos, mes_num, trimestre_*).

| Scaler | Location | n_train | Use |
|---|---|---|---|
| Fase 2 original | `notebooks/fase2/scalers/standard_scaler_fase2.joblib` | varies | Desnormalize master dataset z-scores to physical units |
| GE model scalers | `resultados/ge/ge_scalers.pkl` | 40 | scaler_a (target), scaler_b (Canal B 23 features) |
| GM model scalers | `resultados/gm/gm_scalers.pkl` | — | scaler_a, scaler_b (25 features) |
| Extended scalers | `resultados/scaler_extendido*.json` | 68 | For extended 2019-2025 models |
| Reconstructed | `resultados/scaler_reconstruido.json` | 68 | JSON format with target_mean/target_scale |

GM_v3 scalers (scaler_s, scaler_n, scaler_y) and PCA were NOT saved. Reconstruct by re-fitting on the same training split (deterministic with same data + `random_state=42`).

### Core Classes (`src/`)

| Module | Class | Responsibility |
|---|---|---|
| `src/agro/processor.py` | `AgroProcessor` | Load MIDAGRI CSV, filter by crop, IQR outlier removal, validate ≥24 months |
| `src/weather/processor.py` | `WeatherProcessor` | Load NASA/SENAMHI CSV, standardize district names, impute missing values |
| `src/features/builder.py` | `FeatureBuilder` | Merge datasets by ANHO+MES+UBIGEO, generate lag features, add cyclic seasonality |
| `src/models/lstm_attention.py` | `LSTMDemandForecaster` | Build/train/predict LSTM+Attention Keras model |
| `src/scraping/news_scraper.py` | `NewsScraper` | Ethical scraping of Agraria.pe with robots.txt compliance and random delays |

### Code Organization Rule

All new core logic must be **class-based and placed in the appropriate `src/` subdirectory**. Notebooks (`pipeline/`, `notebooks/`) are sandboxes only.

### Data Flow

```
sources/ (raw CSVs)
  → pipeline/actividad_*.ipynb  (ETL, cleaning, DWH)
  → data/processed/             (master dataset)
  → notebooks/fase2/            (NLP, lags, scaling)
  → notebooks/fase4/            (model training, SHAP, simulations)
  → resultados/                 (trained models, predictions, metrics)
  → dashboard/                  (Streamlit/HTML frontend)
```

### Notebook Conventions

- `actividad_15v5_gm_nlp_v2.ipynb` — source notebook (generated from scripts)
- `actividad_15v5_ejecutado.ipynb` — same notebook with executed outputs saved
- Notebooks are generated programmatically: `python generate_notebooks.py` (Phase 1), `python gen_nb_fase2.py` (Phase 2)

### Pipeline Configuration

`pipeline/config/pipeline_config.json` is generated by Activity 1 and controls directory mappings, time range (`ANHO_INICIO`/`ANHO_FIN`), and `CULTIVO_TARGET`.

`notebooks/fase2/config/fase2_config.json` controls Phase 2: NLP model path, lag values `[1, 3, 6]`, variables to lag, scaler paths, and 5-stage output pipeline.

### Database

`database/` contains PostgreSQL DDL for the Star Schema DWH:
- `dwh_star_schema_v2.sql` — current version; fact table `hechos_produccion` + dimension tables
- `dwh_nasa_clima_schema.sql` — climate-specific schema

### Dashboard

`dashboard/app.py` — Streamlit app with landing, results, SHAP, attention, NLP, and simulation pages. HTML templates in `dashboard/*.html`.

## Key Dependencies

```
tensorflow          # LSTM-Attention model
pysentimiento       # RoBERTuito sentiment analyzer (pysentimiento/robertuito-sentiment-analysis)
transformers        # backend de pysentimiento (RoBERTa/BERT en español)
scikit-learn        # preprocessing, metrics, PCA
pandas / numpy      # data manipulation
shap                # model explainability
xgboost             # XGBoost competitor model
streamlit / plotly  # dashboard
beautifulsoup4 / selenium  # web scraping
```

Install: `pip install -r requirements.txt` (no pinned versions)

## Data Sources

| Source | Format | Path |
|---|---|---|
| MIDAGRI (Sisagri) | CSV | `sources/midagri/` (>100MB, gitignored — download separately) |
| NASA POWER | CSV | `sources/nasa/` / `pipeline/fuentes/nasa/` |
| INDECI (SINPAD) | CSV | `pipeline/fuentes/indeci/` |
| Agraria.pe | Scraped HTML | `pipeline/fuentes/agraria-pe/` |

Sample data files are committed in `data/` for reference.

## Model Performance Metrics (2026-06-21)

### Test Set Results (modelos originales, n_train~44)

| Model | MAE | Notes |
|---|---|---|
| XGBoost | 0.0471 | Best absolute MAE, lag-dominated |
| **GM_v3** | **0.0645** | NLP corpus ampliado, M1+M2+M3+M4+PCA |
| GM_v2 | 0.0646 | NLP original (528 noticias) |
| GE | 0.0673 | Dual-LSTM sin NLP (23 features) |
| Naive | 0.0161 | Baseline (high autocorrelation in non-shock periods) |

### Climate Sensitivity (from El Niño simulation)

| Model | Sensitivity | Mechanism |
|---|---|---|
| GM_v3 | 22% | Clima → PCA → LSTM + NLP |
| GE | 11% | Direct climate access in Canal B × 6 timesteps |
| XGBoost | 5% | Lag-dominated, climate features low importance |

> XGBoost leads in MAE but has lowest climate sensitivity. GM_v3 is the recommended model for the system because it captures both temporal dependencies and external shocks via NLP+climate integration.
