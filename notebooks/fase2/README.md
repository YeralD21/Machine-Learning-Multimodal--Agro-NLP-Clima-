# Fase 2 — Módulo NLP e Ingeniería de Características Multimodal

**Proyecto:** Predicción de Producción de Limón en el Perú  
**Modelo:** LSTM-Attention Multimodal (HA-LSTM)  
**Base:** `pipeline/output/09_etl/master_dataset_raw_values.csv`

---

## Objetivo

Transformar la información no estructurada en indicadores matemáticos y estructurar
el dataset para capturar dependencias temporales, preparándolo para la arquitectura
de aprendizaje profundo HA-LSTM.

## Actividades

| # | Notebook | Descripción | Entregable |
|---|----------|-------------|------------|
| 1 | `actividad_01_nlp_sentimiento.ipynb` | Cuantificación de sentimiento con BETO | `avg_sentiment` [-1, 1] |
| 2 | `actividad_02_codificacion_ciclica.ipynb` | month_sin, month_cos, lat, lon | Variables cíclicas y geográficas |
| 3 | `actividad_03_rezagos_temporales.ipynb` | Lags t-1, t-3, t-6 | Variables de memoria histórica |
| 4 | `actividad_04_normalizacion_final.ipynb` | Escalado final de todas las variables | scaler_fase2.pkl |
| 5 | `actividad_05_dataset_final.ipynb` | Ensamblaje y validación del dataset final | dataset_fase2_multivariado.csv |

## Flujo de datos

```
master_dataset_raw_values.csv (Fase 1 — 5,880 filas × 17 cols)
        ↓ Actividad 1
+ avg_sentiment (BETO)
        ↓ Actividad 2
+ month_sin, month_cos, lat, lon
        ↓ Actividad 3
+ lags t-1, t-3, t-6 (reducción a ~5,250 filas)
        ↓ Actividad 4
Normalización final (MinMaxScaler)
        ↓ Actividad 5
dataset_fase2_multivariado.csv (~30 variables)
```

## Variables del dataset final

| Grupo | Variables |
|-------|-----------|
| Llaves | fecha_evento, departamento, provincia |
| MIDAGRI | produccion_t, cosecha_ha, precio_chacra_kg |
| INDECI | num_emergencias, total_afectados |
| NASA | T2M, T2M_MAX, T2M_MIN, PRECTOTCORR, RH2M, QV2M, ALLSKY_SFC_SW_DWN, WS2M |
| NLP | n_noticias, avg_sentiment |
| Tiempo | month_sin, month_cos |
| Geografía | lat, lon |
| Lags producción | produccion_t_lag1, produccion_t_lag3, produccion_t_lag6 |
| Lags sentimiento | avg_sentiment_lag1, avg_sentiment_lag3 |
| Lags emergencias | num_emergencias_lag1, num_emergencias_lag3 |
| Lags clima | T2M_lag1, T2M_lag3, PRECTOTCORR_lag1, PRECTOTCORR_lag3 |
