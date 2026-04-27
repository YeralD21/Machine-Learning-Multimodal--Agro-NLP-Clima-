"""
==========================================================================
Fase 2 del Pipeline: NLP Sentiment + Rezagos Temporales (Lags)
==========================================================================
1. Carga las noticias unificadas de Agraria.pe.
2. Calcula el sentimiento con pysentimiento (modelo BETO español).
3. Agrega el sentimiento por mes (promedio mensual nacional).
4. Lo une al dataset maestro de Fase 1.
5. Genera rezagos temporales (t-1, t-3, t-6) por provincia.
6. Escala SOLO las nuevas columnas NLP/lags.
7. Exporta dataset_fase2_multivariado.csv

Entrada:  data/processed/master_dataset_fase1.csv
          data/interim/agraria/noticias_unificadas_2021_2025.csv
Salida:   data/processed/dataset_fase2_multivariado.csv
          models/scalers/scaler_fase2_nlp.pkl
"""

import os
import sys
import warnings
import joblib
import numpy as np
import pandas as pd
from sklearn.preprocessing import StandardScaler

warnings.filterwarnings("ignore")


# =========================================================================
# Paso 1: Cuantificación de Sentimiento NLP
# =========================================================================
def compute_sentiment(df_news: pd.DataFrame) -> pd.DataFrame:
    """
    Analiza el sentimiento de cada noticia con pysentimiento (BETO).
    Devuelve el DataFrame con una columna 'sentiment_score' en [-1, 1].
    
    Estrategia de puntuación:
      score = P(POS) - P(NEG)
    Esto da un continuo en [-1, 1] donde:
      -1 = totalmente negativo
       0 = neutral
      +1 = totalmente positivo
    """
    from pysentimiento import create_analyzer

    print("       Cargando modelo de sentimiento (BETO español)...")
    print("       (primera ejecución descarga ~500MB del modelo)")
    analyzer = create_analyzer(task="sentiment", lang="es")

    scores = []
    total = len(df_news)
    
    for i, row in df_news.iterrows():
        # Usamos el titular porque es más conciso y representativo
        # del tono de la noticia. El cuerpo completo puede ser muy largo
        # y diluir la señal de sentimiento.
        text = str(row['titular'])[:512]  # Truncar a 512 tokens max
        
        try:
            result = analyzer.predict(text)
            # result.probas es un dict: {'POS': 0.x, 'NEG': 0.y, 'NEU': 0.z}
            p_pos = result.probas.get('POS', 0)
            p_neg = result.probas.get('NEG', 0)
            score = p_pos - p_neg  # Rango [-1, 1]
        except Exception:
            score = 0.0  # Neutral como fallback
        
        scores.append(score)
        
        # Progreso cada 50 noticias
        if (i + 1) % 50 == 0 or (i + 1) == total:
            print(f"       Procesadas: {i + 1}/{total} noticias")

    df_news['sentiment_score'] = scores
    return df_news


def aggregate_sentiment_monthly(df_news: pd.DataFrame) -> pd.DataFrame:
    """
    Agrega el sentimiento por mes (YYYY-MM).
    Las noticias son nacionales, así que generamos un indicador
    mensual que se propagará a todas las provincias.
    """
    df_news['fecha'] = pd.to_datetime(df_news['fecha'], errors='coerce')
    df_news['fecha_evento'] = df_news['fecha'].dt.strftime('%Y-%m')

    df_monthly = (
        df_news
        .groupby('fecha_evento')
        .agg(
            nlp_sentiment=('sentiment_score', 'mean'),
            nlp_news_count=('sentiment_score', 'count')
        )
        .reset_index()
    )

    # Redondear
    df_monthly['nlp_sentiment'] = df_monthly['nlp_sentiment'].round(4)
    
    return df_monthly


# =========================================================================
# Paso 2: Generación de Rezagos Temporales (Lags)
# =========================================================================
def create_lags(df: pd.DataFrame) -> pd.DataFrame:
    """
    Genera rezagos de 1, 3 y 6 meses para produccion_t y nlp_sentiment,
    agrupando por provincia para no mezclar series de diferentes regiones.
    """
    lag_periods = [1, 3, 6]
    lag_columns = ['produccion_t', 'nlp_sentiment']

    df = df.sort_values(['provincia', 'fecha_evento']).copy()

    for col in lag_columns:
        for lag in lag_periods:
            new_col_name = f"{col}_lag{lag}"
            df[new_col_name] = (
                df.groupby('provincia')[col]
                .shift(lag)
            )

    # Eliminar filas con NaN generados por los shifts
    lag_cols_created = [
        f"{col}_lag{lag}"
        for col in lag_columns
        for lag in lag_periods
    ]
    rows_before = len(df)
    df = df.dropna(subset=lag_cols_created)
    rows_after = len(df)
    print(f"       Filas eliminadas por NaN de rezagos: {rows_before - rows_after}")
    print(f"       Filas restantes: {rows_after}")

    return df, lag_cols_created


# =========================================================================
# Paso 3: Escalado Exclusivo de Nuevas Variables
# =========================================================================
def scale_new_features(df: pd.DataFrame, cols_to_scale: list) -> pd.DataFrame:
    """
    Aplica StandardScaler SOLO a las nuevas columnas de NLP y rezagos.
    Guarda el scaler parcial para desnormalización futura.
    """
    scaler = StandardScaler()
    df[cols_to_scale] = scaler.fit_transform(df[cols_to_scale])

    scaler_path = os.path.join("models", "scalers", "scaler_fase2_nlp.pkl")
    os.makedirs(os.path.dirname(scaler_path), exist_ok=True)
    joblib.dump(scaler, scaler_path)
    print(f"       Scaler guardado en: {scaler_path}")
    print(f"       Variables escaladas: {cols_to_scale}")

    return df


# =========================================================================
# Pipeline Principal
# =========================================================================
def run_fase2():
    print("=" * 70)
    print("  PIPELINE FASE 2: NLP Sentiment + Rezagos Temporales")
    print("=" * 70)

    # --- Cargar datos ---
    print("\n[0/4] Cargando datasets...")
    df_master = pd.read_csv("data/processed/master_dataset_fase1.csv")
    df_news = pd.read_csv("data/interim/agraria/noticias_unificadas_2021_2025.csv")
    print(f"       Dataset Fase 1: {df_master.shape}")
    print(f"       Noticias:       {df_news.shape}")

    # --- Paso 1: NLP ---
    print("\n[1/4] Analizando sentimiento de noticias con BETO...")
    df_news = compute_sentiment(df_news)

    # Guardar noticias con scores intermedios para trazabilidad
    df_news.to_csv(
        "data/interim/agraria/noticias_con_sentimiento.csv",
        index=False, encoding='utf-8'
    )
    print("       Noticias con sentimiento guardadas en interim/agraria/")

    # Agregar por mes
    print("\n       Agregando sentimiento por mes...")
    df_sentiment = aggregate_sentiment_monthly(df_news)
    print(f"       Meses con datos de sentimiento: {len(df_sentiment)}")
    print(f"       Rango: {df_sentiment['fecha_evento'].min()} -> "
          f"{df_sentiment['fecha_evento'].max()}")
    print(f"       Sentimiento promedio global: "
          f"{df_sentiment['nlp_sentiment'].mean():.4f}")

    # --- Unir al dataset maestro ---
    print("\n[2/4] Uniendo sentimiento al dataset maestro...")
    df_master = pd.merge(
        df_master, df_sentiment,
        on='fecha_evento', how='left'
    )
    # Meses sin noticias -> sentimiento neutral (0)
    df_master['nlp_sentiment'] = df_master['nlp_sentiment'].fillna(0)
    df_master['nlp_news_count'] = df_master['nlp_news_count'].fillna(0)
    print(f"       Dataset tras unión NLP: {df_master.shape}")

    # --- Paso 2: Lags ---
    print("\n[3/4] Generando rezagos temporales (t-1, t-3, t-6)...")
    df_master, lag_cols = create_lags(df_master)

    # --- Paso 3: Escalar nuevas variables ---
    print("\n[4/4] Escalando nuevas variables NLP y rezagos...")
    new_cols_to_scale = ['nlp_sentiment', 'nlp_news_count'] + lag_cols
    df_master = scale_new_features(df_master, new_cols_to_scale)

    # --- Exportar ---
    output_path = "data/processed/dataset_fase2_multivariado.csv"
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df_master.to_csv(output_path, index=False, encoding='utf-8')

    # --- Reporte Final ---
    print("\n" + "=" * 70)
    print("  REPORTE FINAL - FASE 2")
    print("=" * 70)
    print(f"  Dimensiones finales   : {df_master.shape}")
    print(f"  Columnas totales      : {df_master.columns.tolist()}")
    print(f"  Provincias            : {df_master['provincia'].nunique()}")
    print(f"  Rango temporal        : {df_master['fecha_evento'].min()} -> "
          f"{df_master['fecha_evento'].max()}")
    print(f"  Archivo guardado      : {output_path}")
    print("=" * 70)

    print("\n  Primeras 5 filas del dataset final:")
    print(df_master.head(5).to_string(index=False))


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding='utf-8')
    run_fase2()
