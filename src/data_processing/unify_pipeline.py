import pandas as pd
import numpy as np
import os
import glob
from textblob import TextBlob
from datetime import datetime

# Configuración de rutas
DATA_RAW_NEWS = 'data/raw/agraria_pe/'
DATA_INTERIM_MIDAGRI = 'data/interim/midagri/midagri_limon_procesado.csv'
DATA_INTERIM_INDECI = 'data/interim/indeci/indeci_temporal_2021_2025.csv'
DATA_INTERIM_NASA = 'data/interim/nasa/clima_dataset_final.csv'
DATA_PROCESSED = 'data/processed/master_dataset_lstm.csv'

def get_sentiment(text):
    """Calcula el sentimiento de un texto. 
    Nota: TextBlob es nativo para inglés. Para español, lo ideal sería traducir 
    o usar un modelo específico, pero usaremos el estándar por ahora."""
    if pd.isna(text) or text == "":
        return 0
    try:
        # Intentamos un análisis básico
        return TextBlob(str(text)).sentiment.polarity
    except:
        return 0

def process_news():
    print("Procesando noticias de agraria_pe...")
    news_files = glob.glob(os.path.join(DATA_RAW_NEWS, 'agro_news_*.csv'))
    all_news = []
    
    for f in news_files:
        df = pd.read_csv(f)
        all_news.append(df)
        
    df_news = pd.concat(all_news, ignore_index=True)
    
    # Combinar titular y cuerpo para mejor contexto
    df_news['full_text'] = df_news['titular'].fillna('') + " " + df_news['cuerpo_completo'].fillna('')
    
    # Calcular sentimiento
    print("Calculando scores de sentimiento (esto puede tardar un poco)...")
    df_news['sentiment_score'] = df_news['full_text'].apply(get_sentiment)
    
    # Convertir fecha a YYYY-MM
    df_news['date'] = pd.to_datetime(df_news['fecha']).dt.strftime('%Y-%m')
    
    # Agrupar por mes y calcular promedio
    df_monthly_news = df_news.groupby('date')['sentiment_score'].mean().reset_index()
    
    print(f"Noticias procesadas: {len(df_monthly_news)} meses encontrados.")
    return df_monthly_news

def load_and_standardize():
    print("Cargando y estandarizando datasets interim...")
    
    # Cargar MIDAGRI
    df_midagri = pd.read_csv(DATA_INTERIM_MIDAGRI)
    df_midagri.rename(columns={'fecha_evento': 'date'}, inplace=True)
    
    # Cargar INDECI
    df_indeci = pd.read_csv(DATA_INTERIM_INDECI)
    df_indeci.rename(columns={'fecha_evento': 'date'}, inplace=True)
    
    # Cargar NASA
    df_nasa = pd.read_csv(DATA_INTERIM_NASA)
    # Convertir DATE (YYYY-MM-DD) a YYYY-MM
    df_nasa['date'] = pd.to_datetime(df_nasa['DATE']).dt.strftime('%Y-%m')
    # Eliminar columnas cíclicas previas de NASA para recalcularlas luego
    cols_to_drop = ['month_sin', 'month_cos', 'DATE']
    df_nasa.drop(columns=[c for c in cols_to_drop if c in df_nasa.columns], inplace=True)
    
    # Estandarizar nombres de columnas a minúsculas
    for df in [df_midagri, df_indeci, df_nasa]:
        df.columns = [c.lower() for c in df.columns]
        
    return df_midagri, df_indeci, df_nasa

def apply_cyclic_encoding(df, date_col='date'):
    print("Aplicando codificación cíclica universal...")
    # Convertir a datetime para extraer mes y día
    temp_date = pd.to_datetime(df[date_col])
    
    # Mes (1-12)
    df['month_sin'] = np.sin(2 * np.pi * temp_date.dt.month / 12)
    df['month_cos'] = np.cos(2 * np.pi * temp_date.dt.month / 12)
    
    # Día (1-31) - Aunque sea mensual, el día suele ser 1, pero lo incluimos por robustez
    df['day_sin'] = np.sin(2 * np.pi * temp_date.dt.day / 31)
    df['day_cos'] = np.cos(2 * np.pi * temp_date.dt.day / 31)
    
    return df

def main():
    # 1. Procesar noticias
    df_news = process_news()
    
    # 2. Cargar otros datos
    df_midagri, df_indeci, df_nasa = load_and_standardize()
    
    # 3. Unir datasets (Master Merge)
    print("Iniciando unión de datasets...")
    
    # Empezamos con MIDAGRI como base (producción/precios)
    master_df = df_midagri.merge(df_nasa, on=['date', 'departamento', 'provincia'], how='left')
    
    # Unir con INDECI
    master_df = master_df.merge(df_indeci, on=['date', 'departamento', 'provincia'], how='left')
    
    # Unir con Noticias (solo por fecha, ya que las noticias suelen ser nacionales/regionales generales)
    master_df = master_df.merge(df_news, on='date', how='left')
    
    # 4. Limpieza post-merge
    # Rellenar con 0 las emergencias si no hay datos (asumimos que no hubo)
    emergencia_cols = ['num_emergencias', 'total_afectados', 'hectareas_cultivo_perdidas']
    for col in emergencia_cols:
        if col in master_df.columns:
            master_df[col] = master_df[col].fillna(0)
            
    # Rellenar sentimiento con 0 (neutral) si no hay noticias
    master_df['sentiment_score'] = master_df['sentiment_score'].fillna(0)
    
    # Eliminar duplicados
    master_df.drop_duplicates(inplace=True)
    
    # 5. Codificación Cíclica
    master_df = apply_cyclic_encoding(master_df)
    
    # 6. Guardar resultado
    print(f"Guardando Dataset Maestro en {DATA_PROCESSED}...")
    os.makedirs(os.path.dirname(DATA_PROCESSED), exist_ok=True)
    master_df.to_csv(DATA_PROCESSED, index=False)
    
    print("¡Proceso completado exitosamente!")
    print(f"Dimensiones finales: {master_df.shape}")
    print("\nPrimeras filas del dataset:")
    print(master_df.head())

if __name__ == "__main__":
    main()
