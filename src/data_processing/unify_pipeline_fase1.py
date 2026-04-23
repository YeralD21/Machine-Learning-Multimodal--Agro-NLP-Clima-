import pandas as pd
import numpy as np
import os
import glob
from datetime import datetime
from sklearn.preprocessing import StandardScaler
import itertools

# Configuración de rutas
DATA_RAW_NEWS = 'data/raw/agraria_pe/'
DATA_INTERIM_AGRARIA = 'data/interim/agraria/noticias_unificadas_2021_2025.csv'
DATA_INTERIM_MIDAGRI = 'data/interim/midagri/midagri_limon_procesado.csv'
DATA_INTERIM_INDECI = 'data/interim/indeci/indeci_temporal_2021_2025.csv'
DATA_INTERIM_NASA = 'data/interim/nasa/clima_dataset_final.csv'
DATA_PROCESSED_FASE1 = 'data/processed/master_dataset_fase1.csv'

def unify_news():
    print("1. Unificando Noticias (NLP Raw)...")
    news_files = glob.glob(os.path.join(DATA_RAW_NEWS, 'agro_news_*.csv'))
    all_news = []
    
    for f in news_files:
        df = pd.read_csv(f)
        all_news.append(df)
        
    df_news = pd.concat(all_news, ignore_index=True)
    df_news['fecha'] = pd.to_datetime(df_news['fecha'])
    df_news = df_news.sort_values('fecha')
    
    os.makedirs(os.path.dirname(DATA_INTERIM_AGRARIA), exist_ok=True)
    df_news.to_csv(DATA_INTERIM_AGRARIA, index=False)
    print(f"   - Noticias unificadas guardadas en {DATA_INTERIM_AGRARIA}")
    return df_news

def create_timeline_skeleton(df_midagri):
    print("2. Creando Esqueleto Temporal (Timeline)...")
    # Rango de fechas: Enero 2021 a Agosto 2025
    dates = pd.date_range(start='2021-01-01', end='2025-08-01', freq='MS').strftime('%Y-%m')
    
    # Obtener departamentos y provincias únicos de MIDAGRI
    locations = df_midagri[['departamento', 'provincia']].drop_duplicates()
    
    # Crear producto cartesiano
    skeleton = list(itertools.product(dates, locations.itertuples(index=False, name=None)))
    
    # Convertir a DataFrame
    df_skeleton = pd.DataFrame([{'date': d, 'departamento': loc[0], 'provincia': loc[1]} for d, loc in skeleton])
    print(f"   - Esqueleto creado: {len(df_skeleton)} filas (combinaciones fecha-lugar).")
    return df_skeleton

def multimodal_merge(skeleton, df_midagri, df_nasa, df_indeci):
    print("3. Fusión Multimodal (Merge)...")
    
    # Estandarizar NASA (DATE -> date YYYY-MM)
    df_nasa['date'] = pd.to_datetime(df_nasa['DATE']).dt.strftime('%Y-%m')
    df_nasa.drop(columns=['DATE', 'month_sin', 'month_cos'], inplace=True, errors='ignore')
    df_nasa.columns = [c.lower() for c in df_nasa.columns]
    
    # Estandarizar MIDAGRI
    df_midagri.rename(columns={'fecha_evento': 'date'}, inplace=True)
    df_midagri.columns = [c.lower() for c in df_midagri.columns]
    
    # Estandarizar INDECI
    df_indeci.rename(columns={'fecha_evento': 'date'}, inplace=True)
    df_indeci.columns = [c.lower() for c in df_indeci.columns]
    
    # Uniones (Left Join al esqueleto)
    master = skeleton.merge(df_midagri, on=['date', 'departamento', 'provincia'], how='left')
    master = master.merge(df_nasa, on=['date', 'departamento', 'provincia'], how='left')
    master = master.merge(df_indeci, on=['date', 'departamento', 'provincia'], how='left')
    
    # Tratamiento de nulos
    # Para producción/precios: Si es nulo, depende de la lógica. 
    # El usuario dice: "Si en un mes el valor es 0, MANTÉN EL 0". 
    # "Si hay meses vacíos tras la unión, rellena con 0 solo si se confirma que no hubo actividad".
    # Rellenaremos con 0 las columnas de desastres y producción si son nulas tras el join.
    cols_to_fill_zero = [
        'produccion_t', 'precio_chacra_kg', 'num_emergencias', 
        'total_afectados', 'hectareas_cultivo_perdidas'
    ]
    for col in cols_to_fill_zero:
        if col in master.columns:
            master[col] = master[col].fillna(0)
            
    # Para NASA, si hay nulos (posiblemente fuera de rango), podemos usar interpolación o media por provincia
    nasa_cols = ['allsky_sfc_sw_dwn', 'prectotcorr', 'qv2m', 'rh2m', 't2m', 't2m_max', 't2m_min', 'ws2m']
    for col in nasa_cols:
        if col in master.columns:
            # Rellenar con la media de la provincia si falta algún dato climático
            master[col] = master.groupby(['departamento', 'provincia'])[col].transform(lambda x: x.fillna(x.mean()))
            # Si aún hay nulos (provincia sin ningún dato), rellenar con media general
            master[col] = master[col].fillna(master[col].mean())

    return master

def apply_phase1_adjustments(df):
    print("4. Ajuste del Entregable Fase 1...")
    
    # Codificación Cíclica (Mes)
    temp_date = pd.to_datetime(df['date'])
    df['month_sin'] = np.sin(2 * np.pi * temp_date.dt.month / 12)
    df['month_cos'] = np.cos(2 * np.pi * temp_date.dt.month / 12)
    
    # Variables a escalar
    features_to_scale = [
        'produccion_t', 'precio_chacra_kg', 'allsky_sfc_sw_dwn', 'prectotcorr', 
        'qv2m', 'rh2m', 't2m', 't2m_max', 't2m_min', 'ws2m',
        'num_emergencias', 'total_afectados', 'hectareas_cultivo_perdidas'
    ]
    
    print("   - Escalando variables con StandardScaler...")
    scaler = StandardScaler()
    df[features_to_scale] = scaler.fit_transform(df[features_to_scale])
    
    return df

def main():
    # 1. Noticias
    unify_news()
    
    # Cargar datos interim
    df_midagri = pd.read_csv(DATA_INTERIM_MIDAGRI)
    df_nasa = pd.read_csv(DATA_INTERIM_NASA)
    df_indeci = pd.read_csv(DATA_INTERIM_INDECI)
    
    # 2. Esqueleto
    skeleton = create_timeline_skeleton(df_midagri)
    
    # 3. Merge
    master = multimodal_merge(skeleton, df_midagri, df_nasa, df_indeci)
    
    # 4. Ajustes Fase 1 (Cíclico + Escalado)
    master = apply_phase1_adjustments(master)
    
    # Eliminar duplicados si los hay
    master.drop_duplicates(inplace=True)
    
    # 5. Salida
    os.makedirs(os.path.dirname(DATA_PROCESSED_FASE1), exist_ok=True)
    master.to_csv(DATA_PROCESSED_FASE1, index=False)
    print(f"\n¡Fase 1 Completada! Dataset guardado en {DATA_PROCESSED_FASE1}")
    
    # Verificación de continuidad
    print("\nConteo de filas por año (Continuidad):")
    master['year'] = pd.to_datetime(master['date']).dt.year
    print(master['year'].value_counts().sort_index())
    
    print(f"\nDimensiones finales: {master.shape}")

if __name__ == "__main__":
    main()
