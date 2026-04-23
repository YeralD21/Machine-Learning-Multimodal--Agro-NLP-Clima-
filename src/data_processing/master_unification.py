"""
==========================================================================
Paso 3 del Pipeline: Unificación Multimodal y Dataset Maestro (Fase 1)
==========================================================================
Este script realiza la integración final de todas las fuentes:
1. Unifica noticias (NLP Raw) sin procesar sentimiento.
2. Crea un esqueleto temporal completo (Jan 2021 - Aug 2025).
3. Une MIDAGRI, NASA e INDECI al esqueleto.
4. Aplica ingeniería de características (Ciclicidad + Escalamiento).
5. Guarda el escalador para uso futuro.

Salida: data/processed/master_dataset_fase1.csv
        models/scalers/scaler_fase1.pkl
"""

import os
import sys
import glob
import joblib
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.preprocessing import StandardScaler

def run_unification():
    print("=" * 70)
    print("  PIPELINE DE UNIFICACIÓN - FASE 1")
    print("=" * 70)

    # 1. UNIFICACIÓN DE NOTICIAS (NLP RAW)
    print("\n[1/5] Unificando noticias de Agraria.pe...")
    news_files = glob.glob("data/raw/agraria_pe/agro_news_*.csv")
    if news_files:
        df_news = pd.concat([pd.read_csv(f) for f in news_files], ignore_index=True)
        df_news['fecha'] = pd.to_datetime(df_news['fecha'], errors='coerce')
        df_news = df_news.dropna(subset=['fecha']).sort_values('fecha')
        
        news_output = "data/interim/agraria/noticias_unificadas_2021_2025.csv"
        os.makedirs(os.path.dirname(news_output), exist_ok=True)
        df_news.to_csv(news_output, index=False, encoding='utf-8')
        print(f"       Noticias unificadas: {len(df_news)} registros.")
    else:
        print("       [!] No se encontraron archivos de noticias.")

    # 2. CREACIÓN DEL ESQUELETO TEMPORAL
    print("\n[2/5] Generando esqueleto temporal (Jan 2021 - Aug 2025)...")
    dates = pd.date_range(start="2021-01-01", end="2025-08-01", freq="MS")
    
    # Obtener ubicaciones únicas desde MIDAGRI para el esqueleto
    df_midagri = pd.read_csv("data/interim/midagri/midagri_limon_procesado.csv")
    locations = df_midagri[['departamento', 'provincia']].drop_duplicates()
    
    # Producto cartesiano entre fechas y ubicaciones
    skeleton = []
    for d in dates:
        for _, loc in locations.iterrows():
            skeleton.append({
                'fecha_evento': d.strftime('%Y-%m'),
                'departamento': loc['departamento'],
                'provincia': loc['provincia']
            })
    df_master = pd.DataFrame(skeleton)
    print(f"       Esqueleto creado: {len(df_master)} filas (Mes x Provincia).")

    # 3. MERGE MULTIMODAL
    print("\n[3/5] Fusionando fuentes de datos...")
    
    # A. MIDAGRI
    print("       Merging MIDAGRI...")
    df_master = pd.merge(df_master, df_midagri, on=['fecha_evento', 'departamento', 'provincia'], how='left')
    
    # B. INDECI
    print("       Merging INDECI...")
    df_indeci = pd.read_csv("data/interim/indeci/indeci_temporal_2021_2025.csv")
    df_master = pd.merge(df_master, df_indeci, on=['fecha_evento', 'departamento', 'provincia'], how='left')
    
    # C. NASA (Clima)
    print("       Merging NASA...")
    df_nasa = pd.read_csv("data/interim/nasa/clima_dataset_final.csv")
    # Alinear fecha de NASA (YYYY-MM-DD -> YYYY-MM)
    df_nasa['fecha_evento'] = pd.to_datetime(df_nasa['DATE']).dt.strftime('%Y-%m')
    # Eliminar columnas cíclicas previas y DATE original para recalcular
    cols_to_drop = ['DATE', 'month_sin', 'month_cos']
    df_nasa = df_nasa.drop(columns=[c for c in cols_to_drop if c in df_nasa.columns])
    
    df_master = pd.merge(df_master, df_nasa, on=['fecha_evento', 'departamento', 'provincia'], how='left')

    # Relleno de nulos estratégico
    # Producción y emergencias: NaNs son ceros (ausencia de evento)
    cols_to_zero = ['produccion_t', 'num_emergencias', 'total_afectados', 'hectareas_cultivo_perdidas']
    df_master[cols_to_zero] = df_master[cols_to_zero].fillna(0)
    
    # Precios y Clima: Imputación hacia adelante por provincia (FFILL)
    # Si hay huecos climáticos, se asume que se mantiene el estado anterior
    df_master = df_master.sort_values(['departamento', 'provincia', 'fecha_evento'])
    cols_to_fill = ['precio_chacra_kg', 'ALLSKY_SFC_SW_DWN', 'PRECTOTCORR', 'QV2M', 'RH2M', 'T2M', 'T2M_MAX', 'T2M_MIN', 'WS2M']
    df_master[cols_to_fill] = df_master.groupby(['departamento', 'provincia'])[cols_to_fill].ffill().bfill()

    # 4. FEATURE ENGINEERING
    print("\n[4/5] Aplicando ingeniería de características...")
    
    # A. Codificación Cíclica
    print("       Calculando estacionalidad cíclica...")
    month_val = pd.to_datetime(df_master['fecha_evento']).dt.month
    df_master['month_sin'] = np.sin(2 * np.pi * month_val / 12)
    df_master['month_cos'] = np.cos(2 * np.pi * month_val / 12)
    
    # B. Escalamiento (StandardScaler)
    print("       Escalando variables y guardando StandardScaler...")
    features_to_scale = [
        'produccion_t', 'precio_chacra_kg', 'num_emergencias', 'total_afectados',
        'hectareas_cultivo_perdidas', 'ALLSKY_SFC_SW_DWN', 'PRECTOTCORR', 
        'QV2M', 'RH2M', 'T2M', 'T2M_MAX', 'T2M_MIN', 'WS2M'
    ]
    
    scaler = StandardScaler()
    df_master[features_to_scale] = scaler.fit_transform(df_master[features_to_scale])
    
    scaler_path = "models/scalers/scaler_fase1.pkl"
    os.makedirs(os.path.dirname(scaler_path), exist_ok=True)
    joblib.dump(scaler, scaler_path)
    print(f"       Escalador guardado en: {scaler_path}")

    # 5. EXPORTACIÓN Y VALIDACIÓN
    print("\n[5/5] Exportando Dataset Maestro...")
    output_path = "data/processed/master_dataset_fase1.csv"
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df_master.to_csv(output_path, index=False)
    
    # Conteo por año
    df_master['anho'] = df_master['fecha_evento'].str.split('-').str[0]
    print("\n       Conteo de registros por año:")
    print(df_master.groupby('anho').size())

    # Visualización de tendencia de producción (usando datos sin escalar para el gráfico)
    print("\n       Generando gráfico de validación...")
    # Recuperar producción real para el gráfico
    prod_original = scaler.inverse_transform(df_master[features_to_scale])[:, 0]
    df_plot = df_master.copy()
    df_plot['produccion_real'] = prod_original
    
    # Suma mensual nacional
    trend = df_plot.groupby('fecha_evento')['produccion_real'].sum()
    
    plt.figure(figsize=(12, 6))
    trend.plot(kind='line', marker='o', color='forestgreen')
    plt.title('Producción Total Mensual de Limón (Fase 1 - Validada)')
    plt.xlabel('Fecha (YYYY-MM)')
    plt.ylabel('Toneladas')
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig("data/processed/trend_check.png")
    print("       Gráfico guardado en: data/processed/trend_check.png")

    print("\n" + "=" * 70)
    print("  UNIFICACIÓN COMPLETADA EXITOSAMENTE")
    print("  Dataset: data/processed/master_dataset_fase1.csv")
    print("=" * 70)

if __name__ == "__main__":
    sys.stdout.reconfigure(encoding='utf-8')
    run_unification()
