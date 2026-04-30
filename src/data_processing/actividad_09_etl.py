"""
Pipeline Fase 1 - Actividad 9: Pipeline ETL Completo
Extrae de 02_interim/, transforma (codificación cíclica + escalamiento),
carga en PostgreSQL y exporta el CSV final a 03_processed/.
"""
import sys; sys.stdout.reconfigure(encoding='utf-8')
import os, json, warnings
import numpy as np
import pandas as pd
import joblib
from sklearn.preprocessing import StandardScaler
from sqlalchemy import create_engine, text

warnings.filterwarnings('ignore')

with open('data/02_interim/pipeline_config.json', 'r', encoding='utf-8') as f:
    CONFIG = json.load(f)

DIRS = CONFIG['DIRS']
INTERIM_DIR = DIRS['interim']
PROCESSED_DIR = DIRS['processed']
SCALERS_DIR = DIRS['scalers']
PG_URI = CONFIG['PG_URI']

print('=' * 70)
print('  ACTIVIDAD 9: Pipeline ETL Completo')
print('=' * 70)

# ─────────────────────────────────────────────
# 9.1 Extraer dataset integrado
# ─────────────────────────────────────────────
print('\n[9.1] Extrayendo dataset integrado...')
df = pd.read_csv(os.path.join(INTERIM_DIR, 'dataset_integrado.csv'))
print(f'  Filas: {len(df):,} | Columnas: {len(df.columns)}')
print(f'  Rango: {df["fecha_evento"].min()} → {df["fecha_evento"].max()}')

# ─────────────────────────────────────────────
# 9.2 Feature Engineering
# ─────────────────────────────────────────────
print('\n[9.2] Feature Engineering — Codificación Cíclica y Temporal')

df['fecha_dt'] = pd.to_datetime(df['fecha_evento'])
df['anho']     = df['fecha_dt'].dt.year
df['mes']      = df['fecha_dt'].dt.month
df['trimestre']= df['fecha_dt'].dt.quarter

# Codificación cíclica del mes (captura estacionalidad biológica)
df['month_sin'] = np.sin(2 * np.pi * df['mes'] / 12)
df['month_cos'] = np.cos(2 * np.pi * df['mes'] / 12)
df = df.drop(columns=['fecha_dt'])

print(f'  month_sin / month_cos calculados')
print(f'  Trimestres: {df["trimestre"].unique().tolist()}')

# ─────────────────────────────────────────────
# 9.3 Escalamiento de variables numéricas
# ─────────────────────────────────────────────
print('\n[9.3] Escalamiento con StandardScaler')

FEATURES_TO_SCALE = [
    'produccion_t', 'cosecha_ha', 'precio_chacra_kg',
    'num_emergencias', 'total_afectados', 'has_cultivo_perdidas',
    'n_noticias',
    'temp_max_c', 'temp_min_c', 'precipitacion_mm', 
    'humedad_rel_pct', 'velocidad_viento', 'radiacion_solar'
]

# Solo escalar columnas que existan
cols_to_scale = [c for c in FEATURES_TO_SCALE if c in df.columns]
df_scale_input = df[cols_to_scale].fillna(0)

scaler = StandardScaler()
df[cols_to_scale] = scaler.fit_transform(df_scale_input)

scaler_path = os.path.join(SCALERS_DIR, 'scaler_fase1_v2.pkl')
joblib.dump(scaler, scaler_path)
print(f'  Variables escaladas: {cols_to_scale}')
print(f'  Scaler guardado: {scaler_path}')

# ─────────────────────────────────────────────
# 9.4 Cargar dim_tiempo en PostgreSQL
# ─────────────────────────────────────────────
print('\n[9.4] ETL → PostgreSQL: dim_tiempo')

try:
    engine = create_engine(PG_URI)

    dim_tiempo = (
        df[['fecha_evento', 'anho', 'mes', 'trimestre', 'month_sin', 'month_cos']]
        .drop_duplicates(subset=['fecha_evento'])
        .reset_index(drop=True)
    )
    with engine.connect() as conn:
        conn.execute(text("TRUNCATE TABLE fact_produccion_limon RESTART IDENTITY CASCADE"))
        conn.execute(text("TRUNCATE TABLE dim_tiempo RESTART IDENTITY CASCADE"))
        conn.execute(text("TRUNCATE TABLE dim_ubicacion RESTART IDENTITY CASCADE"))
        conn.commit()

    dim_tiempo.to_sql('dim_tiempo', engine, if_exists='append', index=False,
                      method='multi', chunksize=500)
    print(f'  [OK] dim_tiempo: {len(dim_tiempo)} registros')

    # ─── dim_ubicacion ───
    print('[9.5] ETL → PostgreSQL: dim_ubicacion')
    # Coordenadas aproximadas por departamento (base referencial)
    COORDS = {
        'AMAZONAS': (-6.23, -77.87), 'ANCASH': (-9.53, -77.53), 'APURIMAC': (-13.64, -72.88),
        'AREQUIPA': (-16.41, -71.54), 'AYACUCHO': (-13.16, -74.22), 'CAJAMARCA': (-7.16, -78.50),
        'CALLAO': (-12.06, -77.15), 'CUSCO': (-13.53, -71.97), 'HUANCAVELICA': (-12.78, -74.97),
        'HUANUCO': (-9.93, -76.24), 'ICA': (-14.07, -75.73), 'JUNIN': (-11.16, -75.00),
        'LA LIBERTAD': (-8.11, -79.03), 'LAMBAYEQUE': (-6.77, -79.84), 'LIMA': (-12.05, -77.04),
        'LORETO': (-3.75, -73.25), 'MADRE DE DIOS': (-12.59, -69.19), 'MOQUEGUA': (-17.19, -70.93),
        'PASCO': (-10.69, -76.26), 'PIURA': (-5.19, -80.63), 'PUNO': (-15.84, -70.02),
        'SAN MARTIN': (-6.52, -76.36), 'TACNA': (-18.01, -70.25), 'TUMBES': (-3.57, -80.45),
        'UCAYALI': (-8.38, -74.54),
    }
    dim_ubic = (
        df[['departamento', 'provincia']].drop_duplicates().reset_index(drop=True)
    )
    dim_ubic['lat'] = dim_ubic['departamento'].map(lambda d: COORDS.get(d, (None, None))[0])
    dim_ubic['lon'] = dim_ubic['departamento'].map(lambda d: COORDS.get(d, (None, None))[1])
    dim_ubic.to_sql('dim_ubicacion', engine, if_exists='append', index=False,
                    method='multi', chunksize=500)
    print(f'  [OK] dim_ubicacion: {len(dim_ubic)} registros')

    # ─── fact_produccion_limon ───
    print('[9.6] ETL → PostgreSQL: fact_produccion_limon')

    # Leer IDs generados por PostgreSQL
    with engine.connect() as conn:
        dt_map = pd.read_sql('SELECT id_tiempo, fecha_evento FROM dim_tiempo', conn)
        du_map = pd.read_sql('SELECT id_ubicacion, departamento, provincia FROM dim_ubicacion', conn)

    df_fact = df.merge(dt_map, on='fecha_evento', how='left') \
                .merge(du_map, on=['departamento', 'provincia'], how='left')

    FACT_COLS = [
        'id_tiempo', 'id_ubicacion',
        'produccion_t', 'cosecha_ha', 'precio_chacra_kg',
        'num_emergencias', 'total_afectados', 'has_cultivo_perdidas',
        'n_noticias',
        'temp_max_c', 'temp_min_c', 'precipitacion_mm', 'humedad_rel_pct',
        'velocidad_viento', 'radiacion_solar'
    ]
    fact_cols_exist = [c for c in FACT_COLS if c in df_fact.columns]
    df_fact_load = df_fact[fact_cols_exist].dropna(subset=['id_tiempo', 'id_ubicacion'])
    df_fact_load['id_tiempo'] = df_fact_load['id_tiempo'].astype(int)
    df_fact_load['id_ubicacion'] = df_fact_load['id_ubicacion'].astype(int)

    df_fact_load.to_sql('fact_produccion_limon', engine, if_exists='append',
                        index=False, method='multi', chunksize=500)
    print(f'  [OK] fact_produccion_limon: {len(df_fact_load):,} registros')
    engine.dispose()

except Exception as e:
    print(f'  [ERROR PostgreSQL] {e}')
    print('  Continuando sin carga en BD...')

print('\n  [NASA] Integración climática completada ✅')

# ─────────────────────────────────────────────
# 9.7 Exportar CSV final a 03_processed/
# ─────────────────────────────────────────────
print('\n[9.7] Exportando master_dataset_fase1_v2.csv')
output_path = os.path.join(PROCESSED_DIR, 'master_dataset_fase1_v2.csv')
df.to_csv(output_path, index=False, encoding='utf-8-sig')
print(f'  Shape final: {df.shape}')
print(f'  Columnas: {df.columns.tolist()}')
print(f'  [OK] {output_path}')

print()
print('[ACTIVIDAD 9] COMPLETADA.')
print(f'  Descripcion: ETL completo — Feature Engineering + Escalado + PostgreSQL + CSV.')
print(f'  Scaler guardado: {scaler_path}')
print(f'  Dataset final: {output_path}')
