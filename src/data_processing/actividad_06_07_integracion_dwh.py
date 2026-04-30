"""
Pipeline Fase 1 - Actividades 6 y 7: Integración al DWH + Diseño Star Schema
Actividad 6: Resuelve granularidades y lógica de JOIN mensual.
Actividad 7: Documenta el UML Star Schema y genera el dataset integrado.
Salida: data/02_interim/dataset_integrado.csv  +  database/dwh_star_schema.sql
"""
import sys; sys.stdout.reconfigure(encoding='utf-8')
import os, json, warnings
import numpy as np
import pandas as pd

warnings.filterwarnings('ignore')

with open('data/02_interim/pipeline_config.json', 'r', encoding='utf-8') as f:
    CONFIG = json.load(f)
DIRS = CONFIG['DIRS']
INTERIM_DIR = DIRS['interim']
DB_DIR = DIRS['database']
ANHO_INICIO = CONFIG['ANHO_INICIO']
ANHO_FIN = CONFIG['ANHO_FIN']

print('=' * 70)
print('  ACTIVIDAD 6: Integración al Data Warehouse')
print('=' * 70)

# ─────────────────────────────────────────────
# 6.1 Cargar fuentes limpias
# ─────────────────────────────────────────────
print('\n[6.1] Cargando fuentes limpias...')
df_m = pd.read_csv(os.path.join(INTERIM_DIR, 'midagri_limon_clean.csv'))
df_ev = pd.read_csv(os.path.join(INTERIM_DIR, 'indeci_eventos_clean.csv'), low_memory=False)
df_n  = pd.read_csv(os.path.join(INTERIM_DIR, 'agraria_noticias_clean.csv'))
print(f'  MIDAGRI:       {len(df_m):,} filas')
print(f'  INDECI eventos:{len(df_ev):,} filas')
print(f'  Noticias:      {len(df_n):,} filas')


# ─────────────────────────────────────────────
# 6.2 MIDAGRI — Agregación mensual por Dpto/Prov
# ─────────────────────────────────────────────
print('\n[6.2] MIDAGRI — Agregación mensual a nivel Dpto/Prov')
df_midagri_mensual = (
    df_m.groupby(['fecha_evento', 'departamento', 'provincia'])
    .agg(
        produccion_t=('produccion_t', 'sum'),
        cosecha_ha=('cosecha_ha', 'sum'),
        precio_chacra_kg=('precio_chacra_kg', 'mean'),
    )
    .reset_index()
)
print(f'  Filas agregadas MIDAGRI: {len(df_midagri_mensual):,}')
print(f'  Periodos únicos: {df_midagri_mensual["fecha_evento"].nunique()}')


# ─────────────────────────────────────────────
# 6.3 INDECI — Agregación mensual por Dpto/Prov
# ─────────────────────────────────────────────
print('\n[6.3] INDECI — Resolución de granularidad: evento → mensual')
indeci_agg_cols = {}
for col in ['personas_afectadas', 'personas_damnificadas', 'has_cultivo_afectadas', 'has_cultivo_perdidas']:
    if col in df_ev.columns:
        indeci_agg_cols[col] = ('sum',)

agg_dict = {
    'fenomeno': ('count',),  # Número de emergencias
}
for col in ['personas_afectadas', 'personas_damnificadas', 'has_cultivo_afectadas', 'has_cultivo_perdidas']:
    if col in df_ev.columns:
        agg_dict[col] = ('sum',)

df_indeci_mensual = (
    df_ev.dropna(subset=['fecha_evento', 'departamento', 'provincia'])
    .groupby(['fecha_evento', 'departamento', 'provincia'])
    .agg(num_emergencias=('fenomeno', 'count'),
         total_afectados=('personas_afectadas', 'sum') if 'personas_afectadas' in df_ev.columns else ('fenomeno', 'count'),
         has_cultivo_perdidas=('has_cultivo_perdidas', 'sum') if 'has_cultivo_perdidas' in df_ev.columns else ('fenomeno', 'count'),
    )
    .reset_index()
)
print(f'  Filas agregadas INDECI: {len(df_indeci_mensual):,}')


# ─────────────────────────────────────────────
# 6.4 NOTICIAS — Sentimiento mensual nacional
# ─────────────────────────────────────────────
print('\n[6.4] Noticias — Señal mensual nacional (promedio de sentimiento)')
# En esta fase sólo usamos el conteo de noticias por mes
# El sentimiento (de Fase 2) se integraría aquí cuando esté disponible
df_n['fecha_evento'] = pd.to_datetime(df_n['fecha'], errors='coerce').dt.strftime('%Y-%m')
df_noticias_mensual = (
    df_n.groupby('fecha_evento')
    .agg(n_noticias=('titular', 'count'))
    .reset_index()
)
print(f'  Meses con noticias: {len(df_noticias_mensual)}')

# ─────────────────────────────────────────────
# 6.5 Esqueleto temporal + JOIN maestro
# ─────────────────────────────────────────────
print('\n[6.5] Creando esqueleto temporal 2021-2025 y fusión multimodal')

# Esqueleto: todas las combinaciones Mes x Provincia
fechas = pd.date_range(start=f'{ANHO_INICIO}-01-01', end=f'{ANHO_FIN}-08-01', freq='MS')
provincias = df_midagri_mensual[['departamento', 'provincia']].drop_duplicates()
skeleton = pd.DataFrame(
    [(d.strftime('%Y-%m'), row['departamento'], row['provincia'])
     for d in fechas for _, row in provincias.iterrows()],
    columns=['fecha_evento', 'departamento', 'provincia']
)
print(f'  Esqueleto generado: {len(skeleton):,} filas')

# Left Joins
df_int = skeleton.copy()
df_int = pd.merge(df_int, df_midagri_mensual, on=['fecha_evento', 'departamento', 'provincia'], how='left')
df_int = pd.merge(df_int, df_indeci_mensual,  on=['fecha_evento', 'departamento', 'provincia'], how='left')
df_int = pd.merge(df_int, df_noticias_mensual, on='fecha_evento', how='left')

# Relleno estratégico
df_int['produccion_t']      = df_int['produccion_t'].fillna(0)
df_int['num_emergencias']   = df_int['num_emergencias'].fillna(0)
df_int['total_afectados']   = df_int['total_afectados'].fillna(0)
df_int['n_noticias']        = df_int['n_noticias'].fillna(0)
# Precio: forward-fill por provincia
df_int = df_int.sort_values(['departamento', 'provincia', 'fecha_evento'])
df_int['precio_chacra_kg'] = df_int.groupby(['departamento', 'provincia'])['precio_chacra_kg'].ffill().bfill()

# ─────────────────────────────────────────────
# 6.6 INTEGRACIÓN DATA NASA
# ─────────────────────────────────────────────
print('\n[6.6] Integrando variables climáticas NASA POWER...')
nasa_path = 'data/03_processed_nasa/nasa_climatic_raw_values.csv'
if os.path.exists(nasa_path):
    df_nasa = pd.read_csv(nasa_path)
    # Seleccionar columnas relevantes
    cols_nasa = ['fecha_evento', 'DEPARTAMENTO', 'PROVINCIA', 
                 'T2M', 'T2M_MAX', 'T2M_MIN', 'PRECTOTCORR', 'RH2M', 'WS2M', 'ALLSKY_SFC_SW_DWN']
    df_nasa = df_nasa[cols_nasa].copy()
    
    # Renombrar para coincidir con el DWH (Star Schema)
    df_nasa = df_nasa.rename(columns={
        'DEPARTAMENTO': 'departamento',
        'PROVINCIA': 'provincia',
        'T2M_MAX': 'temp_max_c',
        'T2M_MIN': 'temp_min_c',
        'PRECTOTCORR': 'precipitacion_mm',
        'RH2M': 'humedad_rel_pct',
        'WS2M': 'velocidad_viento',
        'ALLSKY_SFC_SW_DWN': 'radiacion_solar'
    })

    # Asegurar mayúsculas en geo para el join
    for c in ['departamento', 'provincia']:
        df_nasa[c] = df_nasa[c].astype(str).str.upper().str.strip()

    # Merge con el dataset integrado
    df_int = pd.merge(df_int, df_nasa, on=['fecha_evento', 'departamento', 'provincia'], how='left')
    
    # Rellenar nulos climáticos con la media de la provincia (interpolación simple)
    cols_clima = ['temp_max_c', 'temp_min_c', 'precipitacion_mm', 'humedad_rel_pct', 'velocidad_viento', 'radiacion_solar']
    for col in cols_clima:
        df_int[col] = df_int.groupby(['departamento', 'provincia'])[col].transform(lambda x: x.fillna(x.mean()))
        # Si aún hay nulos (provincias sin data), usar media nacional
        df_int[col] = df_int[col].fillna(df_int[col].mean())
    
    print(f'  Variables NASA integradas: {cols_clima}')
    print(f'  Nulos climáticos remanentes: {df_int[cols_clima].isnull().sum().sum()}')
else:
    print(f'  ⚠️ Archivo NASA no encontrado en {nasa_path}. Saltando integración climática.')

# Verificar sin duplicados
dupes = df_int.duplicated(subset=['fecha_evento', 'departamento', 'provincia']).sum()
print(f'  Filas dataset integrado: {len(df_int):,}')
print(f'  Duplicados en llave maestra: {dupes}')

out_int = os.path.join(INTERIM_DIR, 'dataset_integrado.csv')
df_int.to_csv(out_int, index=False, encoding='utf-8-sig')
print(f'\n  [OK] {out_int}')

print()
print('[ACTIVIDAD 6] COMPLETADA.')
print('  Descripcion: Resolucion de granularidades y fusion multimodal con esqueleto temporal.')
print(f'  Resultado: {len(df_int):,} filas con llave (fecha_evento, departamento, provincia)')

# ─────────────────────────────────────────────
# ACTIVIDAD 7: Diseño del Star Schema DWH
# ─────────────────────────────────────────────
print()
print('=' * 70)
print('  ACTIVIDAD 7: Diseño del Star Schema del Data Warehouse')
print('=' * 70)

# UML Star Schema (documentado en comentarios)
# ╔══════════════════════════════════════════════════════════╗
# ║  STAR SCHEMA — limon_analytics_db                       ║
# ╠══════════════════════════════════════════════════════════╣
# ║  TABLA DE HECHOS:  fact_produccion_limon                 ║
# ║  ┌─────────────────────────────────────────────────┐     ║
# ║  │ id_hecho (PK)                                   │     ║
# ║  │ id_tiempo (FK → dim_tiempo)                     │     ║
# ║  │ id_ubicacion (FK → dim_ubicacion)               │     ║
# ║  │ --- MÉTRICAS AGRÍCOLAS ---                      │     ║
# ║  │ produccion_t    FLOAT                           │     ║
# ║  │ cosecha_ha      FLOAT                           │     ║
# ║  │ precio_chacra_kg FLOAT                          │     ║
# ║  │ --- MÉTRICAS EMERGENCIAS ---                    │     ║
# ║  │ num_emergencias  INT                            │     ║
# ║  │ total_afectados  INT                            │     ║
# ║  │ has_cultivo_perdidas FLOAT                      │     ║
# ║  │ --- MÉTRICAS NOTICIAS ---                       │     ║
# ║  │ n_noticias       INT                            │     ║
# ║  │ avg_sentimiento  FLOAT  (Fase 2 — NLP)          │     ║
# ║  │ --- MÉTRICAS CLIMÁTICAS (NASA PLACEHOLDER) ---  │     ║
# ║  │ temp_max_c       FLOAT  (T2M_MAX)               │     ║
# ║  │ temp_min_c       FLOAT  (T2M_MIN)               │     ║
# ║  │ precipitacion_mm FLOAT  (PRECTOTCORR)           │     ║
# ║  │ humedad_rel_pct  FLOAT  (RH2M)                  │     ║
# ║  │ velocidad_viento FLOAT  (WS2M)                  │     ║
# ║  └─────────────────────────────────────────────────┘     ║
# ║                                                          ║
# ║  dim_tiempo           dim_ubicacion                      ║
# ║  ─────────────        ─────────────────                   ║
# ║  id_tiempo (PK)       id_ubicacion (PK)                  ║
# ║  fecha_evento         departamento                       ║
# ║  anho                 provincia                          ║
# ║  mes                  lat                                ║
# ║  trimestre            lon                                ║
# ║  month_sin                                               ║
# ║  month_cos                                               ║
# ╚══════════════════════════════════════════════════════════╝

sql_ddl = """-- =================================================================
-- STAR SCHEMA: limon_analytics_db
-- Proyecto: Predicción de Producción de Limón — LSTM Multimodal
-- Generado por: Pipeline Fase 1 — Actividad 7
-- =================================================================

-- Dimensión Tiempo
CREATE TABLE IF NOT EXISTS dim_tiempo (
    id_tiempo     SERIAL PRIMARY KEY,
    fecha_evento  VARCHAR(7)  NOT NULL UNIQUE,   -- YYYY-MM
    anho          SMALLINT    NOT NULL,
    mes           SMALLINT    NOT NULL,
    trimestre     SMALLINT    GENERATED ALWAYS AS (((mes - 1) / 3) + 1) STORED,
    month_sin     FLOAT,
    month_cos     FLOAT
);

-- Dimensión Ubicación
CREATE TABLE IF NOT EXISTS dim_ubicacion (
    id_ubicacion  SERIAL PRIMARY KEY,
    departamento  VARCHAR(60) NOT NULL,
    provincia     VARCHAR(60) NOT NULL,
    lat           FLOAT,
    lon           FLOAT,
    UNIQUE (departamento, provincia)
);

-- Tabla de Hechos
CREATE TABLE IF NOT EXISTS fact_produccion_limon (
    id_hecho              SERIAL PRIMARY KEY,
    id_tiempo             INT  NOT NULL REFERENCES dim_tiempo(id_tiempo),
    id_ubicacion          INT  NOT NULL REFERENCES dim_ubicacion(id_ubicacion),

    -- Métricas Agrícolas (MIDAGRI)
    produccion_t          FLOAT   DEFAULT 0,
    cosecha_ha            FLOAT   DEFAULT 0,
    precio_chacra_kg      FLOAT,

    -- Métricas Emergencias (INDECI)
    num_emergencias       INT     DEFAULT 0,
    total_afectados       INT     DEFAULT 0,
    has_cultivo_perdidas  FLOAT   DEFAULT 0,

    -- Métricas Noticias (Agraria.pe)
    n_noticias            INT     DEFAULT 0,
    avg_sentimiento       FLOAT,       -- Fase 2: NLP

    -- Métricas Climáticas (NASA POWER — Pendiente)
    temp_max_c            FLOAT,       -- TODO: T2M_MAX
    temp_min_c            FLOAT,       -- TODO: T2M_MIN
    precipitacion_mm      FLOAT,       -- TODO: PRECTOTCORR
    humedad_rel_pct       FLOAT,       -- TODO: RH2M
    velocidad_viento      FLOAT,       -- TODO: WS2M
    radiacion_solar       FLOAT,       -- TODO: ALLSKY_SFC_SW_DWN

    UNIQUE (id_tiempo, id_ubicacion)
);

-- Índices para optimizar consultas de series temporales
CREATE INDEX IF NOT EXISTS idx_fact_tiempo     ON fact_produccion_limon(id_tiempo);
CREATE INDEX IF NOT EXISTS idx_fact_ubicacion  ON fact_produccion_limon(id_ubicacion);
"""

sql_path = os.path.join(DB_DIR, 'dwh_star_schema.sql')
with open(sql_path, 'w', encoding='utf-8') as f:
    f.write(sql_ddl)
print(f'\n  [OK] DDL guardado: {sql_path}')
print('\n  Star Schema documentado:')
print('  fact_produccion_limon ─── dim_tiempo')
print('                        └── dim_ubicacion')

print()
print('[ACTIVIDAD 7] COMPLETADA.')
print('  Descripcion: Diseño UML Star Schema con placeholders NASA y Fase 2 NLP.')
print(f'  Archivo generado: {sql_path}')
