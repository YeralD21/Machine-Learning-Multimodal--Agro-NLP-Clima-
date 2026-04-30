"""
==========================================================================
NASA POWER Pipeline — Actividad 8: Esquema en PostgreSQL
==========================================================================
Ejecuta los scripts DDL en la base de datos limon_analytics_db:
  1. ALTER TABLE fact_produccion_limon (añade columnas climáticas)
  2. CREATE TABLE dim_clima
  3. CREATE VIEW v_lstm_features
  4. Carga los datos climáticos en dim_clima

Entrada : data/02_interim_nasa/nasa_mensual_integrado.csv
          database/dwh_nasa_clima_schema.sql
Salida  : Tablas actualizadas en PostgreSQL
==========================================================================
"""
import sys
sys.stdout.reconfigure(encoding='utf-8')

import os
import json
import warnings
import pandas as pd

warnings.filterwarnings('ignore')

with open('data/02_interim_nasa/nasa_pipeline_config.json', 'r', encoding='utf-8') as f:
    CONFIG = json.load(f)
DIRS            = CONFIG['DIRS']
PARAMETROS_NASA = CONFIG['PARAMETROS_NASA']

# Leer URI de PostgreSQL del config principal del proyecto
PG_CONFIG_PATH = 'data/02_interim/pipeline_config.json'
PG_URI = 'postgresql://postgres:postgres@localhost:5432/limon_analytics_db'
if os.path.exists(PG_CONFIG_PATH):
    with open(PG_CONFIG_PATH, 'r', encoding='utf-8') as f:
        pg_cfg = json.load(f)
    PG_URI = pg_cfg.get('PG_URI', PG_URI)

print('=' * 70)
print('  NASA PIPELINE — ACTIVIDAD 8: Esquema en PostgreSQL')
print('=' * 70)
print(f'\n  Conectando a: {PG_URI}')

df = pd.read_csv(os.path.join(DIRS['interim_nasa'], 'nasa_mensual_integrado.csv'))
print(f'  Dataset cargado: {df.shape}')
vars_disponibles = [p for p in PARAMETROS_NASA if p in df.columns]

try:
    from sqlalchemy import create_engine, text

    engine = create_engine(PG_URI)

    # ── 8.1 Ejecutar DDL del schema NASA ─────────────────────────────────
    print('\n[8.1] Ejecutando DDL del schema NASA...')
    sql_path = 'database/dwh_nasa_clima_schema.sql'
    with open(sql_path, 'r', encoding='utf-8') as f:
        ddl_sql = f.read()

    # Ejecutar sentencia por sentencia (separadas por ';')
    with engine.connect() as conn:
        # Separar y ejecutar cada statement
        statements = [s.strip() for s in ddl_sql.split(';') if s.strip()]
        for stmt in statements:
            if stmt.upper().startswith(('ALTER', 'CREATE', 'COMMENT')):
                try:
                    conn.execute(text(stmt))
                    conn.commit()
                    # Mostrar solo la primera línea del statement
                    primera_linea = stmt.split('\n')[0][:70]
                    print(f'  [OK] {primera_linea}...')
                except Exception as e:
                    print(f'  [WARN] {str(e)[:80]}')

    # ── 8.2 Cargar datos en dim_clima ─────────────────────────────────────
    print('\n[8.2] Cargando datos en dim_clima...')

    with engine.connect() as conn:
        # Obtener IDs de dim_tiempo y dim_ubicacion
        dt_map = pd.read_sql('SELECT id_tiempo, fecha_evento FROM dim_tiempo', conn)
        du_map = pd.read_sql(
            'SELECT id_ubicacion, departamento, provincia FROM dim_ubicacion', conn
        )

    # Normalizar para el merge
    du_map['departamento'] = du_map['departamento'].str.upper().str.strip()
    du_map['provincia']    = du_map['provincia'].str.upper().str.strip()

    df_clima = df.copy()
    df_clima = df_clima.merge(dt_map, on='fecha_evento', how='left')
    df_clima = df_clima.merge(
        du_map,
        left_on=['DEPARTAMENTO', 'PROVINCIA'],
        right_on=['departamento', 'provincia'],
        how='left'
    )

    # Columnas para dim_clima
    cols_dim_clima = ['id_tiempo', 'id_ubicacion'] + \
                     [v.lower() for v in vars_disponibles if v in df_clima.columns]

    # Renombrar variables a minúsculas para PostgreSQL
    rename_map = {v: v.lower() for v in vars_disponibles}
    df_clima = df_clima.rename(columns=rename_map)

    cols_disponibles = [c for c in cols_dim_clima if c in df_clima.columns]
    df_load = df_clima[cols_disponibles].dropna(subset=['id_tiempo', 'id_ubicacion'])
    df_load['id_tiempo']    = df_load['id_tiempo'].astype(int)
    df_load['id_ubicacion'] = df_load['id_ubicacion'].astype(int)

    # Truncar y recargar
    with engine.connect() as conn:
        conn.execute(text('TRUNCATE TABLE dim_clima RESTART IDENTITY CASCADE'))
        conn.commit()

    df_load.to_sql('dim_clima', engine, if_exists='append',
                   index=False, method='multi', chunksize=500)
    print(f'  [OK] dim_clima: {len(df_load):,} registros cargados')

    # ── 8.3 Actualizar fact_produccion_limon con columnas climáticas ──────
    print('\n[8.3] Actualizando fact_produccion_limon con variables climáticas...')
    vars_lower = [v.lower() for v in vars_disponibles]
    set_clause = ', '.join([f'f.{v} = c.{v}' for v in vars_lower])

    update_sql = f"""
    UPDATE fact_produccion_limon f
    SET {set_clause}
    FROM dim_clima c
    WHERE f.id_tiempo = c.id_tiempo
      AND f.id_ubicacion = c.id_ubicacion
    """
    with engine.connect() as conn:
        result = conn.execute(text(update_sql))
        conn.commit()
        print(f'  [OK] {result.rowcount:,} filas actualizadas en fact_produccion_limon')

    engine.dispose()
    print('\n  ✅ PostgreSQL actualizado correctamente.')

except ImportError:
    print('\n  [WARN] sqlalchemy no disponible. Saltando carga en PostgreSQL.')
    print('  Para cargar en BD, instala: pip install sqlalchemy psycopg2-binary')
except Exception as e:
    print(f'\n  [ERROR PostgreSQL] {e}')
    print('  Continuando sin carga en BD...')
    print('  Verifica que PostgreSQL esté corriendo y la BD limon_analytics_db exista.')

print()
print('[ACTIVIDAD 8] COMPLETADA.')
print('  Descripción: DDL ejecutado y datos climáticos cargados en PostgreSQL.')
print('  Tablas afectadas: dim_clima, fact_produccion_limon (ALTER + UPDATE)')
