"""
Pipeline Fase 1 - Actividad 8: Crear Esquemas en PostgreSQL
Crea la base de datos limon_analytics_db y las tablas del Star Schema.
"""
import sys; sys.stdout.reconfigure(encoding='utf-8')
import os, json, warnings
import pandas as pd
from sqlalchemy import create_engine, text

warnings.filterwarnings('ignore')

with open('data/02_interim/pipeline_config.json', 'r', encoding='utf-8') as f:
    CONFIG = json.load(f)

DIRS = CONFIG['DIRS']
DB_DIR = DIRS['database']
PG_URI = CONFIG['PG_URI']

print('=' * 70)
print('  ACTIVIDAD 8: Crear Esquemas en PostgreSQL')
print('=' * 70)
print(f'  Conexión: {PG_URI}')

# ─────────────────────────────────────────────
# 8.1 Conectar a PostgreSQL y crear DB si no existe
# ─────────────────────────────────────────────
# Primero conectar al servidor sin especificar DB para crearla
PG_BASE_URI = PG_URI.rsplit('/', 1)[0] + '/postgres'

try:
    engine_base = create_engine(PG_BASE_URI, isolation_level='AUTOCOMMIT')
    with engine_base.connect() as conn:
        result = conn.execute(
            text("SELECT 1 FROM pg_database WHERE datname = 'limon_analytics_db'")
        )
        exists = result.fetchone()
        if not exists:
            conn.execute(text("CREATE DATABASE limon_analytics_db ENCODING 'UTF8'"))
            print('  [OK] Base de datos limon_analytics_db CREADA.')
        else:
            print('  [OK] Base de datos limon_analytics_db ya existe.')
    engine_base.dispose()
except Exception as e:
    print(f'  [!] Error creando DB: {e}')
    print('      Asegúrate de que PostgreSQL esté corriendo en localhost:5432')
    print('      y que el usuario postgres tenga permisos de creación.')
    sys.exit(1)

# ─────────────────────────────────────────────
# 8.2 Crear tablas del Star Schema
# ─────────────────────────────────────────────
print('\n[8.2] Creando tablas del Star Schema...')

DDL_STATEMENTS = [
    # Dimensión Tiempo
    """
    CREATE TABLE IF NOT EXISTS dim_tiempo (
        id_tiempo     SERIAL PRIMARY KEY,
        fecha_evento  VARCHAR(7)  NOT NULL,
        anho          SMALLINT    NOT NULL,
        mes           SMALLINT    NOT NULL,
        trimestre     SMALLINT,
        month_sin     FLOAT,
        month_cos     FLOAT,
        UNIQUE (fecha_evento)
    )
    """,
    # Dimensión Ubicación
    """
    CREATE TABLE IF NOT EXISTS dim_ubicacion (
        id_ubicacion  SERIAL PRIMARY KEY,
        departamento  VARCHAR(60) NOT NULL,
        provincia     VARCHAR(60) NOT NULL,
        lat           FLOAT,
        lon           FLOAT,
        UNIQUE (departamento, provincia)
    )
    """,
    # Tabla de Hechos
    """
    CREATE TABLE IF NOT EXISTS fact_produccion_limon (
        id_hecho              SERIAL PRIMARY KEY,
        id_tiempo             INT  NOT NULL REFERENCES dim_tiempo(id_tiempo),
        id_ubicacion          INT  NOT NULL REFERENCES dim_ubicacion(id_ubicacion),
        produccion_t          FLOAT   DEFAULT 0,
        cosecha_ha            FLOAT   DEFAULT 0,
        precio_chacra_kg      FLOAT,
        num_emergencias       INT     DEFAULT 0,
        total_afectados       INT     DEFAULT 0,
        has_cultivo_perdidas  FLOAT   DEFAULT 0,
        n_noticias            INT     DEFAULT 0,
        avg_sentimiento       FLOAT,
        temp_max_c            FLOAT,
        temp_min_c            FLOAT,
        precipitacion_mm      FLOAT,
        humedad_rel_pct       FLOAT,
        velocidad_viento      FLOAT,
        radiacion_solar       FLOAT,
        UNIQUE (id_tiempo, id_ubicacion)
    )
    """,
    # Índices
    "CREATE INDEX IF NOT EXISTS idx_fact_tiempo    ON fact_produccion_limon(id_tiempo)",
    "CREATE INDEX IF NOT EXISTS idx_fact_ubicacion ON fact_produccion_limon(id_ubicacion)",
]

try:
    engine = create_engine(PG_URI)
    with engine.connect() as conn:
        for stmt in DDL_STATEMENTS:
            conn.execute(text(stmt.strip()))
            conn.commit()
    print('  [OK] Tablas creadas: dim_tiempo, dim_ubicacion, fact_produccion_limon')
    print('  [OK] Índices creados: idx_fact_tiempo, idx_fact_ubicacion')

    # Verificar tablas
    with engine.connect() as conn:
        result = conn.execute(text(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'public' ORDER BY table_name"
        ))
        tablas = [r[0] for r in result]
    print(f'\n  Tablas en limon_analytics_db: {tablas}')
    engine.dispose()
except Exception as e:
    print(f'  [ERROR] {e}')
    sys.exit(1)

# TODO: INTEGRACIÓN DATA NASA
# Cuando se integre, añadir las columnas NASA a fact_produccion_limon si no existen:
#   ALTER TABLE fact_produccion_limon ADD COLUMN IF NOT EXISTS temp_max_c FLOAT;
#   ALTER TABLE fact_produccion_limon ADD COLUMN IF NOT EXISTS precipitacion_mm FLOAT;
#   -- Las columnas ya están definidas en el DDL inicial como NULL (placeholder)

print()
print('[ACTIVIDAD 8] COMPLETADA.')
print('  Descripcion: DB limon_analytics_db creada con Star Schema (3 tablas + 2 indices).')
print(f'  DB URI: {PG_URI}')
