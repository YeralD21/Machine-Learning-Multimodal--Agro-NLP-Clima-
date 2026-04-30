"""
==========================================================================
NASA POWER Pipeline — Actividad 7: Diseño del Esquema DWH
==========================================================================
Define el esquema de la tabla dim_clima y cómo se conecta a la tabla
de hechos fact_produccion_limon mediante id_ubicacion e id_tiempo.

Genera el DDL SQL para:
  - dim_clima: dimensión climática con las 8 variables NASA
  - ALTER TABLE para añadir columnas climáticas a fact_produccion_limon

Salida: database/dwh_nasa_clima_schema.sql
==========================================================================
"""
import sys
sys.stdout.reconfigure(encoding='utf-8')

import os
import json

with open('data/02_interim_nasa/nasa_pipeline_config.json', 'r', encoding='utf-8') as f:
    CONFIG = json.load(f)
DIRS = CONFIG['DIRS']

print('=' * 70)
print('  NASA PIPELINE — ACTIVIDAD 7: Diseño del Esquema DWH')
print('=' * 70)

# =========================================================================
# DISEÑO DEL STAR SCHEMA — EXTENSIÓN CLIMÁTICA
# =========================================================================
#
# ESQUEMA ACTUAL (pipeline principal):
# ─────────────────────────────────────
#   fact_produccion_limon
#     ├── id_tiempo    → dim_tiempo (fecha_evento, anho, mes, ...)
#     └── id_ubicacion → dim_ubicacion (departamento, provincia, lat, lon)
#
# EXTENSIÓN NASA (esta actividad):
# ─────────────────────────────────
#   OPCIÓN A — Columnas directas en fact_produccion_limon (ELEGIDA):
#     Se añaden las 8 variables climáticas directamente a la tabla de hechos.
#     Ventaja: un solo JOIN para obtener todos los datos.
#     Desventaja: tabla más ancha.
#
#   OPCIÓN B — Tabla dim_clima separada:
#     dim_clima (id_clima PK, id_tiempo FK, id_ubicacion FK, T2M, ...)
#     Ventaja: separación de concerns.
#     Desventaja: JOIN adicional.
#
# DECISIÓN: Se implementan AMBAS opciones en el DDL.
# El pipeline ETL (Actividad 9) usará la Opción A (columnas directas)
# por simplicidad de consulta para el modelo LSTM.
# =========================================================================

print('\n  Diseño del esquema:')
print()
print('  fact_produccion_limon')
print('    ├── id_tiempo    → dim_tiempo')
print('    ├── id_ubicacion → dim_ubicacion')
print('    ├── [MIDAGRI]    produccion_t, cosecha_ha, precio_chacra_kg')
print('    ├── [INDECI]     num_emergencias, total_afectados, has_cultivo_perdidas')
print('    ├── [NOTICIAS]   n_noticias, avg_sentimiento')
print('    └── [NASA]       allsky_sfc_sw_dwn, prectotcorr, qv2m, rh2m,')
print('                     t2m, t2m_max, t2m_min, ws2m')
print()
print('  dim_clima (tabla separada — Opción B)')
print('    ├── id_clima     PK SERIAL')
print('    ├── id_tiempo    FK → dim_tiempo')
print('    ├── id_ubicacion FK → dim_ubicacion')
print('    └── [NASA]       allsky_sfc_sw_dwn, prectotcorr, qv2m, rh2m,')
print('                     t2m, t2m_max, t2m_min, ws2m')

sql_ddl = """-- =================================================================
-- EXTENSIÓN NASA POWER — limon_analytics_db
-- Proyecto: Predicción de Producción de Limón — LSTM Multimodal
-- Generado por: NASA Pipeline — Actividad 7
-- =================================================================

-- ─────────────────────────────────────────────────────────────────
-- OPCIÓN A: Añadir columnas climáticas directamente a fact_produccion_limon
-- (Recomendada para el modelo LSTM — un solo JOIN)
-- ─────────────────────────────────────────────────────────────────

ALTER TABLE fact_produccion_limon
    ADD COLUMN IF NOT EXISTS allsky_sfc_sw_dwn  FLOAT,   -- Radiación solar (MJ/m²/día)
    ADD COLUMN IF NOT EXISTS prectotcorr         FLOAT,   -- Precipitación corregida (mm/mes)
    ADD COLUMN IF NOT EXISTS qv2m                FLOAT,   -- Humedad específica a 2m (g/kg)
    ADD COLUMN IF NOT EXISTS rh2m                FLOAT,   -- Humedad relativa a 2m (%)
    ADD COLUMN IF NOT EXISTS t2m                 FLOAT,   -- Temperatura media a 2m (°C)
    ADD COLUMN IF NOT EXISTS t2m_max             FLOAT,   -- Temperatura máxima a 2m (°C)
    ADD COLUMN IF NOT EXISTS t2m_min             FLOAT,   -- Temperatura mínima a 2m (°C)
    ADD COLUMN IF NOT EXISTS ws2m                FLOAT;   -- Velocidad del viento a 2m (m/s)

COMMENT ON COLUMN fact_produccion_limon.allsky_sfc_sw_dwn  IS 'NASA POWER: Radiación solar superficial (MJ/m²/día)';
COMMENT ON COLUMN fact_produccion_limon.prectotcorr         IS 'NASA POWER: Precipitación corregida acumulada mensual (mm)';
COMMENT ON COLUMN fact_produccion_limon.qv2m                IS 'NASA POWER: Humedad específica a 2 metros (g/kg)';
COMMENT ON COLUMN fact_produccion_limon.rh2m                IS 'NASA POWER: Humedad relativa a 2 metros (%)';
COMMENT ON COLUMN fact_produccion_limon.t2m                 IS 'NASA POWER: Temperatura media a 2 metros (°C)';
COMMENT ON COLUMN fact_produccion_limon.t2m_max             IS 'NASA POWER: Temperatura máxima a 2 metros (°C)';
COMMENT ON COLUMN fact_produccion_limon.t2m_min             IS 'NASA POWER: Temperatura mínima a 2 metros (°C)';
COMMENT ON COLUMN fact_produccion_limon.ws2m                IS 'NASA POWER: Velocidad del viento a 2 metros (m/s)';


-- ─────────────────────────────────────────────────────────────────
-- OPCIÓN B: Tabla dim_clima separada
-- (Alternativa para análisis climático independiente)
-- ─────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS dim_clima (
    id_clima             SERIAL PRIMARY KEY,
    id_tiempo            INT    NOT NULL REFERENCES dim_tiempo(id_tiempo),
    id_ubicacion         INT    NOT NULL REFERENCES dim_ubicacion(id_ubicacion),

    -- Variables NASA POWER
    allsky_sfc_sw_dwn    FLOAT,   -- Radiación solar (MJ/m²/día)
    prectotcorr          FLOAT,   -- Precipitación corregida (mm/mes)
    qv2m                 FLOAT,   -- Humedad específica a 2m (g/kg)
    rh2m                 FLOAT,   -- Humedad relativa a 2m (%)
    t2m                  FLOAT,   -- Temperatura media a 2m (°C)
    t2m_max              FLOAT,   -- Temperatura máxima a 2m (°C)
    t2m_min              FLOAT,   -- Temperatura mínima a 2m (°C)
    ws2m                 FLOAT,   -- Velocidad del viento a 2m (m/s)

    -- Metadatos de la fuente
    fuente               VARCHAR(20) DEFAULT 'NASA_POWER',
    fecha_carga          TIMESTAMP   DEFAULT CURRENT_TIMESTAMP,

    UNIQUE (id_tiempo, id_ubicacion)
);

-- Índices para optimizar JOINs con la tabla de hechos
CREATE INDEX IF NOT EXISTS idx_dim_clima_tiempo     ON dim_clima(id_tiempo);
CREATE INDEX IF NOT EXISTS idx_dim_clima_ubicacion  ON dim_clima(id_ubicacion);

COMMENT ON TABLE dim_clima IS 'Dimensión climática NASA POWER — variables mensuales por ubicación';


-- ─────────────────────────────────────────────────────────────────
-- VISTA: Consulta unificada para el modelo LSTM
-- Combina producción + clima en una sola vista
-- ─────────────────────────────────────────────────────────────────

CREATE OR REPLACE VIEW v_lstm_features AS
SELECT
    dt.fecha_evento,
    dt.anho,
    dt.mes,
    dt.trimestre,
    dt.month_sin,
    dt.month_cos,
    du.departamento,
    du.provincia,
    du.lat,
    du.lon,
    -- Métricas agrícolas
    f.produccion_t,
    f.cosecha_ha,
    f.precio_chacra_kg,
    -- Métricas de emergencias
    f.num_emergencias,
    f.total_afectados,
    f.has_cultivo_perdidas,
    -- Métricas NLP
    f.n_noticias,
    f.avg_sentimiento,
    -- Métricas climáticas NASA
    f.allsky_sfc_sw_dwn,
    f.prectotcorr,
    f.qv2m,
    f.rh2m,
    f.t2m,
    f.t2m_max,
    f.t2m_min,
    f.ws2m
FROM fact_produccion_limon f
JOIN dim_tiempo    dt ON f.id_tiempo    = dt.id_tiempo
JOIN dim_ubicacion du ON f.id_ubicacion = du.id_ubicacion
ORDER BY dt.fecha_evento, du.departamento, du.provincia;

COMMENT ON VIEW v_lstm_features IS 'Vista unificada para entrenamiento del modelo LSTM-Attention';
"""

# Guardar DDL
os.makedirs('database', exist_ok=True)
sql_path = 'database/dwh_nasa_clima_schema.sql'
with open(sql_path, 'w', encoding='utf-8') as f:
    f.write(sql_ddl)

print(f'\n  [OK] DDL guardado: {sql_path}')
print()
print('[ACTIVIDAD 7] COMPLETADA.')
print('  Descripción: Diseño del Star Schema extendido con variables climáticas NASA.')
print(f'  Archivo generado: {sql_path}')
print('  Tablas/vistas: dim_clima, ALTER fact_produccion_limon, v_lstm_features')
