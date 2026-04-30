
-- =============================================
-- STAR SCHEMA v2.0 — limon_analytics_db
-- Proyecto: Prediccion de Produccion de Limon
-- 5 Dimensiones Puras + 1 Tabla de Hechos
-- =============================================

-- 1. Dimensiones secundarias (sin FKs propias)
CREATE TABLE IF NOT EXISTS dim_clima (
    id_clima SERIAL PRIMARY KEY,
    temp_max_c FLOAT, temp_min_c FLOAT, precipitacion_mm FLOAT,
    humedad_rel_pct FLOAT, velocidad_viento FLOAT, radiacion_solar FLOAT,
    is_extreme_weather BOOLEAN DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS dim_emergencia (
    id_emergencia SERIAL PRIMARY KEY,
    tipo_emergencia VARCHAR(100),
    num_emergencias INT DEFAULT 0,
    total_afectados INT DEFAULT 0,
    has_cultivo_perdidas FLOAT DEFAULT 0,
    gravedad SMALLINT
);

CREATE TABLE IF NOT EXISTS dim_noticias (
    id_noticias SERIAL PRIMARY KEY,
    avg_sentimiento FLOAT,
    n_noticias INT DEFAULT 0,
    tema_principal VARCHAR(100)
);

-- 2. Dimensiones base
CREATE TABLE IF NOT EXISTS dim_tiempo (
    id_tiempo SERIAL PRIMARY KEY,
    fecha_evento VARCHAR(7) NOT NULL UNIQUE,
    anho SMALLINT NOT NULL, mes SMALLINT NOT NULL,
    trimestre SMALLINT,
    month_sin FLOAT, month_cos FLOAT
);

CREATE TABLE IF NOT EXISTS dim_ubicacion (
    id_ubicacion SERIAL PRIMARY KEY,
    departamento VARCHAR(60) NOT NULL,
    provincia VARCHAR(60) NOT NULL,
    distrito VARCHAR(80),
    lat FLOAT, lon FLOAT,
    UNIQUE(departamento, provincia)
);

-- 3. Tabla de Hechos LIMPIA (solo FKs + metricas produccion)
CREATE TABLE IF NOT EXISTS fact_produccion_limon (
    id_hecho SERIAL PRIMARY KEY,
    id_tiempo INT REFERENCES dim_tiempo(id_tiempo),
    id_ubicacion INT REFERENCES dim_ubicacion(id_ubicacion),
    id_clima INT REFERENCES dim_clima(id_clima),
    id_emergencia INT REFERENCES dim_emergencia(id_emergencia),
    id_noticias INT REFERENCES dim_noticias(id_noticias),
    -- Metricas MIDAGRI (unicas metricas en la tabla de hechos)
    produccion_t FLOAT DEFAULT 0,
    cosecha_ha FLOAT DEFAULT 0,
    precio_chacra_kg FLOAT,
    UNIQUE(id_tiempo, id_ubicacion)
);

-- Indices
CREATE INDEX IF NOT EXISTS idx_fact_tiempo ON fact_produccion_limon(id_tiempo);
CREATE INDEX IF NOT EXISTS idx_fact_ubicacion ON fact_produccion_limon(id_ubicacion);
CREATE INDEX IF NOT EXISTS idx_fact_clima ON fact_produccion_limon(id_clima);
