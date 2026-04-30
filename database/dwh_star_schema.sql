-- =================================================================
-- STAR SCHEMA: limon_analytics_db
-- Tesis: Predicción de Producción de Limón — LSTM Multimodal
-- Generado por: Pipeline Fase 1 — Actividad 7
-- =================================================================

CREATE TABLE IF NOT EXISTS dim_tiempo (
    id_tiempo     SERIAL PRIMARY KEY,
    fecha_evento  VARCHAR(7)  NOT NULL UNIQUE,
    anho          SMALLINT    NOT NULL,
    mes           SMALLINT    NOT NULL,
    trimestre     SMALLINT,
    month_sin     FLOAT,
    month_cos     FLOAT
);

CREATE TABLE IF NOT EXISTS dim_ubicacion (
    id_ubicacion  SERIAL PRIMARY KEY,
    departamento  VARCHAR(60) NOT NULL,
    provincia     VARCHAR(60) NOT NULL,
    lat           FLOAT,
    lon           FLOAT,
    UNIQUE (departamento, provincia)
);

CREATE TABLE IF NOT EXISTS fact_produccion_limon (
    id_hecho              SERIAL PRIMARY KEY,
    id_tiempo             INT NOT NULL REFERENCES dim_tiempo(id_tiempo),
    id_ubicacion          INT NOT NULL REFERENCES dim_ubicacion(id_ubicacion),
    produccion_t          FLOAT DEFAULT 0,
    cosecha_ha            FLOAT DEFAULT 0,
    precio_chacra_kg      FLOAT,
    num_emergencias       INT   DEFAULT 0,
    total_afectados       INT   DEFAULT 0,
    has_cultivo_perdidas  FLOAT DEFAULT 0,
    n_noticias            INT   DEFAULT 0,
    avg_sentimiento       FLOAT,
    temp_max_c            FLOAT,
    temp_min_c            FLOAT,
    precipitacion_mm      FLOAT,
    humedad_rel_pct       FLOAT,
    velocidad_viento      FLOAT,
    radiacion_solar       FLOAT,
    UNIQUE (id_tiempo, id_ubicacion)
);

CREATE INDEX IF NOT EXISTS idx_fact_tiempo    ON fact_produccion_limon(id_tiempo);
CREATE INDEX IF NOT EXISTS idx_fact_ubicacion ON fact_produccion_limon(id_ubicacion);
