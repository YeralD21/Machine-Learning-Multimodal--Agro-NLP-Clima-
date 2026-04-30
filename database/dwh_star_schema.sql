-- =================================================================
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
