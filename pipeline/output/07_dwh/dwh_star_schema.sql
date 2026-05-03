
-- =================================================================
-- STAR SCHEMA: limon_analytics_db
-- Proyecto: Prediccion de Produccion de Limon — LSTM-Attention
-- Generado por: Pipeline Fase 1 — Actividad 7
-- =================================================================

-- 1. Dimension Tiempo
CREATE TABLE IF NOT EXISTS dim_tiempo (
    id_tiempo     SERIAL PRIMARY KEY,
    fecha_evento  VARCHAR(7)  NOT NULL UNIQUE,
    anho          SMALLINT    NOT NULL,
    mes           SMALLINT    NOT NULL,
    trimestre     SMALLINT    GENERATED ALWAYS AS (((mes - 1) / 3) + 1) STORED,
    nombre_mes    VARCHAR(20)
);

-- 2. Dimension Ubicacion
CREATE TABLE IF NOT EXISTS dim_ubicacion (
    id_ubicacion  SERIAL PRIMARY KEY,
    departamento  VARCHAR(60) NOT NULL,
    provincia     VARCHAR(60) NOT NULL,
    region_natural VARCHAR(20),
    UNIQUE (departamento, provincia)
);

-- 3. Dimension Clima (NASA POWER)
CREATE TABLE IF NOT EXISTS dim_clima (
    id_clima              SERIAL PRIMARY KEY,
    T2M                   FLOAT,
    T2M_MAX               FLOAT,
    T2M_MIN               FLOAT,
    PRECTOTCORR           FLOAT,
    RH2M                  FLOAT,
    QV2M                  FLOAT,
    ALLSKY_SFC_SW_DWN     FLOAT,
    WS2M                  FLOAT
);

-- 4. Dimension Emergencia (INDECI)
CREATE TABLE IF NOT EXISTS dim_emergencia (
    id_emergencia         SERIAL PRIMARY KEY,
    num_emergencias       INT     DEFAULT 0,
    total_afectados       INT     DEFAULT 0,
    has_cultivo_perdidas  FLOAT   DEFAULT 0
);

-- 5. Dimension Noticias (Agraria.pe)
CREATE TABLE IF NOT EXISTS dim_noticias (
    id_noticias           SERIAL PRIMARY KEY,
    n_noticias            INT     DEFAULT 0,
    avg_sentimiento       FLOAT   -- NULL en Fase 1, se llena en Fase 2 con BETO
);

-- 6. Tabla de Hechos
CREATE TABLE IF NOT EXISTS fact_produccion_limon (
    id_hecho              SERIAL PRIMARY KEY,
    id_tiempo             INT NOT NULL REFERENCES dim_tiempo(id_tiempo),
    id_ubicacion          INT NOT NULL REFERENCES dim_ubicacion(id_ubicacion),
    id_clima              INT REFERENCES dim_clima(id_clima),
    id_emergencia         INT REFERENCES dim_emergencia(id_emergencia),
    id_noticias           INT REFERENCES dim_noticias(id_noticias),
    produccion_t          FLOAT   DEFAULT 0,
    cosecha_ha            FLOAT   DEFAULT 0,
    precio_chacra_kg      FLOAT,
    UNIQUE (id_tiempo, id_ubicacion)
);

-- Indices para optimizar consultas de series temporales
CREATE INDEX IF NOT EXISTS idx_fact_tiempo     ON fact_produccion_limon(id_tiempo);
CREATE INDEX IF NOT EXISTS idx_fact_ubicacion  ON fact_produccion_limon(id_ubicacion);
CREATE INDEX IF NOT EXISTS idx_fact_clima      ON fact_produccion_limon(id_clima);
CREATE INDEX IF NOT EXISTS idx_fact_emergencia ON fact_produccion_limon(id_emergencia);
CREATE INDEX IF NOT EXISTS idx_fact_noticias   ON fact_produccion_limon(id_noticias);
CREATE INDEX IF NOT EXISTS idx_tiempo_fecha    ON dim_tiempo(fecha_evento);
CREATE INDEX IF NOT EXISTS idx_ubic_dpto_prov  ON dim_ubicacion(departamento, provincia);
