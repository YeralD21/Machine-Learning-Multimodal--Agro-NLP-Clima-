-- =================================================================
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
