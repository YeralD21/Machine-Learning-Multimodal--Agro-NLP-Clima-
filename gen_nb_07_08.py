"""Genera notebooks 07 y 08."""
import nbformat as nbf, os, sys
sys.stdout.reconfigure(encoding='utf-8')
NOTEBOOKS_DIR = "notebooks"

def nb(cells, filename):
    n = nbf.v4.new_notebook()
    n.metadata['kernelspec'] = {'display_name':'Python 3','language':'python','name':'python3'}
    for t, s in cells:
        n.cells.append(nbf.v4.new_markdown_cell(s) if t=='md' else nbf.v4.new_code_cell(s))
    path = os.path.join(NOTEBOOKS_DIR, filename)
    with open(path, 'w', encoding='utf-8') as f: nbf.write(n, f)
    print(f"[OK] {path}")

SETUP = """
import os, sys, json, warnings
import pandas as pd
from sqlalchemy import create_engine, text
warnings.filterwarnings('ignore')
if os.path.basename(os.getcwd()) == 'notebooks':
    os.chdir(os.path.abspath('..'))
with open('data/02_interim/pipeline_config.json','r',encoding='utf-8') as f:
    CFG = json.load(f)
DIRS=CFG['DIRS']; INTERIM=DIRS['interim']; PG_URI=CFG['PG_URI']
print(f"CWD: {os.getcwd()} | Config OK")
"""

# ── ACTIVIDAD 07 — DISEÑO STAR SCHEMA ────────────────────────────────
act07 = [
('md', """# ⭐ Actividad 07: Diseño del Star Schema — Data Warehouse
---
**Objetivo:** Documentar visualmente el modelo dimensional (Star Schema) para `limon_analytics_db`.

Este es el diseño que soportará las consultas analíticas del modelo LSTM-Attention, permitiendo
cruzar producción agrícola con emergencias, noticias y datos climáticos.

---

## 7.1 Modelo Estrella — Diagrama Visual

![Star Schema Diagram](../data/04_reports/g07_star_schema.png)

```
                    ┌─────────────────────────────┐
                    │        dim_tiempo            │
                    │─────────────────────────────│
                    │ id_tiempo (PK)              │
                    │ fecha_evento    VARCHAR(7)   │
                    │ anho            SMALLINT     │
                    │ mes             SMALLINT     │
                    │ trimestre       SMALLINT     │
                    │ month_sin       FLOAT        │
                    │ month_cos       FLOAT        │
                    └──────────┬──────────────────┘
                               │
                               │ 1:N
                               │
┌─────────────────────────────┐│┌────────────────────────────────────────────┐
│      dim_ubicacion          │││         fact_produccion_limon               │
│─────────────────────────────│││────────────────────────────────────────────│
│ id_ubicacion (PK)           │││ id_hecho (PK)                             │
│ departamento  VARCHAR(60)   ├┤│ id_tiempo (FK) ─────────────► dim_tiempo  │
│ provincia     VARCHAR(60)   │││ id_ubicacion (FK) ──────────► dim_ubicacion│
│ lat           FLOAT         │││                                            │
│ lon           FLOAT         │││ ── MÉTRICAS AGRÍCOLAS (MIDAGRI) ──        │
└─────────────────────────────┘││ produccion_t         FLOAT                │
                               ││ cosecha_ha           FLOAT                │
                               ││ precio_chacra_kg     FLOAT                │
                               ││                                            │
                               ││ ── MÉTRICAS EMERGENCIAS (INDECI) ──       │
                               ││ num_emergencias      INT                  │
                               ││ total_afectados      INT                  │
                               ││ has_cultivo_perdidas  FLOAT               │
                               ││                                            │
                               ││ ── MÉTRICAS NOTICIAS (AGRARIA.PE) ──      │
                               ││ n_noticias           INT                  │
                               ││ avg_sentimiento      FLOAT  ◄── Fase 2   │
                               ││                                            │
                               ││ ── MÉTRICAS CLIMÁTICAS (NASA) ──          │
                               ││ temp_max_c           FLOAT  ◄── TODO     │
                               ││ temp_min_c           FLOAT  ◄── TODO     │
                               ││ precipitacion_mm     FLOAT  ◄── TODO     │
                               ││ humedad_rel_pct      FLOAT  ◄── TODO     │
                               ││ velocidad_viento     FLOAT  ◄── TODO     │
                               ││ radiacion_solar      FLOAT  ◄── TODO     │
                               │└────────────────────────────────────────────┘

    LLAVE PRIMARIA COMPUESTA: (id_tiempo, id_ubicacion) → UNIQUE
```

---

## 7.2 Justificación del Diseño

| Decisión | Justificación |
|:---------|:-------------|
| **Star Schema** (no Snowflake) | Optimiza consultas analíticas con JOINs simples. Ideal para OLAP y alimentar LSTM. |
| **Granularidad mensual** | La producción agrícola se reporta mensualmente. INDECI y noticias se agregan a este nivel. |
| **dim_tiempo con month_sin/cos** | La codificación cíclica captura la estacionalidad biológica del limón sin discontinuidad diciembre→enero. |
| **dim_ubicacion con lat/lon** | Permite futuras integraciones espaciales (NASA POWER por coordenadas). |
| **Columnas NASA como NULL** | Placeholder listo. Las columnas existen en la tabla pero se llenan cuando se integren datos climáticos. |
| **avg_sentimiento como NULL** | Se llenará en Fase 2 cuando se ejecute NLP (BETO/pysentimiento) sobre las noticias. |

---

## 7.3 Relaciones y Cardinalidad

| Relación | Tipo | Descripción |
|:---------|:-----|:------------|
| `fact → dim_tiempo` | N:1 | Muchos registros de producción pueden compartir el mismo mes |
| `fact → dim_ubicacion` | N:1 | Muchos registros de producción pueden compartir la misma provincia |
| **Unicidad** | `(id_tiempo, id_ubicacion)` | Cada combinación mes-provincia tiene exactamente un registro |
"""),

('code', SETUP),

('md', "## 7.4 Generación del Script DDL"),

('code', """
DDL = '''-- =================================================================
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
'''

sql_path = f"{DIRS['database']}/dwh_star_schema.sql"
with open(sql_path, 'w', encoding='utf-8') as f:
    f.write(DDL)
print(f"[OK] DDL guardado: {sql_path}")
print("\\nContenido del DDL:")
print(DDL)
print("[ACTIVIDAD 07] COMPLETADA.")
"""),

('md', """## TODO: INTEGRACIÓN DATA NASA (COMPAÑERO)

Cuando integres datos NASA, las columnas ya están reservadas en `fact_produccion_limon`:

| Columna en fact | Variable NASA POWER | Unidad |
|:---------------|:-------------------|:-------|
| `temp_max_c` | T2M_MAX | °C |
| `temp_min_c` | T2M_MIN | °C |
| `precipitacion_mm` | PRECTOTCORR | mm/día |
| `humedad_rel_pct` | RH2M | % |
| `velocidad_viento` | WS2M | m/s |
| `radiacion_solar` | ALLSKY_SFC_SW_DWN | kW-hr/m²/day |

Solo necesitas hacer `UPDATE` o reinsertar con los valores.
"""),
]
nb(act07, "actividad_07_dwh_schema.ipynb")

# ── ACTIVIDAD 08 — POSTGRESQL ────────────────────────────────────────
act08 = [
('md', """# 🗄️ Actividad 08: Crear Esquemas en PostgreSQL
---
**Objetivo:** Crear la base de datos `limon_analytics_db` y las tablas del Star Schema.  
**Conexión:** `postgresql://postgres:postgres@localhost:5432/limon_analytics_db`
"""),

('code', SETUP),

('md', "## 8.1 Crear Base de Datos"),

('code', """
PG_BASE = PG_URI.rsplit('/',1)[0] + '/postgres'
print(f"Conectando a: {PG_BASE}")

try:
    engine_base = create_engine(PG_BASE, isolation_level='AUTOCOMMIT')
    with engine_base.connect() as conn:
        exists = conn.execute(text("SELECT 1 FROM pg_database WHERE datname='limon_analytics_db'")).fetchone()
        if not exists:
            conn.execute(text("CREATE DATABASE limon_analytics_db ENCODING 'UTF8'"))
            print("  [OK] Base de datos limon_analytics_db CREADA.")
        else:
            print("  [OK] Base de datos limon_analytics_db ya existe.")
    engine_base.dispose()
except Exception as e:
    print(f"  [ERROR] {e}")
    print("  Asegúrate de que PostgreSQL esté corriendo en localhost:5432")
"""),

('md', "## 8.2 Crear Tablas del Star Schema"),

('code', """
DDL_STMTS = [
    "CREATE TABLE IF NOT EXISTS dim_tiempo (id_tiempo SERIAL PRIMARY KEY, fecha_evento VARCHAR(7) NOT NULL, anho SMALLINT NOT NULL, mes SMALLINT NOT NULL, trimestre SMALLINT, month_sin FLOAT, month_cos FLOAT, UNIQUE(fecha_evento))",
    "CREATE TABLE IF NOT EXISTS dim_ubicacion (id_ubicacion SERIAL PRIMARY KEY, departamento VARCHAR(60) NOT NULL, provincia VARCHAR(60) NOT NULL, lat FLOAT, lon FLOAT, UNIQUE(departamento, provincia))",
    '''CREATE TABLE IF NOT EXISTS fact_produccion_limon (
        id_hecho SERIAL PRIMARY KEY,
        id_tiempo INT NOT NULL REFERENCES dim_tiempo(id_tiempo),
        id_ubicacion INT NOT NULL REFERENCES dim_ubicacion(id_ubicacion),
        produccion_t FLOAT DEFAULT 0, cosecha_ha FLOAT DEFAULT 0, precio_chacra_kg FLOAT,
        num_emergencias INT DEFAULT 0, total_afectados INT DEFAULT 0,
        has_cultivo_perdidas FLOAT DEFAULT 0, n_noticias INT DEFAULT 0,
        avg_sentimiento FLOAT,
        temp_max_c FLOAT, temp_min_c FLOAT, precipitacion_mm FLOAT,
        humedad_rel_pct FLOAT, velocidad_viento FLOAT, radiacion_solar FLOAT,
        UNIQUE(id_tiempo, id_ubicacion))''',
    "CREATE INDEX IF NOT EXISTS idx_fact_tiempo ON fact_produccion_limon(id_tiempo)",
    "CREATE INDEX IF NOT EXISTS idx_fact_ubicacion ON fact_produccion_limon(id_ubicacion)",
]

try:
    engine = create_engine(PG_URI)
    with engine.connect() as conn:
        for stmt in DDL_STMTS:
            conn.execute(text(stmt.strip()))
            conn.commit()
    print("  [OK] Tablas creadas: dim_tiempo, dim_ubicacion, fact_produccion_limon")
    print("  [OK] Índices creados: idx_fact_tiempo, idx_fact_ubicacion")
    
    with engine.connect() as conn:
        tablas = [r[0] for r in conn.execute(text(
            "SELECT table_name FROM information_schema.tables WHERE table_schema='public' ORDER BY table_name"
        ))]
    print(f"\\n  Tablas en limon_analytics_db: {tablas}")
    engine.dispose()
except Exception as e:
    print(f"  [ERROR] {e}")

print("\\n[ACTIVIDAD 08] COMPLETADA.")
"""),

('md', """## TODO: INTEGRACIÓN DATA NASA (COMPAÑERO)
Las columnas NASA ya están creadas como `NULL` en `fact_produccion_limon`.
Cuando tengas datos, usa `UPDATE` para llenarlas:
```sql
UPDATE fact_produccion_limon f
SET temp_max_c = nasa.t2m_max,
    precipitacion_mm = nasa.prectotcorr
FROM staging_nasa nasa
WHERE f.id_tiempo = nasa.id_tiempo
  AND f.id_ubicacion = nasa.id_ubicacion;
```
"""),
]
nb(act08, "actividad_08_postgresql.ipynb")

print("\n✅ Notebooks 07 y 08 generados.")
