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
```"""),
('md', """## 7.1 Modelo Estrella — Diagrama Multimodal (4 Dimensiones)

Este diseño separa las fuentes de datos en 4 dimensiones clave para permitir un análisis granular del impacto climático y social en la producción de limón.

### Vista Conceptual (Esquema de la Tesis)
![Star Schema Diagram](../data/04_reports/g07_star_schema_v2.png)

---

### Vista Técnica (Modelo Dimensional)
```mermaid
erDiagram
    fact_produccion_limon }|--|| dim_tiempo : "FK_Tiempo"
    fact_produccion_limon }|--|| dim_ubicacion : "FK_Ubicacion"
    fact_produccion_limon }|--|| dim_clima : "FK_Clima"
    fact_produccion_limon }|--|| dim_multimodal : "FK_Multimodal"

    dim_tiempo {
        int id_tiempo PK
        varchar fecha_evento "YYYY-MM"
        int anho
        int mes
        float month_sin "Estacionalidad"
        float month_cos "Estacionalidad"
    }

    dim_ubicacion {
        int id_ubicacion PK
        varchar departamento "Región"
        varchar provincia
        float lat
        float lon
    }

    dim_clima {
        int id_clima PK
        float temp_max_c "NASA"
        float temp_min_c "NASA"
        float precipitacion_mm "NASA"
        float radiacion_solar "NASA"
    }

    dim_multimodal {
        int id_multimodal PK
        int n_noticias "NLP"
        int num_emergencias "INDECI"
        float avg_sentimiento "NLP"
        int total_afectados "INDECI"
    }

    fact_produccion_limon {
        int id_hecho PK
        int id_tiempo FK
        int id_ubicacion FK
        int id_clima FK
        int id_multimodal FK
        float produccion_t "Métrica Principal"
        float cosecha_ha
        float precio_chacra_kg "Volatilidad"
    }
```

---

## 7.2 Justificación del Diseño

| Decisión | Justificación |
|:---------|:-------------|
| **Star Schema** | Optimiza consultas analíticas con JOINs simples. Ideal para alimentar el modelo LSTM-Attention. |
| **Granularidad mensual** | La producción agrícola se reporta mensualmente. NASA e INDECI se agregan a este nivel. |
| **Dimensión Clima** | Independiza las variables de la NASA para permitir análisis histórico de correlación. |
| **Dimensión Multimodal** | Consolida el riesgo social (Noticias) y desastres (INDECI) en una sola jerarquía de impacto. |

---

## 7.3 Relaciones y Cardinalidad

| Relación | Tipo | Descripción |
|:---------|:-----|:------------|
| `fact → dim_tiempo` | N:1 | Muchos registros de producción pueden compartir el mismo mes |
| `fact → dim_ubicacion` | N:1 | Muchos registros de producción pueden compartir la misma provincia |
| `fact → dim_clima` | 1:1 | Cada registro de producción tiene un perfil climático único |
| `fact → dim_multimodal` | 1:1 | Cada registro tiene un contexto de noticias y emergencias asociado |
""")
]
nb(act07, "actividad_07_dwh_schema.ipynb")

# ── ACTIVIDAD 08 — POSTGRESQL ────────────────────────────────────────
act08 = [
('md', """# 🗄️ Actividad 08: Crear Esquemas en PostgreSQL
---
> **NOTA:** Este notebook es auto-generado. Los cambios manuales deben sincronizarse en `gen_nb_07_08.py`.

**Objetivo:** Crear la base de datos `limon_analytics_db` y las tablas del Star Schema de 4 Dimensiones.  
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

('md', """## 8.2 Estructura del Data Warehouse (Star Schema)

| Tabla | Tipo | Descripción |
|:------|:-----|:------------|
| **`fact_produccion_limon`** | Hechos | Contiene las métricas de producción y las llaves a dimensiones. |
| **`dim_tiempo`** | Dimensión | Jerarquía temporal: Mes, Año, Trimestre y Codificación Cíclica. |
| **`dim_ubicacion`** | Dimensión | Jerarquía geográfica: Departamento, Provincia y Coordenadas. |
| **`dim_clima`** | Dimensión | Datos de la NASA POWER (Temperatura, Precipitación, Radiación). |
| **`dim_multimodal`** | Dimensión | Impacto externo (Noticias NLP + Emergencias INDECI). |

## 8.3 Crear Tablas en PostgreSQL"""),

('code', """
DDL_STMTS = [
    "CREATE TABLE IF NOT EXISTS dim_tiempo (id_tiempo SERIAL PRIMARY KEY, fecha_evento VARCHAR(7) NOT NULL, anho SMALLINT NOT NULL, mes SMALLINT NOT NULL, trimestre SMALLINT, month_sin FLOAT, month_cos FLOAT, UNIQUE(fecha_evento))",
    "CREATE TABLE IF NOT EXISTS dim_ubicacion (id_ubicacion SERIAL PRIMARY KEY, departamento VARCHAR(60) NOT NULL, provincia VARCHAR(60) NOT NULL, lat FLOAT, lon FLOAT, UNIQUE(departamento, provincia))",
    "CREATE TABLE IF NOT EXISTS dim_clima (id_clima SERIAL PRIMARY KEY, temp_max_c FLOAT, temp_min_c FLOAT, precipitacion_mm FLOAT, humedad_rel_pct FLOAT, radiacion_solar FLOAT)",
    "CREATE TABLE IF NOT EXISTS dim_multimodal (id_multimodal SERIAL PRIMARY KEY, n_noticias INT DEFAULT 0, num_emergencias INT DEFAULT 0, total_afectados INT DEFAULT 0, avg_sentimiento FLOAT)",
    '''CREATE TABLE IF NOT EXISTS fact_produccion_limon (
        id_hecho SERIAL PRIMARY KEY,
        id_tiempo INT NOT NULL REFERENCES dim_tiempo(id_tiempo),
        id_ubicacion INT NOT NULL REFERENCES dim_ubicacion(id_ubicacion),
        id_clima INT REFERENCES dim_clima(id_clima),
        id_multimodal INT REFERENCES dim_multimodal(id_multimodal),
        produccion_t FLOAT DEFAULT 0, cosecha_ha FLOAT DEFAULT 0, precio_chacra_kg FLOAT,
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
    print("  [OK] Tablas creadas: dim_tiempo, dim_ubicacion, dim_clima, dim_multimodal, fact_produccion_limon")
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
