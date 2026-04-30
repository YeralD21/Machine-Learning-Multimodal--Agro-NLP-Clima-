"""
Generador de Notebooks: Actividades 1 y 2 del Pipeline de Tesis.
Crea archivos .ipynb con formato correcto usando nbformat.
"""
import nbformat as nbf
import os
import sys

sys.stdout.reconfigure(encoding='utf-8')

NOTEBOOKS_DIR = "notebooks"
os.makedirs(NOTEBOOKS_DIR, exist_ok=True)

def create_notebook(cells_data, filename):
    """Crea un notebook a partir de una lista de (tipo, source)."""
    nb = nbf.v4.new_notebook()
    nb.metadata['kernelspec'] = {
        'display_name': 'Python 3',
        'language': 'python',
        'name': 'python3'
    }
    for cell_type, source in cells_data:
        if cell_type == 'md':
            nb.cells.append(nbf.v4.new_markdown_cell(source))
        else:
            nb.cells.append(nbf.v4.new_code_cell(source))
    
    path = os.path.join(NOTEBOOKS_DIR, filename)
    with open(path, 'w', encoding='utf-8') as f:
        nbf.write(nb, f)
    print(f"[OK] Notebook creado: {path}")

# =====================================================================
# ACTIVIDAD 1: CONFIGURACIÓN DEL PROYECTO
# =====================================================================
act1_cells = [
    ('md', """# 🔧 Actividad 1: Configuración del Proyecto Python
---
**Tesis:** Predicción de Producción de Limón mediante LSTM Multimodal  
**Pipeline:** Fase 1 — Ingeniería de Datos  
**Objetivo:** Configurar el entorno de trabajo, librerías y estructura de carpetas para garantizar la reproducibilidad total del pipeline.

### Fuentes de Datos del Proyecto
| Fuente | Tipo | Archivo |
|:-------|:-----|:--------|
| MIDAGRI (SISAGRI) | Producción agrícola | `Sisagri_2016_2025.xlsx` |
| INDECI (SINPAD) | Emergencias y peligros | `resumen_emergencias_2003_2024.xlsx`, DBFs 2021-2023 |
| Agraria.pe | Noticias agrícolas | `agro_news_2021.csv` a `agro_news_2025.csv` |
| NASA POWER | Variables climáticas | `nasa_climatic_raw_values.csv`, `nasa_long_raw.csv` |
"""),

    ('code', """# ==========================================================================
# 1.1 Importación de Librerías
# ==========================================================================
import os
import sys
import shutil
import glob
import warnings

# Data Science
import pandas as pd
import numpy as np

# Visualización
import matplotlib.pyplot as plt
import seaborn as sns

# Base de Datos
from sqlalchemy import create_engine, text

# NLP & ML
from sklearn.preprocessing import StandardScaler

# Lectura de archivos DBF (INDECI SINPAD)
from dbfread import DBF

# Serialización
import joblib

# Configuración global
warnings.filterwarnings('ignore')
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', 100)
pd.set_option('display.width', 200)
sns.set_theme(style='whitegrid', palette='muted')

# Navegar a la raíz del proyecto
if os.path.basename(os.getcwd()) == 'notebooks':
    os.chdir(os.path.abspath('..'))
print(f"Directorio de trabajo: {os.getcwd()}")

print("✅ Todas las librerías cargadas correctamente.")
"""),

    ('code', """# ==========================================================================
# 1.2 Creación de la Estructura de Carpetas
# ==========================================================================
# Definición de la arquitectura del proyecto (convención Data Engineering)
DIRS = {
    'raw':       os.path.join('data', '01_raw'),
    'raw_midagri': os.path.join('data', '01_raw', 'midagri'),
    'raw_indeci':  os.path.join('data', '01_raw', 'indeci'),
    'raw_news':    os.path.join('data', '01_raw', 'agraria_pe'),
    'interim':   os.path.join('data', '02_interim'),
    'processed': os.path.join('data', '03_processed'),
    'reports':   os.path.join('data', '04_reports'),
    'database':  'database',
    'scalers':   os.path.join('models', 'scalers'),
    'raw_nasa':       os.path.join('data', '01_raw', 'nasapower'),
    'interim_nasa':   os.path.join('data', '02_interim_nasa'),
    'processed_nasa': os.path.join('data', '03_processed_nasa'),
}

for key, path in DIRS.items():
    os.makedirs(path, exist_ok=True)
    print(f"  📁 {path}")

print("\\n✅ Estructura de carpetas creada exitosamente (incluyendo NASA).")
"""),

    ('code', """# ==========================================================================
# 1.3 Copia de Archivos Fuente a 01_raw/
# ==========================================================================
# Copiar desde la estructura anterior a la nueva, manteniendo trazabilidad.

# --- MIDAGRI ---
src_midagri = os.path.join('data', 'raw', 'midagri', 'Sisagri_2016_2025.xlsx')
dst_midagri = os.path.join(DIRS['raw_midagri'], 'Sisagri_2016_2025.xlsx')
if os.path.exists(src_midagri) and not os.path.exists(dst_midagri):
    shutil.copy2(src_midagri, dst_midagri)
    print(f"  ✅ MIDAGRI copiado: {dst_midagri}")
elif os.path.exists(dst_midagri):
    print(f"  ℹ️  MIDAGRI ya existe en destino.")
else:
    print(f"  ⚠️  MIDAGRI no encontrado en: {src_midagri}")

# --- INDECI ---
indeci_files = [
    'resumen_emergencias_2003_2024.xlsx',
    'resumen_peligros_2003_2024.xlsx',
    'piura_emergencias.xlsx',
    'piura_peligros.xlsx',
]
for fname in indeci_files:
    src = os.path.join('data', 'raw', 'indeci', fname)
    dst = os.path.join(DIRS['raw_indeci'], fname)
    if os.path.exists(src) and not os.path.exists(dst):
        shutil.copy2(src, dst)
        print(f"  ✅ INDECI copiado: {fname}")
    elif os.path.exists(dst):
        print(f"  ℹ️  INDECI ya existe: {fname}")

# Copiar carpetas DBF (shapefiles extraídos)
for year_folder in ['E_2021', 'E_2022', 'E_2023']:
    src_dir = os.path.join('data', 'raw', 'indeci', year_folder)
    dst_dir = os.path.join(DIRS['raw_indeci'], year_folder)
    if os.path.exists(src_dir) and not os.path.exists(dst_dir):
        shutil.copytree(src_dir, dst_dir)
        print(f"  ✅ INDECI DBF copiado: {year_folder}/")
    elif os.path.exists(dst_dir):
        print(f"  ℹ️  INDECI DBF ya existe: {year_folder}/")

# --- AGRARIA.PE ---
for csv_file in glob.glob(os.path.join('data', 'raw', 'agraria_pe', 'agro_news_*.csv')):
    fname = os.path.basename(csv_file)
    dst = os.path.join(DIRS['raw_news'], fname)
    if not os.path.exists(dst):
        shutil.copy2(csv_file, dst)
        print(f"  ✅ AGRARIA copiado: {fname}")
    else:
        print(f"  ℹ️  AGRARIA ya existe: {fname}")

# --- NASA POWER ---
nasa_files = glob.glob(os.path.join('data', 'raw', 'nasapower', '*.csv'))
for src in nasa_files:
    fname = os.path.basename(src)
    dst = os.path.join(DIRS['raw_nasa'], fname)
    if not os.path.exists(dst):
        shutil.copy2(src, dst)
        print(f"  ✅ NASA copiado: {fname}")

print("\\n✅ Todos los archivos fuente (MIDAGRI, INDECI, Noticias, NASA) sincronizados.")
"""),

    ('code', """# ==========================================================================
# 1.4 Definición de Constantes del Proyecto
# ==========================================================================
# Estas constantes se reutilizarán en todos los notebooks.

# --- Rango temporal del estudio ---
ANHO_INICIO = 2021
ANHO_FIN = 2025
MES_FIN = 8  # Agosto 2025 (último mes con data completa)

# --- Cultivo objetivo ---
CULTIVO_TARGET = 'LIMON'

# --- Conexión PostgreSQL ---
PG_USER = 'postgres'
PG_PASS = 'postgres'
PG_HOST = 'localhost'
PG_PORT = '5432'
PG_DB   = 'limon_analytics_db'
PG_URI  = f'postgresql://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}'

# --- Peligros hidrometeorológicos válidos (INDECI) ---
PELIGROS_VALIDOS = [
    'LLUVIAS INTENSAS', 'INUNDACION', 'HUAYCO', 'SEQUIA',
    'HELADAS', 'FRIAJE', 'GRANIZADA', 'NEVADA', 'VIENTOS FUERTES',
    'DESLIZAMIENTO', 'EROSION'
]

# Guardar constantes para reutilización entre notebooks
import json
config = {
    'ANHO_INICIO': ANHO_INICIO, 'ANHO_FIN': ANHO_FIN, 'MES_FIN': MES_FIN,
    'CULTIVO_TARGET': CULTIVO_TARGET, 'PG_URI': PG_URI,
    'PELIGROS_VALIDOS': PELIGROS_VALIDOS,
    'DIRS': DIRS,
}
config_path = os.path.join(DIRS['interim'], 'pipeline_config.json')
with open(config_path, 'w', encoding='utf-8') as f:
    json.dump(config, f, indent=2, ensure_ascii=False)

print(f"✅ Configuración guardada en: {config_path}")
act1_cells.append(('code', """
print(f"\\n[ACTIVIDAD 1] COMPLETADA.")
"""))

create_notebook(act1_cells, "actividad_01_configuracion.ipynb")

# =====================================================================
# ACTIVIDAD 2: LECTURA DE DATASETS
# =====================================================================
act2_cells = [
    ('md', """# 📖 Actividad 2: Lectura de los Datasets con Pandas
---
**Objetivo:** Cargar las fuentes de datos principales en DataFrames de Pandas. Documentar las fuentes originales y generar archivos intermedios.

### Fuentes Documentadas
| Fuente | URL de Referencia | Formato |
|:-------|:------------------|:--------|
| MIDAGRI | [MIDAGRI](https://www.gob.pe/midagri) | Excel (.xlsx) |
| INDECI | [INDECI](https://www.gob.pe/indeci) | Excel + DBF |
| Agraria.pe | [Agraria.pe](https://www.agraria.pe/) | CSV (scraping) |
| NASA POWER | [NASA POWER](https://power.larc.nasa.gov/) | CSV (API NASA) |

### Visualización de Reportes Climáticos (Ingeniería previa)
A continuación se listan los gráficos clave generados en la fase de ingeniería de datos climáticos:
- `g1_eda_distribucion.png`: Distribución estadística.
- `g2_eda_cobertura.png`: Disponibilidad de datos.
- `g3_temperatura_series.png`: Tendencia de temperaturas.
- `g4_precipitacion_series.png`: Tendencia de lluvias.
- `g5_correlacion_clima.png`: Correlación climática.
- `g6_estacionalidad_temp.png`: Estacionalidad.
"""),

    ('code', """# ==========================================================================
# 2.0 Carga de Configuración del Pipeline
# ==========================================================================
import os, sys, json, glob, warnings
import pandas as pd
import numpy as np
from dbfread import DBF

warnings.filterwarnings('ignore')
pd.set_option('display.max_columns', None)

if os.path.basename(os.getcwd()) == 'notebooks':
    os.chdir(os.path.abspath('..'))
print(f"Directorio de trabajo: {os.getcwd()}")

with open(os.path.join('data', '02_interim', 'pipeline_config.json'), 'r', encoding='utf-8') as f:
    CONFIG = json.load(f)

DIRS = CONFIG['DIRS']
print("✅ Configuración cargada.")
"""),

    ('md', """## 2.1 Lectura de MIDAGRI (SISAGRI)"""),

    ('code', """
midagri_path = os.path.join(DIRS['raw_midagri'], 'Sisagri_2016_2025.xlsx')
df_midagri_full = pd.read_excel(midagri_path, sheet_name='2021_2025', engine='openpyxl')
df_midagri = df_midagri_full[df_midagri_full['dsc_Cultivo'].str.contains('LIMON', case=False, na=False)].copy()
print(f"🍋 Registros de LIMÓN: {len(df_midagri):,}")
"""),

    ('md', """## 2.2 Lectura de INDECI (SINPAD)"""),

    ('code', """
em_path = os.path.join(DIRS['raw_indeci'], 'resumen_emergencias_2003_2024.xlsx')
df_indeci_dpto = pd.read_excel(em_path, sheet_name='POR DPTO', skiprows=7)
dbf_files = {2021: 'E_2021/Emergencias_2021.dbf', 2022: 'E_2022/Emergencias_2022.dbf', 2023: 'E_2023/E_2023.dbf'}
dfs_dbf = []
for yr, rel_path in dbf_files.items():
    p = os.path.join(DIRS['raw_indeci'], rel_path)
    if os.path.exists(p):
        dfs_dbf.append(pd.DataFrame(list(DBF(p, encoding='latin1', load=True))))
df_indeci_eventos = pd.concat(dfs_dbf, ignore_index=True) if dfs_dbf else pd.DataFrame()
print(f"Total eventos INDECI: {len(df_indeci_eventos):,}")
"""),

    ('md', """## 2.3 NASA POWER — Datos Climáticos"""),

    ('code', """
nasa_path = os.path.join('data', '03_processed_nasa', 'nasa_climatic_raw_values.csv')
if os.path.exists(nasa_path):
    df_nasa = pd.read_csv(nasa_path)
    print(f"✅ NASA POWER cargado: {len(df_nasa):,}")
    display(df_nasa.head(3))
"""),

    ('md', """## 2.4 Guardado de Datos Intermedios"""),

    ('code', """
interim_dir = DIRS['interim']
df_midagri.to_csv(os.path.join(interim_dir, 'midagri_limon_raw.csv'), index=False, encoding='utf-8-sig')
df_indeci_dpto.to_csv(os.path.join(interim_dir, 'indeci_resumen_dpto.csv'), index=False, encoding='utf-8-sig')
if not df_indeci_eventos.empty:
    df_indeci_eventos.to_csv(os.path.join(interim_dir, 'indeci_eventos_dbf.csv'), index=False, encoding='utf-8-sig')
print(f"✅ Archivos intermedios generados.")
"""),
]

create_notebook(act2_cells, "actividad_02_lectura_datos.ipynb")
print("\n✅ Notebooks de Actividades 1 y 2 generados exitosamente.")
