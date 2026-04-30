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
| NASA POWER | Variables climáticas | *Pendiente de integración* |
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

# Encoding para Windows (solo en scripts, no en notebooks)
# if sys.platform == 'win32':
#     sys.stdout.reconfigure(encoding='utf-8')

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
}

for key, path in DIRS.items():
    os.makedirs(path, exist_ok=True)
    print(f"  📁 {path}")

# TODO: INTEGRACIÓN NASA
# os.makedirs(os.path.join('data', '01_raw', 'nasa_power'), exist_ok=True)
# Crear subcarpeta para almacenar los CSVs descargados de NASA POWER API.
# Parámetros sugeridos: T2M, T2M_MAX, T2M_MIN, PRECTOTCORR, RH2M, WS2M, ALLSKY_SFC_SW_DWN, QV2M

print("\\n✅ Estructura de carpetas creada exitosamente.")
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

# TODO: INTEGRACIÓN NASA
# Copiar los archivos CSV descargados desde NASA POWER API a:
# data/01_raw/nasa_power/
# Formato esperado: clima_regional_{departamento}.csv o similar.

print("\\n✅ Todos los archivos fuente copiados a data/01_raw/")
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
print(f"\\n[ACTIVIDAD 1] COMPLETADA.")
print(f"  Descripción: Configuración del entorno, estructura de carpetas y constantes.")
print(f"  Resultado: {len(DIRS)} directorios creados, archivos fuente copiados.")
print(f"  Archivo generado: {config_path}")
"""),
]

create_notebook(act1_cells, "actividad_01_configuracion.ipynb")


# =====================================================================
# ACTIVIDAD 2: LECTURA DE DATASETS
# =====================================================================
act2_cells = [
    ('md', """# 📖 Actividad 2: Lectura de los Datasets con Pandas
---
**Objetivo:** Cargar las 3 fuentes de datos principales (MIDAGRI, INDECI, AGRARIA.PE) en DataFrames de Pandas. Documentar las fuentes originales y generar archivos intermedios para las siguientes actividades.

### Fuentes Documentadas
| Fuente | URL de Referencia | Formato |
|:-------|:------------------|:--------|
| MIDAGRI | [https://app.powerbi.com/view?r=...SISAGRI](https://www.gob.pe/midagri) | Excel (.xlsx) |
| INDECI | [https://www.gob.pe/indeci](https://www.gob.pe/indeci) — SINPAD / Datos Abiertos | Excel (.xlsx) + Shapefile (.dbf) |
| Agraria.pe | [https://www.agraria.pe/](https://www.agraria.pe/) | CSV (scraping) |
| NASA POWER | [https://power.larc.nasa.gov/](https://power.larc.nasa.gov/) | *Pendiente* |
"""),

    ('code', """# ==========================================================================
# 2.0 Carga de Configuración del Pipeline
# ==========================================================================
import os, sys, json, glob, warnings
import unicodedata
import pandas as pd
import numpy as np
from dbfread import DBF

warnings.filterwarnings('ignore')
pd.set_option('display.max_columns', None)
# Encoding (no aplica en Jupyter kernel)
# if sys.platform == 'win32':
#     sys.stdout.reconfigure(encoding='utf-8')

# Navegar a la raíz del proyecto (necesario si se ejecuta desde notebooks/)
PROJECT_ROOT = os.path.abspath(os.path.join(os.getcwd(), '..'))
if os.path.basename(os.getcwd()) == 'notebooks':
    os.chdir(PROJECT_ROOT)
print(f"Directorio de trabajo: {os.getcwd()}")

# Cargar configuración de la Actividad 1
with open(os.path.join('data', '02_interim', 'pipeline_config.json'), 'r', encoding='utf-8') as f:
    CONFIG = json.load(f)

DIRS = CONFIG['DIRS']
ANHO_INICIO = CONFIG['ANHO_INICIO']
ANHO_FIN = CONFIG['ANHO_FIN']
CULTIVO_TARGET = CONFIG['CULTIVO_TARGET']

print("✅ Configuración cargada desde Actividad 1.")
print(f"   Rango temporal: {ANHO_INICIO} - {ANHO_FIN}")
print(f"   Cultivo target: {CULTIVO_TARGET}")
"""),

    ('md', """## 2.1 Lectura de MIDAGRI (SISAGRI)
Fuente oficial del Ministerio de Desarrollo Agrario y Riego.  
Contiene producción agrícola por cultivo, departamento, provincia y distrito a nivel mensual.
"""),

    ('code', """# ==========================================================================
# 2.1 MIDAGRI — Sistema Integrado de Estadísticas Agrarias (SISAGRI)
# ==========================================================================
print("=" * 70)
print("  LECTURA FUENTE 1: MIDAGRI (SISAGRI)")
print("=" * 70)

midagri_path = os.path.join(DIRS['raw_midagri'], 'Sisagri_2016_2025.xlsx')

# Lectura de la hoja 2021-2025
df_midagri_full = pd.read_excel(midagri_path, sheet_name='2021_2025', engine='openpyxl')

print(f"\\n  📊 Hoja leída: '2021_2025'")
print(f"  Filas totales: {len(df_midagri_full):,}")
print(f"  Columnas: {df_midagri_full.columns.tolist()}")
print(f"  Tipos de dato:")
print(df_midagri_full.dtypes.to_string())

# Filtro por cultivo LIMON
cultivo_col = df_midagri_full['dsc_Cultivo'].astype(str).str.upper().str.strip()
mask_limon = cultivo_col.str.contains(CULTIVO_TARGET, case=False, na=False)

# Filtro temporal
df_midagri_full['anho'] = pd.to_numeric(df_midagri_full['anho'], errors='coerce')
mask_tiempo = df_midagri_full['anho'].between(ANHO_INICIO, ANHO_FIN)

df_midagri = df_midagri_full.loc[mask_limon & mask_tiempo].copy()

print(f"\\n  🍋 Registros de LIMÓN ({ANHO_INICIO}-{ANHO_FIN}): {len(df_midagri):,}")
print(f"  Variantes encontradas: {df_midagri['dsc_Cultivo'].unique().tolist()}")
print(f"  Departamentos con limón: {df_midagri['Dpto'].nunique()}")

# Vista previa
print("\\n  Muestra de datos:")
print(df_midagri[['anho', 'mes', 'Dpto', 'Prov', 'dsc_Cultivo', 'PRODUCCION(t)', 'MTO_PRECCHAC (S/ x kg)']].head(5).to_string(index=False))
"""),

    ('md', """## 2.2 Lectura de INDECI (SINPAD)
Se cargan **dos tipos de fuentes**:
1. **Resúmenes consolidados** (2003-2024): Tablas agregadas por departamento y provincia. Útiles para el EDA geográfico.
2. **Registros evento-a-evento** (DBFs 2021-2023): Data granular con fecha, tipo de fenómeno y afectados. Estos alimentan la serie temporal del LSTM.
"""),

    ('code', """# ==========================================================================
# 2.2a INDECI — Resúmenes Consolidados (Excel)
# ==========================================================================
print("\\n" + "=" * 70)
print("  LECTURA FUENTE 2a: INDECI — Resúmenes Consolidados")
print("=" * 70)

# --- Resumen por Departamento ---
em_path = os.path.join(DIRS['raw_indeci'], 'resumen_emergencias_2003_2024.xlsx')
df_indeci_dpto = pd.read_excel(em_path, sheet_name='POR DPTO', header=None)

# El header real está en las filas 4-6 (merged cells)
# Fila 6 tiene los sub-headers: AFECT, DAMNIF, DESAP, LESION, FALLEC, etc.
col_names = [
    'departamento', 'emergencias',
    'pers_afectadas', 'pers_damnificadas', 'pers_desaparecidas',
    'pers_lesionadas', 'pers_fallecidas',
    'viv_afectadas', 'viv_destruidas',
    'salud_afectadas', 'salud_destruidas',
    'cultivo_has_afectadas', 'cultivo_has_perdidas',
    'puentes_afectados', 'puentes_perdidos',
    'carreteras_km_afectadas', 'carreteras_km_perdidas'
]

# Saltar las 7 primeras filas (encabezado complejo)
df_indeci_dpto = pd.read_excel(em_path, sheet_name='POR DPTO', header=None, skiprows=7)
df_indeci_dpto.columns = col_names[:len(df_indeci_dpto.columns)]

# Eliminar la fila TOTAL y filas vacías
df_indeci_dpto = df_indeci_dpto.dropna(subset=['departamento'])
df_indeci_dpto = df_indeci_dpto[df_indeci_dpto['departamento'].astype(str).str.strip() != 'TOTAL']
df_indeci_dpto = df_indeci_dpto[~df_indeci_dpto['departamento'].astype(str).str.contains('Fuente|NOTA', na=True)]

print(f"  Departamentos en resumen INDECI: {len(df_indeci_dpto)}")
print(f"  Columnas: {df_indeci_dpto.columns.tolist()}")

# --- Resumen por Departamento + Provincia ---
df_indeci_prov = pd.read_excel(em_path, sheet_name='POR DPTO_PROV', header=None, skiprows=7)
df_indeci_prov.columns = col_names[:len(df_indeci_prov.columns)]
df_indeci_prov = df_indeci_prov.dropna(subset=['departamento'])
df_indeci_prov = df_indeci_prov[df_indeci_prov['departamento'].astype(str).str.strip() != 'TOTAL']

print(f"  Filas en resumen por provincia: {len(df_indeci_prov)}")
"""),

    ('code', """# ==========================================================================
# 2.2b INDECI — Registros Evento a Evento (DBFs del SINPAD)
# ==========================================================================
print("\\n" + "=" * 70)
print("  LECTURA FUENTE 2b: INDECI — DBFs Evento a Evento (2021-2023)")
print("=" * 70)

dbf_files = {
    2021: os.path.join(DIRS['raw_indeci'], 'E_2021', 'Emergencias_2021.dbf'),
    2022: os.path.join(DIRS['raw_indeci'], 'E_2022', 'Emergencias_2022.dbf'),
    2023: os.path.join(DIRS['raw_indeci'], 'E_2023', 'E_2023.dbf'),
}

dfs_dbf = []
for year, dbf_path in dbf_files.items():
    if os.path.exists(dbf_path):
        table = DBF(dbf_path, encoding='latin1', load=True)
        df_year = pd.DataFrame(list(table))
        df_year.columns = [str(c).lower() for c in df_year.columns]
        dfs_dbf.append(df_year)
        print(f"  ✅ {year}: {len(df_year):,} registros | Columnas: {len(df_year.columns)}")
    else:
        print(f"  ⚠️  {year}: Archivo no encontrado en {dbf_path}")

if dfs_dbf:
    df_indeci_eventos = pd.concat(dfs_dbf, ignore_index=True)
    print(f"\\n  Total eventos combinados: {len(df_indeci_eventos):,}")
    print(f"  Campos clave: {['ide_sinpad', 'fecha', 'departamen', 'provincia', 'fenomeno', 'safecta', 'sdamni', 'sareaculti', 'sareacul_1']}")
    print(f"  Muestra:")
    print(df_indeci_eventos[['ide_sinpad', 'fecha', 'departamen', 'provincia', 'fenomeno']].head(3).to_string(index=False))
else:
    df_indeci_eventos = pd.DataFrame()
    print("  ⚠️  No se cargaron datos de eventos INDECI.")
"""),

    ('md', """## 2.3 Lectura de Noticias Agrícolas (Agraria.pe)
Noticias web scrapeadas del portal Agraria.pe, cubriendo el período 2021-2025.  
Contienen: fecha, titular, cuerpo completo, fuente (sección) y URL.
"""),

    ('code', """# ==========================================================================
# 2.3 AGRARIA.PE — Noticias Agrícolas
# ==========================================================================
print("\\n" + "=" * 70)
print("  LECTURA FUENTE 3: AGRARIA.PE — Noticias Agrícolas")
print("=" * 70)

news_files = sorted(glob.glob(os.path.join(DIRS['raw_news'], 'agro_news_*.csv')))
print(f"  Archivos encontrados: {len(news_files)}")

dfs_news = []
for f in news_files:
    df_n = pd.read_csv(f)
    year = os.path.basename(f).replace('agro_news_', '').replace('.csv', '')
    print(f"  📰 {os.path.basename(f)}: {len(df_n)} noticias")
    dfs_news.append(df_n)

df_noticias = pd.concat(dfs_news, ignore_index=True)
df_noticias['fecha'] = pd.to_datetime(df_noticias['fecha'], errors='coerce')
df_noticias = df_noticias.dropna(subset=['fecha']).sort_values('fecha')

print(f"\\n  Total noticias consolidadas: {len(df_noticias)}")
print(f"  Rango temporal: {df_noticias['fecha'].min()} → {df_noticias['fecha'].max()}")
print(f"  Columnas: {df_noticias.columns.tolist()}")
print(f"  Categorías (fuente):")
print(df_noticias['fuente'].value_counts().to_string())
"""),

    ('code', """# ==========================================================================
# TODO: INTEGRACIÓN DATA NASA
# ==========================================================================
# INSTRUCCIONES PARA EL COMPAÑERO DE TESIS:
# 
# 1. Descargar datos desde NASA POWER API:
#    https://power.larc.nasa.gov/data-access-viewer/
#    Parámetros: T2M, T2M_MAX, T2M_MIN, PRECTOTCORR, RH2M, WS2M, ALLSKY_SFC_SW_DWN, QV2M
#    Resolución: Mensual, por coordenadas de cada provincia/departamento.
#
# 2. Guardar los CSVs en: data/01_raw/nasa_power/
#
# 3. Código de lectura sugerido:
#    df_nasa = pd.read_csv('data/01_raw/nasa_power/clima_regional.csv')
#    df_nasa['DATE'] = pd.to_datetime(df_nasa['DATE'])
#    df_nasa['fecha_evento'] = df_nasa['DATE'].dt.strftime('%Y-%m')
#    print(f"NASA POWER cargado: {len(df_nasa)} registros")
#
# 4. Variables esperadas:
#    - ALLSKY_SFC_SW_DWN: Radiación solar (kW-hr/m²/day)
#    - PRECTOTCORR: Precipitación corregida (mm/day)
#    - T2M, T2M_MAX, T2M_MIN: Temperatura a 2m (°C)
#    - RH2M: Humedad relativa (%)
#    - WS2M: Velocidad del viento (m/s)
#    - QV2M: Humedad específica (g/kg)

print("  ℹ️  NASA POWER: Pendiente de integración (ver instrucciones arriba).")
"""),

    ('md', """## 2.4 Guardado de Datos Intermedios
Se guardan los DataFrames cargados en `data/02_interim/` para ser consumidos por las actividades siguientes sin necesidad de releer los archivos originales pesados.
"""),

    ('code', """# ==========================================================================
# 2.4 Exportación a Archivos Intermedios
# ==========================================================================
print("\\n" + "=" * 70)
print("  GUARDADO DE DATOS INTERMEDIOS")
print("=" * 70)

interim_dir = DIRS['interim']

# MIDAGRI
midagri_interim = os.path.join(interim_dir, 'midagri_limon_raw.csv')
df_midagri.to_csv(midagri_interim, index=False, encoding='utf-8-sig')
print(f"  ✅ MIDAGRI → {midagri_interim} ({len(df_midagri):,} filas)")

# INDECI Resumen Dpto
indeci_dpto_interim = os.path.join(interim_dir, 'indeci_resumen_dpto.csv')
df_indeci_dpto.to_csv(indeci_dpto_interim, index=False, encoding='utf-8-sig')
print(f"  ✅ INDECI Resumen Dpto → {indeci_dpto_interim} ({len(df_indeci_dpto)} filas)")

# INDECI Resumen Prov
indeci_prov_interim = os.path.join(interim_dir, 'indeci_resumen_prov.csv')
df_indeci_prov.to_csv(indeci_prov_interim, index=False, encoding='utf-8-sig')
print(f"  ✅ INDECI Resumen Prov → {indeci_prov_interim} ({len(df_indeci_prov)} filas)")

# INDECI Eventos (DBFs)
if not df_indeci_eventos.empty:
    indeci_eventos_interim = os.path.join(interim_dir, 'indeci_eventos_dbf.csv')
    df_indeci_eventos.to_csv(indeci_eventos_interim, index=False, encoding='utf-8-sig')
    print(f"  ✅ INDECI Eventos → {indeci_eventos_interim} ({len(df_indeci_eventos):,} filas)")

# Noticias
noticias_interim = os.path.join(interim_dir, 'agraria_noticias_raw.csv')
df_noticias.to_csv(noticias_interim, index=False, encoding='utf-8-sig')
print(f"  ✅ Noticias → {noticias_interim} ({len(df_noticias)} filas)")

# TODO: INTEGRACIÓN NASA
# nasa_interim = os.path.join(interim_dir, 'nasa_clima_raw.csv')
# df_nasa.to_csv(nasa_interim, index=False, encoding='utf-8-sig')
# print(f"  ✅ NASA → {nasa_interim}")

print(f"\\n[ACTIVIDAD 2] COMPLETADA.")
print(f"  Descripción: Lectura de MIDAGRI, INDECI y AGRARIA.PE en DataFrames.")
print(f"  Resultado: 5 archivos intermedios generados en {interim_dir}")
print(f"  Archivos generados: midagri_limon_raw.csv, indeci_resumen_dpto.csv,")
print(f"    indeci_resumen_prov.csv, indeci_eventos_dbf.csv, agraria_noticias_raw.csv")
"""),
]

create_notebook(act2_cells, "actividad_02_lectura_datos.ipynb")

print("\n✅ Notebooks de Actividades 1 y 2 generados exitosamente en notebooks/")
