"""
Pipeline Fase 1 - Actividad 2: Lectura de Datasets
Lee MIDAGRI, INDECI y AGRARIA.PE, genera archivos intermedios en 02_interim/.
"""
import sys; sys.stdout.reconfigure(encoding='utf-8')
import os, json, glob, warnings
import pandas as pd
from dbfread import DBF

warnings.filterwarnings('ignore')

# Cargar config de Actividad 1
with open('data/02_interim/pipeline_config.json', 'r', encoding='utf-8') as f:
    CONFIG = json.load(f)

DIRS = CONFIG['DIRS']
ANHO_INICIO = CONFIG['ANHO_INICIO']
ANHO_FIN = CONFIG['ANHO_FIN']
CULTIVO_TARGET = CONFIG['CULTIVO_TARGET']

print('=' * 70)
print('  ACTIVIDAD 2: Lectura de Datasets')
print('=' * 70)

# -------------------------------------------------------------------------
# 2.1 MIDAGRI
# -------------------------------------------------------------------------
print('\n[2.1] MIDAGRI — Sisagri_2016_2025.xlsx')
midagri_path = os.path.join(DIRS['raw_midagri'], 'Sisagri_2016_2025.xlsx')
df_midagri_full = pd.read_excel(midagri_path, sheet_name='2021_2025', engine='openpyxl')
print(f'  Filas totales: {len(df_midagri_full):,}')
print(f'  Columnas: {df_midagri_full.columns.tolist()}')

# Filtro cultivo y tiempo
cultivo_col = df_midagri_full['dsc_Cultivo'].astype(str).str.upper().str.strip()
mask_limon = cultivo_col.str.contains(CULTIVO_TARGET, case=False, na=False)
df_midagri_full['anho'] = pd.to_numeric(df_midagri_full['anho'], errors='coerce')
mask_tiempo = df_midagri_full['anho'].between(ANHO_INICIO, ANHO_FIN)
df_midagri = df_midagri_full.loc[mask_limon & mask_tiempo].copy()

n_dptos = df_midagri['Dpto'].nunique()
variantes = df_midagri['dsc_Cultivo'].unique().tolist()
print(f'  Registros LIMON {ANHO_INICIO}-{ANHO_FIN}: {len(df_midagri):,}')
print(f'  Departamentos con limon: {n_dptos}')
print(f'  Variantes de cultivo: {variantes}')
print(f'  Muestra:')
print(df_midagri[['anho','mes','Dpto','Prov','PRODUCCION(t)','MTO_PRECCHAC (S/ x kg)']].head(3).to_string(index=False))

# -------------------------------------------------------------------------
# 2.2a INDECI — Resúmenes consolidados (Excel)
# -------------------------------------------------------------------------
print('\n[2.2a] INDECI — Resumen por Departamento/Provincia')
em_path = os.path.join(DIRS['raw_indeci'], 'resumen_emergencias_2003_2024.xlsx')

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

# Hoja por departamento
df_indeci_dpto = pd.read_excel(em_path, sheet_name='POR DPTO', header=None, skiprows=7)
df_indeci_dpto.columns = col_names[:len(df_indeci_dpto.columns)]
df_indeci_dpto = df_indeci_dpto.dropna(subset=['departamento'])
df_indeci_dpto = df_indeci_dpto[df_indeci_dpto['departamento'].astype(str).str.strip() != 'TOTAL']
df_indeci_dpto = df_indeci_dpto[~df_indeci_dpto['departamento'].astype(str).str.contains('Fuente|NOTA', na=True)]
df_indeci_dpto['departamento'] = df_indeci_dpto['departamento'].astype(str).str.strip()
print(f'  Departamentos resumen: {len(df_indeci_dpto)}')

# Hoja por departamento + provincia
df_indeci_prov = pd.read_excel(em_path, sheet_name='POR DPTO_PROV', header=None, skiprows=7)
df_indeci_prov.columns = col_names[:len(df_indeci_prov.columns)]
df_indeci_prov = df_indeci_prov.dropna(subset=['departamento'])
df_indeci_prov = df_indeci_prov[df_indeci_prov['departamento'].astype(str).str.strip() != 'TOTAL']
print(f'  Filas por provincia: {len(df_indeci_prov)}')

# -------------------------------------------------------------------------
# 2.2b INDECI — Eventos DBF (SINPAD 2021-2023)
# -------------------------------------------------------------------------
print('\n[2.2b] INDECI — Eventos DBF (2021-2023)')
dbf_map = {
    2021: os.path.join(DIRS['raw_indeci'], 'E_2021', 'Emergencias_2021.dbf'),
    2022: os.path.join(DIRS['raw_indeci'], 'E_2022', 'Emergencias_2022.dbf'),
    2023: os.path.join(DIRS['raw_indeci'], 'E_2023', 'E_2023.dbf'),
}

dfs_dbf = []
for year, dbf_path in dbf_map.items():
    if os.path.exists(dbf_path):
        table = DBF(dbf_path, encoding='latin1', load=True)
        df_y = pd.DataFrame(list(table))
        df_y.columns = [str(c).lower() for c in df_y.columns]
        dfs_dbf.append(df_y)
        print(f'  {year}: {len(df_y):,} registros | {len(df_y.columns)} campos')
    else:
        print(f'  {year}: No encontrado en {dbf_path}')

if dfs_dbf:
    df_indeci_eventos = pd.concat(dfs_dbf, ignore_index=True)
    print(f'  Total eventos combinados: {len(df_indeci_eventos):,}')
    key_cols = [c for c in ['ide_sinpad','fecha','departamen','provincia','fenomeno','safecta','sdamni'] if c in df_indeci_eventos.columns]
    print(f'  Campos clave disponibles: {key_cols}')
else:
    df_indeci_eventos = pd.DataFrame()

# -------------------------------------------------------------------------
# 2.3 AGRARIA.PE — Noticias
# -------------------------------------------------------------------------
print('\n[2.3] AGRARIA.PE — Noticias Agricolas')
news_files = sorted(glob.glob(os.path.join(DIRS['raw_news'], 'agro_news_*.csv')))
print(f'  Archivos encontrados: {len(news_files)}')
dfs_news = []
for f in news_files:
    df_n = pd.read_csv(f)
    fname = os.path.basename(f)
    print(f'  {fname}: {len(df_n)} noticias')
    dfs_news.append(df_n)

df_noticias = pd.concat(dfs_news, ignore_index=True)
df_noticias['fecha'] = pd.to_datetime(df_noticias['fecha'], errors='coerce')
df_noticias = df_noticias.dropna(subset=['fecha']).sort_values('fecha')
fecha_min = df_noticias['fecha'].min().date()
fecha_max = df_noticias['fecha'].max().date()
print(f'  Total consolidadas: {len(df_noticias)} | Rango: {fecha_min} -> {fecha_max}')
print(f'  Columnas: {df_noticias.columns.tolist()}')
print('  Categorias:')
print(df_noticias['fuente'].value_counts().to_string())

# TODO: INTEGRACIÓN DATA NASA
# 1. Descargar datos desde https://power.larc.nasa.gov/data-access-viewer/
#    Parámetros: T2M, T2M_MAX, T2M_MIN, PRECTOTCORR, RH2M, WS2M, ALLSKY_SFC_SW_DWN, QV2M
# 2. Guardar CSVs en: data/01_raw/nasa_power/
# 3. Código de lectura:
#    df_nasa = pd.read_csv('data/01_raw/nasa_power/clima_regional.csv')
#    df_nasa['fecha_evento'] = pd.to_datetime(df_nasa['DATE']).dt.strftime('%Y-%m')
print('\n  [NASA] Pendiente de integracion (ver bloque TODO)')

# -------------------------------------------------------------------------
# 2.4 Exportar intermedios
# -------------------------------------------------------------------------
print('\n[2.4] Guardando archivos intermedios en data/02_interim/')
interim = DIRS['interim']

df_midagri.to_csv(os.path.join(interim, 'midagri_limon_raw.csv'), index=False, encoding='utf-8-sig')
print(f'  [OK] midagri_limon_raw.csv ({len(df_midagri):,} filas)')

df_indeci_dpto.to_csv(os.path.join(interim, 'indeci_resumen_dpto.csv'), index=False, encoding='utf-8-sig')
print(f'  [OK] indeci_resumen_dpto.csv ({len(df_indeci_dpto)} filas)')

df_indeci_prov.to_csv(os.path.join(interim, 'indeci_resumen_prov.csv'), index=False, encoding='utf-8-sig')
print(f'  [OK] indeci_resumen_prov.csv ({len(df_indeci_prov)} filas)')

if not df_indeci_eventos.empty:
    df_indeci_eventos.to_csv(os.path.join(interim, 'indeci_eventos_dbf.csv'), index=False, encoding='utf-8-sig')
    print(f'  [OK] indeci_eventos_dbf.csv ({len(df_indeci_eventos):,} filas)')

df_noticias.to_csv(os.path.join(interim, 'agraria_noticias_raw.csv'), index=False, encoding='utf-8-sig')
print(f'  [OK] agraria_noticias_raw.csv ({len(df_noticias)} filas)')

# NASA placeholder
# df_nasa.to_csv(os.path.join(interim, 'nasa_clima_raw.csv'), index=False, encoding='utf-8-sig')

print()
print('[ACTIVIDAD 2] COMPLETADA.')
print('  Descripcion: Lectura de MIDAGRI, INDECI (resumen + DBFs) y AGRARIA.PE')
print(f'  Resultado: 5 archivos intermedios generados en {interim}')
print('  Archivos: midagri_limon_raw.csv | indeci_resumen_dpto.csv | indeci_resumen_prov.csv | indeci_eventos_dbf.csv | agraria_noticias_raw.csv')
