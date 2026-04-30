"""
==========================================================================
NASA POWER Pipeline — Actividad 2: Lectura de Datasets
==========================================================================
Lee los CSVs ya pre-procesados por departamento/provincia desde:
    data/raw/nasapower/   (ej: PIURA-SULLANA.csv, AMAZONAS-BAGUA.csv)

CONTEXTO — CÓMO SE GENERARON ESOS ARCHIVOS (proceso previo en Jupyter):
─────────────────────────────────────────────────────────────────────────
Cuando se descarga data de NASA POWER API, el archivo crudo tiene un
formato complejo con un bloque de metadata al inicio (-BEGIN HEADER-)
que hace que pandas no pueda leerlo directamente.

El proceso de conversión que se aplicó manualmente en Jupyter para
cada departamento/provincia fue el siguiente:

    # Paso 1: Detectar la fila donde empieza el encabezado real
    with open('PIURA-SULLANA.csv', 'r') as f:
        lineas = f.readlines()
    indice_header = 0
    for i, linea in enumerate(lineas):
        if 'PARAMETER,YEAR,JAN,FEB' in linea.replace(" ", ""):
            indice_header = i
            break

    # Paso 2: Cargar saltando el bloque de metadata
    df_clima = pd.read_csv('PIURA-SULLANA.csv', skiprows=indice_header)
    df_clima.columns = [c.strip() for c in df_clima.columns]

    # Paso 3: Agregar columnas de ubicación geográfica
    df_clima['departamento'] = 'PIURA'
    df_clima['provincia']    = 'SULLANA'

    # Paso 4: Reordenar columnas (dpto y prov al inicio)
    cols = ['departamento', 'provincia'] + [
        c for c in df_clima.columns
        if c not in ['departamento', 'provincia']
    ]
    df_clima = df_clima[cols]

    # Paso 5: Guardar el archivo ordenado
    df_clima.to_csv('PIURA-SULLANA.csv', index=False)

Este proceso se repitió para cada uno de los ~102 archivos de provincias.
El resultado es el formato WIDE que ahora leemos en esta actividad:

    departamento | provincia | PARAMETER | YEAR | JAN | FEB | ... | DEC | ANN
    PIURA        | SULLANA   | T2M       | 2021 | 26.1| 26.3| ... | 25.8| 26.0
    PIURA        | SULLANA   | T2M       | 2022 | 25.9| 26.0| ... | 25.7| 25.8
    ...

Esta actividad toma esos archivos ya ordenados y los transforma al
formato LONG (tidy) que necesita el pipeline:

    DEPARTAMENTO | PROVINCIA | ANIO | MES | T2M | PRECTOTCORR | RH2M | ...
    PIURA        | SULLANA   | 2021 |   1 | 26.1|        0.12 | 68.3 | ...

Salida: data/02_interim_nasa/nasa_long_raw.csv
        data/02_interim_nasa/reporte_lectura.txt
==========================================================================
"""
import sys
sys.stdout.reconfigure(encoding='utf-8')

import os
import json
import glob
import warnings
import pandas as pd
import numpy as np

warnings.filterwarnings('ignore')

# ─── Cargar configuración ─────────────────────────────────────────────────
CONFIG_PATH = 'data/02_interim_nasa/nasa_pipeline_config.json'
with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
    CONFIG = json.load(f)

DIRS            = CONFIG['DIRS']
PARAMETROS_NASA = CONFIG['PARAMETROS_NASA']
NASA_MISSING    = CONFIG['NASA_MISSING_VALUE']
ANHO_INICIO     = CONFIG['ANHO_INICIO']
ANHO_FIN        = CONFIG['ANHO_FIN']

MESES   = ['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN',
           'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC']
MES_MAP = {m: i + 1 for i, m in enumerate(MESES)}

print('=' * 70)
print('  NASA PIPELINE — ACTIVIDAD 2: Lectura de Datasets')
print('=' * 70)

# ─── Nota metodológica ────────────────────────────────────────────────────
print("""
  NOTA METODOLÓGICA — Proceso de conversión previo (Jupyter):
  ─────────────────────────────────────────────────────────────
  Los archivos en data/raw/nasapower/ fueron generados manualmente
  aplicando el siguiente proceso a cada CSV crudo de NASA POWER:

    1. Detectar la fila del encabezado real (buscar 'PARAMETER,YEAR,JAN')
    2. Cargar el CSV saltando el bloque de metadata con skiprows
    3. Agregar columnas 'departamento' y 'provincia' con valores fijos
    4. Reordenar columnas y guardar el archivo ordenado

  Esto convirtió el formato crudo NASA (con -BEGIN HEADER-) al formato
  WIDE estructurado que ahora procesamos aquí.
  ─────────────────────────────────────────────────────────────
""")


# =========================================================================
# FUNCIÓN CENTRAL: Parsear un CSV pre-procesado (formato WIDE con dpto/prov)
# =========================================================================
def parse_nasa_wide_csv(filepath: str) -> pd.DataFrame:
    """
    Transforma un CSV pre-procesado de NASA POWER (formato WIDE con columnas
    departamento/provincia ya añadidas) a formato LONG (tidy).

    Formato de entrada (WIDE):
        departamento | provincia | PARAMETER | YEAR | JAN | FEB | ... | DEC
        PIURA        | SULLANA   | T2M       | 2021 | 26.1| 26.3| ... | 25.8

    Formato de salida (LONG / tidy):
        DEPARTAMENTO | PROVINCIA | ANIO | MES | T2M | PRECTOTCORR | RH2M | ...
        PIURA        | SULLANA   | 2021 |   1 | 26.1|        0.12 | 68.3 | ...

    Pasos:
      1. Cargar el CSV (ya no tiene bloque de metadata — fue removido en Jupyter)
      2. Extraer departamento y provincia de las columnas del archivo
      3. Filtrar solo los parámetros climáticos que necesitamos
      4. Pivotar de WIDE a LONG (melt por columnas de meses)
      5. Reemplazar el centinela -999 por NaN
      6. Pivotar PARAMETER como columnas (pivot_table)
    """
    # ── Paso 1: Cargar el CSV ─────────────────────────────────────────────
    try:
        df = pd.read_csv(filepath, encoding='utf-8')
    except UnicodeDecodeError:
        try:
            df = pd.read_csv(filepath, encoding='latin1')
        except Exception as e:
            print(f'    [ERROR] {os.path.basename(filepath)}: {e}')
            return pd.DataFrame()
    except Exception as e:
        print(f'    [ERROR] {os.path.basename(filepath)}: {e}')
        return pd.DataFrame()

    df.columns = [str(c).strip() for c in df.columns]

    # ── Paso 2: Extraer departamento y provincia ──────────────────────────
    if 'departamento' not in df.columns or 'provincia' not in df.columns:
        print(f'    [SKIP] {os.path.basename(filepath)}: sin columnas dpto/prov')
        return pd.DataFrame()

    dpto_val = str(df['departamento'].iloc[0]).strip().upper()
    prov_val = str(df['provincia'].iloc[0]).strip().upper()
    df = df.drop(columns=['departamento', 'provincia'])

    # ── Paso 3: Filtrar parámetros necesarios ─────────────────────────────
    if 'PARAMETER' not in df.columns:
        return pd.DataFrame()

    df = df[df['PARAMETER'].isin(PARAMETROS_NASA)].copy()
    if df.empty:
        return pd.DataFrame()

    # ── Paso 4: Pivotar WIDE → LONG ───────────────────────────────────────
    meses_disponibles = [m for m in MESES if m in df.columns]
    if not meses_disponibles:
        return pd.DataFrame()

    df = df.drop(columns=['ANN'], errors='ignore')

    df['YEAR'] = pd.to_numeric(df['YEAR'], errors='coerce')
    df = df.dropna(subset=['YEAR'])
    df['YEAR'] = df['YEAR'].astype(int)
    df = df[df['YEAR'].between(ANHO_INICIO, ANHO_FIN)]
    if df.empty:
        return pd.DataFrame()

    df_long = df.melt(
        id_vars=['PARAMETER', 'YEAR'],
        value_vars=meses_disponibles,
        var_name='MES_STR',
        value_name='valor'
    )

    # ── Paso 5: Reemplazar centinela -999 por NaN ─────────────────────────
    df_long['valor'] = pd.to_numeric(df_long['valor'], errors='coerce')
    df_long.loc[df_long['valor'] <= NASA_MISSING, 'valor'] = np.nan

    df_long['MES'] = df_long['MES_STR'].map(MES_MAP)
    df_long = df_long.drop(columns=['MES_STR'])

    # ── Paso 6: Pivotar PARAMETER como columnas ───────────────────────────
    df_pivot = df_long.pivot_table(
        index=['YEAR', 'MES'],
        columns='PARAMETER',
        values='valor',
        aggfunc='mean'
    ).reset_index()
    df_pivot.columns.name = None

    df_pivot.insert(0, 'DEPARTAMENTO', dpto_val)
    df_pivot.insert(1, 'PROVINCIA',    prov_val)
    df_pivot = df_pivot.rename(columns={'YEAR': 'ANIO'})

    cols_finales = ['DEPARTAMENTO', 'PROVINCIA', 'ANIO', 'MES'] + \
                   [p for p in PARAMETROS_NASA if p in df_pivot.columns]
    df_pivot = df_pivot[[c for c in cols_finales if c in df_pivot.columns]]

    return df_pivot


# =========================================================================
# 2.1 Leer archivos pre-procesados (data/raw/nasapower/)
# =========================================================================
print('[2.1] Leyendo archivos pre-procesados por departamento/provincia...')
print(f'      Fuente: {DIRS["raw_nasa_power"]}')
print()

archivos = sorted(glob.glob(os.path.join(DIRS['raw_nasa_power'], '*.csv')))
print(f'  Archivos encontrados: {len(archivos)}')

dfs = []
skipped = []
for filepath in archivos:
    fname = os.path.basename(filepath)
    df_parsed = parse_nasa_wide_csv(filepath)
    if not df_parsed.empty:
        dfs.append(df_parsed)
    else:
        skipped.append(fname)

if skipped:
    print(f'\n  Archivos omitidos ({len(skipped)}):')
    for s in skipped:
        print(f'    - {s}')

if not dfs:
    print('\n  [ERROR] No se pudo leer ningún archivo.')
    sys.exit(1)

df_combined = pd.concat(dfs, ignore_index=True)

# Deduplicar por si algún archivo se procesó dos veces
antes = len(df_combined)
df_combined = df_combined.drop_duplicates(
    subset=['DEPARTAMENTO', 'PROVINCIA', 'ANIO', 'MES']
)
despues = len(df_combined)

df_combined = df_combined.sort_values(
    ['DEPARTAMENTO', 'PROVINCIA', 'ANIO', 'MES']
).reset_index(drop=True)


# =========================================================================
# 2.2 Reporte de cobertura
# =========================================================================
print('\n[2.2] Cobertura geográfica:')
n_dptos = df_combined['DEPARTAMENTO'].nunique()
n_provs = df_combined['PROVINCIA'].nunique()
print(f'  Departamentos cubiertos: {n_dptos}')
print(f'  Provincias cubiertas   : {n_provs}')
print(f'  Filas totales          : {len(df_combined):,}')
if antes != despues:
    print(f'  Duplicados eliminados  : {antes - despues}')
print()
print('  Detalle por departamento:')
cob = df_combined.groupby('DEPARTAMENTO')['PROVINCIA'].nunique().sort_index()
for dpto, n in cob.items():
    provs = sorted(df_combined[df_combined['DEPARTAMENTO'] == dpto]['PROVINCIA'].unique())
    print(f'    {dpto:<25} {n:>2} prov → {", ".join(provs)}')


# =========================================================================
# 2.3 Guardar intermedio y reporte
# =========================================================================
print('\n[2.3] Guardando archivo intermedio...')

out_path = os.path.join(DIRS['interim_nasa'], 'nasa_long_raw.csv')
df_combined.to_csv(out_path, index=False, encoding='utf-8-sig')
print(f'  [OK] {out_path}')
print(f'  Shape: {df_combined.shape}')
print(f'  Columnas: {df_combined.columns.tolist()}')

reporte_lines = [
    'REPORTE DE LECTURA — NASA POWER PIPELINE',
    '=' * 60,
    f'Archivos leídos      : {len(archivos) - len(skipped)} de {len(archivos)}',
    f'Filas totales        : {len(df_combined):,}',
    f'Departamentos        : {n_dptos}',
    f'Provincias           : {n_provs}',
    f'Rango temporal       : {df_combined["ANIO"].min()} → {df_combined["ANIO"].max()}',
    f'Columnas climáticas  : {[c for c in df_combined.columns if c not in ["DEPARTAMENTO","PROVINCIA","ANIO","MES"]]}',
    '',
    'PROCESO DE CONVERSIÓN PREVIO (Jupyter — por cada archivo crudo NASA):',
    '─' * 60,
    'Los archivos en data/raw/nasapower/ fueron generados aplicando:',
    '  1. Detectar fila del encabezado real (buscar PARAMETER,YEAR,JAN)',
    '  2. Cargar CSV saltando el bloque -BEGIN HEADER- con skiprows',
    '  3. Agregar columnas departamento y provincia con valores fijos',
    '  4. Reordenar columnas y guardar el archivo ordenado',
    'Este proceso se repitió manualmente para cada provincia (~102 archivos).',
    '',
    'COBERTURA GEOGRÁFICA:',
    '─' * 60,
]
for dpto, grp in df_combined.groupby('DEPARTAMENTO'):
    provs = sorted(grp['PROVINCIA'].unique().tolist())
    reporte_lines.append(f'  {dpto}: {", ".join(provs)}')

reporte_path = os.path.join(DIRS['interim_nasa'], 'reporte_lectura.txt')
with open(reporte_path, 'w', encoding='utf-8') as f:
    f.write('\n'.join(reporte_lines))
print(f'  [OK] {reporte_path}')

print()
print('[ACTIVIDAD 2] COMPLETADA.')
print(f'  Descripción: Lectura de {len(archivos) - len(skipped)} archivos pre-procesados NASA POWER.')
print(f'  Resultado: {len(df_combined):,} filas × {len(df_combined.columns)} columnas')
print(f'  Cobertura: {n_dptos} departamentos, {n_provs} provincias')
