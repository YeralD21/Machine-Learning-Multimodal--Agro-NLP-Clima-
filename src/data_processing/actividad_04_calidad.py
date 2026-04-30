"""
Pipeline Fase 1 - Actividad 4: Calidad de los Datos
Reporte de nulos, duplicados y outliers por cada fuente.
Salida: data/04_reports/reporte_calidad_datos.txt
"""
import sys; sys.stdout.reconfigure(encoding='utf-8')
import os, json, warnings
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

warnings.filterwarnings('ignore')

with open('data/02_interim/pipeline_config.json', 'r', encoding='utf-8') as f:
    CONFIG = json.load(f)

DIRS = CONFIG['DIRS']
INTERIM_DIR = DIRS['interim']
REPORTS_DIR = DIRS['reports']

print('=' * 70)
print('  ACTIVIDAD 4: Calidad de los Datos')
print('=' * 70)

report_lines = ['REPORTE DE CALIDAD DE DATOS — FASE 1\n', '=' * 70 + '\n']

def check_quality(df, nombre, key_cols):
    print(f'\n[4.x] {nombre}')
    print(f'  Shape: {df.shape}')

    # Nulos
    nulls = df.isnull().sum()
    nulls_pct = (nulls / len(df) * 100).round(2)
    null_df = pd.DataFrame({'nulos': nulls, 'pct': nulls_pct})
    null_df = null_df[null_df['nulos'] > 0]
    if len(null_df) > 0:
        print(f'  Columnas con nulos ({len(null_df)}):')
        for col, row in null_df.iterrows():
            print(f'    {col:<40} {int(row["nulos"]):>6} ({row["pct"]:.1f}%)')
    else:
        print('  Sin nulos detectados.')

    # Duplicados
    if key_cols:
        dupes = df.duplicated(subset=key_cols).sum()
    else:
        dupes = df.duplicated().sum()
    print(f'  Duplicados (por {key_cols}): {dupes}')

    lines = [
        f'\n{nombre}\n', '-' * 40 + '\n',
        f'  Shape: {df.shape}\n',
        f'  Nulos por columna:\n{null_df.to_string()}\n',
        f'  Duplicados: {dupes}\n',
    ]
    return lines

# --- MIDAGRI ---
df_m = pd.read_csv(os.path.join(INTERIM_DIR, 'midagri_limon_raw.csv'))
lines = check_quality(df_m, 'MIDAGRI — midagri_limon_raw.csv', ['anho', 'mes', 'COD_UBIGEO', 'dsc_Cultivo'])
report_lines.extend(lines)

# Outliers produccion
prod = pd.to_numeric(df_m['PRODUCCION(t)'], errors='coerce').dropna()
q1, q3 = prod.quantile(0.25), prod.quantile(0.75)
iqr = q3 - q1
outliers_prod = ((prod < q1 - 1.5*iqr) | (prod > q3 + 1.5*iqr)).sum()
print(f'  Outliers produccion (IQR 1.5x): {outliers_prod}')
print(f'  Stats produccion: min={prod.min():.1f} | Q1={q1:.1f} | median={prod.median():.1f} | Q3={q3:.1f} | max={prod.max():.1f}')
report_lines.append(f'  Outliers produccion IQR 1.5x: {outliers_prod}\n')

# --- INDECI Eventos ---
df_ev = pd.read_csv(os.path.join(INTERIM_DIR, 'indeci_eventos_dbf.csv'), low_memory=False)
lines = check_quality(df_ev, 'INDECI — indeci_eventos_dbf.csv', ['ide_sinpad'])
report_lines.extend(lines)

# --- AGRARIA.PE ---
df_n = pd.read_csv(os.path.join(INTERIM_DIR, 'agraria_noticias_raw.csv'))
lines = check_quality(df_n, 'AGRARIA.PE — agraria_noticias_raw.csv', ['url'])
report_lines.extend(lines)
# Noticias con cuerpo muy corto (posibles scrapings vacíos)
short_body = (df_n['cuerpo_completo'].astype(str).str.len() < 100).sum()
print(f'  Noticias con cuerpo < 100 chars: {short_body}')
report_lines.append(f'  Noticias con cuerpo < 100 chars: {short_body}\n')

# TODO: INTEGRACIÓN DATA NASA
# Validaciones a añadir cuando se integre NASA POWER:
# - Temperatura: rango físico válido T2M entre -10°C y 45°C (para regiones peruanas)
# - Precipitación: PRECTOTCORR >= 0 mm/día
# - Humedad: RH2M entre 0% y 100%
# - Velocidad viento: WS2M >= 0 m/s
# Código sugerido:
#   df_nasa = pd.read_csv(os.path.join(INTERIM_DIR, 'nasa_clima_raw.csv'))
#   invalid_temp = ((df_nasa['T2M'] < -10) | (df_nasa['T2M'] > 45)).sum()
#   invalid_prec = (df_nasa['PRECTOTCORR'] < 0).sum()
#   print(f'NASA — Temperaturas inválidas: {invalid_temp} | Precipitaciones negativas: {invalid_prec}')
print('\n  [NASA] Placeholder validación de rangos físicos (ver TODO)')

# Guardar reporte TXT
report_path = os.path.join(REPORTS_DIR, 'reporte_calidad_datos.txt')
with open(report_path, 'w', encoding='utf-8') as f:
    f.writelines(report_lines)

print()
print('[ACTIVIDAD 4] COMPLETADA.')
print('  Descripcion: Reporte de nulos, duplicados y outliers para las 3 fuentes.')
print(f'  Archivo generado: {report_path}')
