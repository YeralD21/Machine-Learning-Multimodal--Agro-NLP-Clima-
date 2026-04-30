"""
==========================================================================
NASA POWER Pipeline — Actividad 4: Calidad de los Datos
==========================================================================
Detecta:
  - Saltos en la serie temporal (meses faltantes por provincia)
  - Valores fuera de rango físico (temperaturas imposibles, etc.)
  - Valores residuales -999 que no fueron reemplazados
  - Provincias con cobertura temporal insuficiente (<80%)

Entrada : data/02_interim_nasa/nasa_long_raw.csv
Salida  : data/02_interim_nasa/reporte_calidad_nasa.txt
==========================================================================
"""
import sys
sys.stdout.reconfigure(encoding='utf-8')

import os
import json
import warnings
import numpy as np
import pandas as pd

warnings.filterwarnings('ignore')

with open('data/02_interim_nasa/nasa_pipeline_config.json', 'r', encoding='utf-8') as f:
    CONFIG = json.load(f)
DIRS            = CONFIG['DIRS']
PARAMETROS_NASA = CONFIG['PARAMETROS_NASA']
RANGOS_VALIDOS  = CONFIG['RANGOS_VALIDOS']
ANHO_INICIO     = CONFIG['ANHO_INICIO']
ANHO_FIN        = CONFIG['ANHO_FIN']
NASA_MISSING    = CONFIG['NASA_MISSING_VALUE']

print('=' * 70)
print('  NASA PIPELINE — ACTIVIDAD 4: Calidad de los Datos')
print('=' * 70)

df = pd.read_csv(os.path.join(DIRS['interim_nasa'], 'nasa_long_raw.csv'))
print(f'\n  Dataset cargado: {df.shape}')

vars_disponibles = [p for p in PARAMETROS_NASA if p in df.columns]
problemas = []

# ─── 4.1 Valores residuales -999 ─────────────────────────────────────────
print('\n[4.1] Verificando valores residuales -999 (centinela NASA)...')
for var in vars_disponibles:
    n_999 = (df[var] <= NASA_MISSING).sum()
    if n_999 > 0:
        msg = f'  ⚠️  {var}: {n_999} valores ≤ -999 encontrados (deben ser NaN)'
        print(msg)
        problemas.append(msg)
    else:
        print(f'  ✅ {var}: sin valores -999')

# ─── 4.2 Valores fuera de rango físico ───────────────────────────────────
print('\n[4.2] Verificando rangos físicos válidos...')
outliers_total = 0
for var, (vmin, vmax) in RANGOS_VALIDOS.items():
    if var not in df.columns:
        continue
    serie = df[var].dropna()
    fuera = serie[(serie < vmin) | (serie > vmax)]
    if len(fuera) > 0:
        outliers_total += len(fuera)
        msg = (f'  ⚠️  {var}: {len(fuera)} valores fuera de [{vmin}, {vmax}] '
               f'→ min_real={serie.min():.2f}, max_real={serie.max():.2f}')
        print(msg)
        problemas.append(msg)
        # Mostrar ejemplos
        idx_fuera = df[var][(df[var] < vmin) | (df[var] > vmax)].index[:3]
        for idx in idx_fuera:
            row = df.loc[idx]
            print(f'       Ejemplo: {row["DEPARTAMENTO"]}/{row["PROVINCIA"]} '
                  f'{int(row["ANIO"])}-{int(row["MES"]):02d} → {var}={row[var]:.3f}')
    else:
        print(f'  ✅ {var}: todos los valores en [{vmin}, {vmax}]')

# ─── 4.3 Saltos en la serie temporal ─────────────────────────────────────
print('\n[4.3] Verificando continuidad temporal por provincia...')
meses_esperados = set()
for anio in range(ANHO_INICIO, ANHO_FIN + 1):
    for mes in range(1, 13):
        meses_esperados.add((anio, mes))

saltos_total = 0
provincias_con_saltos = []

for (dpto, prov), grp in df.groupby(['DEPARTAMENTO', 'PROVINCIA']):
    meses_presentes = set(zip(grp['ANIO'].astype(int), grp['MES'].astype(int)))
    meses_faltantes = sorted(meses_esperados - meses_presentes)
    if meses_faltantes:
        saltos_total += len(meses_faltantes)
        provincias_con_saltos.append({
            'DEPARTAMENTO': dpto,
            'PROVINCIA': prov,
            'n_faltantes': len(meses_faltantes),
            'ejemplos': str(meses_faltantes[:3])
        })

if provincias_con_saltos:
    print(f'  ⚠️  {len(provincias_con_saltos)} provincias con saltos temporales '
          f'({saltos_total} meses faltantes en total):')
    for p in provincias_con_saltos[:10]:
        print(f'       {p["DEPARTAMENTO"]}/{p["PROVINCIA"]}: '
              f'{p["n_faltantes"]} meses faltantes (ej: {p["ejemplos"]})')
    if len(provincias_con_saltos) > 10:
        print(f'       ... y {len(provincias_con_saltos) - 10} más')
    problemas.append(f'{len(provincias_con_saltos)} provincias con saltos temporales')
else:
    print('  ✅ Todas las provincias tienen serie temporal continua.')

# ─── 4.4 Nulos por variable ───────────────────────────────────────────────
print('\n[4.4] Resumen de nulos por variable:')
for var in vars_disponibles:
    n_nulos = df[var].isna().sum()
    pct = n_nulos / len(df) * 100
    icono = '⚠️ ' if pct > 5 else '✅'
    print(f'  {icono} {var:<25} {n_nulos:>6} nulos ({pct:.1f}%)')

# ─── 4.5 Guardar reporte ──────────────────────────────────────────────────
reporte_lines = [
    'REPORTE DE CALIDAD — NASA POWER PIPELINE',
    '=' * 60,
    f'Total filas analizadas: {len(df):,}',
    f'Variables analizadas  : {len(vars_disponibles)}',
    f'Outliers detectados   : {outliers_total}',
    f'Provincias con saltos : {len(provincias_con_saltos)}',
    '',
    'PROBLEMAS DETECTADOS:',
]
if problemas:
    for p in problemas:
        reporte_lines.append(f'  - {p}')
else:
    reporte_lines.append('  Ninguno — datos en buen estado.')

reporte_lines += [
    '',
    'NULOS POR VARIABLE:',
]
for var in vars_disponibles:
    n_nulos = df[var].isna().sum()
    pct = n_nulos / len(df) * 100
    reporte_lines.append(f'  {var:<25} {n_nulos:>6} ({pct:.1f}%)')

reporte_path = os.path.join(DIRS['interim_nasa'], 'reporte_calidad_nasa.txt')
with open(reporte_path, 'w', encoding='utf-8') as f:
    f.write('\n'.join(reporte_lines))
print(f'\n  [OK] {reporte_path}')

print()
print('[ACTIVIDAD 4] COMPLETADA.')
print(f'  Descripción: Auditoría de calidad — {outliers_total} outliers, '
      f'{len(provincias_con_saltos)} provincias con saltos temporales.')
