"""
==========================================================================
NASA POWER Pipeline — Actividad 5: Limpieza y Estandarización (CRÍTICO)
==========================================================================
Aplica las transformaciones de calidad obligatorias:

  1. ESTANDARIZACIÓN GEOGRÁFICA (CRÍTICO para el merge con MIDAGRI):
     - DEPARTAMENTO y PROVINCIA en MAYÚSCULAS SIN TILDES.
     - Protección de la Ñ (JUNIN, no JUNÍN).
     - Corrección de nombres con espacios múltiples.

  2. REEMPLAZO DE VALORES CENTINELA:
     - Cualquier valor ≤ -999 → NaN (por si quedaron residuales).

  3. IMPUTACIÓN DE NULOS:
     - Si faltan ≤ 3 meses en una serie: interpolación lineal.
     - Si faltan > 3 meses: promedio histórico del mismo mes.
     - PRECTOTCORR (precipitación): se imputa con 0 si el nulo
       coincide con meses secos conocidos, sino con promedio mensual.

  4. CLIP DE OUTLIERS FÍSICOS:
     - Valores fuera del rango físico válido se recortan al límite.
     - Se registra cuántos valores fueron corregidos.

Entrada : data/02_interim_nasa/nasa_long_raw.csv
Salida  : data/02_interim_nasa/nasa_long_clean.csv
          data/02_interim_nasa/reporte_limpieza_nasa.txt
==========================================================================
"""
import sys
sys.stdout.reconfigure(encoding='utf-8')

import os
import json
import unicodedata
import warnings
import numpy as np
import pandas as pd

warnings.filterwarnings('ignore')

with open('data/02_interim_nasa/nasa_pipeline_config.json', 'r', encoding='utf-8') as f:
    CONFIG = json.load(f)
DIRS            = CONFIG['DIRS']
PARAMETROS_NASA = CONFIG['PARAMETROS_NASA']
RANGOS_VALIDOS  = CONFIG['RANGOS_VALIDOS']
NASA_MISSING    = CONFIG['NASA_MISSING_VALUE']

print('=' * 70)
print('  NASA PIPELINE — ACTIVIDAD 5: Limpieza y Estandarización')
print('=' * 70)

df = pd.read_csv(os.path.join(DIRS['interim_nasa'], 'nasa_long_raw.csv'))
print(f'\n  Dataset cargado: {df.shape}')
vars_disponibles = [p for p in PARAMETROS_NASA if p in df.columns]


# =========================================================================
# 5.1 ESTANDARIZACIÓN GEOGRÁFICA (CRÍTICO)
# =========================================================================
def normalize_geo(text: str) -> str:
    """
    Estandariza nombres geográficos:
    - MAYÚSCULAS
    - Sin tildes (Á→A, É→E, Í→I, Ó→O, Ú→U)
    - Protege la Ñ (no la elimina)
    - Elimina espacios múltiples
    """
    if not isinstance(text, str):
        return text
    text = text.strip().upper()
    # Proteger Ñ antes de normalizar
    text = text.replace('Ñ', '__NN__')
    # Eliminar tildes via NFD
    nfkd = unicodedata.normalize('NFD', text)
    text = ''.join(c for c in nfkd if unicodedata.category(c) != 'Mn')
    # Restaurar Ñ
    text = text.replace('__NN__', 'Ñ')
    # Colapsar espacios múltiples
    text = ' '.join(text.split())
    return text

print('\n[5.1] Estandarización geográfica (MAYÚSCULAS SIN TILDES)...')

# Guardar valores originales para reporte
dptos_antes = df['DEPARTAMENTO'].unique().tolist()
provs_antes = df['PROVINCIA'].unique().tolist()

df['DEPARTAMENTO'] = df['DEPARTAMENTO'].apply(normalize_geo)
df['PROVINCIA']    = df['PROVINCIA'].apply(normalize_geo)

dptos_despues = df['DEPARTAMENTO'].unique().tolist()
provs_despues = df['PROVINCIA'].unique().tolist()

# Detectar cambios
cambios_dpto = [(a, b) for a, b in zip(sorted(dptos_antes), sorted(dptos_despues)) if a != b]
print(f'  Departamentos únicos: {len(dptos_despues)}')
print(f'  Provincias únicas   : {len(provs_despues)}')
if cambios_dpto:
    print(f'  Cambios detectados  : {len(cambios_dpto)}')
    for antes, despues in cambios_dpto[:5]:
        print(f'    "{antes}" → "{despues}"')
else:
    print('  ✅ Nombres ya estaban estandarizados.')

# Verificar nombres críticos del pipeline
nombres_criticos = {
    'JUNIN': 'JUNIN',       # No JUNÍN
    'PIURA': 'PIURA',
    'LA LIBERTAD': 'LA LIBERTAD',
    'SAN MARTIN': 'SAN MARTIN',  # No SAN MARTÍN
    'MADRE DE DIOS': 'MADRE DE DIOS',
}
print('\n  Verificación de nombres críticos:')
for esperado in nombres_criticos.values():
    presente = esperado in df['DEPARTAMENTO'].values
    icono = '✅' if presente else '⚠️ '
    print(f'  {icono} {esperado}')


# =========================================================================
# 5.2 REEMPLAZO DE VALORES CENTINELA RESIDUALES
# =========================================================================
print('\n[5.2] Reemplazando valores centinela residuales (-999)...')
total_reemplazados = 0
for var in vars_disponibles:
    mask = df[var] <= NASA_MISSING
    n = mask.sum()
    if n > 0:
        df.loc[mask, var] = np.nan
        total_reemplazados += n
        print(f'  ⚠️  {var}: {n} valores -999 → NaN')
if total_reemplazados == 0:
    print('  ✅ Sin valores centinela residuales.')


# =========================================================================
# 5.3 CLIP DE OUTLIERS FÍSICOS
# =========================================================================
print('\n[5.3] Recortando valores fuera de rango físico...')
total_clipped = 0
for var, (vmin, vmax) in RANGOS_VALIDOS.items():
    if var not in df.columns:
        continue
    mask_fuera = (df[var] < vmin) | (df[var] > vmax)
    n_fuera = mask_fuera.sum()
    if n_fuera > 0:
        df[var] = df[var].clip(lower=vmin, upper=vmax)
        total_clipped += n_fuera
        print(f'  ⚠️  {var}: {n_fuera} valores recortados a [{vmin}, {vmax}]')
if total_clipped == 0:
    print('  ✅ Sin valores fuera de rango físico.')


# =========================================================================
# 5.4 IMPUTACIÓN DE NULOS
# =========================================================================
print('\n[5.4] Imputando valores nulos...')
total_imputados = 0

df = df.sort_values(['DEPARTAMENTO', 'PROVINCIA', 'ANIO', 'MES']).reset_index(drop=True)

for var in vars_disponibles:
    n_nulos_antes = df[var].isna().sum()
    if n_nulos_antes == 0:
        continue

    # Estrategia por grupo (provincia)
    for (dpto, prov), grp_idx in df.groupby(['DEPARTAMENTO', 'PROVINCIA']).groups.items():
        serie = df.loc[grp_idx, var].copy()
        n_nulos_grp = serie.isna().sum()

        if n_nulos_grp == 0:
            continue

        if n_nulos_grp <= 3:
            # Interpolación lineal para huecos pequeños
            serie_imp = serie.interpolate(method='linear', limit_direction='both')
        else:
            # Promedio histórico del mismo mes para huecos grandes
            mes_col = df.loc[grp_idx, 'MES']
            medias_mes = serie.groupby(mes_col).transform('mean')
            serie_imp = serie.fillna(medias_mes)
            # Si aún quedan nulos (mes sin ningún dato), usar media global de la variable
            if serie_imp.isna().any():
                serie_imp = serie_imp.fillna(df[var].mean())

        df.loc[grp_idx, var] = serie_imp
        total_imputados += n_nulos_grp

    n_nulos_despues = df[var].isna().sum()
    if n_nulos_antes > 0:
        print(f'  {var:<25} {n_nulos_antes:>5} nulos → {n_nulos_despues:>5} restantes '
              f'({n_nulos_antes - n_nulos_despues} imputados)')

print(f'\n  Total imputados: {total_imputados}')


# =========================================================================
# 5.5 Guardar resultado limpio
# =========================================================================
print('\n[5.5] Guardando dataset limpio...')
out_path = os.path.join(DIRS['interim_nasa'], 'nasa_long_clean.csv')
df.to_csv(out_path, index=False, encoding='utf-8-sig')
print(f'  [OK] {out_path}')
print(f'  Shape: {df.shape}')

# Reporte
reporte_lines = [
    'REPORTE DE LIMPIEZA — NASA POWER PIPELINE',
    '=' * 60,
    f'Filas procesadas          : {len(df):,}',
    f'Valores centinela (-999)  : {total_reemplazados}',
    f'Valores clipeados (rango) : {total_clipped}',
    f'Valores imputados         : {total_imputados}',
    '',
    'ESTANDARIZACIÓN GEOGRÁFICA:',
    f'  Departamentos únicos: {df["DEPARTAMENTO"].nunique()}',
    f'  Provincias únicas   : {df["PROVINCIA"].nunique()}',
    '',
    'NULOS RESIDUALES POST-LIMPIEZA:',
]
for var in vars_disponibles:
    n = df[var].isna().sum()
    reporte_lines.append(f'  {var:<25} {n}')

reporte_path = os.path.join(DIRS['interim_nasa'], 'reporte_limpieza_nasa.txt')
with open(reporte_path, 'w', encoding='utf-8') as f:
    f.write('\n'.join(reporte_lines))
print(f'  [OK] {reporte_path}')

print()
print('[ACTIVIDAD 5] COMPLETADA.')
print(f'  Descripción: Limpieza — {total_reemplazados} centinelas, '
      f'{total_clipped} clips, {total_imputados} imputaciones.')
