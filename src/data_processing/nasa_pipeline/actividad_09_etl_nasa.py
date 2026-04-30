"""
==========================================================================
NASA POWER Pipeline — Actividad 9: Pipeline ETL Completo
==========================================================================
Ejecuta la transformación final y genera el archivo de salida:
  data/03_processed_nasa/nasa_climatic_processed.csv

Pasos:
  1. Carga el dataset mensual limpio.
  2. Aplica StandardScaler a las variables climáticas.
  3. Guarda el scaler para desnormalización futura.
  4. Genera el CSV final con las columnas requeridas para el merge:
     DEPARTAMENTO, PROVINCIA, ANIO, MES, fecha_evento + variables climáticas.
  5. Verifica compatibilidad con el pipeline de MIDAGRI.

REQUERIMIENTO DE COMPATIBILIDAD:
  Las columnas DEPARTAMENTO, PROVINCIA, ANIO, MES deben ser idénticas
  a las del pipeline de Producción/Noticias para realizar merge sin errores.

Entrada : data/02_interim_nasa/nasa_mensual_integrado.csv
Salida  : data/03_processed_nasa/nasa_climatic_processed.csv
          models/scalers/scaler_nasa_clima.pkl
==========================================================================
"""
import sys
sys.stdout.reconfigure(encoding='utf-8')

import os
import json
import warnings
import joblib
import numpy as np
import pandas as pd
from sklearn.preprocessing import StandardScaler

warnings.filterwarnings('ignore')

with open('data/02_interim_nasa/nasa_pipeline_config.json', 'r', encoding='utf-8') as f:
    CONFIG = json.load(f)
DIRS            = CONFIG['DIRS']
PARAMETROS_NASA = CONFIG['PARAMETROS_NASA']
OUTPUT_FILENAME = CONFIG['OUTPUT_FILENAME']

print('=' * 70)
print('  NASA PIPELINE — ACTIVIDAD 9: Pipeline ETL Completo')
print('=' * 70)

# ─── 9.1 Cargar dataset mensual limpio ───────────────────────────────────
print('\n[9.1] Cargando dataset mensual integrado...')
df = pd.read_csv(os.path.join(DIRS['interim_nasa'], 'nasa_mensual_integrado.csv'))
print(f'  Shape: {df.shape}')
print(f'  Rango: {df["fecha_evento"].min()} → {df["fecha_evento"].max()}')
print(f'  Departamentos: {df["DEPARTAMENTO"].nunique()}')
print(f'  Provincias   : {df["PROVINCIA"].nunique()}')

vars_disponibles = [p for p in PARAMETROS_NASA if p in df.columns]
print(f'  Variables climáticas: {vars_disponibles}')


# ─── 9.2 Verificar nulos antes del escalado ───────────────────────────────
print('\n[9.2] Verificando nulos pre-escalado...')
for var in vars_disponibles:
    n = df[var].isna().sum()
    if n > 0:
        print(f'  ⚠️  {var}: {n} nulos → rellenando con media global')
        df[var] = df[var].fillna(df[var].mean())
    else:
        print(f'  ✅ {var}: sin nulos')


# ─── 9.3 Escalamiento con StandardScaler ─────────────────────────────────
print('\n[9.3] Aplicando StandardScaler a variables climáticas...')
scaler = StandardScaler()
df_scale_input = df[vars_disponibles].copy()
df[vars_disponibles] = scaler.fit_transform(df_scale_input)

# Guardar scaler
os.makedirs('models/scalers', exist_ok=True)
scaler_path = 'models/scalers/scaler_nasa_clima.pkl'
joblib.dump(scaler, scaler_path)
print(f'  [OK] Scaler guardado: {scaler_path}')
print(f'  Variables escaladas: {vars_disponibles}')

# Estadísticas del scaler
print('\n  Estadísticas del scaler (media y std originales):')
for i, var in enumerate(vars_disponibles):
    print(f'    {var:<25} media={scaler.mean_[i]:>8.4f}  std={scaler.scale_[i]:>7.4f}')


# ─── 9.4 Reordenar y renombrar columnas para compatibilidad ──────────────
print('\n[9.4] Preparando columnas para compatibilidad con pipeline MIDAGRI...')

# Columnas finales requeridas (nombres exactos para el merge)
# DEPARTAMENTO, PROVINCIA, ANIO, MES, fecha_evento + variables climáticas
cols_finales = ['DEPARTAMENTO', 'PROVINCIA', 'ANIO', 'MES', 'fecha_evento'] + \
               [v for v in vars_disponibles if v in df.columns]

df_final = df[[c for c in cols_finales if c in df.columns]].copy()

# Redondear variables escaladas a 6 decimales
for var in vars_disponibles:
    if var in df_final.columns:
        df_final[var] = df_final[var].round(6)

print(f'  Columnas finales: {df_final.columns.tolist()}')
print(f'  Shape final: {df_final.shape}')


# ─── 9.5 Verificación de compatibilidad con MIDAGRI ──────────────────────
print('\n[9.5] Verificando compatibilidad con pipeline de MIDAGRI...')

# Cargar dataset MIDAGRI para verificar
midagri_path = 'data/interim/midagri/midagri_limon_procesado.csv'
if os.path.exists(midagri_path):
    df_midagri = pd.read_csv(midagri_path)
    # Normalizar nombres para comparación
    midagri_dptos = set(df_midagri['departamento'].str.upper().str.strip().unique())
    nasa_dptos    = set(df_final['DEPARTAMENTO'].unique())
    comunes = midagri_dptos & nasa_dptos
    solo_midagri = midagri_dptos - nasa_dptos
    solo_nasa    = nasa_dptos - midagri_dptos

    print(f'  Departamentos en MIDAGRI : {len(midagri_dptos)}')
    print(f'  Departamentos en NASA    : {len(nasa_dptos)}')
    print(f'  Departamentos en común   : {len(comunes)}')
    if solo_midagri:
        print(f'  ⚠️  Solo en MIDAGRI (sin clima): {sorted(solo_midagri)}')
    if solo_nasa:
        print(f'  ℹ️  Solo en NASA (sin producción): {sorted(solo_nasa)[:5]}...')
else:
    print('  [INFO] Dataset MIDAGRI no encontrado para verificación cruzada.')
    print('  Asegúrate de que data/interim/midagri/midagri_limon_procesado.csv exista.')


# ─── 9.6 Exportar CSV final ───────────────────────────────────────────────
print('\n[9.6] Exportando CSV final...')
os.makedirs(DIRS['processed_nasa'], exist_ok=True)
output_path = os.path.join(DIRS['processed_nasa'], OUTPUT_FILENAME)
df_final.to_csv(output_path, index=False, encoding='utf-8-sig')
print(f'  [OK] {output_path}')

# También guardar una copia sin escalar para referencia
df_sin_escalar = df_scale_input.copy()
df_sin_escalar.insert(0, 'DEPARTAMENTO', df_final['DEPARTAMENTO'].values)
df_sin_escalar.insert(1, 'PROVINCIA',    df_final['PROVINCIA'].values)
df_sin_escalar.insert(2, 'ANIO',         df_final['ANIO'].values)
df_sin_escalar.insert(3, 'MES',          df_final['MES'].values)
df_sin_escalar.insert(4, 'fecha_evento', df_final['fecha_evento'].values)
raw_output_path = os.path.join(DIRS['processed_nasa'], 'nasa_climatic_raw_values.csv')
df_sin_escalar.to_csv(raw_output_path, index=False, encoding='utf-8-sig')
print(f'  [OK] {raw_output_path} (valores originales sin escalar)')


# ─── 9.7 Reporte final ───────────────────────────────────────────────────
print()
print('=' * 70)
print('  REPORTE FINAL — ACTIVIDAD 9')
print('=' * 70)
print(f'  Shape final          : {df_final.shape}')
print(f'  Departamentos        : {df_final["DEPARTAMENTO"].nunique()}')
print(f'  Provincias           : {df_final["PROVINCIA"].nunique()}')
print(f'  Rango temporal       : {df_final["fecha_evento"].min()} → {df_final["fecha_evento"].max()}')
print(f'  Variables climáticas : {len(vars_disponibles)}')
print(f'  Scaler guardado      : {scaler_path}')
print(f'  Archivo final        : {output_path}')
print('=' * 70)

print()
print('[ACTIVIDAD 9] COMPLETADA.')
print(f'  Descripción: ETL completo — escalado + exportación de nasa_climatic_processed.csv')
