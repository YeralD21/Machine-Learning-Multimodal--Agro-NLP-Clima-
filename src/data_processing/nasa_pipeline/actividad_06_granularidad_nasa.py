"""
==========================================================================
NASA POWER Pipeline — Actividad 6: Integración y Granularidad
==========================================================================
La data de la NASA ya viene en granularidad MENSUAL (promedio mensual
por punto de coordenada). Esta actividad:

  1. Verifica que la granularidad sea correcta (1 fila por dpto/prov/año/mes).
  2. Aplica agregación de seguridad por si hubiera duplicados:
     - PRECTOTCORR → SUMA (precipitación acumulada mensual en mm)
     - Resto de variables → PROMEDIO
  3. Construye la clave temporal fecha_evento (YYYY-MM) compatible
     con el pipeline de MIDAGRI.
  4. Genera el esqueleto temporal completo (2021-01 a 2025-08) y
     verifica que no haya meses faltantes.

Entrada : data/02_interim_nasa/nasa_long_clean.csv
Salida  : data/02_interim_nasa/nasa_mensual_integrado.csv
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
DIRS             = CONFIG['DIRS']
PARAMETROS_NASA  = CONFIG['PARAMETROS_NASA']
PARAMETROS_SUMA  = CONFIG['PARAMETROS_SUMA']
ANHO_INICIO      = CONFIG['ANHO_INICIO']
ANHO_FIN         = CONFIG['ANHO_FIN']
MES_FIN          = CONFIG['MES_FIN']

print('=' * 70)
print('  NASA PIPELINE — ACTIVIDAD 6: Integración y Granularidad')
print('=' * 70)

df = pd.read_csv(os.path.join(DIRS['interim_nasa'], 'nasa_long_clean.csv'))
print(f'\n  Dataset cargado: {df.shape}')
vars_disponibles = [p for p in PARAMETROS_NASA if p in df.columns]


# ─── 6.1 Verificar granularidad actual ───────────────────────────────────
print('\n[6.1] Verificando granularidad (1 fila por dpto/prov/año/mes)...')
llave = ['DEPARTAMENTO', 'PROVINCIA', 'ANIO', 'MES']
duplicados = df.duplicated(subset=llave).sum()
print(f'  Duplicados en llave maestra: {duplicados}')

if duplicados > 0:
    print(f'  ⚠️  Aplicando agregación para resolver duplicados...')


# ─── 6.2 Agregación de seguridad ─────────────────────────────────────────
print('\n[6.2] Aplicando agregación mensual...')
print(f'  Variables con SUMA  : {PARAMETROS_SUMA}')
print(f'  Variables con MEDIA : {[v for v in vars_disponibles if v not in PARAMETROS_SUMA]}')

# Construir diccionario de agregación dinámico
agg_dict = {}
for var in vars_disponibles:
    if var in PARAMETROS_SUMA:
        agg_dict[var] = 'sum'
    else:
        agg_dict[var] = 'mean'

df_mensual = (
    df.groupby(llave)
    .agg(agg_dict)
    .reset_index()
)

# Redondear a 4 decimales
for var in vars_disponibles:
    if var in df_mensual.columns:
        df_mensual[var] = df_mensual[var].round(4)

print(f'  Filas post-agregación: {len(df_mensual):,}')
print(f'  Duplicados residuales: {df_mensual.duplicated(subset=llave).sum()}')


# ─── 6.3 Construir clave temporal fecha_evento (YYYY-MM) ─────────────────
print('\n[6.3] Construyendo clave temporal fecha_evento...')
df_mensual['fecha_evento'] = (
    df_mensual['ANIO'].astype(str) + '-' +
    df_mensual['MES'].astype(str).str.zfill(2)
)

# Filtrar ventana temporal del proyecto (hasta MES_FIN del ANHO_FIN)
df_mensual['fecha_dt'] = pd.to_datetime(df_mensual['fecha_evento'])
fecha_limite = pd.to_datetime(f'{ANHO_FIN}-{MES_FIN:02d}-01')
df_mensual = df_mensual[df_mensual['fecha_dt'] <= fecha_limite].copy()
df_mensual = df_mensual.drop(columns=['fecha_dt'])

print(f'  Rango temporal: {df_mensual["fecha_evento"].min()} → {df_mensual["fecha_evento"].max()}')
print(f'  Filas en ventana temporal: {len(df_mensual):,}')


# ─── 6.4 Esqueleto temporal completo ─────────────────────────────────────
print('\n[6.4] Verificando completitud del esqueleto temporal...')
fechas_esperadas = pd.date_range(
    start=f'{ANHO_INICIO}-01-01',
    end=f'{ANHO_FIN}-{MES_FIN:02d}-01',
    freq='MS'
)
n_meses_esperados = len(fechas_esperadas)
print(f'  Meses esperados por provincia: {n_meses_esperados}')

# Verificar por provincia
completitud = (
    df_mensual.groupby(['DEPARTAMENTO', 'PROVINCIA'])
    .size()
    .reset_index(name='n_meses')
)
completitud['completo'] = completitud['n_meses'] == n_meses_esperados
n_completas = completitud['completo'].sum()
n_total = len(completitud)
print(f'  Provincias con serie completa: {n_completas}/{n_total}')

incompletas = completitud[~completitud['completo']]
if not incompletas.empty:
    print(f'  ⚠️  Provincias incompletas:')
    print(incompletas.to_string(index=False))


# ─── 6.5 Reordenar columnas para compatibilidad con MIDAGRI ──────────────
print('\n[6.5] Reordenando columnas para compatibilidad con pipeline MIDAGRI...')
# Orden final: DEPARTAMENTO, PROVINCIA, ANIO, MES, fecha_evento, variables climáticas
cols_orden = ['DEPARTAMENTO', 'PROVINCIA', 'ANIO', 'MES', 'fecha_evento'] + \
             [v for v in vars_disponibles if v in df_mensual.columns]
df_mensual = df_mensual[[c for c in cols_orden if c in df_mensual.columns]]

print(f'  Columnas finales: {df_mensual.columns.tolist()}')


# ─── 6.6 Guardar ─────────────────────────────────────────────────────────
out_path = os.path.join(DIRS['interim_nasa'], 'nasa_mensual_integrado.csv')
df_mensual.to_csv(out_path, index=False, encoding='utf-8-sig')
print(f'\n  [OK] {out_path}')
print(f'  Shape: {df_mensual.shape}')

print()
print('[ACTIVIDAD 6] COMPLETADA.')
print(f'  Descripción: Granularidad mensual verificada y clave fecha_evento construida.')
print(f'  Resultado: {len(df_mensual):,} filas × {len(df_mensual.columns)} columnas')
print(f'  Llave de merge: DEPARTAMENTO + PROVINCIA + fecha_evento')
