"""
==========================================================================
NASA POWER Pipeline — Actividad 10: Reexploración Post-ETL
==========================================================================
Genera gráficos de validación sobre el dataset climático procesado:

  Gráfico 1: Serie temporal de Temperatura (T2M) en provincias clave
             (Piura, Sullana, Lambayeque, Virú, Ica, San Martín)
  Gráfico 2: Serie temporal de Precipitación (PRECTOTCORR) en las mismas
  Gráfico 3: Heatmap de correlación entre variables climáticas
  Gráfico 4: Boxplot estacional (temperatura por mes) — estacionalidad

Entrada : data/03_processed_nasa/nasa_climatic_raw_values.csv (sin escalar)
Salida  : data/03_processed_nasa/reports/g3_temperatura_series.png
          data/03_processed_nasa/reports/g4_precipitacion_series.png
          data/03_processed_nasa/reports/g5_correlacion_clima.png
          data/03_processed_nasa/reports/g6_estacionalidad_temp.png
==========================================================================
"""
import sys
sys.stdout.reconfigure(encoding='utf-8')

import os
import json
import warnings
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import seaborn as sns

warnings.filterwarnings('ignore')
sns.set_theme(style='whitegrid', palette='muted')

with open('data/02_interim_nasa/nasa_pipeline_config.json', 'r', encoding='utf-8') as f:
    CONFIG = json.load(f)
DIRS              = CONFIG['DIRS']
PARAMETROS_NASA   = CONFIG['PARAMETROS_NASA']
PROVINCIAS_CLAVE  = CONFIG['PROVINCIAS_CLAVE']

print('=' * 70)
print('  NASA PIPELINE — ACTIVIDAD 10: Reexploración Post-ETL')
print('=' * 70)

# Usar valores sin escalar para los gráficos
raw_path = os.path.join(DIRS['processed_nasa'], 'nasa_climatic_raw_values.csv')
if not os.path.exists(raw_path):
    # Fallback al procesado escalado
    raw_path = os.path.join(DIRS['processed_nasa'], CONFIG['OUTPUT_FILENAME'])

df = pd.read_csv(raw_path)
print(f'\n  Dataset cargado: {df.shape}')
vars_disponibles = [p for p in PARAMETROS_NASA if p in df.columns]
os.makedirs(DIRS['reports_nasa'], exist_ok=True)

# Filtrar provincias clave disponibles
provincias_disponibles = []
for dpto, prov in PROVINCIAS_CLAVE:
    mask = (df['DEPARTAMENTO'] == dpto) & (df['PROVINCIA'] == prov)
    if mask.sum() > 0:
        provincias_disponibles.append((dpto, prov))
    else:
        print(f'  [INFO] {dpto}/{prov} no encontrada en el dataset')

if not provincias_disponibles:
    # Usar las primeras 6 provincias disponibles
    combos = df.groupby(['DEPARTAMENTO', 'PROVINCIA']).size().reset_index()
    provincias_disponibles = [
        (row['DEPARTAMENTO'], row['PROVINCIA'])
        for _, row in combos.head(6).iterrows()
    ]
    print(f'  [INFO] Usando primeras {len(provincias_disponibles)} provincias disponibles')

print(f'\n  Provincias para gráficos: {provincias_disponibles}')

# Paleta de colores
COLORES = ['#e74c3c', '#3498db', '#2ecc71', '#f39c12', '#9b59b6', '#1abc9c']


# =========================================================================
# Gráfico 1: Serie temporal de Temperatura (T2M)
# =========================================================================
print('\n[10.1] Gráfico 1: Serie temporal de Temperatura (T2M)...')

if 'T2M' in df.columns:
    fig, ax = plt.subplots(figsize=(16, 6))

    for i, (dpto, prov) in enumerate(provincias_disponibles):
        mask = (df['DEPARTAMENTO'] == dpto) & (df['PROVINCIA'] == prov)
        df_prov = df[mask].sort_values('fecha_evento')
        if df_prov.empty:
            continue
        label = f'{dpto[:3]}-{prov[:8]}'
        ax.plot(range(len(df_prov)), df_prov['T2M'],
                marker='o', markersize=2, linewidth=1.5,
                color=COLORES[i % len(COLORES)], label=label, alpha=0.85)

    # Eje X con fechas
    df_ref = df[
        (df['DEPARTAMENTO'] == provincias_disponibles[0][0]) &
        (df['PROVINCIA'] == provincias_disponibles[0][1])
    ].sort_values('fecha_evento')
    tick_step = max(1, len(df_ref) // 12)
    ax.set_xticks(range(0, len(df_ref), tick_step))
    ax.set_xticklabels(df_ref['fecha_evento'].iloc[::tick_step], rotation=45, ha='right', fontsize=8)

    ax.set_xlabel('Fecha (YYYY-MM)', fontsize=11)
    ax.set_ylabel('Temperatura Media (°C)', fontsize=11)
    ax.set_title('Evolución de la Temperatura Media (T2M)\nProvincias Productoras de Limón — 2021-2025',
                 fontsize=13, fontweight='bold')
    ax.legend(loc='upper right', fontsize=8, ncol=2)
    ax.grid(True, alpha=0.4)
    plt.tight_layout()

    g1_path = os.path.join(DIRS['reports_nasa'], 'g3_temperatura_series.png')
    plt.savefig(g1_path, dpi=150, bbox_inches='tight')
    plt.close()
    print(f'  [OK] {g1_path}')
else:
    print('  [SKIP] T2M no disponible')
    g1_path = None


# =========================================================================
# Gráfico 2: Serie temporal de Precipitación (PRECTOTCORR)
# =========================================================================
print('\n[10.2] Gráfico 2: Serie temporal de Precipitación (PRECTOTCORR)...')

if 'PRECTOTCORR' in df.columns:
    fig, ax = plt.subplots(figsize=(16, 6))

    for i, (dpto, prov) in enumerate(provincias_disponibles):
        mask = (df['DEPARTAMENTO'] == dpto) & (df['PROVINCIA'] == prov)
        df_prov = df[mask].sort_values('fecha_evento')
        if df_prov.empty:
            continue
        label = f'{dpto[:3]}-{prov[:8]}'
        ax.plot(range(len(df_prov)), df_prov['PRECTOTCORR'],
                marker='s', markersize=2, linewidth=1.5,
                color=COLORES[i % len(COLORES)], label=label, alpha=0.85)

    ax.set_xticks(range(0, len(df_ref), tick_step))
    ax.set_xticklabels(df_ref['fecha_evento'].iloc[::tick_step], rotation=45, ha='right', fontsize=8)
    ax.set_xlabel('Fecha (YYYY-MM)', fontsize=11)
    ax.set_ylabel('Precipitación (mm/día)', fontsize=11)
    ax.set_title('Evolución de la Precipitación (PRECTOTCORR)\nProvincias Productoras de Limón — 2021-2025',
                 fontsize=13, fontweight='bold')
    ax.legend(loc='upper right', fontsize=8, ncol=2)
    ax.grid(True, alpha=0.4)
    plt.tight_layout()

    g2_path = os.path.join(DIRS['reports_nasa'], 'g4_precipitacion_series.png')
    plt.savefig(g2_path, dpi=150, bbox_inches='tight')
    plt.close()
    print(f'  [OK] {g2_path}')
else:
    print('  [SKIP] PRECTOTCORR no disponible')
    g2_path = None


# =========================================================================
# Gráfico 3: Heatmap de correlación entre variables climáticas
# =========================================================================
print('\n[10.3] Gráfico 3: Heatmap de correlación climática...')

if len(vars_disponibles) >= 2:
    corr_matrix = df[vars_disponibles].corr()

    fig, ax = plt.subplots(figsize=(10, 8))
    mask = np.triu(np.ones_like(corr_matrix, dtype=bool))
    cmap = sns.diverging_palette(250, 10, as_cmap=True)
    sns.heatmap(corr_matrix, mask=mask, cmap=cmap, center=0,
                annot=True, fmt='.2f', square=True,
                linewidths=0.5, cbar_kws={"shrink": 0.8}, ax=ax)
    ax.set_title('Correlación entre Variables Climáticas NASA POWER\n(Todas las provincias, 2021-2025)',
                 fontsize=13, fontweight='bold')
    plt.tight_layout()

    g3_path = os.path.join(DIRS['reports_nasa'], 'g5_correlacion_clima.png')
    plt.savefig(g3_path, dpi=150, bbox_inches='tight')
    plt.close()
    print(f'  [OK] {g3_path}')
else:
    g3_path = None


# =========================================================================
# Gráfico 4: Boxplot estacional — Temperatura por mes
# =========================================================================
print('\n[10.4] Gráfico 4: Estacionalidad de temperatura por mes...')

if 'T2M' in df.columns and 'MES' in df.columns:
    MESES_ES = {1:'Ene', 2:'Feb', 3:'Mar', 4:'Abr', 5:'May', 6:'Jun',
                7:'Jul', 8:'Ago', 9:'Sep', 10:'Oct', 11:'Nov', 12:'Dic'}

    # Filtrar solo provincias clave para el boxplot
    df_clave = pd.DataFrame()
    for dpto, prov in provincias_disponibles[:3]:  # Solo las 3 primeras para claridad
        mask = (df['DEPARTAMENTO'] == dpto) & (df['PROVINCIA'] == prov)
        df_sub = df[mask].copy()
        df_sub['Provincia'] = f'{dpto[:3]}-{prov[:8]}'
        df_clave = pd.concat([df_clave, df_sub], ignore_index=True)

    if not df_clave.empty:
        df_clave['Mes'] = df_clave['MES'].map(MESES_ES)
        orden_meses = [MESES_ES[i] for i in range(1, 13)]

        fig, ax = plt.subplots(figsize=(14, 6))
        sns.boxplot(data=df_clave, x='Mes', y='T2M', hue='Provincia',
                    order=orden_meses, palette='Set2', ax=ax)
        ax.set_xlabel('Mes', fontsize=11)
        ax.set_ylabel('Temperatura Media (°C)', fontsize=11)
        ax.set_title('Estacionalidad de la Temperatura por Mes\n(Provincias Productoras Clave)',
                     fontsize=13, fontweight='bold')
        ax.legend(title='Provincia', fontsize=9)
        plt.tight_layout()

        g4_path = os.path.join(DIRS['reports_nasa'], 'g6_estacionalidad_temp.png')
        plt.savefig(g4_path, dpi=150, bbox_inches='tight')
        plt.close()
        print(f'  [OK] {g4_path}')
    else:
        g4_path = None
else:
    g4_path = None


# ─── Resumen final ────────────────────────────────────────────────────────
print()
print('  Resumen del dataset final:')
print(f'    Departamentos: {df["DEPARTAMENTO"].nunique()}')
print(f'    Provincias   : {df["PROVINCIA"].nunique()}')
print(f'    Rango        : {df["fecha_evento"].min()} → {df["fecha_evento"].max()}')
print(f'    Variables    : {vars_disponibles}')
print()
print('  Estadísticas por variable (valores originales):')
for var in vars_disponibles:
    if var in df.columns:
        s = df[var].dropna()
        print(f'    {var:<25} media={s.mean():>8.3f}  std={s.std():>7.3f}  '
              f'min={s.min():>8.3f}  max={s.max():>8.3f}')

print()
print('[ACTIVIDAD 10] COMPLETADA.')
print('  Descripción: 4 gráficos de validación post-ETL generados.')
graficos = [p for p in [g1_path, g2_path, g3_path, g4_path] if p]
for g in graficos:
    print(f'    {g}')
