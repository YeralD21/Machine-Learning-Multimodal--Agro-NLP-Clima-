"""
==========================================================================
NASA POWER Pipeline — Actividad 3: EDA Climático
==========================================================================
Genera estadísticas descriptivas de las 8 variables climáticas y un
reporte de cobertura geográfica.

Entrada : data/02_interim_nasa/nasa_long_raw.csv
Salida  : data/02_interim_nasa/reporte_eda_climatico.txt
          data/03_processed_nasa/reports/g1_eda_distribucion.png
          data/03_processed_nasa/reports/g2_eda_cobertura.png
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

# ─── Config ───────────────────────────────────────────────────────────────
with open('data/02_interim_nasa/nasa_pipeline_config.json', 'r', encoding='utf-8') as f:
    CONFIG = json.load(f)
DIRS            = CONFIG['DIRS']
PARAMETROS_NASA = CONFIG['PARAMETROS_NASA']

print('=' * 70)
print('  NASA PIPELINE — ACTIVIDAD 3: EDA Climático')
print('=' * 70)

df = pd.read_csv(os.path.join(DIRS['interim_nasa'], 'nasa_long_raw.csv'))
print(f'\n  Dataset cargado: {df.shape}')

# ─── 3.1 Estadísticas descriptivas por variable ───────────────────────────
print('\n[3.1] Estadísticas descriptivas por variable climática')
print()

vars_disponibles = [p for p in PARAMETROS_NASA if p in df.columns]
stats_rows = []

for var in vars_disponibles:
    serie = df[var].dropna()
    stats = {
        'Variable':  var,
        'N_válidos':  len(serie),
        'N_nulos':    df[var].isna().sum(),
        'Media':      round(serie.mean(), 3),
        'Mediana':    round(serie.median(), 3),
        'Std':        round(serie.std(), 3),
        'Min':        round(serie.min(), 3),
        'Max':        round(serie.max(), 3),
        'P25':        round(serie.quantile(0.25), 3),
        'P75':        round(serie.quantile(0.75), 3),
    }
    stats_rows.append(stats)
    print(f'  {var:<25} media={stats["Media"]:>8.3f}  std={stats["Std"]:>7.3f}  '
          f'min={stats["Min"]:>8.3f}  max={stats["Max"]:>8.3f}  '
          f'nulos={stats["N_nulos"]:>5}')

df_stats = pd.DataFrame(stats_rows)

# ─── 3.2 Cobertura geográfica ─────────────────────────────────────────────
print('\n[3.2] Cobertura geográfica')
n_dptos = df['DEPARTAMENTO'].nunique()
n_provs = df['PROVINCIA'].nunique()
n_meses = df.groupby(['DEPARTAMENTO', 'PROVINCIA', 'ANIO', 'MES']).ngroups

print(f'  Departamentos cubiertos : {n_dptos}')
print(f'  Provincias cubiertas    : {n_provs}')
print(f'  Combinaciones únicas    : {n_meses} (dpto × prov × año × mes)')
print()

# Provincias por departamento
print('  Detalle por departamento:')
cob = df.groupby('DEPARTAMENTO')['PROVINCIA'].nunique().sort_index()
for dpto, n in cob.items():
    provs = sorted(df[df['DEPARTAMENTO'] == dpto]['PROVINCIA'].unique())
    print(f'    {dpto:<25} {n:>2} prov → {", ".join(provs)}')

# ─── 3.3 Completitud temporal por provincia ───────────────────────────────
print('\n[3.3] Completitud temporal (meses esperados vs. encontrados)')
meses_esperados = (CONFIG['ANHO_FIN'] - CONFIG['ANHO_INICIO'] + 1) * 12
completitud = (
    df.groupby(['DEPARTAMENTO', 'PROVINCIA'])
    .size()
    .reset_index(name='n_meses')
)
completitud['pct_completitud'] = (completitud['n_meses'] / meses_esperados * 100).round(1)
incompletas = completitud[completitud['pct_completitud'] < 90]
if incompletas.empty:
    print('  ✅ Todas las provincias tienen ≥90% de completitud temporal.')
else:
    print(f'  ⚠️  {len(incompletas)} provincias con <90% de completitud:')
    print(incompletas.to_string(index=False))

# ─── 3.4 Gráfico 1: Distribución de variables climáticas ─────────────────
print('\n[3.4] Generando gráfico de distribuciones...')
os.makedirs(DIRS['reports_nasa'], exist_ok=True)

n_vars = len(vars_disponibles)
fig, axes = plt.subplots(2, 4, figsize=(18, 8))
axes = axes.flatten()

UNIDADES = {
    'T2M':               '°C',
    'T2M_MAX':           '°C',
    'T2M_MIN':           '°C',
    'PRECTOTCORR':       'mm/día',
    'RH2M':              '%',
    'QV2M':              'g/kg',
    'ALLSKY_SFC_SW_DWN': 'MJ/m²/día',
    'WS2M':              'm/s',
}

for i, var in enumerate(vars_disponibles):
    ax = axes[i]
    data = df[var].dropna()
    ax.hist(data, bins=40, color='steelblue', edgecolor='white', alpha=0.8)
    ax.axvline(data.mean(), color='red', linestyle='--', linewidth=1.5, label=f'Media: {data.mean():.2f}')
    ax.set_title(f'{var}\n({UNIDADES.get(var, "")})', fontsize=10, fontweight='bold')
    ax.set_xlabel(UNIDADES.get(var, ''), fontsize=8)
    ax.set_ylabel('Frecuencia', fontsize=8)
    ax.legend(fontsize=7)

# Ocultar ejes sobrantes
for j in range(n_vars, len(axes)):
    axes[j].set_visible(False)

fig.suptitle('EDA Climático — Distribución de Variables NASA POWER\n(Todas las provincias, 2021-2025)',
             fontsize=13, fontweight='bold')
plt.tight_layout()
g1_path = os.path.join(DIRS['reports_nasa'], 'g1_eda_distribucion.png')
plt.savefig(g1_path, dpi=150, bbox_inches='tight')
plt.close()
print(f'  [OK] {g1_path}')

# ─── 3.5 Gráfico 2: Cobertura geográfica (provincias por departamento) ────
print('\n[3.5] Generando gráfico de cobertura geográfica...')
cob_sorted = cob.sort_values(ascending=True)

fig, ax = plt.subplots(figsize=(10, max(6, len(cob_sorted) * 0.35)))
bars = ax.barh(cob_sorted.index, cob_sorted.values,
               color='teal', edgecolor='white', alpha=0.85)
for bar in bars:
    ax.text(bar.get_width() + 0.05, bar.get_y() + bar.get_height()/2,
            str(int(bar.get_width())), va='center', fontsize=9)
ax.set_xlabel('Número de Provincias', fontsize=11)
ax.set_title('Cobertura Geográfica NASA POWER\n(Provincias por Departamento)',
             fontsize=13, fontweight='bold')
ax.set_xlim(0, cob_sorted.max() + 2)
plt.tight_layout()
g2_path = os.path.join(DIRS['reports_nasa'], 'g2_eda_cobertura.png')
plt.savefig(g2_path, dpi=150, bbox_inches='tight')
plt.close()
print(f'  [OK] {g2_path}')

# ─── 3.6 Guardar reporte ──────────────────────────────────────────────────
reporte_lines = [
    'REPORTE EDA CLIMÁTICO — NASA POWER PIPELINE',
    '=' * 60,
    f'Departamentos: {n_dptos} | Provincias: {n_provs}',
    f'Combinaciones únicas (dpto×prov×año×mes): {n_meses:,}',
    '',
    'ESTADÍSTICAS DESCRIPTIVAS:',
    df_stats.to_string(index=False),
    '',
    'COMPLETITUD TEMPORAL:',
    completitud.to_string(index=False),
]
reporte_path = os.path.join(DIRS['interim_nasa'], 'reporte_eda_climatico.txt')
with open(reporte_path, 'w', encoding='utf-8') as f:
    f.write('\n'.join(reporte_lines))
print(f'  [OK] {reporte_path}')

print()
print('[ACTIVIDAD 3] COMPLETADA.')
print(f'  Descripción: EDA climático — {n_dptos} departamentos, {n_provs} provincias.')
print(f'  Gráficos: {g1_path} | {g2_path}')
