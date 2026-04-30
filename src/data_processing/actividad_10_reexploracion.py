"""
Pipeline Fase 1 - Actividad 10: Reexploración Post-ETL
Genera 3 gráficos finales de validación sobre el dataset procesado.
"""
import sys; sys.stdout.reconfigure(encoding='utf-8')
import os, json, warnings
import numpy as np
import pandas as pd
import joblib
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import seaborn as sns

warnings.filterwarnings('ignore')
sns.set_theme(style='whitegrid', palette='muted')

with open('data/02_interim/pipeline_config.json', 'r', encoding='utf-8') as f:
    CONFIG = json.load(f)

DIRS = CONFIG['DIRS']
PROCESSED_DIR = DIRS['processed']
REPORTS_DIR = DIRS['reports']
SCALERS_DIR = DIRS['scalers']

print('=' * 70)
print('  ACTIVIDAD 10: Reexploración Post-ETL')
print('=' * 70)

# Cargar dataset final
df = pd.read_csv(os.path.join(PROCESSED_DIR, 'master_dataset_fase1_v2.csv'))
print(f'\n  Dataset cargado: {df.shape}')
print(f'  Columnas: {df.columns.tolist()}')

# Desnormalizar producción y precio para los gráficos
scaler_path = os.path.join(SCALERS_DIR, 'scaler_fase1_v2.pkl')
if os.path.exists(scaler_path):
    scaler = joblib.load(scaler_path)
    cols_scaled = [c for c in ['produccion_t','cosecha_ha','precio_chacra_kg',
                                'num_emergencias','total_afectados','has_cultivo_perdidas',
                                'n_noticias'] if c in df.columns]
    df_real = df.copy()
    df_real[cols_scaled] = scaler.inverse_transform(df[cols_scaled].fillna(0))
    print('  Scaler cargado — valores desnormalizados para gráficos.')
else:
    df_real = df.copy()
    print('  Scaler no encontrado — usando valores escalados.')

# ─────────────────────────────────────────────
# Gráfico 1: Serie de Tiempo — Producción vs Precio
# ─────────────────────────────────────────────
print('\n[10.1] Gráfico 1: Serie temporal Producción vs Precio')
trend = df_real.groupby('fecha_evento').agg(
    produccion_total=('produccion_t', 'sum'),
    precio_promedio=('precio_chacra_kg', 'mean'),
).reset_index()
trend = trend.sort_values('fecha_evento')

fig, ax1 = plt.subplots(figsize=(14, 6))
color_prod = 'forestgreen'
color_price = 'darkorange'

ax1.set_xlabel('Fecha (YYYY-MM)', fontsize=11)
ax1.set_ylabel('Producción Total (t)', color=color_prod, fontsize=11)
ax1.plot(range(len(trend)), trend['produccion_total'], color=color_prod,
         marker='o', markersize=3, linewidth=1.5, label='Producción (t)')
ax1.tick_params(axis='y', labelcolor=color_prod)
ax1.set_xticks(range(0, len(trend), 6))
ax1.set_xticklabels(trend['fecha_evento'].iloc[::6], rotation=45, ha='right')

ax2 = ax1.twinx()
ax2.set_ylabel('Precio Chacra (S/. / kg)', color=color_price, fontsize=11)
ax2.plot(range(len(trend)), trend['precio_promedio'], color=color_price,
         marker='s', markersize=3, linewidth=1.5, linestyle='--', label='Precio (S/./kg)')
ax2.tick_params(axis='y', labelcolor=color_price)

fig.suptitle('Producción Total vs Precio Promedio del Limón (2021-2025)',
             fontsize=14, fontweight='bold')
lines1, labels1 = ax1.get_legend_handles_labels()
lines2, labels2 = ax2.get_legend_handles_labels()
ax1.legend(lines1 + lines2, labels1 + labels2, loc='upper left')
plt.tight_layout()

g1_path = os.path.join(REPORTS_DIR, 'g4_produccion_vs_precio.png')
plt.savefig(g1_path, dpi=150, bbox_inches='tight')
plt.close()
print(f'  [OK] {g1_path}')

# ─────────────────────────────────────────────
# Gráfico 2: Heatmap de Correlación
# ─────────────────────────────────────────────
print('\n[10.2] Gráfico 2: Heatmap de correlación')
corr_cols = ['produccion_t', 'precio_chacra_kg', 'num_emergencias',
             'total_afectados', 'n_noticias', 'month_sin', 'month_cos']
# TODO: INTEGRACIÓN NASA — añadir a corr_cols:
# 'temp_max_c', 'temp_min_c', 'precipitacion_mm', 'humedad_rel_pct', 'velocidad_viento'
corr_cols = [c for c in corr_cols if c in df.columns]
corr_matrix = df[corr_cols].corr()

fig, ax = plt.subplots(figsize=(10, 8))
mask = np.triu(np.ones_like(corr_matrix, dtype=bool))
cmap = sns.diverging_palette(250, 10, as_cmap=True)
sns.heatmap(corr_matrix, mask=mask, cmap=cmap, center=0, annot=True, fmt='.2f',
            square=True, linewidths=0.5, cbar_kws={"shrink": 0.8}, ax=ax)
ax.set_title('Correlación: Producción × Emergencias × Noticias\n(Espacio reservado para NASA)',
             fontsize=13, fontweight='bold')
plt.tight_layout()

g2_path = os.path.join(REPORTS_DIR, 'g5_correlacion_heatmap.png')
plt.savefig(g2_path, dpi=150, bbox_inches='tight')
plt.close()
print(f'  [OK] {g2_path}')

# ─────────────────────────────────────────────
# Gráfico 3: Volumen de Noticias por Año
# ─────────────────────────────────────────────
print('\n[10.3] Gráfico 3: Distribución de noticias por año')
df_n = pd.read_csv(os.path.join(DIRS['interim'], 'agraria_noticias_clean.csv'))
df_n['fecha'] = pd.to_datetime(df_n['fecha'], errors='coerce')
df_n['anho'] = df_n['fecha'].dt.year

freq_anho = df_n.groupby('anho').size()

fig, ax = plt.subplots(figsize=(8, 5))
bars = ax.bar(freq_anho.index.astype(str), freq_anho.values,
              color=['#2ecc71','#3498db','#9b59b6','#e74c3c','#f39c12'],
              edgecolor='black', linewidth=0.5)
for bar in bars:
    ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 2,
            str(int(bar.get_height())), ha='center', fontsize=11, fontweight='bold')
ax.set_xlabel('Año', fontsize=12)
ax.set_ylabel('Cantidad de Noticias', fontsize=12)
ax.set_title('Volumen de Noticias Agrícolas por Año (Agraria.pe)',
             fontsize=14, fontweight='bold')
plt.tight_layout()

g3_path = os.path.join(REPORTS_DIR, 'g6_noticias_por_anho.png')
plt.savefig(g3_path, dpi=150, bbox_inches='tight')
plt.close()
print(f'  [OK] {g3_path}')

# TODO: INTEGRACIÓN DATA NASA
# Código comentado para agregar línea de precipitaciones en gráfico 1:
#   ax3 = ax1.twinx()
#   ax3.spines['right'].set_position(('axes', 1.12))
#   trend_nasa = df_real.groupby('fecha_evento')['precipitacion_mm'].mean()
#   ax3.plot(range(len(trend)), trend_nasa.values, color='royalblue',
#            linewidth=1.5, linestyle=':', label='Precip. (mm)')
#   ax3.set_ylabel('Precipitación (mm)', color='royalblue')
print('\n  [NASA] Gráfico multivariable con precipitaciones pendiente (ver TODO)')

# ─────────────────────────────────────────────
# Resumen final
# ─────────────────────────────────────────────
print()
print('  Conteo de registros por año:')
for yr, cnt in df.groupby('anho').size().items():
    print(f'    {yr}: {cnt:,} filas')

print()
print('[ACTIVIDAD 10] COMPLETADA.')
print('  Descripcion: 3 graficos de validacion post-ETL generados.')
print(f'  Graficos generados:')
print(f'    {g1_path}')
print(f'    {g2_path}')
print(f'    {g3_path}')
