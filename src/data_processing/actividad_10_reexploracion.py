"""
Pipeline Fase 1 - Actividad 10: Reexploración Post-ETL
Genera gráficos de validación integrando las variables climáticas de la NASA.
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
print('  ACTIVIDAD 10: Reexploración Post-ETL (Multimodal + NASA)')
print('=' * 70)

# Cargar dataset final
df = pd.read_csv(os.path.join(PROCESSED_DIR, 'master_dataset_fase1_v2.csv'))
print(f'\n  Dataset cargado: {df.shape}')

# Desnormalizar para los gráficos
scaler_path = os.path.join(SCALERS_DIR, 'scaler_fase1_v2.pkl')
if os.path.exists(scaler_path):
    scaler = joblib.load(scaler_path)
    # Lista exacta de columnas escaladas en Actividad 09
    cols_scaled = [
        'produccion_t', 'cosecha_ha', 'precio_chacra_kg',
        'num_emergencias', 'total_afectados', 'has_cultivo_perdidas',
        'n_noticias',
        'temp_max_c', 'temp_min_c', 'precipitacion_mm', 
        'humedad_rel_pct', 'velocidad_viento', 'radiacion_solar'
    ]
    # Filtrar solo las que existen en el dataframe
    cols_exist = [c for c in cols_scaled if c in df.columns]
    
    df_real = df.copy()
    if len(cols_exist) == len(scaler.scale_):
        df_real[cols_exist] = scaler.inverse_transform(df[cols_exist].fillna(0))
        print('  Scaler cargado — valores desnormalizados para gráficos.')
    else:
        print(f'  ⚠️ Dimensiones del scaler ({len(scaler.scale_)}) no coinciden con columnas ({len(cols_exist)}).')
        print('  Usando valores escalados para evitar errores.')
else:
    df_real = df.copy()
    print('  Scaler no encontrado — usando valores escalados.')

# ─────────────────────────────────────────────
# 10.1 Producción vs Precio vs Precipitación
# ─────────────────────────────────────────────
print('\n[10.1] Gráfico 1: Multivariable Producción-Precio-Clima')
trend = df_real.groupby('fecha_evento').agg(
    produccion_total=('produccion_t', 'sum'),
    precio_promedio=('precio_chacra_kg', 'mean'),
    precip_promedio=('precipitacion_mm', 'mean') if 'precipitacion_mm' in df_real.columns else ('produccion_t', 'mean')
).reset_index().sort_values('fecha_evento')

fig, ax1 = plt.subplots(figsize=(15, 7))
x = range(len(trend))

# Área: Producción
ax1.fill_between(x, trend['produccion_total'], alpha=0.2, color='forestgreen')
ax1.plot(x, trend['produccion_total'], color='forestgreen', linewidth=2, label='Producción (t)')
ax1.set_ylabel('Producción Total (t)', color='forestgreen', fontsize=12)
ax1.set_xticks(range(0, len(trend), 6))
ax1.set_xticklabels(trend['fecha_evento'].iloc[::6], rotation=45, ha='right')

# Eje 2: Precio
ax2 = ax1.twinx()
ax2.plot(x, trend['precio_promedio'], color='darkorange', linewidth=2, linestyle='--', label='Precio (S/./kg)')
ax2.set_ylabel('Precio Chacra (S/./kg)', color='darkorange', fontsize=12)

# Eje 3: Precipitación
if 'precipitacion_mm' in df_real.columns:
    ax3 = ax1.twinx()
    ax3.spines['right'].set_position(('axes', 1.12))
    ax3.plot(x, trend['precip_promedio'], color='royalblue', linewidth=1.5, linestyle=':', label='Precip. (mm)')
    ax3.set_ylabel('Precipitación (mm/día)', color='royalblue', fontsize=12)

fig.suptitle('Análisis Multimodal: Producción, Precio y Clima (2021-2025)', fontsize=15, fontweight='bold')
plt.tight_layout()

g1_path = os.path.join(REPORTS_DIR, 'g4_produccion_vs_precio_vs_clima.png')
plt.savefig(g1_path, dpi=150, bbox_inches='tight')
plt.close()
print(f'  [OK] {g1_path}')

# ─────────────────────────────────────────────
# 10.2 Heatmap de Correlación Extendido
# ─────────────────────────────────────────────
print('\n[10.2] Gráfico 2: Heatmap de correlación extendido (Multimodal + NASA)')
corr_cols = [
    'produccion_t', 'precio_chacra_kg', 'num_emergencias', 'n_noticias',
    'temp_max_c', 'precipitacion_mm', 'humedad_rel_pct', 'month_sin', 'month_cos'
]
corr_cols = [c for c in corr_cols if c in df.columns]
corr_matrix = df[corr_cols].corr()

fig, ax = plt.subplots(figsize=(12, 10))
mask = np.triu(np.ones_like(corr_matrix, dtype=bool))
sns.heatmap(corr_matrix, mask=mask, cmap='coolwarm', center=0, annot=True, fmt='.2f',
            square=True, linewidths=0.5, cbar_kws={"shrink": 0.8}, ax=ax)
ax.set_title('Mapa de Calor de Correlación: Producción × Emergencias × Clima × Estacionalidad',
             fontsize=14, fontweight='bold')
plt.tight_layout()

g2_path = os.path.join(REPORTS_DIR, 'g5_correlacion_heatmap_final.png')
plt.savefig(g2_path, dpi=150, bbox_inches='tight')
plt.close()
print(f'  [OK] {g2_path}')

# ─────────────────────────────────────────────
# 10.3 Boxplot Producción por Año (Anomalías)
# ─────────────────────────────────────────────
print('\n[10.3] Gráfico 3: Boxplot Producción por Año')
fig, ax = plt.subplots(figsize=(10, 6))
sns.boxplot(data=df_real, x='anho', y='produccion_t', palette='YlGn', ax=ax)
ax.set_title('Distribución de Producción de Limón por Año (Detección de Anomalías)', fontsize=14, fontweight='bold')
ax.set_ylabel('Producción (t)')
plt.tight_layout()

g3_path = os.path.join(REPORTS_DIR, 'g6_boxplot_anual.png')
plt.savefig(g3_path, dpi=150, bbox_inches='tight')
plt.close()
print(f'  [OK] {g3_path}')

print()
print('[ACTIVIDAD 10] COMPLETADA.')
print(f'  Resultado: 3 gráficos finales con integración climática NASA generados.')
