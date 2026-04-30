"""
Pipeline Fase 1 - Actividad 3: Análisis Exploratorio de Datos (EDA)
Lee los intermedios de Actividad 2 y genera el Reporte Geográfico de 23
departamentos, análisis de frecuencia de noticias, distribución de peligros
INDECI y guarda gráficos en data/04_reports/.
"""
import sys; sys.stdout.reconfigure(encoding='utf-8')
import os, json, warnings
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import seaborn as sns

warnings.filterwarnings('ignore')
sns.set_theme(style='whitegrid', palette='muted')

with open('data/02_interim/pipeline_config.json', 'r', encoding='utf-8') as f:
    CONFIG = json.load(f)

DIRS = CONFIG['DIRS']
REPORTS_DIR = DIRS['reports']
INTERIM_DIR = DIRS['interim']

print('=' * 70)
print('  ACTIVIDAD 3: Análisis Exploratorio de Datos (EDA)')
print('=' * 70)

# -------------------------------------------------------------------------
# 3.1 REPORTE GEOGRÁFICO — 23 Departamentos Productores de Limón
# -------------------------------------------------------------------------
print('\n[3.1] REPORTE ESTRUCTURADO: ORIGEN GEOGRÁFICO DEL LIMÓN')
df_m = pd.read_csv(os.path.join(INTERIM_DIR, 'midagri_limon_raw.csv'))

# Normalizar: MAYÚSCULAS sin tildes
import unicodedata

def normalize_geo(text):
    if not isinstance(text, str): return text
    text = text.strip().upper()
    # Proteger Ñ
    text = text.replace('Ñ', '__NN__').replace('ñ', '__nn__')
    # Quitar tildes
    nfkd = unicodedata.normalize('NFD', text)
    text = ''.join(c for c in nfkd if unicodedata.category(c) != 'Mn')
    # Restaurar Ñ
    text = text.replace('__NN__', 'Ñ').replace('__nn__', 'ñ')
    return text

df_m['Dpto'] = df_m['Dpto'].apply(normalize_geo)
df_m['Prov'] = df_m['Prov'].apply(normalize_geo)
df_m['PRODUCCION(t)'] = pd.to_numeric(df_m['PRODUCCION(t)'], errors='coerce').fillna(0)

# Agrupar por Departamento
geo_dpto = (
    df_m.groupby('Dpto')
    .agg(
        produccion_total_t=('PRODUCCION(t)', 'sum'),
        n_provincias=('Prov', 'nunique'),
        n_registros=('PRODUCCION(t)', 'count'),
    )
    .sort_values('produccion_total_t', ascending=False)
    .reset_index()
)
total_prod = geo_dpto['produccion_total_t'].sum()
geo_dpto['pct_participacion'] = (geo_dpto['produccion_total_t'] / total_prod * 100).round(2)
geo_dpto['produccion_total_t'] = geo_dpto['produccion_total_t'].round(2)

print()
print('  DEPARTAMENTO             | PRODUCCION(t)  | PROVINCIAS | REGISTROS | % PART.')
print('  ' + '-' * 75)
for _, row in geo_dpto.iterrows():
    dpto_fmt = str(row['Dpto'])[:24].ljust(24)
    print(f"  {dpto_fmt} | {row['produccion_total_t']:>14,.2f} | {int(row['n_provincias']):>10} | {int(row['n_registros']):>9} | {row['pct_participacion']:>7.2f}%")
print('  ' + '-' * 75)
print(f"  {'TOTAL':<24} | {total_prod:>14,.2f} | {'':>10} | {int(df_m.shape[0]):>9} | {'100.00%':>8}")
print(f'\n  Departamentos con produccion de limon: {len(geo_dpto)}')

# Guardar reporte TXT
report_txt = os.path.join(REPORTS_DIR, 'reporte_geografico_limon.txt')
with open(report_txt, 'w', encoding='utf-8') as f:
    f.write('REPORTE ESTRUCTURADO: ORIGEN GEOGRÁFICO DEL LIMÓN (2021-2025)\n')
    f.write('=' * 80 + '\n')
    f.write(geo_dpto.to_string(index=False))
    f.write(f'\n\nTotal producción: {total_prod:,.2f} t\n')
    f.write(f'Departamentos: {len(geo_dpto)}\n')
print(f'\n  [OK] Reporte TXT: {report_txt}')

# Gráfico 1: Top departamentos por producción
fig, ax = plt.subplots(figsize=(12, 6))
top15 = geo_dpto.head(15)
bars = ax.barh(top15['Dpto'], top15['produccion_total_t'], color='mediumseagreen')
ax.set_xlabel('Producción Total (t)', fontsize=12)
ax.set_title('Top Departamentos Productores de Limón (2021-2025)', fontsize=14, fontweight='bold')
ax.invert_yaxis()
for bar, val in zip(bars, top15['produccion_total_t']):
    ax.text(bar.get_width() + total_prod * 0.002, bar.get_y() + bar.get_height()/2,
            f'{val:,.0f} t', va='center', fontsize=8)
plt.tight_layout()
g1_path = os.path.join(REPORTS_DIR, 'g1_produccion_por_dpto.png')
plt.savefig(g1_path, dpi=150, bbox_inches='tight')
plt.close()
print(f'  [OK] Gráfico 1: {g1_path}')

# -------------------------------------------------------------------------
# 3.2 INDECI — Distribución de tipos de emergencia
# -------------------------------------------------------------------------
print('\n[3.2] INDECI — Distribución de Emergencias por Tipo de Fenómeno')
df_ev = pd.read_csv(os.path.join(INTERIM_DIR, 'indeci_eventos_dbf.csv'), low_memory=False)
df_ev['fenomeno'] = df_ev['fenomeno'].astype(str).str.strip().str.upper()

top_fenomenos = df_ev['fenomeno'].value_counts().head(15)
print(f'  Total eventos cargados: {len(df_ev):,}')
print(f'  Top 15 fenómenos:')
for fen, cnt in top_fenomenos.items():
    pct = cnt / len(df_ev) * 100
    print(f'    {fen[:40]:<40} {cnt:>6,} ({pct:.1f}%)')

fig, ax = plt.subplots(figsize=(12, 6))
top_fenomenos.plot(kind='barh', ax=ax, color='steelblue')
ax.set_xlabel('Cantidad de Eventos', fontsize=12)
ax.set_title('Top 15 Fenómenos de Emergencia INDECI (2021-2023)', fontsize=14, fontweight='bold')
ax.invert_yaxis()
plt.tight_layout()
g2_path = os.path.join(REPORTS_DIR, 'g2_indeci_fenomenos.png')
plt.savefig(g2_path, dpi=150, bbox_inches='tight')
plt.close()
print(f'  [OK] Gráfico 2: {g2_path}')

# -------------------------------------------------------------------------
# 3.3 AGRARIA.PE — Frecuencia de noticias
# -------------------------------------------------------------------------
print('\n[3.3] AGRARIA.PE — Frecuencia de Noticias por Mes')
df_n = pd.read_csv(os.path.join(INTERIM_DIR, 'agraria_noticias_raw.csv'))
df_n['fecha'] = pd.to_datetime(df_n['fecha'], errors='coerce')
df_n['anho_mes'] = df_n['fecha'].dt.to_period('M').astype(str)
df_n['anho'] = df_n['fecha'].dt.year

freq_anho = df_n.groupby('anho').size()
print('  Noticias por año:')
for yr, cnt in freq_anho.items():
    print(f'    {yr}: {cnt} noticias')

fig, axes = plt.subplots(1, 2, figsize=(14, 5))
# Por mes
freq_mes = df_n.groupby('anho_mes').size()
freq_mes.plot(ax=axes[0], color='coral', marker='o', linewidth=1.5)
axes[0].set_title('Noticias por Mes (Agraria.pe)', fontsize=12, fontweight='bold')
axes[0].set_xlabel('Mes')
axes[0].set_ylabel('Cantidad')
axes[0].tick_params(axis='x', rotation=45)
# Por año
freq_anho.plot(kind='bar', ax=axes[1], color='darkorange', edgecolor='black')
axes[1].set_title('Noticias por Año', fontsize=12, fontweight='bold')
axes[1].set_xlabel('Año')
axes[1].set_ylabel('Cantidad')
for bar in axes[1].patches:
    axes[1].text(bar.get_x() + bar.get_width()/2, bar.get_height() + 1,
                 str(int(bar.get_height())), ha='center', fontsize=9)
plt.tight_layout()
g3_path = os.path.join(REPORTS_DIR, 'g3_noticias_frecuencia.png')
plt.savefig(g3_path, dpi=150, bbox_inches='tight')
plt.close()
print(f'  [OK] Gráfico 3: {g3_path}')

# TODO: INTEGRACIÓN DATA NASA
# Agregar aquí:
# - Gráfico de serie temporal de temperatura mensual promedio por región
# - Gráfico de precipitaciones acumuladas mensuales
# - Correlación inicial: clima vs produccion_t (scatter o heatmap)
# Código sugerido:
#   df_nasa = pd.read_csv(os.path.join(INTERIM_DIR, 'nasa_clima_raw.csv'))
#   df_nasa['fecha_evento'] = pd.to_datetime(df_nasa['DATE']).dt.strftime('%Y-%m')
#   clima_mensual = df_nasa.groupby('fecha_evento')[['T2M','PRECTOTCORR']].mean()
#   clima_mensual.plot(figsize=(12,5), title='Clima Mensual NASA POWER')
print('  [NASA] Placeholder para gráficos climáticos (ver TODO arriba)')

print()
print('[ACTIVIDAD 3] COMPLETADA.')
print('  Descripcion: EDA — Reporte geografico, distribucion INDECI y frecuencia de noticias.')
print(f'  Resultado: {len(geo_dpto)} departamentos analizados, 3 graficos generados.')
print(f'  Archivos generados: {report_txt}')
print(f'    {g1_path}')
print(f'    {g2_path}')
print(f'    {g3_path}')
