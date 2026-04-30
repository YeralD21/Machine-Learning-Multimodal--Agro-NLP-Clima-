"""
Generador de Notebooks — Actividades 03 a 10
Ejecutar desde la raíz del proyecto.
"""
import nbformat as nbf
import os, sys
sys.stdout.reconfigure(encoding='utf-8')

NOTEBOOKS_DIR = "notebooks"
os.makedirs(NOTEBOOKS_DIR, exist_ok=True)

def nb(cells_data, filename):
    notebook = nbf.v4.new_notebook()
    notebook.metadata['kernelspec'] = {'display_name':'Python 3','language':'python','name':'python3'}
    for t, s in cells_data:
        if t == 'md': notebook.cells.append(nbf.v4.new_markdown_cell(s))
        else:         notebook.cells.append(nbf.v4.new_code_cell(s))
    path = os.path.join(NOTEBOOKS_DIR, filename)
    with open(path, 'w', encoding='utf-8') as f:
        nbf.write(notebook, f)
    print(f"[OK] {path}")

# ═══════════════════════════════════════════════════════════════════
# SETUP CELL — Se repite en todos los notebooks
# ═══════════════════════════════════════════════════════════════════
SETUP = """
import os, sys, json, glob, re, warnings, unicodedata
import numpy as np
import pandas as pd
import matplotlib

import matplotlib.pyplot as plt
import seaborn as sns
warnings.filterwarnings('ignore')
pd.set_option('display.max_columns', None)
sns.set_theme(style='whitegrid', palette='muted')

# Navegar a la raíz del proyecto
if os.path.basename(os.getcwd()) == 'notebooks':
    os.chdir(os.path.abspath('..'))
print(f"Directorio: {os.getcwd()}")

with open('data/02_interim/pipeline_config.json', 'r', encoding='utf-8') as f:
    CFG = json.load(f)
DIRS = CFG['DIRS']
INTERIM = DIRS['interim']
REPORTS = DIRS['reports']
PROCESSED = DIRS['processed']
print("Configuración cargada OK")
"""

# ═══════════════════════════════════════════════════════════════════
# ACTIVIDAD 03 — EDA
# ═══════════════════════════════════════════════════════════════════
act03 = [
('md', """# 📊 Actividad 03: Análisis Exploratorio de Datos (EDA)
---
**Entrada:** `data/02_interim/midagri_limon_raw.csv`, `indeci_eventos_dbf.csv`, `agraria_noticias_raw.csv`  
**Salida:** Reporte geográfico TXT + 3 gráficos en `data/04_reports/`

> Este notebook genera el **Reporte Estructurado de 23 Departamentos** que producen limón en Perú,
> analiza la distribución de emergencias INDECI por fenómeno y la frecuencia de noticias Agraria.pe.
"""),

('code', SETUP),

('md', """## 3.1 Reporte Geográfico — 23 Departamentos Productores de Limón
Cargamos el dataset de MIDAGRI filtrado para calcular la producción total, número de provincias
y participación porcentual de cada departamento.
"""),

('code', """
# Cargar MIDAGRI limón
df_m = pd.read_csv(f"{INTERIM}/midagri_limon_raw.csv")
df_m['PRODUCCION(t)'] = pd.to_numeric(df_m['PRODUCCION(t)'], errors='coerce').fillna(0)

# Normalización geográfica
def norm(t):
    if not isinstance(t, str): return t
    t = t.strip().upper()
    for a,b in [('Á','A'),('É','E'),('Í','I'),('Ó','O'),('Ú','U'),('Ñ','__N__')]:
        t = t.replace(a,b)
    t = ''.join(c for c in unicodedata.normalize('NFD',t) if unicodedata.category(c)!='Mn')
    return t.replace('__N__','Ñ')

df_m['Dpto'] = df_m['Dpto'].apply(norm)
df_m['Prov'] = df_m['Prov'].apply(norm)

# Reporte por departamento
geo = (df_m.groupby('Dpto')
       .agg(produccion_total_t=('PRODUCCION(t)','sum'),
            n_provincias=('Prov','nunique'),
            n_registros=('PRODUCCION(t)','count'))
       .sort_values('produccion_total_t', ascending=False)
       .reset_index())
total = geo['produccion_total_t'].sum()
geo['pct'] = (geo['produccion_total_t'] / total * 100).round(2)

print(f"Total departamentos con limón: {len(geo)}")
print(f"Total producción 2021-2025: {total:,.2f} toneladas\\n")
print(f"{'DEPARTAMENTO':<25} {'PRODUCCIÓN (t)':>15} {'PROVINCIAS':>10} {'REGISTROS':>10} {'% PART.':>8}")
print('-'*73)
for _, r in geo.iterrows():
    print(f"{r['Dpto']:<25} {r['produccion_total_t']:>15,.2f} {int(r['n_provincias']):>10} {int(r['n_registros']):>10} {r['pct']:>7.2f}%")
print('-'*73)
print(f"{'TOTAL':<25} {total:>15,.2f} {'':>10} {int(df_m.shape[0]):>10} {'100.00%':>8}")
"""),

('md', """### 3.1.1 Detalle por Provincias (Top Departamentos)
Exploramos las provincias dentro de los departamentos con mayor producción.
"""),

('code', """
# Top 5 departamentos — desglose provincial
top5_dptos = geo.head(5)['Dpto'].tolist()
for dpto in top5_dptos:
    df_prov = (df_m[df_m['Dpto']==dpto]
               .groupby('Prov')['PRODUCCION(t)'].sum()
               .sort_values(ascending=False)
               .reset_index())
    df_prov['pct_dpto'] = (df_prov['PRODUCCION(t)']/df_prov['PRODUCCION(t)'].sum()*100).round(1)
    print(f"\\n  {dpto}:")
    for _, r in df_prov.iterrows():
        print(f"    {r['Prov']:<30} {r['PRODUCCION(t)']:>12,.2f} t  ({r['pct_dpto']:.1f}%)")
"""),

('md', "## 3.2 Gráfico 1 — Producción por Departamento"),

('code', """
fig, axes = plt.subplots(1, 2, figsize=(16, 6))

# Barras horizontales
colors = plt.cm.YlGn(np.linspace(0.4, 0.95, len(geo)))[::-1]
bars = axes[0].barh(geo['Dpto'], geo['produccion_total_t'], color=colors)
axes[0].set_xlabel('Producción Total (t)', fontsize=11)
axes[0].set_title('Producción de Limón por Departamento\\n(2021-2025)', fontsize=13, fontweight='bold')
axes[0].invert_yaxis()
for bar, val in zip(bars, geo['produccion_total_t']):
    if val > total*0.01:
        axes[0].text(bar.get_width()+total*0.002, bar.get_y()+bar.get_height()/2,
                     f'{val/1000:.1f}K t', va='center', fontsize=7)

# Torta (top 8 + otros)
top8 = geo.head(8).copy()
otros_val = geo.iloc[8:]['produccion_total_t'].sum()
otros_pct = geo.iloc[8:]['pct'].sum()
pie_vals = list(top8['produccion_total_t']) + [otros_val]
pie_labels = list(top8['Dpto'].str[:12]) + [f'OTROS ({otros_pct:.1f}%)']
wedges, texts, autotexts = axes[1].pie(
    pie_vals, labels=pie_labels, autopct='%1.1f%%',
    startangle=140, pctdistance=0.8,
    colors=plt.cm.Set3(np.linspace(0, 1, len(pie_vals))))
axes[1].set_title('Participación % en Producción\\nTop 8 + Otros', fontsize=13, fontweight='bold')

plt.tight_layout()
g_path = f"{REPORTS}/g1_produccion_por_dpto.png"
plt.savefig(g_path, dpi=150, bbox_inches='tight')
plt.show()
print(f"[OK] {g_path}")
"""),

('md', "## 3.3 INDECI — Distribución de Fenómenos de Emergencia"),

('code', """
df_ev = pd.read_csv(f"{INTERIM}/indeci_eventos_dbf.csv", low_memory=False)
df_ev['fenomeno'] = df_ev['fenomeno'].astype(str).str.strip().str.upper()

top_fen = df_ev['fenomeno'].value_counts().head(12)
print(f"Total eventos: {len(df_ev):,}")
print("\\nTop 12 fenómenos:")
for fen, cnt in top_fen.items():
    print(f"  {fen:<40} {cnt:>6,}  ({cnt/len(df_ev)*100:.1f}%)")

fig, ax = plt.subplots(figsize=(12, 5))
top_fen.sort_values().plot(kind='barh', ax=ax, color='steelblue', edgecolor='navy', linewidth=0.5)
ax.set_xlabel('Cantidad de Eventos', fontsize=11)
ax.set_title('Top 12 Fenómenos de Emergencia — INDECI (2021-2023)', fontsize=13, fontweight='bold')
for p in ax.patches:
    ax.text(p.get_width()+10, p.get_y()+p.get_height()/2,
            f'{int(p.get_width()):,}', va='center', fontsize=8)
plt.tight_layout()
g_path2 = f"{REPORTS}/g2_indeci_fenomenos.png"
plt.savefig(g_path2, dpi=150, bbox_inches='tight')
plt.show()
print(f"[OK] {g_path2}")
"""),

('md', "## 3.4 AGRARIA.PE — Frecuencia de Noticias"),

('code', """
df_n = pd.read_csv(f"{INTERIM}/agraria_noticias_raw.csv")
df_n['fecha'] = pd.to_datetime(df_n['fecha'], errors='coerce')
df_n['anho'] = df_n['fecha'].dt.year
df_n['mes_periodo'] = df_n['fecha'].dt.to_period('M').astype(str)

print(f"Total noticias: {len(df_n)}")
print("\\nNoticias por año:")
print(df_n['anho'].value_counts().sort_index().to_string())

fig, axes = plt.subplots(1, 2, figsize=(14, 5))
freq_mes = df_n.groupby('mes_periodo').size()
freq_mes.plot(ax=axes[0], color='coral', marker='o', markersize=3, linewidth=1.5)
axes[0].set_title('Noticias por Mes', fontsize=12, fontweight='bold')
axes[0].set_xlabel('Mes'); axes[0].set_ylabel('Cantidad')
axes[0].tick_params(axis='x', rotation=60)
step = max(1, len(freq_mes)//8)
axes[0].set_xticks(range(0, len(freq_mes), step))
axes[0].set_xticklabels(freq_mes.index[::step], rotation=60, ha='right', fontsize=8)

freq_anho = df_n.groupby('anho').size()
bars = axes[1].bar(freq_anho.index.astype(str), freq_anho.values,
                   color=['#2ecc71','#3498db','#9b59b6','#e74c3c','#f39c12'], edgecolor='black')
axes[1].set_title('Noticias por Año', fontsize=12, fontweight='bold')
for bar in bars:
    axes[1].text(bar.get_x()+bar.get_width()/2, bar.get_height()+1,
                 str(int(bar.get_height())), ha='center', fontsize=11, fontweight='bold')
plt.tight_layout()
g_path3 = f"{REPORTS}/g3_noticias_frecuencia.png"
plt.savefig(g_path3, dpi=150, bbox_inches='tight')
plt.show()
print(f"[OK] {g_path3}")
"""),

('md', """## 3.5 NASA POWER — EDA Climático
Analizamos las tendencias de temperatura y precipitación de la NASA POWER."""),
('code', """
nasa_path = f"data/03_processed_nasa/nasa_climatic_raw_values.csv"
if os.path.exists(nasa_path):
    df_nasa = pd.read_csv(nasa_path)
    df_nasa['fecha'] = pd.to_datetime(df_nasa['fecha_evento'])
    
    fig, axes = plt.subplots(1, 2, figsize=(16, 5))
    
    # Serie temporal temperatura
    df_nasa.groupby('fecha_evento')['T2M_MAX'].mean().plot(ax=axes[0], color='red', label='Temp Max')
    df_nasa.groupby('fecha_evento')['T2M_MIN'].mean().plot(ax=axes[0], color='blue', label='Temp Min')
    axes[0].set_title('Tendencia de Temperatura Mensual (NASA)', fontweight='bold')
    axes[0].legend()
    
    # Heatmap Precipitaciones
    pivot = df_nasa.pivot_table(values='PRECTOTCORR', index='DEPARTAMENTO', columns='fecha_evento')
    sns.heatmap(pivot, cmap='YlGnBu', ax=axes[1])
    axes[1].set_title('Mapa de Calor: Precipitaciones (mm/día)', fontweight='bold')
    
    plt.tight_layout()
    plt.show()
else:
    print("⚠️ Datos NASA no encontrados para EDA.")
"""),

('code', """
# Guardar reporte geográfico TXT
report_path = f"{REPORTS}/reporte_geografico_limon.txt"
with open(report_path, 'w', encoding='utf-8') as f:
    f.write("REPORTE ESTRUCTURADO: PRODUCCIÓN DE LIMÓN POR DEPARTAMENTO (2021-2025)\\n")
    f.write("="*75+"\\n")
    f.write(f"{'DEPARTAMENTO':<25} {'PRODUCCIÓN (t)':>15} {'PROVINCIAS':>10} {'% PART.':>8}\\n")
    f.write("-"*75+"\\n")
    for _, r in geo.iterrows():
        f.write(f"{r['Dpto']:<25} {r['produccion_total_t']:>15,.2f} {int(r['n_provincias']):>10} {r['pct']:>7.2f}%\\n")
    f.write("-"*75+"\\n")
    f.write(f"{'TOTAL':<25} {total:>15,.2f} {'':>10} {'100.00%':>8}\\n")

print(f"[OK] Reporte guardado: {report_path}")
print()
print("[ACTIVIDAD 03] COMPLETADA.")
print(f"  Departamentos analizados: {len(geo)}")
print(f"  Gráficos generados: g1, g2, g3 en {REPORTS}/")
"""),
]

nb(act03, "actividad_03_eda.ipynb")

# ═══════════════════════════════════════════════════════════════════
# ACTIVIDAD 04 — CALIDAD DE DATOS
# ═══════════════════════════════════════════════════════════════════
act04 = [
('md', """# 🔍 Actividad 04: Calidad de los Datos
---
**Entrada:** CSVs de `data/02_interim/`  
**Salida:** `data/04_reports/reporte_calidad_datos.txt`

> Análisis visual de nulos, duplicados y outliers para las 3 fuentes del pipeline.
"""),

('code', SETUP),

('md', "## 4.1 Función de Diagnóstico de Calidad"),

('code', """
def quality_report(df, name, key_cols=None):
    print(f"\\n{'='*60}")
    print(f"  {name}")
    print(f"{'='*60}")
    print(f"  Shape: {df.shape[0]:,} filas x {df.shape[1]} columnas")
    
    # Nulos
    nulls = df.isnull().sum()
    nulls_pct = (nulls/len(df)*100).round(2)
    null_df = pd.DataFrame({'Nulos': nulls, '% Nulos': nulls_pct})
    null_df = null_df[null_df['Nulos'] > 0].sort_values('Nulos', ascending=False)
    
    if len(null_df) > 0:
        print(f"\\n  Columnas con valores nulos ({len(null_df)}):")
        print(null_df.to_string())
    else:
        print("  ✅ Sin valores nulos.")
    
    # Duplicados
    if key_cols:
        dupes = df.duplicated(subset=key_cols).sum()
        print(f"\\n  Duplicados (llave {key_cols}): {dupes}")
    else:
        dupes = df.duplicated().sum()
        print(f"\\n  Duplicados totales: {dupes}")
    
    return null_df, dupes

df_m = pd.read_csv(f"{INTERIM}/midagri_limon_raw.csv")
df_ev = pd.read_csv(f"{INTERIM}/indeci_eventos_dbf.csv", low_memory=False)
df_n  = pd.read_csv(f"{INTERIM}/agraria_noticias_raw.csv")

null_m,  dup_m  = quality_report(df_m,  "MIDAGRI — midagri_limon_raw.csv",    ['anho','mes','COD_UBIGEO','dsc_Cultivo'])
null_ev, dup_ev = quality_report(df_ev, "INDECI — indeci_eventos_dbf.csv",     ['ide_sinpad'])
null_n,  dup_n  = quality_report(df_n,  "AGRARIA.PE — agraria_noticias_raw.csv", ['url'])
"""),

('md', "## 4.2 Visualización de Nulos por Fuente"),

('code', """
fig, axes = plt.subplots(1, 3, figsize=(18, 5))

fuentes = [
    (df_m,  "MIDAGRI",    'mediumseagreen'),
    (df_ev, "INDECI",     'steelblue'),
    (df_n,  "AGRARIA.PE", 'coral'),
]

for ax, (df, nombre, color) in zip(axes, fuentes):
    nulls = (df.isnull().sum()/len(df)*100).sort_values(ascending=False)
    nulls = nulls[nulls > 0].head(15)
    if len(nulls) > 0:
        nulls.plot(kind='bar', ax=ax, color=color, edgecolor='black', linewidth=0.5)
        ax.set_title(f'{nombre}\\n% Nulos por Columna', fontsize=11, fontweight='bold')
        ax.set_ylabel('% Nulos')
        ax.tick_params(axis='x', rotation=60)
        for p in ax.patches:
            if p.get_height() > 0:
                ax.text(p.get_x()+p.get_width()/2, p.get_height()+0.5,
                        f'{p.get_height():.1f}%', ha='center', fontsize=7)
    else:
        ax.text(0.5, 0.5, '✅ Sin nulos', ha='center', va='center',
                transform=ax.transAxes, fontsize=14, color='green', fontweight='bold')
        ax.set_title(f'{nombre}\\nCalidad de Nulos', fontsize=11, fontweight='bold')

plt.suptitle('Análisis de Valores Nulos por Fuente de Datos', fontsize=14, fontweight='bold')
plt.tight_layout()
g_path = f"{REPORTS}/g4_calidad_nulos.png"
plt.savefig(g_path, dpi=150, bbox_inches='tight')
plt.show()
print(f"[OK] {g_path}")
"""),

('md', "## 4.3 Análisis de Outliers — Producción MIDAGRI"),

('code', """
prod = pd.to_numeric(df_m['PRODUCCION(t)'], errors='coerce').dropna()
q1, q3 = prod.quantile(0.25), prod.quantile(0.75)
iqr = q3 - q1
lower, upper = q1 - 1.5*iqr, q3 + 1.5*iqr
outliers = prod[(prod < lower) | (prod > upper)]

print(f"Estadísticas de Producción (t):")
print(f"  Min:    {prod.min():>10.2f}")
print(f"  Q1:     {q1:>10.2f}")
print(f"  Median: {prod.median():>10.2f}")
print(f"  Q3:     {q3:>10.2f}")
print(f"  Max:    {prod.max():>10.2f}")
print(f"  IQR:    {iqr:>10.2f}")
print(f"  Outliers (1.5×IQR): {len(outliers):,} ({len(outliers)/len(prod)*100:.1f}%)")

fig, axes = plt.subplots(1, 2, figsize=(14, 5))
axes[0].boxplot(prod.clip(upper=prod.quantile(0.99)), patch_artist=True,
                boxprops=dict(facecolor='lightgreen'))
axes[0].set_title('Boxplot Producción (t)\\n(truncado en p99)', fontsize=12, fontweight='bold')
axes[0].set_ylabel('Toneladas')
axes[0].axhline(upper, color='red', linestyle='--', label=f'Límite IQR: {upper:.0f}')
axes[0].legend()

prod.clip(upper=prod.quantile(0.99)).hist(bins=50, ax=axes[1], color='mediumseagreen', edgecolor='black')
axes[1].set_title('Distribución de Producción (t)\\n(truncado en p99)', fontsize=12, fontweight='bold')
axes[1].set_xlabel('Toneladas'); axes[1].set_ylabel('Frecuencia')

plt.tight_layout()
g_path2 = f"{REPORTS}/g5_outliers_produccion.png"
plt.savefig(g_path2, dpi=150, bbox_inches='tight')
plt.show()
print(f"[OK] {g_path2}")
"""),

('md', "## 4.4 Resumen de Calidad — Tabla de Decisiones"),

('code', """
# Tabla resumen de decisiones por fuente
resumen = {
    'Fuente': ['MIDAGRI', 'INDECI Eventos', 'AGRARIA.PE'],
    'Filas': [len(df_m), len(df_ev), len(df_n)],
    'Columnas': [df_m.shape[1], df_ev.shape[1], df_n.shape[1]],
    'Cols con Nulos': [len(null_m), len(null_ev), len(null_n)],
    'Duplicados': [dup_m, dup_ev, dup_n],
    'Acción': [
        'Renombrar cols + normalizar geo',
        'Filtrar fenómenos hidrometeorológicos + normalizar geo',
        'Limpiar HTML/URLs + normalizar texto'
    ]
}
df_resumen = pd.DataFrame(resumen)
print(df_resumen.to_string(index=False))

# Guardar reporte TXT
report_path = f"{REPORTS}/reporte_calidad_datos.txt"
with open(report_path, 'w', encoding='utf-8') as f:
    f.write("REPORTE DE CALIDAD DE DATOS — FASE 1\\n")
    f.write("="*60+"\\n")
    f.write(df_resumen.to_string(index=False))
    f.write("\\n\\nDecisiones de limpieza documentadas.\\n")
print(f"\\n[OK] {report_path}")
print("[ACTIVIDAD 04] COMPLETADA.")
"""),

('md', """## 4.5 NASA POWER — Calidad Climática
Validamos que los datos climáticos estén dentro de los rangos físicos esperados."""),
('code', """
nasa_path = f"data/03_processed_nasa/nasa_climatic_raw_values.csv"
if os.path.exists(nasa_path):
    df_nasa = pd.read_csv(nasa_path)
    print("NASA - Nulos por columna:")
    print(df_nasa.isnull().sum())
    
    # Validaciones de rango
    t_out = ((df_nasa['T2M_MAX'] < 0) | (df_nasa['T2M_MAX'] > 50)).sum()
    p_out = (df_nasa['PRECTOTCORR'] < 0).sum()
    print(f"\\nAlertas de Calidad NASA:")
    print(f"  - Temperaturas fuera de rango (0-50°C): {t_out}")
    print(f"  - Precipitaciones negativas: {p_out}")
    
    if t_out + p_out == 0:
        print("\\n✅ Datos climáticos de NASA superan validación de rangos físicos.")
else:
    print("⚠️ No hay datos NASA para validar calidad.")
"""),
]

nb(act04, "actividad_04_calidad.ipynb")

print("\n✅ Notebooks 03 y 04 generados.")
