"""
Regenera y ejecuta los notebooks 03-10 con outputs embebidos.
Usa %matplotlib inline para que los gráficos queden en las celdas.
"""
import nbformat as nbf, subprocess, os, sys
sys.stdout.reconfigure(encoding='utf-8')

NOTEBOOKS_DIR = "notebooks"
SETUP = """%matplotlib inline
import os, sys, json, re, warnings, unicodedata
import numpy as np, pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
warnings.filterwarnings('ignore')
pd.set_option('display.max_columns', None)
sns.set_theme(style='whitegrid', palette='muted', font_scale=1.1)
plt.rcParams.update({'figure.dpi': 120, 'figure.figsize': (12, 5)})
if os.path.basename(os.getcwd()) == 'notebooks':
    os.chdir(os.path.abspath('..'))
with open('data/02_interim/pipeline_config.json','r',encoding='utf-8') as f:
    CFG = json.load(f)
DIRS=CFG['DIRS']; INTERIM=DIRS['interim']; REPORTS=DIRS['reports']; PROCESSED=DIRS['processed']
print(f"✅ Setup OK | CWD: {os.getcwd()}")"""

def make_nb(cells, filename):
    n = nbf.v4.new_notebook()
    n.metadata['kernelspec'] = {'display_name':'Python 3','language':'python','name':'python3'}
    for t, s in cells:
        n.cells.append(nbf.v4.new_markdown_cell(s) if t=='md' else nbf.v4.new_code_cell(s))
    path = os.path.join(NOTEBOOKS_DIR, filename)
    with open(path, 'w', encoding='utf-8') as f: nbf.write(n, f)
    return path

def execute(path, timeout=600):
    print(f"\n⏳ Ejecutando: {path}")
    r = subprocess.run([
        sys.executable, '-m', 'jupyter', 'nbconvert',
        '--to', 'notebook', '--execute', '--inplace',
        f'--ExecutePreprocessor.timeout={timeout}',
        '--ExecutePreprocessor.kernel_name=python3',
        path
    ], capture_output=True, text=True, encoding='utf-8')
    if r.returncode == 0:
        print(f"  ✅ Ejecutado con éxito")
    else:
        # Show last 20 lines of stderr
        lines = (r.stderr or '').strip().split('\n')
        print(f"  ⚠️  Error:\n" + '\n'.join(lines[-20:]))
    return r.returncode == 0

# ─────────────────────────────────────────────────────────────────
# ACTIVIDAD 03 — EDA
# ─────────────────────────────────────────────────────────────────
p03 = make_nb([
('md', """# 📊 Actividad 03 — EDA: Análisis Exploratorio de Datos
**Tesis:** Predicción de Producción de Limón | LSTM Multimodal  
**Fuente:** `data/02_interim/` → `data/04_reports/`"""),
('code', SETUP),
('md', "## 3.1 Carga de Datos"),
('code', """
df_m = pd.read_csv(f"{INTERIM}/midagri_limon_raw.csv")
df_m['PRODUCCION(t)'] = pd.to_numeric(df_m['PRODUCCION(t)'], errors='coerce').fillna(0)
def norm(t):
    if not isinstance(t, str): return t
    t = t.strip().upper()
    for a,b in [('Á','A'),('É','E'),('Í','I'),('Ó','O'),('Ú','U')]: t=t.replace(a,b)
    t = ''.join(c for c in unicodedata.normalize('NFD',t) if unicodedata.category(c)!='Mn')
    return t
df_m['Dpto'] = df_m['Dpto'].apply(norm)
df_m['Prov'] = df_m['Prov'].apply(norm)
print(f"MIDAGRI: {len(df_m):,} registros | {df_m['Dpto'].nunique()} departamentos")
df_m.head()
"""),
('md', "## 3.2 Reporte Geográfico — 23 Departamentos Productores de Limón"),
('code', """
geo = (df_m.groupby('Dpto')
       .agg(prod_total=('PRODUCCION(t)','sum'), n_prov=('Prov','nunique'), n_reg=('PRODUCCION(t)','count'))
       .sort_values('prod_total', ascending=False).reset_index())
total = geo['prod_total'].sum()
geo['pct'] = (geo['prod_total']/total*100).round(2)

# Tabla completa
print(f"{'DEPARTAMENTO':<25} {'PRODUCCIÓN (t)':>16} {'PROVINCIAS':>11} {'%':>7}")
print('-'*62)
for _, r in geo.iterrows():
    print(f"{r['Dpto']:<25} {r['prod_total']:>16,.2f} {int(r['n_prov']):>11} {r['pct']:>6.2f}%")
print('-'*62)
print(f"{'TOTAL':>25} {total:>16,.2f} {'':>11} {'100.00%':>7}")
"""),
('md', "## 3.3 Gráfico 1 — Producción por Departamento (Barra Horizontal)"),
('code', """
fig, axes = plt.subplots(1, 2, figsize=(18, 8))
colors = plt.cm.YlGn(np.linspace(0.3, 0.95, len(geo)))[::-1]
axes[0].barh(geo['Dpto'], geo['prod_total'], color=colors, edgecolor='white', linewidth=0.5)
axes[0].set_xlabel('Producción Total (t)', fontsize=12)
axes[0].set_title(f'Los {len(geo)} Departamentos Productores de Limón\\n2021-2025', fontsize=13, fontweight='bold')
axes[0].invert_yaxis()
for i, (val, dpto) in enumerate(zip(geo['prod_total'], geo['Dpto'])):
    axes[0].text(val + total*0.001, i, f'{val/1000:.1f}K', va='center', fontsize=7.5)

# Participación % (top 8 + otros)
top = geo.head(8); otros = geo.iloc[8:]
pvals = list(top['prod_total']) + [otros['prod_total'].sum()]
plbls = [d[:14] for d in top['Dpto']] + [f'Otros ({len(otros)} dptos)']
wcols = plt.cm.Set3(np.linspace(0, 1, len(pvals)))
wedges, _, autotexts = axes[1].pie(pvals, labels=plbls, autopct='%1.1f%%',
    startangle=140, colors=wcols, pctdistance=0.82)
axes[1].set_title('Participación % Top 8 Departamentos', fontsize=13, fontweight='bold')
plt.tight_layout()
plt.savefig(f"{REPORTS}/g01_prod_dpto.png", dpi=150, bbox_inches='tight')
plt.show()
"""),
('md', "## 3.4 Gráfico 2 — Serie Temporal de Producción Nacional"),
('code', """
df_m['fecha_dt'] = pd.to_datetime(df_m['anho'].astype(str)+'-'+df_m['mes'].astype(str), format='%Y-%m')
ts = df_m.groupby('fecha_dt')['PRODUCCION(t)'].sum().reset_index()
fig, ax = plt.subplots(figsize=(14,5))
ax.fill_between(ts['fecha_dt'], ts['PRODUCCION(t)'], alpha=0.3, color='green')
ax.plot(ts['fecha_dt'], ts['PRODUCCION(t)'], 'g-o', markersize=4, linewidth=2)
ax.set_title('Serie Temporal — Producción Nacional de Limón (2021-2025)', fontsize=14, fontweight='bold')
ax.set_xlabel('Fecha'); ax.set_ylabel('Producción Total (t)')
ax.xaxis.set_tick_params(rotation=45)
plt.tight_layout(); plt.savefig(f"{REPORTS}/g02_serie_temporal.png", dpi=150, bbox_inches='tight')
plt.show()
"""),
('md', "## 3.5 INDECI — Top Fenómenos de Emergencia"),
('code', """
df_ev = pd.read_csv(f"{INTERIM}/indeci_eventos_dbf.csv", low_memory=False)
df_ev['fenomeno'] = df_ev['fenomeno'].astype(str).str.upper().str.strip()
top_fen = df_ev['fenomeno'].value_counts().head(12)
fig, ax = plt.subplots(figsize=(13, 5))
colors2 = sns.color_palette("Blues_r", len(top_fen))
bars = ax.barh(top_fen.index, top_fen.values, color=colors2, edgecolor='navy', linewidth=0.3)
ax.set_xlabel('Cantidad de Eventos', fontsize=12)
ax.set_title('Top 12 Fenómenos de Emergencia — INDECI SINPAD (2021-2023)', fontsize=13, fontweight='bold')
ax.invert_yaxis()
for bar in bars:
    ax.text(bar.get_width()+5, bar.get_y()+bar.get_height()/2,
            f'{int(bar.get_width()):,}', va='center', fontsize=9)
plt.tight_layout(); plt.savefig(f"{REPORTS}/g03_fenomenos.png", dpi=150, bbox_inches='tight')
plt.show()
print(f"Total eventos: {len(df_ev):,}")
"""),
('md', "## 3.6 AGRARIA.PE — Frecuencia de Noticias"),
('code', """
df_n = pd.read_csv(f"{INTERIM}/agraria_noticias_raw.csv")
df_n['fecha'] = pd.to_datetime(df_n['fecha'], errors='coerce')
df_n['anho'] = df_n['fecha'].dt.year
df_n['mes_p'] = df_n['fecha'].dt.to_period('M').astype(str)
fig, axes = plt.subplots(1, 2, figsize=(16, 5))
# Mensual
freq_m = df_n.groupby('mes_p').size()
axes[0].plot(range(len(freq_m)), freq_m.values, 'coral', marker='o', markersize=4, linewidth=1.8)
axes[0].fill_between(range(len(freq_m)), freq_m.values, alpha=0.2, color='coral')
step = max(1, len(freq_m)//8)
axes[0].set_xticks(range(0, len(freq_m), step))
axes[0].set_xticklabels(freq_m.index[::step], rotation=45, ha='right', fontsize=9)
axes[0].set_title('Noticias por Mes (Agraria.pe)', fontsize=12, fontweight='bold')
axes[0].set_ylabel('Cantidad de Noticias')
# Anual
freq_a = df_n.groupby('anho').size()
bar_colors = ['#2ecc71','#3498db','#9b59b6','#e74c3c','#f39c12']
bars2 = axes[1].bar(freq_a.index.astype(str), freq_a.values, color=bar_colors[:len(freq_a)], edgecolor='black', linewidth=0.5)
axes[1].set_title('Noticias por Año', fontsize=12, fontweight='bold')
for b in bars2:
    axes[1].text(b.get_x()+b.get_width()/2, b.get_height()+1, str(int(b.get_height())),
                 ha='center', fontsize=12, fontweight='bold')
plt.tight_layout(); plt.savefig(f"{REPORTS}/g04_noticias.png", dpi=150, bbox_inches='tight')
plt.show()
print(f"Total noticias: {len(df_n)} | Rango: {df_n['fecha'].min().date()} → {df_n['fecha'].max().date()}")
"""),
('md', """## TODO: INTEGRACIÓN DATA NASA (COMPAÑERO)
```python
df_nasa = pd.read_csv(f"{INTERIM}/nasa_clima_raw.csv")
df_nasa['DATE'] = pd.to_datetime(df_nasa['DATE'])
# Temperatura mensual
df_nasa.set_index('DATE')['T2M'].resample('M').mean().plot(figsize=(14,4), title='T2M Mensual NASA POWER')
# Precipitación acumulada
df_nasa.set_index('DATE')['PRECTOTCORR'].resample('M').sum().plot(figsize=(14,4), color='blue', title='Precipitación Acumulada')
```"""),
('code', """
print("✅ [ACTIVIDAD 03] COMPLETADA — EDA con gráficos embebidos.")
print(f"  Departamentos analizados: {len(geo)}")
print(f"  Gráficos generados en {REPORTS}/")
"""),
], "actividad_03_eda.ipynb")

ok03 = execute(p03, timeout=300)

# ─────────────────────────────────────────────────────────────────
# ACTIVIDAD 04 — CALIDAD
# ─────────────────────────────────────────────────────────────────
p04 = make_nb([
('md', """# 🔍 Actividad 04 — Calidad de los Datos
Análisis visual de nulos, duplicados y outliers para MIDAGRI, INDECI y AGRARIA.PE."""),
('code', SETUP),
('code', """
df_m  = pd.read_csv(f"{INTERIM}/midagri_limon_raw.csv")
df_ev = pd.read_csv(f"{INTERIM}/indeci_eventos_dbf.csv", low_memory=False)
df_n  = pd.read_csv(f"{INTERIM}/agraria_noticias_raw.csv")
print(f"MIDAGRI: {df_m.shape} | INDECI: {df_ev.shape} | Noticias: {df_n.shape}")
"""),
('md', "## 4.1 Heatmap de Nulos — Missingno Style (sns.heatmap)"),
('code', """
fig, axes = plt.subplots(1, 3, figsize=(18, 5))
fuentes = [(df_m.head(200), "MIDAGRI", 'YlGn'), (df_ev.head(200), "INDECI", 'Blues'), (df_n, "AGRARIA.PE", 'Oranges')]
for ax, (df, name, cmap) in zip(axes, fuentes):
    null_pct = (df.isnull().mean() * 100).reset_index()
    null_pct.columns = ['columna','pct_nulos']
    null_pct = null_pct[null_pct['pct_nulos'] > 0].sort_values('pct_nulos', ascending=False).head(20)
    if len(null_pct) > 0:
        pivot = null_pct.set_index('columna')[['pct_nulos']]
        sns.heatmap(pivot, annot=True, fmt='.1f', cmap=cmap, ax=ax, cbar=True,
                    linewidths=0.5, vmin=0, vmax=100, annot_kws={'size':8})
        ax.set_title(f'{name}\\n% Nulos por Columna', fontsize=11, fontweight='bold')
        ax.set_xlabel(''); ax.tick_params(axis='y', labelsize=7)
    else:
        ax.text(0.5, 0.5, '✅ Sin valores\\nnulos', ha='center', va='center',
                fontsize=16, color='green', fontweight='bold', transform=ax.transAxes)
        ax.set_title(f'{name}\\nCalidad: Sin Nulos', fontsize=11, fontweight='bold')
        ax.set_xticks([]); ax.set_yticks([])
plt.suptitle('Análisis de Valores Nulos por Fuente de Datos', fontsize=14, fontweight='bold', y=1.02)
plt.tight_layout()
plt.savefig(f"{REPORTS}/g05_calidad_nulos.png", dpi=150, bbox_inches='tight')
plt.show()
"""),
('md', "## 4.2 Antes vs Después — Comparativa de Estandarización"),
('code', """
print("=== ANTES de la estandarización (MIDAGRI) ===")
antes = df_m[['Dpto','Prov','dsc_Cultivo']].head(8)
display(antes)

def norm(t):
    if not isinstance(t, str): return t
    t = t.strip().upper()
    for a,b in [('Á','A'),('É','E'),('Í','I'),('Ó','O'),('Ú','U')]: t=t.replace(a,b)
    return ''.join(c for c in unicodedata.normalize('NFD',t) if unicodedata.category(c)!='Mn')

despues = antes.copy()
for c in ['Dpto','Prov','dsc_Cultivo']: despues[c] = despues[c].apply(norm)
print("\\n=== DESPUÉS de la estandarización ===")
display(despues)
"""),
('md', "## 4.3 Outliers — Producción MIDAGRI"),
('code', """
prod = pd.to_numeric(df_m['PRODUCCION(t)'], errors='coerce').dropna()
q1, q3 = prod.quantile(0.25), prod.quantile(0.75)
iqr = q3-q1; upper = q3+1.5*iqr
outliers_n = ((prod < q1-1.5*iqr) | (prod > upper)).sum()
fig, axes = plt.subplots(1, 2, figsize=(14,5))
axes[0].boxplot(prod.clip(upper=prod.quantile(0.98)), patch_artist=True,
                boxprops=dict(facecolor='lightgreen', color='darkgreen'),
                medianprops=dict(color='red', linewidth=2))
axes[0].axhline(upper, color='red', linestyle='--', linewidth=1, label=f'Límite IQR: {upper:.0f}')
axes[0].set_title(f'Boxplot Producción (t)\\nOutliers IQR 1.5x: {outliers_n:,}', fontsize=12, fontweight='bold')
axes[0].set_ylabel('Toneladas'); axes[0].legend()
prod.clip(upper=prod.quantile(0.98)).hist(bins=50, ax=axes[1], color='mediumseagreen', edgecolor='white')
axes[1].set_title('Distribución de Producción (t)\\n(truncado en p98)', fontsize=12, fontweight='bold')
axes[1].set_xlabel('Toneladas'); axes[1].set_ylabel('Frecuencia')
plt.tight_layout()
plt.savefig(f"{REPORTS}/g06_outliers.png", dpi=150, bbox_inches='tight')
plt.show()
print(f"Outliers detectados: {outliers_n:,} ({outliers_n/len(prod)*100:.1f}%)")
"""),
('md', "## 4.4 Resumen de Decisiones"),
('code', """
resumen = pd.DataFrame({
    'Fuente': ['MIDAGRI', 'INDECI Eventos', 'AGRARIA.PE'],
    'Filas': [len(df_m), len(df_ev), len(df_n)],
    'Cols con Nulos': [(df_m.isnull().sum()>0).sum(), (df_ev.isnull().sum()>0).sum(), (df_n.isnull().sum()>0).sum()],
    'Duplicados': [df_m.duplicated().sum(), df_ev.duplicated(subset=['ide_sinpad'] if 'ide_sinpad' in df_ev.columns else None).sum(), df_n.duplicated(subset=['url'] if 'url' in df_n.columns else None).sum()],
    'Acción': ['Renombrar + normalizar geo', 'Filtrar hidrometeorológicos + normalizar geo', 'Limpiar HTML/URLs con Regex']
})
display(resumen)
print("\\n✅ [ACTIVIDAD 04] COMPLETADA")
"""),
('md', """## TODO: INTEGRACIÓN DATA NASA (COMPAÑERO)
```python
df_nasa = pd.read_csv(f"{INTERIM}/nasa_clima_raw.csv")
print("NASA nulos:\\n", df_nasa.isnull().sum())
# Validar rangos físicos
invalidos = {'T2M fuera [-10,45]°C': ((df_nasa['T2M']<-10)|(df_nasa['T2M']>45)).sum(),
             'PRECTOTCORR negativo': (df_nasa['PRECTOTCORR']<0).sum(),
             'RH2M fuera [0,100]%': ((df_nasa['RH2M']<0)|(df_nasa['RH2M']>100)).sum()}
pd.Series(invalidos).plot(kind='bar', title='Registros NASA fuera de rango físico')
```"""),
], "actividad_04_calidad.ipynb")

ok04 = execute(p04, timeout=300)

print(f"\n{'='*60}")
print(f"  Act. 03 EDA:     {'✅ OK' if ok03 else '❌ Error'}")
print(f"  Act. 04 Calidad: {'✅ OK' if ok04 else '❌ Error'}")
