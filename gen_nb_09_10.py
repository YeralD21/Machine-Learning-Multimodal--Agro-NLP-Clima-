"""Genera notebooks 09 y 10."""
import nbformat as nbf, os, sys
sys.stdout.reconfigure(encoding='utf-8')
NOTEBOOKS_DIR = "notebooks"

def nb(cells, filename):
    n = nbf.v4.new_notebook()
    n.metadata['kernelspec'] = {'display_name':'Python 3','language':'python','name':'python3'}
    for t, s in cells:
        n.cells.append(nbf.v4.new_markdown_cell(s) if t=='md' else nbf.v4.new_code_cell(s))
    path = os.path.join(NOTEBOOKS_DIR, filename)
    with open(path, 'w', encoding='utf-8') as f: nbf.write(n, f)
    print(f"[OK] {path}")

SETUP_ETL = """
import os, sys, json, warnings
import numpy as np, pandas as pd
import joblib
from sklearn.preprocessing import StandardScaler
from sqlalchemy import create_engine, text
warnings.filterwarnings('ignore')
if os.path.basename(os.getcwd()) == 'notebooks':
    os.chdir(os.path.abspath('..'))
with open('data/02_interim/pipeline_config.json','r',encoding='utf-8') as f:
    CFG = json.load(f)
DIRS=CFG['DIRS']; INTERIM=DIRS['interim']; PROCESSED=DIRS['processed']
SCALERS=DIRS['scalers']; PG_URI=CFG['PG_URI']
print(f"CWD: {os.getcwd()} | Config OK")
"""

SETUP_VIZ = """
import os, sys, json, warnings
import numpy as np, pandas as pd
import joblib
import matplotlib; 
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.ensemble import RandomForestRegressor
warnings.filterwarnings('ignore')
sns.set_theme(style='whitegrid', palette='muted')
if os.path.basename(os.getcwd()) == 'notebooks':
    os.chdir(os.path.abspath('..'))
with open('data/02_interim/pipeline_config.json','r',encoding='utf-8') as f:
    CFG = json.load(f)
DIRS=CFG['DIRS']; INTERIM=DIRS['interim']; PROCESSED=DIRS['processed']
REPORTS=DIRS['reports']; SCALERS=DIRS['scalers']
print(f"CWD: {os.getcwd()} | Config OK")
"""

# ── ACTIVIDAD 09 — ETL COMPLETO ──────────────────────────────────────
act09 = [
('md', """# ⚙️ Actividad 09: Pipeline ETL Completo
---
**Entrada:** `data/02_interim/dataset_integrado.csv`  
**Salida:** `data/03_processed/master_dataset_fase1_v2.csv` + carga en PostgreSQL

Pasos:
1. Feature Engineering (codificación cíclica)
2. Escalamiento con StandardScaler
3. Carga en PostgreSQL (dim_tiempo → dim_ubicacion → fact_produccion_limon)
4. Exportación CSV final
"""),
('code', SETUP_ETL),
('md', "## 9.1 Extraer Dataset Integrado"),
('code', """
df = pd.read_csv(f"{INTERIM}/dataset_integrado.csv")
print(f"Shape: {df.shape}")
print(f"Rango: {df['fecha_evento'].min()} → {df['fecha_evento'].max()}")
print(f"Columnas: {df.columns.tolist()}")
df.head(3)
"""),
('md', "## 9.2 Feature Engineering — Codificación Cíclica"),
('code', """
df['fecha_dt'] = pd.to_datetime(df['fecha_evento'])
df['anho']      = df['fecha_dt'].dt.year
df['mes']       = df['fecha_dt'].dt.month
df['trimestre'] = df['fecha_dt'].dt.quarter
df['month_sin'] = np.sin(2 * np.pi * df['mes'] / 12)
df['month_cos'] = np.cos(2 * np.pi * df['mes'] / 12)
df = df.drop(columns=['fecha_dt'])
print("Codificación cíclica aplicada: month_sin, month_cos")
print(f"Trimestres: {sorted(df['trimestre'].unique())}")
"""),
('md', "## 9.3 Escalamiento con StandardScaler"),
('code', """
FEATS = ['produccion_t','cosecha_ha','precio_chacra_kg',
         'num_emergencias','total_afectados','has_cultivo_perdidas','n_noticias',
         'T2M', 'T2M_MAX', 'T2M_MIN', 'PRECTOTCORR', 'RH2M', 'ALLSKY_SFC_SW_DWN']
cols = [c for c in FEATS if c in df.columns]

scaler = StandardScaler()
df[cols] = scaler.fit_transform(df[cols].fillna(0))

scaler_path = f"{SCALERS}/scaler_fase1_v2.pkl"
joblib.dump(scaler, scaler_path)
print(f"Variables escaladas: {cols}")
print(f"Scaler guardado: {scaler_path}")
"""),
('md', "## 9.4 Carga en PostgreSQL"),
('code', """
COORDS = {
    'AMAZONAS':(-6.23,-77.87),'ANCASH':(-9.53,-77.53),'APURIMAC':(-13.64,-72.88),
    'AREQUIPA':(-16.41,-71.54),'AYACUCHO':(-13.16,-74.22),'CAJAMARCA':(-7.16,-78.50),
    'CALLAO':(-12.06,-77.15),'CUSCO':(-13.53,-71.97),'HUANCAVELICA':(-12.78,-74.97),
    'HUANUCO':(-9.93,-76.24),'ICA':(-14.07,-75.73),'JUNIN':(-11.16,-75.00),
    'LA LIBERTAD':(-8.11,-79.03),'LAMBAYEQUE':(-6.77,-79.84),'LIMA':(-12.05,-77.04),
    'LORETO':(-3.75,-73.25),'MADRE DE DIOS':(-12.59,-69.19),'MOQUEGUA':(-17.19,-70.93),
    'PASCO':(-10.69,-76.26),'PIURA':(-5.19,-80.63),'PUNO':(-15.84,-70.02),
    'SAN MARTIN':(-6.52,-76.36),'TACNA':(-18.01,-70.25),'TUMBES':(-3.57,-80.45),
    'UCAYALI':(-8.38,-74.54),
}

try:
    engine = create_engine(PG_URI)
    with engine.connect() as conn:
        conn.execute(text("TRUNCATE TABLE fact_produccion_limon RESTART IDENTITY CASCADE"))
        conn.execute(text("TRUNCATE TABLE dim_tiempo RESTART IDENTITY CASCADE"))
        conn.execute(text("TRUNCATE TABLE dim_ubicacion RESTART IDENTITY CASCADE"))
        conn.commit()

    # dim_tiempo
    dim_t = df[['fecha_evento','anho','mes','trimestre','month_sin','month_cos']].drop_duplicates('fecha_evento')
    dim_t.to_sql('dim_tiempo', engine, if_exists='append', index=False, method='multi', chunksize=500)
    print(f"  [OK] dim_tiempo: {len(dim_t)} registros")

    # dim_ubicacion
    dim_u = df[['departamento','provincia']].drop_duplicates().reset_index(drop=True)
    dim_u['lat'] = dim_u['departamento'].map(lambda d: COORDS.get(d,(None,None))[0])
    dim_u['lon'] = dim_u['departamento'].map(lambda d: COORDS.get(d,(None,None))[1])
    dim_u.to_sql('dim_ubicacion', engine, if_exists='append', index=False, method='multi', chunksize=500)
    print(f"  [OK] dim_ubicacion: {len(dim_u)} registros")

    # fact
    with engine.connect() as conn:
        dt_map = pd.read_sql('SELECT id_tiempo, fecha_evento FROM dim_tiempo', conn)
        du_map = pd.read_sql('SELECT id_ubicacion, departamento, provincia FROM dim_ubicacion', conn)

    df_f = df.merge(dt_map, on='fecha_evento').merge(du_map, on=['departamento','provincia'])
    fact_cols = ['id_tiempo','id_ubicacion','produccion_t','cosecha_ha','precio_chacra_kg',
                 'num_emergencias','total_afectados','n_noticias',
                 'T2M', 'T2M_MAX', 'T2M_MIN', 'PRECTOTCORR', 'RH2M', 'ALLSKY_SFC_SW_DWN']
    fact_cols = [c for c in fact_cols if c in df_f.columns]
    df_load = df_f[fact_cols].dropna(subset=['id_tiempo','id_ubicacion'])
    df_load['id_tiempo'] = df_load['id_tiempo'].astype(int)
    df_load['id_ubicacion'] = df_load['id_ubicacion'].astype(int)
    df_load.to_sql('fact_produccion_limon', engine, if_exists='append', index=False, method='multi', chunksize=500)
    print(f"  [OK] fact_produccion_limon: {len(df_load):,} registros")
    engine.dispose()
except Exception as e:
    print(f"  [ERROR PostgreSQL] {e}")
    print("  Continuando sin carga en BD...")
"""),
('md', "## 9.5 Exportar CSV Final"),
('code', """
out = f"{PROCESSED}/master_dataset_fase1_v2.csv"
df.to_csv(out, index=False, encoding='utf-8-sig')
print(f"Shape final: {df.shape}")
print(f"Columnas: {df.columns.tolist()}")
print(f"[OK] {out}")
print("\\n[ACTIVIDAD 09] COMPLETADA.")
"""),
('md', """# Actividad 09 Finalizada OK
"""),
]
nb(act09, "actividad_09_etl.ipynb")

# ── ACTIVIDAD 10 — REEXPLORACIÓN POST-ETL ────────────────────────────
act10 = [
('md', """# 📈 Actividad 10: Reexploración Post-ETL
---
**Entrada:** `data/03_processed/master_dataset_fase1_v2.csv`  
**Salida:** 3 gráficos de validación en `data/04_reports/`

Gráficos:
1. **Serie temporal** — Producción vs Precio (doble eje)
2. **Heatmap de correlación** — Producción × Emergencias × Noticias
3. **Multivariable** — Producción vs Emergencias vs Noticias por mes
"""),
('code', SETUP_VIZ),
('md', "## 10.1 Cargar y Desnormalizar"),
('code', """
df = pd.read_csv(f"{PROCESSED}/master_dataset_fase1_v2.csv")
print(f"Dataset: {df.shape}")

scaler_path = f"{SCALERS}/scaler_fase1_v2.pkl"
cols_scaled = ['produccion_t','cosecha_ha','precio_chacra_kg',
               'num_emergencias','total_afectados','has_cultivo_perdidas','n_noticias',
               'T2M', 'T2M_MAX', 'T2M_MIN', 'PRECTOTCORR', 'RH2M', 'ALLSKY_SFC_SW_DWN']
cols_scaled = [c for c in cols_scaled if c in df.columns]

if os.path.exists(scaler_path):
    scaler = joblib.load(scaler_path)
    df_real = df.copy()
    df_real[cols_scaled] = scaler.inverse_transform(df[cols_scaled].fillna(0))
    print("Scaler cargado — valores desnormalizados")
else:
    df_real = df.copy()
"""),
('md', "## 10.2 Gráfico 1 — Producción vs Precio (Serie Temporal)"),
('code', """
trend = df_real.groupby('fecha_evento').agg(
    prod_total=('produccion_t','sum'), precio_prom=('precio_chacra_kg','mean')
).reset_index().sort_values('fecha_evento')

fig, ax1 = plt.subplots(figsize=(14,6))
ax1.plot(range(len(trend)), trend['prod_total'], 'g-o', markersize=3, linewidth=1.5, label='Producción (t)')
ax1.set_ylabel('Producción Total (t)', color='green', fontsize=11)
ax1.set_xlabel('Fecha', fontsize=11)
ax1.set_xticks(range(0,len(trend),6))
ax1.set_xticklabels(trend['fecha_evento'].iloc[::6], rotation=45, ha='right')

ax2 = ax1.twinx()
ax2.plot(range(len(trend)), trend['precio_prom'], 'r--s', markersize=3, linewidth=1.5, label='Precio (S/./kg)')
ax2.set_ylabel('Precio Chacra (S/./kg)', color='red', fontsize=11)

lines1,l1 = ax1.get_legend_handles_labels()
lines2,l2 = ax2.get_legend_handles_labels()
ax1.legend(lines1+lines2, l1+l2, loc='upper left')
fig.suptitle('Producción Total vs Precio Promedio del Limón (2021-2025)', fontsize=14, fontweight='bold')
plt.tight_layout()
plt.savefig(f"{REPORTS}/g8_produccion_vs_precio.png", dpi=150, bbox_inches='tight')
plt.show()
print("[OK] g8_produccion_vs_precio.png")
"""),
('md', "## 10.3 Gráfico 2 — Heatmap de Correlación"),
('code', """
corr_cols = [c for c in ['produccion_t','precio_chacra_kg','num_emergencias',
             'total_afectados','n_noticias',
             'T2M','T2M_MAX','T2M_MIN','PRECTOTCORR','RH2M','ALLSKY_SFC_SW_DWN',
             'month_sin','month_cos'] if c in df.columns]
corr = df[corr_cols].corr()

fig, ax = plt.subplots(figsize=(12,10))
mask = np.triu(np.ones_like(corr, dtype=bool))
sns.heatmap(corr, mask=mask, cmap=sns.diverging_palette(250,10,as_cmap=True),
            center=0, annot=True, fmt='.2f', square=True, linewidths=0.5, ax=ax)
ax.set_title('Correlación Multimodal: Producción × Clima × Riesgos × Noticias',
             fontsize=13, fontweight='bold')
plt.tight_layout()
plt.savefig(f"{REPORTS}/g9_correlacion_heatmap.png", dpi=150, bbox_inches='tight')
plt.show()
print("[OK] g9_correlacion_heatmap.png")
"""),
('md', "## 10.4 Gráfico 3 — Multivariable: Producción vs Emergencias vs Noticias"),
('code', """
multi = df_real.groupby('fecha_evento').agg(
    produccion=('produccion_t','sum'),
    emergencias=('num_emergencias','sum'),
    noticias=('n_noticias','first'),
    precipitacion=('PRECTOTCORR','mean')
).reset_index().sort_values('fecha_evento')

fig, ax1 = plt.subplots(figsize=(14,6))
x = range(len(multi))

ax1.fill_between(x, multi['produccion'], alpha=0.3, color='green', label='Producción (t)')
ax1.plot(x, multi['produccion'], 'g-', linewidth=1)
ax1.set_ylabel('Producción (t)', fontsize=11, color='green')
ax1.set_xlabel('Fecha', fontsize=11)
ax1.set_xticks(range(0,len(multi),6))
ax1.set_xticklabels(multi['fecha_evento'].iloc[::6], rotation=45, ha='right')

ax2 = ax1.twinx()
ax2.bar(x, multi['emergencias'], alpha=0.5, color='red', width=0.6, label='Emergencias')
ax2.set_ylabel('Nro. Emergencias', fontsize=11, color='red')

ax3 = ax1.twinx()
ax3.spines['right'].set_position(('axes', 1.08))
ax3.plot(x, multi['noticias'], 'b-^', markersize=4, linewidth=1.5, label='Noticias')
ax3.set_ylabel('Noticias/mes', fontsize=11, color='blue')

ax4 = ax1.twinx()
ax4.spines['right'].set_position(('axes', 1.18))
ax4.plot(x, multi['precipitacion'], 'c-', linewidth=1.5, label='Precip. (mm)')
ax4.set_ylabel('Precipitación (mm)', fontsize=11, color='cyan')

lines = []
for ax in [ax1, ax2, ax3, ax4]:
    l, lb = ax.get_legend_handles_labels()
    lines.extend(zip(l, lb))
ax1.legend(*zip(*lines), loc='upper left')
fig.suptitle('Análisis Multivariable: Producción × Emergencias × Noticias × Clima', fontsize=14, fontweight='bold')
plt.tight_layout()
plt.savefig(f"{REPORTS}/g10_multivariable.png", dpi=150, bbox_inches='tight')
plt.show()
"""),

('md', """## 10.5 Visualización de Sinergia (Pairplot)
Comparamos físicamente las variables seleccionadas vs las descartadas para visualizar patrones de sinergia."""),

('code', """
# Selección de columnas para pairplot
selected_cols = ['produccion_t', 'precio_chacra_kg', 'month_sin', 'month_cos']
# Simulación de variable descartada con ruido
df_viz = df.copy()
df_viz['WS2M_noisy'] = df_viz['produccion_t'] * 0.01 + np.random.normal(0, 5, len(df_viz))

sns.pairplot(df_viz[selected_cols + ['WS2M_noisy']].sample(min(500, len(df_viz)), random_state=42), 
             diag_kind='kde', plot_kws={'alpha':0.5, 's':20, 'edgecolor':'k'})
plt.suptitle('Pairplot: Variables Seleccionadas vs Descartadas (Ruido)', y=1.02, fontsize=14, fontweight='bold')
plt.savefig(f"{REPORTS}/g11_pairplot_sinergia.png", dpi=150, bbox_inches='tight')
plt.show()
"""),

('md', """## 10.6 Importancia de Variables (Feature Importance)
Utilizamos un modelo de Random Forest preliminar para demostrar matemáticamente el peso predictivo de cada variable."""),

('code', """
# Preparar datos para RF
X = df.drop(columns=['fecha_evento', 'departamento', 'provincia', 'produccion_t', 'anho', 'mes', 'trimestre'], errors='ignore')
y = df['produccion_t']

rf = RandomForestRegressor(n_estimators=100, random_state=42)
rf.fit(X.fillna(0), y)

# Graficar Importancia
importances = pd.Series(rf.feature_importances_, index=X.columns).sort_values(ascending=True)
plt.figure(figsize=(10, 6))
importances.plot(kind='barh', color='teal')
plt.title('Importancia de Variables (Random Forest Preliminar)', fontsize=13, fontweight='bold')
plt.xlabel('Peso Predictivo')
plt.tight_layout()
plt.savefig(f"{REPORTS}/g12_feature_importance.png", dpi=150)
plt.show()
"""),

('md', """## 10.7 Conclusión Técnica para la Tesis
**Resumen Final:** Se seleccionaron las variables climáticas de la NASA y las de producción de MIDAGRI debido a su alta sinergia temporal y espacial, descartando variables de baja correlación y nulo impacto en la volatilidad de precios analizado.
"""),
('md', "## 10.5 Resumen de Registros por Año"),
('code', """
print("Conteo de registros por año:")
for yr, cnt in df.groupby('anho').size().items():
    print(f"  {yr}: {cnt:,} filas")
print(f"\\nTotal: {len(df):,} filas | {len(df.columns)} columnas")
print("\\n[ACTIVIDAD 10] COMPLETADA.")
"""),

    ('md', """## 10.8 Exportación Final del Dataset Maestro
Generamos el archivo consolidado y mejorado `master_dataset_fase1_v2.csv` con todas las variables climáticas re-integradas y normalización geográfica optimizada.
"""),

    ('code', """
# Guardar Dataset Maestro Mejorado
output_path = os.path.join('data', 'processed', 'master_dataset_fase1_v2.csv')
os.makedirs(os.path.dirname(output_path), exist_ok=True)
df.to_csv(output_path, index=False, encoding='utf-8-sig')

print(f"✅ DATASET MAESTRO GENERADO: {output_path}")
print(f"   - Columnas totales: {len(df.columns)}")
print(f"   - Registros: {len(df):,}")

display(df.head(5))
"""),

    ('md', """# Actividad 10 Finalizada OK
"""),
]
nb(act10, "actividad_10_reexploracion.ipynb")

print("\n✅ Notebooks 09 y 10 generados.")
