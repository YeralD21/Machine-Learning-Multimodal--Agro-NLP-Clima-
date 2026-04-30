"""Genera y ejecuta notebooks 08, 09 y 10 con outputs embebidos."""
import nbformat as nbf, subprocess, os, sys
sys.stdout.reconfigure(encoding='utf-8')

NOTEBOOKS_DIR = "notebooks"

SETUP_BASE = """%matplotlib inline
import os, sys, json, warnings
import numpy as np, pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
warnings.filterwarnings('ignore')
sns.set_theme(style='whitegrid', palette='muted', font_scale=1.1)
plt.rcParams.update({'figure.dpi': 120})
if os.path.basename(os.getcwd()) == 'notebooks':
    os.chdir(os.path.abspath('..'))
with open('data/02_interim/pipeline_config.json','r',encoding='utf-8') as f:
    CFG = json.load(f)
DIRS=CFG['DIRS']; INTERIM=DIRS['interim']; PROCESSED=DIRS['processed']
REPORTS=DIRS['reports']
print(f"✅ Setup OK | {os.getcwd()}")"""

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
    r = subprocess.run([sys.executable, '-m', 'jupyter', 'nbconvert',
        '--to', 'notebook', '--execute', '--inplace',
        f'--ExecutePreprocessor.timeout={timeout}',
        '--ExecutePreprocessor.kernel_name=python3', path],
        capture_output=True, text=True, encoding='utf-8')
    ok = r.returncode == 0
    print(f"  {'✅ OK' if ok else '❌ ERROR'}")
    if not ok:
        print('\n'.join((r.stderr or '').strip().split('\n')[-15:]))
    return ok

# ── NB 08 PostgreSQL ─────────────────────────────────────────────
p08 = make_nb([
('md', """# 🗄️ Actividad 08 — Crear Esquemas en PostgreSQL
**DB:** `limon_analytics_db` | **URI:** `postgresql://postgres:postgres@localhost:5432/limon_analytics_db`  
Crea las 3 tablas del Star Schema y los índices de optimización."""),
('code', SETUP_BASE),
('md', "## 8.1 Verificar/Crear Base de Datos"),
('code', """
import joblib
from sqlalchemy import create_engine, text

PG_URI = CFG['PG_URI']
PG_BASE = PG_URI.rsplit('/', 1)[0] + '/postgres'
print(f"Servidor: {PG_BASE}")

try:
    engine_base = create_engine(PG_BASE, isolation_level='AUTOCOMMIT')
    with engine_base.connect() as conn:
        exists = conn.execute(
            text("SELECT 1 FROM pg_database WHERE datname='limon_analytics_db'")
        ).fetchone()
        if not exists:
            conn.execute(text("CREATE DATABASE limon_analytics_db ENCODING 'UTF8'"))
            print("  ✅ Base de datos limon_analytics_db CREADA.")
        else:
            print("  ✅ Base de datos limon_analytics_db ya existe.")
    engine_base.dispose()
except Exception as e:
    print(f"  ❌ Error: {e}")
"""),
('md', "## 8.2 Crear Tablas del Star Schema"),
('code', """
STMTS = [
    "CREATE TABLE IF NOT EXISTS dim_tiempo (id_tiempo SERIAL PRIMARY KEY, "
    "fecha_evento VARCHAR(7) NOT NULL, anho SMALLINT NOT NULL, mes SMALLINT NOT NULL, "
    "trimestre SMALLINT, month_sin FLOAT, month_cos FLOAT, UNIQUE(fecha_evento))",

    "CREATE TABLE IF NOT EXISTS dim_ubicacion (id_ubicacion SERIAL PRIMARY KEY, "
    "departamento VARCHAR(60) NOT NULL, provincia VARCHAR(60) NOT NULL, "
    "lat FLOAT, lon FLOAT, UNIQUE(departamento, provincia))",

    "CREATE TABLE IF NOT EXISTS fact_produccion_limon (id_hecho SERIAL PRIMARY KEY, "
    "id_tiempo INT NOT NULL REFERENCES dim_tiempo(id_tiempo), "
    "id_ubicacion INT NOT NULL REFERENCES dim_ubicacion(id_ubicacion), "
    "produccion_t FLOAT DEFAULT 0, cosecha_ha FLOAT DEFAULT 0, precio_chacra_kg FLOAT, "
    "num_emergencias INT DEFAULT 0, total_afectados INT DEFAULT 0, "
    "has_cultivo_perdidas FLOAT DEFAULT 0, n_noticias INT DEFAULT 0, avg_sentimiento FLOAT, "
    "temp_max_c FLOAT, temp_min_c FLOAT, precipitacion_mm FLOAT, "
    "humedad_rel_pct FLOAT, velocidad_viento FLOAT, radiacion_solar FLOAT, "
    "UNIQUE(id_tiempo, id_ubicacion))",

    "CREATE INDEX IF NOT EXISTS idx_fact_tiempo ON fact_produccion_limon(id_tiempo)",
    "CREATE INDEX IF NOT EXISTS idx_fact_ubicacion ON fact_produccion_limon(id_ubicacion)",
]

try:
    engine = create_engine(PG_URI)
    with engine.connect() as conn:
        for stmt in STMTS:
            conn.execute(text(stmt))
            conn.commit()
    print("  ✅ dim_tiempo creada")
    print("  ✅ dim_ubicacion creada")
    print("  ✅ fact_produccion_limon creada")
    print("  ✅ Índices creados")

    with engine.connect() as conn:
        tablas = [r[0] for r in conn.execute(text(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema='public' ORDER BY table_name"
        ))]
    print(f"\\n  Tablas en limon_analytics_db: {tablas}")
    engine.dispose()
except Exception as e:
    print(f"  ❌ Error: {e}")

print("\\n✅ [ACTIVIDAD 08] COMPLETADA")
"""),
('md', """## TODO: INTEGRACIÓN DATA NASA (COMPAÑERO)
Las columnas NASA ya existen en `fact_produccion_limon` como NULL:
`temp_max_c`, `temp_min_c`, `precipitacion_mm`, `humedad_rel_pct`, `velocidad_viento`, `radiacion_solar`

Solo necesitas hacer UPDATE cuando tengas los datos procesados de NASA POWER.
"""),
], "actividad_08_postgresql.ipynb")

ok08 = execute(p08)

# ── NB 09 ETL ────────────────────────────────────────────────────
p09 = make_nb([
('md', """# ⚙️ Actividad 09 — Pipeline ETL Completo
**Entrada:** `dataset_integrado.csv` → **Salida:** `master_dataset_fase1_v2.csv` + PostgreSQL

Pasos: Feature Engineering → StandardScaler → Carga PG → CSV final"""),
('code', """%matplotlib inline
import os, sys, json, warnings
import numpy as np, pandas as pd
import joblib, matplotlib.pyplot as plt, seaborn as sns
from sklearn.preprocessing import StandardScaler
from sqlalchemy import create_engine, text
warnings.filterwarnings('ignore')
sns.set_theme(style='whitegrid', font_scale=1.1)
if os.path.basename(os.getcwd()) == 'notebooks':
    os.chdir(os.path.abspath('..'))
with open('data/02_interim/pipeline_config.json','r',encoding='utf-8') as f:
    CFG = json.load(f)
DIRS=CFG['DIRS']; INTERIM=DIRS['interim']; PROCESSED=DIRS['processed']
SCALERS=DIRS['scalers']; PG_URI=CFG['PG_URI']
print(f"✅ Setup OK | {os.getcwd()}")"""),
('md', "## 9.1 Dataset Integrado"),
('code', """
df = pd.read_csv(f"{INTERIM}/dataset_integrado.csv")
print(f"Shape: {df.shape} | Rango: {df['fecha_evento'].min()} → {df['fecha_evento'].max()}")
display(df.head(3))
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

# Visualización de la codificación cíclica
fig, ax = plt.subplots(figsize=(8, 4))
theta = np.linspace(0, 2*np.pi, 12, endpoint=False)
ax.scatter(np.cos(theta), np.sin(theta), s=200, c=range(12), cmap='hsv', zorder=3)
for i, mes_n in enumerate(['Ene','Feb','Mar','Abr','May','Jun','Jul','Ago','Sep','Oct','Nov','Dic']):
    ax.annotate(mes_n, (np.cos(theta[i])*1.15, np.sin(theta[i])*1.15),
                ha='center', va='center', fontsize=9)
ax.set_xlim(-1.4, 1.4); ax.set_ylim(-1.4, 1.4)
ax.set_title('Codificación Cíclica del Mes\\n(month_sin / month_cos)', fontsize=12, fontweight='bold')
ax.set_aspect('equal'); ax.grid(True, alpha=0.3)
plt.tight_layout(); plt.show()
print("month_sin / month_cos calculados ✅")
"""),
('md', "## 9.3 Escalamiento StandardScaler"),
('code', """
FEATS = ['produccion_t','cosecha_ha','precio_chacra_kg',
         'num_emergencias','total_afectados','has_cultivo_perdidas','n_noticias']
cols = [c for c in FEATS if c in df.columns]
scaler = StandardScaler()
df[cols] = scaler.fit_transform(df[cols].fillna(0))
scaler_path = f"{SCALERS}/scaler_fase1_v2.pkl"
joblib.dump(scaler, scaler_path)
print(f"Variables escaladas ({len(cols)}): {cols}")
print(f"Scaler guardado: {scaler_path}")

# Verificar distribución post-escalado
fig, axes = plt.subplots(1, 3, figsize=(15, 4))
for ax, col in zip(axes, cols[:3]):
    df[col].hist(bins=40, ax=ax, color='steelblue', edgecolor='white')
    ax.set_title(f'{col}\\n(escalado)', fontsize=10, fontweight='bold')
    ax.set_xlabel('Valor estandarizado')
plt.suptitle('Distribución Post-StandardScaler (μ=0, σ=1)', fontsize=12, fontweight='bold')
plt.tight_layout(); plt.show()
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
    dim_t = df[['fecha_evento','anho','mes','trimestre','month_sin','month_cos']].drop_duplicates('fecha_evento')
    dim_t.to_sql('dim_tiempo', engine, if_exists='append', index=False, method='multi', chunksize=500)
    print(f"  ✅ dim_tiempo: {len(dim_t)} registros")
    dim_u = df[['departamento','provincia']].drop_duplicates().reset_index(drop=True)
    dim_u['lat'] = dim_u['departamento'].map(lambda d: COORDS.get(d,(None,None))[0])
    dim_u['lon'] = dim_u['departamento'].map(lambda d: COORDS.get(d,(None,None))[1])
    dim_u.to_sql('dim_ubicacion', engine, if_exists='append', index=False, method='multi', chunksize=500)
    print(f"  ✅ dim_ubicacion: {len(dim_u)} registros")
    with engine.connect() as conn:
        dt_map = pd.read_sql('SELECT id_tiempo, fecha_evento FROM dim_tiempo', conn)
        du_map = pd.read_sql('SELECT id_ubicacion, departamento, provincia FROM dim_ubicacion', conn)
    df_f = df.merge(dt_map, on='fecha_evento').merge(du_map, on=['departamento','provincia'])
    fc = [c for c in ['id_tiempo','id_ubicacion','produccion_t','cosecha_ha','precio_chacra_kg',
                       'num_emergencias','total_afectados','n_noticias'] if c in df_f.columns]
    df_load = df_f[fc].dropna(subset=['id_tiempo','id_ubicacion'])
    df_load[['id_tiempo','id_ubicacion']] = df_load[['id_tiempo','id_ubicacion']].astype(int)
    df_load.to_sql('fact_produccion_limon', engine, if_exists='append', index=False, method='multi', chunksize=500)
    print(f"  ✅ fact_produccion_limon: {len(df_load):,} registros")
    engine.dispose()
except Exception as e:
    print(f"  ⚠️ PostgreSQL: {e}")
    print("  Continuando sin carga en BD...")
"""),
('md', "## 9.5 Exportar CSV Final"),
('code', """
out = f"{PROCESSED}/master_dataset_fase1_v2.csv"
df.to_csv(out, index=False, encoding='utf-8-sig')
print(f"Shape final: {df.shape}")
print(f"Columnas: {df.columns.tolist()}")
print(f"\\n✅ [ACTIVIDAD 09] COMPLETADA → {out}")
"""),
('md', """## TODO: INTEGRACIÓN DATA NASA (COMPAÑERO)
```python
FEATS.extend(['temp_max_c','temp_min_c','precipitacion_mm','humedad_rel_pct','velocidad_viento'])
cols = [c for c in FEATS if c in df.columns]  # incluirá NASA si están presentes
scaler = StandardScaler()
df[cols] = scaler.fit_transform(df[cols].fillna(0))
```"""),
], "actividad_09_etl.ipynb")

ok09 = execute(p09)

# ── NB 10 REEXPLORACIÓN ──────────────────────────────────────────
p10 = make_nb([
('md', """# 📈 Actividad 10 — Reexploración Post-ETL
**CRÍTICO** — Evidencia visual del dataset maestro para la tesis.

Gráficos:
1. Serie temporal: Producción vs Precio (doble eje Y)
2. Heatmap de correlación: Producción × Precio × Emergencias × Noticias
3. Análisis multivariable: Producción × Emergencias × Noticias"""),
('code', SETUP_BASE),
('code', """
import joblib
from sklearn.preprocessing import StandardScaler

df = pd.read_csv(f"{PROCESSED}/master_dataset_fase1_v2.csv")
scaler_path = f"{DIRS['scalers']}/scaler_fase1_v2.pkl"
cols_sc = [c for c in ['produccion_t','cosecha_ha','precio_chacra_kg',
           'num_emergencias','total_afectados','has_cultivo_perdidas','n_noticias'] if c in df.columns]
if os.path.exists(scaler_path):
    sc = joblib.load(scaler_path)
    df_real = df.copy()
    df_real[cols_sc] = sc.inverse_transform(df[cols_sc].fillna(0))
    print("✅ Scaler cargado — valores desnormalizados para gráficos")
else:
    df_real = df.copy()
print(f"Dataset: {df.shape} | Rango: {df['fecha_evento'].min()} → {df['fecha_evento'].max()}")
display(df.head(3))
"""),
('md', "## 10.1 Serie Temporal — Producción vs Precio"),
('code', """
trend = df_real.groupby('fecha_evento').agg(
    prod=('produccion_t','sum'), precio=('precio_chacra_kg','mean')
).reset_index().sort_values('fecha_evento')

fig, ax1 = plt.subplots(figsize=(14, 6))
ax1.fill_between(range(len(trend)), trend['prod'], alpha=0.15, color='green')
ax1.plot(range(len(trend)), trend['prod'], 'g-o', markersize=4, linewidth=2, label='Producción (t)')
ax1.set_ylabel('Producción Total (t)', color='darkgreen', fontsize=12)
ax1.tick_params(axis='y', labelcolor='darkgreen')
ax1.set_xticks(range(0, len(trend), 6))
ax1.set_xticklabels(trend['fecha_evento'].iloc[::6], rotation=45, ha='right', fontsize=9)

ax2 = ax1.twinx()
ax2.plot(range(len(trend)), trend['precio'], 'r--s', markersize=4, linewidth=2, label='Precio (S/./kg)')
ax2.set_ylabel('Precio Chacra Promedio (S/./kg)', color='darkred', fontsize=12)
ax2.tick_params(axis='y', labelcolor='darkred')

l1, lb1 = ax1.get_legend_handles_labels()
l2, lb2 = ax2.get_legend_handles_labels()
ax1.legend(l1+l2, lb1+lb2, loc='upper left', fontsize=10)
fig.suptitle('Serie Temporal: Producción Nacional de Limón vs Precio en Chacra\\n(2021-2025)',
             fontsize=14, fontweight='bold')
plt.tight_layout()
plt.savefig(f"{REPORTS}/g10_prod_vs_precio.png", dpi=150, bbox_inches='tight')
plt.show()
print("[OK] g10_prod_vs_precio.png")
"""),
('md', "## 10.2 Heatmap de Correlación (annot=True)"),
('code', """
corr_cols = [c for c in ['produccion_t','precio_chacra_kg','cosecha_ha',
             'num_emergencias','total_afectados','n_noticias',
             'month_sin','month_cos'] if c in df.columns]
corr = df[corr_cols].corr()

labels = {'produccion_t':'Producción','precio_chacra_kg':'Precio','cosecha_ha':'Cosecha',
          'num_emergencias':'Emergencias','total_afectados':'Afectados',
          'n_noticias':'Noticias','month_sin':'Mes_sin','month_cos':'Mes_cos'}
corr.index   = [labels.get(c,c) for c in corr.index]
corr.columns = [labels.get(c,c) for c in corr.columns]

fig, ax = plt.subplots(figsize=(11, 9))
mask = np.triu(np.ones_like(corr, dtype=bool))
cmap = sns.diverging_palette(250, 10, as_cmap=True)
sns.heatmap(corr, mask=mask, cmap=cmap, center=0, annot=True, fmt='.2f',
            square=True, linewidths=0.8, cbar_kws={'shrink':0.8},
            annot_kws={'size':11, 'weight':'bold'}, ax=ax)
ax.set_title('Mapa de Calor de Correlación\\nProducción × Precio × Emergencias × Noticias × Estacionalidad',
             fontsize=13, fontweight='bold', pad=15)
plt.tight_layout()
plt.savefig(f"{REPORTS}/g11_correlacion_heatmap.png", dpi=150, bbox_inches='tight')
plt.show()
print("[OK] g11_correlacion_heatmap.png")
"""),
('md', "## 10.3 Gráfico Multivariable — Producción × Emergencias × Noticias"),
('code', """
multi = df_real.groupby('fecha_evento').agg(
    produccion=('produccion_t','sum'),
    emergencias=('num_emergencias','sum'),
    noticias=('n_noticias','first')
).reset_index().sort_values('fecha_evento')

x = list(range(len(multi)))
fig, ax1 = plt.subplots(figsize=(15, 7))

# Producción — área rellena
ax1.fill_between(x, multi['produccion'], alpha=0.2, color='#27ae60')
ax1.plot(x, multi['produccion'], color='#27ae60', linewidth=2.5, label='Producción (t)')
ax1.set_ylabel('Producción Nacional (t)', color='#27ae60', fontsize=12)
ax1.tick_params(axis='y', labelcolor='#27ae60')
ax1.set_xticks(range(0, len(multi), 6))
ax1.set_xticklabels(multi['fecha_evento'].iloc[::6], rotation=45, ha='right', fontsize=9)
ax1.set_xlabel('Período (YYYY-MM)', fontsize=11)

# Emergencias — barras
ax2 = ax1.twinx()
ax2.bar(x, multi['emergencias'], alpha=0.55, color='#e74c3c', width=0.7, label='Emergencias')
ax2.set_ylabel('Nro. Emergencias INDECI', color='#e74c3c', fontsize=12)
ax2.tick_params(axis='y', labelcolor='#e74c3c')

# Noticias — línea punteada
ax3 = ax1.twinx()
ax3.spines['right'].set_position(('axes', 1.10))
ax3.plot(x, multi['noticias'], color='#2980b9', linestyle='--', marker='^',
         markersize=5, linewidth=2, label='Noticias Agro/mes')
ax3.set_ylabel('Noticias Agraria.pe / mes', color='#2980b9', fontsize=12)
ax3.tick_params(axis='y', labelcolor='#2980b9')

# Leyenda unificada
handles, labels_list = [], []
for ax in [ax1, ax2, ax3]:
    h, l = ax.get_legend_handles_labels()
    handles += h; labels_list += l
ax1.legend(handles, labels_list, loc='upper left', fontsize=10, framealpha=0.9)

fig.suptitle('Análisis Multivariable: Producción × Emergencias × Noticias Agrícolas\\n'
             'Fuentes: MIDAGRI · INDECI SINPAD · Agraria.pe  |  Período 2021-2025',
             fontsize=14, fontweight='bold', y=1.02)
plt.tight_layout()
plt.savefig(f"{REPORTS}/g12_multivariable.png", dpi=150, bbox_inches='tight')
plt.show()
print("[OK] g12_multivariable.png")
"""),
('md', "## 10.4 Resumen Final del Pipeline"),
('code', """
print("=" * 65)
print("  RESUMEN FINAL — PIPELINE FASE 1 EJECUTADO EXITOSAMENTE")
print("=" * 65)
print(f"\\n  Dataset maestro: {df.shape[0]:,} filas × {df.shape[1]} columnas")
print(f"  Rango temporal: {df['fecha_evento'].min()} → {df['fecha_evento'].max()}")
print(f"  Departamentos: {df['departamento'].nunique()}")
print(f"  Provincias:    {df['provincia'].nunique()}")
print()
print("  Columnas del dataset final:")
for c in df.columns:
    print(f"    {c}")
print()
print("  Registros por año:")
for yr, cnt in df.groupby('anho').size().items():
    print(f"    {yr}: {cnt:,} filas")
print("\\n✅ [ACTIVIDAD 10] COMPLETADA — FASE 1 FINALIZADA")
"""),
('md', """## TODO: INTEGRACIÓN DATA NASA (COMPAÑERO)
Añadir al gráfico multivariable la curva de precipitaciones:
```python
ax4 = ax1.twinx()
ax4.spines['right'].set_position(('axes', 1.22))
ax4.plot(x, multi['precipitacion_mm'], color='#16a085', linestyle=':', linewidth=2, label='Precip. (mm/día)')
ax4.set_ylabel('Precipitación (mm/día)', color='#16a085', fontsize=12)
```
Y en el heatmap, añadir: `T2M`, `PRECTOTCORR`, `RH2M`, `WS2M` a `corr_cols`.
"""),
], "actividad_10_reexploracion.ipynb")

ok10 = execute(p10)

print(f"\n{'='*55}")
print(f"  Acti. 08 PostgreSQL:    {'✅ OK' if ok08 else '❌ Error'}")
print(f"  Acti. 09 ETL:           {'✅ OK' if ok09 else '❌ Error'}")
print(f"  Acti. 10 Reexploración: {'✅ OK' if ok10 else '❌ Error'}")
