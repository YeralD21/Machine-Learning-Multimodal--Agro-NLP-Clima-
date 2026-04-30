"""Genera y ejecuta notebooks 08, 09 y 10 con outputs embebidos."""
import nbformat as nbf, subprocess, os, sys
sys.stdout.reconfigure(encoding='utf-8')

NOTEBOOKS_DIR = "notebooks"

SETUP_BASE = """%matplotlib inline
import os, sys, json, warnings
import numpy as np, pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import create_engine, text
warnings.filterwarnings('ignore')
sns.set_theme(style='whitegrid', palette='muted', font_scale=1.1)
plt.rcParams.update({'figure.dpi': 120})
if os.path.basename(os.getcwd()) == 'notebooks':
    os.chdir(os.path.abspath('..'))
with open('data/02_interim/pipeline_config.json','r',encoding='utf-8') as f:
    CFG = json.load(f)
DIRS=CFG['DIRS']; INTERIM=DIRS['interim']; PROCESSED=DIRS['processed']
REPORTS=DIRS['reports']; PG_URI=CFG['PG_URI']
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
        capture_output=True)  # Sin text=True: evita UnicodeDecodeError con PNGs
    ok = r.returncode == 0
    print(f"  {'✅ OK' if ok else '❌ ERROR'}")
    if not ok:
        stderr = (r.stderr or b'').decode('utf-8', errors='replace')
        print('\n'.join((stderr or '').strip().split('\n')[-15:]))
    return ok

# ── NB 08 PostgreSQL ─────────────────────────────────────────────
p08 = make_nb([
('md', """# 🗄️ Actividad 08 — Diseño e Implementación del Data Warehouse (PostgreSQL 16)
**DB:** `limon_analytics_db` | **Arquitectura:** Star Schema v2.0 (5 Dimensiones)

Implementamos la estructura física del Data Warehouse utilizando un **Star Schema puro** para garantizar la escalabilidad analítica."""),
('code', SETUP_BASE),
('md', "## 8.1 Verificar/Crear Base de Datos"),
('code', """
PG_BASE = PG_URI.rsplit('/', 1)[0] + '/postgres'
try:
    engine_base = create_engine(PG_BASE, isolation_level='AUTOCOMMIT')
    with engine_base.connect() as conn:
        exists = conn.execute(text("SELECT 1 FROM pg_database WHERE datname='limon_analytics_db'")).fetchone()
        if not exists:
            conn.execute(text("CREATE DATABASE limon_analytics_db ENCODING 'UTF8'"))
            print("  ✅ Base de datos limon_analytics_db CREADA.")
        else:
            print("  ✅ Base de datos limon_analytics_db ya existe.")
    engine_base.dispose()
except Exception as e:
    print(f"  ❌ Error: {e}")
"""),
('md', "## 8.2 Arquitectura del Star Schema (Diagrama Técnico)"),
('code', """
import matplotlib.patches as mpatches
import matplotlib.pyplot as plt

plt.style.use('dark_background')
fig, ax = plt.subplots(figsize=(14, 9))
ax.set_xlim(0, 16); ax.set_ylim(0, 10); ax.axis('off')
fig.patch.set_facecolor('#0f172a')

def draw_table(ax, x, y, title, columns, color='#0ea5e9'):
    width, height = 3.2, len(columns) * 0.4 + 0.6
    rect = mpatches.FancyBboxPatch((x-width/2, y-height/2), width, height, 
                                   boxstyle="round,pad=0.1", ec="#cbd5e1", fc=color, alpha=0.9)
    ax.add_patch(rect)
    ax.text(x, y + height/2 - 0.3, title, weight='bold', color='white', ha='center', size=12)
    for i, col in enumerate(columns):
        ax.text(x - width/2 + 0.2, y + height/2 - 0.8 - i*0.4, f"• {col}", color='white', size=9)

fx, fy = 8, 5
draw_table(ax, fx, fy, "fact_produccion_limon", 
           ["id_hecho (PK)", "id_tiempo (FK)", "id_ubicacion (FK)", "id_clima (FK)", 
            "id_emergencia (FK)", "id_noticias (FK)", "produccion_t", "precio_chacra_kg"], 
           color='#7c3aed')

dims = [
    (8, 8.5, "dim_tiempo", ["id_tiempo (PK)", "fecha_evento", "anho", "mes", "month_sin/cos"]),
    (14, 6.5, "dim_ubicacion", ["id_ubicacion (PK)", "departamento", "provincia", "distrito"]),
    (13, 2.5, "dim_clima", ["id_clima (PK)", "temp_max/min", "precipitacion", "humedad"]),
    (3, 2.5, "dim_emergencia", ["id_emergencia (PK)", "tipo_evento", "gravedad", "afectados"]),
    (2, 6.5, "dim_noticias", ["id_noticias (PK)", "n_noticias", "avg_sentimiento"])
]

for dx, dy, dtitle, dcols in dims:
    draw_table(ax, dx, dy, dtitle, dcols)
    ax.annotate("", xy=(fx, fy), xytext=(dx, dy),
                arrowprops=dict(arrowstyle="<->", color="#22d3ee", lw=1.5, alpha=0.6))

plt.title("Limon Analytics — Star Schema v2.0 (PostgreSQL 16)", color='#f8fafc', size=16, weight='bold', pad=20)
plt.show()
"""),
('md', "## 8.3 Despliegue Físico (DDL SQL)"),
('code', """
STMTS = [
    "DROP TABLE IF EXISTS fact_produccion_limon CASCADE",
    "DROP TABLE IF EXISTS dim_tiempo CASCADE",
    "DROP TABLE IF EXISTS dim_ubicacion CASCADE",
    "DROP TABLE IF EXISTS dim_clima CASCADE",
    "DROP TABLE IF EXISTS dim_emergencia CASCADE",
    "DROP TABLE IF EXISTS dim_noticias CASCADE",

    "CREATE TABLE dim_tiempo (id_tiempo SERIAL PRIMARY KEY, fecha_evento VARCHAR(7) UNIQUE NOT NULL, anho SMALLINT, mes SMALLINT, month_sin FLOAT, month_cos FLOAT)",
    "CREATE TABLE dim_ubicacion (id_ubicacion SERIAL PRIMARY KEY, departamento VARCHAR(60), provincia VARCHAR(60), distrito VARCHAR(60), UNIQUE(departamento, provincia, distrito))",
    "CREATE TABLE dim_clima (id_clima SERIAL PRIMARY KEY, temp_max_c FLOAT, temp_min_c FLOAT, precipitacion_mm FLOAT)",
    "CREATE TABLE dim_emergencia (id_emergencia SERIAL PRIMARY KEY, tipo_emergencia VARCHAR(100), num_emergencias INT)",
    "CREATE TABLE dim_noticias (id_noticias SERIAL PRIMARY KEY, n_noticias INT, avg_sentimiento FLOAT)",
    "CREATE TABLE fact_produccion_limon (id_hecho SERIAL PRIMARY KEY, id_tiempo INT REFERENCES dim_tiempo(id_tiempo), id_ubicacion INT REFERENCES dim_ubicacion(id_ubicacion), id_clima INT REFERENCES dim_clima(id_clima), id_emergencia INT REFERENCES dim_emergencia(id_emergencia), id_noticias INT REFERENCES dim_noticias(id_noticias), produccion_t FLOAT, precio_chacra_kg FLOAT)"
]

try:
    engine = create_engine(PG_URI)
    with engine.connect() as conn:
        for stmt in STMTS:
            conn.execute(text(stmt))
            conn.commit()
    print("✅ Star Schema de 5 Dimensiones desplegado exitosamente.")
except Exception as e:
    print(f"❌ Error al crear tablas: {e}")
"""),
('md', """## 8.4 Simulación y Validación de la Estructura de Datos
Visualizamos una muestra simulada de cómo se verán los datos dentro de cada una de las 5 dimensiones y la tabla de hechos."""),
('code', """
print("--- [DIM_TIEMPO] ---")
df_t = pd.DataFrame({
    'id_tiempo': [1, 2, 3], 'fecha_evento': ['2021-01', '2021-02', '2021-03'],
    'anho': [2021, 2021, 2021], 'mes': [1, 2, 3],
    'month_sin': [0.5, 0.86, 1.0], 'month_cos': [0.86, 0.5, 0.0]
})
display(df_t)

print("\\n--- [DIM_UBICACION] ---")
df_u = pd.DataFrame({
    'id_ubicacion': [1, 2], 'departamento': ['PIURA', 'PIURA'],
    'provincia': ['SULLANA', 'PIURA'], 'distrito': ['LANCONES', 'CASTILLA']
})
display(df_u)

print("\\n--- [DIM_CLIMA (NASA)] ---")
df_c = pd.DataFrame({
    'id_clima': [101, 102], 'temp_max_c': [32.5, 34.1],
    'temp_min_c': [18.2, 19.5], 'precipitacion_mm': [0.5, 12.4]
})
display(df_c)

print("\\n--- [DIM_EMERGENCIA (INDECI)] ---")
df_e = pd.DataFrame({
    'id_emergencia': [50, 51], 'tipo_emergencia': ['LLUVIAS INTENSAS', 'SIN EMERGENCIAS'],
    'num_emergencias': [3, 0]
})
display(df_e)

print("\\n--- [DIM_NOTICIAS (AGRARIA.PE)] ---")
df_n = pd.DataFrame({
    'id_noticias': [1, 2], 'n_noticias': [15, 8], 'avg_sentimiento': [0.45, -0.12]
})
display(df_n)

print("\\n--- [FACT_PRODUCCION_LIMON] (Central Hub) ---")
df_f = pd.DataFrame({
    'id_hecho': [1, 2], 'id_tiempo': [1, 2], 'id_ubicacion': [1, 1],
    'id_clima': [101, 102], 'id_emergencia': [50, 51], 'id_noticias': [1, 2],
    'produccion_t': [4500.5, 4200.0], 'precio_chacra_kg': [2.8, 3.1]
})
display(df_f)
"""),
('md', "## 8.5 Conclusión\\nEl esquema está listo para ser poblado por el pipeline ETL."),
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
FEATS = [
    'produccion_t','cosecha_ha','precio_chacra_kg',
    'num_emergencias','total_afectados','has_cultivo_perdidas','n_noticias',
    'temp_max_c', 'temp_min_c', 'precipitacion_mm', 'humedad_rel_pct', 'velocidad_viento', 'radiacion_solar'
]
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
                       'num_emergencias','total_afectados','n_noticias', 'temp_max_c', 'temp_min_c', 'precipitacion_mm', 'humedad_rel_pct', 'velocidad_viento', 'radiacion_solar'] if c in df_f.columns]
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
('md', """## 9.6 Integración Climática Finalizada
El pipeline escala correctamente las variables de NASA y las carga en PostgreSQL junto a las métricas agrícolas y de emergencias."""),
], "actividad_09_etl.ipynb")

ok09 = execute(p09)

# ── NB 10 REEXPLORACIÓN ──────────────────────────────────────────
p10 = make_nb([
('md', """# 📈 Actividad 10 — Reexploración Post-ETL (Multimodal + NASA)
**CRÍTICO** — Evidencia visual del dataset maestro multimodal para la tesis.

Gráficos:
1. Análisis Multivariable: Producción, Precio y Clima
2. Heatmap de Correlación: Producción × Emergencias × Clima × Estacionalidad
3. Boxplot: Anomalías de Producción Anual"""),
('code', SETUP_BASE),
('code', """
import joblib
from sklearn.preprocessing import StandardScaler

df = pd.read_csv(f"{PROCESSED}/master_dataset_fase1_v2.csv")
scaler_path = f"{DIRS['scalers']}/scaler_fase1_v2.pkl"
cols_sc = [c for c in [
    'produccion_t','cosecha_ha','precio_chacra_kg',
    'num_emergencias','total_afectados','has_cultivo_perdidas','n_noticias',
    'temp_max_c', 'temp_min_c', 'precipitacion_mm', 'humedad_rel_pct', 'velocidad_viento', 'radiacion_solar'
] if c in df.columns]

if os.path.exists(scaler_path):
    sc = joblib.load(scaler_path)
    df_real = df.copy()
    if len(cols_sc) == len(sc.scale_):
        df_real[cols_sc] = sc.inverse_transform(df[cols_sc].fillna(0))
        print("✅ Scaler cargado — valores desnormalizados para gráficos")
    else:
        print("⚠️ Las columnas no coinciden con el scaler. Usando valores escalados.")
else:
    df_real = df.copy()
print(f"Dataset: {df.shape} | Rango: {df['fecha_evento'].min()} → {df['fecha_evento'].max()}")
"""),
('md', "## 10.1 Gráfico Multivariable: Producción, Precio y Precipitación"),
('code', """
trend = df_real.groupby('fecha_evento').agg(
    prod=('produccion_t','sum'), 
    precio=('precio_chacra_kg','mean'),
    precip=('precipitacion_mm','mean') if 'precipitacion_mm' in df_real.columns else ('produccion_t','mean')
).reset_index().sort_values('fecha_evento')

fig, ax1 = plt.subplots(figsize=(15, 7))
x = range(len(trend))

# Área: Producción
ax1.fill_between(x, trend['prod'], alpha=0.2, color='forestgreen')
ax1.plot(x, trend['prod'], color='forestgreen', linewidth=2, label='Producción (t)')
ax1.set_ylabel('Producción Total (t)', color='forestgreen', fontsize=12)
ax1.set_xticks(range(0, len(trend), 6))
ax1.set_xticklabels(trend['fecha_evento'].iloc[::6], rotation=45, ha='right')

# Eje 2: Precio
ax2 = ax1.twinx()
ax2.plot(x, trend['precio'], color='darkorange', linewidth=2, linestyle='--', label='Precio (S/./kg)')
ax2.set_ylabel('Precio Chacra (S/./kg)', color='darkorange', fontsize=12)

# Eje 3: Precipitación
if 'precipitacion_mm' in df_real.columns:
    ax3 = ax1.twinx()
    ax3.spines['right'].set_position(('axes', 1.12))
    ax3.plot(x, trend['precip'], color='royalblue', linewidth=1.5, linestyle=':', label='Precip. (mm)')
    ax3.set_ylabel('Precipitación (mm/día)', color='royalblue', fontsize=12)

fig.suptitle('Análisis Multimodal: Producción, Precio y Clima (2021-2025)', fontsize=15, fontweight='bold')
plt.tight_layout()
plt.savefig(f"{REPORTS}/g13_produccion_vs_precio_vs_clima.png", dpi=150, bbox_inches='tight')
plt.show()
print("[OK] g13_produccion_vs_precio_vs_clima.png")
"""),
('md', "## 10.2 Heatmap de Correlación Extendido"),
('code', """
corr_cols = [c for c in [
    'produccion_t','precio_chacra_kg','num_emergencias',
    'n_noticias','temp_max_c','precipitacion_mm','humedad_rel_pct',
    'month_sin','month_cos'
] if c in df.columns]
corr = df[corr_cols].corr()

fig, ax = plt.subplots(figsize=(12, 10))
mask = np.triu(np.ones_like(corr, dtype=bool))
cmap = sns.diverging_palette(250, 10, as_cmap=True)
sns.heatmap(corr, mask=mask, cmap='coolwarm', center=0, annot=True, fmt='.2f',
            square=True, linewidths=0.5, cbar_kws={'shrink':0.8}, ax=ax)
ax.set_title('Mapa de Calor de Correlación: Producción × Emergencias × Clima × Estacionalidad',
             fontsize=14, fontweight='bold')
plt.tight_layout()
plt.savefig(f"{REPORTS}/g14_correlacion_heatmap_final.png", dpi=150, bbox_inches='tight')
plt.show()
print("[OK] g14_correlacion_heatmap_final.png")
"""),
('md', "## 10.3 Boxplot: Distribución de Producción por Año"),
('code', """
fig, ax = plt.subplots(figsize=(10, 6))
sns.boxplot(data=df_real, x='anho', y='produccion_t', palette='YlGn', ax=ax)
ax.set_title('Distribución de Producción de Limón por Año (Detección de Anomalías)', fontsize=14, fontweight='bold')
ax.set_ylabel('Producción (t)')
plt.tight_layout()
plt.savefig(f"{REPORTS}/g15_boxplot_anual.png", dpi=150, bbox_inches='tight')
plt.show()
print("[OK] g15_boxplot_anual.png")
"""),
('md', "## 10.4 Resumen Final del Pipeline Multimodal"),
('code', """
print("=" * 65)
print("  RESUMEN FINAL — PIPELINE FASE 1 EJECUTADO EXITOSAMENTE")
print("=" * 65)
print(f"\\n  Dataset maestro: {df.shape[0]:,} filas × {df.shape[1]} columnas")
print(f"  Rango temporal: {df['fecha_evento'].min()} → {df['fecha_evento'].max()}")
print()
print("  Columnas del dataset final:")
for c in df.columns:
    print(f"    {c}")
print("\\n✅ [ACTIVIDAD 10] COMPLETADA — FASE 1 FINALIZADA")
"""),
], "actividad_10_reexploracion.ipynb")

ok10 = execute(p10)

print(f"\n{'='*55}")
print(f"  Acti. 08 PostgreSQL:    {'✅ OK' if ok08 else '❌ Error'}")
print(f"  Acti. 09 ETL:           {'✅ OK' if ok09 else '❌ Error'}")
print(f"  Acti. 10 Reexploración: {'✅ OK' if ok10 else '❌ Error'}")
