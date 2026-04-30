"""Regenera y ejecuta notebooks 05, 06, 07."""
import nbformat as nbf, subprocess, os, sys
sys.stdout.reconfigure(encoding='utf-8')

NOTEBOOKS_DIR = "notebooks"
SETUP = """%matplotlib inline
import os, sys, json, re, warnings, unicodedata
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
DIRS=CFG['DIRS']; INTERIM=DIRS['interim']; REPORTS=DIRS['reports']; PROCESSED=DIRS['processed']
print(f"✅ Setup OK | {os.getcwd()}")"""

def make_nb(cells, filename):
    n = nbf.v4.new_notebook()
    n.metadata['kernelspec'] = {'display_name':'Python 3','language':'python','name':'python3'}
    for t,s in cells:
        n.cells.append(nbf.v4.new_markdown_cell(s) if t=='md' else nbf.v4.new_code_cell(s))
    path = os.path.join(NOTEBOOKS_DIR, filename)
    with open(path,'w',encoding='utf-8') as f: nbf.write(n,f)
    return path

def execute(path, timeout=600):
    print(f"\n⏳ Ejecutando: {path}")
    r = subprocess.run([sys.executable,'-m','jupyter','nbconvert','--to','notebook',
        '--execute','--inplace',f'--ExecutePreprocessor.timeout={timeout}',
        '--ExecutePreprocessor.kernel_name=python3', path],
        capture_output=True)  # Sin text=True: evita UnicodeDecodeError con PNGs
    ok = r.returncode == 0
    print(f"  {'✅ OK' if ok else '❌ ERROR'}")
    if not ok:
        stderr = (r.stderr or b'').decode('utf-8', errors='replace')
        print('\n'.join(stderr.strip().split('\n')[-15:]))
    return ok

# ── NB 05 LIMPIEZA ───────────────────────────────────────────────
p05 = make_nb([
('md',"# 🧹 Actividad 05 — Limpieza y Estandarización\nGeo: MAYÚSCULAS SIN TILDES | NLP: Regex HTML/URL"),
('code', SETUP),
('md',"## 5.1 Funciones de limpieza"),
('code',"""
def norm_geo(t):
    if not isinstance(t,str): return t
    t=t.strip().upper()
    for a,b in [('Á','A'),('É','E'),('Í','I'),('Ó','O'),('Ú','U')]: t=t.replace(a,b)
    return ''.join(c for c in unicodedata.normalize('NFD',t) if unicodedata.category(c)!='Mn')
def clean_nlp(t):
    if not isinstance(t,str): return ''
    t=re.sub(r'<[^>]+>',' ',t); t=re.sub(r'https?://\\S+|www\\.\\S+',' ',t)
    t=re.sub(r'[^\\w\\s\\.,;:\\-\\(\\)¿\\?¡\\!áéíóúÁÉÍÓÚñÑ]',' ',t)
    return re.sub(r'\\s+',' ',t).strip()
print("Funciones definidas: norm_geo() y clean_nlp()")
"""),
('md',"## 5.2 MIDAGRI — Antes vs Después"),
('code',"""
df_m = pd.read_csv(f"{INTERIM}/midagri_limon_raw.csv")
antes = df_m[['Dpto','Prov','dsc_Cultivo']].head(6).copy()
df_m = df_m.rename(columns={'anho':'anho','mes':'mes','COD_UBIGEO':'cod_ubigeo','Dpto':'departamento',
    'Prov':'provincia','Dist':'distrito','dsc_Cultivo':'cultivo','PRODUCCION(t)':'produccion_t',
    'COSECHA (ha)':'cosecha_ha','MTO_PRECCHAC (S/ x kg)':'precio_chacra_kg'})
for c in ['departamento','provincia','distrito','cultivo']: df_m[c]=df_m[c].apply(norm_geo)
df_m['fecha_evento']=df_m['anho'].astype(str)+'-'+df_m['mes'].astype(str).str.zfill(2)
df_m['produccion_t']=pd.to_numeric(df_m['produccion_t'],errors='coerce').fillna(0)
df_m['cosecha_ha']=pd.to_numeric(df_m['cosecha_ha'],errors='coerce').fillna(0)
df_m['precio_chacra_kg']=pd.to_numeric(df_m['precio_chacra_kg'],errors='coerce')
print("ANTES:"); display(antes)
print("DESPUÉS:"); display(df_m[['departamento','provincia','cultivo']].head(6))
out=f"{INTERIM}/midagri_limon_clean.csv"; df_m.to_csv(out,index=False,encoding='utf-8-sig')
print(f"[OK] {out} — {len(df_m):,} filas")
"""),
('md',"## 5.3 INDECI — Filtro Fenómenos"),
('code',"""
df_ev=pd.read_csv(f"{INTERIM}/indeci_eventos_dbf.csv",low_memory=False)
df_ev=df_ev.rename(columns={c:c for c in df_ev.columns})
if 'departamen' in df_ev.columns: df_ev=df_ev.rename(columns={'departamen':'departamento'})
for c in ['departamento','provincia','fenomeno']:
    if c in df_ev.columns: df_ev[c]=df_ev[c].apply(norm_geo)
df_ev['fecha']=pd.to_datetime(df_ev['fecha'],errors='coerce')
df_ev['fecha_evento']=df_ev['fecha'].dt.strftime('%Y-%m')
for s,d in [('safecta','personas_afectadas'),('sdamni','personas_damnificadas'),
            ('sareaculti','has_cultivo_afectadas'),('sareacul_1','has_cultivo_perdidas')]:
    if s in df_ev.columns: df_ev[d]=pd.to_numeric(df_ev[s],errors='coerce').fillna(0)
PELIGROS=[p.upper() for p in CFG['PELIGROS_VALIDOS']]
df_clean=df_ev[df_ev['fenomeno'].isin(PELIGROS)].copy()
print(f"Originales: {len(df_ev):,} → Filtrados: {len(df_clean):,} ({len(df_clean)/len(df_ev)*100:.1f}%)")
fig,ax=plt.subplots(figsize=(12,4))
df_clean['fenomeno'].value_counts().plot(kind='bar',ax=ax,color='steelblue',edgecolor='black')
ax.set_title('Fenómenos Hidrometeorológicos Válidos tras Filtro',fontsize=12,fontweight='bold')
ax.tick_params(axis='x',rotation=30); plt.tight_layout(); plt.show()
out=f"{INTERIM}/indeci_eventos_clean.csv"; df_clean.to_csv(out,index=False,encoding='utf-8-sig')
print(f"[OK] {out}")
"""),
('md',"## 5.4 AGRARIA.PE — Limpieza NLP"),
('code',"""
df_n=pd.read_csv(f"{INTERIM}/agraria_noticias_raw.csv")
df_n['titular_clean']=df_n['titular'].apply(clean_nlp)
df_n['cuerpo_clean']=df_n['cuerpo_completo'].apply(clean_nlp)
df_n['fecha']=pd.to_datetime(df_n['fecha'],errors='coerce')
df_n['fecha_evento']=df_n['fecha'].dt.strftime('%Y-%m')
orig=df_n['cuerpo_completo'].astype(str).str.len().mean()
clean=df_n['cuerpo_clean'].str.len().mean()
print(f"Longitud media original: {orig:,.0f} | limpia: {clean:,.0f} chars ({(1-clean/orig)*100:.1f}% reducción)")
# Muestra
idx=df_n['cuerpo_completo'].astype(str).str.len().idxmax()
print("\\nEjemplo Antes:", str(df_n.loc[idx,'cuerpo_completo'])[:150])
print("Ejemplo Después:", df_n.loc[idx,'cuerpo_clean'][:150])
out=f"{INTERIM}/agraria_noticias_clean.csv"; df_n.to_csv(out,index=False,encoding='utf-8-sig')
print(f"\\n[OK] {out} — {len(df_n)} noticias")
print("✅ [ACTIVIDAD 05] COMPLETADA")
"""),
('md',"""## 5.5 Estandarización Data Climática (NASA POWER)
Al igual que las fuentes locales, los datos de la NASA se estandarizan para asegurar que los nombres de Departamentos y Provincias coincidan exactamente (MAYÚSCULAS y sin tildes) y que el formato de fecha sea `YYYY-MM`."""),
('code',"""
nasa_raw_path = "data/02_interim_nasa/nasa_long_raw.csv"
if os.path.exists(nasa_raw_path):
    df_nasa = pd.read_csv(nasa_raw_path)
    # Estandarización Geo
    for c in ['DEPARTAMENTO', 'PROVINCIA']:
        df_nasa[c] = df_nasa[c].apply(norm_geo)
    
    # Estandarización Temporal
    if 'DATE' in df_nasa.columns:
        df_nasa['fecha_evento'] = pd.to_datetime(df_nasa['DATE']).dt.strftime('%Y-%m')
    
    print(f"Data NASA estandarizada: {len(df_nasa):,} registros")
    # Mostrar solo columnas disponibles para evitar KeyError
    cols_show = [c for c in ['DEPARTAMENTO', 'PROVINCIA', 'fecha_evento'] if c in df_nasa.columns]
    display(df_nasa[cols_show].head(3))
else:
    print("Nota: El archivo raw de NASA se procesa en el pipeline especializado 'main_nasa_pipeline.py'")
"""),
],"actividad_05_limpieza.ipynb")

ok05=execute(p05)

# ── NB 06 INTEGRACIÓN ────────────────────────────────────────────
p06=make_nb([
('md',"# 🔗 Actividad 06 — Integración Multimodal\nMerge Provincia/Mes: MIDAGRI + INDECI + Noticias"),
('code',SETUP),
('code',"""
df_m=pd.read_csv(f"{INTERIM}/midagri_limon_clean.csv")
df_ev=pd.read_csv(f"{INTERIM}/indeci_eventos_clean.csv",low_memory=False)
df_n=pd.read_csv(f"{INTERIM}/agraria_noticias_clean.csv")
print(f"MIDAGRI: {len(df_m):,} | INDECI: {len(df_ev):,} | Noticias: {len(df_n):,}")
"""),
('md',"## 6.1 Agregación MIDAGRI — Mensual × Provincia"),
('code',"""
midagri_agg=(df_m.groupby(['fecha_evento','departamento','provincia'])
    .agg(produccion_t=('produccion_t','sum'),cosecha_ha=('cosecha_ha','sum'),
         precio_chacra_kg=('precio_chacra_kg','mean')).reset_index())
print(f"MIDAGRI agregado: {len(midagri_agg):,} filas")
display(midagri_agg.head(4))
"""),
('md',"## 6.2 Agregación INDECI"),
('code',"""
pa='personas_afectadas' if 'personas_afectadas' in df_ev.columns else 'fenomeno'
hc='has_cultivo_perdidas' if 'has_cultivo_perdidas' in df_ev.columns else 'fenomeno'
indeci_agg=(df_ev.dropna(subset=['fecha_evento','departamento','provincia'])
    .groupby(['fecha_evento','departamento','provincia'])
    .agg(num_emergencias=('fenomeno','count'),total_afectados=(pa,'sum'),
         has_cultivo_perdidas=(hc,'sum')).reset_index())
print(f"INDECI agregado: {len(indeci_agg):,} filas")
display(indeci_agg.head(4))
"""),
('md',"## 6.3 Esqueleto Temporal + Left Joins"),
('code',"""
df_n['fecha_evento']=pd.to_datetime(df_n['fecha'],errors='coerce').dt.strftime('%Y-%m')
noticias_agg=df_n.groupby('fecha_evento').agg(n_noticias=('titular','count')).reset_index()
ANHO_I,ANHO_F=CFG['ANHO_INICIO'],CFG['ANHO_FIN']
fechas=pd.date_range(f'{ANHO_I}-01-01',f'{ANHO_F}-08-01',freq='MS')
provincias=midagri_agg[['departamento','provincia']].drop_duplicates()
skeleton=pd.DataFrame([(d.strftime('%Y-%m'),r['departamento'],r['provincia'])
    for d in fechas for _,r in provincias.iterrows()],
    columns=['fecha_evento','departamento','provincia'])
print(f"Esqueleto: {len(skeleton):,} ({len(fechas)} meses × {len(provincias)} provincias)")
df_int=skeleton.merge(midagri_agg,on=['fecha_evento','departamento','provincia'],how='left')
df_int=df_int.merge(indeci_agg,on=['fecha_evento','departamento','provincia'],how='left')
df_int=df_int.merge(noticias_agg,on='fecha_evento',how='left')
df_int['produccion_t']=df_int['produccion_t'].fillna(0)
df_int['num_emergencias']=df_int['num_emergencias'].fillna(0)
df_int['n_noticias']=df_int['n_noticias'].fillna(0)
df_int=df_int.sort_values(['departamento','provincia','fecha_evento'])
df_int['precio_chacra_kg']=df_int.groupby(['departamento','provincia'])['precio_chacra_kg'].ffill().bfill()
print(f"Dataset integrado: {len(df_int):,} filas | Duplicados: {df_int.duplicated(['fecha_evento','departamento','provincia']).sum()}")
"""),
('md',"## 6.4 Visualización de Cobertura"),
('code',"""
fig,axes=plt.subplots(1,2,figsize=(16,5))
cob=df_int.groupby('departamento').apply(lambda x:(x['produccion_t']>0).mean()*100).sort_values()
cob.plot(kind='barh',ax=axes[0],color='mediumseagreen',edgecolor='white')
axes[0].set_title('Cobertura Mensual con Datos MIDAGRI (%)',fontsize=11,fontweight='bold')
axes[0].set_xlabel('% Meses con producción > 0')
em=df_int.groupby('departamento')['num_emergencias'].sum().sort_values(ascending=False).head(10)
em.plot(kind='bar',ax=axes[1],color='steelblue',edgecolor='black')
axes[1].set_title('Top 10 Dpto por Emergencias Integradas',fontsize=11,fontweight='bold')
axes[1].tick_params(axis='x',rotation=40)
plt.tight_layout(); plt.show()
out=f"{INTERIM}/dataset_integrado.csv"; df_int.to_csv(out,index=False,encoding='utf-8-sig')
print(f"[OK] {out}")
print("✅ [ACTIVIDAD 06] COMPLETADA")
"""),
('md',"""## 6.5 Integración Data Climática (NASA POWER)
Se leen los datos climáticos procesados por el pipeline paralelo y se unen usando las mismas llaves geográficas y temporales."""),
('code',"""
nasa_path = f"{DIRS['processed']}_nasa/nasa_climatic_raw_values.csv"
if os.path.exists(nasa_path):
    df_nasa = pd.read_csv(nasa_path)
    cols_nasa = ['fecha_evento', 'DEPARTAMENTO', 'PROVINCIA', 
                 'T2M_MAX', 'T2M_MIN', 'PRECTOTCORR', 'RH2M', 'WS2M', 'ALLSKY_SFC_SW_DWN']
    df_nasa = df_nasa[cols_nasa].rename(columns={
        'DEPARTAMENTO': 'departamento', 'PROVINCIA': 'provincia',
        'T2M_MAX': 'temp_max_c', 'T2M_MIN': 'temp_min_c',
        'PRECTOTCORR': 'precipitacion_mm', 'RH2M': 'humedad_rel_pct',
        'WS2M': 'velocidad_viento', 'ALLSKY_SFC_SW_DWN': 'radiacion_solar'
    })
    for c in ['departamento', 'provincia']:
        df_nasa[c] = df_nasa[c].astype(str).str.upper().str.strip()

    df_int = pd.merge(df_int, df_nasa, on=['fecha_evento', 'departamento', 'provincia'], how='left')
    
    # Relleno de nulos climáticos por media provincial
    cols_clima = ['temp_max_c', 'temp_min_c', 'precipitacion_mm', 'humedad_rel_pct', 'velocidad_viento', 'radiacion_solar']
    for col in cols_clima:
        df_int[col] = df_int.groupby(['departamento', 'provincia'])[col].transform(lambda x: x.fillna(x.mean()))
        df_int[col] = df_int[col].fillna(df_int[col].mean())
    print(f"Variables NASA integradas exitosamente: {cols_clima}")
"""),
],"actividad_06_integracion.ipynb")

ok06=execute(p06)

# ── NB 07 STAR SCHEMA ────────────────────────────────────────────
p07=make_nb([
('md',"""# ⭐ Actividad 07 — Data Warehouse: Star Schema v2.0
---
## Modelo Dimensional Puro — `limon_analytics_db`

**Arquitectura Star Schema con 5 Dimensiones Satélite**

```
                         ┌─────────────────────┐
                         │     dim_tiempo       │
                         │ id_tiempo (PK)       │
                         │ fecha_evento, anho   │
                         │ month_sin, month_cos │
                         └──────────┬──────────┘
                                    │ FK
  ┌────────────────┐      ┌─────────▼──────────────┐      ┌────────────────────┐
  │  dim_ubicacion │      │  fact_produccion_limon ★│      │   dim_emergencia   │
  │ id_ubicacion(PK)◄────►│ id_hecho (PK)          │◄────►│ id_emergencia (PK) │
  │ departamento   │      │ id_tiempo (FK)          │      │ tipo_emergencia     │
  │ provincia      │      │ id_ubicacion (FK)       │      │ gravedad           │
  │ distrito       │      │ id_clima (FK)           │      └────────────────────┘
  │ lat, lon       │      │ id_emergencia (FK)      │
  └────────────────┘      │ id_noticias (FK)        │      ┌────────────────────┐
                          │ --- Métricas MIDAGRI ---│      │    dim_noticias     │
                          │ produccion_t FLOAT      │◄────►│ id_noticias (PK)   │
                          │ cosecha_ha FLOAT        │      │ avg_sentimiento    │
                          │ precio_chacra_kg FLOAT  │      │ n_noticias         │
                          └─────────┬──────────────┘      └────────────────────┘
                                    │ FK
                         ┌──────────▼──────────┐
                         │      dim_clima       │
                         │ id_clima (PK)        │
                         │ temp_max_c, temp_min │
                         │ precipitacion_mm     │
                         │ is_extreme_weather   │
                         └─────────────────────┘
```

| Aspecto | Decisión |
|:--------|:---------|
| **Tipo** | Star Schema Puro — 5 dimensiones, JOINs simples para OLAP |
| **Granularidad** | Mensual × Provincia × Distrito |
| **fact_produccion_limon** | Solo FKs + métricas de producción MIDAGRI |
| **dim_clima** | Encapsula toda la data NASA POWER |
| **dim_emergencia** | Encapsula toda la data INDECI SINPAD |
| **dim_noticias** | Encapsula sentimiento NLP + conteo Agraria.pe |
| **Sentimiento** | avg_sentimiento en dim_noticias (Fase 2: BETO) |
"""),
('code',SETUP),
('md',"## 7.1 Visualización del Diagrama con Matplotlib (Star Schema v2.0)"),
('code',"""
import matplotlib.patches as mpatches

fig, ax = plt.subplots(figsize=(16, 10))
ax.set_xlim(0, 16); ax.set_ylim(0, 10); ax.axis('off')
fig.patch.set_facecolor('#fdfdfd')

def draw_table(ax, x, y, w, h, title, rows, color_h='#2c3e50', color_b='#ffffff'):
    ax.add_patch(mpatches.FancyBboxPatch((x,y), w, h, boxstyle="round,pad=0.1",
        facecolor=color_b, edgecolor='#34495e', linewidth=1.5))
    ax.add_patch(mpatches.FancyBboxPatch((x,y+h-0.6), w, 0.6, boxstyle="round,pad=0.05",
        facecolor=color_h, edgecolor='none'))
    ax.text(x+w/2, y+h-0.3, title, ha='center', va='center', fontsize=10, fontweight='bold', color='white')
    for i, (row, clr) in enumerate(rows):
        color = '#e74c3c' if clr=='k' else '#3498db' if clr=='f' else '#2c3e50'
        ax.text(x+0.2, y+h-1.0-i*0.4, row, va='center', fontsize=8, color=color)

# --- TABLA CENTRAL: HECHOS ---
draw_table(ax, 6, 3, 4, 4, '★ fact_produccion_limon', [
    ('id_hecho (PK)', 'k'), ('id_tiempo (FK)', 'f'), ('id_ubicacion (FK)', 'f'),
    ('id_clima (FK)', 'f'), ('id_emergencia (FK)', 'f'), ('id_noticias (FK)', 'f'),
    ('produccion_t FLOAT', 'n'), ('cosecha_ha FLOAT', 'n'), ('precio_chacra_kg FLOAT', 'n')
], '#8e44ad', '#f5eef8')

# --- 5 DIMENSIONES EN CIRCULO ---
# Arriba: dim_tiempo
draw_table(ax, 6.5, 8, 3, 1.8, 'dim_tiempo', [
    ('id_tiempo (PK)', 'k'), ('fecha_evento, anho, mes', 'n'), ('month_sin, month_cos', 'n')
], '#16a085', '#e8f8f5')

# Izquierda: dim_ubicacion
draw_table(ax, 0.5, 6, 3.5, 2.2, 'dim_ubicacion', [
    ('id_ubicacion (PK)', 'k'), ('departamento, provincia', 'n'), ('distrito (NUEVO)', 'n'), ('lat, lon', 'n')
], '#2980b9', '#ebf5fb')

# Abajo-izquierda: dim_clima
draw_table(ax, 0.5, 1.5, 3.5, 2.2, 'dim_clima  (NASA)', [
    ('id_clima (PK)', 'k'), ('temp_max, temp_min', 'n'), ('precipitacion_mm', 'n'), ('is_extreme_weather', 'n')
], '#d35400', '#fef5e7')

# Derecha: dim_emergencia
draw_table(ax, 11.5, 6, 4, 2.2, 'dim_emergencia', [
    ('id_emergencia (PK)', 'k'), ('tipo_desastre', 'n'), ('gravedad_index', 'n'), ('impacto_agricola', 'n')
], '#c0392b', '#fdedec')

# Abajo-derecha: dim_noticias
draw_table(ax, 11.5, 1.5, 4, 2.2, 'dim_noticias', [
    ('id_noticias (PK)', 'k'), ('avg_sentimiento', 'n'), ('num_menciones', 'n'), ('top_topic', 'n')
], '#2c3e50', '#eaeded')

# --- FLECHAS FK ---
arrow_kw = dict(arrowstyle='->', color='#2980b9', lw=1.8,
                connectionstyle='arc3,rad=0.0')
# fact -> dim_tiempo
ax.annotate('', xy=(8, 8.0), xytext=(8, 7.0), arrowprops=arrow_kw)
ax.text(8.1, 7.5, 'FK', fontsize=7, color='#2980b9', fontweight='bold')
# fact -> dim_ubicacion
ax.annotate('', xy=(4.0, 7.2), xytext=(6.0, 6.5), arrowprops=arrow_kw)
ax.text(4.8, 7.0, 'FK', fontsize=7, color='#2980b9', fontweight='bold')
# fact -> dim_clima
ax.annotate('', xy=(4.0, 2.8), xytext=(6.0, 3.5), arrowprops=arrow_kw)
ax.text(4.8, 3.0, 'FK', fontsize=7, color='#2980b9', fontweight='bold')
# fact -> dim_emergencia
ax.annotate('', xy=(11.5, 7.2), xytext=(10.0, 6.5), arrowprops=arrow_kw)
ax.text(10.5, 7.0, 'FK', fontsize=7, color='#2980b9', fontweight='bold')
# fact -> dim_noticias
ax.annotate('', xy=(11.5, 2.8), xytext=(10.0, 3.5), arrowprops=arrow_kw)
ax.text(10.5, 3.0, 'FK', fontsize=7, color='#2980b9', fontweight='bold')

plt.title("Data Warehouse Star Schema v2.0 — Limon Analytics", fontsize=16, fontweight='bold', color='#2c3e50')
plt.tight_layout()
plt.savefig(f"{REPORTS}/g07_star_schema_v2.png", dpi=150, bbox_inches='tight')
plt.show()
print("Diagrama Star Schema v2.0 (5 dimensiones) generado")
"""),
('md',"## 7.2 DDL del Star Schema v2.0 (5 Dimensiones Puras)"),
('code',"""
DDL = '''
-- =============================================
-- STAR SCHEMA v2.0 — limon_analytics_db
-- Proyecto: Prediccion de Produccion de Limon
-- 5 Dimensiones Puras + 1 Tabla de Hechos
-- =============================================

-- 1. Dimensiones secundarias (sin FKs propias)
CREATE TABLE IF NOT EXISTS dim_clima (
    id_clima SERIAL PRIMARY KEY,
    temp_max_c FLOAT, temp_min_c FLOAT, precipitacion_mm FLOAT,
    humedad_rel_pct FLOAT, velocidad_viento FLOAT, radiacion_solar FLOAT,
    is_extreme_weather BOOLEAN DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS dim_emergencia (
    id_emergencia SERIAL PRIMARY KEY,
    tipo_emergencia VARCHAR(100),
    num_emergencias INT DEFAULT 0,
    total_afectados INT DEFAULT 0,
    has_cultivo_perdidas FLOAT DEFAULT 0,
    gravedad SMALLINT
);

CREATE TABLE IF NOT EXISTS dim_noticias (
    id_noticias SERIAL PRIMARY KEY,
    avg_sentimiento FLOAT,
    n_noticias INT DEFAULT 0,
    tema_principal VARCHAR(100)
);

-- 2. Dimensiones base
CREATE TABLE IF NOT EXISTS dim_tiempo (
    id_tiempo SERIAL PRIMARY KEY,
    fecha_evento VARCHAR(7) NOT NULL UNIQUE,
    anho SMALLINT NOT NULL, mes SMALLINT NOT NULL,
    trimestre SMALLINT,
    month_sin FLOAT, month_cos FLOAT
);

CREATE TABLE IF NOT EXISTS dim_ubicacion (
    id_ubicacion SERIAL PRIMARY KEY,
    departamento VARCHAR(60) NOT NULL,
    provincia VARCHAR(60) NOT NULL,
    distrito VARCHAR(80),
    lat FLOAT, lon FLOAT,
    UNIQUE(departamento, provincia)
);

-- 3. Tabla de Hechos LIMPIA (solo FKs + metricas produccion)
CREATE TABLE IF NOT EXISTS fact_produccion_limon (
    id_hecho SERIAL PRIMARY KEY,
    id_tiempo INT REFERENCES dim_tiempo(id_tiempo),
    id_ubicacion INT REFERENCES dim_ubicacion(id_ubicacion),
    id_clima INT REFERENCES dim_clima(id_clima),
    id_emergencia INT REFERENCES dim_emergencia(id_emergencia),
    id_noticias INT REFERENCES dim_noticias(id_noticias),
    -- Metricas MIDAGRI (unicas metricas en la tabla de hechos)
    produccion_t FLOAT DEFAULT 0,
    cosecha_ha FLOAT DEFAULT 0,
    precio_chacra_kg FLOAT,
    UNIQUE(id_tiempo, id_ubicacion)
);

-- Indices
CREATE INDEX IF NOT EXISTS idx_fact_tiempo ON fact_produccion_limon(id_tiempo);
CREATE INDEX IF NOT EXISTS idx_fact_ubicacion ON fact_produccion_limon(id_ubicacion);
CREATE INDEX IF NOT EXISTS idx_fact_clima ON fact_produccion_limon(id_clima);
'''
import os
sql_path = f"{DIRS['database']}/dwh_star_schema_v2.sql"
with open(sql_path, 'w', encoding='utf-8') as f: f.write(DDL)
print(DDL)
print(f"[OK] {sql_path}")
print("[ACTIVIDAD 07] COMPLETADA - Star Schema v2.0 con 5 dimensiones puras")
"""),
('md',"""## 7.3 Arquitectura Star Schema v2.0 — Decisión de Diseño

En el **Star Schema v2.0**, los datos de la NASA POWER se encapsulan en **`dim_clima`**, una dimensión satélite dedicada. Esto sigue las mejores prácticas de Data Warehousing:

| Decisión | Justificación |
|:---------|:-------------|
| `dim_clima` separada | Agrupa coherentemente las 6 variables climáticas NASA |
| `dim_emergencia` separada | Desacopla los datos INDECI de la tabla de hechos |
| `dim_noticias` separada | Permite escalar el módulo NLP en Fase 2 sin alterar la fact |
| `fact` solo con 3 métricas | Principio de tabla de hechos limpia: solo FKs + métricas de producción |"""),
],"actividad_07_dwh_schema.ipynb")

ok07=execute(p07)

print(f"\n{'='*55}")
print(f"  Acti. 05 Limpieza:   {'✅ OK' if ok05 else '❌ Error'}")
print(f"  Acti. 06 Integración:{'✅ OK' if ok06 else '❌ Error'}")
print(f"  Acti. 07 DWH Schema: {'✅ OK' if ok07 else '❌ Error'}")
