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
        capture_output=True, text=True, encoding='utf-8')
    ok = r.returncode == 0
    print(f"  {'✅ OK' if ok else '❌ ERROR'}")
    if not ok: print('\n'.join((r.stderr or '').strip().split('\n')[-15:]))
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
    display(df_nasa[['DEPARTAMENTO', 'PROVINCIA', 'fecha_evento']].head(3))
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
('md',"""# ⭐ Actividad 07 — Data Warehouse: Star Schema
---
## Modelo Dimensional — `limon_analytics_db`

```
                   ┌─────────────────────────┐
                   │       dim_tiempo         │
                   │─────────────────────────│
                   │  id_tiempo (PK)          │
                   │  fecha_evento VARCHAR(7) │
                   │  anho       SMALLINT     │
                   │  mes        SMALLINT     │
                   │  trimestre  SMALLINT     │
                   │  month_sin  FLOAT        │
                   │  month_cos  FLOAT        │
                   └─────────┬───────────────┘
                             │ FK
           ┌─────────────────┼─────────────────────┐
           │                 ▼                     │
┌──────────┴──────────┐  ┌──────────────────────────────────────────┐
│   dim_ubicacion     │  │       fact_produccion_limon  ★           │
│─────────────────────│  │──────────────────────────────────────────│
│ id_ubicacion (PK)   │  │ id_hecho         (PK)                   │
│ departamento        ├─►│ id_tiempo        (FK)                   │
│ provincia           │  │ id_ubicacion     (FK)                   │
│ lat  FLOAT          │  │ ── MIDAGRI ──                           │
│ lon  FLOAT          │  │ produccion_t      FLOAT                  │
└─────────────────────┘  │ cosecha_ha        FLOAT                  │
                         │ precio_chacra_kg  FLOAT                  │
                         │ ── INDECI ──                            │
                         │ num_emergencias   INT                    │
                         │ total_afectados   INT                    │
                         │ has_cultivo_perdidas FLOAT               │
                         │ ── AGRARIA.PE ──                        │
                         │ n_noticias        INT                    │
                         │ avg_sentimiento   FLOAT ◄── Fase 2      │
                         │ ── NASA POWER (TODO) ──                 │
                         │ temp_max_c        FLOAT ◄── NASA        │
                         │ precipitacion_mm  FLOAT ◄── NASA        │
                         │ humedad_rel_pct   FLOAT ◄── NASA        │
                         │ velocidad_viento  FLOAT ◄── NASA        │
                         │ radiacion_solar   FLOAT ◄── NASA        │
                         └──────────────────────────────────────────┘
```

| Aspecto | Decisión |
|:--------|:---------|
| **Tipo** | Star Schema (no Snowflake) — JOINs simples para OLAP |
| **Granularidad** | Mensual × Provincia |
| **Llave única** | `(id_tiempo, id_ubicacion)` — UNIQUE |
| **dim_tiempo** | month_sin/cos capturan estacionalidad biológica |
| **NASA** | Columnas reservadas como NULL hasta integración |
| **Sentimiento** | avg_sentimiento llenado en Fase 2 (NLP/BETO) |
"""),
('code',SETUP),
('md',"## 7.1 Visualización del Diagrama con Matplotlib"),
('code',"""
import matplotlib.patches as mpatches, matplotlib.patheffects as pe

fig,ax=plt.subplots(figsize=(14,9))
ax.set_xlim(0,14); ax.set_ylim(0,9); ax.axis('off')
ax.set_facecolor('#f8f9fa'); fig.patch.set_facecolor('#f8f9fa')
ax.set_title('Star Schema — limon_analytics_db\\nPipeline Fase 1: Predicción de Producción de Limón',
             fontsize=15, fontweight='bold', pad=15)

def draw_table(ax, x, y, w, h, title, rows, color_header='#2c3e50', color_bg='#ecf0f1'):
    ax.add_patch(mpatches.FancyBboxPatch((x,y),w,h,boxstyle="round,pad=0.1",
        facecolor=color_bg, edgecolor='#34495e', linewidth=2))
    ax.add_patch(mpatches.FancyBboxPatch((x,y+h-0.55),w,0.55,boxstyle="round,pad=0.05",
        facecolor=color_header, edgecolor='none'))
    ax.text(x+w/2,y+h-0.27,title,ha='center',va='center',fontsize=9,
            fontweight='bold',color='white')
    for i,(row,clr) in enumerate(rows):
        ax.text(x+0.15,y+h-0.85-i*0.38,row,va='center',fontsize=7,
                color='#2c3e50' if clr=='n' else '#e74c3c' if clr=='k' else '#3498db')

# Tabla central — HECHOS
draw_table(ax,4.5,1.5,5,6,'★  fact_produccion_limon',[
    ('id_hecho (PK)','k'),('id_tiempo (FK)','b'),('id_ubicacion (FK)','b'),
    ('── MIDAGRI ──','n'),('produccion_t  FLOAT','n'),('cosecha_ha  FLOAT','n'),
    ('precio_chacra_kg  FLOAT','n'),('── INDECI ──','n'),('num_emergencias  INT','n'),
    ('── NOTICIAS ──','n'),('n_noticias  INT','n'),('avg_sentimiento  FLOAT','n'),
    ('── NASA (TODO) ──','n'),('temp_max_c  FLOAT ◄','n'),('precipitacion_mm  FLOAT ◄','n'),
],'#8e44ad','#fdf2f8')

# dim_tiempo arriba
draw_table(ax,5.5,8.1,3,1.8,'⏰  dim_tiempo',[
    ('id_tiempo (PK)','k'),('fecha_evento, anho, mes','n'),
    ('trimestre, month_sin/cos','n'),
],'#16a085','#d5f5e3')

# dim_ubicacion izquierda
draw_table(ax,0.2,4,3.5,3,'📍  dim_ubicacion',[
    ('id_ubicacion (PK)','k'),('departamento VARCHAR','n'),
    ('provincia VARCHAR','n'),('lat, lon FLOAT','n'),
],'#1a5276','#d6eaf8')

# Flechas
ax.annotate('',xy=(7,7.5),xytext=(7,8.05),arrowprops=dict(arrowstyle='<-',color='#16a085',lw=2))
ax.annotate('',xy=(4.5,5),xytext=(3.7,5),arrowprops=dict(arrowstyle='<-',color='#1a5276',lw=2))
ax.text(3.9,5.15,'FK',fontsize=7,color='#1a5276',fontweight='bold')
ax.text(7.05,7.75,'FK',fontsize=7,color='#16a085',fontweight='bold')

plt.tight_layout()
plt.savefig(f"{REPORTS}/g07_star_schema.png",dpi=150,bbox_inches='tight')
plt.show()
print("✅ Diagrama Star Schema generado")
"""),
('md',"## 7.2 DDL del Star Schema"),
('code',"""
DDL='''CREATE TABLE IF NOT EXISTS dim_tiempo (
    id_tiempo SERIAL PRIMARY KEY, fecha_evento VARCHAR(7) NOT NULL UNIQUE,
    anho SMALLINT NOT NULL, mes SMALLINT NOT NULL, trimestre SMALLINT,
    month_sin FLOAT, month_cos FLOAT);

CREATE TABLE IF NOT EXISTS dim_ubicacion (
    id_ubicacion SERIAL PRIMARY KEY, departamento VARCHAR(60) NOT NULL,
    provincia VARCHAR(60) NOT NULL, lat FLOAT, lon FLOAT,
    UNIQUE(departamento,provincia));

CREATE TABLE IF NOT EXISTS fact_produccion_limon (
    id_hecho SERIAL PRIMARY KEY,
    id_tiempo INT NOT NULL REFERENCES dim_tiempo(id_tiempo),
    id_ubicacion INT NOT NULL REFERENCES dim_ubicacion(id_ubicacion),
    produccion_t FLOAT DEFAULT 0, cosecha_ha FLOAT DEFAULT 0, precio_chacra_kg FLOAT,
    num_emergencias INT DEFAULT 0, total_afectados INT DEFAULT 0,
    has_cultivo_perdidas FLOAT DEFAULT 0, n_noticias INT DEFAULT 0,
    avg_sentimiento FLOAT, temp_max_c FLOAT, temp_min_c FLOAT,
    precipitacion_mm FLOAT, humedad_rel_pct FLOAT, velocidad_viento FLOAT,
    radiacion_solar FLOAT, UNIQUE(id_tiempo,id_ubicacion));'''
import os
sql_path=f"{DIRS['database']}/dwh_star_schema.sql"
with open(sql_path,'w',encoding='utf-8') as f: f.write(DDL)
print(DDL); print(f"\\n[OK] {sql_path}")
print("✅ [ACTIVIDAD 07] COMPLETADA")
"""),
('md',"""## 7.3 Documentación de Integración NASA
Las columnas climáticas (`temp_max_c`, `precipitacion_mm`, etc.) ya forman parte integral de la tabla `fact_produccion_limon`. No se requiere una dimensión separada ya que el clima es un conjunto de métricas (hechos) medidas en el mismo nivel de granularidad (Mes-Provincia)."""),
],"actividad_07_dwh_schema.ipynb")

ok07=execute(p07)

print(f"\n{'='*55}")
print(f"  Acti. 05 Limpieza:   {'✅ OK' if ok05 else '❌ Error'}")
print(f"  Acti. 06 Integración:{'✅ OK' if ok06 else '❌ Error'}")
print(f"  Acti. 07 DWH Schema: {'✅ OK' if ok07 else '❌ Error'}")
