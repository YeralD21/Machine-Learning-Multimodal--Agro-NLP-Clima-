"""Genera notebooks 05 y 06."""
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

SETUP = """
import os, sys, json, re, warnings, unicodedata
import numpy as np, pandas as pd
import matplotlib; 
import matplotlib.pyplot as plt
import seaborn as sns
warnings.filterwarnings('ignore')
pd.set_option('display.max_columns', None)
sns.set_theme(style='whitegrid', palette='muted')
if os.path.basename(os.getcwd()) == 'notebooks':
    os.chdir(os.path.abspath('..'))
with open('data/02_interim/pipeline_config.json','r',encoding='utf-8') as f:
    CFG = json.load(f)
DIRS=CFG['DIRS']; INTERIM=DIRS['interim']; REPORTS=DIRS['reports']; PROCESSED=DIRS['processed']
print(f"CWD: {os.getcwd()} | Config OK")
"""

# ── ACTIVIDAD 05 ─────────────────────────────────────────────────────
act05 = [
('md', """# 🧹 Actividad 05: Limpieza y Estandarización de Datos
---
**Entrada:** `midagri_limon_raw.csv`, `indeci_eventos_dbf.csv`, `agraria_noticias_raw.csv`  
**Salida:** `midagri_limon_clean.csv`, `indeci_eventos_clean.csv`, `agraria_noticias_clean.csv`

Reglas de limpieza:
- Geografía → **MAYÚSCULAS SIN TILDES** (se conserva Ñ)
- Noticias → Remover HTML, URLs y caracteres especiales con **Regex**
- INDECI → Filtrar solo fenómenos **hidrometeorológicos válidos**
"""),
('code', SETUP),
('md', "## 5.1 Función de Normalización Geográfica"),
('code', """
def norm_geo(t):
    if not isinstance(t, str): return t
    t = t.strip().upper()
    t = t.replace('Ñ','__N__')
    for a,b in [('Á','A'),('É','E'),('Í','I'),('Ó','O'),('Ú','U')]:
        t = t.replace(a,b)
    t = ''.join(c for c in unicodedata.normalize('NFD',t) if unicodedata.category(c)!='Mn')
    return t.replace('__N__','Ñ')

def clean_text(t):
    if not isinstance(t, str): return ''
    t = re.sub(r'<[^>]+>', ' ', t)           # HTML
    t = re.sub(r'https?://\\S+|www\\.\\S+', ' ', t)  # URLs
    t = re.sub(r'[^\\w\\s\\.,;:\\-\\(\\)¿\\?¡\\!áéíóúÁÉÍÓÚñÑ]', ' ', t)
    return re.sub(r'\\s+', ' ', t).strip()

print("Funciones definidas: norm_geo() y clean_text()")
"""),
('md', "## 5.2 MIDAGRI — Renombrado y Estandarización"),
('code', """
df_m = pd.read_csv(f"{INTERIM}/midagri_limon_raw.csv")
print(f"Shape original: {df_m.shape}")

df_m = df_m.rename(columns={
    'anho':'anho','mes':'mes','COD_UBIGEO':'cod_ubigeo',
    'Dpto':'departamento','Prov':'provincia','Dist':'distrito',
    'dsc_Cultivo':'cultivo','PRODUCCION(t)':'produccion_t',
    'COSECHA (ha)':'cosecha_ha','MTO_PRECCHAC (S/ x kg)':'precio_chacra_kg'
})
for c in ['departamento','provincia','distrito','cultivo']:
    df_m[c] = df_m[c].apply(norm_geo)

df_m['fecha_evento'] = df_m['anho'].astype(str)+'-'+df_m['mes'].astype(str).str.zfill(2)
df_m['produccion_t']     = pd.to_numeric(df_m['produccion_t'], errors='coerce').fillna(0)
df_m['cosecha_ha']       = pd.to_numeric(df_m['cosecha_ha'], errors='coerce').fillna(0)
df_m['precio_chacra_kg'] = pd.to_numeric(df_m['precio_chacra_kg'], errors='coerce')

out = f"{INTERIM}/midagri_limon_clean.csv"
df_m.to_csv(out, index=False, encoding='utf-8-sig')
print(f"Dptos únicos: {df_m['departamento'].nunique()} | Provincias: {df_m['provincia'].nunique()}")
print(f"[OK] {out}")
print(df_m[['fecha_evento','departamento','provincia','produccion_t','precio_chacra_kg']].head(3).to_string(index=False))
"""),
('md', "## 5.3 INDECI — Filtro de Fenómenos + Estandarización"),
('code', """
df_ev = pd.read_csv(f"{INTERIM}/indeci_eventos_dbf.csv", low_memory=False)
print(f"Eventos originales: {len(df_ev):,}")

# Renombrar columnas clave
rename_map = {'departamen':'departamento','provincia':'provincia','fenomeno':'fenomeno'}
df_ev = df_ev.rename(columns={k:v for k,v in rename_map.items() if k in df_ev.columns})
for c in ['departamento','provincia','fenomeno']:
    if c in df_ev.columns:
        df_ev[c] = df_ev[c].apply(norm_geo)

df_ev['fecha'] = pd.to_datetime(df_ev['fecha'], errors='coerce')
df_ev['fecha_evento'] = df_ev['fecha'].dt.strftime('%Y-%m')

num_map = {'safecta':'personas_afectadas','sdamni':'personas_damnificadas',
           'sareaculti':'has_cultivo_afectadas','sareacul_1':'has_cultivo_perdidas'}
for s,d in num_map.items():
    if s in df_ev.columns:
        df_ev[d] = pd.to_numeric(df_ev[s], errors='coerce').fillna(0)

PELIGROS = [p.upper() for p in CFG['PELIGROS_VALIDOS']]
df_clean = df_ev[df_ev['fenomeno'].isin(PELIGROS)].copy()
print(f"Eventos hidrometeorológicos: {len(df_clean):,} ({len(df_clean)/len(df_ev)*100:.1f}%)")

fig, ax = plt.subplots(figsize=(10,4))
kept = df_clean['fenomeno'].value_counts()
kept.plot(kind='bar', ax=ax, color='steelblue', edgecolor='black')
ax.set_title('Fenómenos Válidos tras Filtro', fontsize=12, fontweight='bold')
ax.tick_params(axis='x', rotation=45)
plt.tight_layout(); plt.savefig(f"{REPORTS}/g6_indeci_filtro.png", dpi=150); plt.show()

out = f"{INTERIM}/indeci_eventos_clean.csv"
df_clean.to_csv(out, index=False, encoding='utf-8-sig')
print(f"[OK] {out}")
"""),
('md', "## 5.4 AGRARIA.PE — Limpieza NLP con Regex"),
('code', """
df_n = pd.read_csv(f"{INTERIM}/agraria_noticias_raw.csv")
df_n['fecha'] = pd.to_datetime(df_n['fecha'], errors='coerce')
df_n['fecha_evento'] = df_n['fecha'].dt.strftime('%Y-%m')

df_n['titular_clean'] = df_n['titular'].apply(clean_text)
df_n['cuerpo_clean']  = df_n['cuerpo_completo'].apply(clean_text)

orig_len  = df_n['cuerpo_completo'].astype(str).str.len().mean()
clean_len = df_n['cuerpo_clean'].str.len().mean()
print(f"Longitud media original:  {orig_len:,.0f} chars")
print(f"Longitud media limpia:    {clean_len:,.0f} chars  (reducción {(1-clean_len/orig_len)*100:.1f}%)")

# Comparación antes/después — primera noticia
idx = df_n['cuerpo_completo'].astype(str).str.len().idxmax()
print(f"\\nEjemplo — Antes (primeros 200 chars):")
print(str(df_n.loc[idx,'cuerpo_completo'])[:200])
print(f"\\nEjemplo — Después (primeros 200 chars):")
print(df_n.loc[idx,'cuerpo_clean'][:200])

out = f"{INTERIM}/agraria_noticias_clean.csv"
df_n.to_csv(out, index=False, encoding='utf-8-sig')
print(f"\\n[OK] {out} ({len(df_n)} filas)")
print("[ACTIVIDAD 05] COMPLETADA.")
"""),
('md', """## TODO: INTEGRACIÓN DATA NASA (COMPAÑERO)
Normalizar al integrar:
```python
df_nasa = pd.read_csv(f"{INTERIM}/nasa_clima_raw.csv")
df_nasa['departamento'] = df_nasa['departamento'].apply(norm_geo)  # misma función
df_nasa['fecha_evento'] = pd.to_datetime(df_nasa['DATE']).dt.strftime('%Y-%m')
# Validar coordenadas dentro de Perú
valid = df_nasa['lat'].between(-18.5,-0.1) & df_nasa['lon'].between(-81.4,-68.7)
print(f"Registros fuera de Perú: {(~valid).sum()}")
```
"""),
]
nb(act05, "actividad_05_limpieza.ipynb")

# ── ACTIVIDAD 06 ─────────────────────────────────────────────────────
act06 = [
('md', """# 🔗 Actividad 06: Integración al Data Warehouse
---
**Entrada:** `midagri_limon_clean.csv`, `indeci_eventos_clean.csv`, `agraria_noticias_clean.csv`  
**Salida:** `dataset_integrado.csv`

Lógica de integración:
1. **Esqueleto temporal** → todas las combinaciones `Mes × Provincia` (2021-2025)
2. **MIDAGRI** → agrega producción y precio a nivel mensual/provincia
3. **INDECI** → cuenta emergencias y suma afectados por mes/provincia
4. **Noticias** → cuenta noticias por mes (señal nacional)
5. **Left Join** sobre el esqueleto → resultado sin pérdida de períodos
"""),
('code', SETUP),
('md', "## 6.1 Cargar fuentes limpias"),
('code', """
df_m  = pd.read_csv(f"{INTERIM}/midagri_limon_clean.csv")
df_ev = pd.read_csv(f"{INTERIM}/indeci_eventos_clean.csv", low_memory=False)
df_n  = pd.read_csv(f"{INTERIM}/agraria_noticias_clean.csv")
print(f"MIDAGRI:  {len(df_m):,} filas")
print(f"INDECI:   {len(df_ev):,} filas")
print(f"Noticias: {len(df_n):,} filas")
"""),
('md', "## 6.2 Agregación MIDAGRI — Nivel Mensual / Provincia"),
('code', """
midagri_agg = (df_m
    .groupby(['fecha_evento','departamento','provincia'])
    .agg(produccion_t=('produccion_t','sum'),
         cosecha_ha=('cosecha_ha','sum'),
         precio_chacra_kg=('precio_chacra_kg','mean'))
    .reset_index())
print(f"MIDAGRI agregado: {len(midagri_agg):,} filas | Períodos únicos: {midagri_agg['fecha_evento'].nunique()}")
print(midagri_agg.head(3).to_string(index=False))
"""),
('md', "## 6.3 Agregación INDECI — Conteo de Emergencias por Mes / Provincia"),
('code', """
agg_dict = {'fenomeno':('count',)}
for c in ['personas_afectadas','has_cultivo_perdidas']:
    if c in df_ev.columns: agg_dict[c] = ('sum',)

indeci_agg = (df_ev
    .dropna(subset=['fecha_evento','departamento','provincia'])
    .groupby(['fecha_evento','departamento','provincia'])
    .agg(num_emergencias=('fenomeno','count'),
         total_afectados=('personas_afectadas','sum') if 'personas_afectadas' in df_ev.columns else ('fenomeno','count'),
         has_cultivo_perdidas=('has_cultivo_perdidas','sum') if 'has_cultivo_perdidas' in df_ev.columns else ('fenomeno','count'))
    .reset_index())
print(f"INDECI agregado: {len(indeci_agg):,} filas")
print(indeci_agg.head(3).to_string(index=False))
"""),
('md', "## 6.4 Agregación Noticias — Señal Nacional Mensual"),
('code', """
df_n['fecha_evento'] = pd.to_datetime(df_n['fecha'], errors='coerce').dt.strftime('%Y-%m')
noticias_agg = df_n.groupby('fecha_evento').agg(n_noticias=('titular','count')).reset_index()
print(f"Meses con noticias: {len(noticias_agg)}")
print(noticias_agg.to_string(index=False))
"""),
('md', "## 6.5 Esqueleto Temporal + Left Joins"),
('code', """
from pandas.tseries.offsets import MonthBegin
ANHO_I, ANHO_F = CFG['ANHO_INICIO'], CFG['ANHO_FIN']
fechas = pd.date_range(f'{ANHO_I}-01-01', f'{ANHO_F}-08-01', freq='MS')
provincias = midagri_agg[['departamento','provincia']].drop_duplicates()

skeleton = pd.DataFrame(
    [(d.strftime('%Y-%m'), r['departamento'], r['provincia'])
     for d in fechas for _, r in provincias.iterrows()],
    columns=['fecha_evento','departamento','provincia']
)
print(f"Esqueleto: {len(skeleton):,} filas  ({len(fechas)} meses × {len(provincias)} provincias)")

df_int = skeleton.copy()
df_int = pd.merge(df_int, midagri_agg,  on=['fecha_evento','departamento','provincia'], how='left')
df_int = pd.merge(df_int, indeci_agg,   on=['fecha_evento','departamento','provincia'], how='left')
df_int = pd.merge(df_int, noticias_agg, on='fecha_evento', how='left')

# Rellenar valores nulos
df_int['produccion_t']     = df_int['produccion_t'].fillna(0)
df_int['num_emergencias']  = df_int['num_emergencias'].fillna(0)
df_int['total_afectados']  = df_int['total_afectados'].fillna(0)
df_int['n_noticias']       = df_int['n_noticias'].fillna(0)
df_int = df_int.sort_values(['departamento','provincia','fecha_evento'])
df_int['precio_chacra_kg'] = df_int.groupby(['departamento','provincia'])['precio_chacra_kg'].ffill().bfill()

dupes = df_int.duplicated(subset=['fecha_evento','departamento','provincia']).sum()
print(f"Dataset integrado: {len(df_int):,} filas | Duplicados en llave: {dupes}")
print(f"Columnas: {df_int.columns.tolist()}")
"""),
('md', "## 6.6 Visualización del Join"),
('code', """
fig, axes = plt.subplots(1, 2, figsize=(14, 5))

# Cobertura temporal por departamento
pivot = df_int.pivot_table(values='produccion_t', index='departamento', columns='fecha_evento', aggfunc='sum')
pct_filled = (pivot.notna().sum(axis=1)/pivot.shape[1]*100).sort_values()
pct_filled.plot(kind='barh', ax=axes[0], color='mediumseagreen')
axes[0].set_title('Cobertura Temporal por Departamento (%)', fontsize=11, fontweight='bold')
axes[0].set_xlabel('% Meses con datos MIDAGRI')

# Distribución de emergencias integradas
em_dpto = df_int.groupby('departamento')['num_emergencias'].sum().sort_values(ascending=False).head(10)
em_dpto.plot(kind='bar', ax=axes[1], color='steelblue', edgecolor='black')
axes[1].set_title('Emergencias Integradas — Top 10 Dptos', fontsize=11, fontweight='bold')
axes[1].tick_params(axis='x', rotation=45)

plt.tight_layout()
plt.savefig(f"{REPORTS}/g7_integracion_cobertura.png", dpi=150)
plt.show()

out = f"{INTERIM}/dataset_integrado.csv"
df_int.to_csv(out, index=False, encoding='utf-8-sig')
print(f"[OK] {out}")
print("[ACTIVIDAD 06] COMPLETADA.")
"""),
('md', """## TODO: INTEGRACIÓN DATA NASA (COMPAÑERO)
```python
df_nasa = pd.read_csv(f"{INTERIM}/nasa_clima_raw.csv")
df_nasa['fecha_evento'] = pd.to_datetime(df_nasa['DATE']).dt.strftime('%Y-%m')
nasa_agg = df_nasa.groupby(['fecha_evento','departamento','provincia'])[
    ['T2M','T2M_MAX','T2M_MIN','PRECTOTCORR','RH2M','WS2M']].mean().reset_index()
df_int = pd.merge(df_int, nasa_agg, on=['fecha_evento','departamento','provincia'], how='left')
print("Variables NASA integradas:", ['T2M','PRECTOTCORR','RH2M','WS2M'])
```
"""),
]
nb(act06, "actividad_06_integracion.ipynb")

print("\n✅ Notebooks 05 y 06 generados.")
