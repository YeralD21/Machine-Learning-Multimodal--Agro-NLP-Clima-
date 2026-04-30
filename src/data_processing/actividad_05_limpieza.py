"""
Pipeline Fase 1 - Actividad 5: Limpieza de Datos
- Estandarización geográfica: MAYÚSCULAS SIN TILDES en todas las fuentes.
- Limpieza NLP: Remover HTML, URLs y caracteres especiales de noticias.
- Salida: 3 CSVs limpios en data/02_interim/
"""
import sys; sys.stdout.reconfigure(encoding='utf-8')
import os, json, re, warnings, unicodedata
import pandas as pd

warnings.filterwarnings('ignore')

with open('data/02_interim/pipeline_config.json', 'r', encoding='utf-8') as f:
    CONFIG = json.load(f)
DIRS = CONFIG['DIRS']
INTERIM_DIR = DIRS['interim']
REPORTS_DIR = DIRS['reports']

print('=' * 70)
print('  ACTIVIDAD 5: Limpieza y Estandarización de Datos')
print('=' * 70)

# ─────────────────────────────────────────────
# Funciones de utilidad
# ─────────────────────────────────────────────
def normalize_geo(text: str) -> str:
    """Estandariza texto geográfico: MAYÚSCULAS, sin tildes, protege Ñ."""
    if not isinstance(text, str):
        return text
    text = text.strip().upper()
    text = text.replace('Ñ', '__NN__').replace('Á', 'A').replace('É', 'E') \
               .replace('Í', 'I').replace('Ó', 'O').replace('Ú', 'U')
    nfkd = unicodedata.normalize('NFD', text)
    text = ''.join(c for c in nfkd if unicodedata.category(c) != 'Mn')
    text = text.replace('__NN__', 'Ñ')
    return text

def clean_nlp_text(text: str) -> str:
    """Limpia texto de noticias: elimina HTML, URLs y caracteres especiales."""
    if not isinstance(text, str):
        return ''
    # Remover etiquetas HTML
    text = re.sub(r'<[^>]+>', ' ', text)
    # Remover URLs
    text = re.sub(r'https?://\S+|www\.\S+', ' ', text)
    # Remover caracteres especiales (mantener letras, números, puntuación básica)
    text = re.sub(r'[^\w\s\.,;:\-\(\)¿\?¡\!áéíóúÁÉÍÓÚñÑ]', ' ', text)
    # Colapsar espacios múltiples
    text = re.sub(r'\s+', ' ', text).strip()
    return text


# ─────────────────────────────────────────────
# 5.1 MIDAGRI — Estandarización geográfica
# ─────────────────────────────────────────────
print('\n[5.1] MIDAGRI — Estandarización geográfica')
df_m = pd.read_csv(os.path.join(INTERIM_DIR, 'midagri_limon_raw.csv'))

# Renombrar columnas a nombres estándar del pipeline
df_m = df_m.rename(columns={
    'anho': 'anho',
    'mes': 'mes',
    'COD_UBIGEO': 'cod_ubigeo',
    'Dpto': 'departamento',
    'Prov': 'provincia',
    'Dist': 'distrito',
    'dsc_Cultivo': 'cultivo',
    'PRODUCCION(t)': 'produccion_t',
    'COSECHA (ha)': 'cosecha_ha',
    'MTO_PRECCHAC (S/ x kg)': 'precio_chacra_kg',
})

# Aplicar normalización geográfica
for col in ['departamento', 'provincia', 'distrito', 'cultivo']:
    df_m[col] = df_m[col].apply(normalize_geo)

# Crear clave temporal estándar
df_m['fecha_evento'] = df_m['anho'].astype(str) + '-' + df_m['mes'].astype(str).str.zfill(2)

# Tipos numéricos
df_m['produccion_t']   = pd.to_numeric(df_m['produccion_t'], errors='coerce').fillna(0)
df_m['cosecha_ha']     = pd.to_numeric(df_m['cosecha_ha'], errors='coerce').fillna(0)
df_m['precio_chacra_kg'] = pd.to_numeric(df_m['precio_chacra_kg'], errors='coerce')

out_m = os.path.join(INTERIM_DIR, 'midagri_limon_clean.csv')
df_m.to_csv(out_m, index=False, encoding='utf-8-sig')
print(f'  Departamentos únicos: {df_m["departamento"].nunique()}')
print(f'  Provincias únicas:    {df_m["provincia"].nunique()}')
print(f'  [OK] {out_m} ({len(df_m):,} filas)')


# ─────────────────────────────────────────────
# 5.2 INDECI — Estandarización geográfica
# ─────────────────────────────────────────────
print('\n[5.2] INDECI — Estandarización geográfica + filtro fenómenos')
df_ev = pd.read_csv(os.path.join(INTERIM_DIR, 'indeci_eventos_dbf.csv'), low_memory=False)

# Normalizar columnas clave de geolocalización
geo_cols_indeci = {'departamen': 'departamento', 'provincia': 'provincia', 'fenomeno': 'fenomeno'}
df_ev = df_ev.rename(columns=geo_cols_indeci)

for col in ['departamento', 'provincia', 'fenomeno']:
    if col in df_ev.columns:
        df_ev[col] = df_ev[col].apply(normalize_geo)

# Convertir fecha
df_ev['fecha'] = pd.to_datetime(df_ev['fecha'], errors='coerce')
df_ev['fecha_evento'] = df_ev['fecha'].dt.strftime('%Y-%m')

# Campos numéricos clave de afectación
num_cols = {'safecta': 'personas_afectadas', 'sdamni': 'personas_damnificadas',
            'sareaculti': 'has_cultivo_afectadas', 'sareacul_1': 'has_cultivo_perdidas'}
for src, dst in num_cols.items():
    if src in df_ev.columns:
        df_ev[dst] = pd.to_numeric(df_ev[src], errors='coerce').fillna(0)

# Filtrar solo fenómenos hidrometeorológicos válidos
PELIGROS = [p.upper() for p in CONFIG['PELIGROS_VALIDOS']]
mask_fen = df_ev['fenomeno'].isin(PELIGROS)
df_ev_filtered = df_ev[mask_fen].copy()
print(f'  Eventos originales:            {len(df_ev):,}')
print(f'  Eventos hidrometeorológicos:   {len(df_ev_filtered):,} ({len(df_ev_filtered)/len(df_ev)*100:.1f}%)')

out_ev = os.path.join(INTERIM_DIR, 'indeci_eventos_clean.csv')
df_ev_filtered.to_csv(out_ev, index=False, encoding='utf-8-sig')
print(f'  [OK] {out_ev} ({len(df_ev_filtered):,} filas)')


# ─────────────────────────────────────────────
# 5.3 AGRARIA.PE — Limpieza NLP del cuerpo de noticias
# ─────────────────────────────────────────────
print('\n[5.3] AGRARIA.PE — Limpieza NLP de titulares y cuerpos')
df_n = pd.read_csv(os.path.join(INTERIM_DIR, 'agraria_noticias_raw.csv'))

# Limpiar HTML/URLs del cuerpo y titular
df_n['titular_clean']  = df_n['titular'].apply(clean_nlp_text)
df_n['cuerpo_clean']   = df_n['cuerpo_completo'].apply(clean_nlp_text)

# Longitud del texto antes/después
orig_len = df_n['cuerpo_completo'].astype(str).str.len().mean()
clean_len = df_n['cuerpo_clean'].str.len().mean()
print(f'  Longitud media cuerpo original: {orig_len:,.0f} chars')
print(f'  Longitud media cuerpo limpio:   {clean_len:,.0f} chars')
print(f'  Reducción: {(1 - clean_len/orig_len)*100:.1f}%')

# Clave temporal
df_n['fecha'] = pd.to_datetime(df_n['fecha'], errors='coerce')
df_n['fecha_evento'] = df_n['fecha'].dt.strftime('%Y-%m')

out_n = os.path.join(INTERIM_DIR, 'agraria_noticias_clean.csv')
df_n.to_csv(out_n, index=False, encoding='utf-8-sig')
print(f'  [OK] {out_n} ({len(df_n)} filas)')

# TODO: INTEGRACIÓN DATA NASA
# Cuando se integre, normalizar aquí:
# - Nombres de estaciones climáticas: strip + UPPER
# - Verificar que las coordenadas (lat, lon) estén dentro del territorio peruano:
#   Lat: -18.5 a -0.1 | Lon: -81.4 a -68.7
# Código sugerido:
#   df_nasa['estacion'] = df_nasa['estacion'].str.strip().str.upper()
#   valid_peru = ((df_nasa['lat'].between(-18.5, -0.1)) &
#                 (df_nasa['lon'].between(-81.4, -68.7)))
#   print(f'Registros fuera de Perú: {(~valid_peru).sum()}')
print('\n  [NASA] Placeholder normalización de estaciones climáticas (ver TODO)')

print()
print('[ACTIVIDAD 5] COMPLETADA.')
print('  Descripcion: Estandarizacion geografica (MAYUSC/SIN TILDES) + Limpieza NLP.')
print(f'  Archivos generados:')
print(f'    {out_m}')
print(f'    {out_ev}')
print(f'    {out_n}')
