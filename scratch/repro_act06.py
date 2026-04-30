import pandas as pd
import os
import json

with open('data/02_interim/pipeline_config.json','r',encoding='utf-8') as f:
    CFG = json.load(f)
INTERIM = CFG['DIRS']['interim']

print(f"Reading from {INTERIM}...")
df_m  = pd.read_csv(f"{INTERIM}/midagri_limon_clean.csv")
df_ev = pd.read_csv(f"{INTERIM}/indeci_eventos_clean.csv", low_memory=False)
df_n  = pd.read_csv(f"{INTERIM}/agraria_noticias_clean.csv")

print("MIDAGRI cols:", df_m.columns.tolist())
midagri_agg = (df_m
    .groupby(['fecha_evento','departamento','provincia'])
    .agg(produccion_t=('produccion_t','sum'))
    .reset_index())
print("MIDAGRI agg OK")

print("INDECI cols:", df_ev.columns.tolist())
indeci_agg = (df_ev
    .dropna(subset=['fecha_evento','departamento','provincia'])
    .groupby(['fecha_evento','departamento','provincia'])
    .agg(num_emergencias=('fenomeno','count'))
    .reset_index())
print("INDECI agg OK")
