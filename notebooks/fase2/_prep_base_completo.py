"""
_prep_base_completo.py
━━━━━━━━━━━━━━━━━━━━━
Construye el dataset base correcto para Fase 2 LSTM:
  Fuente 1: pipeline/output/06_integracion/dataset_integrado.csv
            (5,880 filas | 17 cols | incluye cosecha_ha)
  Fuente 2: data/processed/dataset_fase2_multivariado.csv
            (5,250 filas | aporta nlp_sentiment, month_sin, month_cos)
Salida   : data/processed/dataset_fase2_base_completo.csv
           (5,880 filas | con todas las features de Fase 2)
"""
import os
import sys
import numpy as np
import pandas as pd

sys.stdout.reconfigure(encoding='utf-8')

# El script está en notebooks/fase2/ → subimos 2 niveles para llegar a la raíz del proyecto
ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

SRC_INTEGRADO  = os.path.join(ROOT, 'pipeline', 'output', '06_integracion', 'dataset_integrado.csv')
SRC_FASE2      = os.path.join(ROOT, 'data', 'processed', 'dataset_fase2_multivariado.csv')
OUT_BASE       = os.path.join(ROOT, 'data', 'processed', 'dataset_fase2_base_completo.csv')

# ── 1. Cargar dataset base (fuente oficial) ───────────────────────────────────
print("Cargando dataset_integrado.csv ...")
df_base = pd.read_csv(SRC_INTEGRADO)
print(f"  Shape: {df_base.shape} | Rango: {df_base['fecha_evento'].min()} -> {df_base['fecha_evento'].max()}")

# Normalizar formato de fecha a YYYY-MM (por si acaso)
df_base['fecha_evento'] = df_base['fecha_evento'].astype(str).str[:7]
df_base['departamento'] = df_base['departamento'].str.upper().str.strip()
df_base['provincia']    = df_base['provincia'].str.upper().str.strip()

# ── 2. Cargar Fase 2 para extraer nlp_sentiment + month_sin + month_cos ───────
print("\nCargando dataset_fase2_multivariado.csv ...")
df_fase2 = pd.read_csv(SRC_FASE2)
print(f"  Shape: {df_fase2.shape} | Rango: {df_fase2['fecha_evento'].min()} -> {df_fase2['fecha_evento'].max()}")

# Normalizar fecha (puede venir como 2021-07-01)
df_fase2['fecha_evento'] = pd.to_datetime(df_fase2['fecha_evento']).dt.strftime('%Y-%m')
df_fase2['departamento'] = df_fase2['departamento'].str.upper().str.strip()
df_fase2['provincia']    = df_fase2['provincia'].str.upper().str.strip()

# Columnas a extraer de fase2
nlp_cols = ['nlp_sentiment', 'month_sin', 'month_cos']
available_nlp = [c for c in nlp_cols if c in df_fase2.columns]
print(f"  Columnas NLP/cíclicas disponibles: {available_nlp}")

df_nlp = df_fase2[['fecha_evento', 'departamento', 'provincia'] + available_nlp].copy()

# ── 3. Join: integrado LEFT JOIN nlp ─────────────────────────────────────────
print("\nRealizando LEFT JOIN ...")
df_merged = pd.merge(
    df_base,
    df_nlp,
    on=['fecha_evento', 'departamento', 'provincia'],
    how='left'
)
print(f"  Shape merged: {df_merged.shape}")

# ── 4. Calcular month_sin / month_cos si faltan ───────────────────────────────
if 'month_sin' not in df_merged.columns or df_merged['month_sin'].isna().all():
    print("  Calculando codificación cíclica desde fecha_evento ...")
    df_merged['month_num'] = pd.to_datetime(df_merged['fecha_evento'] + '-01').dt.month
    df_merged['month_sin'] = np.sin(2 * np.pi * df_merged['month_num'] / 12)
    df_merged['month_cos'] = np.cos(2 * np.pi * df_merged['month_num'] / 12)
    df_merged.drop(columns=['month_num'], inplace=True)
else:
    # Rellenar filas sin month_sin/cos (primeros 6 meses) usando la fórmula
    mask_nan = df_merged['month_sin'].isna()
    if mask_nan.sum() > 0:
        month_num = pd.to_datetime(df_merged.loc[mask_nan, 'fecha_evento'] + '-01').dt.month
        df_merged.loc[mask_nan, 'month_sin'] = np.sin(2 * np.pi * month_num / 12)
        df_merged.loc[mask_nan, 'month_cos'] = np.cos(2 * np.pi * month_num / 12)
        print(f"  Codificación cíclica calculada para {mask_nan.sum()} filas faltantes.")

# ── 5. Rellenar nlp_sentiment faltante con 0.0 (sentimiento neutro) ──────────
if 'nlp_sentiment' in df_merged.columns:
    nan_nlp = df_merged['nlp_sentiment'].isna().sum()
    if nan_nlp > 0:
        df_merged['nlp_sentiment'] = df_merged['nlp_sentiment'].fillna(0.0)
        print(f"  nlp_sentiment: {nan_nlp} NaNs rellenados con 0.0 (sentimiento neutro por ausencia de noticias)")
else:
    df_merged['nlp_sentiment'] = 0.0
    print("  nlp_sentiment no encontrado → asignado 0.0 a todo el dataset")

# ── 6. Reordenar columnas de forma lógica ────────────────────────────────────
id_cols    = ['fecha_evento', 'departamento', 'provincia']
agro_cols  = [c for c in ['produccion_t', 'cosecha_ha', 'precio_chacra_kg',
                           'num_emergencias', 'total_afectados'] if c in df_merged.columns]
nasa_cols  = [c for c in ['ALLSKY_SFC_SW_DWN', 'PRECTOTCORR', 'QV2M',
                           'RH2M', 'T2M', 'T2M_MAX', 'T2M_MIN', 'WS2M'] if c in df_merged.columns]
feat_cols  = [c for c in ['nlp_sentiment', 'n_noticias', 'month_sin', 'month_cos']
              if c in df_merged.columns]

ordered_cols = id_cols + agro_cols + nasa_cols + feat_cols
remaining    = [c for c in df_merged.columns if c not in ordered_cols]
df_final     = df_merged[ordered_cols + remaining]

# ── 7. Verificación final ────────────────────────────────────────────────────
print(f"\n=== DATASET BASE COMPLETO ===")
print(f"  Shape      : {df_final.shape}")
print(f"  Rango      : {df_final['fecha_evento'].min()} -> {df_final['fecha_evento'].max()}")
print(f"  Provincias : {df_final['provincia'].nunique()}")
print(f"  Columnas   : {df_final.columns.tolist()}")
print(f"  NaNs total : {df_final.isna().sum().sum()}")

# ── 8. Guardar ────────────────────────────────────────────────────────────────
os.makedirs(os.path.dirname(OUT_BASE), exist_ok=True)
df_final.to_csv(OUT_BASE, index=False)
print(f"\n✅ Guardado en: {OUT_BASE}")
