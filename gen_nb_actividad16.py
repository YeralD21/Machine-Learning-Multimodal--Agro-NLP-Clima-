#!/usr/bin/env python3
"""Genera notebooks/fase4/actividad_16_shap.ipynb"""
import json
from pathlib import Path

ROOT = Path('C:/Machine-learming/Machine-Learning-Multimodal--Agro-NLP-Clima-')
OUT  = ROOT / 'notebooks' / 'fase4' / 'actividad_16_shap.ipynb'
OUT.parent.mkdir(parents=True, exist_ok=True)


def md(n, src):
    return {'cell_type': 'markdown', 'id': f'a16-{n:04d}', 'metadata': {}, 'source': src}


def code(n, src):
    return {
        'cell_type': 'code', 'execution_count': None,
        'id': f'a16-{n:04d}', 'metadata': {}, 'outputs': [], 'source': src,
    }


cells = []

# ── 1. Título ────────────────────────────────────────────────────────────────
cells.append(md(1, """\
# Actividad 16 — Análisis SHAP: Explicabilidad del Modelo GE

**Fase 4 — Análisis XAI (Explainable AI)**
**Modelo analizado:** GE — Dual-Input LSTM-64 + Bahdanau Attention  (MAE=0.0673)
**Método:** SHAP KernelExplainer (model-agnóstico, compatible con arquitecturas dual-input)

## Objetivos

1. **Summary plot global** — importancia promedio de cada feature sobre el conjunto de prueba
2. **Comparativa Canal A vs Canal B** — ¿cuánto aporta la historia de producción vs las exógenas?
3. **Análisis local: meses de shock** — qué variables explican los peores errores (Ene-Mar 2025)
4. **Heatmap temporal** — evolución de la importancia de features a lo largo del test

## Método SHAP

```
X_flat = [ Canal A (6 timesteps) | Canal B (6 × 23 timesteps) ]
         shape (n, 6) concatenado con (n, 138) → (n, 144)

KernelExplainer(predict_fn, background=kmeans(X_train, 10))
  → shap_values: (n_test, 144)

Agregación por feature:
  shap_A[i] = sum(shap_values[i, :6])           # 1 valor por muestra
  shap_B[i,j] = sum(shap_values[i, 6+j*6:6+j*6+6])  # 1 valor por feature × muestra
  → shap_agg: (n_test, 24)
```

> **Nota:** El archivo del modelo es `ge_dual_lstm_attn.keras` (no `ge_modelo.keras`).
"""))

# ── 2. Imports ───────────────────────────────────────────────────────────────
cells.append(code(2, """\
import os, platform, sys, warnings
os.environ['TF_CPP_MIN_LOG_LEVEL']  = '3'
os.environ['TF_ENABLE_ONEDNN_OPTS'] = '0'
warnings.filterwarnings('ignore')

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import seaborn as sns
import json, joblib
from pathlib import Path

import tensorflow as tf
tf.get_logger().setLevel('ERROR')
from tensorflow import keras
from tensorflow.keras import layers

import shap

from sklearn.preprocessing import StandardScaler
from sklearn.metrics import mean_absolute_error

sns.set_theme(style='whitegrid', palette='muted')
plt.rcParams['figure.facecolor'] = 'white'

print(f'Python      : {sys.version.split()[0]}')
print(f'TensorFlow  : {tf.__version__}')
print(f'SHAP        : {shap.__version__}')
print(f'NumPy       : {np.__version__}')
print(f'Pandas      : {pd.__version__}')
"""))

# ── 3. Rutas y constantes ────────────────────────────────────────────────────
cells.append(code(3, """\
def detect_project_root():
    if platform.system() == 'Windows':
        p = Path('C:/Machine-learming/Machine-Learning-Multimodal--Agro-NLP-Clima-')
        if p.exists():
            return p
    for c in [
        Path('/mnt/c/Machine-learming/Machine-Learning-Multimodal--Agro-NLP-Clima-'),
        Path.home() / 'Machine-learming' / 'Machine-Learning-Multimodal--Agro-NLP-Clima-',
    ]:
        if c.exists():
            return c
    raise FileNotFoundError('No se encontro la raiz del proyecto.')

PROYECTO_ROOT  = detect_project_root()
DATASET_PATH   = PROYECTO_ROOT / 'data' / 'processed' / 'master_dataset_fase2_multivariado.csv'
MODEL_PATH     = PROYECTO_ROOT / 'resultados' / 'ge' / 'ge_dual_lstm_attn.keras'
SCALERS_PATH   = PROYECTO_ROOT / 'resultados' / 'ge' / 'ge_scalers.pkl'
PREDS_PATH     = PROYECTO_ROOT / 'resultados' / 'ge' / 'ge_predicciones.csv'
SHAP_DIR       = PROYECTO_ROOT / 'resultados' / 'shap'
SHAP_DIR.mkdir(parents=True, exist_ok=True)

SEQ_LEN      = 6
SPLIT_RATIO  = 0.80
RANDOM_STATE = 42
np.random.seed(RANDOM_STATE)

CANAL_B_COLS = [
    'lag_1', 'lag_3', 'lag_6',
    'precio_chacra_kg',
    'num_emergencias', 'total_afectados', 'hectareas_cultivo_perdidas',
    'ALLSKY_SFC_SW_DWN', 'PRECTOTCORR', 'QV2M', 'RH2M',
    'T2M', 'T2M_MAX', 'T2M_MIN', 'WS2M',
    'lat', 'lon',
    'month_sin', 'month_cos', 'mes_num',
    'trimestre_num', 'trimestre_sin', 'trimestre_cos',
]
N_FEATURES_A = 1
N_FEATURES_B = len(CANAL_B_COLS)   # 23
N_A_FLAT     = SEQ_LEN * N_FEATURES_A   # 6
N_B_FLAT     = SEQ_LEN * N_FEATURES_B   # 138
N_FLAT       = N_A_FLAT + N_B_FLAT      # 144

# Nombres de features agregados (nivel feature, no timestep)
FEAT_NAMES = ['produccion_t (hist)'] + CANAL_B_COLS   # 24 nombres

# Etiquetas legibles para gráficos
FEAT_LABELS = {
    'produccion_t (hist)': 'prod_t (hist)',
    'lag_1': 'lag t-1', 'lag_3': 'lag t-3', 'lag_6': 'lag t-6',
    'precio_chacra_kg': 'precio chacra',
    'num_emergencias': 'n_emergencias', 'total_afectados': 'tot_afectados',
    'hectareas_cultivo_perdidas': 'ha_perdidas',
    'ALLSKY_SFC_SW_DWN': 'radiacion', 'PRECTOTCORR': 'precipitacion',
    'QV2M': 'humedad_esp', 'RH2M': 'humedad_rel',
    'T2M': 'temp_media', 'T2M_MAX': 'temp_max', 'T2M_MIN': 'temp_min',
    'WS2M': 'vel_viento',
    'lat': 'latitud', 'lon': 'longitud',
    'month_sin': 'mes_sin', 'month_cos': 'mes_cos', 'mes_num': 'mes_num',
    'trimestre_num': 'trim_num', 'trimestre_sin': 'trim_sin', 'trimestre_cos': 'trim_cos',
}

print(f'Proyecto     : {PROYECTO_ROOT}')
print(f'Modelo GE    : {MODEL_PATH.name}  existe={MODEL_PATH.exists()}')
print(f'Scalers      : {SCALERS_PATH.name}  existe={SCALERS_PATH.exists()}')
print(f'SHAP dir     : {SHAP_DIR}')
print(f'Input flat   : {N_FLAT} dimensiones ({N_A_FLAT} Canal A + {N_B_FLAT} Canal B)')
print(f'Features agg : {len(FEAT_NAMES)}')
"""))

# ── 4. Definir BahdanauAttention (necesario para cargar el modelo) ───────────
cells.append(md(4, """\
## 1. Carga del modelo GE

La capa `BahdanauAttention` es custom — debe redefinirse antes de cargar el `.keras`.
"""))

cells.append(code(5, """\
class BahdanauAttention(keras.layers.Layer):
    def __init__(self, units, **kwargs):
        super().__init__(**kwargs)
        self.units    = units
        self.W_query  = layers.Dense(units, use_bias=False)
        self.W_values = layers.Dense(units, use_bias=False)
        self.V        = layers.Dense(1,     use_bias=False)

    def call(self, query, values):
        q_exp  = tf.expand_dims(self.W_query(query), axis=1)
        energy = self.V(tf.nn.tanh(self.W_values(values) + q_exp))
        alpha  = tf.nn.softmax(energy, axis=1)
        ctx    = tf.reduce_sum(alpha * values, axis=1)
        return ctx, alpha

    def get_config(self):
        cfg = super().get_config()
        cfg.update({'units': self.units})
        return cfg


model = keras.models.load_model(
    MODEL_PATH,
    custom_objects={'BahdanauAttention': BahdanauAttention}
)

scalers = joblib.load(SCALERS_PATH)
scaler_a = scalers['scaler_a']
scaler_b = scalers['scaler_b']

model.summary(line_length=65)
print(f'\\nModelo cargado: {MODEL_PATH.name}')
print(f'Inputs  : {[i.shape for i in model.inputs]}')
print(f'Output  : {model.output.shape}')
"""))

# ── 5. Preparación de datos ───────────────────────────────────────────────────
cells.append(md(6, "## 2. Preparación de datos y secuencias"))

cells.append(code(7, """\
AGG_COLS = [
    'produccion_t', 'precio_chacra_kg',
    'num_emergencias', 'total_afectados', 'hectareas_cultivo_perdidas',
    'ALLSKY_SFC_SW_DWN', 'PRECTOTCORR', 'QV2M', 'RH2M',
    'T2M', 'T2M_MAX', 'T2M_MIN', 'WS2M',
    'lat', 'lon',
    'month_sin', 'month_cos', 'mes_num',
    'trimestre_num', 'trimestre_sin', 'trimestre_cos',
]

df_raw   = pd.read_csv(DATASET_PATH, parse_dates=['fecha_evento'])
nacional = df_raw.groupby('fecha_evento')[AGG_COLS].mean().sort_index()
nacional.index = pd.DatetimeIndex(nacional.index, freq='MS')

nacional['lag_1'] = nacional['produccion_t'].shift(1)
nacional['lag_3'] = nacional['produccion_t'].shift(3)
nacional['lag_6'] = nacional['produccion_t'].shift(6)
nacional = nacional.dropna()

n_total = len(nacional)
n_train = int(n_total * SPLIT_RATIO)
n_test  = n_total - n_train

train_df = nacional.iloc[:n_train]
test_df  = nacional.iloc[n_train:]

y_train  = train_df['produccion_t'].values
y_test   = test_df['produccion_t'].values
B_train  = train_df[CANAL_B_COLS].values
B_test   = test_df[CANAL_B_COLS].values

y_train_sc = scaler_a.transform(y_train.reshape(-1,1)).flatten()
y_test_sc  = scaler_a.transform(y_test.reshape(-1,1)).flatten()
B_train_sc = scaler_b.transform(B_train).astype(np.float32)
B_test_sc  = scaler_b.transform(B_test).astype(np.float32)

print(f'Serie: {n_total} meses  Train={n_train}  Test={n_test}')
print(f'Test : {test_df.index.min().date()} -> {test_df.index.max().date()}')
"""))

cells.append(code(8, """\
# ── Secuencias de entrenamiento (para background SHAP) ───────────────────
def make_dual_sequences(y_sc, B_sc, seq_len):
    Xa, Xb, Y = [], [], []
    for i in range(len(y_sc) - seq_len):
        Xa.append(y_sc[i:i+seq_len].reshape(-1, 1))
        Xb.append(B_sc[i:i+seq_len])
        Y.append(y_sc[i+seq_len])
    return (np.array(Xa, np.float32),
            np.array(Xb, np.float32),
            np.array(Y,  np.float32))

Xa_train, Xb_train, y_seq_train = make_dual_sequences(y_train_sc, B_train_sc, SEQ_LEN)

# ── Secuencias de TEST con valores observados (sin recursión) ────────────────
# Cada test step i usa los 6 meses anteriores de los datos reales
y_all_sc = np.concatenate([y_train_sc, y_test_sc])
B_all_sc = np.vstack([B_train_sc, B_test_sc])

Xa_test_s = np.array(
    [y_all_sc[n_train+i-SEQ_LEN : n_train+i].reshape(-1,1) for i in range(n_test)],
    dtype=np.float32
)
Xb_test_s = np.array(
    [B_all_sc[n_train+i-SEQ_LEN : n_train+i] for i in range(n_test)],
    dtype=np.float32
)

n_seq = len(Xa_train)
print(f'Secuencias train : {n_seq}  shape Xa={Xa_train.shape}  Xb={Xb_train.shape}')
print(f'Secuencias test  : {n_test}  shape Xa={Xa_test_s.shape}  Xb={Xb_test_s.shape}')
"""))

# ── 6. Aplanar inputs ─────────────────────────────────────────────────────────
cells.append(code(9, """\
# Aplanar para KernelExplainer: (n, 144)
X_train_flat = np.hstack([
    Xa_train.reshape(n_seq, N_A_FLAT),
    Xb_train.reshape(n_seq, N_B_FLAT)
]).astype(np.float64)

X_test_flat = np.hstack([
    Xa_test_s.reshape(n_test, N_A_FLAT),
    Xb_test_s.reshape(n_test, N_B_FLAT)
]).astype(np.float64)

print(f'X_train_flat : {X_train_flat.shape}')
print(f'X_test_flat  : {X_test_flat.shape}')
print(f'Dimensiones  : {N_A_FLAT} (Canal A) + {N_B_FLAT} (Canal B) = {N_FLAT}')
"""))

# ── 7. KernelExplainer ────────────────────────────────────────────────────────
cells.append(md(10, """\
## 3. SHAP — KernelExplainer

**KernelExplainer** evalúa el modelo como caja negra usando una función envolvente
que reconstruye los inputs duales desde el vector aplanado de 144 dimensiones.

- **Background:** 10 centroides k-means del conjunto de entrenamiento
- **nsamples:** 200 por muestra de test (equilibrio velocidad/precisión)
- **Tiempo estimado:** ~2-4 min en CPU
"""))

cells.append(code(11, """\
def predict_fn(X_flat):
    n  = len(X_flat)
    xa = X_flat[:, :N_A_FLAT].reshape(n, SEQ_LEN, N_FEATURES_A).astype(np.float32)
    xb = X_flat[:, N_A_FLAT:].reshape(n, SEQ_LEN, N_FEATURES_B).astype(np.float32)
    return model.predict([xa, xb], verbose=0).flatten()


print('Generando background (k-means, k=10)...')
background = shap.kmeans(X_train_flat, 10)

print('Inicializando KernelExplainer...')
explainer = shap.KernelExplainer(predict_fn, background)

print(f'Calculando SHAP para {n_test} meses de prueba (nsamples=200)...')
shap_values = explainer.shap_values(X_test_flat, nsamples=200, silent=True)
# shap_values: (n_test, 144)

print(f'SHAP calculado. Shape: {np.array(shap_values).shape}')
print(f'Base value (predicción esperada): {explainer.expected_value:.4f}')
"""))

# ── 8. Agregación por feature ─────────────────────────────────────────────────
cells.append(md(12, """\
## 4. Agregación de SHAP por feature

Suma los SHAP de los 6 timesteps de cada feature:

```
shap_agg[i, 0]   = Σ shap_values[i, 0:6]         # Canal A (produccion_t)
shap_agg[i, 1]   = Σ shap_values[i, 6:12]         # lag_1  (todos sus timesteps)
shap_agg[i, 2]   = Σ shap_values[i, 12:18]        # lag_3
...
shap_agg[i, 23]  = Σ shap_values[i, 132:138]      # trimestre_cos
```
"""))

cells.append(code(13, """\
shap_arr = np.array(shap_values)   # (10, 144)

shap_a_agg = shap_arr[:, :N_A_FLAT].sum(axis=1, keepdims=True)          # (10, 1)
shap_b_raw = shap_arr[:, N_A_FLAT:].reshape(n_test, SEQ_LEN, N_FEATURES_B)
shap_b_agg = shap_b_raw.sum(axis=1)                                      # (10, 23)
shap_agg   = np.hstack([shap_a_agg, shap_b_agg])                        # (10, 24)

# Feature values medios sobre la ventana (para colorear beeswarm)
x_a_mean = X_test_flat[:, :N_A_FLAT].mean(axis=1, keepdims=True)
x_b_mean = X_test_flat[:, N_A_FLAT:].reshape(n_test, SEQ_LEN, N_FEATURES_B).mean(axis=1)
X_test_agg = np.hstack([x_a_mean, x_b_mean])   # (10, 24)

# Importancia global (media de |SHAP|)
importance_global = np.abs(shap_agg).mean(axis=0)  # (24,)
rank_idx = np.argsort(importance_global)[::-1]

print('Importancia global |SHAP| promedio (top 10):')
for i, idx in enumerate(rank_idx[:10], 1):
    print(f'  {i:2d}. {FEAT_NAMES[idx]:<30} {importance_global[idx]:.5f}')
"""))

# ── 9. Summary Plot ───────────────────────────────────────────────────────────
cells.append(md(14, "## 5. Summary Plot — Importancia Global"))

cells.append(code(15, """\
labels = [FEAT_LABELS.get(f, f) for f in FEAT_NAMES]

# ── Gráfico de barras (media |SHAP|) ─────────────────────────────────────────
fig, ax = plt.subplots(figsize=(9, 8))
imp_sorted = importance_global[rank_idx]
lbl_sorted = [labels[i] for i in rank_idx]
colors     = plt.cm.RdYlGn_r(np.linspace(0.1, 0.9, len(imp_sorted)))

bars = ax.barh(range(len(imp_sorted)), imp_sorted, color=colors, alpha=0.85)
ax.set_yticks(range(len(imp_sorted)))
ax.set_yticklabels(lbl_sorted, fontsize=8)
ax.invert_yaxis()
ax.set_xlabel('Importancia media |SHAP| (z-score)', fontsize=9)
ax.set_title('Importancia Global de Features — Modelo GE\\n(media |SHAP| sobre 10 meses de prueba)',
             fontsize=11, fontweight='bold')
for bar, v in zip(bars, imp_sorted):
    ax.text(v + 0.0002, bar.get_y() + bar.get_height()/2,
            f'{v:.4f}', va='center', fontsize=7)
plt.tight_layout()
plt.savefig(SHAP_DIR / 'shap_summary_bar.png', dpi=150, bbox_inches='tight')
plt.show()
print('Guardado -> shap_summary_bar.png')
"""))

cells.append(code(16, """\
# ── Beeswarm (dot plot) ──────────────────────────────────────────────────────
# Ordenar features por importancia (top 15 para legibilidad)
top_n  = 15
top_idx = rank_idx[:top_n]
sv_top  = shap_agg[:, top_idx]          # (10, 15)
fv_top  = X_test_agg[:, top_idx]        # (10, 15)
lbl_top = [labels[i] for i in top_idx]

fig, ax = plt.subplots(figsize=(10, 7))
norm = plt.Normalize(fv_top.min(), fv_top.max())
cmap = plt.cm.coolwarm

for j in range(top_n):
    y_pos = top_n - 1 - j
    for k in range(n_test):
        ax.scatter(sv_top[k, j], y_pos + np.random.uniform(-0.25, 0.25),
                   c=cmap(norm(fv_top[k, j])), s=60, alpha=0.85, zorder=3)

ax.set_yticks(range(top_n))
ax.set_yticklabels(lbl_top[::-1], fontsize=9)
ax.axvline(0, color='black', lw=0.8)
ax.set_xlabel('Valor SHAP (impacto en la predicción z-score)', fontsize=9)
ax.set_title('Beeswarm Plot — Top 15 Features\\n(color = valor de la feature; rojo=alto, azul=bajo)',
             fontsize=11, fontweight='bold')

sm = plt.cm.ScalarMappable(cmap=cmap, norm=norm)
sm.set_array([])
plt.colorbar(sm, ax=ax, label='Valor normalizado de la feature', fraction=0.03, pad=0.01)
plt.tight_layout()
plt.savefig(SHAP_DIR / 'shap_beeswarm.png', dpi=150, bbox_inches='tight')
plt.show()
print('Guardado -> shap_beeswarm.png')
"""))

# ── 10. Canal A vs Canal B ────────────────────────────────────────────────────
cells.append(md(17, "## 6. Canal A vs Canal B — Contribución Total"))

cells.append(code(18, """\
# Contribución media por canal
contrib_a = np.abs(shap_agg[:, 0]).mean()           # produccion_t histórica
contrib_b = np.abs(shap_agg[:, 1:]).mean(axis=0)    # (23,) una por feature

# Agrupaciones de Canal B
grupos = {
    'Lags\\n(prod. t-1,3,6)' : [CANAL_B_COLS.index(f) for f in ['lag_1','lag_3','lag_6']],
    'Clima\\n(NASA POWER)'   : [CANAL_B_COLS.index(f) for f in
                                  ['ALLSKY_SFC_SW_DWN','PRECTOTCORR','QV2M','RH2M',
                                   'T2M','T2M_MAX','T2M_MIN','WS2M']],
    'INDECI\\n(desastres)'   : [CANAL_B_COLS.index(f) for f in
                                  ['num_emergencias','total_afectados','hectareas_cultivo_perdidas']],
    'Precio\\nchacra'        : [CANAL_B_COLS.index('precio_chacra_kg')],
    'Temporal\\ncíclico'     : [CANAL_B_COLS.index(f) for f in
                                  ['month_sin','month_cos','mes_num',
                                   'trimestre_num','trimestre_sin','trimestre_cos']],
    'Geo\\n(lat, lon)'       : [CANAL_B_COLS.index(f) for f in ['lat','lon']],
}

vals_grupo  = [contrib_a] + [contrib_b[idx].sum() for idx in grupos.values()]
lbl_grupo   = ['Canal A\\nProd. hist.'] + list(grupos.keys())
colores_grp = ['#2E86AB', '#4C9A52', '#5B8DB8', '#E07B39', '#C0392B', '#8E44AD', '#F39C12']

fig, axes = plt.subplots(1, 2, figsize=(14, 5))

# ── Panel izq: por canal (A vs B total) ──────────────────────────────────────
contrib_b_total = np.abs(shap_agg[:, 1:]).sum(axis=1).mean()
axes[0].bar(['Canal A\\n(produccion_t)', 'Canal B\\n(exógenas)'],
            [contrib_a, contrib_b_total],
            color=['#2E86AB', '#C0392B'], alpha=0.85, width=0.5)
axes[0].set_title('Contribución Media |SHAP|\\nCanal A vs Canal B', fontsize=11, fontweight='bold')
axes[0].set_ylabel('|SHAP| medio (z-score)')
for i, v in enumerate([contrib_a, contrib_b_total]):
    axes[0].text(i, v + 0.0003, f'{v:.4f}', ha='center', fontsize=10, fontweight='bold')

# ── Panel der: por grupo de features ─────────────────────────────────────────
bars = axes[1].bar(range(len(lbl_grupo)), vals_grupo, color=colores_grp, alpha=0.85, width=0.6)
axes[1].set_xticks(range(len(lbl_grupo)))
axes[1].set_xticklabels(lbl_grupo, fontsize=8)
axes[1].set_title('Contribución por Grupo de Features', fontsize=11, fontweight='bold')
axes[1].set_ylabel('|SHAP| agregado medio (z-score)')
for bar, v in zip(bars, vals_grupo):
    axes[1].text(bar.get_x() + bar.get_width()/2, v + 0.0002,
                 f'{v:.4f}', ha='center', fontsize=8, fontweight='bold')

plt.tight_layout()
plt.savefig(SHAP_DIR / 'shap_canal_comparativa.png', dpi=150, bbox_inches='tight')
plt.show()
print('Guardado -> shap_canal_comparativa.png')
"""))

# ── 11. Análisis local: meses de shock ────────────────────────────────────────
cells.append(md(19, """\
## 7. Análisis Local — Meses de Shock

Waterfall plots para los 3 meses del período de prueba con mayor error absoluto.
Muestra qué features empujaron la predicción hacia arriba (+) o abajo (−).
"""))

cells.append(code(20, """\
# Cargar predicciones reales del GE
df_preds = pd.read_csv(PREDS_PATH, parse_dates=['fecha'])
df_preds['mes_label'] = df_preds['fecha'].dt.strftime('%b %Y')

print('Predicciones GE por mes (test):')
print(df_preds[['fecha','real','prediccion','error_abs']].round(4).to_string(index=False))
print()

# Identificar los 3 peores meses
shock_idx = np.argsort(df_preds['error_abs'].values)[::-1][:3]
print(f'Top 3 meses con mayor error:')
for i, idx in enumerate(shock_idx, 1):
    row = df_preds.iloc[idx]
    print(f'  {i}. {row.mes_label}  |  real={row.real:.4f}  pred={row.prediccion:.4f}  err={row.error_abs:.4f}')
"""))

cells.append(code(21, """\
def waterfall_plot(ax, shap_vals, feat_names, feat_labels, base_val, pred_val,
                   mes_label, y_real, top_n=15):
    # Top features por |SHAP|
    idx_ord  = np.argsort(np.abs(shap_vals))[::-1][:top_n]
    sv_plot  = shap_vals[idx_ord]
    lbl_plot = [feat_labels.get(feat_names[i], feat_names[i]) for i in idx_ord]
    colors   = ['#2196F3' if v >= 0 else '#F44336' for v in sv_plot]

    # Gráfico de barras horizontal (waterfall simplificado)
    y_pos = range(top_n)
    ax.barh(y_pos, sv_plot, color=colors, alpha=0.85, edgecolor='white', height=0.7)
    ax.set_yticks(y_pos)
    ax.set_yticklabels(lbl_plot[::-1] if False else lbl_plot, fontsize=8)
    ax.invert_yaxis()
    ax.axvline(0, color='black', lw=0.8)
    ax.set_xlabel('Valor SHAP (z-score)', fontsize=8)
    ax.set_title(
        f'{mes_label}\\nreal={y_real:.4f}  base={base_val:.4f}  '
        f'total_SHAP={shap_vals.sum():.4f}',
        fontsize=9, fontweight='bold')
    ax.grid(axis='x', alpha=0.3)

    # Valores sobre barras
    for i, v in enumerate(sv_plot):
        offset = max(abs(sv_plot)) * 0.02
        ax.text(v + (offset if v >= 0 else -offset), i,
                f'{v:+.4f}', va='center', fontsize=6.5,
                ha='left' if v >= 0 else 'right')


base_val = float(explainer.expected_value)
fig, axes = plt.subplots(1, 3, figsize=(18, 7))

for col, s_idx in enumerate(shock_idx):
    row = df_preds.iloc[s_idx]
    waterfall_plot(
        axes[col],
        shap_agg[s_idx],
        FEAT_NAMES, FEAT_LABELS,
        base_val,
        pred_val=float(row.prediccion),
        mes_label=row.mes_label,
        y_real=float(row.real),
        top_n=12,
    )

plt.suptitle('Análisis Local SHAP — Top 3 meses de mayor error (GE)',
             fontsize=12, fontweight='bold', y=1.01)
plt.tight_layout()
plt.savefig(SHAP_DIR / 'shap_waterfall_shock.png', dpi=150, bbox_inches='tight')
plt.show()
print('Guardado -> shap_waterfall_shock.png')
"""))

# ── 12. Heatmap temporal ──────────────────────────────────────────────────────
cells.append(md(22, "## 8. Heatmap Temporal — Evolución de Importancia por Mes"))

cells.append(code(23, """\
# Ordenar features por importancia global
top_heat = 15
top_heat_idx = rank_idx[:top_heat]
sv_heat  = shap_agg[:, top_heat_idx].T   # (15, 10)
lbl_heat = [FEAT_LABELS.get(FEAT_NAMES[i], FEAT_NAMES[i]) for i in top_heat_idx]
mes_heat = df_preds['mes_label'].tolist()

fig, ax = plt.subplots(figsize=(13, 7))

im = ax.imshow(sv_heat, aspect='auto', cmap='RdBu_r',
               vmin=-np.abs(sv_heat).max(), vmax=np.abs(sv_heat).max())

ax.set_xticks(range(n_test))
ax.set_xticklabels(mes_heat, rotation=45, ha='right', fontsize=9)
ax.set_yticks(range(top_heat))
ax.set_yticklabels(lbl_heat, fontsize=9)

# Valores en cada celda
for i in range(top_heat):
    for j in range(n_test):
        v = sv_heat[i, j]
        ax.text(j, i, f'{v:+.3f}', ha='center', va='center',
                fontsize=6.5,
                color='white' if abs(v) > np.abs(sv_heat).max() * 0.55 else 'black')

plt.colorbar(im, ax=ax, label='Valor SHAP (z-score)', fraction=0.03, pad=0.01)
ax.set_title('Heatmap SHAP — Top 15 features × 10 meses de prueba\\n'
             '(azul=reduce pred., rojo=aumenta pred.)',
             fontsize=11, fontweight='bold')

plt.tight_layout()
plt.savefig(SHAP_DIR / 'shap_heatmap_temporal.png', dpi=150, bbox_inches='tight')
plt.show()
print('Guardado -> shap_heatmap_temporal.png')
"""))

# ── 13. Dependence plots (top 3) ──────────────────────────────────────────────
cells.append(md(24, "## 9. Dependence Plots — Top 3 Features"))

cells.append(code(25, """\
# Para cada una de las 3 features más importantes: SHAP vs valor de la feature
top3 = rank_idx[:3]
fig, axes = plt.subplots(1, 3, figsize=(14, 4))

for col, feat_idx in enumerate(top3):
    ax    = axes[col]
    name  = FEAT_NAMES[feat_idx]
    label = FEAT_LABELS.get(name, name)
    sv    = shap_agg[:, feat_idx]
    fv    = X_test_agg[:, feat_idx]

    scatter = ax.scatter(fv, sv, c=sv, cmap='RdBu_r', s=80,
                         vmin=-max(abs(sv)), vmax=max(abs(sv)),
                         edgecolors='white', linewidths=0.5, zorder=3)
    ax.axhline(0, color='black', lw=0.8, ls='--', alpha=0.5)

    # Etiquetas de mes en cada punto
    for k, (x_pt, y_pt, mes) in enumerate(zip(fv, sv, mes_heat)):
        ax.annotate(mes, (x_pt, y_pt), fontsize=6,
                    xytext=(3, 3), textcoords='offset points', alpha=0.8)

    ax.set_xlabel(f'{label} (z-score)', fontsize=9)
    ax.set_ylabel('SHAP value', fontsize=9)
    ax.set_title(f'Dependence: {label}\\n(rank #{col+1})', fontsize=10, fontweight='bold')
    plt.colorbar(scatter, ax=ax, fraction=0.05, pad=0.02)

plt.tight_layout()
plt.savefig(SHAP_DIR / 'shap_dependence_top3.png', dpi=150, bbox_inches='tight')
plt.show()
print('Guardado -> shap_dependence_top3.png')
"""))

# ── 14. Guardar JSON ──────────────────────────────────────────────────────────
cells.append(md(26, "## 10. Persistencia — Resultados SHAP"))

cells.append(code(27, """\
# JSON con ranking de importancia global
importance_dict = {
    'modelo'       : 'GE_DualLSTM_BahdanauAttention',
    'metodo_shap'  : 'KernelExplainer',
    'n_background' : 10,
    'nsamples'     : 200,
    'n_test'       : n_test,
    'base_value'   : round(float(explainer.expected_value), 6),
    'periodo_test' : f'{test_df.index.min().date()} / {test_df.index.max().date()}',
    'ranking_global': [
        {
            'rank'      : int(i+1),
            'feature'   : FEAT_NAMES[idx],
            'label'     : FEAT_LABELS.get(FEAT_NAMES[idx], FEAT_NAMES[idx]),
            'importance': round(float(importance_global[idx]), 6),
        }
        for i, idx in enumerate(rank_idx)
    ],
    'shap_por_mes': [
        {
            'mes'       : df_preds.iloc[i]['mes_label'],
            'error_abs' : round(float(df_preds.iloc[i]['error_abs']), 6),
            'shap_top5' : {
                FEAT_NAMES[j]: round(float(shap_agg[i, j]), 6)
                for j in rank_idx[:5]
            },
        }
        for i in range(n_test)
    ],
}

out_json = SHAP_DIR / 'shap_resultados.json'
with open(out_json, 'w', encoding='utf-8') as f:
    json.dump(importance_dict, f, indent=2, ensure_ascii=False)

# Guardar shap_values completos como numpy
np.save(SHAP_DIR / 'shap_values_agg.npy',   shap_agg)
np.save(SHAP_DIR / 'shap_values_raw.npy',   shap_arr)
np.save(SHAP_DIR / 'X_test_agg.npy',        X_test_agg)

print('Archivos guardados en resultados/shap/:')
for p in sorted(SHAP_DIR.iterdir()):
    print(f'  {p.name:<45} {p.stat().st_size / 1024:>7.1f} KB')
"""))

# ── 15. Conclusiones ─────────────────────────────────────────────────────────
cells.append(md(28, """\
## 11. Conclusiones del Análisis SHAP

### Guía de interpretación

| Hallazgo | Significado |
|---|---|
| Lags (`lag_1`, `lag_3`, `lag_6`) dominan el ranking | El modelo se basa principalmente en la inercia de la producción reciente |
| Variables climáticas con SHAP alto | NASA POWER captura condiciones que afectan la cosecha 1-2 meses antes |
| INDECI con SHAP bajo | Eventos de desastre son poco frecuentes en el período de prueba → señal débil |
| `precio_chacra_kg` con SHAP positivo en meses de caída | Caída de precio precede a caída de producción (o viceversa) |
| SHAP negativo en meses de shock | Features empujando la predicción lejos del real → caída no anticipada |

### Implicaciones para la Fase 4

1. **Actividad 16 completada** — SHAP muestra los drivers del modelo GE
2. **Actividad 17** — Dashboard Streamlit puede mostrar:
   - Predicción con intervalo de confianza
   - Top 5 features SHAP del último mes
   - Señal de alerta si SHAP de clima < umbral

### Archivos generados

```
resultados/shap/
├── shap_summary_bar.png          Importancia global (barras)
├── shap_beeswarm.png             Distribución beeswarm (top 15)
├── shap_canal_comparativa.png    Canal A vs Canal B vs grupos
├── shap_waterfall_shock.png      Waterfall de los 3 meses con mayor error
├── shap_heatmap_temporal.png     Heatmap features × meses de prueba
├── shap_dependence_top3.png      Dependence plots de top 3 features
├── shap_resultados.json          Ranking + valores por mes (JSON)
├── shap_values_agg.npy           SHAP agregados (10, 24)
├── shap_values_raw.npy           SHAP crudos (10, 144)
└── X_test_agg.npy                Feature values test agregados (10, 24)
```
"""))


# ── Construir notebook ────────────────────────────────────────────────────────
notebook = {
    'nbformat': 4,
    'nbformat_minor': 5,
    'metadata': {
        'kernelspec': {
            'display_name': 'Python 3 (ipykernel)',
            'language': 'python',
            'name': 'python3',
        },
        'language_info': {
            'codemirror_mode': {'name': 'ipython', 'version': 3},
            'file_extension': '.py',
            'mimetype': 'text/x-python',
            'name': 'python',
            'version': '3.11.9',
        },
    },
    'cells': cells,
}

with open(OUT, 'w', encoding='utf-8') as f:
    json.dump(notebook, f, indent=1, ensure_ascii=False)

size_kb = OUT.stat().st_size / 1024
print(f'Notebook generado : {OUT}')
print(f'  Celdas          : {len(cells)}')
print(f'  Tamanio         : {size_kb:.1f} KB')
