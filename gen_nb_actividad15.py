#!/usr/bin/env python3
"""Genera notebooks/fase4/actividad_15_multimodal_nlp.ipynb"""
import json
from pathlib import Path

ROOT = Path('C:/Machine-learming/Machine-Learning-Multimodal--Agro-NLP-Clima-')
OUT  = ROOT / 'notebooks' / 'fase4' / 'actividad_15_multimodal_nlp.ipynb'
OUT.parent.mkdir(parents=True, exist_ok=True)


def md(n, src):
    return {'cell_type': 'markdown', 'id': f'a15-{n:04d}', 'metadata': {}, 'source': src}


def code(n, src):
    return {
        'cell_type': 'code', 'execution_count': None,
        'id': f'a15-{n:04d}', 'metadata': {}, 'outputs': [], 'source': src,
    }


# ─────────────────────────────────────────────────────────────────
cells = []

# 1 ── Título
cells.append(md(1, """\
# Actividad 15 — GM: Modelo Multimodal con NLP (BETO + Dual-LSTM Attention)

**Fase 4 — Grupo Multimodal (GM)**
**Proyecto:** Predicción de Demanda Agroindustrial — Limón Peruano
**Referencia:** Gu et al. (2022) + integración NLP (BETO, Fase 2)

## Hipótesis

El sentimiento de noticias agrícolas (`avg_sentiment` vía BETO) captura shocks externos
—plagas, políticas de precios, desastres mediáticos— que las variables físicas no detectan.
Añadirlo al Canal B debería mejorar el R² negativo del modelo GE.

## Diferencia clave respecto a GE (Actividad 14)

| Componente | GE (Act. 14) | **GM (Act. 15)** |
|---|---|---|
| Canal A | `produccion_t` | `produccion_t` |
| Canal B | 23 vars | **25 vars** (`+avg_sentiment`, `+n_noticias_beto`) |
| NLP (BETO) | ❌ ausente | ✅ fusionado |

## Arquitectura

```
Canal A (produccion_t)        Canal B (25 features + NLP)
shape (6, 1)                  shape (6, 25)
     │                              │
LSTM-64 (L2=0.001)           LSTM-64 (L2=0.001)
     │ hidden states (6,64)        │ hidden states (6,64)
Bahdanau Attention A          Bahdanau Attention B
context_a (64,)               context_b (64,)
     └──────── Concat (128,) ──────┘
                    │
              Dense-64 + ReLU
              Dropout-0.15
              Dense-16 + ReLU
              Dense-1 (output)
```
"""))

# 2 ── Imports
cells.append(code(2, """\
import os, platform, sys
os.environ['TF_CPP_MIN_LOG_LEVEL']  = '3'
os.environ['TF_ENABLE_ONEDNN_OPTS'] = '0'

import warnings
warnings.filterwarnings('ignore')

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import seaborn as sns
import json, joblib
from pathlib import Path

import tensorflow as tf
tf.get_logger().setLevel('ERROR')
from tensorflow import keras
from tensorflow.keras import layers, regularizers, callbacks

from sklearn.preprocessing import StandardScaler
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

sns.set_theme(style='whitegrid', palette='muted')
plt.rcParams['figure.facecolor'] = 'white'

print(f'Python      : {sys.version.split()[0]}')
print(f'TensorFlow  : {tf.__version__}')
print(f'Keras       : {keras.__version__}')
print(f'NumPy       : {np.__version__}')
print(f'Pandas      : {pd.__version__}')
"""))

# 3 ── GPU
cells.append(code(3, """\
gpus = tf.config.list_physical_devices('GPU')
if gpus:
    for g in gpus:
        tf.config.experimental.set_memory_growth(g, True)
    print(f'GPU detectada: {len(gpus)} dispositivo(s)')
else:
    print('GPU: no detectada — entrenamiento en CPU')
print(f'Dispositivo TF: {"GPU" if gpus else "CPU"}')
"""))

# 4 ── Rutas + hiperparámetros
cells.append(code(4, """\
def detect_project_root() -> Path:
    if platform.system() == 'Windows':
        p = Path('C:/Machine-learming/Machine-Learning-Multimodal--Agro-NLP-Clima-')
        if p.exists():
            return p
    for candidate in [
        Path('/mnt/c/Machine-learming/Machine-Learning-Multimodal--Agro-NLP-Clima-'),
        Path.home() / 'Machine-learming' / 'Machine-Learning-Multimodal--Agro-NLP-Clima-',
    ]:
        if candidate.exists():
            return candidate
    raise FileNotFoundError('No se encontro la raiz del proyecto.')

PROYECTO_ROOT    = detect_project_root()
DATASET_PATH     = PROYECTO_ROOT / 'data' / 'processed' / 'master_dataset_fase2_multivariado.csv'
SENTIMIENTO_PATH = (PROYECTO_ROOT / 'notebooks' / 'fase2' / 'output'
                    / '01_nlp_sentimiento' / 'sentimiento_mensual.csv')
RESULTADOS_DIR   = PROYECTO_ROOT / 'resultados' / 'gm'
GE_METRICAS_PATH = PROYECTO_ROOT / 'resultados' / 'ge' / 'ge_metricas.json'
RESULTADOS_DIR.mkdir(parents=True, exist_ok=True)

# Hiperparámetros — idénticos a GE para comparación justa
SPLIT_RATIO   = 0.80
SEQ_LEN       = 6
LSTM_UNITS    = 64
ATTN_UNITS    = 64
DROPOUT       = 0.30
L2_REG        = 0.001
LEARNING_RATE = 1e-3
EPOCHS        = 300
BATCH_SIZE    = 8
PATIENCE      = 15
RANDOM_STATE  = 42

# Canal B: 23 originales (GE) + 2 NLP (BETO) = 25 features
CANAL_B_COLS = [
    'lag_1', 'lag_3', 'lag_6',
    'precio_chacra_kg',
    'num_emergencias', 'total_afectados', 'hectareas_cultivo_perdidas',
    'ALLSKY_SFC_SW_DWN', 'PRECTOTCORR', 'QV2M', 'RH2M',
    'T2M', 'T2M_MAX', 'T2M_MIN', 'WS2M',
    'lat', 'lon',
    'month_sin', 'month_cos', 'mes_num',
    'trimestre_num', 'trimestre_sin', 'trimestre_cos',
    'avg_sentiment',    # BETO — sentimiento promedio mensual
    'n_noticias_beto',  # BETO — cobertura de noticias
]
N_FEATURES_A   = 1
N_FEATURES_B   = len(CANAL_B_COLS)   # 25
TOTAL_FEATURES = N_FEATURES_A + N_FEATURES_B  # 26

tf.random.set_seed(RANDOM_STATE)
np.random.seed(RANDOM_STATE)

print(f'Proyecto        : {PROYECTO_ROOT}')
print(f'Dataset maestro : {DATASET_PATH.name}  existe={DATASET_PATH.exists()}')
print(f'Sentimiento NLP : {SENTIMIENTO_PATH.name}  existe={SENTIMIENTO_PATH.exists()}')
print(f'Resultados      : {RESULTADOS_DIR}')
print(f'Canal A : {N_FEATURES_A} feature  (produccion_t)')
print(f'Canal B : {N_FEATURES_B} features ({N_FEATURES_B - 2} originales + 2 NLP)')
print(f'Total   : {TOTAL_FEATURES} features  seq_len={SEQ_LEN}')
"""))

# 5 ── Sección 1
cells.append(md(5, "## 1. Carga y exploración de fuentes de datos"))

# 6 ── Carga master dataset
cells.append(code(6, """\
df_raw = pd.read_csv(DATASET_PATH, parse_dates=['fecha_evento'])
print(f'Master dataset  : {df_raw.shape[0]:,} filas x {df_raw.shape[1]} columnas')
print(f'Columnas        : {list(df_raw.columns)}')
print(f'Rango temporal  : {df_raw.fecha_evento.min().date()} -> {df_raw.fecha_evento.max().date()}')
print(f'Provincias      : {df_raw.provincia.nunique()}  Departamentos: {df_raw.departamento.nunique()}')
"""))

# 7 ── Carga sentimiento
cells.append(code(7, """\
df_sent = pd.read_csv(SENTIMIENTO_PATH)
df_sent['fecha_evento'] = pd.to_datetime(
    df_sent['fecha_evento'].astype(str) + '-01', format='%Y-%m-%d'
)
df_sent = df_sent[['fecha_evento', 'avg_sentiment', 'n_noticias_beto']].copy()

print(f'Sentimiento BETO : {df_sent.shape[0]} meses con noticias')
print(f'Rango            : {df_sent.fecha_evento.min().date()} -> {df_sent.fecha_evento.max().date()}')
print(f'avg_sentiment    : min={df_sent.avg_sentiment.min():.4f}  max={df_sent.avg_sentiment.max():.4f}  mean={df_sent.avg_sentiment.mean():.4f}')
print(f'n_noticias_beto  : min={df_sent.n_noticias_beto.min():.0f}  max={df_sent.n_noticias_beto.max():.0f}  mean={df_sent.n_noticias_beto.mean():.1f}')
print()
print(df_sent.to_string(index=False))
"""))

# 8 ── EDA sentimiento
cells.append(code(8, """\
fig, axes = plt.subplots(2, 1, figsize=(13, 6), sharex=True)

col = df_sent['avg_sentiment'].apply(lambda v: 'steelblue' if v >= 0 else 'tomato')
axes[0].bar(df_sent['fecha_evento'], df_sent['avg_sentiment'],
            color=col, alpha=0.8, width=20)
axes[0].axhline(0, color='black', lw=0.8)
axes[0].set_title('Sentimiento BETO mensual — Agraria.pe (limón peruano)', fontsize=11)
axes[0].set_ylabel('avg_sentiment')

axes[1].bar(df_sent['fecha_evento'], df_sent['n_noticias_beto'],
            color='steelblue', alpha=0.7, width=20)
axes[1].set_title('Cobertura de noticias por mes', fontsize=11)
axes[1].set_ylabel('n_noticias_beto')
axes[1].set_xlabel('Fecha')

for ax in axes:
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
    ax.tick_params(axis='x', rotation=45)

plt.tight_layout()
plt.savefig(RESULTADOS_DIR / 'gm_sentimiento_eda.png', dpi=150, bbox_inches='tight')
plt.show()
print('EDA guardado -> gm_sentimiento_eda.png')
"""))

# 9 ── Sección 2
cells.append(md(9, """\
## 2. Agregación nacional y fusión NLP

Protocolo:
1. Agregación provincial `mean()` — mismo criterio GC1/GC2/GE
2. `join` con `sentimiento_mensual` por `fecha_evento`
3. Meses sin noticias → `avg_sentiment=0`, `n_noticias_beto=0` (señal neutra)
4. Lags explícitos y eliminación de NaN
"""))

# 10 ── Aggregate + join + lags
cells.append(code(10, """\
AGG_COLS = [
    'produccion_t', 'precio_chacra_kg',
    'num_emergencias', 'total_afectados', 'hectareas_cultivo_perdidas',
    'ALLSKY_SFC_SW_DWN', 'PRECTOTCORR', 'QV2M', 'RH2M',
    'T2M', 'T2M_MAX', 'T2M_MIN', 'WS2M',
    'lat', 'lon',
    'month_sin', 'month_cos', 'mes_num',
    'trimestre_num', 'trimestre_sin', 'trimestre_cos',
]

nacional = (
    df_raw.groupby('fecha_evento')[AGG_COLS]
    .mean()
    .sort_index()
)
nacional.index = pd.DatetimeIndex(nacional.index, freq='MS')

# Fusionar sentimiento NLP
sent_idx = df_sent.set_index('fecha_evento')[['avg_sentiment', 'n_noticias_beto']]
nacional = nacional.join(sent_idx, how='left')

n_sin_noticias = int(nacional['avg_sentiment'].isna().sum())
nacional['avg_sentiment']   = nacional['avg_sentiment'].fillna(0.0)
nacional['n_noticias_beto'] = nacional['n_noticias_beto'].fillna(0.0)

# Lags de produccion_t
nacional['lag_1'] = nacional['produccion_t'].shift(1)
nacional['lag_3'] = nacional['produccion_t'].shift(3)
nacional['lag_6'] = nacional['produccion_t'].shift(6)
nacional = nacional.dropna()

print(f'Serie nacional (tras lags) : {nacional.shape}')
print(f'Periodo : {nacional.index.min().date()} -> {nacional.index.max().date()}')
print(f'Meses sin noticias (NLP=0) : {n_sin_noticias}')
print()
print(f'avg_sentiment   : mean={nacional.avg_sentiment.mean():.4f}  std={nacional.avg_sentiment.std():.4f}')
print(f'n_noticias_beto : mean={nacional.n_noticias_beto.mean():.2f}  max={nacional.n_noticias_beto.max():.0f}')
print(f'Canal B completo: {all(c in nacional.columns for c in CANAL_B_COLS)}')
nacional[['produccion_t', 'avg_sentiment', 'n_noticias_beto']].tail(6)
"""))

# 11 ── Sección 3
cells.append(md(11, "## 3. Split cronológico 80/20"))

# 12 ── Split
cells.append(code(12, """\
n_total = len(nacional)
n_train = int(n_total * SPLIT_RATIO)
n_test  = n_total - n_train

train_df   = nacional.iloc[:n_train].copy()
test_df    = nacional.iloc[n_train:].copy()
split_date = nacional.index[n_train]

y_train = train_df['produccion_t'].values
y_test  = test_df['produccion_t'].values
B_train = train_df[CANAL_B_COLS].values
B_test  = test_df[CANAL_B_COLS].values

print('=' * 60)
print(f'  Total      : {n_total} meses (post-lags)')
print(f'  Train (80%): {n_train} meses  [{train_df.index.min().date()} -> {train_df.index.max().date()}]')
print(f'  Test  (20%): {n_test} meses  [{test_df.index.min().date()} -> {test_df.index.max().date()}]')
print(f'  Corte      : {split_date.date()}')
print('=' * 60)

fig, ax = plt.subplots(figsize=(13, 4))
ax.plot(train_df.index, y_train, label=f'Train ({n_train}m)', color='steelblue', lw=1.8)
ax.plot(test_df.index,  y_test,  label=f'Test ({n_test}m)',   color='darkorange', lw=1.8)
ax.axvline(split_date, color='gray', ls='--', lw=1.4, label=f'Corte {split_date.date()}')
ax.set_title('Split Cronológico 80/20 — GM Multimodal NLP', fontsize=12)
ax.set_ylabel('produccion_t (z-score, media provincial)')
ax.legend()
ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
ax.tick_params(axis='x', rotation=45)
plt.tight_layout()
plt.savefig(RESULTADOS_DIR / 'gm_split_cronologico.png', dpi=150, bbox_inches='tight')
plt.show()
"""))

# 13 ── Sección 4
cells.append(md(13, "## 4. Normalización — fit exclusivo sobre entrenamiento"))

# 14 ── Normalizar
cells.append(code(14, """\
scaler_a = StandardScaler()
y_train_sc = scaler_a.fit_transform(y_train.reshape(-1, 1)).flatten()
y_test_sc  = scaler_a.transform(y_test.reshape(-1, 1)).flatten()

scaler_b = StandardScaler()
B_train_sc = scaler_b.fit_transform(B_train).astype(np.float32)
B_test_sc  = scaler_b.transform(B_test).astype(np.float32)

idx_lag1 = CANAL_B_COLS.index('lag_1')
idx_lag3 = CANAL_B_COLS.index('lag_3')
idx_lag6 = CANAL_B_COLS.index('lag_6')

print('Normalización completada (fit SOLO sobre train).')
print(f'  y_train_sc : mean={y_train_sc.mean():.4f}  std={y_train_sc.std():.4f}')
print(f'  y_test_sc  : mean={y_test_sc.mean():.4f}   std={y_test_sc.std():.4f}')
print(f'  B_train_sc : {B_train_sc.shape}')
print(f'  B_test_sc  : {B_test_sc.shape}')
print(f'Índices lags: lag_1={idx_lag1}  lag_3={idx_lag3}  lag_6={idx_lag6}')
print('avg_sentiment y n_noticias_beto son exógenos observados (sin update recursivo)')
"""))

# 15 ── Sección 5
cells.append(md(15, "## 5. Secuencias Dual Canal"))

# 16 ── Secuencias
cells.append(code(16, """\
def make_dual_sequences(target_arr, exog_arr, seq_len):
    Xa, Xb, Y = [], [], []
    for i in range(len(target_arr) - seq_len):
        Xa.append(target_arr[i : i + seq_len].reshape(-1, 1))
        Xb.append(exog_arr[i : i + seq_len])
        Y.append(target_arr[i + seq_len])
    return (
        np.array(Xa, dtype=np.float32),
        np.array(Xb, dtype=np.float32),
        np.array(Y,  dtype=np.float32),
    )


Xa_train, Xb_train, y_seq = make_dual_sequences(y_train_sc, B_train_sc, SEQ_LEN)

n_seq = len(Xa_train)
n_val = max(3, int(n_seq * 0.15))
n_tr  = n_seq - n_val

Xa_tr, Xa_val = Xa_train[:n_tr], Xa_train[n_tr:]
Xb_tr, Xb_val = Xb_train[:n_tr], Xb_train[n_tr:]
y_tr,  y_val  = y_seq[:n_tr],    y_seq[n_tr:]

print(f'Secuencias totales : {n_seq}')
print(f'  Train            : {n_tr}   Xa={Xa_tr.shape}  Xb={Xb_tr.shape}')
print(f'  Validación       : {n_val}  Xa={Xa_val.shape}  Xb={Xb_val.shape}')
print(f'  Canal B shape    : ({SEQ_LEN}, {N_FEATURES_B})  <- 25 features (23 + 2 NLP)')
"""))

# 17 ── Sección 6
cells.append(md(17, """\
## 6. Arquitectura: BahdanauAttention + Dual LSTM

Arquitectura idéntica a GE (Actividad 14). Único cambio estructural: Canal B tiene 25 features.
"""))

# 18 ── BahdanauAttention
cells.append(code(18, """\
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

print('BahdanauAttention definida.')
"""))

# 19 ── Build model
cells.append(code(19, """\
def build_dual_lstm_attention(seq_len, n_feat_a, n_feat_b,
                               lstm_units=64, attn_units=64,
                               dropout=0.30, l2_reg=0.001, lr=1e-3):
    reg = regularizers.l2(l2_reg)

    inp_a = keras.Input(shape=(seq_len, n_feat_a), name='canal_a')
    h_a   = layers.LSTM(lstm_units, return_sequences=True,
                        kernel_regularizer=reg, recurrent_regularizer=reg,
                        name='lstm_a')(inp_a)
    h_a   = layers.Dropout(dropout, name='drop_lstm_a')(h_a)
    q_a   = h_a[:, -1, :]
    ctx_a, alpha_a = BahdanauAttention(attn_units, name='attn_a')(q_a, h_a)

    inp_b = keras.Input(shape=(seq_len, n_feat_b), name='canal_b')
    h_b   = layers.LSTM(lstm_units, return_sequences=True,
                        kernel_regularizer=reg, recurrent_regularizer=reg,
                        name='lstm_b')(inp_b)
    h_b   = layers.Dropout(dropout, name='drop_lstm_b')(h_b)
    q_b   = h_b[:, -1, :]
    ctx_b, alpha_b = BahdanauAttention(attn_units, name='attn_b')(q_b, h_b)

    merged = layers.Concatenate(name='fusion')([ctx_a, ctx_b])
    x      = layers.Dense(64, activation='relu', kernel_regularizer=reg, name='dense_1')(merged)
    x      = layers.Dropout(dropout / 2, name='drop_head')(x)
    x      = layers.Dense(16, activation='relu', name='dense_2')(x)
    output = layers.Dense(1, name='output')(x)

    model = keras.Model(inputs=[inp_a, inp_b], outputs=output,
                        name='GM_DualLSTM_BahdanauAttention_NLP')
    model.compile(optimizer=keras.optimizers.Adam(learning_rate=lr),
                  loss='mse', metrics=['mae'])

    model_attn = keras.Model(inputs=[inp_a, inp_b],
                             outputs=[output, alpha_a, alpha_b],
                             name='GM_Viz')
    return model, model_attn


model, model_attn = build_dual_lstm_attention(
    seq_len=SEQ_LEN, n_feat_a=N_FEATURES_A, n_feat_b=N_FEATURES_B,
    lstm_units=LSTM_UNITS, attn_units=ATTN_UNITS,
    dropout=DROPOUT, l2_reg=L2_REG, lr=LEARNING_RATE,
)
model.summary()
"""))

# 20 ── Sección 7
cells.append(md(20, "## 7. Entrenamiento"))

# 21 ── Fit
cells.append(code(21, """\
cb_es   = callbacks.EarlyStopping(monitor='val_loss', patience=PATIENCE,
                                  restore_best_weights=True, verbose=1)
cb_rlr  = callbacks.ReduceLROnPlateau(monitor='val_loss', factor=0.5,
                                      patience=8, min_lr=1e-6, verbose=0)
cb_ckpt = callbacks.ModelCheckpoint(
    filepath=str(RESULTADOS_DIR / 'gm_best_checkpoint.keras'),
    monitor='val_loss', save_best_only=True, verbose=0)

print('Entrenando GM Dual-LSTM Attention + NLP...')
print(f'  Train: {n_tr} secuencias | Val: {n_val} | Epochs max: {EPOCHS} | Patience: {PATIENCE}')
print()

history = model.fit(
    [Xa_tr, Xb_tr], y_tr,
    validation_data=([Xa_val, Xb_val], y_val),
    epochs=EPOCHS, batch_size=BATCH_SIZE,
    callbacks=[cb_es, cb_rlr, cb_ckpt],
    verbose=0,
)

n_ep     = len(history.history['loss'])
best_val = min(history.history['val_loss'])
print(f'Detenido en época {n_ep}.  Mejor val_loss={best_val:.6f}')
"""))

# 22 ── Training curves
cells.append(code(22, """\
fig, axes = plt.subplots(1, 2, figsize=(13, 4))

axes[0].plot(history.history['loss'],     label='Train MSE', color='steelblue', lw=1.8)
axes[0].plot(history.history['val_loss'], label='Val MSE',   color='darkorange', lw=1.8)
axes[0].set_title('Pérdida MSE', fontsize=11)
axes[0].set_xlabel('Época'); axes[0].set_ylabel('MSE'); axes[0].legend()

axes[1].plot(history.history['mae'],     label='Train MAE', color='steelblue', lw=1.8)
axes[1].plot(history.history['val_mae'], label='Val MAE',   color='darkorange', lw=1.8)
axes[1].set_title('MAE', fontsize=11)
axes[1].set_xlabel('Época'); axes[1].set_ylabel('MAE'); axes[1].legend()

plt.suptitle(f'GM Dual-LSTM + NLP — {n_ep} épocas', fontsize=12)
plt.tight_layout()
plt.savefig(RESULTADOS_DIR / 'gm_training_curve.png', dpi=150, bbox_inches='tight')
plt.show()
"""))

# 23 ── Sección 8
cells.append(md(23, """\
## 8. Predicciones multi-step (sin data leakage)

- **Canal A**: predicción recursiva (usa predicciones previas del modelo)
- **Canal B**: exógenas observadas del período de prueba
- `lag_1/3/6`: actualizados con valores predichos
- `avg_sentiment`, `n_noticias_beto`: exógenos conocidos, tomados de `B_test_sc` sin modificación
"""))

# 24 ── Prediction loop
cells.append(code(24, """\
print(f'Generando {n_test} predicciones multi-step...')

canal_a_window = y_train_sc[-SEQ_LEN:].copy()
pred_buffer_sc = list(y_train_sc[-6:])

preds_sc = []
for step in range(n_test):
    x_a = canal_a_window.reshape(1, SEQ_LEN, 1).astype(np.float32)

    start_b = max(0, step - SEQ_LEN + 1)
    b_avail = B_test_sc[start_b : step + 1]
    if len(b_avail) < SEQ_LEN:
        pad_b    = np.tile(B_train_sc[-1], (SEQ_LEN - len(b_avail), 1))
        b_window = np.vstack([pad_b, b_avail]).copy()
    else:
        b_window = b_avail.copy()

    for t in range(SEQ_LEN):
        def get_pred(off, buf=pred_buffer_sc):
            pos = len(buf) - off
            return buf[pos] if pos >= 0 else 0.0
        b_window[t, idx_lag1] = get_pred(1)
        b_window[t, idx_lag3] = get_pred(3)
        b_window[t, idx_lag6] = get_pred(6)
    # avg_sentiment y n_noticias_beto ya están en b_window desde B_test_sc

    x_b     = b_window.reshape(1, SEQ_LEN, N_FEATURES_B).astype(np.float32)
    pred_sc = float(model.predict([x_a, x_b], verbose=0)[0, 0])
    preds_sc.append(pred_sc)
    pred_buffer_sc.append(pred_sc)
    canal_a_window = np.roll(canal_a_window, -1)
    canal_a_window[-1] = pred_sc

y_pred = scaler_a.inverse_transform(np.array(preds_sc).reshape(-1, 1)).flatten()

comparacion = pd.DataFrame({
    'fecha'      : test_df.index,
    'real'       : y_test,
    'prediccion' : y_pred,
    'error_abs'  : np.abs(y_test - y_pred),
    'sentiment'  : test_df['avg_sentiment'].values,
})
print(f'Predicciones: {len(y_pred)}')
print(comparacion.round(4).to_string(index=False))
"""))

# 25 ── Sección 9
cells.append(md(25, "## 9. Métricas de evaluación"))

# 26 ── Métricas
cells.append(code(26, """\
def safe_mape(y_true, y_pred, eps=1e-8):
    mask = np.abs(y_true) > eps
    if mask.sum() == 0:
        return float('nan')
    return float(np.mean(np.abs((y_true[mask] - y_pred[mask]) / y_true[mask])) * 100)


mae  = mean_absolute_error(y_test, y_pred)
rmse = float(np.sqrt(mean_squared_error(y_test, y_pred)))
mape = safe_mape(y_test, y_pred)
r2   = r2_score(y_test, y_pred)

metricas = {
    'modelo'        : 'GM_DualLSTM_BahdanauAttention_NLP',
    'referencia'    : 'Gu et al. (2022) + BETO NLP (Fase 2)',
    'arquitectura'  : f'DualInput-LSTM({LSTM_UNITS})-BahdanauAttn({ATTN_UNITS})+NLP',
    'seq_len'       : SEQ_LEN,
    'n_features_a'  : N_FEATURES_A,
    'n_features_b'  : N_FEATURES_B,
    'nlp_features'  : ['avg_sentiment', 'n_noticias_beto'],
    'total_features': TOTAL_FEATURES,
    'lstm_units'    : LSTM_UNITS,
    'attn_units'    : ATTN_UNITS,
    'dropout'       : DROPOUT,
    'l2_reg'        : L2_REG,
    'epochs_run'    : n_ep,
    'best_val_loss' : round(best_val, 6),
    'n_train'       : int(n_train),
    'n_test'        : int(n_test),
    'split_date'    : str(split_date.date()),
    'MAE'           : round(mae,  6),
    'RMSE'          : round(rmse, 6),
    'MAPE_pct'      : round(mape, 4) if mape == mape else None,
    'R2'            : round(r2,   6),
    'nota'          : 'metricas en z-score — media provincial (Fase 2)',
}

print('=' * 60)
print('  METRICAS GM — Dual-LSTM + NLP  (conjunto de prueba)')
print('=' * 60)
print(f'  MAE  : {mae:.6f}')
print(f'  RMSE : {rmse:.6f}')
print(f'  MAPE : {mape:.4f} %')
print(f'  R2   : {r2:.6f}')
print('=' * 60)
print('  Métricas en z-score (media provincial).')
"""))

# 27 ── Sección 10
cells.append(md(27, "## 10. Persistencia"))

# 28 ── Guardar
cells.append(code(28, """\
model.save(RESULTADOS_DIR / 'gm_dual_lstm_attn_nlp.keras')

joblib.dump({'scaler_a': scaler_a, 'scaler_b': scaler_b},
            RESULTADOS_DIR / 'gm_scalers.pkl')

with open(RESULTADOS_DIR / 'gm_metricas.json', 'w', encoding='utf-8') as f:
    json.dump(metricas, f, indent=2, ensure_ascii=False)

comparacion.to_csv(RESULTADOS_DIR / 'gm_predicciones.csv', index=False)
pd.DataFrame(history.history).to_csv(
    RESULTADOS_DIR / 'gm_training_history.csv', index=False)

print('Archivos en resultados/gm/:')
for p in sorted(RESULTADOS_DIR.iterdir()):
    print(f'  {p.name:<50} {p.stat().st_size / 1024:>7.1f} KB')
"""))

# 29 ── Sección 11
cells.append(md(29, "## 11. Gráficos de predicción"))

# 30 ── Plots
cells.append(code(30, """\
fig, axes = plt.subplots(2, 1, figsize=(14, 11))

axes[0].plot(train_df.index, y_train, label=f'Train ({n_train}m)',
             color='steelblue', lw=1.6, alpha=0.8)
axes[0].plot(test_df.index, y_test,  label='Real (test)',
             color='darkorange', lw=2)
axes[0].plot(test_df.index, y_pred,  label='GM Dual-LSTM+NLP',
             color='darkgreen', lw=2, ls='--')
axes[0].axvline(split_date, color='gray', ls=':', lw=1.5)
axes[0].set_title(
    f'GM — Dual-LSTM+NLP  |  MAE={mae:.4f}  RMSE={rmse:.4f}  R2={r2:.4f}',
    fontsize=12)
axes[0].set_ylabel('produccion_t (z-score, media provincial)')
axes[0].legend(fontsize=9)
axes[0].xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
axes[0].tick_params(axis='x', rotation=45)

axes[1].plot(test_df.index, y_test,  label='Real',
             color='darkorange', lw=2, marker='o', ms=5)
axes[1].plot(test_df.index, y_pred,  label='Predicción GM+NLP',
             color='darkgreen',  lw=2, marker='s', ms=5, ls='--')
axes[1].fill_between(test_df.index, y_pred - rmse, y_pred + rmse,
                     alpha=0.12, color='darkgreen', label='±RMSE')
axes[1].set_title(f'Zoom período de prueba — {n_test} meses', fontsize=12)
axes[1].set_ylabel('produccion_t (z-score)')
axes[1].legend(fontsize=9)
axes[1].xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
axes[1].tick_params(axis='x', rotation=45)

plt.tight_layout()
plt.savefig(RESULTADOS_DIR / 'gm_prediccion_vs_real.png', dpi=150, bbox_inches='tight')
plt.show()
print('Guardado -> gm_prediccion_vs_real.png')
"""))

# 31 ── Sección 12
cells.append(md(31, """\
## 12. Comparativa GM vs GE — Impacto del NLP

- **GE** (Act. 14): Canal B = 23 features, **sin NLP**
- **GM** (Act. 15): Canal B = 25 features, **con NLP (BETO)**
"""))

# 32 ── Comparativa GE vs GM
cells.append(code(32, """\
ge_met = {}
if GE_METRICAS_PATH.exists():
    with open(GE_METRICAS_PATH) as f:
        ge_met = json.load(f)

print('=' * 68)
print('  COMPARATIVA — IMPACTO DEL NLP (BETO)')
print('=' * 68)
print(f'  {"Modelo":<22} {"Canal B":<12} {"MAE":>10} {"RMSE":>10} {"R2":>10}')
print('  ' + '-' * 66)

if ge_met:
    print(f'  {"GE (sin NLP)":<22} {"23 feat":<12} {ge_met["MAE"]:>10.4f} {ge_met["RMSE"]:>10.4f} {ge_met["R2"]:>10.4f}')
print(f'  {"GM (con NLP)":<22} {"25 feat":<12} {mae:>10.4f} {rmse:>10.4f} {r2:>10.4f}')
print('=' * 68)

if ge_met:
    delta_mae  = (ge_met['MAE']  - mae)  / ge_met['MAE']  * 100
    delta_rmse = (ge_met['RMSE'] - rmse) / ge_met['RMSE'] * 100
    delta_r2   = r2 - ge_met['R2']
    print()
    print(f'  delta MAE  : {delta_mae:+.1f}%  ({"mejor" if delta_mae > 0 else "peor"})')
    print(f'  delta RMSE : {delta_rmse:+.1f}%  ({"mejor" if delta_rmse > 0 else "peor"})')
    print(f'  delta R2   : {delta_r2:+.4f}  ({"mejor" if delta_r2 > 0 else "peor"})')
    print()
    concl = 'MEJORA' if delta_mae > 0 else 'NO mejora (introduce ruido)'
    print(f'  Conclusión: el NLP {concl} el rendimiento del modelo.')
"""))

# 33 ── Bar chart comparativo
cells.append(code(33, """\
if ge_met:
    fig, axes = plt.subplots(1, 3, figsize=(13, 5))
    labels = ['GE (sin NLP)', 'GM (con NLP)']
    colors = ['crimson', 'darkgreen']

    for ax, vals, title, ylabel in zip(
        axes,
        [[ge_met['MAE'],  mae],  [ge_met['RMSE'], rmse], [ge_met['R2'],  r2]],
        ['MAE (menor = mejor)', 'RMSE (menor = mejor)', 'R2 (mayor = mejor)'],
        ['MAE (z-score)',       'RMSE (z-score)',        'R2'],
    ):
        bars = ax.bar(labels, vals, color=colors, alpha=0.85,
                      width=0.5, edgecolor='white', linewidth=0.8)
        for bar, v in zip(bars, vals):
            yoff = abs(v) * 0.04
            ax.text(bar.get_x() + bar.get_width() / 2,
                    v + yoff if v >= 0 else v - yoff * 2,
                    f'{v:.4f}', ha='center', va='bottom', fontsize=9, fontweight='bold')
        if 'R2' in title:
            ax.axhline(0, color='black', lw=0.8, alpha=0.5)
        ax.set_title(title, fontsize=10, fontweight='bold')
        ax.set_ylabel(ylabel)
        ax.tick_params(axis='x', rotation=10)

    plt.suptitle('Impacto del NLP (BETO) — GE vs GM', fontsize=12, fontweight='bold')
    plt.tight_layout()
    plt.savefig(RESULTADOS_DIR / 'gm_comparativa_ge_vs_gm.png', dpi=150, bbox_inches='tight')
    plt.show()
    print('Guardado -> gm_comparativa_ge_vs_gm.png')
"""))

# 34 ── Pesos de atención
cells.append(md(34, "## 13. Pesos de atención Bahdanau"))

# 35 ── Attention weights
cells.append(code(35, """\
_, alpha_a_tr, alpha_b_tr = model_attn.predict(
    [Xa_train, Xb_train], verbose=0, batch_size=32
)
alpha_a_mean = alpha_a_tr[:, :, 0].mean(axis=0)
alpha_b_mean = alpha_b_tr[:, :, 0].mean(axis=0)

fig, axes = plt.subplots(1, 2, figsize=(12, 4))
time_labels = [f't-{SEQ_LEN - i - 1}' if SEQ_LEN - i - 1 > 0 else 't'
               for i in range(SEQ_LEN)]

axes[0].bar(time_labels, alpha_a_mean, color='darkgreen', alpha=0.85)
axes[0].set_title('Canal A — produccion_t', fontsize=11)
axes[0].set_ylabel('Peso promedio (alpha)')

axes[1].bar(time_labels, alpha_b_mean, color='seagreen', alpha=0.85)
axes[1].set_title('Canal B — 23 vars + 2 NLP', fontsize=11)
axes[1].set_ylabel('Peso promedio (alpha)')

for ax in axes:
    ax.set_xlabel('Timestep')

plt.suptitle('Atención Bahdanau — GM (promedio sobre secuencias de train)', fontsize=12)
plt.tight_layout()
plt.savefig(RESULTADOS_DIR / 'gm_attention_weights.png', dpi=150, bbox_inches='tight')
plt.show()
print(f'Canal A pesos: {[f"{v:.4f}" for v in alpha_a_mean]}')
print(f'Canal B pesos: {[f"{v:.4f}" for v in alpha_b_mean]}')
"""))

# 36 ── Conclusiones
cells.append(md(36, """\
## 14. Conclusiones

### Interpretación del resultado

| Escenario | Significado | Acción sugerida |
|---|---|---|
| GM MAE < GE MAE | NLP captura señal real → arquitectura multimodal válida | Continuar con SHAP (Act. 16) |
| GM ≈ GE | Sentimiento mensual no añade info extra en esta granularidad | Probar embeddings diarios o semanal |
| GM MAE > GE MAE | NLP introduce ruido (meses con sentimiento=0 confunden al modelo) | Usar máscara de cobertura o eliminar NLP |

### Próximo paso — Actividad 16: Análisis SHAP

SHAP sobre el modelo GM (o GE si GM no mejora) para cuantificar:
- Contribución de `avg_sentiment` vs variables climáticas vs lags
- Qué features del Canal B son realmente relevantes

### Archivos generados

```
resultados/gm/
├── gm_dual_lstm_attn_nlp.keras      Modelo GM
├── gm_scalers.pkl                   Scalers A y B
├── gm_metricas.json                 Métricas evaluación
├── gm_predicciones.csv              Predicciones + sentimiento observado
├── gm_training_history.csv          Historial MSE/MAE
├── gm_sentimiento_eda.png           EDA sentimiento BETO
├── gm_split_cronologico.png         Split 80/20
├── gm_training_curve.png            Curvas entrenamiento
├── gm_prediccion_vs_real.png        Predicción vs real
├── gm_comparativa_ge_vs_gm.png      Barras GE vs GM
└── gm_attention_weights.png         Pesos Bahdanau
```
"""))


# ─────────────────────────────────────────────────────────────────
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
print(f'Notebook generado: {OUT}')
print(f'  Celdas : {len(cells)}')
print(f'  Tamanio: {size_kb:.1f} KB')
