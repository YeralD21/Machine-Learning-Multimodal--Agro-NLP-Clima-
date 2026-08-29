import os, sys, platform, json, warnings
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'
os.environ['TF_ENABLE_ONEDNN_OPTS'] = '0'
warnings.filterwarnings('ignore')

import numpy as np
import pandas as pd
import joblib
import seaborn as sns
from pathlib import Path
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
import xgboost as xgb

import tensorflow as tf
tf.get_logger().setLevel('ERROR')
from tensorflow import keras
from tensorflow.keras import layers

SEED = 42
np.random.seed(SEED)
tf.random.set_seed(SEED)

print(f'Python     : {sys.version.split()[0]}')
print(f'TensorFlow : {tf.__version__}')
print(f'XGBoost    : {xgb.__version__}')

# --- PATCH vs notebook original: ROOT y MASTER_PATH ---
ROOT = Path('D:/Machine-Learning-Multimodal--Agro-NLP-Clima-')
MASTER_PATH  = ROOT / 'scratch/master_dataset_fase2_multivariado_RECONSTRUIDO.csv'   # reconstruido desde master_dataset_fase1.csv via actividad_02 (56 meses, exacto)
NLP_V1_PATH  = ROOT / 'notebooks/fase2/output/01_nlp_sentimiento/sentimiento_mensual.csv'
NLP_V2_PATH  = ROOT / 'notebooks/fase2/output/01_nlp_sentimiento/sentimiento_mensual_v2.csv'
SCALER_F2    = ROOT / 'notebooks/fase2/scalers/standard_scaler_fase2.joblib'
GE_MODEL     = ROOT / 'resultados/ge/ge_dual_lstm_attn.keras'
GE_SCALERS   = ROOT / 'resultados/ge/ge_scalers.pkl'
GMV3_MODEL   = ROOT / 'resultados/gm_v3/gm_v3_model.keras'

for label, p in [('Master', MASTER_PATH), ('NLP v1', NLP_V1_PATH),
                 ('NLP v2', NLP_V2_PATH), ('Scaler F2', SCALER_F2),
                 ('GE model', GE_MODEL), ('GE scalers', GE_SCALERS),
                 ('GM_v3 model', GMV3_MODEL)]:
    print(f'{label:12s}: {"OK" if p.exists() else "FALTA"}')

# ---------- Celda 4 ----------
scaler_f2 = joblib.load(SCALER_F2)
f2_features = list(scaler_f2.feature_names_in_)

def f2_mean(col): return scaler_f2.mean_[f2_features.index(col)]
def f2_scale(col): return scaler_f2.scale_[f2_features.index(col)]
def desn_f2(z, col): return z * f2_scale(col) + f2_mean(col)
def norm_f2(raw, col): return (raw - f2_mean(col)) / f2_scale(col)

df_raw = pd.read_csv(MASTER_PATH, parse_dates=['fecha_evento'])

# --- PATCH vs notebook original: derivar columnas temporales ausentes ---
faltan = [c for c in ['mes_num','trimestre_num','trimestre_sin','trimestre_cos'] if c not in df_raw.columns]
print(f'\n[PATCH] Columnas temporales derivadas de fecha_evento (faltaban en el CSV): {faltan}')
df_raw['mes_num'] = df_raw['fecha_evento'].dt.month
df_raw['trimestre_num'] = (df_raw['mes_num'] - 1) // 3 + 1
df_raw['trimestre_sin'] = np.sin(2*np.pi*df_raw['trimestre_num']/4)
df_raw['trimestre_cos'] = np.cos(2*np.pi*df_raw['trimestre_num']/4)
if 'month_sin' not in df_raw.columns:
    df_raw['month_sin'] = np.sin(2*np.pi*df_raw['mes_num']/12)
if 'month_cos' not in df_raw.columns:
    df_raw['month_cos'] = np.cos(2*np.pi*df_raw['mes_num']/12)

ZSCORE_COLS = ['produccion_t', 'precio_chacra_kg', 'num_emergencias',
               'total_afectados', 'hectareas_cultivo_perdidas',
               'ALLSKY_SFC_SW_DWN', 'PRECTOTCORR', 'QV2M', 'RH2M',
               'T2M', 'T2M_MAX', 'T2M_MIN', 'WS2M']
RAW_COLS = ['lat', 'lon', 'month_sin', 'month_cos', 'mes_num',
            'trimestre_num', 'trimestre_sin', 'trimestre_cos']

print(f'Master dataset: {df_raw.shape}')
print(f'Rango: {df_raw.fecha_evento.min().date()} -> {df_raw.fecha_evento.max().date()}')

# ---------- Celda 5 ----------
AGG_COLS = ZSCORE_COLS + RAW_COLS
nacional = df_raw.groupby('fecha_evento')[AGG_COLS].mean().sort_index()
ultimo = nacional.iloc[-1]

ws2m_raw   = desn_f2(ultimo['WS2M'], 'WS2M')
prec_raw   = desn_f2(ultimo['PRECTOTCORR'], 'PRECTOTCORR')
tmax_raw   = desn_f2(ultimo['T2M_MAX'], 'T2M_MAX')
emerg_raw  = desn_f2(ultimo['num_emergencias'], 'num_emergencias')

print(f'\nUltimo mes conocido: {nacional.index[-1].date()}')
print(f'  WS2M            : z={ultimo["WS2M"]:.4f}  ->  {ws2m_raw:.2f} m/s')
print(f'  PRECTOTCORR     : z={ultimo["PRECTOTCORR"]:.4f}  ->  {prec_raw:.2f} mm')
print(f'  T2M_MAX         : z={ultimo["T2M_MAX"]:.4f}  ->  {tmax_raw:.2f} C')
print(f'  num_emergencias : z={ultimo["num_emergencias"]:.4f}  ->  {emerg_raw:.2f}')

nino_ws2m_z  = norm_f2(ws2m_raw * 1.5,  'WS2M')
nino_prec_z  = norm_f2(prec_raw * 3.0,   'PRECTOTCORR')
nino_tmax_z  = norm_f2(tmax_raw + 2.5,   'T2M_MAX')
nino_emerg_z = norm_f2(emerg_raw * 4,    'num_emergencias')
nino_nlp     = -0.6

print(f'\nEscenario El Nino Q1 2026:')
print(f'  WS2M            : {ws2m_raw:.2f} -> {ws2m_raw*1.5:.2f} m/s  (z={nino_ws2m_z:.4f})')
print(f'  PRECTOTCORR     : {prec_raw:.2f} -> {prec_raw*3.0:.2f} mm    (z={nino_prec_z:.4f})')
print(f'  T2M_MAX         : {tmax_raw:.2f} -> {tmax_raw+2.5:.2f} C   (z={nino_tmax_z:.4f})')
print(f'  num_emergencias : {emerg_raw:.2f} -> {emerg_raw*4:.2f}       (z={nino_emerg_z:.4f})')
print(f'  nlp_index       : -> {nino_nlp}')

NINO_DELTAS = {
    'WS2M':             nino_ws2m_z,
    'PRECTOTCORR':      nino_prec_z,
    'T2M_MAX':          nino_tmax_z,
    'num_emergencias':  nino_emerg_z,
}

# ---------- Celda 6 ----------
Q1_MONTHS = [1, 2, 3]

def temporal_encoding(mes):
    return {
        'mes_num':        mes,
        'month_sin':      np.sin(2 * np.pi * mes / 12),
        'month_cos':      np.cos(2 * np.pi * mes / 12),
        'trimestre_num':  (mes - 1) // 3 + 1,
        'trimestre_sin':  np.sin(2 * np.pi * ((mes - 1) // 3 + 1) / 4),
        'trimestre_cos':  np.cos(2 * np.pi * ((mes - 1) // 3 + 1) / 4),
    }

for m in Q1_MONTHS:
    enc = temporal_encoding(m)
    print(f'Mes {m:2d}: sin={enc["month_sin"]:+.4f}  cos={enc["month_cos"]:+.4f}  '
          f'trim={enc["trimestre_num"]}  t_sin={enc["trimestre_sin"]:+.4f}  t_cos={enc["trimestre_cos"]:+.4f}')

# ---------- Celda 8: GE model ----------
class BahdanauAttention(keras.layers.Layer):
    def __init__(self, units, **kwargs):
        super().__init__(**kwargs)
        self.units = units
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

ge_model = keras.models.load_model(
    GE_MODEL, custom_objects={'BahdanauAttention': BahdanauAttention}
)
ge_sc = joblib.load(GE_SCALERS)
ge_scaler_a = ge_sc['scaler_a']
ge_scaler_b = ge_sc['scaler_b']

SEQ_LEN = 6
GE_CANAL_B = [
    'lag_1', 'lag_3', 'lag_6',
    'precio_chacra_kg',
    'num_emergencias', 'total_afectados', 'hectareas_cultivo_perdidas',
    'ALLSKY_SFC_SW_DWN', 'PRECTOTCORR', 'QV2M', 'RH2M',
    'T2M', 'T2M_MAX', 'T2M_MIN', 'WS2M',
    'lat', 'lon',
    'month_sin', 'month_cos', 'mes_num',
    'trimestre_num', 'trimestre_sin', 'trimestre_cos',
]

print(f'\nGE modelo cargado: Canal A=1, Canal B={len(GE_CANAL_B)}')
print(f'Scaler A: mean={ge_scaler_a.mean_[0]:.6f}  scale={ge_scaler_a.scale_[0]:.6f}')

# ---------- Celda 9 ----------
ge_nac = df_raw.groupby('fecha_evento')[AGG_COLS].mean().sort_index()
ge_nac.index = pd.DatetimeIndex(ge_nac.index, freq='MS')
ge_nac['lag_1'] = ge_nac['produccion_t'].shift(1)
ge_nac['lag_3'] = ge_nac['produccion_t'].shift(3)
ge_nac['lag_6'] = ge_nac['produccion_t'].shift(6)
ge_nac = ge_nac.dropna()

n_total_ge = len(ge_nac)
n_train_ge = int(n_total_ge * 0.80)
ge_train = ge_nac.iloc[:n_train_ge]

y_all_sc = ge_scaler_a.transform(ge_nac['produccion_t'].values.reshape(-1, 1)).flatten()
B_all_sc = ge_scaler_b.transform(ge_nac[GE_CANAL_B].values)

print(f'Serie GE: {len(ge_nac)} meses | Train: {n_train_ge}')
print(f'Ultimo mes: {ge_nac.index[-1].date()}')

# ---------- Celda 10 ----------
def ge_predict_q1(nino=False):
    canal_a_buf = list(y_all_sc[-SEQ_LEN:])
    preds = []
    for mes in Q1_MONTHS:
        x_a = np.array(canal_a_buf[-SEQ_LEN:]).reshape(1, SEQ_LEN, 1).astype(np.float32)
        b_rows = []
        for t in range(SEQ_LEN):
            row = ge_nac.iloc[-(SEQ_LEN - t)].copy() if t < SEQ_LEN - 1 else ge_nac.iloc[-1].copy()
            if t == SEQ_LEN - 1:
                enc = temporal_encoding(mes)
                for k, v in enc.items():
                    row[k] = v
                row['lag_1'] = ge_scaler_a.inverse_transform([[canal_a_buf[-1]]])[0, 0]
                if len(canal_a_buf) >= 3:
                    row['lag_3'] = ge_scaler_a.inverse_transform([[canal_a_buf[-3]]])[0, 0]
                if len(canal_a_buf) >= 6:
                    row['lag_6'] = ge_scaler_a.inverse_transform([[canal_a_buf[-6]]])[0, 0]
            if nino:
                for col, z_val in NINO_DELTAS.items():
                    row[col] = z_val
            b_rows.append(ge_scaler_b.transform(row[GE_CANAL_B].values.reshape(1, -1))[0])
        b_window = np.array(b_rows, dtype=np.float32)
        x_b = b_window.reshape(1, SEQ_LEN, len(GE_CANAL_B)).astype(np.float32)
        pred_sc = float(ge_model.predict([x_a, x_b], verbose=0)[0, 0])
        preds.append(pred_sc)
        canal_a_buf.append(pred_sc)
    preds_zscore = ge_scaler_a.inverse_transform(np.array(preds).reshape(-1, 1)).flatten()
    return preds_zscore

ge_normal = ge_predict_q1(nino=False)
ge_nino   = ge_predict_q1(nino=True)

print('\nGE predicciones Q1 2026 (z-score fase 2):')
for i, mes in enumerate(Q1_MONTHS):
    print(f'  Mes {mes}: Normal={ge_normal[i]:.6f}  Nino={ge_nino[i]:.6f}  '
          f'Delta={ge_nino[i]-ge_normal[i]:.6f}')

# ---------- Celda 12: GM_v3 data ----------
gm_master = df_raw.groupby('fecha_evento').mean(numeric_only=True).reset_index()
gm_master = gm_master.sort_values('fecha_evento').reset_index(drop=True)

df_nlp = pd.read_csv(NLP_V2_PATH, encoding='utf-8-sig')
fc = [c for c in df_nlp.columns if any(k in c.lower() for k in ['fecha','periodo','mes','month'])][0]
df_nlp = df_nlp.rename(columns={fc: 'fecha_evento'})
df_nlp['fecha_evento'] = pd.to_datetime(df_nlp['fecha_evento'])
df_nlp['nlp_index']      = df_nlp['avg_sentiment'] * np.log1p(df_nlp['n_noticias_beto'])
df_nlp['nlp_index_lag1'] = df_nlp['nlp_index'].shift(1).fillna(0)

gm_df = gm_master.merge(df_nlp[['fecha_evento','nlp_index','nlp_index_lag1']],
                         on='fecha_evento', how='left')
gm_df['nlp_index']      = gm_df['nlp_index'].fillna(0)
gm_df['nlp_index_lag1'] = gm_df['nlp_index_lag1'].fillna(0)
gm_df = gm_df.sort_values('fecha_evento').reset_index(drop=True)

TARGET = 'produccion_t'
META   = ['fecha_evento', TARGET]
# --- PATCH vs notebook original: STRUCT restringido a la lista EXACTA usada
# en el entrenamiento real (verificada en el output de actividad_15v5_ejecutado.ipynb,
# celda 5: "Struct (20): [...]"). El dataset disponible en disco trae columnas
# extra (nlp_sentiment, produccion_t_lag1/3/6, etc.) que NO estaban en el
# dataset original de entrenamiento y rompian el PCA (28->11 componentes en vez de 8).
STRUCT_ENTRENAMIENTO_REAL = [
    'precio_chacra_kg', 'num_emergencias', 'total_afectados',
    'hectareas_cultivo_perdidas', 'ALLSKY_SFC_SW_DWN', 'PRECTOTCORR',
    'QV2M', 'RH2M', 'T2M', 'T2M_MAX', 'T2M_MIN', 'WS2M',
    'lat', 'lon', 'month_sin', 'month_cos', 'mes_num',
    'trimestre_num', 'trimestre_sin', 'trimestre_cos',
]
STRUCT = [c for c in STRUCT_ENTRENAMIENTO_REAL if c in gm_master.columns]
faltantes_struct = [c for c in STRUCT_ENTRENAMIENTO_REAL if c not in gm_master.columns]
print(f'[PATCH] STRUCT restringido a {len(STRUCT)}/{len(STRUCT_ENTRENAMIENTO_REAL)} columnas reales de entrenamiento.')
if faltantes_struct:
    print(f'[PATCH] ATENCION - faltan columnas del STRUCT original: {faltantes_struct}')
NLP_F  = ['nlp_index', 'nlp_index_lag1']
TIMESTEPS = 6

n_total_gm = len(gm_df)
n_train_gm = int(n_total_gm * 0.80)
gm_train = gm_df.iloc[:n_train_gm]
gm_test  = gm_df.iloc[n_train_gm:]

gm_scaler_s = StandardScaler()
gm_scaler_n = StandardScaler()
gm_scaler_y = StandardScaler()

Xs_tr_raw = gm_scaler_s.fit_transform(gm_train[STRUCT])
Xs_te_raw = gm_scaler_s.transform(gm_test[STRUCT])
Xn_tr_raw = gm_scaler_n.fit_transform(gm_train[NLP_F])
Xn_te_raw = gm_scaler_n.transform(gm_test[NLP_F])
y_tr_sc   = gm_scaler_y.fit_transform(gm_train[[TARGET]])
y_te_sc   = gm_scaler_y.transform(gm_test[[TARGET]])

gm_pca = PCA(n_components=0.95, random_state=SEED)
Xs_tr = gm_pca.fit_transform(Xs_tr_raw)
Xs_te = gm_pca.transform(Xs_te_raw)
n_comp = gm_pca.n_components_

Xs_all_raw = gm_scaler_s.transform(gm_df[STRUCT])
Xs_all_pca = gm_pca.transform(Xs_all_raw)
Xn_all     = gm_scaler_n.transform(gm_df[NLP_F])
y_all_gm   = gm_scaler_y.transform(gm_df[[TARGET]])

print(f'\nGM_v3 data: {n_total_gm} meses | Train: {n_train_gm}')
print(f'PCA: {len(STRUCT)} features -> {n_comp} componentes')
print(f'NLP: {NLP_F}')

# ---------- Celda 13: build GM_v3 ----------
from tensorflow.keras import regularizers, Model

class ReduceSumLayer(keras.layers.Layer):
    def call(self, x):
        return tf.reduce_sum(x, axis=1)

def build_gm_v3(ss, ns, units=64, drs=0.2, drn=0.5):
    inp_s = layers.Input(shape=ss, name='inp_struct')
    h = layers.LSTM(units, return_sequences=True)(inp_s)
    sc = layers.Dense(1, activation='tanh')(h)
    sw = layers.Softmax(axis=1)(sc)
    ca = layers.Multiply()([h, sw])
    ca = ReduceSumLayer()(ca)
    ca = layers.Dropout(drs)(ca)
    inp_n = layers.Input(shape=ns, name='inp_nlp')
    cb = layers.LSTM(16, return_sequences=False)(inp_n)
    cb = layers.Dropout(drn, name='dropout_nlp_M3')(cb)
    mg = layers.Concatenate()([ca, cb])
    x = layers.Dense(32, activation='relu', kernel_regularizer=regularizers.l2(1e-4))(mg)
    x = layers.Dropout(0.2)(x)
    x = layers.Dense(16, activation='relu')(x)
    out = layers.Dense(1)(x)
    return Model(inputs=[inp_s, inp_n], outputs=out, name='GM_v3')

ss = (TIMESTEPS, n_comp)
ns = (TIMESTEPS, len(NLP_F))
gmv3_model = build_gm_v3(ss, ns)
gmv3_model.load_weights(GMV3_MODEL)
print(f'GM_v3 modelo reconstruido y pesos cargados')

# ---------- Celda 14 ----------
def gm_predict_q1(nino=False):
    preds = []
    for i, mes in enumerate(Q1_MONTHS):
        xs_rows = []
        xn_rows = []
        for t in range(TIMESTEPS):
            idx = -(TIMESTEPS - t) if t < TIMESTEPS - 1 else -1
            struct_row = gm_df.iloc[idx][STRUCT].copy()
            nlp_row = gm_df.iloc[idx][NLP_F].values.copy()
            if t == TIMESTEPS - 1:
                enc = temporal_encoding(mes)
                for k, v in enc.items():
                    if k in struct_row.index:
                        struct_row[k] = v
            if nino:
                for col, z_val in NINO_DELTAS.items():
                    if col in struct_row.index:
                        struct_row[col] = z_val
                nlp_row[0] = nino_nlp
                if t > 0:
                    nlp_row[1] = nino_nlp
            s_sc = gm_scaler_s.transform(struct_row.values.reshape(1, -1))
            s_pca = gm_pca.transform(s_sc)
            n_sc = gm_scaler_n.transform(nlp_row.reshape(1, -1))
            xs_rows.append(s_pca[0])
            xn_rows.append(n_sc[0])
        xs_seq = np.array(xs_rows).reshape(1, TIMESTEPS, n_comp).astype(np.float32)
        xn_seq = np.array(xn_rows).reshape(1, TIMESTEPS, len(NLP_F)).astype(np.float32)
        pred_sc = float(gmv3_model.predict([xs_seq, xn_seq], verbose=0)[0, 0])
        preds.append(pred_sc)
    preds_zscore = gm_scaler_y.inverse_transform(np.array(preds).reshape(-1, 1)).flatten()
    return preds_zscore

gm_normal = gm_predict_q1(nino=False)
gm_nino   = gm_predict_q1(nino=True)

print('\nGM_v3 predicciones Q1 2026 (z-score fase 2):')
for i, mes in enumerate(Q1_MONTHS):
    print(f'  Mes {mes}: Normal={gm_normal[i]:.6f}  Nino={gm_nino[i]:.6f}  '
          f'Delta={gm_nino[i]-gm_normal[i]:.6f}')

# ---------- Celda 16: XGBoost ----------
xgb_master = df_raw.groupby('fecha_evento').mean(numeric_only=True).reset_index()
xgb_master = xgb_master.sort_values('fecha_evento').reset_index(drop=True)

df_nlp_v1 = pd.read_csv(NLP_V1_PATH, encoding='utf-8-sig')
fc = [c for c in df_nlp_v1.columns if any(k in c.lower() for k in ['fecha','periodo','mes','month'])][0]
df_nlp_v1 = df_nlp_v1.rename(columns={fc: 'fecha_evento'})
df_nlp_v1['fecha_evento'] = pd.to_datetime(df_nlp_v1['fecha_evento'])
df_nlp_v1['nlp_index']      = df_nlp_v1['avg_sentiment'] * np.log1p(df_nlp_v1['n_noticias_beto'])
df_nlp_v1['nlp_index_lag1'] = df_nlp_v1['nlp_index'].shift(1).fillna(0)

xgb_df = xgb_master.merge(df_nlp_v1[['fecha_evento','nlp_index','nlp_index_lag1']],
                           on='fecha_evento', how='left')
xgb_df['nlp_index']      = xgb_df['nlp_index'].fillna(0)
xgb_df['nlp_index_lag1'] = xgb_df['nlp_index_lag1'].fillna(0)

TARGET = 'produccion_t'
META_XGB = ['fecha_evento', TARGET]
EXOG = [c for c in xgb_df.columns if c not in META_XGB]

for lag in [1, 2, 3, 6]:
    xgb_df[f'prod_lag{lag}'] = xgb_df[TARGET].shift(lag)
xgb_df['prod_roll3_mean'] = xgb_df[TARGET].shift(1).rolling(3).mean()
xgb_df['prod_roll6_mean'] = xgb_df[TARGET].shift(1).rolling(6).mean()
xgb_df['prod_roll3_std']  = xgb_df[TARGET].shift(1).rolling(3).std()

xgb_df_model = xgb_df.dropna().reset_index(drop=True)

ALL_FEATURES = EXOG + [f'prod_lag{l}' for l in [1,2,3,6]] + \
               ['prod_roll3_mean', 'prod_roll6_mean', 'prod_roll3_std']

n_total_xgb = len(xgb_df_model)
n_train_xgb = int(n_total_xgb * 0.80)

X_train_xgb = xgb_df_model.iloc[:n_train_xgb][ALL_FEATURES].values
y_train_xgb = xgb_df_model.iloc[:n_train_xgb][TARGET].values

BEST_PARAMS = {'max_depth': 2, 'n_estimators': 200, 'learning_rate': 0.05,
               'subsample': 0.8, 'colsample_bytree': 0.8}

model_xgb = xgb.XGBRegressor(**BEST_PARAMS, random_state=SEED, verbosity=0)
model_xgb.fit(X_train_xgb, y_train_xgb)

print(f'\nXGBoost reentrenado: {len(ALL_FEATURES)} features, train={n_train_xgb}')
print(f'Features climaticas: WS2M, PRECTOTCORR, T2M_MAX estan en posiciones '
      f'{[ALL_FEATURES.index(c) for c in ["WS2M","PRECTOTCORR","T2M_MAX"] if c in ALL_FEATURES]}')

imp = pd.Series(model_xgb.feature_importances_, index=ALL_FEATURES).sort_values(ascending=False)
print(f'\nTop 5 features (importancia):')
for feat, val in imp.head(5).items():
    print(f'  {feat:>25}: {val:.4f}')

lag_feats = [f for f in ALL_FEATURES if 'lag' in f or 'roll' in f]
clima_feats = ['ALLSKY_SFC_SW_DWN','PRECTOTCORR','QV2M','RH2M','T2M','T2M_MAX','T2M_MIN','WS2M']
print(f'\nImportancia acumulada:')
print(f'  Lags/rolling ({len(lag_feats)}) : {imp[lag_feats].sum():.4f}')
print(f'  Clima ({len(clima_feats)})       : {imp[[c for c in clima_feats if c in imp.index]].sum():.4f}')

# ---------- Celda 17 ----------
def xgb_predict_q1(nino=False):
    prod_hist = list(xgb_df_model[TARGET].values)
    preds = []
    for mes in Q1_MONTHS:
        base = xgb_df_model.iloc[-1].copy()
        enc = temporal_encoding(mes)
        for k, v in enc.items():
            if k in base.index:
                base[k] = v
        base['prod_lag1'] = prod_hist[-1]
        base['prod_lag2'] = prod_hist[-2]
        base['prod_lag3'] = prod_hist[-3]
        base['prod_lag6'] = prod_hist[-6]
        base['prod_roll3_mean'] = np.mean(prod_hist[-3:])
        base['prod_roll6_mean'] = np.mean(prod_hist[-6:])
        base['prod_roll3_std']  = np.std(prod_hist[-3:], ddof=1) if len(prod_hist) >= 3 else 0
        if nino:
            for col, z_val in NINO_DELTAS.items():
                if col in base.index:
                    base[col] = z_val
            if 'nlp_index' in base.index:
                base['nlp_index'] = nino_nlp
        x_vec = base[ALL_FEATURES].values.reshape(1, -1)
        pred = float(model_xgb.predict(x_vec)[0])
        preds.append(pred)
        prod_hist.append(pred)
    return np.array(preds)

xgb_normal = xgb_predict_q1(nino=False)
xgb_nino   = xgb_predict_q1(nino=True)

print('\nXGBoost predicciones Q1 2026 (z-score fase 2):')
for i, mes in enumerate(Q1_MONTHS):
    print(f'  Mes {mes}: Normal={xgb_normal[i]:.6f}  Nino={xgb_nino[i]:.6f}  '
          f'Delta={xgb_nino[i]-xgb_normal[i]:.6f}')

# ---------- Celda 19 ----------
MEAN_PROD  = scaler_f2.mean_[f2_features.index('produccion_t')]
SCALE_PROD = scaler_f2.scale_[f2_features.index('produccion_t')]

def desn_t(z): return z * SCALE_PROD + MEAN_PROD

meses_label = {1: 'Ene-26', 2: 'Feb-26', 3: 'Mar-26'}

rows = []
for modelo, norm, nino_pred in [('GE', ge_normal, ge_nino),
                                 ('GM_v3', gm_normal, gm_nino),
                                 ('XGBoost', xgb_normal, xgb_nino)]:
    for i, mes in enumerate(Q1_MONTHS):
        z_n = norm[i]; z_ni = nino_pred[i]
        t_n = desn_t(z_n); t_ni = desn_t(z_ni)
        ajuste_t = t_ni - t_n
        ajuste_pct = (t_ni - t_n) / t_n * 100 if t_n != 0 else 0
        rows.append({
            'Mes': meses_label[mes], 'Modelo': modelo,
            'Pred_normal_z': z_n, 'Pred_nino_z': z_ni,
            'Pred_normal_t': t_n, 'Pred_nino_t': t_ni,
            'Ajuste_t': ajuste_t, 'Ajuste_pct': ajuste_pct,
        })

df_comp = pd.DataFrame(rows)

for mes in Q1_MONTHS:
    ml = meses_label[mes]
    df_m = df_comp[df_comp.Mes == ml]
    print('=' * 100)
    print(f'  {ml} - Prediccion Normal vs El Nino')
    print('=' * 100)
    print(f'  {"Modelo":<10} {"Pred normal (t)":>16} {"Pred Nino (t)":>16} '
          f'{"Ajuste (t)":>12} {"Ajuste %":>10} {"Interpretacion":<30}')
    print('  ' + '-' * 96)
    for _, r in df_m.iterrows():
        if abs(r.Ajuste_pct) > 5:
            interp = 'Reacciona al clima' if r.Ajuste_pct < 0 else 'Sobrecompensa'
        elif abs(r.Ajuste_pct) > 1:
            interp = 'Ajuste moderado' if r.Ajuste_pct < 0 else 'Ajuste moderado'
        else:
            interp = 'Insensible al clima'
        print(f'  {r.Modelo:<10} {r.Pred_normal_t:>16.2f} {r.Pred_nino_t:>16.2f} '
              f'{r.Ajuste_t:>+12.2f} {r.Ajuste_pct:>+9.1f}% {interp:<30}')
    print()

# ---------- Celda 20 ----------
print('=' * 100)
print('  RESUMEN Q1 2026 - SENSIBILIDAD CLIMATICA PROMEDIO')
print('=' * 100)
print(f'  {"Modelo":<10} {"Pred normal (t)":>16} {"Pred Nino (t)":>16} '
      f'{"Ajuste prom (t)":>16} {"Ajuste prom %":>14} {"Sensibilidad":<20}')
print('  ' + '-' * 96)

resumen_sens = {}
for modelo in ['GE', 'GM_v3', 'XGBoost']:
    dm = df_comp[df_comp.Modelo == modelo]
    avg_n = dm.Pred_normal_t.mean()
    avg_ni = dm.Pred_nino_t.mean()
    avg_adj = dm.Ajuste_t.mean()
    avg_pct = dm.Ajuste_pct.mean()
    resumen_sens[modelo] = avg_pct
    if abs(avg_pct) > 5:
        sens = 'ALTA'
    elif abs(avg_pct) > 1:
        sens = 'MODERADA'
    else:
        sens = 'BAJA'
    print(f'  {modelo:<10} {avg_n:>16.2f} {avg_ni:>16.2f} '
          f'{avg_adj:>+16.2f} {avg_pct:>+13.1f}% {sens:<20}')
print('=' * 100)

print('\n[RESULTADO CLAVE] Ajuste_pct promedio Q1 2026 por modelo (ejecucion real):')
for modelo, pct in resumen_sens.items():
    print(f'  {modelo:<10}: {pct:+.2f}%')

# ---------- Celda 25: segundo calculo economico ----------
COSTO_MERMA_KG    = 1.80
COSTO_STOCKOUT_KG = 3.20
CAIDA_REAL_ESTIMADA = 0.40

print('\n' + '=' * 110)
print('  IMPACTO ECONOMICO (celda 25) - Si El Nino causa -40% de produccion real')
print('=' * 110)
print(f'  {"Mes":<8} {"Modelo":<10} {"Pred Nino (t)":>14} {"Real est. (t)":>14} '
      f'{"Error (kg)":>11} {"Tipo":>10} {"Perdida (S/)":>14}')
print('  ' + '-' * 106)

econ_rows = []
for i, mes in enumerate(Q1_MONTHS):
    for modelo, norm, nino_p in [('GE', ge_normal, ge_nino),
                                  ('GM_v3', gm_normal, gm_nino),
                                  ('XGBoost', xgb_normal, xgb_nino)]:
        pred_t = desn_t(nino_p[i])
        real_est = desn_t(norm[i]) * (1 - CAIDA_REAL_ESTIMADA)
        error_kg = abs(pred_t - real_est) * 1000
        if pred_t > real_est:
            tipo = 'Merma'
            perdida = error_kg * COSTO_MERMA_KG
        else:
            tipo = 'Stockout'
            perdida = error_kg * COSTO_STOCKOUT_KG
        econ_rows.append({
            'Mes': meses_label[mes], 'Modelo': modelo,
            'pred_nino_t': pred_t, 'real_est_t': real_est,
            'error_kg': error_kg, 'tipo': tipo, 'perdida': perdida,
        })
        print(f'  {meses_label[mes]:<8} {modelo:<10} {pred_t:>14.2f} {real_est:>14.2f} '
              f'{error_kg:>11,.0f} {tipo:>10} {perdida:>14,.2f}')
    print('  ' + '-' * 106)
print('=' * 110)

df_econ = pd.DataFrame(econ_rows)
print(f'\n  PERDIDA TOTAL Q1 2026 (celda 25):')
for modelo in ['GE', 'GM_v3', 'XGBoost']:
    dm = df_econ[df_econ.Modelo == modelo]
    print(f'  {modelo:<10} S/ {dm.perdida.sum():>12,.2f}')

print('\nFIN EJECUCION')
