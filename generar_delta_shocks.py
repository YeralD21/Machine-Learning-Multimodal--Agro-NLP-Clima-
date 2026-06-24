"""Genera figura aislada del Panel 2: barras de deterioro Ds."""
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import json

C_VERDE = '#375623'
C_ROJO  = '#C00000'
C_GRIS  = '#475569'

SCALER_MEAN  = 16.32
SCALER_SCALE = 27.99

plt.rcParams.update({
    'font.family': 'serif',
    'font.size': 11,
    'axes.linewidth': 0.8,
    'axes.spines.top': False,
    'axes.spines.right': False,
})

ge  = pd.read_csv('resultados/ge/ge_predicciones.csv', parse_dates=['fecha'])
gm  = pd.read_csv('resultados/gm_v3/gm_v3_predicciones.csv', parse_dates=['fecha'])
xgb = pd.read_csv('resultados/xgboost/xgb_predicciones.csv', parse_dates=['fecha'])

df = ge[['fecha', 'real']].copy()
df = df.merge(
    ge[['fecha', 'prediccion']].rename(columns={'prediccion': 'pred_ge'}),
    on='fecha',
)
df = df.merge(xgb[['fecha', 'pred_xgb']], on='fecha', how='left')
df = df.merge(gm[['fecha', 'pred_gm_v3']], on='fecha', how='left')
df = df.sort_values('fecha').reset_index(drop=True)
df['pred_naive'] = df['real'].shift(1)

df['z_pct'] = df['real'].pct_change() * 100
df['is_shock'] = df['z_pct'].abs() > 20
df.loc[df['z_pct'].isna(), 'is_shock'] = False

modelos = {
    'GE':      'pred_ge',
    'GM_v3':   'pred_gm_v3',
    'XGBoost': 'pred_xgb',
    'Naive':   'pred_naive',
}

deterioro = {}
for nombre, col in modelos.items():
    sub = df.dropna(subset=[col]).copy()
    sub['ae'] = (sub['real'] - sub[col]).abs() * SCALER_SCALE
    mae_overall = sub['ae'].mean()
    shock_mask = sub['is_shock']
    if shock_mask.sum() > 0:
        mae_shock = sub.loc[shock_mask, 'ae'].mean()
        deterioro[nombre] = ((mae_shock - mae_overall) / mae_overall) * 100
    else:
        deterioro[nombre] = 0.0

det_sorted = sorted(deterioro.items(), key=lambda x: x[1])
nombres = [d[0] for d in det_sorted]
valores = [d[1] for d in det_sorted]
colores = [C_VERDE if n in ('GE', 'GM_v3') else C_ROJO for n in nombres]

fig, ax = plt.subplots(figsize=(10, 6), facecolor='white')

bars = ax.barh(nombres, valores, color=colores, edgecolor='white', height=0.55)

x_max = max(valores) * 1.30
for bar, val in zip(bars, valores):
    ax.text(
        bar.get_width() + x_max * 0.02,
        bar.get_y() + bar.get_height() / 2,
        f'+{val:.1f} %',
        va='center', fontsize=12, fontweight='bold', color=C_GRIS,
    )

ax.set_xlim(0, x_max)
ax.set_xlabel('Deterioro Ds (%)', fontsize=12)
ax.set_title(
    'Deterioro del MAE en meses de shock (Ds) — menor es mas resiliente',
    fontsize=13, fontweight='bold', pad=14,
)
ax.grid(axis='x', alpha=0.3, linewidth=0.5)
ax.tick_params(axis='y', labelsize=12)

fig.savefig(
    'dashboard/fig_delta_shocks.png',
    dpi=200, facecolor='white', bbox_inches='tight', pad_inches=0.3,
)
plt.close(fig)
print('Guardado: dashboard/fig_delta_shocks.png')
for n, v in zip(nombres, valores):
    print(f'  {n:10s}: +{v:.1f} %')
