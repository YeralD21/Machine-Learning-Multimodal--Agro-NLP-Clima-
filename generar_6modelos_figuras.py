"""
Genera dos figuras comparativas con los 6 modelos:
  1. dashboard/fig_6modelos_linea_temporal.png  — predicciones vs real
  2. dashboard/fig_6modelos_delta_shocks.png    — barras Ds
"""
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import os

C_AZUL    = '#1F4E79'
C_VERDE   = '#375623'
C_ROJO    = '#C00000'
C_GRIS    = '#475569'
C_NEGRO   = '#000000'
C_NARANJA = '#D45B00'
C_MORADO  = '#6B3FA0'

SCALER_MEAN  = 16.32
SCALER_SCALE = 27.99

def denorm(z):
    return np.asarray(z) * SCALER_SCALE + SCALER_MEAN

# ══════════════════════════════════════════════════════════════════
# 1. Cargar todas las predicciones
# ══════════════════════════════════════════════════════════════════
ge = pd.read_csv('resultados/ge/ge_predicciones.csv', parse_dates=['fecha'])
gm = pd.read_csv('resultados/gm_v3/gm_v3_predicciones.csv', parse_dates=['fecha'])
xgb = pd.read_csv('resultados/xgboost/xgb_predicciones.csv', parse_dates=['fecha'])

sarima = pd.read_csv(
    'resultados/gc1/gc1_sarima_predicciones.csv', parse_dates=['fecha_evento'],
).rename(columns={'fecha_evento': 'fecha', 'prediccion': 'pred_sarima'})

prophet = pd.read_csv(
    'resultados/gc1/gc1_prophet_predicciones.csv', parse_dates=['fecha'],
).rename(columns={'prediccion': 'pred_prophet'})

gc2 = pd.read_csv(
    'resultados/gc2/gc2_predicciones.csv', parse_dates=['fecha'],
).rename(columns={'hibrido': 'pred_gc2'})

# Base: SARIMA has the widest range (Sep 2024 - Aug 2025, 12 months)
df = sarima[['fecha', 'real', 'pred_sarima']].copy()
df = df.merge(prophet[['fecha', 'pred_prophet']], on='fecha', how='left')
df = df.merge(gc2[['fecha', 'pred_gc2']], on='fecha', how='left')
df = df.merge(
    ge[['fecha', 'prediccion']].rename(columns={'prediccion': 'pred_ge'}),
    on='fecha', how='left',
)
df = df.merge(gm[['fecha', 'pred_gm_v3']], on='fecha', how='left')
df = df.merge(xgb[['fecha', 'pred_xgb']], on='fecha', how='left')
df = df.sort_values('fecha').reset_index(drop=True)

# ══════════════════════════════════════════════════════════════════
# 2. Shock classification (z-score pct_change > 20%)
# ══════════════════════════════════════════════════════════════════
df['z_pct'] = df['real'].pct_change() * 100
df['is_shock'] = df['z_pct'].abs() > 20
df.loc[df['z_pct'].isna(), 'is_shock'] = False

# ══════════════════════════════════════════════════════════════════
# 3. Desnormalizar
# ══════════════════════════════════════════════════════════════════
df['real_t'] = denorm(df['real'])
for col in ['pred_sarima', 'pred_prophet', 'pred_gc2',
            'pred_ge', 'pred_gm_v3', 'pred_xgb']:
    df[f'{col}_t'] = denorm(df[col])

# ══════════════════════════════════════════════════════════════════
# FIGURA 1 — Linea temporal 6 modelos
# ══════════════════════════════════════════════════════════════════
plt.rcParams.update({
    'font.family': 'serif',
    'font.size': 10,
    'axes.linewidth': 0.8,
    'axes.spines.top': False,
    'axes.spines.right': False,
})

fig1, ax = plt.subplots(figsize=(15, 7.5), facecolor='white')

# Shock shading
for _, row in df[df['is_shock']].iterrows():
    ax.axvspan(
        row['fecha'] - pd.Timedelta(days=14),
        row['fecha'] + pd.Timedelta(days=14),
        color=C_ROJO, alpha=0.07, zorder=0,
    )

# Real
ax.plot(df['fecha'], df['real_t'],
        color=C_NEGRO, linewidth=2.8, label='Real', zorder=6)

# GE
mask = df['pred_ge_t'].notna()
ax.plot(df.loc[mask, 'fecha'], df.loc[mask, 'pred_ge_t'],
        color=C_AZUL, linewidth=1.6, label='GE (sin NLP)',
        marker='o', markersize=4, zorder=5)

# GM v3
mask = df['pred_gm_v3_t'].notna()
ax.plot(df.loc[mask, 'fecha'], df.loc[mask, 'pred_gm_v3_t'],
        color=C_VERDE, linewidth=1.6, label='GM v3 (NLP)',
        marker='s', markersize=4, zorder=5)

# XGBoost
mask = df['pred_xgb_t'].notna()
ax.plot(df.loc[mask, 'fecha'], df.loc[mask, 'pred_xgb_t'],
        color=C_ROJO, linewidth=1.4, label='XGBoost',
        linestyle='--', marker='^', markersize=4, zorder=4)

# SARIMA
ax.plot(df['fecha'], df['pred_sarima_t'],
        color=C_NARANJA, linewidth=1.2, label='SARIMA',
        linestyle=':', marker='d', markersize=3.5, zorder=3)

# Prophet
ax.plot(df['fecha'], df['pred_prophet_t'],
        color=C_MORADO, linewidth=1.2, label='Prophet',
        linestyle=':', marker='v', markersize=3.5, zorder=3)

# SARIMAX-LSTM (clip for display)
gc2_t = df['pred_gc2_t'].copy()
y_floor = 0
gc2_clipped = gc2_t.clip(lower=y_floor)
ax.plot(df['fecha'], gc2_clipped,
        color=C_GRIS, linewidth=1.2, label='SARIMAX-LSTM',
        linestyle=':', marker='x', markersize=3.5, zorder=2, alpha=0.7)

# Annotate worst SARIMAX-LSTM points
for idx in df.index:
    if pd.notna(gc2_t.iloc[idx]) and gc2_t.iloc[idx] < y_floor:
        ax.annotate(
            f'{gc2_t.iloc[idx]:.0f} t',
            xy=(df.loc[idx, 'fecha'], y_floor),
            fontsize=7, color=C_GRIS, ha='center', va='top',
        )

# Annotation shock Jan 2025
jan_mask = df['fecha'] == '2025-01-01'
if jan_mask.any():
    jan_real_t = df.loc[jan_mask, 'real_t'].values[0]
    jan_fecha = df.loc[jan_mask, 'fecha'].values[0]
    ax.annotate(
        'Shock maximo\n+1,021 %',
        xy=(jan_fecha, jan_real_t),
        xytext=(jan_fecha + pd.Timedelta(days=45), jan_real_t + 2.5),
        fontsize=9.5, fontweight='bold', color=C_ROJO,
        arrowprops=dict(arrowstyle='->', color=C_ROJO, lw=1.8),
        ha='left', va='bottom',
        bbox=dict(boxstyle='round,pad=0.3', facecolor='white',
                  edgecolor=C_ROJO, alpha=0.92),
    )

ax.set_ylabel('Produccion (t)', fontsize=12)
ax.set_title(
    'Predicciones vs. valor real — 6 modelos (sep 2024 - ago 2025)',
    fontsize=14, fontweight='bold', pad=12,
)
ax.legend(loc='upper right', frameon=True, fancybox=False,
          edgecolor=C_GRIS, fontsize=9, ncol=2)
ax.xaxis.set_major_formatter(mdates.DateFormatter('%b\n%Y'))
ax.xaxis.set_major_locator(mdates.MonthLocator())
ax.grid(axis='y', alpha=0.3, linewidth=0.5)

y_min_safe = max(0, df['real_t'].min() - 2)
y_max_safe = max(
    df[['real_t', 'pred_sarima_t', 'pred_prophet_t',
        'pred_ge_t', 'pred_xgb_t']].max().max() + 1.5,
    20,
)
ax.set_ylim(y_min_safe, y_max_safe)

os.makedirs('dashboard', exist_ok=True)
out1 = 'dashboard/fig_6modelos_linea_temporal.png'
fig1.savefig(out1, dpi=200, facecolor='white', bbox_inches='tight', pad_inches=0.3)
plt.close(fig1)
print(f'Figura 1 guardada: {out1}')

# ══════════════════════════════════════════════════════════════════
# FIGURA 2 — Barras Ds (7 modelos)
# ══════════════════════════════════════════════════════════════════
modelos_ds = [
    ('GE',           2.3,  C_VERDE),
    ('GM v3',       11.7,  C_VERDE),
    ('XGBoost',     16.5,  C_ROJO),
    ('Naive',       28.5,  C_ROJO),
    ('SARIMA',      None,  C_GRIS),
    ('Prophet',     None,  C_GRIS),
    ('SARIMAX-LSTM', None, C_GRIS),
]

fig2, ax2 = plt.subplots(figsize=(10, 6), facecolor='white')

nombres = [m[0] for m in modelos_ds]
valores = [m[1] if m[1] is not None else 0 for m in modelos_ds]
colores = [m[2] for m in modelos_ds]
has_val = [m[1] is not None for m in modelos_ds]

bars = ax2.barh(nombres, valores, color=colores, edgecolor='white', height=0.55)

x_max = max(v for v in valores if v > 0) * 1.40
for i, (bar, val, has) in enumerate(zip(bars, valores, has_val)):
    if has:
        ax2.text(
            bar.get_width() + x_max * 0.02,
            bar.get_y() + bar.get_height() / 2,
            f'+{val:.1f} %', va='center', fontsize=11,
            fontweight='bold', color=colores[i],
        )
    else:
        ax2.text(
            x_max * 0.02,
            bar.get_y() + bar.get_height() / 2,
            'sin dato de shock', va='center', fontsize=10,
            fontstyle='italic', color=C_GRIS,
        )

ax2.set_xlim(0, x_max)
ax2.set_xlabel('Deterioro Ds (%)', fontsize=12)
ax2.set_title(
    'Deterioro del MAE en meses de shock (Ds) — menor es mas resiliente',
    fontsize=13, fontweight='bold', pad=14,
)
ax2.grid(axis='x', alpha=0.3, linewidth=0.5)
ax2.tick_params(axis='y', labelsize=11)

out2 = 'dashboard/fig_6modelos_delta_shocks.png'
fig2.savefig(out2, dpi=200, facecolor='white', bbox_inches='tight', pad_inches=0.3)
plt.close(fig2)
print(f'Figura 2 guardada: {out2}')
