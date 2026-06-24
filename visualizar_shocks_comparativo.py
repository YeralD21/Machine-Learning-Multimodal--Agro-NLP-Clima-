"""
Genera figura comparativa de modelos durante periodos de shock.
Tres paneles: línea temporal, deterioro Δs, SHAP global vs shocks.
Producción desnormalizada con scaler Fase 2 (mean=16.32, scale=27.99).
"""
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import matplotlib.dates as mdates
import json
import os

# ── Paleta académica ──────────────────────────────────────────────
C_AZUL  = '#1F4E79'
C_VERDE = '#375623'
C_ROJO  = '#C00000'
C_GRIS  = '#475569'
C_NEGRO = '#000000'

# ── Scaler Fase 2 ────────────────────────────────────────────────
SCALER_MEAN  = 16.32
SCALER_SCALE = 27.99

def denorm(z):
    return z * SCALER_SCALE + SCALER_MEAN

plt.rcParams.update({
    'font.family': 'serif',
    'font.size': 10,
    'axes.linewidth': 0.8,
    'axes.spines.top': False,
    'axes.spines.right': False,
})

# ── 1. Cargar predicciones ────────────────────────────────────────
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

# Naive: pred[t] = real[t-1]; Nov 2024 queda NaN (sin predecesor en test)
df['pred_naive'] = df['real'].shift(1)

# ── 2. Desnormalizar ──────────────────────────────────────────────
for col in ['real', 'pred_ge', 'pred_xgb', 'pred_gm_v3', 'pred_naive']:
    df[f'{col}_t'] = denorm(df[col])

gm['real_t']       = denorm(gm['real'])
gm['pred_gm_v3_t'] = denorm(gm['pred_gm_v3'])

# ── 3. Meses de shock (|variación z-score| > 20 %) ───────────────
df['z_pct'] = df['real'].pct_change() * 100
df['is_shock'] = df['z_pct'].abs() > 20
df.loc[df['z_pct'].isna(), 'is_shock'] = False

shock_dates_set = set(df.loc[df['is_shock'], 'fecha'].dt.strftime('%Y-%m-%d'))

# ── 4. Deterioro Δs = (MAE_shock − MAE_overall) / MAE_overall ────
modelos = {
    'GE':      'pred_ge',
    'GM_v3':   'pred_gm_v3',
    'XGBoost': 'pred_xgb',
    'Naive':   'pred_naive',
}

deterioro = {}
for nombre, col in modelos.items():
    sub = df.dropna(subset=[col]).copy()
    sub['ae'] = (denorm(sub['real']) - denorm(sub[col])).abs()
    mae_overall = sub['ae'].mean()
    shock_mask = sub['is_shock']
    if shock_mask.sum() > 0:
        mae_shock = sub.loc[shock_mask, 'ae'].mean()
        deterioro[nombre] = ((mae_shock - mae_overall) / mae_overall) * 100
    else:
        deterioro[nombre] = 0.0

# ── 5. SHAP: global vs shocks ────────────────────────────────────
with open('resultados/shap/shap_resultados.json', 'r', encoding='utf-8') as f:
    shap_data = json.load(f)

mes_a_fecha = {
    'Nov 2024': '2024-11-01', 'Dec 2024': '2024-12-01',
    'Jan 2025': '2025-01-01', 'Feb 2025': '2025-02-01',
    'Mar 2025': '2025-03-01', 'Apr 2025': '2025-04-01',
    'May 2025': '2025-05-01', 'Jun 2025': '2025-06-01',
    'Jul 2025': '2025-07-01', 'Aug 2025': '2025-08-01',
}

top5 = shap_data['ranking_global'][:5]
top5_features = [r['feature'] for r in top5]
top5_labels   = [r['label']   for r in top5]
top5_global   = [r['importance'] for r in top5]

top5_shock = []
for feat in top5_features:
    vals = []
    for mes in shap_data['shap_por_mes']:
        fecha_str = mes_a_fecha.get(mes['mes'], '')
        if fecha_str in shock_dates_set and feat in mes['shap_top5']:
            vals.append(abs(mes['shap_top5'][feat]))
    top5_shock.append(np.mean(vals) if vals else 0.0)

# ── 6. Construir figura ──────────────────────────────────────────
fig = plt.figure(figsize=(14, 13.5), facecolor='white')
gs = gridspec.GridSpec(
    2, 2, height_ratios=[1.3, 1],
    hspace=0.38, wspace=0.38,
    left=0.07, right=0.96, top=0.94, bottom=0.05,
)

# ─── Panel 1: línea temporal desnormalizada ───────────────────────
ax1 = fig.add_subplot(gs[0, :])

for _, row in df[df['is_shock']].iterrows():
    ax1.axvspan(
        row['fecha'] - pd.Timedelta(days=14),
        row['fecha'] + pd.Timedelta(days=14),
        color=C_ROJO, alpha=0.08, zorder=0,
    )

ax1.plot(
    df['fecha'], df['real_t'],
    color=C_NEGRO, linewidth=2.8, label='Real', zorder=5,
)
ax1.plot(
    df['fecha'], df['pred_ge_t'],
    color=C_AZUL, linewidth=1.6, label='GE (sin NLP)',
    marker='o', markersize=5, zorder=4,
)
ax1.plot(
    gm['fecha'], gm['pred_gm_v3_t'],
    color=C_VERDE, linewidth=1.6, label='GM v3 (NLP)',
    marker='s', markersize=5, zorder=4,
)
ax1.plot(
    df['fecha'], df['pred_xgb_t'],
    color=C_ROJO, linewidth=1.6, label='XGBoost',
    linestyle='--', marker='^', markersize=5, zorder=4,
)

jan_idx = df[df['fecha'] == '2025-01-01'].index[0]
jan_real_t = df.loc[jan_idx, 'real_t']
ax1.annotate(
    'Shock máximo\n+1,021 %',
    xy=(df.loc[jan_idx, 'fecha'], jan_real_t),
    xytext=(df.loc[jan_idx, 'fecha'] + pd.Timedelta(days=50), jan_real_t + 1.8),
    fontsize=9.5, fontweight='bold', color=C_ROJO,
    arrowprops=dict(arrowstyle='->', color=C_ROJO, lw=1.8),
    ha='left', va='bottom',
    bbox=dict(
        boxstyle='round,pad=0.3', facecolor='white',
        edgecolor=C_ROJO, alpha=0.92,
    ),
)

ax1.set_ylabel('Producción (t)', fontsize=11)
ax1.set_title(
    'Predicciones vs. valor real — periodo de test (nov 2024 – ago 2025)',
    fontsize=13, fontweight='bold', pad=12,
)
ax1.legend(
    loc='lower left', frameon=True, fancybox=False,
    edgecolor=C_GRIS, fontsize=9.5,
)
ax1.xaxis.set_major_formatter(mdates.DateFormatter('%b\n%Y'))
ax1.xaxis.set_major_locator(mdates.MonthLocator())
ax1.grid(axis='y', alpha=0.3, linewidth=0.5)

# ─── Panel 2: barras de deterioro Δs ─────────────────────────────
ax2 = fig.add_subplot(gs[1, 0])

det_sorted = sorted(deterioro.items(), key=lambda x: x[1])
nombres = [d[0] for d in det_sorted]
valores = [d[1] for d in det_sorted]
colores = [C_VERDE if n in ('GE', 'GM_v3') else C_ROJO for n in nombres]

bars = ax2.barh(nombres, valores, color=colores, edgecolor='white', height=0.55)
x_max = max(valores) * 1.35
for bar, val in zip(bars, valores):
    ax2.text(
        bar.get_width() + x_max * 0.02,
        bar.get_y() + bar.get_height() / 2,
        f'+{val:.1f} %', va='center', fontsize=10.5,
        fontweight='bold', color=C_GRIS,
    )
ax2.set_xlim(0, x_max)
ax2.set_xlabel('Deterioro Δs (%)', fontsize=11)
ax2.set_title(
    'Deterioro del MAE en meses de shock (Δs)',
    fontsize=12, fontweight='bold', pad=12,
)
ax2.grid(axis='x', alpha=0.3, linewidth=0.5)

# ─── Panel 3: SHAP global vs shocks ──────────────────────────────
ax3 = fig.add_subplot(gs[1, 1])

y_pos = np.arange(len(top5_labels))
bh = 0.34

ax3.barh(
    y_pos + bh / 2, top5_global, bh,
    label='Global', color=C_AZUL, edgecolor='white',
)
ax3.barh(
    y_pos - bh / 2, top5_shock, bh,
    label='En shocks', color=C_ROJO, alpha=0.85, edgecolor='white',
)

ws2m_idx = top5_features.index('WS2M') if 'WS2M' in top5_features else None
if ws2m_idx is not None:
    ax3.annotate(
        '*  x2.5',
        xy=(top5_shock[ws2m_idx], ws2m_idx - bh / 2),
        xytext=(top5_shock[ws2m_idx] + 0.012, ws2m_idx - bh / 2),
        fontsize=11, fontweight='bold', color=C_ROJO, va='center',
    )

ax3.set_yticks(y_pos)
ax3.set_yticklabels(top5_labels)
ax3.invert_yaxis()
ax3.set_xlabel('Importancia SHAP (|φ|)', fontsize=11)
ax3.set_title(
    'SHAP: importancia global vs. shocks (top 5)',
    fontsize=12, fontweight='bold', pad=12,
)
ax3.legend(
    loc='lower right', frameon=True, fancybox=False,
    edgecolor=C_GRIS, fontsize=9.5,
)
ax3.grid(axis='x', alpha=0.3, linewidth=0.5)

# ── 7. Guardar ────────────────────────────────────────────────────
os.makedirs('dashboard', exist_ok=True)
out = 'dashboard/fig_resultados_jornada.png'
fig.savefig(out, dpi=200, facecolor='white', bbox_inches='tight', pad_inches=0.3)
plt.close(fig)

print(f'Figura guardada en {out}')
print('\nDeterioro Ds por modelo:')
for n, v in sorted(deterioro.items(), key=lambda x: x[1]):
    print(f'  {n:10s}: +{v:.1f} %')
