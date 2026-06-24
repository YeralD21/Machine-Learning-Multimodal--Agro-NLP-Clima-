"""
Genera dashboard/fig_impacto_economico.png con 3 paneles:
  1. Perdida mensual por modelo (barras agrupadas)
  2. Perdida acumulada (linea temporal)
  3. Simulacion El Nino Q1 2026 (barras)
"""
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import matplotlib.dates as mdates
import matplotlib.ticker as mticker
import os

C_AZUL  = '#1F4E79'
C_VERDE = '#375623'
C_ROJO  = '#C00000'
C_GRIS  = '#475569'

SCALER_MEAN  = 16.32
SCALER_SCALE = 27.99
COSTO_MERMA    = 1.80   # S/kg sobreestimacion
COSTO_STOCKOUT = 3.20   # S/kg subestimacion

def denorm(z):
    return np.asarray(z, dtype=float) * SCALER_SCALE + SCALER_MEAN

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

# Shock classification
df['z_pct'] = df['real'].pct_change() * 100
df['is_shock'] = df['z_pct'].abs() > 20
df.loc[df['z_pct'].isna(), 'is_shock'] = False

# Desnormalize to tons
df['real_t'] = denorm(df['real'])
df['ge_t']   = denorm(df['pred_ge'])
df['xgb_t']  = denorm(df['pred_xgb'])
df['gm_t']   = denorm(df['pred_gm_v3'])

# ── 2. Compute monthly economic loss per model ───────────────────
def compute_loss(pred_t, real_t):
    error_kg = (pred_t - real_t) * 1000
    if error_kg > 0:
        return error_kg * COSTO_MERMA
    else:
        return abs(error_kg) * COSTO_STOCKOUT

models_cfg = [
    ('GE',      'ge_t',  C_AZUL),
    ('GM v3',   'gm_t',  C_VERDE),
    ('XGBoost', 'xgb_t', C_ROJO),
]

for name, col, _ in models_cfg:
    losses = []
    for _, row in df.iterrows():
        if pd.notna(row[col]):
            losses.append(compute_loss(row[col], row['real_t']))
        else:
            losses.append(np.nan)
    df[f'loss_{name}'] = losses

# ══════════════════════════════════════════════════════════════════
# FIGURA
# ══════════════════════════════════════════════════════════════════
fig = plt.figure(figsize=(16, 13), facecolor='white')
gs = gridspec.GridSpec(
    2, 2, height_ratios=[1, 1], width_ratios=[1.2, 1],
    hspace=0.34, wspace=0.30,
    left=0.06, right=0.97, top=0.93, bottom=0.06,
)

# ─── Panel 1: Barras agrupadas perdida mensual ────────────────────
ax1 = fig.add_subplot(gs[0, :])

mes_labels = [d.strftime('%b\n%Y') for d in df['fecha']]
x = np.arange(len(df))
n_models = 3
w = 0.25
offsets = [-(w + 0.02), 0, w + 0.02]

for _, row in df[df['is_shock']].iterrows():
    idx = df.index[df['fecha'] == row['fecha']][0]
    ax1.axvspan(idx - 0.45, idx + 0.45, color=C_ROJO, alpha=0.07, zorder=0)

totals = {}
for i, (name, col, color) in enumerate(models_cfg):
    vals = df[f'loss_{name}'].fillna(0).values
    valid_mask = df[col].notna().values
    bar_vals = np.where(valid_mask, vals, 0)
    totals[name] = df[f'loss_{name}'].sum()

    bars = ax1.bar(
        x + offsets[i], bar_vals, w,
        color=color, alpha=0.80, edgecolor='white', linewidth=0.6,
        label=f'{name} (total S/ {totals[name]:,.0f})',
    )

ax1.set_xticks(x)
ax1.set_xticklabels(mes_labels, fontsize=9)
ax1.set_ylabel('Perdida mensual (S/)', fontsize=11)
ax1.set_title(
    'Perdida economica mensual por error de prediccion',
    fontsize=13, fontweight='bold', pad=12,
)
ax1.legend(loc='upper left', frameon=True, fancybox=False,
           edgecolor=C_GRIS, fontsize=9.5)
ax1.yaxis.set_major_formatter(mticker.FuncFormatter(
    lambda v, _: f'S/ {v:,.0f}'))
ax1.grid(axis='y', alpha=0.3, linewidth=0.5)

# ─── Panel 2: Perdida acumulada ───────────────────────────────────
ax2 = fig.add_subplot(gs[1, 0])

for name, col, color in models_cfg:
    valid = df[df[col].notna()].copy()
    cumsum = valid[f'loss_{name}'].cumsum()
    ax2.plot(
        valid['fecha'], cumsum,
        color=color, linewidth=2.2, marker='o', markersize=4,
        label=name,
    )

for _, row in df[df['is_shock']].iterrows():
    ax2.axvspan(
        row['fecha'] - pd.Timedelta(days=14),
        row['fecha'] + pd.Timedelta(days=14),
        color=C_ROJO, alpha=0.06, zorder=0,
    )

ax2.set_ylabel('Perdida acumulada (S/)', fontsize=11)
ax2.set_title(
    'Perdida acumulada — la separacion crece en shocks',
    fontsize=12, fontweight='bold', pad=10,
)
ax2.legend(loc='upper left', frameon=True, fancybox=False,
           edgecolor=C_GRIS, fontsize=9.5)
ax2.xaxis.set_major_formatter(mdates.DateFormatter('%b\n%Y'))
ax2.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
ax2.yaxis.set_major_formatter(mticker.FuncFormatter(
    lambda v, _: f'S/ {v:,.0f}'))
ax2.grid(axis='y', alpha=0.3, linewidth=0.5)

# ─── Panel 3: Simulacion El Nino Q1 2026 ─────────────────────────
ax3 = fig.add_subplot(gs[1, 1])

nino_models  = ['XGBoost', 'GE', 'GM v3']
nino_losses  = [2_673_000, 2_135_400, 1_765_800]
nino_colors  = [C_ROJO, C_AZUL, C_VERDE]

bars3 = ax3.bar(nino_models, nino_losses, color=nino_colors, alpha=0.85,
                edgecolor='white', width=0.55)

for bar, val in zip(bars3, nino_losses):
    ax3.text(
        bar.get_x() + bar.get_width() / 2, val + 50_000,
        f'S/ {val:,.0f}', ha='center', va='bottom',
        fontsize=10, fontweight='bold', color=C_GRIS,
    )

diff = nino_losses[0] - nino_losses[2]
y_line = (nino_losses[0] + nino_losses[2]) / 2
ax3.annotate(
    '', xy=(0, nino_losses[0] * 0.60), xytext=(2, nino_losses[0] * 0.60),
    arrowprops=dict(arrowstyle='<->', color=C_ROJO, lw=2),
)
ax3.text(
    1, nino_losses[0] * 0.55,
    f'Ahorro\nS/ {diff:,.0f}',
    ha='center', va='top', fontsize=11, fontweight='bold', color=C_ROJO,
)

ax3.set_ylabel('Perdida Q1 2026 (S/)', fontsize=11)
ax3.set_title(
    'Simulacion El Nino 2026\nQ1 perdida proyectada por modelo',
    fontsize=12, fontweight='bold', pad=10,
)
ax3.yaxis.set_major_formatter(mticker.FuncFormatter(
    lambda v, _: f'S/ {v / 1e6:.1f}M'))
ax3.grid(axis='y', alpha=0.3, linewidth=0.5)
ax3.set_ylim(0, max(nino_losses) * 1.20)

# ── Guardar ───────────────────────────────────────────────────────
os.makedirs('dashboard', exist_ok=True)
out = 'dashboard/fig_impacto_economico.png'
fig.savefig(out, dpi=200, facecolor='white', bbox_inches='tight', pad_inches=0.3)
plt.close(fig)

print(f'Figura guardada: {out}')
print(f'\nPerdida total periodo de test:')
for name, _, _ in models_cfg:
    print(f'  {name:10s}: S/ {totals[name]:>10,.0f}')
print(f'\nSimulacion El Nino Q1 2026:')
for m, v in zip(nino_models, nino_losses):
    print(f'  {m:10s}: S/ {v:>12,.0f}')
print(f'  Ahorro GM v3 vs XGBoost: S/ {diff:,.0f}')
