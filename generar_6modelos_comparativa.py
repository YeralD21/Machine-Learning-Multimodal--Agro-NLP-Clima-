"""
Genera figura comparativa de 6 modelos: tabla de metricas + barras MAE global vs shocks.
"""
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import numpy as np

C_VERDE = '#375623'
C_ROJO  = '#C00000'
C_GRIS  = '#475569'
C_AZUL  = '#1F4E79'

plt.rcParams.update({
    'font.family': 'sans-serif',
    'font.sans-serif': ['Arial', 'Helvetica', 'DejaVu Sans'],
    'font.size': 10,
    'axes.linewidth': 0.8,
    'axes.spines.top': False,
    'axes.spines.right': False,
})

# ── Datos exactos del usuario ─────────────────────────────────────
models = ['SARIMA', 'Prophet', 'SARIMAX-LSTM', 'XGBoost', 'GE', 'GM v3']
mae    = [0.101,    0.092,     0.197,           0.047,     0.067, 0.065]
rmse   = [0.118,    0.105,     0.293,           0.054,     0.070, 0.071]
r2     = [-7.86,    -6.05,     -53.63,          -1.02,     -2.34, -8.18]
ds     = [None,     None,      None,            16.5,      2.3,   11.7]

mae_shock = {}
for i, d in enumerate(ds):
    if d is not None:
        mae_shock[i] = mae[i] * (1 + d / 100)


def model_color(i):
    if models[i] in ('GE', 'GM v3'):
        return C_VERDE
    if ds[i] is not None and ds[i] > 15:
        return C_ROJO
    return C_GRIS


# ── Figura ────────────────────────────────────────────────────────
fig = plt.figure(figsize=(15, 12.5), facecolor='white')
gs = gridspec.GridSpec(
    2, 1, height_ratios=[0.85, 1.15], hspace=0.32,
    top=0.95, bottom=0.06, left=0.06, right=0.97,
)

# ═══════════════════════════════════════════════════════════════════
# Panel 1 — Tabla visual de metricas
# ═══════════════════════════════════════════════════════════════════
ax1 = fig.add_subplot(gs[0])
ax1.axis('off')
ax1.set_title(
    'Comparativa de metricas — 6 modelos',
    fontsize=15, fontweight='bold', pad=18, loc='center',
)

col_labels = ['Modelo', 'MAE', 'RMSE', u'R²', u'Δs (%)']
cell_text = []
for i in range(len(models)):
    cell_text.append([
        models[i],
        f'{mae[i]:.3f}',
        f'{rmse[i]:.3f}',
        f'{r2[i]:.2f}',
        f'+{ds[i]:.1f}' if ds[i] is not None else u'—',
    ])

best = {
    1: int(np.argmin(mae)),
    2: int(np.argmin(rmse)),
    3: int(np.argmax(r2)),
}
worst = {
    1: int(np.argmax(mae)),
    2: int(np.argmax(rmse)),
    3: int(np.argmin(r2)),
}

ds_valid = [(ds[i], i) for i in range(len(ds)) if ds[i] is not None]
if ds_valid:
    best[4]  = min(ds_valid, key=lambda x: x[0])[1]
    worst[4] = max(ds_valid, key=lambda x: x[0])[1]

table = ax1.table(
    cellText=cell_text, colLabels=col_labels,
    loc='center', cellLoc='center',
)
table.auto_set_font_size(False)
table.set_fontsize(12)
table.scale(1, 2.3)

for j in range(len(col_labels)):
    cell = table[0, j]
    cell.set_facecolor(C_AZUL)
    cell.set_text_props(color='white', fontweight='bold', fontsize=12)
    cell.set_edgecolor(C_AZUL)

for i in range(len(models)):
    for j in range(len(col_labels)):
        cell = table[i + 1, j]
        cell.set_edgecolor('#D0D0D0')

        if j == 0:
            cell.set_text_props(fontweight='bold', fontsize=11)
            continue

        if j in best and best[j] == i:
            cell.set_facecolor('#C6EFCE')
            cell.set_text_props(fontweight='bold', color=C_VERDE)
        elif j in worst and worst[j] == i:
            cell.set_facecolor('#FFC7CE')
            cell.set_text_props(fontweight='bold', color=C_ROJO)

# ═══════════════════════════════════════════════════════════════════
# Panel 2 — Barras MAE global vs MAE en shocks
# ═══════════════════════════════════════════════════════════════════
ax2 = fig.add_subplot(gs[1])

order = np.argsort(mae)
s_models = [models[i] for i in order]
s_mae    = [mae[i]    for i in order]
s_ds     = [ds[i]     for i in order]
s_colors = [model_color(i) for i in order]
s_orig   = list(order)

x = np.arange(len(s_models))
w = 0.30

for pos in range(len(s_models)):
    oi = s_orig[pos]
    col = s_colors[pos]

    if oi in mae_shock:
        ax2.bar(x[pos] - w / 2, s_mae[pos], w,
                color=col, alpha=0.45, edgecolor='white', linewidth=0.8)
        ax2.bar(x[pos] + w / 2, mae_shock[oi], w,
                color=col, alpha=0.92, edgecolor='white', linewidth=0.8)
    else:
        ax2.bar(x[pos], s_mae[pos], w * 1.3,
                color=col, alpha=0.60, edgecolor='white', linewidth=0.8)

for pos in range(len(s_models)):
    oi = s_orig[pos]
    d  = s_ds[pos]
    if d is not None:
        y_top = max(s_mae[pos], mae_shock[oi]) + 0.004
        ax2.text(
            x[pos], y_top,
            f'+{d:.1f} %', ha='center', va='bottom',
            fontsize=10.5, fontweight='bold', color=s_colors[pos],
        )

from matplotlib.patches import Patch
legend_handles = [
    Patch(facecolor=C_GRIS, alpha=0.45, label='MAE global'),
    Patch(facecolor=C_AZUL, alpha=0.92, label='MAE en shocks'),
]
ax2.legend(
    handles=legend_handles, loc='upper left',
    fontsize=10.5, frameon=True, fancybox=False, edgecolor='#CCCCCC',
)

ax2.set_xticks(x)
ax2.set_xticklabels(s_models, fontsize=11.5, fontweight='bold')
ax2.set_ylabel('MAE (z-score)', fontsize=12)
ax2.set_title(
    'MAE global vs. MAE en meses de shock',
    fontsize=14, fontweight='bold', pad=14,
)
ax2.grid(axis='y', alpha=0.3, linewidth=0.5)
ax2.set_ylim(0, max(s_mae) * 1.18)

# ── Guardar ───────────────────────────────────────────────────────
fig.savefig(
    'dashboard/fig_6modelos_comparativa.png',
    dpi=200, facecolor='white', bbox_inches='tight', pad_inches=0.3,
)
plt.close(fig)
print('Guardado: dashboard/fig_6modelos_comparativa.png')
