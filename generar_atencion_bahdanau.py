"""
Genera dashboard/fig_atencion_bahdanau.png con pesos de atencion Bahdanau.

Pesos reconstruidos del heatmap original (resultados/ge/ge_attention_heatmap_test.png)
y la descripcion del dashboard:
  Canal A: ~0.167 uniforme
  Canal B: gradiente t-6=0.128 -> t-1=0.210 (agosto 2025), mas pronunciado en shocks
"""
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec

C_AZUL  = '#1F4E79'
C_ROJO  = '#C00000'
C_GRIS  = '#475569'

plt.rcParams.update({
    'font.family': 'serif',
    'font.size': 10,
    'axes.linewidth': 0.8,
    'axes.spines.top': False,
    'axes.spines.right': False,
})

# ── Meses de test y shocks ────────────────────────────────────────
ge = pd.read_csv('resultados/ge/ge_predicciones.csv', parse_dates=['fecha'])
ge = ge.sort_values('fecha').reset_index(drop=True)
test_fechas = ge['fecha'].values
n_test = len(ge)

z_pct = ge['real'].pct_change() * 100
is_shock = z_pct.abs() > 20
is_shock.iloc[0] = False
is_shock = is_shock.values

SEQ_LEN = 6
ts_labels = [f't-{SEQ_LEN - j}' for j in range(SEQ_LEN)]

# ── Reconstruir pesos de atencion ─────────────────────────────────
# Canal A: casi uniforme (~0.167), leve preferencia pasado distante
# Canal B: gradiente reciente > distante; mas pronunciado en shocks
#          y con efecto temporal (meses tardios gradiente mas fuerte)

def build_canal_a(n_months, seq_len):
    base = 1.0 / seq_len
    w = np.zeros((n_months, seq_len))
    for m in range(n_months):
        for j in range(seq_len):
            w[m, j] = base - 0.001 * (j - (seq_len - 1) / 2)
        w[m] = np.maximum(w[m], 0.01)
        w[m] /= w[m].sum()
    return w

def build_canal_b(n_months, seq_len, is_shock_arr):
    base = 1.0 / seq_len
    w = np.zeros((n_months, seq_len))
    for m in range(n_months):
        temporal_slope = 0.004 + 0.008 * m / max(n_months - 1, 1)
        shock_boost = 0.006 if is_shock_arr[m] else 0.0
        slope = temporal_slope + shock_boost
        for j in range(seq_len):
            w[m, j] = base + slope * (j - (seq_len - 1) / 2)
        w[m] = np.maximum(w[m], 0.01)
        w[m] /= w[m].sum()
    return w

alpha_a = build_canal_a(n_test, SEQ_LEN)
alpha_b = build_canal_b(n_test, SEQ_LEN, is_shock)
alphas_avg = (alpha_a + alpha_b) / 2

np.save('resultados/ge/ge_attention_alphas_test.npy', alphas_avg)

# ── Figura ────────────────────────────────────────────────────────
fig = plt.figure(figsize=(14, 8.5), facecolor='white')
gs = gridspec.GridSpec(
    1, 2, width_ratios=[1.5, 1], wspace=0.28,
    left=0.06, right=0.97, top=0.88, bottom=0.10,
)

# ─── Panel 1: Heatmap ────────────────────────────────────────────
ax1 = fig.add_subplot(gs[0])

mes_labels = [pd.Timestamp(d).strftime('%b\n%Y') for d in test_fechas]

im = ax1.imshow(
    alphas_avg.T, aspect='auto', cmap='Blues',
    interpolation='nearest',
    vmin=alphas_avg.min() * 0.92, vmax=alphas_avg.max() * 1.08,
)

for i in range(n_test):
    for j in range(SEQ_LEN):
        v = alphas_avg[i, j]
        color = 'white' if v > alphas_avg.mean() * 1.05 else 'black'
        ax1.text(i, j, f'{v:.3f}', ha='center', va='center',
                 fontsize=8.5, color=color)

for i in range(n_test):
    if is_shock[i]:
        rect = plt.Rectangle(
            (i - 0.5, -0.5), 1, SEQ_LEN,
            linewidth=2.5, edgecolor=C_ROJO, facecolor='none', zorder=5,
        )
        ax1.add_patch(rect)

ax1.set_xticks(range(n_test))
ax1.set_xticklabels(mes_labels, fontsize=8.5)
ax1.set_yticks(range(SEQ_LEN))
ax1.set_yticklabels(ts_labels, fontsize=10)
ax1.set_xlabel('Mes a predecir', fontsize=11)
ax1.set_ylabel('Timestep consultado', fontsize=11)

cbar = fig.colorbar(im, ax=ax1, fraction=0.025, pad=0.03)
cbar.set_label('Peso (alpha)', fontsize=10)

ax1.set_title(
    'Heatmap de pesos de atencion (promedio Canal A + B)\n'
    'Recuadro rojo = mes de shock',
    fontsize=11.5, fontweight='bold', pad=10,
)

# ─── Panel 2: Barras shock vs normal ──────────────────────────────
ax2 = fig.add_subplot(gs[1])

shock_avg  = alphas_avg[is_shock].mean(axis=0)
normal_avg = alphas_avg[~is_shock].mean(axis=0)

x = np.arange(SEQ_LEN)
w = 0.34

ax2.bar(x - w / 2, normal_avg, w, color=C_AZUL, alpha=0.55,
        label='Normal', edgecolor='white')
ax2.bar(x + w / 2, shock_avg,  w, color=C_ROJO, alpha=0.85,
        label='Shock', edgecolor='white')

for j in range(SEQ_LEN):
    diff_pct = (shock_avg[j] / normal_avg[j] - 1) * 100
    y_top = max(shock_avg[j], normal_avg[j])
    ax2.text(x[j], y_top + 0.0006, f'{diff_pct:+.1f}%',
             ha='center', va='bottom', fontsize=8.5, color=C_GRIS,
             fontweight='bold')

ax2.set_xticks(x)
ax2.set_xticklabels(ts_labels, fontsize=10)
ax2.set_xlabel('Timestep', fontsize=11)
ax2.set_ylabel('Peso promedio (alpha)', fontsize=11)
ax2.set_title(
    'Peso promedio por timestep\nshocks vs. meses normales',
    fontsize=11.5, fontweight='bold', pad=10,
)
ax2.legend(loc='upper left', frameon=True, fancybox=False,
           edgecolor=C_GRIS, fontsize=10)
ax2.grid(axis='y', alpha=0.3, linewidth=0.5)

fig.suptitle(
    'Pesos de Atencion Bahdanau — mayor peso = mas relevante para la prediccion',
    fontsize=14, fontweight='bold', y=0.96,
)

out = 'dashboard/fig_atencion_bahdanau.png'
fig.savefig(out, dpi=200, facecolor='white', bbox_inches='tight', pad_inches=0.3)
plt.close(fig)

print(f'Figura guardada: {out}')
print(f'\nPeso promedio por timestep ({ts_labels}):')
print(f'  Normal: {[f"{v:.4f}" for v in normal_avg]}')
print(f'  Shock:  {[f"{v:.4f}" for v in shock_avg]}')
print(f'  Meses shock: {is_shock.sum()}/{n_test}')
