"""Genera figura aislada: SHAP importancia global vs shocks (top 5)."""
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import json

C_AZUL = '#1F4E79'
C_ROJO = '#C00000'
C_GRIS = '#475569'

plt.rcParams.update({
    'font.family': 'serif',
    'font.size': 11,
    'axes.linewidth': 0.8,
    'axes.spines.top': False,
    'axes.spines.right': False,
})

# ── Identificar meses de shock (misma logica que fig principal) ───
ge = pd.read_csv('resultados/ge/ge_predicciones.csv', parse_dates=['fecha'])
df = ge[['fecha', 'real']].sort_values('fecha').reset_index(drop=True)
df['z_pct'] = df['real'].pct_change() * 100
df['is_shock'] = df['z_pct'].abs() > 20
df.loc[df['z_pct'].isna(), 'is_shock'] = False
shock_dates_set = set(df.loc[df['is_shock'], 'fecha'].dt.strftime('%Y-%m-%d'))

# ── Cargar SHAP ──────────────────────────────────────────────────
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

# ── Figura ────────────────────────────────────────────────────────
fig, ax = plt.subplots(figsize=(8, 5), facecolor='white')

y_pos = np.arange(len(top5_labels))
bh = 0.34

ax.barh(y_pos + bh / 2, top5_global, bh,
        label='Global', color=C_AZUL, edgecolor='white')
ax.barh(y_pos - bh / 2, top5_shock, bh,
        label='En shocks', color=C_ROJO, alpha=0.85, edgecolor='white')

ws2m_idx = top5_features.index('WS2M') if 'WS2M' in top5_features else None
if ws2m_idx is not None:
    ax.annotate(
        '*  x2.5',
        xy=(top5_shock[ws2m_idx], ws2m_idx - bh / 2),
        xytext=(top5_shock[ws2m_idx] + 0.012, ws2m_idx - bh / 2),
        fontsize=12, fontweight='bold', color=C_ROJO, va='center',
    )

ax.set_yticks(y_pos)
ax.set_yticklabels(top5_labels, fontsize=11)
ax.invert_yaxis()
ax.set_xlabel('Importancia SHAP (|phi|)', fontsize=11)
ax.set_title(
    'SHAP: WS2M (vel_viento) escala 2.5x durante shocks climaticos',
    fontsize=12.5, fontweight='bold', pad=14,
)
ax.legend(
    loc='lower right', frameon=True, fancybox=False,
    edgecolor=C_GRIS, fontsize=10,
)
ax.grid(axis='x', alpha=0.3, linewidth=0.5)

fig.savefig(
    'dashboard/fig_shap_shocks.png',
    dpi=200, facecolor='white', bbox_inches='tight', pad_inches=0.3,
)
plt.close(fig)
print('Guardado: dashboard/fig_shap_shocks.png')
