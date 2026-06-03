"""
Reporte Ejecutivo Final — Fase 3
Comparativa de 4 modelos: GC1-SARIMA, GC1-Prophet, GC2-SARIMAX+LSTM, GE-DualLSTM-Attention
Universidad Peruana Union (UPEU) | Proyecto Agro-NLP-Clima
"""

import json, sys
from pathlib import Path
from datetime import date

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import matplotlib.gridspec as gridspec
from matplotlib.backends.backend_pdf import PdfPages
from matplotlib.lines import Line2D
from PIL import Image

# ── Rutas ─────────────────────────────────────────────────────────
ROOT    = Path(r'C:\Machine-learming\Machine-Learning-Multimodal--Agro-NLP-Clima-')
RES     = ROOT / 'resultados'
OUT_PDF = RES / 'reporte_final_fase3.pdf'

# ── Paleta y fuentes ──────────────────────────────────────────────
C = {
    'sarima'  : '#2E86AB',   # azul acero
    'prophet' : '#4C9A52',   # verde
    'gc2'     : '#E07B39',   # naranja
    'ge'      : '#C0392B',   # rojo/carmesi
    'fondo'   : '#F8F9FA',
    'encabez' : '#1A1A2E',
    'linea'   : '#DEE2E6',
    'texto'   : '#2D3436',
    'gris'    : '#636E72',
    'dorado'  : '#F39C12',
}

plt.rcParams.update({
    'font.family'      : 'DejaVu Sans',
    'font.size'        : 9,
    'axes.titlesize'   : 11,
    'axes.labelsize'   : 9,
    'axes.spines.top'  : False,
    'axes.spines.right': False,
    'axes.grid'        : True,
    'grid.alpha'       : 0.35,
    'grid.linestyle'   : '--',
    'figure.facecolor' : 'white',
    'axes.facecolor'   : 'white',
})

# ── Datos de metricas ─────────────────────────────────────────────
MODELOS = {
    'GC1\nSARIMA': {
        'color'      : C['sarima'],
        'mae'        : 0.100639,
        'rmse'       : 0.117860,
        'r2'         : -7.864449,
        'tipo'       : 'Estadistico\nunivar.',
        'descripcion': 'SARIMA(1,0,0)(2,1,0)[12]',
        'n_train'    : 44,
        'n_test'     : 12,
        'split'      : '2024-09-01',
        'pred_png'   : RES / 'gc1' / 'gc1_sarima_prediccion_vs_real.png',
    },
    'GC1\nProphet': {
        'color'      : C['prophet'],
        'mae'        : 0.091884,
        'rmse'       : 0.105071,
        'r2'         : -6.045100,
        'tipo'       : 'Estadistico\nunivar.',
        'descripcion': 'Prophet (additive, m=12)',
        'n_train'    : 44,
        'n_test'     : 12,
        'split'      : '2024-09-01',
        'pred_png'   : RES / 'gc1' / 'gc1_prophet_prediccion_vs_real.png',
    },
    'GC2\nSARIMAX+LSTM': {
        'color'      : C['gc2'],
        'mae'        : 0.196891,
        'rmse'       : 0.292599,
        'r2'         : -53.634372,
        'tipo'       : 'Hibrido\nmultivar.',
        'descripcion': 'SARIMAX(1,0,0)(2,1,0,12) + LSTM-32',
        'n_train'    : 44,
        'n_test'     : 12,
        'split'      : '2024-09-01',
        'pred_png'   : RES / 'gc2' / 'gc2_prediccion_vs_real.png',
    },
    'GE\nDual-LSTM\nAttention': {
        'color'      : C['ge'],
        'mae'        : 0.067272,
        'rmse'       : 0.069804,
        'r2'         : -2.344676,
        'tipo'       : 'Deep\nLearning',
        'descripcion': 'Dual-Input LSTM-64 + Bahdanau Attention',
        'n_train'    : 40,
        'n_test'     : 10,
        'split'      : '2024-11-01',
        'pred_png'   : RES / 'ge' / 'ge_prediccion_vs_real.png',
    },
}

NOMBRES  = list(MODELOS.keys())
COLORES  = [m['color']  for m in MODELOS.values()]
MAES     = [m['mae']    for m in MODELOS.values()]
RMSES    = [m['rmse']   for m in MODELOS.values()]
R2S      = [m['r2']     for m in MODELOS.values()]

# Mejor modelo: menor MAE
idx_best = int(np.argmin(MAES))
# Mejora sobre mejor baseline (Prophet = idx 1)
mae_prophet = MAES[1]
mae_ge      = MAES[3]
rmse_prophet= RMSES[1]
rmse_ge     = RMSES[3]
mejora_mae  = (mae_prophet - mae_ge)  / mae_prophet  * 100
mejora_rmse = (rmse_prophet - rmse_ge) / rmse_prophet * 100

print(f'Mejor modelo: {NOMBRES[idx_best].replace(chr(10)," ")}')
print(f'Mejora MAE  : {mejora_mae:.1f}% sobre Prophet')
print(f'Mejora RMSE : {mejora_rmse:.1f}% sobre Prophet')


# ══════════════════════════════════════════════════════════════════
#  FUNCIONES AUXILIARES
# ══════════════════════════════════════════════════════════════════

def page_fondo(fig):
    """Aplica fondo blanco uniforme a la figura."""
    fig.patch.set_facecolor('white')


def encabezado(fig, titulo, subtitulo='', y_titulo=0.97, fuente=14):
    """Encabezado estandarizado con barra de color superior."""
    ax_h = fig.add_axes([0, 0.955, 1, 0.045])
    ax_h.set_facecolor(C['encabez'])
    ax_h.axis('off')
    ax_h.text(0.012, 0.5, titulo,
              color='white', fontsize=fuente, fontweight='bold',
              va='center', transform=ax_h.transAxes)
    if subtitulo:
        ax_h.text(0.988, 0.5, subtitulo,
                  color='#BDC3C7', fontsize=7.5, va='center',
                  ha='right', transform=ax_h.transAxes)


def pie_pagina(fig, n_pag, total=7):
    ax_p = fig.add_axes([0, 0, 1, 0.025])
    ax_p.set_facecolor(C['encabez'])
    ax_p.axis('off')
    ax_p.text(0.012, 0.5,
              'Universidad Peruana Union (UPEU) · FIA · Proyecto Agro-NLP-Clima · 2026',
              color='#95A5A6', fontsize=6.5, va='center', transform=ax_p.transAxes)
    ax_p.text(0.988, 0.5, f'Pagina {n_pag} de {total}',
              color='#95A5A6', fontsize=6.5, va='center',
              ha='right', transform=ax_p.transAxes)


def fig_nueva(tamanio=(11.69, 8.27)):  # A4 landscape
    fig = plt.figure(figsize=tamanio)
    page_fondo(fig)
    return fig


def embed_png(ax, path: Path):
    """Incrusta una imagen PNG en un Axes sin bordes."""
    if not path.exists():
        ax.text(0.5, 0.5, f'[imagen no encontrada]\n{path.name}',
                ha='center', va='center', fontsize=8, color='red',
                transform=ax.transAxes)
        ax.axis('off')
        return
    img = Image.open(path)
    ax.imshow(np.asarray(img))
    ax.axis('off')


# ══════════════════════════════════════════════════════════════════
#  PAGINAS DEL REPORTE
# ══════════════════════════════════════════════════════════════════

def pagina_portada(pdf):
    fig = fig_nueva()
    encabezado(fig,
        'FASE 3 — REPORTE EJECUTIVO FINAL',
        f'Generado: {date.today().strftime("%d de mayo de %Y")}')
    pie_pagina(fig, 1)

    # Marco central
    ax = fig.add_axes([0.05, 0.10, 0.90, 0.83])
    ax.set_facecolor(C['fondo'])
    ax.set_xlim(0, 1); ax.set_ylim(0, 1)
    ax.axis('off')

    # Titulo principal
    ax.text(0.5, 0.88,
            'Sistema Multimodal de Prediccion Agroindustrial',
            ha='center', va='center', fontsize=18, fontweight='bold',
            color=C['encabez'])
    ax.text(0.5, 0.80,
            'Prediccion de Demanda de Limon Peruano',
            ha='center', va='center', fontsize=14, color=C['gris'])

    # Linea separadora
    ax.axhline(0.76, xmin=0.15, xmax=0.85, color=C['linea'], lw=1.5)

    # Autor / universidad
    ax.text(0.5, 0.71,
            'Fabrizio Sanchez S.',
            ha='center', va='center', fontsize=12, fontweight='bold',
            color=C['texto'])
    ax.text(0.5, 0.66,
            'Facultad de Ingenieria y Arquitectura · UPEU',
            ha='center', va='center', fontsize=10, color=C['gris'])
    ax.text(0.5, 0.61,
            'fabrizio.sanchez.s@upeu.edu.pe',
            ha='center', va='center', fontsize=9, color=C['gris'])

    ax.axhline(0.57, xmin=0.15, xmax=0.85, color=C['linea'], lw=1)

    # Resumen en recuadros de color
    boxes = [
        (0.18, C['sarima'],   'GC1-SARIMA',            'MAE 0.1006'),
        (0.38, C['prophet'],  'GC1-Prophet',            'MAE 0.0919'),
        (0.58, C['gc2'],      'GC2-SARIMAX+LSTM',       'MAE 0.1969'),
        (0.78, C['ge'],       'GE-Dual-LSTM Attention', 'MAE 0.0673'),
    ]
    for xc, col, etiq, val in boxes:
        ax.add_patch(mpatches.FancyBboxPatch(
            (xc - 0.09, 0.42), 0.18, 0.10,
            boxstyle='round,pad=0.01', facecolor=col, alpha=0.15,
            edgecolor=col, linewidth=1.5))
        ax.text(xc, 0.51, etiq, ha='center', va='center',
                fontsize=7.5, fontweight='bold', color=col)
        ax.text(xc, 0.45, val, ha='center', va='center',
                fontsize=9, fontweight='bold', color=C['texto'])

    # Resultado estrella
    ax.add_patch(mpatches.FancyBboxPatch(
        (0.25, 0.20), 0.50, 0.17,
        boxstyle='round,pad=0.02', facecolor=C['ge'], alpha=0.10,
        edgecolor=C['ge'], linewidth=2))
    ax.text(0.50, 0.34,
            'MEJOR MODELO — GE Dual-LSTM + Atencion Bahdanau',
            ha='center', va='center', fontsize=11, fontweight='bold',
            color=C['ge'])
    ax.text(0.50, 0.27,
            f'MAE -26.8% | RMSE -33.6%  vs. mejor baseline (GC1-Prophet)',
            ha='center', va='center', fontsize=10, color=C['texto'])

    # Detalles tecnicos
    ax.text(0.5, 0.12,
            'Python 3.11 · TensorFlow 2.21 · Keras · pmdarima · Prophet · BETO (Fase 2)',
            ha='center', va='center', fontsize=8, color=C['gris'])
    ax.text(0.5, 0.06,
            'Datos: MIDAGRI · NASA POWER · INDECI · Agraria.pe  |  56 meses (2021-2025)',
            ha='center', va='center', fontsize=8, color=C['gris'])

    pdf.savefig(fig, bbox_inches='tight')
    plt.close(fig)
    print('  Pagina 1: Portada')


def pagina_tabla_metrica(pdf):
    fig = fig_nueva()
    encabezado(fig, 'TABLA COMPARATIVA DE METRICAS — FASE 3',
               'Conjunto de prueba 20% (cronologico estricto)')
    pie_pagina(fig, 2)

    ax = fig.add_axes([0.04, 0.08, 0.92, 0.84])
    ax.set_facecolor('white')
    ax.axis('off')

    # Cabeceras
    cols = ['Modelo', 'Tipo', 'Configuracion', 'N Train', 'N Test',
            'Corte', 'MAE', 'RMSE', 'R2', 'Rank']
    widths = [0.13, 0.10, 0.28, 0.06, 0.06, 0.09, 0.07, 0.07, 0.08, 0.06]

    # Posiciones x acumuladas
    xs = [0]
    for w in widths[:-1]:
        xs.append(xs[-1] + w)

    # Fila cabecera
    y_hdr = 0.93
    ax.add_patch(mpatches.FancyBboxPatch(
        (-0.01, y_hdr - 0.025), 1.02, 0.055,
        boxstyle='square,pad=0', facecolor=C['encabez'], clip_on=False))
    for xi, col_txt, w in zip(xs, cols, widths):
        ax.text(xi + w/2, y_hdr, col_txt,
                ha='center', va='center', fontsize=8.5,
                fontweight='bold', color='white')

    # Datos
    filas = [
        ('GC1-SARIMA',
         'Estadist.\nunivar.',
         'SARIMA(1,0,0)(2,1,0)[12]',
         44, 12, '2024-09-01',
         0.100639, 0.117860, -7.8644, 3,
         C['sarima']),
        ('GC1-Prophet',
         'Estadist.\nunivar.',
         'Prophet additive, m=12',
         44, 12, '2024-09-01',
         0.091884, 0.105071, -6.0451, 2,
         C['prophet']),
        ('GC2-SARIMAX+LSTM',
         'Hibrido\nmultivar.',
         'SARIMAX(1,0,0)(2,1,0,12) + LSTM-32',
         44, 12, '2024-09-01',
         0.196891, 0.292599, -53.6344, 4,
         C['gc2']),
        ('GE-DualLSTM\nAttention',
         'Deep\nLearning',
         'Dual-Input LSTM-64 + Bahdanau Attn',
         40, 10, '2024-11-01',
         0.067272, 0.069804, -2.3447, 1,
         C['ge']),
    ]

    # Ordenar por MAE para mostrar
    filas_ord = sorted(filas, key=lambda r: r[6])

    for i, fila in enumerate(filas_ord):
        (nom, tipo, cfg, ntr, nte, corte,
         mae, rmse, r2, rank, col) = fila
        y_row = 0.80 - i * 0.175
        bg_alpha = 0.08 if i % 2 == 0 else 0.03
        # Fondo de fila
        ax.add_patch(mpatches.FancyBboxPatch(
            (-0.01, y_row - 0.062), 1.02, 0.125,
            boxstyle='square,pad=0',
            facecolor=col, alpha=bg_alpha, clip_on=False))
        # Borde izquierdo de color
        ax.add_patch(plt.Rectangle(
            (-0.01, y_row - 0.062), 0.008, 0.125,
            facecolor=col, clip_on=False))

        vals = [nom, tipo, cfg,
                str(ntr), str(nte), corte,
                f'{mae:.4f}', f'{rmse:.4f}', f'{r2:.4f}',
                f'#{rank}']
        for xi, val, w in zip(xs, vals, widths):
            peso = 'bold' if val in (f'#{rank}',) else 'normal'
            color_t = C['ge'] if rank == 1 and val == '#1' else C['texto']
            ax.text(xi + w/2, y_row, val,
                    ha='center', va='center', fontsize=8,
                    fontweight=peso, color=color_t, linespacing=1.3)

    # Nota al pie
    ax.text(0.0, 0.04,
            'Nota: metricas en escala z-score (media provincial, Fase 2). '
            'MAPE excluido — no confiable con series que cruzan cero.',
            fontsize=7.5, color=C['gris'], va='bottom')
    ax.text(0.0, 0.00,
            'GE usa split 2024-11-01 (lags eliminan primeros 6 registros). '
            'Split GC1/GC2: 2024-09-01.',
            fontsize=7.5, color=C['gris'], va='bottom')

    ax.set_xlim(0, 1); ax.set_ylim(-0.02, 1)

    pdf.savefig(fig, bbox_inches='tight')
    plt.close(fig)
    print('  Pagina 2: Tabla comparativa')


def pagina_barras(pdf):
    fig = fig_nueva()
    encabezado(fig, 'COMPARATIVA DE METRICAS — GRAFICOS DE BARRAS',
               'MAE / RMSE / R2 por modelo')
    pie_pagina(fig, 3)

    etiquetas = ['GC1-SARIMA', 'GC1-Prophet', 'GC2-SARIMAX+LSTM', 'GE-Dual-LSTM']
    colores4  = [C['sarima'], C['prophet'], C['gc2'], C['ge']]
    mae_vals  = [0.100639, 0.091884, 0.196891, 0.067272]
    rmse_vals = [0.117860, 0.105071, 0.292599, 0.069804]
    r2_vals   = [-7.864, -6.045, -53.634, -2.345]

    x = np.arange(4)
    w = 0.32

    gs = gridspec.GridSpec(1, 3, figure=fig,
                           left=0.06, right=0.97,
                           top=0.88, bottom=0.14,
                           wspace=0.35)

    # ── MAE ────────────────────────────────────────────────────────
    ax1 = fig.add_subplot(gs[0, 0])
    bars = ax1.bar(x, mae_vals, color=colores4, width=0.55, alpha=0.88,
                   edgecolor='white', linewidth=0.8)
    ax1.set_title('MAE\n(menor es mejor)', fontsize=10, fontweight='bold')
    ax1.set_xticks(x)
    ax1.set_xticklabels(etiquetas, fontsize=7.5, rotation=18, ha='right')
    ax1.set_ylabel('MAE (z-score)')
    # Valores sobre barras
    for bar, v in zip(bars, mae_vals):
        ax1.text(bar.get_x() + bar.get_width()/2, v + 0.003,
                 f'{v:.4f}', ha='center', va='bottom', fontsize=7.5, fontweight='bold')
    # Estrella en GE
    ax1.annotate('Mejor', xy=(x[3], mae_vals[3]), xytext=(x[3], mae_vals[3] + 0.025),
                 ha='center', fontsize=7.5, color=C['ge'], fontweight='bold',
                 arrowprops=dict(arrowstyle='->', color=C['ge'], lw=1.2))
    # Linea de referencia Prophet
    ax1.axhline(mae_vals[1], color=C['prophet'], ls=':', lw=1.2, alpha=0.7,
                label=f'Prophet: {mae_vals[1]:.4f}')
    ax1.legend(fontsize=7, loc='upper right')
    ax1.set_ylim(0, max(mae_vals) * 1.32)

    # ── RMSE ───────────────────────────────────────────────────────
    ax2 = fig.add_subplot(gs[0, 1])
    bars2 = ax2.bar(x, rmse_vals, color=colores4, width=0.55, alpha=0.88,
                    edgecolor='white', linewidth=0.8)
    ax2.set_title('RMSE\n(menor es mejor)', fontsize=10, fontweight='bold')
    ax2.set_xticks(x)
    ax2.set_xticklabels(etiquetas, fontsize=7.5, rotation=18, ha='right')
    ax2.set_ylabel('RMSE (z-score)')
    for bar, v in zip(bars2, rmse_vals):
        ax2.text(bar.get_x() + bar.get_width()/2, v + 0.004,
                 f'{v:.4f}', ha='center', va='bottom', fontsize=7.5, fontweight='bold')
    ax2.annotate('Mejor', xy=(x[3], rmse_vals[3]), xytext=(x[3], rmse_vals[3] + 0.04),
                 ha='center', fontsize=7.5, color=C['ge'], fontweight='bold',
                 arrowprops=dict(arrowstyle='->', color=C['ge'], lw=1.2))
    ax2.axhline(rmse_vals[1], color=C['prophet'], ls=':', lw=1.2, alpha=0.7,
                label=f'Prophet: {rmse_vals[1]:.4f}')
    ax2.legend(fontsize=7, loc='upper right')
    ax2.set_ylim(0, max(rmse_vals) * 1.32)

    # ── R2 ─────────────────────────────────────────────────────────
    ax3 = fig.add_subplot(gs[0, 2])
    r2_plot = [max(v, -15) for v in r2_vals]   # limitar GC2 para visualizacion
    bars3 = ax3.bar(x, r2_plot, color=colores4, width=0.55, alpha=0.88,
                    edgecolor='white', linewidth=0.8)
    ax3.set_title('R2  (mayor es mejor)\n[GC2 truncado en -15]',
                  fontsize=10, fontweight='bold')
    ax3.set_xticks(x)
    ax3.set_xticklabels(etiquetas, fontsize=7.5, rotation=18, ha='right')
    ax3.set_ylabel('R2')
    ax3.axhline(0, color='black', lw=0.8, ls='-', alpha=0.5)
    for i_, (bar, v) in enumerate(zip(bars3, r2_vals)):
        txt = f'{v:.3f}' if v > -15 else f'{v:.1f}\n(trunc.)'
        ypos = r2_plot[i_] - 0.6
        ax3.text(bar.get_x() + bar.get_width()/2, ypos,
                 txt, ha='center', va='top', fontsize=7, fontweight='bold',
                 color='white' if r2_plot[i_] < -3 else C['texto'])
    ax3.annotate('Mejor', xy=(x[3], r2_plot[3]),
                 xytext=(x[3], r2_plot[3] + 1.5),
                 ha='center', fontsize=7.5, color=C['ge'], fontweight='bold',
                 arrowprops=dict(arrowstyle='->', color=C['ge'], lw=1.2))
    ax3.set_ylim(min(r2_plot) * 1.15, max(r2_plot) + 3.5)

    # Leyenda de modelos (comun a los 3)
    leyenda = [mpatches.Patch(facecolor=c, alpha=0.85, label=l)
               for c, l in zip(colores4, etiquetas)]
    fig.legend(handles=leyenda, loc='lower center', ncol=4,
               fontsize=8, framealpha=0.9, bbox_to_anchor=(0.5, 0.02))

    pdf.savefig(fig, bbox_inches='tight')
    plt.close(fig)
    print('  Pagina 3: Graficos de barras comparativos')


def pagina_predicciones_gc1(pdf):
    fig = fig_nueva()
    encabezado(fig, 'GC1 — PREDICCIONES VS REAL',
               'SARIMA (izq.) y Prophet (der.) — baseline estadistico univariado')
    pie_pagina(fig, 4)

    gs = gridspec.GridSpec(1, 2, figure=fig,
                           left=0.04, right=0.97,
                           top=0.87, bottom=0.06, wspace=0.06)

    ax_l = fig.add_subplot(gs[0, 0])
    ax_r = fig.add_subplot(gs[0, 1])

    embed_png(ax_l, RES / 'gc1' / 'gc1_sarima_prediccion_vs_real.png')
    embed_png(ax_r, RES / 'gc1' / 'gc1_prophet_prediccion_vs_real.png')

    ax_l.set_title('GC1-SARIMA  |  MAE=0.1006  RMSE=0.1179  R2=-7.86',
                   fontsize=9, fontweight='bold', color=C['sarima'], pad=6)
    ax_r.set_title('GC1-Prophet  |  MAE=0.0919  RMSE=0.1051  R2=-6.05',
                   fontsize=9, fontweight='bold', color=C['prophet'], pad=6)

    pdf.savefig(fig, bbox_inches='tight')
    plt.close(fig)
    print('  Pagina 4: Predicciones GC1')


def pagina_predicciones_gc2_ge(pdf):
    fig = fig_nueva()
    encabezado(fig, 'GC2 & GE — PREDICCIONES VS REAL',
               'SARIMAX+LSTM (izq.) y GE Dual-LSTM-Attention (der.)')
    pie_pagina(fig, 5)

    gs = gridspec.GridSpec(1, 2, figure=fig,
                           left=0.04, right=0.97,
                           top=0.87, bottom=0.06, wspace=0.06)

    ax_l = fig.add_subplot(gs[0, 0])
    ax_r = fig.add_subplot(gs[0, 1])

    embed_png(ax_l, RES / 'gc2' / 'gc2_prediccion_vs_real.png')
    embed_png(ax_r, RES / 'ge'  / 'ge_prediccion_vs_real.png')

    ax_l.set_title('GC2-SARIMAX+LSTM  |  MAE=0.1969  RMSE=0.2926  R2=-53.63',
                   fontsize=9, fontweight='bold', color=C['gc2'], pad=6)
    ax_r.set_title('GE-Dual-LSTM Attention  |  MAE=0.0673  RMSE=0.0698  R2=-2.34',
                   fontsize=9, fontweight='bold', color=C['ge'], pad=6)

    pdf.savefig(fig, bbox_inches='tight')
    plt.close(fig)
    print('  Pagina 5: Predicciones GC2 y GE')


def pagina_atencion_entrenamiento(pdf):
    fig = fig_nueva()
    encabezado(fig, 'GE — ATENCION BAHDANAU Y CURVA DE ENTRENAMIENTO',
               'Interpretabilidad y convergencia del modelo experimental')
    pie_pagina(fig, 6)

    gs = gridspec.GridSpec(1, 2, figure=fig,
                           left=0.04, right=0.97,
                           top=0.87, bottom=0.06, wspace=0.08)

    ax_l = fig.add_subplot(gs[0, 0])
    ax_r = fig.add_subplot(gs[0, 1])

    embed_png(ax_l, RES / 'ge' / 'ge_attention_weights.png')
    embed_png(ax_r, RES / 'ge' / 'ge_training_curve.png')

    ax_l.set_title('Pesos de Atencion Bahdanau — Canal A y Canal B\n'
                   '(timesteps mas relevantes para la prediccion)',
                   fontsize=9, fontweight='bold', color=C['ge'], pad=6)
    ax_r.set_title('Curva de entrenamiento — GE Dual-LSTM\n'
                   f'(early stopping en epoca 20)',
                   fontsize=9, fontweight='bold', color=C['ge'], pad=6)

    pdf.savefig(fig, bbox_inches='tight')
    plt.close(fig)
    print('  Pagina 6: Atencion y curva de entrenamiento')


def pagina_conclusiones(pdf):
    fig = fig_nueva()
    encabezado(fig, 'CONCLUSIONES Y PROXIMOS PASOS — FASE 3',
               'Reporte Final · Proyecto Agro-NLP-Clima · UPEU 2026')
    pie_pagina(fig, 7)

    ax = fig.add_axes([0.04, 0.06, 0.92, 0.85])
    ax.set_facecolor('white')
    ax.set_xlim(0, 1); ax.set_ylim(0, 1)
    ax.axis('off')

    y = 0.96
    def titulo_sec(txt, yp):
        ax.add_patch(mpatches.FancyBboxPatch(
            (0, yp - 0.018), 1.0, 0.038,
            boxstyle='round,pad=0.005',
            facecolor=C['encabez'], alpha=0.88, clip_on=False))
        ax.text(0.012, yp, txt, va='center', fontsize=10,
                fontweight='bold', color='white')
        return yp - 0.065

    def punto(txt, yp, col=C['texto'], indent=0.03, size=9):
        ax.text(indent, yp, u'●', va='top', fontsize=7, color=col)
        ax.text(indent + 0.022, yp, txt, va='top', fontsize=size,
                color=col, wrap=True,
                bbox=dict(boxstyle='square,pad=0', alpha=0, linewidth=0))
        lineas = max(1, len(txt) // 95)
        return yp - 0.050 * lineas - 0.010

    # Seccion 1: Resultados
    y = titulo_sec('1. Resultados Principales de Fase 3', 0.93)
    y = punto('GC1-SARIMA: MAE=0.1006 | RMSE=0.1179 | R2=-7.86  '
              '-- Baseline univariado estadistico (referencia inferior)', y, C['sarima'])
    y = punto('GC1-Prophet: MAE=0.0919 | RMSE=0.1051 | R2=-6.05  '
              '-- Mejor baseline. Supera a SARIMA en MAE -8.7%, RMSE -10.9%', y, C['prophet'])
    y = punto('GC2-SARIMAX+LSTM: MAE=0.1969 | RMSE=0.2926 | R2=-53.63  '
              '-- Peor resultado. Sobreajuste por maldicion de dimensionalidad con ~32 '
              'observaciones efectivas post-diferenciacion estacional y 8 variables exogenas.', y, C['gc2'])
    y = punto('GE-Dual-LSTM Attention: MAE=0.0673 | RMSE=0.0698 | R2=-2.34  '
              '-- MEJOR MODELO. Supera al baseline Prophet en MAE -26.8% y RMSE -33.6%.', y, C['ge'])

    # Seccion 2: Hallazgos
    y -= 0.015
    y = titulo_sec('2. Hallazgos Clave', y + 0.018)
    y = punto('La arquitectura Dual-Input LSTM con Atencion de Bahdanau (Gu et al., 2022) supera a todos '
              'los modelos estadisticos clasicos y al hibrido SARIMAX+LSTM en todas las metricas.', y)
    y = punto('R2 negativo en todos los modelos indica que la caida pronunciada de produccion '
              '(sep-2024 a ago-2025) no es capturada por ninguna arquitectura univariada ni multivariada '
              'actual. Esto motiva la inclusion de variables NLP y multimodales (Fase 4).', y)
    y = punto('El MAPE es no confiable con datos en escala z-score que cruzan por cero. '
              'Se recomienda usar MAE y RMSE como metricas primarias en todas las fases.', y)
    y = punto('GC2 demuestra la maldicion de la dimensionalidad: agregar variables exogenas sin datos '
              'suficientes perjudica el rendimiento. El GE lo supera con regularizacion L2 + Dropout + '
              'Atencion temporal que enfoca el aprendizaje en los timesteps relevantes.', y)

    # Seccion 3: Proximos pasos
    y -= 0.010
    y = titulo_sec('3. Proximos Pasos — Fase 4', y + 0.018)

    tabla_prox = [
        ('Act. 15', 'Modelo Multimodal',   'LSTM-Attention + BETO (embeddings NLP)',    'Pendiente'),
        ('Act. 16', 'Analisis SHAP',        'Explicabilidad XAI — importancia variables', 'Pendiente'),
        ('Act. 17', 'Dashboard Streamlit',  'Visualizacion interactiva de predicciones', 'Pendiente'),
    ]

    y -= 0.005
    col_xs = [0.01, 0.10, 0.28, 0.62, 0.88]
    hdrs   = ['Activ.', 'Modelo', 'Descripcion', 'Estado']
    ax.add_patch(mpatches.FancyBboxPatch(
        (0.01, y - 0.020), 0.98, 0.038,
        boxstyle='square,pad=0', facecolor=C['encabez'], alpha=0.7))
    for xi, h in zip(col_xs[:-1], hdrs):
        ax.text(xi + 0.01, y - 0.002, h, va='center', fontsize=8,
                fontweight='bold', color='white')

    for j, (act, mod, desc, estado) in enumerate(tabla_prox):
        y -= 0.048
        bg = C['fondo'] if j % 2 == 0 else 'white'
        ax.add_patch(mpatches.FancyBboxPatch(
            (0.01, y - 0.020), 0.98, 0.040,
            boxstyle='square,pad=0', facecolor=bg, alpha=0.6))
        for xi, txt in zip(col_xs[:-1], [act, mod, desc, estado]):
            ax.text(xi + 0.01, y - 0.002, txt, va='center',
                    fontsize=8, color=C['texto'])

    pdf.savefig(fig, bbox_inches='tight')
    plt.close(fig)
    print('  Pagina 7: Conclusiones y proximos pasos')


# ══════════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════════

def main():
    print(f'Generando reporte: {OUT_PDF}')
    with PdfPages(str(OUT_PDF)) as pdf:
        # Metadatos del PDF
        d = pdf.infodict()
        d['Title']   = 'Reporte Ejecutivo Final Fase 3 — Agro-NLP-Clima'
        d['Author']  = 'Fabrizio Sanchez S. — UPEU'
        d['Subject'] = 'Comparativa GC1-SARIMA, GC1-Prophet, GC2-SARIMAX+LSTM, GE-DualLSTM-Attention'
        d['Keywords']= 'LSTM Attention SARIMA Prophet Agro Forecast Peru'
        d['Creator'] = 'Claude Code + matplotlib PdfPages'

        pagina_portada(pdf)
        pagina_tabla_metrica(pdf)
        pagina_barras(pdf)
        pagina_predicciones_gc1(pdf)
        pagina_predicciones_gc2_ge(pdf)
        pagina_atencion_entrenamiento(pdf)
        pagina_conclusiones(pdf)

    tam_kb = OUT_PDF.stat().st_size / 1024
    print(f'\nReporte generado: {OUT_PDF}')
    print(f'Tamano: {tam_kb:.0f} KB  |  7 paginas')


if __name__ == '__main__':
    main()
