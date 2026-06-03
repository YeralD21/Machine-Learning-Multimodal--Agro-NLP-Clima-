"""
Genera el reporte ejecutivo GC1 en PDF usando matplotlib.PdfPages.
Sin dependencias externas — solo matplotlib (ya instalado en el venv).
"""
import json
import textwrap
from pathlib import Path

import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import matplotlib.image as mpimg
from matplotlib.backends.backend_pdf import PdfPages
import numpy as np

# ---------------------------------------------------------------------------
# Configuración
# ---------------------------------------------------------------------------
ROOT    = Path(r"C:\Machine-learming\Machine-Learning-Multimodal--Agro-NLP-Clima-")
GC1_DIR = ROOT / "resultados" / "gc1"
OUT_PDF = GC1_DIR / "reporte_ejecutivo_gc1.pdf"

plt.rcParams.update({
    "font.family": "DejaVu Sans",
    "font.size": 10,
    "figure.facecolor": "white",
    "axes.facecolor": "white",
})

AZUL    = "#1a4f72"
GRIS    = "#5d6d7e"
NARANJA = "#e67e22"
VERDE   = "#1e8449"
ROJO    = "#c0392b"
FONDO   = "#f4f6f7"

# ---------------------------------------------------------------------------
# Datos
# ---------------------------------------------------------------------------
with open(GC1_DIR / "gc1_sarima_metricas.json", encoding="utf-8") as f:
    sarima = json.load(f)
with open(GC1_DIR / "gc1_prophet_metricas.json", encoding="utf-8") as f:
    prophet = json.load(f)

FECHA_HOY    = "25 de mayo de 2026"
SPLIT_DATE   = "2024-09-01"
TRAIN_RANGE  = "2021-01-01 → 2024-08-01"
TEST_RANGE   = "2024-09-01 → 2025-08-01"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def header_strip(ax, titulo, subtitulo="", color=AZUL):
    ax.set_facecolor(color)
    ax.set_xlim(0, 1); ax.set_ylim(0, 1)
    ax.axis("off")
    ax.text(0.5, 0.62, titulo, ha="center", va="center",
            fontsize=14, fontweight="bold", color="white", wrap=True)
    if subtitulo:
        ax.text(0.5, 0.30, subtitulo, ha="center", va="center",
                fontsize=9, color="#dfe6e9")


def tabla_metricas(ax, filas, titulo):
    ax.axis("off")
    ax.set_facecolor(FONDO)
    ax.text(0.5, 0.97, titulo, ha="center", va="top",
            fontsize=11, fontweight="bold", color=AZUL,
            transform=ax.transAxes)
    col_labels = ["Métrica", "Valor"]
    tabla = ax.table(
        cellText=filas,
        colLabels=col_labels,
        loc="center",
        cellLoc="center",
        bbox=[0.05, 0.08, 0.90, 0.82],
    )
    tabla.auto_set_font_size(False)
    tabla.set_fontsize(10)
    for (r, c), cell in tabla.get_celld().items():
        cell.set_edgecolor("#bdc3c7")
        if r == 0:
            cell.set_facecolor(AZUL)
            cell.set_text_props(color="white", fontweight="bold")
        elif r % 2 == 0:
            cell.set_facecolor("#eaf0fb")
        else:
            cell.set_facecolor("white")


def imagen(ax, path, titulo=""):
    try:
        img = mpimg.imread(str(path))
        ax.imshow(img, aspect="auto")
        ax.axis("off")
        if titulo:
            ax.set_title(titulo, fontsize=9, color=GRIS, pad=4)
    except Exception as e:
        ax.axis("off")
        ax.text(0.5, 0.5, f"[Imagen no encontrada]\n{path.name}",
                ha="center", va="center", color="red", fontsize=8,
                transform=ax.transAxes)


def nota(ax, texto, color=GRIS):
    ax.axis("off")
    ax.text(0.5, 0.5, texto, ha="center", va="center",
            fontsize=8, color=color, style="italic",
            transform=ax.transAxes,
            bbox=dict(boxstyle="round,pad=0.4", facecolor="#fdfefe",
                      edgecolor="#bdc3c7", alpha=0.9))


def comparativa_bar(ax, sarima_vals, prophet_vals, labels):
    x = np.arange(len(labels))
    w = 0.35
    b1 = ax.bar(x - w/2, sarima_vals,  w, label="SARIMA", color=AZUL,    alpha=0.85)
    b2 = ax.bar(x + w/2, prophet_vals, w, label="Prophet", color=NARANJA, alpha=0.85)
    ax.set_xticks(x); ax.set_xticklabels(labels, fontsize=10)
    ax.legend(fontsize=9)
    ax.set_title("Comparativa GC1: MAE y RMSE", fontsize=11, fontweight="bold", color=AZUL)
    ax.set_ylabel("Valor (z-score)")
    ax.spines[["top","right"]].set_visible(False)
    for bar in [*b1, *b2]:
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.001,
                f"{bar.get_height():.4f}", ha="center", va="bottom", fontsize=8)


# ---------------------------------------------------------------------------
# Generación del PDF
# ---------------------------------------------------------------------------
with PdfPages(OUT_PDF) as pdf:

    # ====================================================================
    # PÁGINA 1 — Portada
    # ====================================================================
    fig = plt.figure(figsize=(11, 8.5))
    fig.patch.set_facecolor(AZUL)

    ax_logo = fig.add_axes([0.0, 0.82, 1.0, 0.18])
    ax_logo.set_facecolor(AZUL); ax_logo.axis("off")
    ax_logo.text(0.5, 0.65, "UNIVERSIDAD PERUANA UNIÓN — UPEU",
                 ha="center", fontsize=11, color="#aed6f1", fontweight="bold")
    ax_logo.text(0.5, 0.20, "Facultad de Ingeniería y Arquitectura",
                 ha="center", fontsize=9, color="#7fb3d3")

    ax_main = fig.add_axes([0.08, 0.32, 0.84, 0.48])
    ax_main.set_facecolor("#1a4f72"); ax_main.axis("off")
    ax_main.set_xlim(0,1); ax_main.set_ylim(0,1)
    ax_main.text(0.5, 0.88,
                 "REPORTE EJECUTIVO — FASE 3",
                 ha="center", va="center", fontsize=13,
                 color="#aed6f1", fontweight="bold")
    ax_main.text(0.5, 0.70,
                 "Grupo de Control GC1\nSARIMA y Prophet",
                 ha="center", va="center", fontsize=22,
                 color="white", fontweight="bold", linespacing=1.4)
    ax_main.text(0.5, 0.48,
                 "Predicción de Demanda Agroindustrial de Limón Peruano\n"
                 "Sistema Multimodal: Agro + Clima + NLP",
                 ha="center", va="center", fontsize=11, color="#aed6f1",
                 linespacing=1.5)
    ax_main.axhline(0.38, xmin=0.15, xmax=0.85, color="#aed6f1", linewidth=0.6)
    ax_main.text(0.5, 0.22,
                 f"Fecha: {FECHA_HOY}     ·     Autor: Fabrizio Sánchez S.\n"
                 f"Asesor: —     ·     Repositorio: Machine-Learning-Multimodal--Agro-NLP-Clima-",
                 ha="center", va="center", fontsize=8.5, color="#7fb3d3",
                 linespacing=1.6)

    ax_foot = fig.add_axes([0.0, 0.0, 1.0, 0.10])
    ax_foot.set_facecolor("#154360"); ax_foot.axis("off")
    ax_foot.text(0.5, 0.5,
                 "Entorno: Python 3.11 · pmdarima 2.1.1 · prophet 1.3.0 · scikit-learn 1.8.0 · TensorFlow 2.21.0",
                 ha="center", fontsize=8, color="#7fb3d3")

    pdf.savefig(fig, dpi=150); plt.close(fig)

    # ====================================================================
    # PÁGINA 2 — Datos y metodología
    # ====================================================================
    fig = plt.figure(figsize=(11, 8.5))
    gs  = gridspec.GridSpec(3, 2, figure=fig,
                            top=0.90, bottom=0.06, left=0.06, right=0.96,
                            hspace=0.55, wspace=0.35)

    ax_title = fig.add_axes([0.0, 0.90, 1.0, 0.10])
    header_strip(ax_title, "1. Datos y Metodología de Validación")

    # Dataset info
    ax_ds = fig.add_subplot(gs[0, :])
    tabla_metricas(ax_ds, [
        ["Archivo fuente",    "master_dataset_fase2_multivariado.csv"],
        ["Dimensiones",       "5,880 filas × 24 columnas"],
        ["Cobertura temporal","2021-01-01 → 2025-08-01  (56 meses)"],
        ["Provincias",        "105  ·  Departamentos: 23"],
        ["Variable objetivo", "produccion_t  (z-score, escala Fase 2)"],
        ["Agregación",        "Media provincial mensual (.mean()) — evita inflación de MAPE"],
    ], "Dataset utilizado")

    # Split info
    ax_split = fig.add_subplot(gs[1, 0])
    tabla_metricas(ax_split, [
        ["Tipo de split",    "Cronológico estricto — sin aleatorización"],
        ["Entrenamiento 80%",f"44 meses  ({TRAIN_RANGE})"],
        ["Prueba 20%",       f"12 meses  ({TEST_RANGE})"],
        ["Corte",            SPLIT_DATE],
        ["Data leakage",     "Ninguno — auto_arima y Prophet ajustan solo sobre train"],
    ], "Protocolo de Split")

    # Decisión de agregación
    ax_nota = fig.add_subplot(gs[1, 1])
    nota(ax_nota,
         "Corrección aplicada en este estudio:\n"
         "Se reemplazó .sum() por .mean() en la\n"
         "agregación nacional.\n\n"
         "Motivo: sumar 105 z-scores produce\n"
         "denominadores ≈ 0 que inflan el MAPE\n"
         "artificialmente (de 9.84 → 0.10 en MAE).",
         color=ROJO)

    # Gráfico del split
    ax_img = fig.add_subplot(gs[2, :])
    imagen(ax_img, GC1_DIR / "gc1_split_cronologico.png",
           "Split cronológico 80/20 — SARIMA (idéntico para Prophet)")

    pdf.savefig(fig, dpi=150); plt.close(fig)

    # ====================================================================
    # PÁGINA 3 — Serie temporal y exploración
    # ====================================================================
    fig = plt.figure(figsize=(11, 8.5))
    gs  = gridspec.GridSpec(2, 1, figure=fig,
                            top=0.90, bottom=0.06, left=0.06, right=0.96,
                            hspace=0.4)

    ax_title = fig.add_axes([0.0, 0.90, 1.0, 0.10])
    header_strip(ax_title, "2. Serie Temporal — Producción Nacional de Limón")

    ax_img = fig.add_subplot(gs[0])
    imagen(ax_img, GC1_DIR / "gc1_serie_temporal.png",
           "Producción mensual nacional (media provincial, z-score) — tendencia y variabilidad")

    ax_desc = fig.add_subplot(gs[1])
    tabla_metricas(ax_desc, [
        ["Período",            "Enero 2021 — Agosto 2025 (56 meses)"],
        ["Media de la serie",  "≈ 0.0  (centrada en z-score)"],
        ["Desviación típica",  "≈ 0.07 (por provincia promedio)"],
        ["Estacionalidad",     "Anual visible (pico productivo: Q1, mínimo: Q3)"],
        ["Tendencia",          "Leve descenso en 2024-2025 (posible ciclo decreciente)"],
        ["Observaciones clave","Caída pronunciada a partir de Sep 2024 (período de prueba)"],
    ], "Características de la serie")

    pdf.savefig(fig, dpi=150); plt.close(fig)

    # ====================================================================
    # PÁGINA 4 — SARIMA: resultados
    # ====================================================================
    fig = plt.figure(figsize=(11, 8.5))
    gs  = gridspec.GridSpec(2, 2, figure=fig,
                            top=0.90, bottom=0.06, left=0.06, right=0.96,
                            hspace=0.5, wspace=0.35)

    ax_title = fig.add_axes([0.0, 0.90, 1.0, 0.10])
    header_strip(ax_title,
                 "3. Modelo SARIMA — GC1 Baseline",
                 "SARIMA(1,0,0)(2,1,0)[12]  ·  Búsqueda automática con auto_arima (pmdarima 2.1.1)")

    # Parámetros
    ax_param = fig.add_subplot(gs[0, 0])
    tabla_metricas(ax_param, [
        ["Algoritmo",          "auto_arima (stepwise, AIC)"],
        ["Orden seleccionado", f"SARIMA{sarima['orden']}"],
        ["Orden estacional",   f"×{sarima['orden_estacional']}"],
        ["AIC (train)",        f"{sarima['aic']:.4f}"],
        ["Criterio búsqueda",  "AIC mínimo"],
        ["Espacio búsqueda",   "p,q ∈ [0,3]  P,Q ∈ [0,2]"],
        ["d, D",               "Detección automática (test KPSS)"],
        ["Estacionalidad",     "m = 12 (mensual)"],
    ], "Configuración del modelo")

    # Métricas
    ax_met = fig.add_subplot(gs[0, 1])
    tabla_metricas(ax_met, [
        ["MAE",  f"{sarima['MAE']:.6f}"],
        ["RMSE", f"{sarima['RMSE']:.6f}"],
        ["MAPE", f"{sarima['MAPE_pct']:.2f} % ⚠"],
        ["R²",   f"{sarima['R2']:.4f}"],
        ["n entrenamiento", f"{sarima['n_train']} meses"],
        ["n prueba",        f"{sarima['n_test']} meses"],
        ["Corte",           sarima['split_date']],
    ], "Métricas — conjunto de prueba")

    # Gráfico
    ax_img = fig.add_subplot(gs[1, :])
    imagen(ax_img, GC1_DIR / "gc1_sarima_prediccion_vs_real.png",
           "SARIMA — predicción vs real (serie completa + zoom período de prueba)")

    pdf.savefig(fig, dpi=150); plt.close(fig)

    # ====================================================================
    # PÁGINA 5 — Prophet: resultados
    # ====================================================================
    fig = plt.figure(figsize=(11, 8.5))
    gs  = gridspec.GridSpec(2, 2, figure=fig,
                            top=0.90, bottom=0.06, left=0.06, right=0.96,
                            hspace=0.5, wspace=0.35)

    ax_title = fig.add_axes([0.0, 0.90, 1.0, 0.10])
    header_strip(ax_title,
                 "4. Modelo Prophet — GC1 Baseline",
                 "Meta Prophet 1.3.0  ·  Estacionalidad aditiva anual")

    ax_param = fig.add_subplot(gs[0, 0])
    tabla_metricas(ax_param, [
        ["Librería",             "prophet 1.3.0 (Meta)"],
        ["Modo estacionalidad",  "Aditivo"],
        ["Estacionalidad anual", "Activa"],
        ["Estacionalidad semanal/diaria", "Desactivada (serie mensual)"],
        ["changepoint_prior_scale", "0.05 (regularización)"],
        ["seasonality_prior_scale", "10.0"],
        ["uncertainty_samples",     "500"],
        ["Changepoints detectados", "Auto (algoritmo interno)"],
    ], "Configuración del modelo")

    ax_met = fig.add_subplot(gs[0, 1])
    tabla_metricas(ax_met, [
        ["MAE",  f"{prophet['MAE']:.6f}"],
        ["RMSE", f"{prophet['RMSE']:.6f}"],
        ["MAPE", f"{prophet['MAPE_pct']:.2f} % ⚠"],
        ["R²",   f"{prophet['R2']:.4f}"],
        ["n entrenamiento", f"{prophet['n_train']} meses"],
        ["n prueba",        f"{prophet['n_test']} meses"],
        ["Corte",           prophet['split_date']],
    ], "Métricas — conjunto de prueba")

    ax_img = fig.add_subplot(gs[1, :])
    imagen(ax_img, GC1_DIR / "gc1_prophet_prediccion_vs_real.png",
           "Prophet — predicción vs real (serie completa + zoom período de prueba)")

    pdf.savefig(fig, dpi=150); plt.close(fig)

    # ====================================================================
    # PÁGINA 6 — Componentes Prophet
    # ====================================================================
    fig = plt.figure(figsize=(11, 8.5))
    gs  = gridspec.GridSpec(1, 1, figure=fig,
                            top=0.90, bottom=0.06, left=0.06, right=0.96)

    ax_title = fig.add_axes([0.0, 0.90, 1.0, 0.10])
    header_strip(ax_title, "5. Descomposición Prophet — Tendencia y Estacionalidad")

    ax_img = fig.add_subplot(gs[0])
    imagen(ax_img, GC1_DIR / "gc1_prophet_componentes.png",
           "Componentes del modelo Prophet: tendencia de largo plazo + patrón estacional anual")

    pdf.savefig(fig, dpi=150); plt.close(fig)

    # ====================================================================
    # PÁGINA 7 — Comparativa GC1 y conclusiones
    # ====================================================================
    fig = plt.figure(figsize=(11, 8.5))
    gs  = gridspec.GridSpec(3, 2, figure=fig,
                            top=0.90, bottom=0.06, left=0.06, right=0.96,
                            hspace=0.65, wspace=0.35)

    ax_title = fig.add_axes([0.0, 0.90, 1.0, 0.10])
    header_strip(ax_title, "6. Comparativa GC1 y Conclusiones")

    # Tabla comparativa
    ax_comp = fig.add_subplot(gs[0, :])

    def fmt(v):
        return f"{v:.4f}" if v is not None else "N/A"

    def mejor(k):
        sv, pv = sarima.get(k), prophet.get(k)
        if sv is None or pv is None: return "", ""
        if k == "R2":
            return ("◀ mejor", "") if sv > pv else ("", "◀ mejor")
        return ("◀ mejor", "") if sv < pv else ("", "◀ mejor")

    filas_comp = []
    for m, label in [("MAE","MAE"), ("RMSE","RMSE"), ("MAPE_pct","MAPE (%)"), ("R2","R²")]:
        ms, mp = mejor(m)
        filas_comp.append([label,
                           f"{fmt(sarima.get(m))} {ms}",
                           f"{fmt(prophet.get(m))} {mp}"])

    ax_comp.axis("off")
    ax_comp.set_facecolor(FONDO)
    ax_comp.text(0.5, 0.97, "Comparativa de métricas GC1: SARIMA vs Prophet (conjunto de prueba, 12 meses)",
                 ha="center", va="top", fontsize=11, fontweight="bold", color=AZUL,
                 transform=ax_comp.transAxes)
    col_labels = ["Métrica", "SARIMA", "Prophet"]
    t = ax_comp.table(cellText=filas_comp, colLabels=col_labels,
                      loc="center", cellLoc="center",
                      bbox=[0.05, 0.08, 0.90, 0.80])
    t.auto_set_font_size(False); t.set_fontsize(10)
    for (r, c), cell in t.get_celld().items():
        cell.set_edgecolor("#bdc3c7")
        if r == 0:
            cell.set_facecolor(AZUL)
            cell.set_text_props(color="white", fontweight="bold")
        elif "◀" in str(cell.get_text().get_text()):
            cell.set_facecolor("#d5f5e3")
        elif r % 2 == 0:
            cell.set_facecolor("#eaf0fb")
        else:
            cell.set_facecolor("white")

    # Gráfico de barras comparativo
    ax_bar = fig.add_subplot(gs[1, :])
    comparativa_bar(ax_bar,
                    [sarima["MAE"], sarima["RMSE"]],
                    [prophet["MAE"], prophet["RMSE"]],
                    ["MAE", "RMSE"])

    # Conclusiones
    ax_conc = fig.add_subplot(gs[2, 0])
    ax_conc.axis("off")
    ax_conc.set_facecolor(FONDO)
    conclusiones = [
        "1. Prophet supera a SARIMA en MAE, RMSE y R².",
        "2. Ambos modelos tienen R² negativo: el baseline",
        "   univariado no captura la caída de 2024-2025.",
        "3. El MAPE (≈240-270%) es no confiable con",
        "   z-scores — usar MAE/RMSE como referencia.",
        "4. GC1 establece el piso de comparación para",
        "   modelos multivariados y deep learning.",
    ]
    ax_conc.text(0.5, 0.97, "Conclusiones", ha="center", va="top",
                 fontsize=11, fontweight="bold", color=AZUL,
                 transform=ax_conc.transAxes)
    for i, linea in enumerate(conclusiones):
        ax_conc.text(0.05, 0.83 - i*0.12, linea, va="top",
                     fontsize=9, color=GRIS, transform=ax_conc.transAxes)

    # Próximos pasos
    ax_next = fig.add_subplot(gs[2, 1])
    ax_next.axis("off")
    ax_next.set_facecolor("#eaf0fb")
    pasos = [
        "GC2 — LSTM-Attention univariado",
        "GC3 — LSTM-Attention multivariado",
        "GM1 — Modelo multimodal completo",
        "      (Agro + Clima + NLP/BETO)",
        "Análisis SHAP de importancia",
        "Dashboard Streamlit (Fase 4)",
    ]
    ax_next.text(0.5, 0.97, "Próximos pasos", ha="center", va="top",
                 fontsize=11, fontweight="bold", color=AZUL,
                 transform=ax_next.transAxes)
    for i, paso in enumerate(pasos):
        marker = "▸ " if not paso.startswith("  ") else "   "
        ax_next.text(0.05, 0.83 - i*0.13, marker + paso, va="top",
                     fontsize=9, color=GRIS, transform=ax_next.transAxes)

    pdf.savefig(fig, dpi=150); plt.close(fig)

    # Metadata del PDF
    d = pdf.infodict()
    d['Title']   = 'Reporte Ejecutivo GC1 — SARIMA y Prophet'
    d['Author']  = 'Fabrizio Sánchez S.'
    d['Subject'] = 'Fase 3 Predicción de Demanda Agroindustrial — Limón Peruano'
    d['Keywords']= 'SARIMA, Prophet, Series Temporales, Agro, NLP, LSTM, UPEU'

print(f"\nReporte generado exitosamente:")
print(f"  → {OUT_PDF}")
print(f"  Páginas: 7")
