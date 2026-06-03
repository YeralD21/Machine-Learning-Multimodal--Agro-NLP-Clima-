"""
dashboard/app_dash.py — AgroNLP-Clima Dashboard (Dash + Plotly)
Ejecutar: python dashboard/app_dash.py
Instalar:  pip install dash dash-bootstrap-components
"""

import base64
import json
import pathlib

import dash
import dash_bootstrap_components as dbc
import numpy as np
import pandas as pd
import plotly.graph_objects as go
from dash import Input, Output, callback, dash_table, dcc, html

# ──────────────────────────────────────────────────────────────────────────────
# PATHS
# ──────────────────────────────────────────────────────────────────────────────

BASE_DIR = pathlib.Path(__file__).resolve().parent.parent

P_GE_PREDS     = BASE_DIR / "resultados/ge/ge_predicciones.csv"
P_GE_METRICS   = BASE_DIR / "resultados/ge/ge_metricas.json"
P_GC1_SARIMA   = BASE_DIR / "resultados/gc1/gc1_sarima_metricas.json"
P_GC1_PROPHET  = BASE_DIR / "resultados/gc1/gc1_prophet_metricas.json"
P_GC2          = BASE_DIR / "resultados/gc2/gc2_metricas.json"
P_XGB_METRICS  = BASE_DIR / "resultados/xgboost/xgb_metricas.json"
P_TCN_METRICS  = BASE_DIR / "resultados/tcn/tcn_metricas.json"
P_GMV2_METRICS = BASE_DIR / "resultados/gm_v2/gm_v2_metricas.json"
P_SHAP         = BASE_DIR / "resultados/shap/shap_resultados.json"
P_VALIDACION   = BASE_DIR / "resultados/validacion/validacion_estadistica.json"

P_XGB_PREDS_CSV  = BASE_DIR / "resultados/xgboost/xgb_predicciones.csv"
P_GMV2_PREDS_CSV = BASE_DIR / "resultados/gm_v2/gm_v2_predicciones.csv"
P_TCN_PREDS_CSV  = BASE_DIR / "resultados/tcn/tcn_predicciones.csv"
P_SARIMA_CSV     = BASE_DIR / "resultados/gc1/gc1_sarima_predicciones.csv"
P_PROPHET_CSV    = BASE_DIR / "resultados/gc1/gc1_prophet_predicciones.csv"
P_GC2_CSV        = BASE_DIR / "resultados/gc2/gc2_predicciones.csv"

P_ATT_HEATMAP   = BASE_DIR / "resultados/ge/ge_attention_heatmap_test.png"
P_NLP_FEAT_IMG  = BASE_DIR / "resultados/gm_v2/nlp_features_engineering.png"
P_PCA_IMG       = BASE_DIR / "resultados/gm_v2/pca_varianza_explicada.png"
P_XGB_FEAT_IMG  = BASE_DIR / "resultados/xgboost/xgb_feature_importance.png"
P_SHAP_BAR_IMG  = BASE_DIR / "resultados/shap/shap_summary_bar.png"
P_SHAP_HEAT_IMG = BASE_DIR / "resultados/shap/shap_heatmap_temporal.png"

# ──────────────────────────────────────────────────────────────────────────────
# PALETTE
# ──────────────────────────────────────────────────────────────────────────────

C_GE      = "#38BDF8"   # azul
C_XGB     = "#F97316"   # naranja
C_GMV2    = "#8B5CF6"   # violeta
C_STAT    = "#6B7280"   # gris
C_ALERT   = "#EF4444"   # rojo
C_SUCCESS = "#22C55E"   # verde
C_NAIVE   = "#94A3B8"
C_TCN     = "#F59E0B"
C_GM_ORIG = "#64748B"
C_PROPHET = "#A78BFA"
C_SARIMA  = "#60A5FA"
C_GC2     = "#FB923C"

DARK_BG   = "#1E293B"
MAIN_BG   = "#F8FAFC"
CARD_BG   = "#0F172A"

SHOCK_DATES = pd.to_datetime([
    "2024-09-01", "2024-10-01", "2024-12-01",
    "2025-01-01", "2025-02-01", "2025-04-01",
    "2025-05-01", "2025-06-01",
])

MODEL_COLORS = {
    "GE (principal)":      C_GE,
    "XGBoost":             C_XGB,
    "GM v2 (NLP mejorado)": C_GMV2,
    "Naive (baseline)":    C_NAIVE,
    "GC1-Prophet":         C_PROPHET,
    "GM original (NLP)":   C_GM_ORIG,
    "GC1-SARIMA":          C_SARIMA,
    "TCN":                 C_TCN,
    "GC2-SARIMAX+LSTM":    C_GC2,
}

ALL_MODELS_TABLE = [
    {"Modelo": "Naive (baseline)",    "MAE": 0.0161, "RMSE": None,   "R²": None,    "MASE": 1.00,  "Deterioro shock": "+28.5%", "Tipo": "Baseline"},
    {"Modelo": "XGBoost",             "MAE": 0.0471, "RMSE": 0.0542, "R²": -1.017,  "MASE": 2.60,  "Deterioro shock": "+16.5%", "Tipo": "ML tabular"},
    {"Modelo": "GM v2 (NLP mejorado)","MAE": 0.0646, "RMSE": 0.0771, "R²": -9.859,  "MASE": 4.02,  "Deterioro shock": "+29.2%", "Tipo": "DL + NLP"},
    {"Modelo": "GE (principal)",      "MAE": 0.0673, "RMSE": 0.0698, "R²": -2.345,  "MASE": 4.19,  "Deterioro shock": "+2.3%",  "Tipo": "DL principal"},
    {"Modelo": "GC1-Prophet",         "MAE": 0.0919, "RMSE": 0.1051, "R²": -6.045,  "MASE": 5.72,  "Deterioro shock": "N/D",    "Tipo": "Estadístico"},
    {"Modelo": "GM original (NLP)",   "MAE": 0.0981, "RMSE": 0.1007, "R²": -5.958,  "MASE": 6.11,  "Deterioro shock": "N/D",    "Tipo": "DL + NLP"},
    {"Modelo": "GC1-SARIMA",          "MAE": 0.1006, "RMSE": 0.1179, "R²": -7.864,  "MASE": 6.26,  "Deterioro shock": "N/D",    "Tipo": "Estadístico"},
    {"Modelo": "TCN",                 "MAE": 0.1860, "RMSE": 0.1987, "R²": -71.083, "MASE": 11.58, "Deterioro shock": "-9.1%*", "Tipo": "DL convolucional"},
    {"Modelo": "GC2-SARIMAX+LSTM",    "MAE": 0.1969, "RMSE": 0.2926, "R²": -53.634, "MASE": 12.26, "Deterioro shock": "N/D",    "Tipo": "Híbrido"},
]

# ──────────────────────────────────────────────────────────────────────────────
# DATA LOADERS
# ──────────────────────────────────────────────────────────────────────────────

def _read_json(path):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _load_csv(path, date_col="fecha"):
    try:
        return pd.read_csv(path, parse_dates=[date_col]).sort_values(date_col).reset_index(drop=True)
    except Exception:
        return pd.DataFrame()


def _img_b64(path: pathlib.Path) -> str | None:
    if not path.exists():
        return None
    with open(path, "rb") as f:
        return "data:image/png;base64," + base64.b64encode(f.read()).decode()


def load_all():
    ge_preds = _load_csv(P_GE_PREDS)
    xgb_preds = _load_csv(P_XGB_PREDS_CSV)
    gmv2_preds = _load_csv(P_GMV2_PREDS_CSV)
    tcn_preds = _load_csv(P_TCN_PREDS_CSV)
    sarima_preds = _load_csv(P_SARIMA_CSV, date_col="fecha_evento")
    if not sarima_preds.empty and "fecha_evento" in sarima_preds.columns:
        sarima_preds = sarima_preds.rename(columns={"fecha_evento": "fecha"})
    prophet_preds = _load_csv(P_PROPHET_CSV)
    gc2_preds = _load_csv(P_GC2_CSV)

    shap_data = _read_json(P_SHAP)
    val_data = _read_json(P_VALIDACION)

    return {
        "ge": ge_preds,
        "xgb": xgb_preds,
        "gmv2": gmv2_preds,
        "tcn": tcn_preds,
        "sarima": sarima_preds,
        "prophet": prophet_preds,
        "gc2": gc2_preds,
        "shap": shap_data,
        "validacion": val_data,
    }


DATA = load_all()

# ──────────────────────────────────────────────────────────────────────────────
# HELPERS
# ──────────────────────────────────────────────────────────────────────────────

def dark_fig(height=400, margin=None):
    m = margin or dict(t=55, b=40, l=55, r=20)
    return go.Figure().update_layout(
        template="plotly_dark",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(15,23,42,0.6)",
        height=height,
        margin=m,
        font=dict(color="#CBD5E1", size=12),
    )


def shock_vrects(fig):
    for sd in SHOCK_DATES:
        fig.add_vrect(
            x0=sd, x1=sd + pd.DateOffset(months=1),
            fillcolor="rgba(239,68,68,0.12)",
            line_width=0,
        )
    return fig


def kpi_card(title, value, subtitle, color, icon):
    return dbc.Card(
        dbc.CardBody([
            html.Div(icon, className="fs-3 mb-1"),
            html.Div(value, style={"fontSize": "1.7rem", "fontWeight": "800", "color": color}),
            html.Div(title, style={"fontSize": "0.75rem", "color": "#94A3B8", "fontWeight": "600",
                                   "textTransform": "uppercase", "letterSpacing": "0.05em"}),
            html.Div(subtitle, style={"fontSize": "0.72rem", "color": "#64748B", "marginTop": "4px"}),
        ]),
        style={"background": "#0F172A", "border": f"1px solid {color}33",
               "borderTop": f"3px solid {color}", "borderRadius": "10px"},
        className="h-100",
    )


def img_or_placeholder(path, alt="Imagen pendiente"):
    src = _img_b64(path)
    if src:
        return html.Img(src=src, style={"width": "100%", "borderRadius": "8px"})
    return dbc.Alert(f"⚠ {alt}", color="warning", className="mt-2")


# ──────────────────────────────────────────────────────────────────────────────
# FIGURES (pre-computed)
# ──────────────────────────────────────────────────────────────────────────────

def fig_ranking_mae():
    df = pd.DataFrame(ALL_MODELS_TABLE)
    colors = [MODEL_COLORS.get(m, "#888") for m in df["Modelo"]]
    fig = dark_fig(height=390, margin=dict(t=55, b=60, l=55, r=20))
    fig.add_trace(go.Bar(
        x=df["Modelo"], y=df["MAE"],
        marker_color=colors, marker_line_width=0,
        text=[f"{v:.4f}" for v in df["MAE"]],
        textposition="outside", textfont=dict(color="#CBD5E1", size=11),
    ))
    fig.add_hline(y=0.0673, line_dash="dash", line_color=C_GE,
                  annotation_text="GE principal", annotation_font_color=C_GE)
    fig.add_hline(y=0.0161, line_dash="dot", line_color=C_NAIVE,
                  annotation_text="Naive baseline", annotation_font_color=C_NAIVE,
                  annotation_position="bottom right")
    fig.update_layout(title="Ranking MAE — 9 modelos (menor es mejor)",
                      yaxis_title="MAE", xaxis_tickangle=-22, showlegend=False)
    fig.update_yaxes(gridcolor="#1E293B")
    return fig


def fig_metrics_bar():
    labels = ["GC1-SARIMA", "GC1-Prophet", "GC2-SARIMAX+LSTM", "GE-DualLSTM-Attn"]
    mae_vals  = [0.1006, 0.0919, 0.1969, 0.0673]
    rmse_vals = [0.1179, 0.1051, 0.2926, 0.0698]
    bar_colors = [C_SARIMA, C_PROPHET, C_GC2, C_GE]

    fig = dark_fig(height=360, margin=dict(t=55, b=50, l=55, r=20))
    fig.add_trace(go.Bar(name="MAE", x=labels, y=mae_vals,
                         marker_color=bar_colors, opacity=0.85,
                         text=[f"{v:.4f}" for v in mae_vals], textposition="outside"))
    fig.add_trace(go.Bar(name="RMSE", x=labels, y=rmse_vals,
                         marker_color=bar_colors, opacity=0.45,
                         marker_line_color="white", marker_line_width=1,
                         text=[f"{v:.4f}" for v in rmse_vals], textposition="outside"))
    fig.update_layout(barmode="group", title="MAE y RMSE — modelos z-score (Fase 3)",
                      yaxis_title="Valor", xaxis_tickangle=-15)
    fig.update_yaxes(gridcolor="#1E293B")
    return fig


def fig_ge_prediction(shock_threshold=0.08):
    ge = DATA["ge"]
    if ge.empty:
        return dark_fig()
    fig = dark_fig(height=440, margin=dict(t=60, b=50, l=60, r=20))

    ge_mae = ge["error_abs"].mean() if "error_abs" in ge.columns else 0.0673

    fig = shock_vrects(fig)

    fig.add_trace(go.Scatter(
        x=ge["fecha"], y=ge["real"],
        name="Real", mode="lines+markers",
        line=dict(color="white", width=2.5),
        marker=dict(size=7, symbol="circle"),
    ))
    fig.add_trace(go.Scatter(
        x=ge["fecha"], y=ge["prediccion"],
        name="Predicción GE", mode="lines+markers",
        line=dict(color=C_GE, width=2.5),
        marker=dict(size=7, symbol="diamond"),
    ))
    # Banda ±MAE
    fig.add_trace(go.Scatter(
        x=pd.concat([ge["fecha"], ge["fecha"][::-1]]),
        y=pd.concat([ge["prediccion"] + ge_mae, (ge["prediccion"] - ge_mae)[::-1]]),
        fill="toself", fillcolor=f"rgba(56,189,248,0.10)",
        line=dict(color="rgba(0,0,0,0)"),
        name=f"Banda ±MAE={ge_mae:.4f}", showlegend=True,
    ))

    fig.add_annotation(
        x=pd.Timestamp("2025-01-01"),
        y=float(ge["real"].max()) * 1.1,
        text="Shock 1021%<br>Ene 2025",
        showarrow=True, arrowhead=2, arrowcolor=C_ALERT,
        font=dict(color=C_ALERT, size=10),
    )
    fig.update_layout(
        title="GE — Predicción vs Real · Período de test (nov 2024 – ago 2025)",
        xaxis_title="Fecha", yaxis_title="Producción (z-score)",
        legend=dict(orientation="h", yanchor="bottom", y=-0.35),
        hovermode="x unified",
    )
    fig.update_xaxes(gridcolor="#1E293B")
    fig.update_yaxes(gridcolor="#1E293B")
    return fig


def fig_ge_error_bars():
    ge = DATA["ge"]
    if ge.empty or "error_abs" not in ge.columns:
        return dark_fig()
    ge = ge.copy()
    ge["mes_label"] = ge["fecha"].dt.strftime("%b %Y")
    ge["bar_color"] = ge["error_abs"].apply(lambda v: C_ALERT if v > 0.08 else C_SUCCESS)
    fig = dark_fig(height=440, margin=dict(t=55, b=40, l=100, r=70))
    fig.add_trace(go.Bar(
        x=ge["error_abs"], y=ge["mes_label"],
        orientation="h",
        marker_color=ge["bar_color"].tolist(),
        text=[f"{v:.4f}" for v in ge["error_abs"]],
        textposition="outside",
    ))
    mae_val = ge["error_abs"].mean()
    fig.add_vline(x=mae_val, line_dash="dash", line_color="#94A3B8",
                  annotation_text=f"MAE={mae_val:.4f}",
                  annotation_font_color="#94A3B8",
                  annotation_position="top right")
    fig.update_layout(title="Error absoluto mensual (GE)",
                      xaxis_title="|real − pred|", showlegend=False)
    fig.update_xaxes(gridcolor="#1E293B")
    return fig


def fig_shap_bars():
    shap_data = DATA["shap"]
    ranking = shap_data.get("ranking_global", [])
    if not ranking:
        return dark_fig()
    top_n = 15
    feats = [r["label"] for r in ranking[:top_n]][::-1]
    imps  = [r["importance"] for r in ranking[:top_n]][::-1]
    bar_colors = [
        C_GE if i >= top_n - 3 else
        "#7DD3FC" if i >= top_n - 6 else
        "#BAE6FD"
        for i in range(top_n)
    ]
    fig = dark_fig(height=520, margin=dict(t=55, b=40, l=160, r=90))
    fig.add_trace(go.Bar(
        x=imps, y=feats, orientation="h",
        marker_color=bar_colors,
        text=[f"{v:.4f}" for v in imps], textposition="outside",
    ))
    fig.update_layout(
        title=f"Top {top_n} variables — Importancia SHAP global media |φ|",
        xaxis_title="Importancia SHAP", showlegend=False,
    )
    fig.update_xaxes(gridcolor="#1E293B")
    return fig


def fig_shap_heatmap():
    shap_data = DATA["shap"]
    ranking = shap_data.get("ranking_global", [])
    meses_shap = shap_data.get("shap_por_mes", [])
    if not ranking or not meses_shap:
        return dark_fig()
    top5_keys = [r["feature"] for r in ranking[:5]]
    top5_labs = [r["label"]   for r in ranking[:5]]
    mes_labels = [m["mes"] for m in meses_shap]
    matrix = np.array([
        [m["shap_top5"].get(k, 0.0) for k in top5_keys]
        for m in meses_shap
    ])
    fig = dark_fig(height=320, margin=dict(t=55, b=55, l=155, r=20))
    fig.add_trace(go.Heatmap(
        z=matrix.T, x=mes_labels, y=top5_labs,
        colorscale="RdBu", zmid=0,
        text=np.round(matrix.T, 3),
        texttemplate="%{text}",
        colorbar=dict(title="φ SHAP", tickfont=dict(color="#CBD5E1")),
    ))
    fig.update_layout(
        title="Contribución SHAP mensual — top 5 variables (rojo=positivo, azul=negativo)",
        xaxis_title="Mes",
    )
    return fig


def fig_competitors_lines(active_models: list[str]):
    catalog = _build_catalog()
    all_reals = []
    for nom, (df, *_) in catalog.items():
        if df is not None and not df.empty and "real" in df.columns:
            all_reals.append(df[["fecha", "real"]])
    if not all_reals:
        return dark_fig()
    real_unified = (
        pd.concat(all_reals).drop_duplicates("fecha")
        .sort_values("fecha").reset_index(drop=True)
    )

    fig = dark_fig(height=500, margin=dict(t=60, b=120, l=60, r=20))
    fig = shock_vrects(fig)

    fig.add_trace(go.Scatter(
        x=real_unified["fecha"], y=real_unified["real"],
        name="Real", mode="lines+markers",
        line=dict(color="white", width=3),
        marker=dict(size=8),
    ))

    for nom, (df, color, dash, sym) in catalog.items():
        if nom not in active_models:
            continue
        if df is None or df.empty:
            continue
        pred_col = "pred"
        if pred_col not in df.columns:
            continue
        df2 = df.dropna(subset=["pred"])
        if df2.empty:
            continue
        fig.add_trace(go.Scatter(
            x=df2["fecha"], y=df2["pred"],
            name=nom, mode="lines+markers",
            line=dict(color=color, width=2, dash=dash),
            marker=dict(size=6, symbol=sym),
        ))

    fig.add_annotation(
        x=pd.Timestamp("2025-01-01"),
        y=float(real_unified["real"].max()) * 1.15 if not real_unified.empty else 1,
        text="Shock 1021%", showarrow=True,
        arrowhead=2, arrowcolor=C_ALERT,
        font=dict(color=C_ALERT, size=10),
    )
    fig.update_layout(
        title="Predicciones vs Real — todos los modelos activos (áreas rojas = shocks)",
        xaxis_title="Fecha", yaxis_title="Producción (z-score)",
        legend=dict(orientation="h", yanchor="bottom", y=-0.55),
        hovermode="x unified",
    )
    fig.update_xaxes(gridcolor="#1E293B")
    fig.update_yaxes(gridcolor="#1E293B")
    return fig


def fig_competitors_bars_global_shock(active_models: list[str]):
    catalog = _build_catalog()
    rows = []
    for nom, (df, color, *_) in catalog.items():
        if nom not in active_models:
            continue
        if df is None or df.empty or "pred" not in df.columns:
            continue
        df2 = df.dropna(subset=["real", "pred"])
        if len(df2) < 2:
            continue
        from sklearn.metrics import mean_absolute_error as mae_fn
        mae_g = mae_fn(df2["real"], df2["pred"])
        mask = df2["fecha"].isin(SHOCK_DATES)
        df_sh = df2[mask]
        mae_s = mae_fn(df_sh["real"], df_sh["pred"]) if len(df_sh) >= 2 else float("nan")
        rows.append({"Modelo": nom, "MAE global": mae_g, "MAE shocks": mae_s, "color": color})

    if not rows:
        return dark_fig(), dark_fig()

    dyn = pd.DataFrame(rows)
    valid = dyn[dyn["MAE shocks"].notna()]

    fig1 = dark_fig(height=420, margin=dict(t=55, b=70, l=55, r=20))
    fig1.add_trace(go.Bar(
        name="MAE global", x=valid["Modelo"], y=valid["MAE global"],
        marker_color=valid["color"].tolist(), opacity=0.45,
        text=[f"{v:.4f}" for v in valid["MAE global"]], textposition="outside",
    ))
    fig1.add_trace(go.Bar(
        name="MAE en shocks", x=valid["Modelo"], y=valid["MAE shocks"],
        marker_color=valid["color"].tolist(), opacity=1.0,
        marker_line_color="white", marker_line_width=1,
        text=[f"{v:.4f}" for v in valid["MAE shocks"]], textposition="outside",
    ))
    fig1.update_layout(barmode="group", title="MAE Global vs MAE en meses de shock",
                       yaxis_title="MAE", xaxis_tickangle=-22)
    fig1.update_yaxes(gridcolor="#1E293B")

    # Deterioro
    dyn2 = dyn.copy()
    dyn2 = dyn2[dyn2["MAE shocks"].notna()].copy()
    dyn2["det"] = (dyn2["MAE shocks"] - dyn2["MAE global"]) / dyn2["MAE global"] * 100
    dyn2 = dyn2.sort_values("det")
    det_colors = [
        C_SUCCESS if v <= 5 else C_XGB if v <= 20 else C_ALERT if v > 0 else C_GMV2
        for v in dyn2["det"]
    ]
    fig2 = dark_fig(height=420, margin=dict(t=60, b=40, l=160, r=110))
    fig2.add_trace(go.Bar(
        x=dyn2["det"], y=dyn2["Modelo"],
        orientation="h",
        marker_color=det_colors,
        text=[f"{v:+.1f}%" for v in dyn2["det"]], textposition="outside",
    ))
    fig2.add_vline(x=0,  line_color="white",  line_width=1.5)
    fig2.add_vline(x=5,  line_dash="dash", line_color=C_SUCCESS,
                   annotation_text="Verde ≤5%", annotation_font_color=C_SUCCESS,
                   annotation_position="top right")
    fig2.add_vline(x=20, line_dash="dash", line_color=C_ALERT,
                   annotation_text="Rojo >20%", annotation_font_color=C_ALERT,
                   annotation_position="top right")
    fig2.update_layout(title="Deterioro MAE en shocks<br>(verde ≤5% / naranja ≤20% / rojo >20%)",
                       xaxis_title="Δ MAE (%)", showlegend=False)
    fig2.update_xaxes(gridcolor="#1E293B")
    return fig1, fig2


def _build_catalog():
    ge = DATA["ge"].copy()
    if not ge.empty and "prediccion" in ge.columns:
        ge = ge.rename(columns={"prediccion": "pred"})

    xgb = DATA["xgb"].copy()
    if not xgb.empty and "pred_xgb" in xgb.columns:
        xgb = xgb.rename(columns={"pred_xgb": "pred"})

    gmv2 = DATA["gmv2"].copy()
    if not gmv2.empty and "pred_gm_v2" in gmv2.columns:
        gmv2 = gmv2.rename(columns={"pred_gm_v2": "pred"})

    tcn = DATA["tcn"].copy()
    if not tcn.empty and "pred_tcn" in tcn.columns:
        tcn = tcn.rename(columns={"pred_tcn": "pred"})

    sarima = DATA["sarima"].copy()
    if not sarima.empty and "prediccion" in sarima.columns:
        sarima = sarima.rename(columns={"prediccion": "pred"})

    prophet = DATA["prophet"].copy()
    if not prophet.empty and "prediccion" in prophet.columns:
        prophet = prophet.rename(columns={"prediccion": "pred"})

    gc2 = DATA["gc2"].copy()
    if not gc2.empty and "hibrido" in gc2.columns:
        gc2 = gc2.rename(columns={"hibrido": "pred"})

    return {
        "GE (principal)":      (ge,      C_GE,      "solid",       "triangle-up"),
        "XGBoost":             (xgb,     C_XGB,     "dash",        "square"),
        "GM v2 (NLP mejorado)":(gmv2,    C_GMV2,    "dot",         "diamond"),
        "GC1-SARIMA":          (sarima,  C_SARIMA,  "dash",        "circle"),
        "GC1-Prophet":         (prophet, C_PROPHET, "dot",         "star"),
        "GC2-SARIMAX+LSTM":    (gc2,     C_GC2,     "longdashdot", "pentagon"),
        "TCN":                 (tcn,     C_TCN,     "dashdot",     "x"),
    }


def robustez_table():
    catalog = _build_catalog()
    rows = []
    for nom, (df, *_) in catalog.items():
        if df is None or df.empty or "pred" not in df.columns:
            continue
        df2 = df.dropna(subset=["real", "pred"])
        if len(df2) < 2:
            continue
        from sklearn.metrics import mean_absolute_error as mae_fn
        mae_g = mae_fn(df2["real"], df2["pred"])
        mask = df2["fecha"].isin(SHOCK_DATES)
        df_sh = df2[mask]
        if len(df_sh) >= 2:
            mae_s = mae_fn(df_sh["real"], df_sh["pred"])
            det = (mae_s - mae_g) / mae_g * 100
            if det <= 5:
                verd = "✅ Robusto"
            elif det <= 20:
                verd = "⚠️ Vulnerable"
            else:
                verd = "❌ No robusto"
            rows.append({
                "Modelo": nom,
                "MAE global": f"{mae_g:.4f}",
                "MAE shocks": f"{mae_s:.4f}",
                "Deterioro": f"{det:+.1f}%",
                "Veredicto": verd,
            })
        else:
            rows.append({
                "Modelo": nom,
                "MAE global": f"{mae_g:.4f}",
                "MAE shocks": "N/D",
                "Deterioro": "N/D",
                "Veredicto": "N/D (< 2 meses de shock)",
            })
    return rows


# ──────────────────────────────────────────────────────────────────────────────
# LAYOUT COMPONENTS
# ──────────────────────────────────────────────────────────────────────────────

def topbar():
    return dbc.Navbar(
        dbc.Container([
            html.Span("🌿", style={"fontSize": "1.6rem", "marginRight": "10px"}),
            dbc.NavbarBrand(
                [html.Span("AgroNLP-Clima", style={"fontWeight": "800", "color": C_GE}),
                 html.Span(" · UPeU", style={"color": "#94A3B8", "fontSize": "0.9rem"})],
                style={"fontSize": "1.15rem"},
            ),
            dbc.Nav([
                dbc.NavItem(dbc.NavLink("Tesis FIA 2026", style={"color": "#64748B", "fontSize": "0.82rem"})),
                dbc.NavItem(dbc.NavLink("Fabrizio Sánchez S.", style={"color": "#64748B", "fontSize": "0.82rem"})),
            ], className="ms-auto"),
        ], fluid=True),
        color=DARK_BG,
        dark=True,
        style={"borderBottom": f"1px solid #334155", "padding": "8px 0"},
    )


def sidebar(active_models_default):
    return html.Div([
        html.Div([
            html.Div("⚙️ Configuración", style={
                "color": C_GE, "fontWeight": "700", "fontSize": "0.85rem",
                "textTransform": "uppercase", "letterSpacing": "0.08em",
                "padding": "16px 16px 8px",
            }),
            html.Hr(style={"borderColor": "#334155", "margin": "0 16px 12px"}),

            html.Div("Modelos activos", style={
                "color": "#94A3B8", "fontSize": "0.75rem", "fontWeight": "600",
                "textTransform": "uppercase", "padding": "0 16px 8px",
            }),
            dcc.Checklist(
                id="model-checklist",
                options=[
                    {"label": html.Span(m, style={"color": MODEL_COLORS.get(m, "#ccc"),
                                                  "fontSize": "0.82rem", "marginLeft": "6px"}),
                     "value": m}
                    for m in [
                        "GE (principal)", "XGBoost", "GM v2 (NLP mejorado)",
                        "GC1-SARIMA", "GC1-Prophet", "GC2-SARIMAX+LSTM", "TCN",
                    ]
                ],
                value=active_models_default,
                style={"padding": "0 16px"},
                inputStyle={"cursor": "pointer"},
                labelStyle={"display": "flex", "alignItems": "center",
                            "marginBottom": "8px", "cursor": "pointer"},
            ),

            html.Hr(style={"borderColor": "#334155", "margin": "16px"}),

            html.Div("Umbral alerta de shock", style={
                "color": "#94A3B8", "fontSize": "0.75rem", "fontWeight": "600",
                "textTransform": "uppercase", "padding": "0 16px 8px",
            }),
            html.Div([
                dcc.Slider(
                    id="shock-threshold",
                    min=0.05, max=0.20, step=0.01, value=0.08,
                    marks={0.05: {"label": "0.05", "style": {"color": "#64748B"}},
                           0.10: {"label": "0.10", "style": {"color": "#64748B"}},
                           0.15: {"label": "0.15", "style": {"color": "#64748B"}},
                           0.20: {"label": "0.20", "style": {"color": "#64748B"}}},
                    tooltip={"placement": "bottom"},
                ),
            ], style={"padding": "0 12px"}),

            html.Hr(style={"borderColor": "#334155", "margin": "16px"}),

            html.Div([
                html.Div("📅 Test GE", style={"color": "#94A3B8", "fontSize": "0.72rem",
                                               "fontWeight": "600", "marginBottom": "4px"}),
                html.Div("Nov 2024 – Ago 2025", style={"color": "#CBD5E1", "fontSize": "0.78rem"}),
                html.Div("10 meses · n=40 train", style={"color": "#64748B", "fontSize": "0.72rem"}),

                html.Hr(style={"borderColor": "#334155", "margin": "12px 0"}),

                html.Div("🏆 Modelo ganador", style={"color": "#94A3B8", "fontSize": "0.72rem",
                                                      "fontWeight": "600", "marginBottom": "4px"}),
                html.Div("GE · MAE = 0.0673", style={"color": C_GE, "fontSize": "0.82rem",
                                                       "fontWeight": "700"}),
                html.Div("XGBoost · MAE = 0.0471", style={"color": C_XGB, "fontSize": "0.78rem"}),
                html.Div("(mayor robustez en shocks: +2.3%)",
                         style={"color": "#64748B", "fontSize": "0.7rem", "marginTop": "2px"}),

                html.Hr(style={"borderColor": "#334155", "margin": "12px 0"}),

                html.Div("🌿 UPEU FIA · Tesis 2026",
                         style={"color": "#475569", "fontSize": "0.72rem", "fontStyle": "italic"}),
                html.Div("fabrizio.sanchez.s@upeu.edu.pe",
                         style={"color": "#475569", "fontSize": "0.68rem"}),
            ], style={"padding": "0 16px 16px"}),
        ]),
    ], style={
        "width": "220px", "minWidth": "220px", "background": DARK_BG,
        "borderRight": "1px solid #334155",
        "height": "calc(100vh - 56px)", "overflowY": "auto",
        "position": "sticky", "top": "56px",
    })


# ──────────────────────────────────────────────────────────────────────────────
# TAB CONTENTS
# ──────────────────────────────────────────────────────────────────────────────

def tab_metricas():
    val = DATA["validacion"]
    crmse = val.get("rmse_condicionado", {})
    ge_rmse_g = crmse.get("GE", {}).get("rmse_global", 0.0698)
    ge_rmse_s = crmse.get("GE", {}).get("rmse_shock", 0.0717)
    n_shocks = crmse.get("GE", {}).get("n_shock", 8)

    kpis = dbc.Row([
        dbc.Col(kpi_card("Mejor MAE (escala orig.)", "0.0471",
                         "XGBoost · test nov 2024–ago 2025", C_XGB, "🥇"), md=3),
        dbc.Col(kpi_card("MAE Modelo GE", "0.0673",
                         "DualLSTM-Bahdanau · modelo principal tesis", C_GE, "🧠"), md=3),
        dbc.Col(kpi_card("Robustez GE en shocks", "+2.3%",
                         f"RMSE global {ge_rmse_g:.4f} → shock {ge_rmse_s:.4f}", C_SUCCESS, "🛡️"), md=3),
        dbc.Col(kpi_card("Meses de shock", f"{n_shocks} / 12",
                         "Variación real > 20% sobre MA3", C_ALERT, "⚡"), md=3),
    ], className="g-3 mb-4")

    bar_ranking = dbc.Card(
        dbc.CardBody([
            html.H6("Ranking completo — 9 modelos", className="card-title",
                    style={"color": "#94A3B8"}),
            dcc.Graph(figure=fig_ranking_mae(), config={"displayModeBar": False}),
        ]),
        style={"background": "#0F172A", "border": "1px solid #1E293B", "borderRadius": "10px"},
        className="mb-4",
    )

    bar_fase3 = dbc.Card(
        dbc.CardBody([
            html.H6("Comparativa MAE/RMSE — Fase 3 (z-score)", className="card-title",
                    style={"color": "#94A3B8"}),
            dcc.Graph(figure=fig_metrics_bar(), config={"displayModeBar": False}),
        ]),
        style={"background": "#0F172A", "border": "1px solid #1E293B", "borderRadius": "10px"},
        className="mb-4",
    )

    df_table = pd.DataFrame(ALL_MODELS_TABLE)

    table = dbc.Card(
        dbc.CardBody([
            html.H6("Tabla comparativa completa", className="card-title",
                    style={"color": "#94A3B8"}),
            dash_table.DataTable(
                data=df_table.assign(
                    MAE=df_table["MAE"].map("{:.4f}".format),
                    RMSE=df_table["RMSE"].apply(lambda v: f"{v:.4f}" if v else "N/D"),
                    **{"R²": df_table["R²"].apply(lambda v: f"{v:.3f}" if v else "N/D")},
                    MASE=df_table["MASE"].map("{:.2f}".format),
                ).to_dict("records"),
                columns=[{"name": c, "id": c} for c in
                         ["Modelo", "MAE", "RMSE", "R²", "MASE", "Deterioro shock", "Tipo"]],
                style_table={"overflowX": "auto"},
                style_header={"backgroundColor": "#1E293B", "color": "#94A3B8",
                              "fontWeight": "600", "border": "none", "fontSize": "0.78rem"},
                style_cell={"backgroundColor": "#0F172A", "color": "#CBD5E1",
                            "border": "1px solid #1E293B", "fontSize": "0.8rem",
                            "padding": "8px 12px"},
                style_data_conditional=[
                    {"if": {"filter_query": '{Modelo} = "GE (principal)"'},
                     "color": C_GE, "fontWeight": "700"},
                    {"if": {"filter_query": '{Modelo} = "XGBoost"'},
                     "color": C_XGB, "fontWeight": "700"},
                    {"if": {"filter_query": '{Modelo} = "GM v2 (NLP mejorado)"'},
                     "color": C_GMV2, "fontWeight": "700"},
                ],
            ),
        ]),
        style={"background": "#0F172A", "border": "1px solid #1E293B", "borderRadius": "10px"},
    )

    return html.Div([kpis, bar_ranking, bar_fase3, table])


def tab_prediccion_ge():
    ge = DATA["ge"]
    ge_table = []
    if not ge.empty:
        ge_cp = ge.copy()
        ge_cp["Mes"] = ge_cp["fecha"].dt.strftime("%b %Y")
        ge_table = ge_cp[["Mes", "real", "prediccion", "error_abs"]].rename(columns={
            "real": "Real", "prediccion": "Pred GE", "error_abs": "|Error|"
        }).assign(
            Real=ge_cp["real"].map("{:.4f}".format),
            **{"Pred GE": ge_cp["prediccion"].map("{:.4f}".format)},
            **{"|Error|": ge_cp["error_abs"].map("{:.4f}".format)},
        ).to_dict("records") if "error_abs" in ge_cp.columns else []

    return dbc.Row([
        dbc.Col([
            dbc.Card(
                dbc.CardBody([
                    html.H6("GE — Predicción vs Real", style={"color": "#94A3B8"}),
                    dcc.Graph(id="ge-pred-graph",
                              figure=fig_ge_prediction(),
                              config={"displayModeBar": False}),
                ]),
                style={"background": "#0F172A", "border": "1px solid #1E293B", "borderRadius": "10px"},
            ),
        ], md=8),
        dbc.Col([
            dbc.Card(
                dbc.CardBody([
                    html.H6("Error por mes", style={"color": "#94A3B8", "marginBottom": "8px"}),
                    dcc.Graph(figure=fig_ge_error_bars(), config={"displayModeBar": False}),
                    html.Hr(style={"borderColor": "#334155"}),
                    dash_table.DataTable(
                        data=ge_table,
                        columns=[{"name": c, "id": c} for c in ["Mes", "Real", "Pred GE", "|Error|"]],
                        style_table={"overflowX": "auto"},
                        style_header={"backgroundColor": "#1E293B", "color": "#94A3B8",
                                      "fontWeight": "600", "border": "none", "fontSize": "0.75rem"},
                        style_cell={"backgroundColor": "#0F172A", "color": "#CBD5E1",
                                    "border": "1px solid #1E293B", "fontSize": "0.78rem",
                                    "padding": "6px 10px"},
                    ) if ge_table else dbc.Alert("Datos no disponibles", color="secondary"),
                ]),
                style={"background": "#0F172A", "border": "1px solid #1E293B", "borderRadius": "10px"},
            ),
        ], md=4),
    ], className="g-3")


def tab_competidores():
    sub_tabs = dbc.Tabs([
        dbc.Tab(label="📊 Resumen", tab_id="sub-resumen"),
        dbc.Tab(label="📈 Predicciones", tab_id="sub-predicciones"),
        dbc.Tab(label="⚡ Robustez", tab_id="sub-robustez"),
    ], id="comp-subtabs", active_tab="sub-resumen",
    style={"marginBottom": "16px"},
    )
    return html.Div([
        sub_tabs,
        html.Div(id="comp-subtab-content"),
    ])


def render_comp_resumen():
    df = pd.DataFrame(ALL_MODELS_TABLE)
    colors = [MODEL_COLORS.get(m, "#888") for m in df["Modelo"]]
    fig = dark_fig(height=380, margin=dict(t=55, b=55, l=55, r=20))
    fig.add_trace(go.Bar(
        x=df["Modelo"], y=df["MAE"],
        marker_color=colors, marker_line_width=0,
        text=[f"{v:.4f}" for v in df["MAE"]], textposition="outside",
    ))
    fig.add_hline(y=0.0673, line_dash="dash", line_color=C_GE,
                  annotation_text="GE 0.0673", annotation_font_color=C_GE)
    fig.add_hline(y=0.0161, line_dash="dot", line_color=C_NAIVE,
                  annotation_text="Naive 0.0161", annotation_font_color=C_NAIVE,
                  annotation_position="bottom right")
    fig.update_layout(title="MAE — Ranking completo", yaxis_title="MAE",
                      xaxis_tickangle=-25, showlegend=False)
    fig.update_yaxes(gridcolor="#1E293B")

    df_disp = df.assign(
        MAE=df["MAE"].map("{:.4f}".format),
        RMSE=df["RMSE"].apply(lambda v: f"{v:.4f}" if v else "N/D"),
        **{"R²": df["R²"].apply(lambda v: f"{v:.3f}" if v else "N/D")},
        MASE=df["MASE"].map("{:.2f}".format),
    )
    return html.Div([
        dbc.Card(
            dbc.CardBody([
                dcc.Graph(figure=fig, config={"displayModeBar": False}),
            ]),
            style={"background": "#0F172A", "border": "1px solid #1E293B",
                   "borderRadius": "10px", "marginBottom": "16px"},
        ),
        dbc.Card(
            dbc.CardBody([
                html.H6("Tabla completa — todos los modelos", style={"color": "#94A3B8"}),
                dash_table.DataTable(
                    data=df_disp.to_dict("records"),
                    columns=[{"name": c, "id": c} for c in
                             ["Modelo", "MAE", "RMSE", "R²", "MASE", "Deterioro shock", "Tipo"]],
                    style_table={"overflowX": "auto"},
                    style_header={"backgroundColor": "#1E293B", "color": "#94A3B8",
                                  "fontWeight": "600", "border": "none", "fontSize": "0.78rem"},
                    style_cell={"backgroundColor": "#0F172A", "color": "#CBD5E1",
                                "border": "1px solid #1E293B", "fontSize": "0.8rem",
                                "padding": "8px 12px"},
                ),
            ]),
            style={"background": "#0F172A", "border": "1px solid #1E293B", "borderRadius": "10px"},
        ),
    ])


def render_comp_predicciones(active_models):
    fig = fig_competitors_lines(active_models)
    return dbc.Card(
        dbc.CardBody([
            html.P("Real en blanco grueso · áreas rojas = meses de shock",
                   style={"color": "#64748B", "fontSize": "0.8rem"}),
            dcc.Graph(id="comp-lines-graph", figure=fig,
                      config={"displayModeBar": True}),
        ]),
        style={"background": "#0F172A", "border": "1px solid #1E293B", "borderRadius": "10px"},
    )


def render_comp_robustez(active_models):
    fig1, fig2 = fig_competitors_bars_global_shock(active_models)
    rows = robustez_table()

    return html.Div([
        dbc.Row([
            dbc.Col(dbc.Card(
                dbc.CardBody(dcc.Graph(figure=fig1, config={"displayModeBar": False})),
                style={"background": "#0F172A", "border": "1px solid #1E293B", "borderRadius": "10px"},
            ), md=6),
            dbc.Col(dbc.Card(
                dbc.CardBody(dcc.Graph(figure=fig2, config={"displayModeBar": False})),
                style={"background": "#0F172A", "border": "1px solid #1E293B", "borderRadius": "10px"},
            ), md=6),
        ], className="g-3 mb-3"),
        dbc.Card(
            dbc.CardBody([
                html.H6("Tabla de robustez — todos los modelos disponibles",
                        style={"color": "#94A3B8"}),
                dash_table.DataTable(
                    data=rows,
                    columns=[{"name": c, "id": c} for c in
                             ["Modelo", "MAE global", "MAE shocks", "Deterioro", "Veredicto"]],
                    style_table={"overflowX": "auto"},
                    style_header={"backgroundColor": "#1E293B", "color": "#94A3B8",
                                  "fontWeight": "600", "border": "none", "fontSize": "0.78rem"},
                    style_cell={"backgroundColor": "#0F172A", "color": "#CBD5E1",
                                "border": "1px solid #1E293B", "fontSize": "0.8rem",
                                "padding": "8px 12px"},
                ) if rows else dbc.Alert("Datos de predicciones no disponibles", color="secondary"),
            ]),
            style={"background": "#0F172A", "border": "1px solid #1E293B", "borderRadius": "10px"},
        ),
        html.P(
            "Deterioro = (MAE_shocks − MAE_global) / MAE_global × 100. "
            "Shocks: 2024-09, 2024-10, 2024-12, 2025-01, 2025-02, 2025-04, 2025-05, 2025-06.",
            style={"color": "#475569", "fontSize": "0.72rem", "marginTop": "10px",
                   "fontStyle": "italic"},
        ),
    ])


def tab_shap():
    return html.Div([
        dbc.Row([
            dbc.Col(
                dbc.Card(
                    dbc.CardBody([
                        html.H6("Top 15 features — Importancia SHAP global",
                                style={"color": "#94A3B8"}),
                        dcc.Graph(figure=fig_shap_bars(), config={"displayModeBar": False}),
                    ]),
                    style={"background": "#0F172A", "border": "1px solid #1E293B",
                           "borderRadius": "10px"},
                ), md=8,
            ),
            dbc.Col([
                _shap_interp_card("#1 💨 Velocidad del viento (WS2M)",
                                  "SHAP = 0.0700",
                                  "Vientos altos aceleran la evapotranspiración, afectando el "
                                  "estrés hídrico del limonero. Picos de viento en ciertos "
                                  "trimestres preceden caídas de producción.", C_GE),
                _shap_interp_card("#2 📅 Trimestre (número)",
                                  "SHAP = 0.0448",
                                  "Captura estacionalidad anual de cuatro fases. "
                                  "El limón tiene ciclos de cosecha marcados (pico trim. 2–3).", C_XGB),
                _shap_interp_card("#3 🔄 Mes — componente coseno",
                                  "SHAP = 0.0308",
                                  "La codificación cíclica del mes permite al modelo entender "
                                  "que diciembre y enero son meses 'cercanos' temporalmente.", C_GMV2),
            ], md=4),
        ], className="g-3 mb-4"),
        dbc.Card(
            dbc.CardBody([
                html.H6("Contribución SHAP mensual — top 5 variables",
                        style={"color": "#94A3B8"}),
                dcc.Graph(figure=fig_shap_heatmap(), config={"displayModeBar": False}),
            ]),
            style={"background": "#0F172A", "border": "1px solid #1E293B", "borderRadius": "10px"},
        ),
        html.P(
            "SHAP KernelExplainer con 200 muestras de fondo. "
            "Valores positivos (rojo) empujan la predicción hacia arriba; negativos (azul), hacia abajo. "
            "Escala z-score igual que las predicciones.",
            style={"color": "#475569", "fontSize": "0.72rem", "marginTop": "10px",
                   "fontStyle": "italic"},
        ),
    ])


def _shap_interp_card(title, subtitle, desc, color):
    return dbc.Card(
        dbc.CardBody([
            html.Div(title, style={"color": color, "fontWeight": "700", "fontSize": "0.82rem"}),
            html.Div(subtitle, style={"color": "#94A3B8", "fontSize": "0.75rem", "marginBottom": "6px"}),
            html.Div(desc, style={"color": "#CBD5E1", "fontSize": "0.78rem"}),
        ]),
        style={"background": "#0F172A", "border": f"1px solid {color}44",
               "borderLeft": f"3px solid {color}", "borderRadius": "8px",
               "marginBottom": "10px"},
    )


def tab_attention():
    return dbc.Row([
        dbc.Col(
            dbc.Card(
                dbc.CardBody([
                    html.H6("Heatmap de pesos de atención Bahdanau — período de test",
                            style={"color": "#94A3B8"}),
                    img_or_placeholder(P_ATT_HEATMAP, "ge_attention_heatmap_test.png pendiente"),
                ]),
                style={"background": "#0F172A", "border": "1px solid #1E293B", "borderRadius": "10px"},
            ), md=7,
        ),
        dbc.Col([
            dbc.Card(
                dbc.CardBody([
                    html.H6("Canal A — Input climatológico + producción (63 features)",
                            style={"color": C_GE, "fontWeight": "700"}),
                    html.P(
                        "Pesos uniformes ≈ 0.167 sobre el horizonte t−5 a t. "
                        "Comportamiento consistente con Small Data: el modelo no identifica "
                        "un rezago dominante y distribuye la atención equitativamente "
                        "entre los 6 pasos anteriores.",
                        style={"color": "#CBD5E1", "fontSize": "0.85rem"},
                    ),
                ]),
                style={"background": "#0F172A", "border": f"1px solid {C_GE}33",
                       "borderLeft": f"3px solid {C_GE}", "borderRadius": "8px",
                       "marginBottom": "12px"},
            ),
            dbc.Card(
                dbc.CardBody([
                    html.H6("Canal B — Features NLP BETO (25 features)",
                            style={"color": C_GMV2, "fontWeight": "700"}),
                    html.P(
                        "Gradiente de atención: t−5 = 0.128 → t = 0.210 (agosto 2025). "
                        "El modelo asigna mayor peso a los pasos recientes, capturando que "
                        "los shocks de precio recientes son más informativos que los rezagos "
                        "distantes. Valida la incorporación del canal NLP para eventos de "
                        "alta volatilidad.",
                        style={"color": "#CBD5E1", "fontSize": "0.85rem"},
                    ),
                ]),
                style={"background": "#0F172A", "border": f"1px solid {C_GMV2}33",
                       "borderLeft": f"3px solid {C_GMV2}", "borderRadius": "8px",
                       "marginBottom": "12px"},
            ),
            html.P(
                "Pesos Bahdanau normalizados con softmax sobre 6 pasos temporales (t−5 a t). "
                "Canal A: 63 features. Canal B: 25 features NLP.",
                style={"color": "#475569", "fontSize": "0.72rem", "fontStyle": "italic"},
            ),
        ], md=5),
    ], className="g-3")


def tab_nlp():
    return html.Div([
        dbc.Row([
            dbc.Col(
                dbc.Card(
                    dbc.CardBody([
                        html.H6("Pipeline de features NLP (M1–M4) — BETO",
                                style={"color": "#94A3B8"}),
                        img_or_placeholder(P_NLP_FEAT_IMG, "nlp_features_engineering.png pendiente"),
                    ]),
                    style={"background": "#0F172A", "border": "1px solid #1E293B", "borderRadius": "10px"},
                ), md=6,
            ),
            dbc.Col(
                dbc.Card(
                    dbc.CardBody([
                        html.H6("PCA — Varianza explicada acumulada (Canal B)",
                                style={"color": "#94A3B8"}),
                        img_or_placeholder(P_PCA_IMG, "pca_varianza_explicada.png pendiente"),
                    ]),
                    style={"background": "#0F172A", "border": "1px solid #1E293B", "borderRadius": "10px"},
                ), md=6,
            ),
        ], className="g-3 mb-4"),
        dbc.Row([
            dbc.Col(
                dbc.Card(
                    dbc.CardBody([
                        html.H6("Feature Importance — XGBoost", style={"color": "#94A3B8"}),
                        img_or_placeholder(P_XGB_FEAT_IMG, "xgb_feature_importance.png pendiente"),
                    ]),
                    style={"background": "#0F172A", "border": "1px solid #1E293B", "borderRadius": "10px"},
                ), md=6,
            ),
            dbc.Col([
                dbc.Card(
                    dbc.CardBody([
                        html.H6("Módulos NLP integrados", style={"color": C_GMV2, "fontWeight": "700"}),
                        html.Ul([
                            html.Li([html.Span("M1 ", style={"color": C_GE, "fontWeight": "700"}),
                                     "Índice de sentimiento BETO (avg_sentiment, n_noticias)"],
                                    style={"color": "#CBD5E1", "fontSize": "0.82rem",
                                           "marginBottom": "6px"}),
                            html.Li([html.Span("M2 ", style={"color": C_XGB, "fontWeight": "700"}),
                                     "Lag-1 de features NLP para capturar rezago informativo"],
                                    style={"color": "#CBD5E1", "fontSize": "0.82rem",
                                           "marginBottom": "6px"}),
                            html.Li([html.Span("M3 ", style={"color": C_GMV2, "fontWeight": "700"}),
                                     "Dropout NLP 0.5 — regularización específica del canal B"],
                                    style={"color": "#CBD5E1", "fontSize": "0.82rem",
                                           "marginBottom": "6px"}),
                            html.Li([html.Span("M4 ", style={"color": C_SUCCESS, "fontWeight": "700"}),
                                     "PCA al 95% de varianza — reduce 25→8 componentes en Canal B"],
                                    style={"color": "#CBD5E1", "fontSize": "0.82rem"}),
                        ], style={"paddingLeft": "18px", "marginTop": "8px"}),
                    ]),
                    style={"background": "#0F172A", "border": f"1px solid {C_GMV2}33",
                           "borderLeft": f"3px solid {C_GMV2}", "borderRadius": "8px",
                           "marginBottom": "12px"},
                ),
                html.P(
                    "GM_v2 (M1+M2+M3+M4) logra MAE=0.0646, superando al GE sin NLP (0.0673). "
                    "El PCA elimina ruido del espacio de embeddings BETO antes de la fusión LSTM.",
                    style={"color": "#475569", "fontSize": "0.72rem", "fontStyle": "italic"},
                ),
            ], md=6),
        ], className="g-3"),
    ])


# ──────────────────────────────────────────────────────────────────────────────
# APP LAYOUT
# ──────────────────────────────────────────────────────────────────────────────

app = dash.Dash(
    __name__,
    external_stylesheets=[dbc.themes.DARKLY],
    suppress_callback_exceptions=True,
    title="AgroNLP-Clima · UPeU",
    meta_tags=[{"name": "viewport", "content": "width=device-width, initial-scale=1"}],
)
server = app.server

DEFAULT_ACTIVE = ["GE (principal)", "XGBoost", "GM v2 (NLP mejorado)", "GC1-SARIMA"]

app.layout = html.Div([
    topbar(),
    html.Div([
        sidebar(DEFAULT_ACTIVE),
        html.Div([
            # Main tabs
            dbc.Tabs([
                dbc.Tab(label="📊 Métricas",          tab_id="tab-metricas"),
                dbc.Tab(label="📈 Predicción GE",     tab_id="tab-pred"),
                dbc.Tab(label="🏆 Competidores",      tab_id="tab-comp"),
                dbc.Tab(label="🔍 SHAP",              tab_id="tab-shap"),
                dbc.Tab(label="🧠 Attention XAI",     tab_id="tab-attn"),
                dbc.Tab(label="💬 NLP Engineering",   tab_id="tab-nlp"),
            ], id="main-tabs", active_tab="tab-metricas",
            style={"position": "sticky", "top": "56px", "zIndex": "100",
                   "background": "#0F172A", "borderBottom": "1px solid #1E293B",
                   "padding": "0 16px"},
            ),
            html.Div(id="tab-content",
                     style={"padding": "24px", "background": MAIN_BG, "minHeight": "calc(100vh - 112px)"}),
        ], style={"flex": "1", "overflowY": "auto", "background": MAIN_BG}),
    ], style={"display": "flex", "height": "calc(100vh - 56px)", "overflow": "hidden"}),
], style={"fontFamily": "'Inter', 'Segoe UI', sans-serif", "background": MAIN_BG})


# ──────────────────────────────────────────────────────────────────────────────
# CALLBACKS
# ──────────────────────────────────────────────────────────────────────────────

@callback(
    Output("tab-content", "children"),
    Input("main-tabs", "active_tab"),
    Input("model-checklist", "value"),
    Input("shock-threshold", "value"),
)
def render_tab(active_tab, active_models, shock_threshold):
    active_models = active_models or DEFAULT_ACTIVE
    if active_tab == "tab-metricas":
        return tab_metricas()
    elif active_tab == "tab-pred":
        return tab_prediccion_ge()
    elif active_tab == "tab-comp":
        return tab_competidores()
    elif active_tab == "tab-shap":
        return tab_shap()
    elif active_tab == "tab-attn":
        return tab_attention()
    elif active_tab == "tab-nlp":
        return tab_nlp()
    return html.Div("Tab no encontrado", style={"color": "#EF4444"})


@callback(
    Output("comp-subtab-content", "children"),
    Input("comp-subtabs", "active_tab"),
    Input("model-checklist", "value"),
)
def render_comp_subtab(active_sub, active_models):
    active_models = active_models or DEFAULT_ACTIVE
    if active_sub == "sub-resumen":
        return render_comp_resumen()
    elif active_sub == "sub-predicciones":
        return render_comp_predicciones(active_models)
    elif active_sub == "sub-robustez":
        return render_comp_robustez(active_models)
    return html.Div()


# ──────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import webbrowser, threading
    def _open():
        import time; time.sleep(1.5)
        webbrowser.open("http://127.0.0.1:8050")
    threading.Thread(target=_open, daemon=True).start()
    print("\n🌿 AgroNLP-Clima Dashboard — Dash + Plotly")
    print("   http://127.0.0.1:8050\n")
    app.run(debug=False, host="127.0.0.1", port=8050)
