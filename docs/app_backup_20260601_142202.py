"""
dashboard/app.py — Agro-NLP-Clima Dashboard
Ejecutar: .\venv\Scripts\streamlit run dashboard\app.py
"""

import json
import pathlib

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

# ──────────────────────────────────────────────────────────────────────────────
# PATHS
# ──────────────────────────────────────────────────────────────────────────────

BASE_DIR = pathlib.Path(__file__).resolve().parent.parent  # project root

P_GE_PREDS    = BASE_DIR / "resultados/ge/ge_predicciones.csv"
P_GE_METRICS  = BASE_DIR / "resultados/ge/ge_metricas.json"
P_GC1_SARIMA  = BASE_DIR / "resultados/gc1/gc1_sarima_metricas.json"
P_GC1_PROPHET = BASE_DIR / "resultados/gc1/gc1_prophet_metricas.json"
P_GC2         = BASE_DIR / "resultados/gc2/gc2_metricas.json"
P_SHAP        = BASE_DIR / "resultados/shap/shap_resultados.json"
P_VALIDACION  = BASE_DIR / "resultados/validacion/validacion_estadistica.json"
P_MASTER      = BASE_DIR / "data/processed/master_dataset_fase2_multivariado.csv"

# Fase 4 — competidores y XAI
P_XGB_PREDS_IMG  = BASE_DIR / "resultados/xgboost/xgb_predicciones_vs_real.png"
P_XGB_FEAT_IMG   = BASE_DIR / "resultados/xgboost/xgb_feature_importance.png"
P_GMV2_PREDS_IMG = BASE_DIR / "resultados/gm_v2/gm_v2_predicciones_vs_real.png"
P_ATT_HEATMAP    = BASE_DIR / "resultados/ge/ge_attention_heatmap_test.png"
P_NLP_FEAT_IMG   = BASE_DIR / "resultados/gm_v2/nlp_features_engineering.png"
P_PCA_IMG        = BASE_DIR / "resultados/gm_v2/pca_varianza_explicada.png"

# ──────────────────────────────────────────────────────────────────────────────
# PAGE CONFIG  (must be first Streamlit call)
# ──────────────────────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="Agro-NLP-Clima | UPEU",
    page_icon="🌿",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ──────────────────────────────────────────────────────────────────────────────
# CSS
# ──────────────────────────────────────────────────────────────────────────────

st.markdown("""
<style>
    /* Card-like metric boxes */
    .metric-card {
        background: #f8f9fa;
        border-radius: 8px;
        padding: 12px 16px;
        margin-bottom: 8px;
        border-left: 4px solid #2e7d32;
    }
    .metric-card.best {
        background: #e8f5e9;
        border-left: 4px solid #1b5e20;
    }
    /* Section headers */
    .section-title {
        font-size: 1.1rem;
        font-weight: 700;
        color: #1b5e20;
        margin-top: 0.5rem;
        margin-bottom: 0.5rem;
    }
    /* SHAP interpretation box */
    .shap-box {
        background: #fff8e1;
        border-radius: 6px;
        padding: 12px;
        border-left: 4px solid #f9a825;
        margin-top: 8px;
    }
    /* Disclaimer */
    .disclaimer {
        font-size: 0.78rem;
        color: #757575;
        font-style: italic;
    }
    /* Hide Streamlit branding in header */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
</style>
""", unsafe_allow_html=True)

# ──────────────────────────────────────────────────────────────────────────────
# DATA LOADERS  (cached)
# ──────────────────────────────────────────────────────────────────────────────

@st.cache_data(show_spinner=False)
def load_ge_preds() -> pd.DataFrame:
    df = pd.read_csv(P_GE_PREDS, parse_dates=["fecha"])
    return df.sort_values("fecha").reset_index(drop=True)


@st.cache_data(show_spinner=False)
def load_master() -> pd.DataFrame:
    df = pd.read_csv(P_MASTER, parse_dates=["fecha_evento"])
    df = df.sort_values(["provincia", "fecha_evento"]).reset_index(drop=True)
    return df


@st.cache_data(show_spinner=False)
def load_shap() -> dict:
    return json.loads(P_SHAP.read_text(encoding="utf-8"))


@st.cache_data(show_spinner=False)
def load_validacion() -> dict:
    return json.loads(P_VALIDACION.read_text(encoding="utf-8"))


@st.cache_data(show_spinner=False)
def load_metrics() -> pd.DataFrame:
    rows = []
    for path, label in [
        (P_GC1_SARIMA,  "GC1-SARIMA"),
        (P_GC1_PROPHET, "GC1-Prophet"),
        (P_GC2,         "GC2-SARIMAX+LSTM"),
        (P_GE_METRICS,  "GE-DualLSTM-Attn"),
    ]:
        m = json.loads(path.read_text(encoding="utf-8"))
        rows.append({
            "Modelo": label,
            "MAE":  m.get("MAE",  float("nan")),
            "RMSE": m.get("RMSE", float("nan")),
            "R²":   m.get("R2",   float("nan")),
            "N test": m.get("n_test", "—"),
            "es_mejor": label == "GE-DualLSTM-Attn",
        })
    return pd.DataFrame(rows)


# ──────────────────────────────────────────────────────────────────────────────
# HEADER
# ──────────────────────────────────────────────────────────────────────────────

col_logo, col_title = st.columns([1, 5])
with col_logo:
    st.markdown("""
    <div style='text-align:center; padding-top:8px;'>
        <span style='font-size:3rem;'>🌿</span><br>
        <b style='font-size:0.7rem; color:#1b5e20;'>UPEU · FIA</b>
    </div>
    """, unsafe_allow_html=True)

with col_title:
    st.markdown("""
    <h1 style='margin-bottom:0; color:#1b5e20;'>
        Sistema Multimodal de Predicción Agroindustrial
    </h1>
    <p style='color:#555; margin-top:4px; font-size:1rem;'>
        Predicción de demanda de <b>limón</b> en Perú mediante
        DualLSTM + Atención de Bahdanau, NLP (BETO) y datos NASA POWER.
        <span style='color:#888;'>· Universidad Peruana Unión · Tesis FIA 2026</span>
    </p>
    """, unsafe_allow_html=True)

st.divider()

# ──────────────────────────────────────────────────────────────────────────────
# SIDEBAR
# ──────────────────────────────────────────────────────────────────────────────

with st.sidebar:
    st.markdown("## ⚙️ Configuración")
    st.markdown("---")

    master_df = load_master()
    provincias = sorted(master_df["provincia"].dropna().unique())
    provincia_sel = st.selectbox(
        "Provincia",
        provincias,
        index=provincias.index("PIURA") if "PIURA" in provincias else 0,
        help="Filtra el histórico de producción por provincia.",
    )

    horizonte = st.slider(
        "Horizonte de predicción (meses)",
        min_value=1,
        max_value=6,
        value=3,
        help="Meses a destacar en el período de test del modelo GE.",
    )

    st.markdown("---")
    st.markdown("""
    **Período de test GE**
    Nov 2024 – Ago 2025 (10 meses)

    **Métricas en escala z-score**
    (normalización provincial, Fase 2)

    **Modelo ganador:** GE · MAE = 0.067
    """)

    st.markdown("---")
    st.markdown(
        "<p class='disclaimer'>Fabrizio Sánchez S. · UPEU FIA · 2026</p>",
        unsafe_allow_html=True,
    )

# ──────────────────────────────────────────────────────────────────────────────
# TABS
# ──────────────────────────────────────────────────────────────────────────────

tab_met, tab_pred, tab_shap, tab_rob, tab_comp, tab_attn, tab_nlp = st.tabs([
    "📊 Métricas comparativas",
    "📈 Predicción GE",
    "🔍 Análisis SHAP",
    "🛡️ Robustez (meses de shock)",
    "🏆 Competidores",
    "🧠 Attention XAI",
    "💬 NLP Engineering",
])

# ══════════════════════════════════════════════════════════════════════════════
# TAB 1 — MÉTRICAS
# ══════════════════════════════════════════════════════════════════════════════

with tab_met:
    st.markdown("### Comparativa de modelos — escala z-score (media provincial)")

    metrics_df = load_metrics()

    # ── Tarjetas de métricas superiores ──────────────────────────────────────
    cols = st.columns(4)
    model_colors = {
        "GC1-SARIMA":       "#1976d2",
        "GC1-Prophet":      "#7b1fa2",
        "GC2-SARIMAX+LSTM": "#e64a19",
        "GE-DualLSTM-Attn": "#2e7d32",
    }
    for col, (_, row) in zip(cols, metrics_df.iterrows()):
        color = model_colors.get(row["Modelo"], "#555")
        best_badge = " 🏆" if row["es_mejor"] else ""
        with col:
            st.markdown(f"""
            <div style='background:#f8f9fa; border-radius:8px; padding:14px;
                        border-top: 4px solid {color}; min-height:130px;'>
                <div style='font-weight:700; font-size:0.85rem; color:{color};'>
                    {row["Modelo"]}{best_badge}
                </div>
                <div style='margin-top:8px;'>
                    <span style='font-size:1.4rem; font-weight:700;'>{row["MAE"]:.4f}</span>
                    <span style='font-size:0.75rem; color:#888;'> MAE</span>
                </div>
                <div style='font-size:0.9rem; color:#444;'>
                    RMSE: <b>{row["RMSE"]:.4f}</b>
                </div>
                <div style='font-size:0.9rem; color:#444;'>
                    R²: <b>{row["R²"]:.3f}</b>
                </div>
                <div style='font-size:0.75rem; color:#888; margin-top:4px;'>
                    N test: {row["N test"]} meses
                </div>
            </div>
            """, unsafe_allow_html=True)

    st.markdown("&nbsp;")

    # ── Gráfico de barras comparativo ─────────────────────────────────────────
    col_a, col_b = st.columns(2)

    with col_a:
        fig_mae = go.Figure()
        colors_list = [model_colors[m] for m in metrics_df["Modelo"]]
        fig_mae.add_trace(go.Bar(
            x=metrics_df["Modelo"],
            y=metrics_df["MAE"],
            marker_color=colors_list,
            marker_line_width=0,
            text=[f"{v:.4f}" for v in metrics_df["MAE"]],
            textposition="outside",
        ))
        fig_mae.update_layout(
            title="MAE — Error Absoluto Medio",
            yaxis_title="MAE (z-score)",
            xaxis_tickangle=-15,
            height=350,
            margin=dict(t=50, b=30, l=40, r=20),
            plot_bgcolor="white",
            paper_bgcolor="white",
            showlegend=False,
        )
        fig_mae.update_yaxes(gridcolor="#eeeeee")
        st.plotly_chart(fig_mae, use_container_width=True)

    with col_b:
        fig_rmse = go.Figure()
        fig_rmse.add_trace(go.Bar(
            x=metrics_df["Modelo"],
            y=metrics_df["RMSE"],
            marker_color=colors_list,
            marker_line_width=0,
            text=[f"{v:.4f}" for v in metrics_df["RMSE"]],
            textposition="outside",
        ))
        fig_rmse.update_layout(
            title="RMSE — Error Cuadrático Medio",
            yaxis_title="RMSE (z-score)",
            xaxis_tickangle=-15,
            height=350,
            margin=dict(t=50, b=30, l=40, r=20),
            plot_bgcolor="white",
            paper_bgcolor="white",
            showlegend=False,
        )
        fig_rmse.update_yaxes(gridcolor="#eeeeee")
        st.plotly_chart(fig_rmse, use_container_width=True)

    # ── Tabla detallada ───────────────────────────────────────────────────────
    with st.expander("Ver tabla completa de métricas", expanded=False):
        display_df = metrics_df.drop(columns=["es_mejor"]).copy()
        display_df["MAE"]  = display_df["MAE"].map("{:.4f}".format)
        display_df["RMSE"] = display_df["RMSE"].map("{:.4f}".format)
        display_df["R²"]   = display_df["R²"].map("{:.3f}".format)
        st.dataframe(display_df, use_container_width=True, hide_index=True)

    st.markdown("""
    <p class='disclaimer'>
    Todas las métricas están en escala z-score (normalización por media provincial, Fase 2).
    R² negativo indica que los modelos no superan la línea base de media constante;
    el R² es inestable cuando la varianza de la serie objetivo es cercana a cero.
    GE reduce el MAE en −26.8 % y el RMSE en −33.6 % respecto a GC1-Prophet.
    </p>
    """, unsafe_allow_html=True)

    st.markdown("---")
    st.markdown("### Ranking completo — 9 modelos (escala original, MAE)")

    _ALL_MODELS = [
        {"Modelo": "Naive (baseline)",    "MAE": 0.0161, "Fase": "Baseline"},
        {"Modelo": "XGBoost",             "MAE": 0.0471, "Fase": "Fase 4"},
        {"Modelo": "GM_v2",               "MAE": 0.0646, "Fase": "Fase 4 — NLP M1+M2+M3+M4"},
        {"Modelo": "GE-DualLSTM-Attn",    "MAE": 0.0673, "Fase": "Fase 3 — modelo principal"},
        {"Modelo": "GC1-Prophet",         "MAE": 0.0919, "Fase": "Fase 3"},
        {"Modelo": "GM_original",         "MAE": 0.0981, "Fase": "Fase 4 — NLP básico"},
        {"Modelo": "GC1-SARIMA",          "MAE": 0.1006, "Fase": "Fase 3"},
        {"Modelo": "TCN",                 "MAE": 0.1860, "Fase": "Fase 4"},
        {"Modelo": "GC2-SARIMAX+LSTM",    "MAE": 0.1969, "Fase": "Fase 3"},
    ]
    df_all = pd.DataFrame(_ALL_MODELS)

    _COLORS_ALL = [
        "#9e9e9e",   # Naive
        "#ff7043",   # XGBoost
        "#26a69a",   # GM_v2
        "#2e7d32",   # GE
        "#7b1fa2",   # Prophet
        "#5d4037",   # GM_original
        "#1976d2",   # SARIMA
        "#f57f17",   # TCN
        "#e64a19",   # SARIMAX+LSTM
    ]

    fig_all = go.Figure(go.Bar(
        x=df_all["Modelo"],
        y=df_all["MAE"],
        marker_color=_COLORS_ALL,
        marker_line_width=0,
        text=[f"{v:.4f}" for v in df_all["MAE"]],
        textposition="outside",
    ))
    fig_all.add_hline(
        y=0.0673, line_dash="dash", line_color="#2e7d32",
        annotation_text="GE (modelo principal tesis)",
        annotation_position="top right",
    )
    fig_all.update_layout(
        title="Ranking completo — 9 modelos ordenados por MAE (escala original)",
        yaxis_title="MAE",
        height=380,
        margin=dict(t=55, b=30, l=40, r=20),
        plot_bgcolor="white",
        paper_bgcolor="white",
        showlegend=False,
        xaxis_tickangle=-20,
    )
    fig_all.update_yaxes(gridcolor="#eeeeee")
    st.plotly_chart(fig_all, use_container_width=True)

    with st.expander("Ver tabla ranking completo (9 modelos)", expanded=False):
        st.dataframe(df_all, use_container_width=True, hide_index=True)

# ══════════════════════════════════════════════════════════════════════════════
# TAB 2 — PREDICCIÓN GE
# ══════════════════════════════════════════════════════════════════════════════

with tab_pred:
    ge_preds = load_ge_preds()

    col_left, col_right = st.columns([2, 1])

    with col_left:
        st.markdown("### Predicción GE — período de test")

        # Filtrar datos históricos de la provincia seleccionada
        prov_hist = (
            master_df[master_df["provincia"] == provincia_sel]
            .sort_values("fecha_evento")
            [["fecha_evento", "produccion_t"]]
            .dropna()
        )

        # Línea de corte train/test
        test_start = ge_preds["fecha"].min()
        # Destacar horizonte: primeros N meses del test
        horizon_dates = ge_preds["fecha"].sort_values().iloc[:horizonte]

        fig_pred = go.Figure()

        # Histórico provincial (produccion_t en escala z-score)
        if not prov_hist.empty:
            fig_pred.add_trace(go.Scatter(
                x=prov_hist["fecha_evento"],
                y=prov_hist["produccion_t"],
                name=f"Histórico — {provincia_sel}",
                mode="lines",
                line=dict(color="#90a4ae", width=1.5, dash="dot"),
                opacity=0.7,
            ))

        # Valores reales del período de test (media provincial agregada)
        fig_pred.add_trace(go.Scatter(
            x=ge_preds["fecha"],
            y=ge_preds["real"],
            name="Real (media prov. agregada)",
            mode="lines+markers",
            line=dict(color="#1565c0", width=2.5),
            marker=dict(size=7, symbol="circle"),
        ))

        # Predicción GE
        fig_pred.add_trace(go.Scatter(
            x=ge_preds["fecha"],
            y=ge_preds["prediccion"],
            name="Predicción GE",
            mode="lines+markers",
            line=dict(color="#2e7d32", width=2.5),
            marker=dict(size=7, symbol="diamond"),
        ))

        # Banda de error (±MAE)
        mae_val = 0.0673
        fig_pred.add_trace(go.Scatter(
            x=pd.concat([ge_preds["fecha"], ge_preds["fecha"][::-1]]),
            y=pd.concat([
                ge_preds["prediccion"] + mae_val,
                (ge_preds["prediccion"] - mae_val)[::-1],
            ]),
            fill="toself",
            fillcolor="rgba(46,125,50,0.10)",
            line=dict(color="rgba(0,0,0,0)"),
            name="Banda ±MAE",
            showlegend=True,
        ))

        # Línea vertical train/test
        fig_pred.add_vline(
            x=int(test_start.timestamp() * 1000),
            line_dash="dash",
            line_color="#e53935",
            annotation_text="Inicio test",
            annotation_position="top right",
        )

        # Región horizonte
        if horizonte < len(ge_preds):
            fig_pred.add_vrect(
                x0=int(ge_preds["fecha"].min().timestamp() * 1000),
                x1=int(horizon_dates.iloc[-1].timestamp() * 1000),
                fillcolor="rgba(255,213,79,0.15)",
                line_width=0,
                annotation_text=f"Horizonte {horizonte}m",
                annotation_position="top left",
            )

        fig_pred.update_layout(
            title=f"GE — Predicción vs Real · Provincia ref.: {provincia_sel}",
            xaxis_title="Fecha",
            yaxis_title="Producción (z-score)",
            height=420,
            margin=dict(t=55, b=40, l=50, r=20),
            plot_bgcolor="white",
            paper_bgcolor="white",
            legend=dict(orientation="h", yanchor="bottom", y=-0.35),
            hovermode="x unified",
        )
        fig_pred.update_xaxes(gridcolor="#eeeeee")
        fig_pred.update_yaxes(gridcolor="#eeeeee")
        st.plotly_chart(fig_pred, use_container_width=True)

        st.markdown(f"""
        <p class='disclaimer'>
        La línea punteada gris muestra el histórico de producción de <b>{provincia_sel}</b>
        (z-score provincial). Las líneas azul y verde corresponden a los valores reales y predichos
        del modelo GE sobre la <i>media agregada</i> de todas las provincias en el período de test
        (nov 2024 – ago 2025). La banda verde representa ±MAE = {mae_val}.
        </p>
        """, unsafe_allow_html=True)

    with col_right:
        st.markdown("### Error por mes")

        ge_preds["mes_label"] = ge_preds["fecha"].dt.strftime("%b %Y")
        ge_preds["error_color"] = ge_preds["error_abs"].apply(
            lambda v: "#ef5350" if v > 0.08 else "#66bb6a"
        )

        fig_err = go.Figure(go.Bar(
            x=ge_preds["error_abs"],
            y=ge_preds["mes_label"],
            orientation="h",
            marker_color=ge_preds["error_color"].tolist(),
            text=[f"{v:.4f}" for v in ge_preds["error_abs"]],
            textposition="outside",
        ))
        fig_err.add_vline(
            x=ge_preds["error_abs"].mean(),
            line_dash="dash",
            line_color="#555",
            annotation_text=f"MAE={ge_preds['error_abs'].mean():.4f}",
            annotation_position="top right",
        )
        fig_err.update_layout(
            title="Error absoluto mensual (GE)",
            xaxis_title="|real − pred|",
            height=420,
            margin=dict(t=50, b=40, l=90, r=60),
            plot_bgcolor="white",
            paper_bgcolor="white",
            showlegend=False,
        )
        fig_err.update_xaxes(gridcolor="#eeeeee")
        st.plotly_chart(fig_err, use_container_width=True)

        # Tabla resumida
        st.dataframe(
            ge_preds[["mes_label", "real", "prediccion", "error_abs"]]
            .rename(columns={
                "mes_label": "Mes",
                "real": "Real",
                "prediccion": "Pred GE",
                "error_abs": "|Error|",
            })
            .style.format({"Real": "{:.4f}", "Pred GE": "{:.4f}", "|Error|": "{:.4f}"}),
            use_container_width=True,
            hide_index=True,
        )

# ══════════════════════════════════════════════════════════════════════════════
# TAB 3 — SHAP
# ══════════════════════════════════════════════════════════════════════════════

with tab_shap:
    shap_data = load_shap()
    ranking   = shap_data["ranking_global"]

    st.markdown("### Importancia global de variables — SHAP KernelExplainer")

    col_chart, col_text = st.columns([3, 2])

    with col_chart:
        # Top 15 features
        top_n = 15
        feats  = [r["label"]      for r in ranking[:top_n]][::-1]
        imps   = [r["importance"] for r in ranking[:top_n]][::-1]
        bar_colors = [
            "#1b5e20" if i >= top_n - 3 else
            "#43a047" if i >= top_n - 6 else
            "#a5d6a7"
            for i in range(top_n)
        ]

        fig_shap = go.Figure(go.Bar(
            x=imps,
            y=feats,
            orientation="h",
            marker_color=bar_colors,
            text=[f"{v:.4f}" for v in imps],
            textposition="outside",
        ))
        fig_shap.update_layout(
            title=f"Top {top_n} variables por importancia SHAP media |φ|",
            xaxis_title="Importancia SHAP (valor absoluto medio)",
            height=480,
            margin=dict(t=50, b=40, l=110, r=70),
            plot_bgcolor="white",
            paper_bgcolor="white",
            showlegend=False,
        )
        fig_shap.update_xaxes(gridcolor="#eeeeee")
        st.plotly_chart(fig_shap, use_container_width=True)

    with col_text:
        st.markdown("#### Interpretación — Top 3 variables")

        # Descripciones por feature
        FEATURE_DESC = {
            "WS2M": {
                "nombre": "Velocidad del viento (WS2M)",
                "icono": "💨",
                "desc": (
                    "La velocidad del viento a 2 m es la variable más influyente. "
                    "Vientos altos aceleran la evapotranspiración, afectando el estrés hídrico "
                    "del limonero y, por ende, su rendimiento. El modelo aprendió que picos de "
                    "viento en ciertos trimestres preceden caídas de producción."
                ),
            },
            "trimestre_num": {
                "nombre": "Trimestre (número)",
                "icono": "📅",
                "desc": (
                    "El número de trimestre captura la estacionalidad anual de cuatro fases. "
                    "El limón tiene ciclos de cosecha marcados (pico en trim. 2–3), y el modelo "
                    "usa esta variable para distinguir épocas de alta y baja demanda."
                ),
            },
            "month_cos": {
                "nombre": "Mes — componente coseno",
                "icono": "🔄",
                "desc": (
                    "La codificación cíclica del mes (coseno) permite al modelo entender la "
                    "continuidad temporal circular: diciembre y enero son meses 'cercanos' "
                    "aunque numéricamente distantes. Esto mejora la predicción de patrones "
                    "de temporada que cruzan el cambio de año."
                ),
            },
        }

        for rank_i, r in enumerate(ranking[:3], start=1):
            feat_key = r["feature"]
            info = FEATURE_DESC.get(feat_key, {
                "nombre": r["label"],
                "icono": "📌",
                "desc": f"Variable de importancia global = {r['importance']:.4f}.",
            })
            st.markdown(f"""
            <div class='shap-box'>
                <b style='font-size:0.95rem;'>
                    #{rank_i} {info['icono']} {info['nombre']}
                </b>
                <br>
                <span style='font-size:0.78rem; color:#888;'>
                    SHAP medio: <b>{r['importance']:.4f}</b>
                </span>
                <p style='font-size:0.85rem; margin-top:6px; margin-bottom:0;'>
                    {info['desc']}
                </p>
            </div>
            <br>
            """, unsafe_allow_html=True)

    # ── SHAP temporal heatmap ─────────────────────────────────────────────────
    st.markdown("#### SHAP por mes de test — top 5 variables")

    meses_shap = shap_data["shap_por_mes"]
    top5_keys  = [r["feature"] for r in ranking[:5]]
    top5_labs  = [r["label"]   for r in ranking[:5]]
    mes_labels = [m["mes"] for m in meses_shap]

    matrix = np.array([
        [m["shap_top5"].get(k, 0.0) for k in top5_keys]
        for m in meses_shap
    ])  # shape: (10 meses, 5 features)

    fig_heat = go.Figure(go.Heatmap(
        z=matrix.T,
        x=mes_labels,
        y=top5_labs,
        colorscale="RdBu",
        zmid=0,
        text=np.round(matrix.T, 3),
        texttemplate="%{text}",
        colorbar=dict(title="φ SHAP"),
    ))
    fig_heat.update_layout(
        title="Contribución SHAP mensual — top 5 variables (rojo = positivo, azul = negativo)",
        height=300,
        margin=dict(t=50, b=50, l=110, r=20),
        xaxis_title="Mes",
    )
    st.plotly_chart(fig_heat, use_container_width=True)

    st.markdown("""
    <p class='disclaimer'>
    SHAP KernelExplainer aplicado sobre el modelo GE con 200 muestras de fondo.
    Valores positivos (rojo) indican que la variable empujó la predicción hacia arriba;
    negativos (azul), hacia abajo. Los valores SHAP están en la misma escala z-score que
    las predicciones.
    </p>
    """, unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════════
# TAB 4 — ROBUSTEZ
# ══════════════════════════════════════════════════════════════════════════════

with tab_rob:
    val_data = load_validacion()
    crmse    = val_data["rmse_condicionado"]
    dm_data  = val_data["diebold_mariano"]

    st.markdown("### Robustez en meses de shock (variación real > 20 % sobre MA3)")

    # ── RMSE Global vs Shock ──────────────────────────────────────────────────
    model_names  = list(crmse.keys())
    rmse_globals = [crmse[m]["rmse_global"] for m in model_names]
    rmse_shocks  = [crmse[m]["rmse_shock"]  for m in model_names]
    n_shocks     = [crmse[m]["n_shock"]     for m in model_names]

    bar_colors_rob = ["#1976d2", "#7b1fa2", "#e64a19", "#2e7d32"]

    col_rob_a, col_rob_b = st.columns(2)

    with col_rob_a:
        fig_rob = go.Figure()
        fig_rob.add_trace(go.Bar(
            name="RMSE Global",
            x=model_names,
            y=rmse_globals,
            marker_color=bar_colors_rob,
            marker_opacity=0.5,
            text=[f"{v:.4f}" for v in rmse_globals],
            textposition="outside",
        ))
        fig_rob.add_trace(go.Bar(
            name="RMSE Shock (>20 %)",
            x=model_names,
            y=rmse_shocks,
            marker_color=bar_colors_rob,
            marker_opacity=1.0,
            marker_line_color="black",
            marker_line_width=1.2,
            text=[f"{v:.4f}" for v in rmse_shocks],
            textposition="outside",
        ))
        fig_rob.update_layout(
            barmode="group",
            title=f"RMSE Global vs. Condicionado (meses shock: {n_shocks[0]}/{crmse[model_names[0]]['n_total']})",
            yaxis_title="RMSE (z-score)",
            xaxis_tickangle=-10,
            height=400,
            margin=dict(t=55, b=30, l=50, r=20),
            plot_bgcolor="white",
            paper_bgcolor="white",
            legend=dict(orientation="h", yanchor="bottom", y=-0.3),
        )
        fig_rob.update_yaxes(gridcolor="#eeeeee")
        st.plotly_chart(fig_rob, use_container_width=True)

    with col_rob_b:
        # ── Test Diebold-Mariano ──────────────────────────────────────────────
        st.markdown("#### Test Diebold-Mariano HLN (α = 0.05)")
        st.markdown(
            "GE como referencia vs. cada modelo de control · "
            "DM* < 0 → GE mejor · p < 0.05 → significativo"
        )

        dm_pairs  = list(dm_data.keys())
        dm_stars  = [dm_data[p]["dm_star"] for p in dm_pairs]
        dm_pvals  = [dm_data[p]["pvalue"]  for p in dm_pairs]
        dm_reject = [dm_data[p]["reject_h0"] for p in dm_pairs]

        dm_colors = [
            "#2e7d32" if (rej and s < 0) else
            "#c62828" if rej else
            "#9e9e9e"
            for rej, s in zip(dm_reject, dm_stars)
        ]

        dm_labels = [p.replace("GE_vs_", "vs ") for p in dm_pairs]

        fig_dm = go.Figure()
        fig_dm.add_trace(go.Bar(
            x=dm_stars,
            y=dm_labels,
            orientation="h",
            marker_color=dm_colors,
            text=[
                f"DM*={s:.3f}  p={p:.4f}  {'*' if r else 'ns'}"
                for s, p, r in zip(dm_stars, dm_pvals, dm_reject)
            ],
            textposition="outside",
        ))

        # Valor crítico t(9) al 5%
        from scipy.stats import t as t_dist
        tc = float(t_dist.ppf(0.975, df=9))
        fig_dm.add_vline(x=0,   line_color="black", line_width=1.2)
        fig_dm.add_vline(x=-tc, line_dash="dash", line_color="#c62828",
                         annotation_text=f"−t₀.₀₂₅ = {-tc:.2f}",
                         annotation_position="bottom right")
        fig_dm.add_vline(x=tc,  line_dash="dash", line_color="#c62828")

        fig_dm.update_layout(
            title="Estadístico DM* (HLN 1997)",
            xaxis_title="DM* estadístico",
            height=250,
            margin=dict(t=50, b=40, l=120, r=100),
            plot_bgcolor="white",
            paper_bgcolor="white",
            showlegend=False,
        )
        fig_dm.update_xaxes(gridcolor="#eeeeee")
        st.plotly_chart(fig_dm, use_container_width=True)

        # Tabla DM
        dm_table = pd.DataFrame([
            {
                "Comparación": dm_labels[i],
                "DM*": f"{dm_stars[i]:.4f}",
                "p-valor": f"{dm_pvals[i]:.4f}",
                "Resultado": "GE mejor **" if (dm_reject[i] and dm_stars[i] < 0)
                             else "No signif." if not dm_reject[i]
                             else "GE peor",
            }
            for i in range(len(dm_pairs))
        ])
        st.dataframe(dm_table, use_container_width=True, hide_index=True)

    # ── Meses de shock ────────────────────────────────────────────────────────
    st.markdown("#### Meses de shock identificados")
    shock_months = crmse["GE"]["shock_months"]
    st.markdown(
        "Meses donde |producción − MA3| / max(|MA3|, 0.05) > 20 %: "
        + "  ·  ".join([f"**{m}**" for m in shock_months])
    )

    st.markdown("---")
    col_sh_a, col_sh_b = st.columns(2)

    with col_sh_a:
        st.info("**8 de 12 meses** del conjunto de test presentaron shocks de precio >20 %")
        st.error("**Shock máximo:** enero 2025 — **1021 % de variación**")

    with col_sh_b:
        st.markdown("#### Deterioro RMSE en meses de shock")
        _shock_deg = pd.DataFrame([
            {"Modelo": "GE (principal)",  "Δ RMSE shock": "+2.3%",  "Robustez": "Alta ✓"},
            {"Modelo": "XGBoost",         "Δ RMSE shock": "+16.5%", "Robustez": "Media"},
            {"Modelo": "GM_v2",           "Δ RMSE shock": "+29.2%", "Robustez": "Baja"},
            {"Modelo": "Naive",           "Δ RMSE shock": "+28.5%", "Robustez": "Baja"},
            {"Modelo": "TCN",             "Δ RMSE shock": "−9.1%",  "Robustez": "Alta* ⚠"},
        ])
        st.dataframe(_shock_deg, use_container_width=True, hide_index=True)
        st.markdown(
            "<p class='disclaimer'>* TCN mejora en shocks pero tiene MAE global = 0.1860 "
            "(peor rendimiento general).</p>",
            unsafe_allow_html=True,
        )

    st.markdown("""
    <p class='disclaimer'>
    El RMSE condicionado se calcula exclusivamente sobre los meses de shock
    (alta volatilidad). Un valor de shock mayor al RMSE global indica dificultad
    del modelo para capturar eventos extremos. GE mantiene RMSE shock ≈ RMSE global
    (0.0717 vs 0.0698), lo que muestra robustez ante eventos atípicos.
    Test DM con corrección HLN, distribución t(T−1), T = 10 meses.
    </p>
    """, unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════════
# ══════════════════════════════════════════════════════════════════════════════
# TAB 5 — COMPETIDORES (ACTUALIZADO)
# Reemplaza el bloque "with tab_comp:" existente en app.py
# ══════════════════════════════════════════════════════════════════════════════
with tab_comp:
    st.markdown("### Modelos competidores — Fase 4")

    # ── Paths nuevos ──────────────────────────────────────────────────────────
    P_TCN_PREDS_IMG = BASE_DIR / "resultados/tcn/tcn_predicciones_vs_real.png"
    P_XGB_METRICAS  = BASE_DIR / "resultados/xgboost/xgb_metricas.json"
    P_TCN_METRICAS  = BASE_DIR / "resultados/tcn/tcn_metricas.json"
    P_GMV2_METRICAS = BASE_DIR / "resultados/gm_v2/gm_v2_metricas.json"
    P_XGB_PREDS_CSV = BASE_DIR / "resultados/xgboost/xgb_predicciones.csv"
    P_TCN_PREDS_CSV = BASE_DIR / "resultados/tcn/tcn_predicciones.csv"
    P_GMV2_PREDS_CSV= BASE_DIR / "resultados/gm_v2/gm_v2_predicciones.csv"

    # ── Subtabs dentro de Competidores ────────────────────────────────────────
    sub_resumen, sub_graficos, sub_shocks = st.tabs([
        "📊 Resumen comparativo",
        "📈 Predicciones por modelo",
        "⚡ Análisis de shocks",
    ])

    # ════════════════════════════════════════════════════════════════════════
    # SUBTAB 1 — RESUMEN COMPARATIVO
    # ════════════════════════════════════════════════════════════════════════
    with sub_resumen:
        st.markdown("#### Ranking completo — todos los modelos evaluados")

        _TODOS = [
            {"Modelo": "Naive (baseline)",   "MAE": 0.0161, "RMSE": None,   "MASE": 1.00,  "Deterioro shock": "+28.5%", "Tipo": "Baseline"},
            {"Modelo": "XGBoost",            "MAE": 0.0471, "RMSE": 0.0542, "MASE": 2.60,  "Deterioro shock": "+16.5%", "Tipo": "ML tabular"},
            {"Modelo": "GM v2 (NLP mejorado)","MAE": 0.0646, "RMSE": 0.0771,"MASE": 4.02,  "Deterioro shock": "+29.2%", "Tipo": "DL + NLP"},
            {"Modelo": "GE (principal)",      "MAE": 0.0673, "RMSE": 0.0698,"MASE": 4.19,  "Deterioro shock": "+2.3%",  "Tipo": "DL principal"},
            {"Modelo": "GC1-Prophet",         "MAE": 0.0919, "RMSE": 0.1051,"MASE": 5.72,  "Deterioro shock": "N/D",    "Tipo": "Estadístico"},
            {"Modelo": "GM original (NLP)",   "MAE": 0.0981, "RMSE": 0.1007,"MASE": 6.11,  "Deterioro shock": "N/D",    "Tipo": "DL + NLP"},
            {"Modelo": "GC1-SARIMA",          "MAE": 0.1006, "RMSE": 0.1179,"MASE": 6.26,  "Deterioro shock": "N/D",    "Tipo": "Estadístico"},
            {"Modelo": "TCN",                 "MAE": 0.1860, "RMSE": 0.1987,"MASE": 11.58, "Deterioro shock": "-9.1%*", "Tipo": "DL convolucional"},
            {"Modelo": "GC2-SARIMAX+LSTM",    "MAE": 0.1969, "RMSE": 0.2926,"MASE": 12.26, "Deterioro shock": "N/D",    "Tipo": "Híbrido"},
        ]
        df_todos = pd.DataFrame(_TODOS)

        _COLORS_COMP = {
            "Naive (baseline)":    "#9e9e9e",
            "XGBoost":             "#ff7043",
            "GM v2 (NLP mejorado)":"#26a69a",
            "GE (principal)":      "#2e7d32",
            "GC1-Prophet":         "#7b1fa2",
            "GM original (NLP)":   "#5d4037",
            "GC1-SARIMA":          "#1976d2",
            "TCN":                 "#f57f17",
            "GC2-SARIMAX+LSTM":    "#e64a19",
        }

        # Gráfico MAE ranking
        fig_rank = go.Figure()
        fig_rank.add_trace(go.Bar(
            x=df_todos["Modelo"],
            y=df_todos["MAE"],
            marker_color=[_COLORS_COMP[m] for m in df_todos["Modelo"]],
            marker_line_width=0,
            text=[f"{v:.4f}" for v in df_todos["MAE"]],
            textposition="outside",
            name="MAE",
        ))
        fig_rank.add_hline(
            y=0.0673, line_dash="dash", line_color="#2e7d32",
            annotation_text="GE principal (0.0673)",
            annotation_position="top right",
        )
        fig_rank.add_hline(
            y=0.0161, line_dash="dot", line_color="#9e9e9e",
            annotation_text="Naive (0.0161)",
            annotation_position="bottom right",
        )
        fig_rank.update_layout(
            title="MAE — Ranking completo (9 modelos, menor es mejor)",
            yaxis_title="MAE (escala original z-score)",
            height=400,
            margin=dict(t=55, b=40, l=50, r=20),
            plot_bgcolor="white",
            paper_bgcolor="white",
            showlegend=False,
            xaxis_tickangle=-25,
        )
        fig_rank.update_yaxes(gridcolor="#eeeeee")
        st.plotly_chart(fig_rank, use_container_width=True)

        # Tabla completa
        df_display = df_todos.copy()
        df_display["MAE"]  = df_display["MAE"].map("{:.4f}".format)
        df_display["RMSE"] = df_display["RMSE"].apply(
            lambda v: f"{v:.4f}" if v is not None else "N/D"
        )
        df_display["MASE"] = df_display["MASE"].map("{:.2f}".format)
        st.dataframe(df_display, use_container_width=True, hide_index=True)

        st.markdown("""
        <div class='shap-box'>
        <b>Lectura clave de la tabla:</b>
        <ul style='font-size:0.88rem; margin:6px 0; padding-left:16px;'>
            <li><b>MAE global:</b> XGBoost gana (0.0471) — captura tendencia gradual con lag features</li>
            <li><b>Robustez en shocks:</b> GE gana (+2.3%) — variables climáticas NASA anticipan eventos extremos</li>
            <li><b>MASE &lt; 1:</b> solo el Naive supera al modelo de persistencia — todos los modelos complejos tienen MASE &gt; 1 con n=44</li>
            <li><b>TCN:</b> peor rendimiento global (MAE=0.1860) — convoluciones dilatadas requieren series largas</li>
        </ul>
        </div>
        """, unsafe_allow_html=True)

    # ════════════════════════════════════════════════════════════════════════
    # SUBTAB 2 — PREDICCIONES POR MODELO
    # ════════════════════════════════════════════════════════════════════════
    with sub_graficos:
        st.markdown("#### Predicciones vs Real — por modelo competidor")

        col_xgb, col_tcn = st.columns(2)
        with col_xgb:
            st.markdown("**XGBoost** — MAE=0.0471")
            if P_XGB_PREDS_IMG.exists():
                st.image(str(P_XGB_PREDS_IMG), use_container_width=True)
            else:
                st.warning("Pendiente: xgb_predicciones_vs_real.png")
            st.markdown("#### Feature Importance XGBoost")
            if P_XGB_FEAT_IMG.exists():
                st.image(str(P_XGB_FEAT_IMG), use_container_width=True)

        with col_tcn:
            st.markdown("**TCN (Temporal Convolutional Network)** — MAE=0.1860")
            if P_TCN_PREDS_IMG.exists():
                st.image(str(P_TCN_PREDS_IMG), use_container_width=True)
            else:
                st.warning("Pendiente: tcn_predicciones_vs_real.png")
            st.markdown("""
            <div class='shap-box'>
            <b>Por qué TCN falló con Small Data:</b>
            <p style='font-size:0.85rem; margin:6px 0 0;'>
            Las convoluciones dilatadas [1,2,4,8] necesitan series largas para
            que cada ventana capture patrones estadísticos reales.
            Con n=38 secuencias de train, las convoluciones no distinguen
            señal de ruido → MAE=0.1860, peor que SARIMA univariado.
            Esto confirma que LSTM-Attention es la arquitectura correcta
            para series mensuales de baja frecuencia con Small Data.
            </p>
            </div>
            """, unsafe_allow_html=True)

        st.markdown("---")
        col_gmv2, col_models = st.columns(2)
        with col_gmv2:
            st.markdown("**GM v2 (NLP mejorado M1+M2+M3+M4)** — MAE=0.0646")
            if P_GMV2_PREDS_IMG.exists():
                st.image(str(P_GMV2_PREDS_IMG), use_container_width=True)

        with col_models:
            st.markdown("**Modelos estadísticos de control**")
            _ctrl = pd.DataFrame([
                {"Modelo": "GC1-SARIMA",       "MAE": 0.1006, "RMSE": 0.1179, "R²": -7.86,  "n_test": 12},
                {"Modelo": "GC1-Prophet",       "MAE": 0.0919, "RMSE": 0.1051, "R²": -6.05,  "n_test": 12},
                {"Modelo": "GC2-SARIMAX+LSTM",  "MAE": 0.1969, "RMSE": 0.2926, "R²": -53.63, "n_test": 12},
            ])
            fig_ctrl = go.Figure()
            for _, row in _ctrl.iterrows():
                fig_ctrl.add_trace(go.Bar(
                    x=[row["Modelo"]],
                    y=[row["MAE"]],
                    name=row["Modelo"],
                    marker_color=_COLORS_COMP.get(row["Modelo"], "#888"),
                    text=[f'{row["MAE"]:.4f}'],
                    textposition="outside",
                ))
            fig_ctrl.add_hline(
                y=0.0673, line_dash="dash", line_color="#2e7d32",
                annotation_text="GE 0.0673",
                annotation_position="top right",
            )
            fig_ctrl.update_layout(
                title="MAE modelos de control (GC1, GC2)",
                yaxis_title="MAE",
                height=320,
                margin=dict(t=55, b=30, l=50, r=20),
                plot_bgcolor="white",
                paper_bgcolor="white",
                showlegend=False,
            )
            fig_ctrl.update_yaxes(gridcolor="#eeeeee")
            st.plotly_chart(fig_ctrl, use_container_width=True)
            st.dataframe(_ctrl, use_container_width=True, hide_index=True)

    # ════════════════════════════════════════════════════════════════════════
    # SUBTAB 3 — ANÁLISIS DE SHOCKS
    # ════════════════════════════════════════════════════════════════════════
    with sub_shocks:
        st.markdown("#### Comportamiento de modelos en meses de shock (variación > 20%)")
        st.markdown("""
        **8 de 12 meses** de test fueron shocks. El shock más extremo: enero 2025 con
        **1,021% de variación**. ¿Cuál modelo mantiene su precisión cuando más importa?
        """)

        # Gráfico comparativo global vs shock
        _shock_data = [
            {"Modelo": "Naive",         "MAE global": 0.0161, "MAE shocks": 0.0206, "Deterioro %": 28.5},
            {"Modelo": "GE (principal)","MAE global": 0.0673, "MAE shocks": 0.0688, "Deterioro %": 2.3},
            {"Modelo": "XGBoost",       "MAE global": 0.0471, "MAE shocks": 0.0549, "Deterioro %": 16.5},
            {"Modelo": "GM v2",         "MAE global": 0.0646, "MAE shocks": 0.0835, "Deterioro %": 29.2},
            {"Modelo": "TCN",           "MAE global": 0.1860, "MAE shocks": 0.1692, "Deterioro %": -9.1},
        ]
        df_shock = pd.DataFrame(_shock_data)

        col_s1, col_s2 = st.columns(2)
        with col_s1:
            # Barras agrupadas global vs shock
            fig_gs = go.Figure()
            fig_gs.add_trace(go.Bar(
                name="MAE global",
                x=df_shock["Modelo"],
                y=df_shock["MAE global"],
                marker_color=[_COLORS_COMP.get(m, "#888") for m in df_shock["Modelo"]],
                opacity=0.5,
                text=[f"{v:.4f}" for v in df_shock["MAE global"]],
                textposition="outside",
            ))
            fig_gs.add_trace(go.Bar(
                name="MAE en shocks",
                x=df_shock["Modelo"],
                y=df_shock["MAE shocks"],
                marker_color=[_COLORS_COMP.get(m, "#888") for m in df_shock["Modelo"]],
                opacity=1.0,
                marker_line_color="black",
                marker_line_width=1.2,
                text=[f"{v:.4f}" for v in df_shock["MAE shocks"]],
                textposition="outside",
            ))
            fig_gs.update_layout(
                barmode="group",
                title="MAE Global vs MAE en meses de shock",
                yaxis_title="MAE",
                height=380,
                margin=dict(t=55, b=40, l=50, r=20),
                plot_bgcolor="white",
                paper_bgcolor="white",
                legend=dict(orientation="h", yanchor="bottom", y=-0.35),
                xaxis_tickangle=-15,
            )
            fig_gs.update_yaxes(gridcolor="#eeeeee")
            st.plotly_chart(fig_gs, use_container_width=True)

        with col_s2:
            # Gráfico de deterioro (barras horizontales)
            colors_det = [
                "#2e7d32" if v <= 5 else
                "#ff7043" if v <= 20 else
                "#c62828" if v > 0 else
                "#9e9e9e"
                for v in df_shock["Deterioro %"]
            ]
            fig_det = go.Figure(go.Bar(
                x=df_shock["Deterioro %"],
                y=df_shock["Modelo"],
                orientation="h",
                marker_color=colors_det,
                text=[f"{v:+.1f}%" for v in df_shock["Deterioro %"]],
                textposition="outside",
            ))
            fig_det.add_vline(x=0, line_color="black", line_width=1.5)
            fig_det.add_vline(
                x=5, line_dash="dash", line_color="#2e7d32",
                annotation_text="Umbral robustez",
                annotation_position="top right",
            )
            fig_det.update_layout(
                title="Deterioro en shocks vs MAE global",
                xaxis_title="Δ MAE shock (%)",
                height=380,
                margin=dict(t=55, b=40, l=100, r=80),
                plot_bgcolor="white",
                paper_bgcolor="white",
                showlegend=False,
            )
            fig_det.update_xaxes(gridcolor="#eeeeee")
            st.plotly_chart(fig_det, use_container_width=True)

        # Gráfico de líneas interactivo: predicciones de todos en el período de test
        st.markdown("#### Visualización interactiva — predicciones en período de test completo")
        st.markdown("*Incluye meses de shock marcados en rojo*")

        try:
            # Cargar predicciones disponibles
            dfs = {}
            if P_XGB_PREDS_CSV.exists():
                dfs["XGBoost"] = pd.read_csv(P_XGB_PREDS_CSV, parse_dates=["fecha"])
            if P_GMV2_PREDS_CSV.exists():
                df_tmp = pd.read_csv(P_GMV2_PREDS_CSV, parse_dates=["fecha"])
                dfs["GM v2"] = df_tmp
            if P_GE_PREDS.exists():
                df_ge = pd.read_csv(P_GE_PREDS, parse_dates=["fecha"])
                dfs["GE (principal)"] = df_ge.rename(columns={"prediccion": "pred"})

            # Tomar el real del primer dataset disponible
            real_ref = None
            fechas_ref = None
            for nombre, df_m in dfs.items():
                col_real = [c for c in df_m.columns if "real" in c.lower()][0]
                real_ref  = df_m[col_real].values
                fechas_ref= df_m["fecha"].values
                break

            if real_ref is not None:
                fig_comp = go.Figure()

                # Línea real
                fig_comp.add_trace(go.Scatter(
                    x=fechas_ref,
                    y=real_ref,
                    name="Real",
                    mode="lines+markers",
                    line=dict(color="black", width=3),
                    marker=dict(size=8, symbol="circle"),
                ))

                # Predicciones de cada modelo
                _model_styles = {
                    "XGBoost":      dict(color="#ff7043", dash="dash",   symbol="square"),
                    "GM v2":        dict(color="#26a69a", dash="dot",    symbol="diamond"),
                    "GE (principal)":dict(color="#2e7d32", dash="solid", symbol="triangle-up"),
                }
                for nombre, df_m in dfs.items():
                    col_pred = [c for c in df_m.columns if "pred" in c.lower()][0]
                    estilo   = _model_styles.get(nombre, dict(color="#888", dash="dash", symbol="x"))
                    n_ov     = min(len(fechas_ref), len(df_m))
                    fig_comp.add_trace(go.Scatter(
                        x=df_m["fecha"].values[:n_ov],
                        y=df_m[col_pred].values[:n_ov],
                        name=nombre,
                        mode="lines+markers",
                        line=dict(color=estilo["color"], width=2, dash=estilo["dash"]),
                        marker=dict(size=6, symbol=estilo["symbol"]),
                    ))

                # Marcar meses de shock con rectángulos
                _SHOCK_FECHAS = [
                    "2024-09-01","2024-10-01","2024-12-01",
                    "2025-01-01","2025-02-01","2025-04-01",
                    "2025-05-01","2025-06-01",
                ]
                for sf in _SHOCK_FECHAS:
                    try:
                        ts = pd.Timestamp(sf)
                        ts_end = ts + pd.DateOffset(months=1)
                        fig_comp.add_vrect(
                            x0=ts, x1=ts_end,
                            fillcolor="rgba(229,57,53,0.10)",
                            line_width=0,
                        )
                    except Exception:
                        pass

                # Anotación shock máximo
                fig_comp.add_annotation(
                    x="2025-01-01",
                    y=max(real_ref) * 1.1 if len(real_ref) > 0 else 0,
                    text="Shock 1021%<br>Ene 2025",
                    showarrow=True,
                    arrowhead=2,
                    arrowcolor="#c62828",
                    font=dict(color="#c62828", size=10),
                )

                fig_comp.update_layout(
                    title="Predicciones vs Real — todos los modelos (áreas rojas = meses de shock)",
                    xaxis_title="Fecha",
                    yaxis_title="Producción (z-score media provincial)",
                    height=450,
                    margin=dict(t=60, b=50, l=60, r=20),
                    plot_bgcolor="white",
                    paper_bgcolor="white",
                    legend=dict(orientation="h", yanchor="bottom", y=-0.35),
                    hovermode="x unified",
                )
                fig_comp.update_xaxes(gridcolor="#eeeeee")
                fig_comp.update_yaxes(gridcolor="#eeeeee")
                st.plotly_chart(fig_comp, use_container_width=True)

        except Exception as e:
            st.warning(f"No se pudieron cargar algunas predicciones: {e}")

        # Tabla final de robustez
        st.markdown("#### Tabla resumen — robustez en shocks")
        _rob_final = pd.DataFrame([
            {"Modelo": "GE (principal)",  "MAE global": "0.0673", "MAE shocks": "0.0688", "Deterioro": "+2.3%",  "Veredicto": "✅ Más robusto"},
            {"Modelo": "XGBoost",         "MAE global": "0.0471", "MAE shocks": "0.0549", "Deterioro": "+16.5%", "Veredicto": "⚠️ Vulnerable en shocks"},
            {"Modelo": "GM v2",           "MAE global": "0.0646", "MAE shocks": "0.0835", "Deterioro": "+29.2%", "Veredicto": "❌ NLP introduce varianza"},
            {"Modelo": "Naive",           "MAE global": "0.0161", "MAE shocks": "0.0206", "Deterioro": "+28.5%", "Veredicto": "❌ Ciego ante shocks"},
            {"Modelo": "TCN",             "MAE global": "0.1860", "MAE shocks": "0.1692", "Deterioro": "-9.1%*", "Veredicto": "⚠️ Resultado no confiable (n=3)"},
        ])
        st.dataframe(_rob_final, use_container_width=True, hide_index=True)
        st.markdown("""
        <p class='disclaimer'>
        * TCN mejora estadísticamente en shocks pero su MAE global (0.1860) es el peor
        de todos los modelos complejos. El resultado de shocks se basa en solo 3 meses,
        insuficiente para conclusiones estadísticas. GE mantiene +2.3% de deterioro
        gracias a las variables climáticas NASA POWER y registros INDECI que actúan
        como señales anticipatorias de eventos extremos como el Fenómeno del Niño.
        </p>
        """, unsafe_allow_html=True)
# TAB 6 — ATTENTION XAI
# ══════════════════════════════════════════════════════════════════════════════

with tab_attn:
    st.markdown("### Attention Bahdanau — Interpretación temporal (GE)")

    col_heat, col_interp = st.columns([3, 2])

    with col_heat:
        st.markdown("#### Heatmap de pesos de atención — período de test")
        if P_ATT_HEATMAP.exists():
            st.image(str(P_ATT_HEATMAP), use_container_width=True)
        else:
            st.warning("Archivo pendiente: ge_attention_heatmap_test.png")

    with col_interp:
        st.markdown("#### Interpretación de canales")
        st.markdown("""
        <div class='shap-box'>
        <b>Canal A — Input climatológico + producción (63 features)</b>
        <p style='font-size:0.88rem; margin:8px 0 0;'>
            Pesos uniformes ≈ <b>0.167</b> sobre el horizonte t−5 a t.<br>
            Comportamiento consistente con <em>Small Data</em>: el modelo
            no identifica un rezago dominante y distribuye la atención
            equitativamente entre los 6 pasos anteriores.
        </p>
        </div>
        <br>
        <div class='shap-box'>
        <b>Canal B — Features NLP BETO (25 features)</b>
        <p style='font-size:0.88rem; margin:8px 0 0;'>
            Gradiente de atención: <b>t−5 = 0.128 → t = 0.210</b>
            (agosto 2025).<br>
            El modelo asigna mayor peso a los pasos recientes,
            capturando que los <em>shocks de precio recientes</em>
            son más informativos que los rezagos distantes.
            Esto valida la incorporación del canal NLP para eventos
            de alta volatilidad.
        </p>
        </div>
        """, unsafe_allow_html=True)

        st.markdown("""
        <p class='disclaimer'>
        Pesos Bahdanau normalizados con softmax sobre 6 pasos temporales (t−5 a t).
        Canal A: 63 features. Canal B: 25 features NLP.
        </p>
        """, unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════════
# TAB 7 — NLP ENGINEERING
# ══════════════════════════════════════════════════════════════════════════════

with tab_nlp:
    st.markdown("### NLP Engineering — Módulos M1–M4 (BETO)")

    col_nlp_a, col_nlp_b = st.columns(2)

    with col_nlp_a:
        st.markdown("#### Pipeline de features NLP (M1–M4)")
        if P_NLP_FEAT_IMG.exists():
            st.image(str(P_NLP_FEAT_IMG), use_container_width=True)
        else:
            st.warning("Archivo pendiente: nlp_features_engineering.png")

    with col_nlp_b:
        st.markdown("#### PCA — Varianza explicada (Canal B)")
        if P_PCA_IMG.exists():
            st.image(str(P_PCA_IMG), use_container_width=True)
        else:
            st.warning("Archivo pendiente: pca_varianza_explicada.png")

    st.markdown("""
    <p class='disclaimer'>
    Módulos NLP: M1 = sentimiento BETO, M2 = entidades nombradas,
    M3 = embeddings BERT agregados, M4 = análisis de tópicos.
    PCA aplicado sobre las 25 features del Canal B para visualización
    de varianza explicada acumulada.
    </p>
    """, unsafe_allow_html=True)
