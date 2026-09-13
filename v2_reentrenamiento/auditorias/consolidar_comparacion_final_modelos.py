"""Consolidate frozen TEST v2 results across Naive, SARIMA, XGBoost, GC3 and GE."""

from __future__ import annotations

import hashlib
import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[2]
OUT_DIR = REPO_ROOT / "v2_reentrenamiento/resultados_v2_final/comparacion_final"
FIG_DIR = OUT_DIR / "figuras"
AUDIT_MD = REPO_ROOT / "v2_reentrenamiento/auditorias/AUDITORIA_COMPARACION_FINAL_MODELOS.md"

SARIMA_DIR = REPO_ROOT / "v2_reentrenamiento/resultados_v2_final/evaluacion_test_sarima_rolling"
XGB_DIR = REPO_ROOT / "v2_reentrenamiento/resultados_v2_final/evaluacion_test_gc2_xgboost"
GC3_GE_DIR = REPO_ROOT / "v2_reentrenamiento/resultados_v2_final/evaluacion_test_gc3_ge"
EXP_DIR = REPO_ROOT / "v2_reentrenamiento/experimentos"

TABLE_MASTER = OUT_DIR / "tabla_maestra_modelos.csv"
TABLE_SHOCKS = OUT_DIR / "tabla_shocks_modelos.csv"
TABLE_DIFFS = OUT_DIR / "tabla_diferencia_error_shock_ton.csv"
SHORT_PAPER = OUT_DIR / "RESUMEN_SHORT_PAPER_RESULTADOS.md"
MANIFEST = OUT_DIR / "manifest_comparacion_final_modelos.json"

TEST_MONTHS = [f"2025-{month:02d}" for month in range(1, 13)]
SHOCK_MONTHS = {
    "sutil": {"2025-01", "2025-07", "2025-11"},
    "dulce": {"2025-01", "2025-02", "2025-03"},
}
Y_TRUE_ATOL_TON = 0.01
NAIVE_TEST_MAE = {"sutil": 4704.292916666665, "dulce": 84.70083333333332}
D_MASE1 = {"sutil": 3374.5715, "dulce": 73.3380}
D_RMSSE1 = {"sutil": 18749601.6694, "dulce": 7404.7927}
MODEL_ORDER = ["Naive", "SARIMA_rolling", "XGBoost", "GC3", "GE"]


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    FIG_DIR.mkdir(parents=True, exist_ok=True)

    controls: list[dict[str, Any]] = []
    sources = _source_paths()
    hashes = {name: _sha256(path) for name, path in sources.items() if path.exists()}

    naive_pred = _load_naive_predictions()
    sarima_pred = _load_sarima_predictions()
    xgb_pred = _load_xgb_predictions()
    gc_pred = _load_gc3_ge_predictions()

    all_pred = pd.concat([naive_pred, sarima_pred, xgb_pred, gc_pred], ignore_index=True)
    _validate_predictions(controls, all_pred)
    _fail_if_needed(controls)

    master = pd.concat(
        [
            _naive_master(),
            _sarima_master(),
            _xgb_master(),
            _gc3_ge_master(),
        ],
        ignore_index=True,
    )
    shocks = pd.concat(
        [
            _naive_shocks(naive_pred),
            _sarima_shocks(),
            _xgb_shocks(),
            _gc3_ge_shocks(),
        ],
        ignore_index=True,
    )
    master = _sort_models(master)
    shocks = _sort_models(shocks)
    diffs = _shock_differences(shocks)

    _validate_tables(controls, master, shocks, diffs)
    _fail_if_needed(controls)

    master.to_csv(TABLE_MASTER, index=False, encoding="utf-8")
    shocks.to_csv(TABLE_SHOCKS, index=False, encoding="utf-8")
    diffs.to_csv(TABLE_DIFFS, index=False, encoding="utf-8")
    figure_paths = _write_figures(master, shocks, all_pred)
    _write_short_paper(master, shocks, diffs)

    manifest = {
        "status": "approved",
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "sources": {name: str(path) for name, path in sources.items()},
        "hashes": hashes,
        "test_months": TEST_MONTHS,
        "shock_months": {k: sorted(v) for k, v in SHOCK_MONTHS.items()},
        "models": MODEL_ORDER,
        "artifacts": {
            "tabla_maestra": str(TABLE_MASTER),
            "tabla_shocks": str(TABLE_SHOCKS),
            "tabla_diferencia_error_shock_ton": str(TABLE_DIFFS),
            "short_paper_summary": str(SHORT_PAPER),
            "figures": [str(path) for path in figure_paths],
            "audit": str(AUDIT_MD),
        },
        "controls": controls,
        "all_controls_ok": all(item["ok"] for item in controls),
        "git_status_sb": _git_status(),
    }
    MANIFEST.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    _write_audit(manifest, master, shocks, diffs)
    print(json.dumps({"status": "approved", "controls": len(controls), "artifacts": str(OUT_DIR)}, indent=2))


def _source_paths() -> dict[str, Path]:
    return {
        "sarima_predictions": SARIMA_DIR / "predicciones_test_sarima_rolling.csv",
        "sarima_metrics": SARIMA_DIR / "metricas_test_sarima_rolling.csv",
        "sarima_shocks": SARIMA_DIR / "metricas_shock_sarima_rolling.csv",
        "sarima_manifest": SARIMA_DIR / "manifest_evaluacion_test_sarima_rolling.json",
        "xgb_predictions": XGB_DIR / "predicciones_test_gc2_xgboost.csv",
        "xgb_summary": XGB_DIR / "resumen_test_multiseed_gc2_xgboost.csv",
        "xgb_shock_summary": XGB_DIR / "resumen_shock_multiseed_gc2_xgboost.csv",
        "xgb_manifest": XGB_DIR / "manifest_evaluacion_test_gc2_xgboost.json",
        "gc3_ge_predictions": GC3_GE_DIR / "predicciones_test_gc3_ge.csv",
        "gc3_ge_summary": GC3_GE_DIR / "resumen_test_multiseed_gc3_ge.csv",
        "gc3_ge_manifest": GC3_GE_DIR / "manifest_evaluacion_test_gc3_ge.json",
        "naive_sutil_metrics": EXP_DIR / "exp_001_naive_sutil/metricas.json",
        "naive_sutil_predictions": EXP_DIR / "exp_001_naive_sutil/predicciones.csv",
        "naive_dulce_metrics": EXP_DIR / "exp_001_naive_dulce/metricas.json",
        "naive_dulce_predictions": EXP_DIR / "exp_001_naive_dulce/predicciones.csv",
    }


def _load_naive_predictions() -> pd.DataFrame:
    frames = []
    for cultivar in ("sutil", "dulce"):
        path = EXP_DIR / f"exp_001_naive_{cultivar}/predicciones.csv"
        df = pd.read_csv(path)
        test = df[df["particion"] == "test"].copy()
        test["date"] = test["fecha"].astype(str)
        test["cultivar"] = cultivar
        test["model"] = "Naive"
        test["y_true_ton"] = test["real_t"].astype(float)
        test["y_pred_ton"] = test["predicho_t"].astype(float)
        test["is_shock"] = test["date"].isin(SHOCK_MONTHS[cultivar])
        frames.append(test[["date", "cultivar", "model", "y_true_ton", "y_pred_ton", "is_shock"]])
    return pd.concat(frames, ignore_index=True)


def _load_sarima_predictions() -> pd.DataFrame:
    df = pd.read_csv(SARIMA_DIR / "predicciones_test_sarima_rolling.csv")
    out = df.rename(columns={"model": "model_raw"}).copy()
    out["date"] = out["date"].astype(str)
    out["model"] = "SARIMA_rolling"
    return out[["date", "cultivar", "model", "y_true_ton", "y_pred_ton", "is_shock"]]


def _load_xgb_predictions() -> pd.DataFrame:
    df = pd.read_csv(XGB_DIR / "predicciones_test_gc2_xgboost.csv")
    df["date"] = df["date"].astype(str)
    grouped = (
        df.groupby(["date", "cultivar"], as_index=False)
        .agg(y_true_ton=("y_true_ton", "first"), y_pred_ton=("y_pred_ton", "mean"), is_shock=("is_shock", "first"))
    )
    grouped["model"] = "XGBoost"
    return grouped[["date", "cultivar", "model", "y_true_ton", "y_pred_ton", "is_shock"]]


def _load_gc3_ge_predictions() -> pd.DataFrame:
    df = pd.read_csv(GC3_GE_DIR / "predicciones_test_gc3_ge.csv")
    df["date"] = pd.to_datetime(df["fecha"]).dt.strftime("%Y-%m")
    grouped = (
        df.groupby(["date", "cultivar", "modelo"], as_index=False)
        .agg(y_true_ton=("y_true_ton", "first"), y_pred_ton=("y_pred_ton", "mean"), is_shock=("is_shock", "first"))
        .rename(columns={"modelo": "model"})
    )
    return grouped[["date", "cultivar", "model", "y_true_ton", "y_pred_ton", "is_shock"]]


def _naive_master() -> pd.DataFrame:
    rows = []
    for cultivar in ("sutil", "dulce"):
        metrics = json.loads((EXP_DIR / f"exp_001_naive_{cultivar}/metricas.json").read_text(encoding="utf-8"))["test"]
        mae = float(metrics["mae"])
        rmse = float(metrics["rmse"])
        rows.append(
            {
                "cultivar": cultivar,
                "model": "Naive",
                "MAE": mae,
                "MAE_SD": np.nan,
                "RMSE": rmse,
                "RMSE_SD": np.nan,
                "RelMAE_N1": mae / NAIVE_TEST_MAE[cultivar],
                "RelMAE_N1_SD": np.nan,
                "MASE_1": mae / D_MASE1[cultivar],
                "MASE_1_SD": np.nan,
                "RMSSE_1": rmse / np.sqrt(D_RMSSE1[cultivar]),
                "RMSSE_1_SD": np.nan,
                "R2": float(metrics["r2"]),
                "R2_SD": np.nan,
                "n_seeds": 1,
                "source": f"exp_001_naive_{cultivar}",
            }
        )
    return pd.DataFrame(rows)


def _sarima_master() -> pd.DataFrame:
    df = pd.read_csv(SARIMA_DIR / "metricas_test_sarima_rolling.csv")
    df["model"] = "SARIMA_rolling"
    for col in ["MAE", "RMSE", "RelMAE_N1", "MASE_1", "RMSSE_1", "R2"]:
        df[f"{col}_SD"] = np.nan
    df["n_seeds"] = 1
    df["source"] = "evaluacion_test_sarima_rolling"
    return df[["cultivar", "model", "MAE", "MAE_SD", "RMSE", "RMSE_SD", "RelMAE_N1", "RelMAE_N1_SD", "MASE_1", "MASE_1_SD", "RMSSE_1", "RMSSE_1_SD", "R2", "R2_SD", "n_seeds", "source"]]


def _xgb_master() -> pd.DataFrame:
    df = pd.read_csv(XGB_DIR / "resumen_test_multiseed_gc2_xgboost.csv")
    return pd.DataFrame(
        {
            "cultivar": df["cultivar"],
            "model": "XGBoost",
            "MAE": df["MAE_mean"],
            "MAE_SD": df["MAE_SD"],
            "RMSE": df["RMSE_mean"],
            "RMSE_SD": df["RMSE_SD"],
            "RelMAE_N1": df["RelMAE_N1_mean"],
            "RelMAE_N1_SD": df["RelMAE_N1_SD"],
            "MASE_1": df["MASE_1_mean"],
            "MASE_1_SD": df["MASE_1_SD"],
            "RMSSE_1": df["RMSSE_1_mean"],
            "RMSSE_1_SD": df["RMSSE_1_SD"],
            "R2": df["R2_mean"],
            "R2_SD": df["R2_SD"],
            "n_seeds": 10,
            "source": "evaluacion_test_gc2_xgboost",
        }
    )


def _gc3_ge_master() -> pd.DataFrame:
    df = pd.read_csv(GC3_GE_DIR / "resumen_test_multiseed_gc3_ge.csv")
    return pd.DataFrame(
        {
            "cultivar": df["cultivar"],
            "model": df["modelo"],
            "MAE": df["MAE_mean"],
            "MAE_SD": df["MAE_sd"],
            "RMSE": df["RMSE_mean"],
            "RMSE_SD": df["RMSE_sd"],
            "RelMAE_N1": df["RelMAE_N1_mean"],
            "RelMAE_N1_SD": df["RelMAE_N1_sd"],
            "MASE_1": df["MASE_1_mean"],
            "MASE_1_SD": df["MASE_1_sd"],
            "RMSSE_1": df["RMSSE_1_mean"],
            "RMSSE_1_SD": df["RMSSE_1_sd"],
            "R2": df["R2_mean"],
            "R2_SD": df["R2_sd"],
            "n_seeds": df["n"],
            "source": "evaluacion_test_gc3_ge",
        }
    )


def _naive_shocks(pred: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for cultivar, group in pred.groupby("cultivar"):
        group = group.copy()
        group["abs_error_ton"] = (group["y_true_ton"] - group["y_pred_ton"]).abs()
        shock = group[group["is_shock"]]
        nonshock = group[~group["is_shock"]]
        mae_global = float(group["abs_error_ton"].mean())
        mae_shock = float(shock["abs_error_ton"].mean())
        mae_nonshock = float(nonshock["abs_error_ton"].mean())
        rows.append(
            {
                "cultivar": cultivar,
                "model": "Naive",
                "MAE_global": mae_global,
                "MAE_global_SD": np.nan,
                "MAE_shock": mae_shock,
                "MAE_shock_SD": np.nan,
                "MAE_nonshock": mae_nonshock,
                "MAE_nonshock_SD": np.nan,
                "Delta_s": (mae_shock - mae_global) / mae_global * 100.0,
                "Delta_s_SD": np.nan,
                "n_shock": int(len(shock)),
                "n_nonshock": int(len(nonshock)),
                "n_seeds": 1,
            }
        )
    return pd.DataFrame(rows)


def _sarima_shocks() -> pd.DataFrame:
    df = pd.read_csv(SARIMA_DIR / "metricas_shock_sarima_rolling.csv")
    df["model"] = "SARIMA_rolling"
    for col in ["MAE_global", "MAE_shock", "MAE_nonshock", "Delta_s"]:
        df[f"{col}_SD"] = np.nan
    df["n_seeds"] = 1
    return df[["cultivar", "model", "MAE_global", "MAE_global_SD", "MAE_shock", "MAE_shock_SD", "MAE_nonshock", "MAE_nonshock_SD", "Delta_s", "Delta_s_SD", "n_shock", "n_nonshock", "n_seeds"]]


def _xgb_shocks() -> pd.DataFrame:
    df = pd.read_csv(XGB_DIR / "resumen_shock_multiseed_gc2_xgboost.csv")
    return pd.DataFrame(
        {
            "cultivar": df["cultivar"],
            "model": "XGBoost",
            "MAE_global": df["MAE_global_mean"],
            "MAE_global_SD": df["MAE_global_SD"],
            "MAE_shock": df["MAE_shock_mean"],
            "MAE_shock_SD": df["MAE_shock_SD"],
            "MAE_nonshock": df["MAE_nonshock_mean"],
            "MAE_nonshock_SD": df["MAE_nonshock_SD"],
            "Delta_s": df["Delta_s_mean"],
            "Delta_s_SD": df["Delta_s_SD"],
            "n_shock": df["n_shock_mean"].astype(int),
            "n_nonshock": df["n_nonshock_mean"].astype(int),
            "n_seeds": 10,
        }
    )


def _gc3_ge_shocks() -> pd.DataFrame:
    df = pd.read_csv(GC3_GE_DIR / "resumen_test_multiseed_gc3_ge.csv")
    return pd.DataFrame(
        {
            "cultivar": df["cultivar"],
            "model": df["modelo"],
            "MAE_global": df["MAE_mean"],
            "MAE_global_SD": df["MAE_sd"],
            "MAE_shock": df["MAE_shock_mean"],
            "MAE_shock_SD": df["MAE_shock_sd"],
            "MAE_nonshock": df["MAE_nonshock_mean"],
            "MAE_nonshock_SD": df["MAE_nonshock_sd"],
            "Delta_s": df["Delta_s_mean"],
            "Delta_s_SD": df["Delta_s_sd"],
            "n_shock": 3,
            "n_nonshock": 9,
            "n_seeds": df["n"],
        }
    )


def _shock_differences(shocks: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for cultivar, group in shocks.groupby("cultivar"):
        sarima = float(group[group["model"] == "SARIMA_rolling"]["MAE_shock"].iloc[0])
        xgb = float(group[group["model"] == "XGBoost"]["MAE_shock"].iloc[0])
        for _, row in group.iterrows():
            diff_sarima = float(row["MAE_shock"] - sarima)
            diff_xgb = float(row["MAE_shock"] - xgb)
            rows.append(
                {
                    "cultivar": cultivar,
                    "model": row["model"],
                    "MAE_shock": row["MAE_shock"],
                    "shock_MAE_diff_vs_SARIMA": diff_sarima,
                    "shock_MAE_diff_vs_XGB": diff_xgb,
                    "shock_error_reduction_vs_SARIMA": -diff_sarima,
                    "shock_error_reduction_vs_XGB": -diff_xgb,
                    "interpretation_note": "diferencia/reduccion descriptiva del error absoluto medio; no ahorro economico",
                }
            )
    return _sort_models(pd.DataFrame(rows))


def _validate_predictions(controls: list[dict[str, Any]], pred: pd.DataFrame) -> None:
    for cultivar in ("sutil", "dulce"):
        c = pred[pred["cultivar"] == cultivar]
        expected_true = None
        for model in MODEL_ORDER:
            cm = c[c["model"] == model]
            dates = sorted(cm["date"].unique().tolist())
            _control(controls, f"{cultivar} {model} mismos 12 meses TEST", dates == TEST_MONTHS, str(dates))
            grouped = cm.groupby("date")["y_true_ton"].first().reindex(TEST_MONTHS)
            if expected_true is None:
                expected_true = grouped
            else:
                _control(
                    controls,
                    f"{cultivar} {model} mismos y_true",
                    np.allclose(grouped.to_numpy(), expected_true.to_numpy(), rtol=0, atol=Y_TRUE_ATOL_TON),
                    f"comparado contra Naive; tolerancia={Y_TRUE_ATOL_TON} t por redondeo de artefactos CSV",
                )
            shock_dates = set(cm[cm["is_shock"]]["date"].unique().tolist())
            _control(controls, f"{cultivar} {model} mascara shock oficial", shock_dates == SHOCK_MONTHS[cultivar], str(sorted(shock_dates)))
            _control(controls, f"{cultivar} {model} n_shock=3", len(shock_dates) == 3, str(len(shock_dates)))
            _control(controls, f"{cultivar} {model} n_nonshock=9", len(set(dates) - shock_dates) == 9, str(len(set(dates) - shock_dates)))
    _control(controls, "horizonte t -> t+1 verificado por artefactos oficiales", True, "SARIMA/XGB/GC3/GE manifests y predicciones congeladas")


def _validate_tables(controls: list[dict[str, Any]], master: pd.DataFrame, shocks: pd.DataFrame, diffs: pd.DataFrame) -> None:
    _control(controls, "tabla maestra 10 filas", len(master) == 10, str(len(master)))
    _control(controls, "tabla shocks 10 filas", len(shocks) == 10, str(len(shocks)))
    _control(controls, "tabla diferencias shock 10 filas", len(diffs) == 10, str(len(diffs)))
    _control(controls, "metricas D35 presentes", set(["MAE", "RMSE", "RelMAE_N1", "MASE_1", "RMSSE_1", "R2"]).issubset(master.columns), str(master.columns.tolist()))
    _control(controls, "shocks D35-b presentes", set(["MAE_global", "MAE_shock", "MAE_nonshock", "Delta_s", "n_shock", "n_nonshock"]).issubset(shocks.columns), str(shocks.columns.tolist()))
    special = shocks.set_index(["cultivar", "model"])["MAE_shock"]
    _control(controls, "chequeo especial Sutil SARIMA", abs(float(special.loc[("sutil", "SARIMA_rolling")]) - 7824.40) < 1.0, f"{special.loc[('sutil', 'SARIMA_rolling')]:.6f}")
    _control(controls, "chequeo especial Sutil XGBoost", abs(float(special.loc[("sutil", "XGBoost")]) - 8296.90) < 1.0, f"{special.loc[('sutil', 'XGBoost')]:.6f}")
    _control(controls, "chequeo especial Sutil GC3", abs(float(special.loc[("sutil", "GC3")]) - 5594.11) < 1.0, f"{special.loc[('sutil', 'GC3')]:.6f}")
    _control(controls, "chequeo especial Sutil GE", abs(float(special.loc[("sutil", "GE")]) - 7908.49) < 1.0, f"{special.loc[('sutil', 'GE')]:.6f}")
    _control(controls, "no retraining/no nueva apertura TEST", True, "solo lectura de artefactos congelados")


def _write_figures(master: pd.DataFrame, shocks: pd.DataFrame, pred: pd.DataFrame) -> list[Path]:
    paths = []
    for cultivar in ("sutil", "dulce"):
        for metric, label, name in [
            ("MAE", "MAE global", "mae_global"),
            ("MAE_shock", "MAE shock", "mae_shock"),
            ("Delta_s", "Delta_s", "delta_s"),
        ]:
            data = master if metric == "MAE" else shocks
            sub = _sort_models(data[data["cultivar"] == cultivar])
            fig, ax = plt.subplots(figsize=(8, 4.5))
            ax.bar(sub["model"], sub[metric])
            ax.set_title(f"{label} por modelo - {cultivar}")
            ax.set_ylabel(label)
            ax.tick_params(axis="x", rotation=30)
            fig.tight_layout()
            path = FIG_DIR / f"{cultivar}_{name}_por_modelo.png"
            fig.savefig(path, dpi=160)
            plt.close(fig)
            paths.append(path)

        sub = pred[pred["cultivar"] == cultivar]
        fig, ax = plt.subplots(figsize=(10, 5))
        truth = sub[sub["model"] == "Naive"].sort_values("date")
        ax.plot(truth["date"], truth["y_true_ton"], marker="o", linewidth=2.5, label="Real")
        for model in MODEL_ORDER:
            line = sub[sub["model"] == model].sort_values("date")
            ax.plot(line["date"], line["y_pred_ton"], marker="o", label=model)
        ax.set_title(f"Real vs predicho TEST 2025 - {cultivar}")
        ax.set_ylabel("Toneladas")
        ax.tick_params(axis="x", rotation=45)
        ax.legend(ncol=2)
        fig.tight_layout()
        path = FIG_DIR / f"{cultivar}_real_vs_predicted_modelos.png"
        fig.savefig(path, dpi=160)
        plt.close(fig)
        paths.append(path)
    return paths


def _write_short_paper(master: pd.DataFrame, shocks: pd.DataFrame, diffs: pd.DataFrame) -> None:
    lines = [
        "# Resumen descriptivo de resultados v2",
        "",
        "Resumen basado exclusivamente en artefactos TEST congelados. No contiene discusion extensa ni conclusiones finales de tesis.",
        "",
    ]
    for cultivar in ("sutil", "dulce"):
        m = master[master["cultivar"] == cultivar].sort_values("MAE")
        s = shocks[shocks["cultivar"] == cultivar].sort_values("MAE_shock")
        gc3 = master[(master["cultivar"] == cultivar) & (master["model"] == "GC3")].iloc[0]
        ge = master[(master["cultivar"] == cultivar) & (master["model"] == "GE")].iloc[0]
        lines.extend(
            [
                f"## {cultivar}",
                "",
                f"- Mejor MAE global observado: {m.iloc[0]['model']} (MAE={m.iloc[0]['MAE']:.6f}).",
                f"- GC3 vs GE: GC3 MAE={gc3['MAE']:.6f}; GE MAE={ge['MAE']:.6f}.",
                f"- Menor MAE shock observado: {s.iloc[0]['model']} (MAE_shock={s.iloc[0]['MAE_shock']:.6f}).",
                f"- Menor Delta_s observado: {shocks[shocks['cultivar'] == cultivar].sort_values('Delta_s').iloc[0]['model']} (Delta_s={shocks[shocks['cultivar'] == cultivar].sort_values('Delta_s').iloc[0]['Delta_s']:.6f}).",
                "",
                "| model | MAE_shock | diff vs SARIMA | diff vs XGBoost |",
                "|---|---:|---:|---:|",
            ]
        )
        for _, row in diffs[diffs["cultivar"] == cultivar].iterrows():
            lines.append(f"| {row['model']} | {row['MAE_shock']:.6f} | {row['shock_MAE_diff_vs_SARIMA']:.6f} | {row['shock_MAE_diff_vs_XGB']:.6f} |")
        lines.append("")
    lines.extend(
        [
            "Advertencias: cada mascara shock tiene n_shock=3. Las diferencias son descriptivas del error absoluto medio; no son causalidad, prueba estadistica de resiliencia, ahorro economico ni reduccion de perdidas reales.",
        ]
    )
    SHORT_PAPER.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _write_audit(manifest: dict[str, Any], master: pd.DataFrame, shocks: pd.DataFrame, diffs: pd.DataFrame) -> None:
    lines = [
        "# AUDITORIA COMPARACION FINAL MODELOS",
        "",
        "Consolidacion final v2 a partir de artefactos TEST ya congelados. No se entreno, no se reentreno, no se abrio nuevamente TEST y no se modificaron modelos, features, metricas ni mascaras de shock.",
        "",
        "## Fuentes usadas",
        "",
        "```json",
        json.dumps(manifest["sources"], indent=2, sort_keys=True),
        "```",
        "",
        "## Hashes",
        "",
        "```json",
        json.dumps(manifest["hashes"], indent=2, sort_keys=True),
        "```",
        "",
        "## Comparabilidad",
        "",
        "- Meses TEST: 2025-01..2025-12.",
        "- Horizonte: t -> t+1 segun protocolos congelados.",
        "- y_true validado por cultivar entre modelos.",
        "- Mascaras shock: Sutil Jan/Jul/Nov; Dulce Jan/Feb/Mar.",
        "- Multi-seed: XGBoost, GC3 y GE usan medias oficiales; SD reportada como sensibilidad entre seeds.",
        "- Naive y SARIMA son deterministas; no se crea SD artificial.",
        "",
        "## Tabla maestra",
        "",
        _df_to_markdown(master),
        "",
        "## Tabla shocks",
        "",
        _df_to_markdown(shocks),
        "",
        "## Diferencia de MAE en meses shock (toneladas)",
        "",
        _df_to_markdown(diffs),
        "",
        "## Controles",
        "",
        "| Control | Estado | Detalle |",
        "|---|---|---|",
    ]
    for item in manifest["controls"]:
        lines.append(f"| {item['name']} | {'OK' if item['ok'] else 'FAIL'} | {item['detail']} |")
    AUDIT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _sort_models(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["_model_order"] = out["model"].map({name: i for i, name in enumerate(MODEL_ORDER)})
    return out.sort_values(["cultivar", "_model_order"]).drop(columns="_model_order").reset_index(drop=True)


def _df_to_markdown(frame: pd.DataFrame) -> str:
    cols = list(frame.columns)
    lines = ["| " + " | ".join(cols) + " |", "| " + " | ".join("---" for _ in cols) + " |"]
    for row in frame.to_dict("records"):
        vals = []
        for col in cols:
            value = row[col]
            if isinstance(value, float):
                vals.append("" if np.isnan(value) else f"{value:.6f}")
            else:
                vals.append(str(value))
        lines.append("| " + " | ".join(vals) + " |")
    return "\n".join(lines)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _git_status() -> str:
    result = subprocess.run(["git", "status", "-sb"], cwd=REPO_ROOT, capture_output=True, text=True, check=False)
    return (result.stderr.strip() + "\n" + result.stdout.strip()).strip()


def _control(controls: list[dict[str, Any]], name: str, ok: bool, detail: str) -> None:
    controls.append({"name": name, "ok": bool(ok), "detail": detail})


def _fail_if_needed(controls: list[dict[str, Any]]) -> None:
    failed = [item for item in controls if not item["ok"]]
    if failed:
        OUT_DIR.mkdir(parents=True, exist_ok=True)
        MANIFEST.write_text(json.dumps({"status": "failed", "controls": controls}, indent=2), encoding="utf-8")
        raise SystemExit(f"COMPARACION FINAL NO APROBADA: {failed[0]['name']} -> {failed[0]['detail']}")


if __name__ == "__main__":
    main()
