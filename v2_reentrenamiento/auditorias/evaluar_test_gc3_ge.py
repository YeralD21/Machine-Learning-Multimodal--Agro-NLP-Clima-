"""Final TEST 2025 evaluation for official GC3/GE v2 runs.

This script opens TEST exactly for final evaluation. It never trains, refits
scalers, changes features, changes hyperparameters, selects a seed, or uses
pilot artifacts.
"""

from __future__ import annotations

import csv
import hashlib
import json
import math
import platform
import subprocess
import sys
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import numpy as np
import pandas as pd

from v2_reentrenamiento.src.evaluation.metrics_d35 import (
    conditional_shock_metrics,
    mae,
    mase1,
    relmae_n1,
    rmse,
    rmsse1,
    r2_score_d35,
)
from v2_reentrenamiento.src.evaluation.shocks_d35 import (
    OFFICIAL_TEST_SHOCKS_2025,
    P75_TRAIN_ONLY,
    shock_mask,
)
from v2_reentrenamiento.src.models.dual_lstm_attention import (
    BahdanauAttention,
    build_gc3_ge_model,
)
from v2_reentrenamiento.src.models.features_gc3_ge import build_feature_spec
from v2_reentrenamiento.src.training.metadata_gc3_ge import sha256_file
from v2_reentrenamiento.src.training.sequences_gc3_ge import build_sequences, load_feature_dataframe


CULTIVARS = ("sutil", "dulce")
MODELS = ("GC3", "GE")
SEEDS = tuple(range(10))
EXPECTED_PARAMS = {"GC3": 6273, "GE": 6657}
EXPECTED_B = {"GC3": 33, "GE": 39}
D_MASE1 = {"sutil": 3374.5715, "dulce": 73.3380}
D_RMSSE1 = {"sutil": 18749601.6694, "dulce": 7404.7927}

OFFICIAL_DIR = REPO_ROOT / "v2_reentrenamiento/resultados_v2_final/official_gc3_ge"
OUTPUT_DIR = REPO_ROOT / "v2_reentrenamiento/resultados_v2_final/evaluacion_test_gc3_ge"
FIGURES_DIR = OUTPUT_DIR / "figures"
PRED_RUN_DIR = OUTPUT_DIR / "predicciones_por_corrida"
AUDIT_PATH = REPO_ROOT / "v2_reentrenamiento/auditorias/AUDITORIA_EVALUACION_TEST_GC3_GE.md"


def ensure_output_dirs() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    FIGURES_DIR.mkdir(parents=True, exist_ok=True)
    PRED_RUN_DIR.mkdir(parents=True, exist_ok=True)


def git_status() -> str:
    result = subprocess.run(
        ["git", "status", "-sb"],
        cwd=REPO_ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    text = result.stdout.strip()
    if result.stderr.strip():
        text += "\n" + result.stderr.strip()
    return text


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def scaler_params(cultivar: str) -> dict[str, float]:
    path = REPO_ROOT / f"v2_reentrenamiento/resultados_v2_final/scalers/scaler_{cultivar}_v2c_parametros.csv"
    frame = pd.read_csv(path)
    target = f"produccion_t_{cultivar}"
    row = frame.loc[frame["feature"] == target]
    if len(row) != 1:
        raise ValueError(f"Target scaler params not found for {cultivar}.")
    item = row.iloc[0]
    return {
        "mean_train": float(item["mean_train"]),
        "scale_train": float(item["scale_train"]),
        "var_train": float(item["var_train"]),
        "params_path": str(path),
        "params_hash": sha256_file(path),
    }


def inverse_target(values, params: dict[str, float]) -> np.ndarray:
    arr = np.asarray(values, dtype=float)
    return arr * params["scale_train"] + params["mean_train"]


def load_raw_features(cultivar: str) -> pd.DataFrame:
    path = REPO_ROOT / f"v2_reentrenamiento/data/processed/master_dataset_{cultivar}_v2_features.csv"
    df = pd.read_csv(path)
    df["fecha"] = pd.to_datetime({"year": df["año"], "month": df["mes"], "day": 1})
    return df.sort_values("fecha").reset_index(drop=True)


def naive_t1_for_test(raw_df: pd.DataFrame, cultivar: str, test_dates: pd.Series) -> np.ndarray:
    target = f"produccion_t_{cultivar}"
    values = []
    for date in test_dates:
        idx = int(raw_df.index[raw_df["fecha"] == date][0])
        if idx <= 0:
            raise ValueError(f"No previous row for naive t-1 at {date}.")
        values.append(float(raw_df.loc[idx - 1, target]))
    return np.asarray(values, dtype=float)


def expected_shock_mask(cultivar: str, dates: pd.Series) -> np.ndarray:
    expected = set(OFFICIAL_TEST_SHOCKS_2025[cultivar])
    return np.asarray([date.strftime("%Y-%m") in expected for date in dates], dtype=bool)


def validate_test_alignment(cultivar: str, bundle, raw_df: pd.DataFrame, params: dict[str, float]) -> dict[str, Any]:
    dates = bundle.test.target_dates
    expected_dates = pd.date_range("2025-01-01", "2025-12-01", freq="MS")
    if list(dates) != list(expected_dates):
        raise ValueError(f"{cultivar}: TEST dates mismatch: {list(dates)}")
    if len(dates) != 12 or dates.duplicated().any():
        raise ValueError(f"{cultivar}: TEST dates must be 12 unique months.")
    reconstructed = inverse_target(bundle.test.y, params)
    raw_targets = raw_df.loc[raw_df["fecha"].isin(dates), f"produccion_t_{cultivar}"].to_numpy(float)
    max_abs_diff = float(np.max(np.abs(reconstructed - raw_targets)))
    if max_abs_diff > 1e-8:
        raise ValueError(f"{cultivar}: inverse y_true does not match raw target, max diff {max_abs_diff}.")
    jan_idx = int(raw_df.index[raw_df["fecha"] == pd.Timestamp("2025-01-01")][0])
    context_end = raw_df.loc[jan_idx - 1, "fecha"]
    if context_end != pd.Timestamp("2024-12-01"):
        raise ValueError(f"{cultivar}: first TEST context must end 2024-12, got {context_end}.")
    previous = naive_t1_for_test(raw_df, cultivar, dates)
    computed_shock = shock_mask(cultivar, previous, raw_targets)
    official_shock = expected_shock_mask(cultivar, dates)
    if not np.array_equal(computed_shock, official_shock):
        raise ValueError(f"{cultivar}: computed shock mask differs from official frozen mask.")
    if int(official_shock.sum()) != 3:
        raise ValueError(f"{cultivar}: official shock mask must have n=3.")
    return {
        "dates": [date.strftime("%Y-%m") for date in dates],
        "inverse_y_true_max_abs_diff": max_abs_diff,
        "first_test_context_end": str(context_end.date()),
        "shock_dates": [date.strftime("%Y-%m") for date, is_shock in zip(dates, official_shock) if is_shock],
    }


def compute_metrics(y_true: np.ndarray, y_pred: np.ndarray, naive_pred: np.ndarray, shock: np.ndarray, cultivar: str) -> dict[str, float | int]:
    mae_value = mae(y_true, y_pred)
    rmse_value = rmse(y_true, y_pred)
    naive_mae = mae(y_true, naive_pred)
    out = {
        "MAE": mae_value,
        "RMSE": rmse_value,
        "RelMAE_N1": relmae_n1(mae_value, naive_mae),
        "MASE_1": mase1(mae_value, D_MASE1[cultivar]),
        "RMSSE_1": rmsse1(rmse_value, D_RMSSE1[cultivar]),
        "R2": r2_score_d35(y_true, y_pred),
        "MAE_Naive_t1": naive_mae,
    }
    out.update(conditional_shock_metrics(y_true, y_pred, shock))
    return out


def summarize_group(frame: pd.DataFrame, group_cols: list[str], metric_cols: list[str]) -> pd.DataFrame:
    rows = []
    for key, group in frame.groupby(group_cols, sort=True):
        if not isinstance(key, tuple):
            key = (key,)
        row = dict(zip(group_cols, key))
        row["n"] = int(len(group))
        for metric in metric_cols:
            values = group[metric].astype(float)
            row[f"{metric}_mean"] = float(values.mean())
            row[f"{metric}_median"] = float(values.median())
            row[f"{metric}_sd"] = float(values.std(ddof=1)) if len(values) > 1 else 0.0
            row[f"{metric}_min"] = float(values.min())
            row[f"{metric}_max"] = float(values.max())
        rows.append(row)
    return pd.DataFrame(rows)


def markdown_table(frame: pd.DataFrame, floatfmt: str = ".6f") -> str:
    def fmt(value: object) -> str:
        if isinstance(value, (float, np.floating)):
            return format(float(value), floatfmt)
        if isinstance(value, (int, np.integer)):
            return str(int(value))
        return str(value)

    columns = list(frame.columns)
    lines = [
        "| " + " | ".join(columns) + " |",
        "| " + " | ".join("---" for _ in columns) + " |",
    ]
    for _, row in frame.iterrows():
        lines.append("| " + " | ".join(fmt(row[column]) for column in columns) + " |")
    return "\n".join(lines)


def make_figures(predictions: pd.DataFrame, metrics: pd.DataFrame, shock_metrics: pd.DataFrame) -> list[str]:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    paths: list[str] = []
    for cultivar in CULTIVARS:
        fig, ax = plt.subplots(figsize=(10, 5))
        subset = predictions[predictions["cultivar"] == cultivar].copy()
        y_true = subset[["fecha", "y_true_ton"]].drop_duplicates().sort_values("fecha")
        ax.plot(pd.to_datetime(y_true["fecha"]), y_true["y_true_ton"], color="black", marker="o", label="y_true")
        for model in MODELS:
            agg = (
                subset[subset["modelo"] == model]
                .groupby("fecha", as_index=False)["y_pred_ton"]
                .agg(["mean", "min", "max"])
                .reset_index()
            )
            x = pd.to_datetime(agg["fecha"])
            ax.plot(x, agg["mean"], marker="o", label=f"{model} mean pred")
            ax.fill_between(x, agg["min"], agg["max"], alpha=0.14, label=f"{model} min-max seeds")
        ax.set_title(f"{cultivar} - Real vs Predicted TEST 2025 (n=12; dispersion = seeds)")
        ax.set_xlabel("Fecha")
        ax.set_ylabel("Toneladas")
        ax.grid(alpha=0.25)
        ax.legend()
        fig.tight_layout()
        path = FIGURES_DIR / f"real_vs_pred_test_2025_{cultivar}.png"
        fig.savefig(path, dpi=150)
        plt.close(fig)
        paths.append(str(path))

    for cultivar in CULTIVARS:
        fig, ax = plt.subplots(figsize=(7, 5))
        data = [
            metrics[(metrics["cultivar"] == cultivar) & (metrics["modelo"] == model)]["MAE"].to_numpy(float)
            for model in MODELS
        ]
        ax.boxplot(data, tick_labels=MODELS)
        ax.scatter(np.repeat([1, 2], [len(d) for d in data]), np.concatenate(data), color="tab:blue", alpha=0.65)
        ax.set_title(f"{cultivar} - Distribucion multi-seed MAE TEST 2025 (n=12)")
        ax.set_ylabel("MAE toneladas")
        ax.grid(axis="y", alpha=0.25)
        fig.tight_layout()
        path = FIGURES_DIR / f"distribucion_mae_test_2025_{cultivar}.png"
        fig.savefig(path, dpi=150)
        plt.close(fig)
        paths.append(str(path))

    shock_summary = (
        shock_metrics.groupby(["cultivar", "modelo"], as_index=False)[
            ["MAE_global", "MAE_shock", "MAE_nonshock", "Delta_s"]
        ]
        .mean()
    )
    for cultivar in CULTIVARS:
        fig, axes = plt.subplots(1, 2, figsize=(12, 5))
        sub = shock_summary[shock_summary["cultivar"] == cultivar]
        x = np.arange(len(MODELS))
        width = 0.25
        for offset, metric, label in [
            (-width, "MAE_global", "MAE global"),
            (0, "MAE_shock", "MAE shock"),
            (width, "MAE_nonshock", "MAE nonshock"),
        ]:
            values = [float(sub[sub["modelo"] == model][metric].iloc[0]) for model in MODELS]
            axes[0].bar(x + offset, values, width=width, label=label)
        axes[0].set_xticks(x)
        axes[0].set_xticklabels(MODELS)
        axes[0].set_title(f"{cultivar} - MAE condicional TEST 2025 (shock n=3)")
        axes[0].set_ylabel("Toneladas")
        axes[0].legend()
        delta_values = [float(sub[sub["modelo"] == model]["Delta_s"].iloc[0]) for model in MODELS]
        axes[1].bar(MODELS, delta_values, color=["tab:orange", "tab:green"])
        axes[1].set_title(f"{cultivar} - Delta_s descriptivo TEST 2025")
        axes[1].set_ylabel("Delta_s (%)")
        for ax in axes:
            ax.grid(axis="y", alpha=0.25)
        fig.suptitle("Diagnostico de shocks: entrenamiento/validacion no se reabre; no evaluacion causal")
        fig.tight_layout()
        path = FIGURES_DIR / f"shock_diagnostico_test_2025_{cultivar}.png"
        fig.savefig(path, dpi=150)
        plt.close(fig)
        paths.append(str(path))
    return paths


def main() -> None:
    started = datetime.now(timezone.utc)
    ensure_output_dirs()

    import joblib
    import tensorflow as tf
    from tensorflow import keras

    all_predictions = []
    metrics_rows = []
    shock_rows = []
    sanity: dict[str, Any] = {}
    checkpoint_hash_before: dict[str, str] = {}
    checkpoint_hash_after: dict[str, str] = {}
    alignment: dict[str, Any] = {}
    input_hashes: dict[str, Any] = {}

    data_cache: dict[str, Any] = {}
    for cultivar in CULTIVARS:
        dataset_path = REPO_ROOT / f"v2_reentrenamiento/data/processed/master_dataset_{cultivar}_v2_escalado.csv"
        scaler_path = REPO_ROOT / f"v2_reentrenamiento/resultados_v2_final/scalers/scaler_{cultivar}_v2c.joblib"
        raw_df = load_raw_features(cultivar)
        params = scaler_params(cultivar)
        scaler = joblib.load(scaler_path)
        if scaler.__class__.__name__ != "StandardScaler" or int(scaler.n_samples_seen_) != 90:
            raise ValueError(f"{cultivar}: scaler v2c invalid.")
        input_hashes[cultivar] = {
            "dataset_path": str(dataset_path),
            "dataset_hash": sha256_file(dataset_path),
            "scaler_path": str(scaler_path),
            "scaler_hash": sha256_file(scaler_path),
            "scaler_params_hash": params["params_hash"],
        }
        data_cache[cultivar] = {"raw_df": raw_df, "params": params, "scaler_path": scaler_path}
        for model_name in MODELS:
            spec = build_feature_spec(cultivar, model_name)
            df = load_feature_dataframe(dataset_path)
            bundle = build_sequences(df, spec)
            if len(bundle.test.y) != 12:
                raise ValueError(f"{cultivar}/{model_name}: expected 12 TEST targets.")
            alignment[f"{cultivar}/{model_name}"] = validate_test_alignment(cultivar, bundle, raw_df, params)
            data_cache[(cultivar, model_name)] = {"spec": spec, "bundle": bundle}

    for cultivar in CULTIVARS:
        raw_df = data_cache[cultivar]["raw_df"]
        params = data_cache[cultivar]["params"]
        for model_name in MODELS:
            bundle = data_cache[(cultivar, model_name)]["bundle"]
            dates = bundle.test.target_dates
            y_true_scaled = bundle.test.y
            y_true_ton = inverse_target(y_true_scaled, params)
            naive_pred = naive_t1_for_test(raw_df, cultivar, dates)
            shock = expected_shock_mask(cultivar, dates)
            for seed in SEEDS:
                run_dir = OFFICIAL_DIR / cultivar / model_name / f"seed_{seed:02d}"
                if "technical_pilot" in run_dir.as_posix():
                    raise ValueError(f"Pilot path reached: {run_dir}")
                checkpoint = run_dir / "checkpoint_best.keras"
                config = read_json(run_dir / "config.json")
                metadata = read_json(run_dir / "metadata.json")
                if config.get("test_loaded_for_fit") is not False or config.get("test_used_for_training") is not False:
                    raise ValueError(f"{cultivar}/{model_name}/seed_{seed:02d}: training TEST flags invalid.")
                checkpoint_hash_before[str(checkpoint)] = file_sha256(checkpoint)
                model = keras.models.load_model(
                    checkpoint,
                    custom_objects={"BahdanauAttention": BahdanauAttention},
                    compile=False,
                )
                params_count = int(model.count_params())
                if params_count != EXPECTED_PARAMS[model_name]:
                    raise ValueError(f"{cultivar}/{model_name}/seed_{seed:02d}: params {params_count}.")
                y_pred_scaled = model.predict([bundle.test.X_a, bundle.test.X_b], verbose=0).reshape(-1)
                if len(y_pred_scaled) != 12:
                    raise ValueError(f"{cultivar}/{model_name}/seed_{seed:02d}: prediction count mismatch.")
                y_pred_ton = inverse_target(y_pred_scaled, params)
                pred_frame = pd.DataFrame(
                    {
                        "fecha": [date.strftime("%Y-%m-%d") for date in dates],
                        "cultivar": cultivar,
                        "modelo": model_name,
                        "seed": seed,
                        "y_true_scaled": y_true_scaled,
                        "y_pred_scaled": y_pred_scaled,
                        "y_true_ton": y_true_ton,
                        "y_pred_ton": y_pred_ton,
                    }
                )
                pred_frame["abs_error_ton"] = (pred_frame["y_true_ton"] - pred_frame["y_pred_ton"]).abs()
                pred_frame["squared_error_ton"] = (pred_frame["y_true_ton"] - pred_frame["y_pred_ton"]) ** 2
                pred_frame["is_shock"] = shock
                pred_frame["checkpoint_path"] = str(checkpoint)
                pred_frame["checkpoint_sha256"] = checkpoint_hash_before[str(checkpoint)]
                run_pred_dir = PRED_RUN_DIR / cultivar / model_name / f"seed_{seed:02d}"
                run_pred_dir.mkdir(parents=True, exist_ok=True)
                pred_frame.to_csv(run_pred_dir / "predicciones_test.csv", index=False)
                all_predictions.append(pred_frame)

                metrics = compute_metrics(y_true_ton, y_pred_ton, naive_pred, shock, cultivar)
                metric_row = {
                    "cultivar": cultivar,
                    "modelo": model_name,
                    "seed": seed,
                    "status": "evaluated",
                    "params": params_count,
                    "checkpoint_path": str(checkpoint),
                    "checkpoint_sha256": checkpoint_hash_before[str(checkpoint)],
                    "dataset_hash": metadata["dataset_hash"],
                    "scaler_hash": metadata["scaler_hash"],
                    **metrics,
                }
                metrics_rows.append(metric_row)
                shock_rows.append(
                    {
                        "cultivar": cultivar,
                        "modelo": model_name,
                        "seed": seed,
                        "MAE_global": metrics["MAE_global"],
                        "MAE_shock": metrics["MAE_shock"],
                        "MAE_nonshock": metrics["MAE_nonshock"],
                        "n_shock": metrics["n_shock"],
                        "n_nonshock": metrics["n_nonshock"],
                        "Delta_s": metrics["Delta_s"],
                    }
                )
                checkpoint_hash_after[str(checkpoint)] = file_sha256(checkpoint)

    predictions = pd.concat(all_predictions, ignore_index=True)
    metrics_df = pd.DataFrame(metrics_rows)
    shock_df = pd.DataFrame(shock_rows)
    metric_cols = [
        "MAE",
        "RMSE",
        "RelMAE_N1",
        "MASE_1",
        "RMSSE_1",
        "R2",
        "MAE_shock",
        "MAE_nonshock",
        "Delta_s",
    ]
    summary_df = summarize_group(metrics_df, ["cultivar", "modelo"], metric_cols)

    pair_rows = []
    for cultivar in CULTIVARS:
        for seed in SEEDS:
            gc3 = metrics_df[(metrics_df["cultivar"] == cultivar) & (metrics_df["modelo"] == "GC3") & (metrics_df["seed"] == seed)].iloc[0]
            ge = metrics_df[(metrics_df["cultivar"] == cultivar) & (metrics_df["modelo"] == "GE") & (metrics_df["seed"] == seed)].iloc[0]
            pair_rows.append(
                {
                    "cultivar": cultivar,
                    "seed": seed,
                    "MAE_GC3": gc3["MAE"],
                    "MAE_GE": ge["MAE"],
                    "delta_MAE_GE_minus_GC3": ge["MAE"] - gc3["MAE"],
                    "RMSE_GC3": gc3["RMSE"],
                    "RMSE_GE": ge["RMSE"],
                    "delta_RMSE_GE_minus_GC3": ge["RMSE"] - gc3["RMSE"],
                    "Delta_s_GC3": gc3["Delta_s"],
                    "Delta_s_GE": ge["Delta_s"],
                    "delta_Delta_s_GE_minus_GC3": ge["Delta_s"] - gc3["Delta_s"],
                    "MAE_shock_GC3": gc3["MAE_shock"],
                    "MAE_shock_GE": ge["MAE_shock"],
                    "delta_MAE_shock_GE_minus_GC3": ge["MAE_shock"] - gc3["MAE_shock"],
                }
            )
    pair_df = pd.DataFrame(pair_rows)

    predictions.to_csv(OUTPUT_DIR / "predicciones_test_gc3_ge.csv", index=False)
    metrics_df.to_csv(OUTPUT_DIR / "metricas_test_por_seed_gc3_ge.csv", index=False)
    summary_df.to_csv(OUTPUT_DIR / "resumen_test_multiseed_gc3_ge.csv", index=False)
    pair_df.to_csv(OUTPUT_DIR / "comparacion_pareada_gc3_vs_ge.csv", index=False)
    shock_df.to_csv(OUTPUT_DIR / "metricas_shock_por_seed_gc3_ge.csv", index=False)
    figure_paths = make_figures(predictions, metrics_df, shock_df)

    # Sanity checks
    sanity["40_corridas_evaluadas"] = int(len(metrics_df)) == 40
    sanity["480_predicciones_totales"] = int(len(predictions)) == 480
    sanity["12_meses_por_corrida"] = bool(predictions.groupby(["cultivar", "modelo", "seed"]).size().eq(12).all())
    sanity["jan_dec_2025"] = bool(
        predictions.groupby(["cultivar", "modelo", "seed"])["fecha"].apply(lambda s: list(s) == [d.strftime("%Y-%m-%d") for d in pd.date_range("2025-01-01", "2025-12-01", freq="MS")]).all()
    )
    sanity["10_seeds_por_cultivar_modelo"] = bool(
        metrics_df.groupby(["cultivar", "modelo"])["seed"].apply(lambda s: sorted(s.tolist()) == list(SEEDS)).all()
    )
    numeric_pred = predictions[["y_true_scaled", "y_pred_scaled", "y_true_ton", "y_pred_ton", "abs_error_ton", "squared_error_ton"]]
    numeric_metrics = metrics_df.select_dtypes(include=[np.number])
    sanity["sin_nan_inf_predicciones"] = bool(np.isfinite(numeric_pred.to_numpy(float)).all())
    sanity["sin_nan_inf_metricas"] = bool(np.isfinite(numeric_metrics.to_numpy(float)).all())
    sanity["y_true_identico_entre_seeds"] = True
    sanity["y_true_identico_gc3_ge"] = True
    for cultivar in CULTIVARS:
        refs = []
        for model_name in MODELS:
            for seed in SEEDS:
                refs.append(
                    predictions[
                        (predictions["cultivar"] == cultivar)
                        & (predictions["modelo"] == model_name)
                        & (predictions["seed"] == seed)
                    ]["y_true_ton"].to_numpy(float)
                )
        base = refs[0]
        if any(not np.allclose(base, arr, atol=1e-8) for arr in refs[1:]):
            sanity["y_true_identico_entre_seeds"] = False
            sanity["y_true_identico_gc3_ge"] = False
    sanity["n_shock_3_siempre"] = bool(shock_df["n_shock"].eq(3).all())
    sanity["n_nonshock_9_siempre"] = bool(shock_df["n_nonshock"].eq(9).all())
    sanity["params_6273_6657"] = bool(metrics_df.apply(lambda r: r["params"] == EXPECTED_PARAMS[r["modelo"]], axis=1).all())
    sanity["scaler_correcto_por_cultivar"] = True
    sanity["inverse_transform_correcto"] = bool(max(item["inverse_y_true_max_abs_diff"] for item in alignment.values()) <= 1e-8)
    sanity["denominadores_mase_rmsse_correctos"] = D_MASE1 == {"sutil": 3374.5715, "dulce": 73.3380} and D_RMSSE1 == {"sutil": 18749601.6694, "dulce": 7404.7927}
    sanity["mascara_shock_exacta"] = bool(shock_df["n_shock"].eq(3).all())
    sanity["checkpoint_oficial_correcto"] = bool(metrics_df["checkpoint_path"].str.contains("official_gc3_ge").all())
    sanity["ninguna_ruta_pilot_usada"] = bool(~metrics_df["checkpoint_path"].str.contains("technical_pilot", regex=False).any())
    sanity["ninguna_corrida_reentrenada"] = True
    sanity["ningun_checkpoint_modificado"] = checkpoint_hash_before == checkpoint_hash_after
    failed = [name for name, ok in sanity.items() if not ok]
    if failed:
        raise RuntimeError(f"Sanity checks failed: {failed}")

    finished = datetime.now(timezone.utc)
    manifest = {
        "started_utc": started.isoformat(),
        "finished_utc": finished.isoformat(),
        "duration_seconds": (finished - started).total_seconds(),
        "checkpoints_evaluated": int(len(metrics_df)),
        "predictions_total": int(len(predictions)),
        "test_opened_for_final_evaluation": True,
        "no_retraining": True,
        "no_hpo": True,
        "no_seed_selection": True,
        "d35_denominators": {
            "D_MASE1": D_MASE1,
            "D_RMSSE1": D_RMSSE1,
        },
        "shock_protocol": {
            "P75_TRAIN_ONLY": P75_TRAIN_ONLY,
            "OFFICIAL_TEST_SHOCKS_2025": OFFICIAL_TEST_SHOCKS_2025,
        },
        "alignment": alignment,
        "input_hashes": input_hashes,
        "sanity_checks": sanity,
        "figures": figure_paths,
        "environment": {
            "python": sys.version,
            "platform": platform.platform(),
            "tensorflow": tf.__version__,
            "keras": keras.__version__,
            "numpy": np.__version__,
            "pandas": pd.__version__,
        },
        "git_status": git_status(),
    }
    (OUTPUT_DIR / "manifest_evaluacion_test_gc3_ge.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    write_report(
        predictions=predictions,
        metrics_df=metrics_df,
        summary_df=summary_df,
        pair_df=pair_df,
        shock_df=shock_df,
        manifest=manifest,
    )
    print(json.dumps({"status": "ok", "output_dir": str(OUTPUT_DIR), "audit": str(AUDIT_PATH)}, indent=2))


def write_report(
    *,
    predictions: pd.DataFrame,
    metrics_df: pd.DataFrame,
    summary_df: pd.DataFrame,
    pair_df: pd.DataFrame,
    shock_df: pd.DataFrame,
    manifest: dict[str, Any],
) -> None:
    lines = [
        "# AUDITORIA EVALUACION TEST GC3/GE",
        "",
        "Apertura unica y evaluacion final de TEST 2025 para las 40 corridas oficiales GC3/GE v2.",
        "No se reentreno ningun modelo, no hubo HPO, no se modificaron arquitectura, hiperparametros, features, scalers, shocks ni metricas, y no se selecciono mejor seed.",
        "",
        "## Alcance y conteos",
        "",
        f"- Checkpoints evaluados: {manifest['checkpoints_evaluated']}",
        f"- Predicciones generadas: {manifest['predictions_total']}",
        "- Matriz: 2 cultivares x 2 modelos x 10 seeds.",
        "- Cada corrida produjo exactamente 12 predicciones TEST: enero 2025 a diciembre 2025.",
        f"- Inicio UTC: {manifest['started_utc']}",
        f"- Fin UTC: {manifest['finished_utc']}",
        f"- Duracion segundos: {manifest['duration_seconds']:.6f}",
        "",
        "## Alineacion temporal",
        "",
        "- Protocolo verificado: secuencia termina en t y predice y_(t+1).",
        "- Primera prediccion TEST 2025-01 usa contexto historico que termina en 2024-12.",
        "- y_true es identico entre seeds del mismo cultivar y entre GC3/GE dentro de cada cultivar.",
        "- Fechas TEST exactas Jan-Dec 2025, sin duplicados ni meses faltantes.",
        "",
        "## Inverse transform",
        "",
        "- y_true_scaled e y_pred_scaled se convirtieron a toneladas usando scaler v2c TRAIN-only por cultivar.",
        "- No se refitteo ni recalculo ningun scaler.",
        "- Reconstruccion de y_true contra dataset raw/features verificada con max abs diff <= 1e-8.",
        "",
        "## Metricas D35",
        "",
        "- Metricas globales calculadas en toneladas: MAE, RMSE, RelMAE_N1, MASE_1, RMSSE_1, R2.",
        "- RelMAE_N1 = MAE_modelo / MAE_Naive_t-1 sobre los mismos 12 meses TEST.",
        "- Denominadores congelados: Sutil D_MASE1=3374.5715, D_RMSSE1=18749601.6694; Dulce D_MASE1=73.3380, D_RMSSE1=7404.7927.",
        "",
        "## Shocks D35",
        "",
        "- Mascara oficial congelada usada sin redefinicion post hoc.",
        "- Sutil shock TEST: 2025-01, 2025-07, 2025-11.",
        "- Dulce shock TEST: 2025-01, 2025-02, 2025-03.",
        "- n_shock=3 y n_nonshock=9 en todas las corridas.",
        "- Delta_s se reporta como indice descriptivo de deterioro condicional ante shocks definido en este estudio; no es prueba causal ni metrica universal de resiliencia.",
        "",
        "## Resumen multi-seed TEST",
        "",
        "Valores descriptivos por cultivar/modelo, n=10 seeds. No constituyen seleccion de seed ni ranking cientifico final.",
        "",
    ]
    display_cols = [
        "cultivar",
        "modelo",
        "n",
        "MAE_mean",
        "MAE_median",
        "MAE_sd",
        "MAE_min",
        "MAE_max",
        "RMSE_mean",
        "RelMAE_N1_mean",
        "MASE_1_mean",
        "RMSSE_1_mean",
        "R2_mean",
        "MAE_shock_mean",
        "MAE_nonshock_mean",
        "Delta_s_mean",
    ]
    lines.append(markdown_table(summary_df.loc[:, display_cols], ".6f"))
    lines.extend(
        [
            "",
            "## Comparacion pareada GC3 -> GE",
            "",
            "- Tabla pareada creada por cultivar + seed.",
            "- Incluye delta_MAE_GE_minus_GC3, delta_RMSE_GE_minus_GC3, delta_Delta_s_GE_minus_GC3 y delta_MAE_shock_GE_minus_GC3.",
            "- No se ejecuto ningun test de significancia y no se descarto ningun par desfavorable.",
            "",
            "Resumen descriptivo de deltas pareados:",
            "",
        ]
    )
    delta_summary = summarize_group(
        pair_df,
        ["cultivar"],
        [
            "delta_MAE_GE_minus_GC3",
            "delta_RMSE_GE_minus_GC3",
            "delta_Delta_s_GE_minus_GC3",
            "delta_MAE_shock_GE_minus_GC3",
        ],
    )
    lines.append(markdown_table(delta_summary, ".6f"))
    lines.extend(
        [
            "",
            "## Figuras",
            "",
        ]
    )
    for path in manifest["figures"]:
        lines.append(f"- `{Path(path).relative_to(REPO_ROOT)}`")
    lines.extend(
        [
            "",
            "## Sanity checks",
            "",
            "| Check | Estado |",
            "|---|---|",
        ]
    )
    for key, value in manifest["sanity_checks"].items():
        lines.append(f"| {key} | {'OK' if value else 'FAIL'} |")
    lines.extend(
        [
            "",
            "## Archivos creados",
            "",
            f"- `{(OUTPUT_DIR / 'predicciones_test_gc3_ge.csv').relative_to(REPO_ROOT)}`",
            f"- `{(OUTPUT_DIR / 'metricas_test_por_seed_gc3_ge.csv').relative_to(REPO_ROOT)}`",
            f"- `{(OUTPUT_DIR / 'resumen_test_multiseed_gc3_ge.csv').relative_to(REPO_ROOT)}`",
            f"- `{(OUTPUT_DIR / 'comparacion_pareada_gc3_vs_ge.csv').relative_to(REPO_ROOT)}`",
            f"- `{(OUTPUT_DIR / 'metricas_shock_por_seed_gc3_ge.csv').relative_to(REPO_ROOT)}`",
            f"- `{(OUTPUT_DIR / 'manifest_evaluacion_test_gc3_ge.json').relative_to(REPO_ROOT)}`",
            f"- `{AUDIT_PATH.relative_to(REPO_ROOT)}`",
            f"- `{FIGURES_DIR.relative_to(REPO_ROOT)}/`",
            "",
            "## Incidencias",
            "",
            "- No se detectaron incidencias en los sanity checks.",
            "- Los resultados se conservan sin modificar, independientemente de su direccion o magnitud.",
            "",
            "## Git status",
            "",
            "```text",
            manifest["git_status"],
            "```",
            "",
            "NO commit.",
            "NO push.",
            "",
            "EVALUACIÓN TEST GC3/GE COMPLETADA Y CONGELADA PARA INTERPRETACIÓN",
        ]
    )
    AUDIT_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
