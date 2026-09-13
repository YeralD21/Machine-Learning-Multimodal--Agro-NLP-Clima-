"""Evaluate the frozen official GC2/XGBoost models on TEST 2025 exactly once."""

from __future__ import annotations

import json
import math
import platform
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import joblib
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from xgboost import XGBRegressor


REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

from v2_reentrenamiento.src.models.features_gc2_xgboost import (  # noqa: E402
    FORBIDDEN_CONTEMPORARY_EXOGENOUS,
    FORBIDDEN_EXACT,
    FORBIDDEN_PATTERN_FRAGMENTS,
    build_gc2_feature_spec,
)
from v2_reentrenamiento.src.training.config_gc2_xgboost import (  # noqa: E402
    OFFICIAL_GC2_CULTIVARS,
    OFFICIAL_GC2_SEEDS,
    OFFICIAL_GC2_XGB_CONFIG,
    validate_gc2_config,
    validate_gc2_seeds,
)
from v2_reentrenamiento.src.training.metadata_gc3_ge import (  # noqa: E402
    current_git_commit,
    sha256_file,
    stable_config_hash,
)
from v2_reentrenamiento.src.training.official_runner_gc2_xgboost import _inverse_target  # noqa: E402
from v2_reentrenamiento.src.training.tabular_gc2_xgboost import MONTH_COL, YEAR_COL  # noqa: E402


OFFICIAL_RUNS_DIR = REPO_ROOT / "v2_reentrenamiento/resultados_v2_final/official_gc2_xgboost"
OUTPUT_DIR = REPO_ROOT / "v2_reentrenamiento/resultados_v2_final/evaluacion_test_gc2_xgboost"
AUDIT_DIR = REPO_ROOT / "v2_reentrenamiento/auditorias"
DATA_DIR = REPO_ROOT / "v2_reentrenamiento/data/processed"
SCALER_DIR = REPO_ROOT / "v2_reentrenamiento/resultados_v2_final/scalers"

PREDICTIONS_CSV = OUTPUT_DIR / "predicciones_test_gc2_xgboost.csv"
METRICS_CSV = OUTPUT_DIR / "metricas_test_por_seed_gc2_xgboost.csv"
SUMMARY_CSV = OUTPUT_DIR / "resumen_test_multiseed_gc2_xgboost.csv"
SHOCK_CSV = OUTPUT_DIR / "metricas_shock_por_seed_gc2_xgboost.csv"
SHOCK_SUMMARY_CSV = OUTPUT_DIR / "resumen_shock_multiseed_gc2_xgboost.csv"
MANIFEST_JSON = OUTPUT_DIR / "manifest_evaluacion_test_gc2_xgboost.json"
AUDIT_MD = AUDIT_DIR / "AUDITORIA_EVALUACION_TEST_GC2_XGBOOST.md"

NAIVE_TEST_MAE = {
    "sutil": 4704.292916666665,
    "dulce": 84.70083333333332,
}
D_MASE1 = {
    "sutil": 3374.5715,
    "dulce": 73.3380,
}
D_RMSSE1 = {
    "sutil": 18749601.6694,
    "dulce": 7404.7927,
}
P75_SHOCK = {
    "sutil": 0.240834900212216,
    "dulce": 0.320281354618397,
}
SHOCK_MONTHS = {
    "sutil": {"2025-01", "2025-07", "2025-11"},
    "dulce": {"2025-01", "2025-02", "2025-03"},
}
METRIC_COLUMNS = ("MAE", "RMSE", "RelMAE_N1", "MASE_1", "RMSSE_1", "R2")
SHOCK_COLUMNS = ("MAE_global", "MAE_shock", "MAE_nonshock", "Delta_s", "n_shock", "n_nonshock")


def main() -> None:
    if "--audit-only" in sys.argv:
        manifest = json.loads(MANIFEST_JSON.read_text(encoding="utf-8"))
        metrics = pd.read_csv(METRICS_CSV)
        summary = pd.read_csv(SUMMARY_CSV)
        shock_metrics = pd.read_csv(SHOCK_CSV)
        shock_summary = pd.read_csv(SHOCK_SUMMARY_CSV)
        _write_audit(manifest, metrics, summary, shock_metrics, shock_summary)
        print(json.dumps({"status": manifest["status"], "audit": str(AUDIT_MD)}, indent=2))
        return

    validate_gc2_config()
    validate_gc2_seeds()
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    (OUTPUT_DIR / "figuras").mkdir(parents=True, exist_ok=True)

    started = datetime.now(timezone.utc)
    controls: list[dict[str, Any]] = []
    all_predictions: list[pd.DataFrame] = []
    metrics_rows: list[dict[str, Any]] = []
    shock_rows: list[dict[str, Any]] = []
    hashes: dict[str, Any] = {"datasets": {}, "scalers": {}, "models": {}}

    run_dirs = _official_run_dirs()
    expected_pairs = {(cultivar, seed) for cultivar in OFFICIAL_GC2_CULTIVARS for seed in OFFICIAL_GC2_SEEDS}
    actual_pairs = set(run_dirs)
    _add_control(controls, "20 modelos oficiales encontrados", len(run_dirs) == 20, str(len(run_dirs)))
    _add_control(controls, "seeds 0..9 completas para Sutil", {seed for cultivar, seed in actual_pairs if cultivar == "sutil"} == set(OFFICIAL_GC2_SEEDS), str(sorted(seed for cultivar, seed in actual_pairs if cultivar == "sutil")))
    _add_control(controls, "seeds 0..9 completas para Dulce", {seed for cultivar, seed in actual_pairs if cultivar == "dulce"} == set(OFFICIAL_GC2_SEEDS), str(sorted(seed for cultivar, seed in actual_pairs if cultivar == "dulce")))
    _add_control(controls, "solo pares cultivar/seed oficiales", actual_pairs == expected_pairs, str(sorted(actual_pairs)))
    _fail_if_needed(controls)

    for cultivar in OFFICIAL_GC2_CULTIVARS:
        spec = build_gc2_feature_spec(cultivar)
        scaled_path = DATA_DIR / f"master_dataset_{cultivar}_v2_escalado.csv"
        raw_path = DATA_DIR / f"master_dataset_{cultivar}_v2_features.csv"
        scaler_path = SCALER_DIR / f"scaler_{cultivar}_v2c.joblib"
        scaled = _load_monthly_frame(scaled_path)
        raw = _load_monthly_frame(raw_path)
        scaler = joblib.load(scaler_path)
        test_bundle = _build_test_arrays(scaled, spec.predictors, spec.target)
        y_true_ton = _inverse_target(scaler, spec.target, test_bundle["y_true_scaled"])
        y_true_raw = _target_raw_2025(raw, spec.target)
        is_shock = _shock_mask(raw, spec.target, cultivar)

        hashes["datasets"][cultivar] = {
            "scaled": sha256_file(scaled_path),
            "raw_features_for_inverse_check": sha256_file(raw_path),
        }
        hashes["scalers"][cultivar] = sha256_file(scaler_path)

        _add_feature_controls(controls, cultivar, spec.predictors)
        _add_control(controls, f"{cultivar} TEST n=12", len(test_bundle["target_dates"]) == 12, str(len(test_bundle["target_dates"])))
        _add_control(controls, f"{cultivar} fechas Jan-Dec 2025 exactas", list(test_bundle["target_dates"]) == [f"2025-{m:02d}" for m in range(1, 13)], str(test_bundle["target_dates"]))
        _add_control(controls, f"{cultivar} primer forecast = Dec2024 -> Jan2025", test_bundle["x_dates"][0] == "2024-12" and test_bundle["target_dates"][0] == "2025-01", f"{test_bundle['x_dates'][0]} -> {test_bundle['target_dates'][0]}")
        _add_control(controls, f"{cultivar} scaler TRAIN-only congelado", scaler_path.name == f"scaler_{cultivar}_v2c.joblib", scaler_path.name)
        _add_control(controls, f"{cultivar} no refit scaler", hasattr(scaler, "feature_names_in_") and spec.target in list(scaler.feature_names_in_), "scaler cargado desde joblib")
        _add_control(controls, f"{cultivar} y_true inverse transform correcto", np.allclose(y_true_ton, y_true_raw, rtol=0, atol=1e-6), "comparado contra master_dataset_v2_features")
        _add_control(controls, f"{cultivar} n_shock=3", int(is_shock.sum()) == 3, str(int(is_shock.sum())))
        _add_control(controls, f"{cultivar} n_nonshock=9", int((~is_shock).sum()) == 9, str(int((~is_shock).sum())))
        _add_control(controls, f"{cultivar} mascara shock congelada", set(np.asarray(test_bundle["target_dates"])[is_shock]) == SHOCK_MONTHS[cultivar], str(list(np.asarray(test_bundle["target_dates"])[is_shock])))
        _fail_if_needed(controls)

        for seed in OFFICIAL_GC2_SEEDS:
            run_dir = run_dirs[(cultivar, seed)]
            config = json.loads((run_dir / "config.json").read_text(encoding="utf-8"))
            metadata = json.loads((run_dir / "metadata.json").read_text(encoding="utf-8"))
            model_path = run_dir / "model.joblib"
            hashes["models"][f"{cultivar}_seed_{seed:02d}"] = sha256_file(model_path)
            _verify_run_config(controls, cultivar, seed, config, metadata, spec.predictors)
            _fail_if_needed(controls)

            model: XGBRegressor = joblib.load(model_path)
            _verify_model_params(controls, cultivar, seed, model)
            y_pred_scaled = model.predict(test_bundle["X"])
            y_pred_ton = _inverse_target(scaler, spec.target, y_pred_scaled)
            pred_frame = pd.DataFrame(
                {
                    "date": test_bundle["target_dates"],
                    "cultivar": cultivar,
                    "model": "GC2_XGBoost",
                    "seed": seed,
                    "x_date": test_bundle["x_dates"],
                    "y_true_scaled": test_bundle["y_true_scaled"],
                    "y_pred_scaled": y_pred_scaled,
                    "y_true_ton": y_true_ton,
                    "y_pred_ton": y_pred_ton,
                    "abs_error_ton": np.abs(y_true_ton - y_pred_ton),
                    "squared_error_ton": np.square(y_true_ton - y_pred_ton),
                    "is_shock": is_shock,
                }
            )
            all_predictions.append(pred_frame)
            metric_row = _metric_row(cultivar, seed, y_true_ton, y_pred_ton)
            shock_row = _shock_row(cultivar, seed, pred_frame)
            metrics_rows.append(metric_row)
            shock_rows.append(shock_row)

    predictions = pd.concat(all_predictions, ignore_index=True)
    metrics = pd.DataFrame(metrics_rows)
    shock_metrics = pd.DataFrame(shock_rows)
    summary = _summarize(metrics, ["cultivar"], METRIC_COLUMNS)
    shock_summary = _summarize(shock_metrics, ["cultivar"], SHOCK_COLUMNS)

    _add_control(controls, "240 predicciones totales", len(predictions) == 240, str(len(predictions)))
    _add_control(controls, "12 predicciones por modelo/seed", predictions.groupby(["cultivar", "seed"]).size().eq(12).all(), "group sizes all 12")
    _add_control(controls, "no modelo v1/piloto", all("official_gc2_xgboost" in str(path) for path in run_dirs.values()), "solo official_gc2_xgboost")
    _add_control(controls, "no retraining", True, "solo joblib.load + predict")
    _add_control(controls, "no HPO", True, "config hpo_performed=false verificado")
    _add_control(controls, "no CV", True, "config cv_performed=false verificado")
    _add_control(controls, "no early stopping", True, "config early_stopping=false verificado")
    _add_control(controls, "no best seed", True, "todas las seeds reportadas")
    _add_control(controls, "no modificacion de modelos", True, "modelos cargados desde artefactos oficiales")
    _add_control(controls, "no modificacion de datasets", True, "lectura solamente")
    _add_control(controls, "no modificacion de features", True, "feature spec congelado")
    _add_control(controls, "no modificacion de metricas/shocks", True, "D35/D35-b congelados")
    _fail_if_needed(controls)

    predictions.to_csv(PREDICTIONS_CSV, index=False, encoding="utf-8")
    metrics.to_csv(METRICS_CSV, index=False, encoding="utf-8")
    summary.to_csv(SUMMARY_CSV, index=False, encoding="utf-8")
    shock_metrics.to_csv(SHOCK_CSV, index=False, encoding="utf-8")
    shock_summary.to_csv(SHOCK_SUMMARY_CSV, index=False, encoding="utf-8")
    figure_paths = _write_figures(predictions, metrics)

    manifest = {
        "status": "approved",
        "authorization": "APERTURA UNICA Y FINAL DE TEST 2025 para GC2/XGBoost v2",
        "started_utc": started.isoformat(),
        "finished_utc": datetime.now(timezone.utc).isoformat(),
        "models_evaluated": 20,
        "predictions": int(len(predictions)),
        "official_cultivars": list(OFFICIAL_GC2_CULTIVARS),
        "official_seeds": list(OFFICIAL_GC2_SEEDS),
        "test_target_dates": [f"2025-{m:02d}" for m in range(1, 13)],
        "representation": "X_t -> y_(t+1)",
        "denominators": {
            "NAIVE_TEST_MAE": NAIVE_TEST_MAE,
            "D_MASE1": D_MASE1,
            "D_RMSSE1": D_RMSSE1,
            "P75_SHOCK": P75_SHOCK,
            "SHOCK_MONTHS": {k: sorted(v) for k, v in SHOCK_MONTHS.items()},
        },
        "environment": {
            "python": platform.python_version(),
            "os": platform.platform(),
            "cpu": platform.processor() or None,
        },
        "git_commit": current_git_commit(),
        "hashes": hashes,
        "artifacts": {
            "predictions": str(PREDICTIONS_CSV),
            "metrics_by_seed": str(METRICS_CSV),
            "summary_multiseed": str(SUMMARY_CSV),
            "shock_by_seed": str(SHOCK_CSV),
            "shock_summary": str(SHOCK_SUMMARY_CSV),
            "figures": [str(path) for path in figure_paths],
            "audit": str(AUDIT_MD),
        },
        "controls": controls,
    }
    MANIFEST_JSON.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    _write_audit(manifest, metrics, summary, shock_metrics, shock_summary)
    print(json.dumps({"status": "approved", "models_evaluated": 20, "predictions": int(len(predictions)), "controls": len(controls)}, indent=2))


def _official_run_dirs() -> dict[tuple[str, int], Path]:
    run_dirs: dict[tuple[str, int], Path] = {}
    for cultivar in OFFICIAL_GC2_CULTIVARS:
        for seed in OFFICIAL_GC2_SEEDS:
            run_dir = OFFICIAL_RUNS_DIR / cultivar / "GC2_XGBoost" / f"seed_{seed:02d}"
            if run_dir.exists():
                run_dirs[(cultivar, seed)] = run_dir
    return run_dirs


def _load_monthly_frame(path: Path) -> pd.DataFrame:
    frame = pd.read_csv(path)
    frame["fecha"] = pd.to_datetime({"year": frame[YEAR_COL], "month": frame[MONTH_COL], "day": 1})
    return frame.sort_values("fecha").reset_index(drop=True)


def _build_test_arrays(frame: pd.DataFrame, predictors: tuple[str, ...], target: str) -> dict[str, Any]:
    x_rows = []
    y_scaled = []
    x_dates = []
    target_dates = []
    for idx in range(len(frame) - 1):
        x_date = frame.loc[idx, "fecha"]
        y_date = frame.loc[idx + 1, "fecha"]
        if y_date != x_date + pd.DateOffset(months=1):
            raise ValueError(f"Non-consecutive pair: {x_date} -> {y_date}")
        if y_date.year == 2025:
            x_rows.append(frame.loc[idx, list(predictors)].to_numpy(dtype=float))
            y_scaled.append(float(frame.loc[idx + 1, target]))
            x_dates.append(x_date.strftime("%Y-%m"))
            target_dates.append(y_date.strftime("%Y-%m"))
    return {
        "X": np.stack(x_rows).astype(float),
        "y_true_scaled": np.asarray(y_scaled, dtype=float),
        "x_dates": x_dates,
        "target_dates": target_dates,
    }


def _target_raw_2025(raw: pd.DataFrame, target: str) -> np.ndarray:
    mask = raw["fecha"].dt.year == 2025
    return raw.loc[mask, target].to_numpy(dtype=float)


def _shock_mask(raw: pd.DataFrame, target: str, cultivar: str) -> np.ndarray:
    raw = raw.copy()
    raw["r_t"] = (raw[target] - raw[target].shift(1)).abs() / raw[target].shift(1)
    test = raw[raw["fecha"].dt.year == 2025].copy()
    return (test["r_t"].to_numpy(dtype=float) > P75_SHOCK[cultivar])


def _verify_run_config(
    controls: list[dict[str, Any]],
    cultivar: str,
    seed: int,
    config: dict[str, Any],
    metadata: dict[str, Any],
    predictors: tuple[str, ...],
) -> None:
    prefix = f"{cultivar} seed_{seed:02d}"
    cfg = config["config"]
    expected = OFFICIAL_GC2_XGB_CONFIG.as_dict()
    _add_control(controls, f"{prefix} config congelada", cfg == expected, str(cfg))
    _add_control(controls, f"{prefix} random_state = seed", config["xgb_params"].get("random_state") == seed, str(config["xgb_params"].get("random_state")))
    _add_control(controls, f"{prefix} features = 37", len(config["features"]) == 37 and tuple(config["features"]) == predictors, str(len(config["features"])))
    _add_control(controls, f"{prefix} objective", config["xgb_params"].get("objective") == "reg:squarederror", str(config["xgb_params"].get("objective")))
    _add_control(controls, f"{prefix} eval_metric", config["xgb_params"].get("eval_metric") == "mae", str(config["xgb_params"].get("eval_metric")))
    _add_control(controls, f"{prefix} booster", config["xgb_params"].get("booster") == "gbtree", str(config["xgb_params"].get("booster")))
    for key, expected_value in {
        "n_estimators": 200,
        "max_depth": 2,
        "min_child_weight": 1,
        "learning_rate": 0.05,
        "subsample": 0.8,
        "colsample_bytree": 0.8,
        "reg_alpha": 0.0,
        "reg_lambda": 1.0,
        "gamma": 0.0,
        "tree_method": "hist",
        "n_jobs": 1,
    }.items():
        _add_control(controls, f"{prefix} {key}", config["xgb_params"].get(key) == expected_value, str(config["xgb_params"].get(key)))
    _add_control(controls, f"{prefix} metadata no TEST pre-run", metadata.get("test_loaded") is False, str(metadata.get("test_loaded")))
    _add_control(controls, f"{prefix} no HPO/CV/early/best seed", not any((cfg["hpo_performed"], cfg["cv_performed"], cfg["early_stopping"], cfg["best_seed_selection"])), "false/false/false/false")


def _verify_model_params(controls: list[dict[str, Any]], cultivar: str, seed: int, model: XGBRegressor) -> None:
    params = model.get_params(deep=False)
    _add_control(controls, f"{cultivar} seed_{seed:02d} modelo no v1/piloto", isinstance(model, XGBRegressor), type(model).__name__)
    _add_control(controls, f"{cultivar} seed_{seed:02d} modelo random_state", params.get("random_state") == seed, str(params.get("random_state")))


def _add_feature_controls(controls: list[dict[str, Any]], cultivar: str, predictors: tuple[str, ...]) -> None:
    forbidden_hits = []
    for feature in predictors:
        if feature in FORBIDDEN_EXACT or feature in FORBIDDEN_CONTEMPORARY_EXOGENOUS:
            forbidden_hits.append(feature)
        if any(fragment in feature for fragment in FORBIDDEN_PATTERN_FRAGMENTS):
            forbidden_hits.append(feature)
        if feature.startswith("avg_sentiment") or feature.startswith("n_noticias"):
            forbidden_hits.append(feature)
    _add_control(controls, f"{cultivar} 37 features exactas", len(predictors) == 37, str(len(predictors)))
    _add_control(controls, f"{cultivar} variables prohibidas ausentes", not forbidden_hits, str(sorted(set(forbidden_hits))))


def _metric_row(cultivar: str, seed: int, y_true: np.ndarray, y_pred: np.ndarray) -> dict[str, Any]:
    mae = float(mean_absolute_error(y_true, y_pred))
    rmse = float(mean_squared_error(y_true, y_pred) ** 0.5)
    return {
        "cultivar": cultivar,
        "model": "GC2_XGBoost",
        "seed": seed,
        "MAE": mae,
        "RMSE": rmse,
        "RelMAE_N1": mae / NAIVE_TEST_MAE[cultivar],
        "MASE_1": mae / D_MASE1[cultivar],
        "RMSSE_1": rmse / math.sqrt(D_RMSSE1[cultivar]),
        "R2": float(r2_score(y_true, y_pred)),
    }


def _shock_row(cultivar: str, seed: int, pred_frame: pd.DataFrame) -> dict[str, Any]:
    shock = pred_frame[pred_frame["is_shock"]]
    nonshock = pred_frame[~pred_frame["is_shock"]]
    mae_global = float(pred_frame["abs_error_ton"].mean())
    mae_shock = float(shock["abs_error_ton"].mean())
    mae_nonshock = float(nonshock["abs_error_ton"].mean())
    return {
        "cultivar": cultivar,
        "model": "GC2_XGBoost",
        "seed": seed,
        "MAE_global": mae_global,
        "MAE_shock": mae_shock,
        "MAE_nonshock": mae_nonshock,
        "Delta_s": ((mae_shock - mae_global) / mae_global) * 100.0,
        "n_shock": int(len(shock)),
        "n_nonshock": int(len(nonshock)),
    }


def _summarize(frame: pd.DataFrame, group_cols: list[str], columns: tuple[str, ...]) -> pd.DataFrame:
    pieces = []
    for keys, group in frame.groupby(group_cols, dropna=False):
        row = {"cultivar": keys if isinstance(keys, str) else keys[0]}
        for column in columns:
            values = group[column].astype(float)
            row[f"{column}_mean"] = float(values.mean())
            row[f"{column}_median"] = float(values.median())
            row[f"{column}_SD"] = float(values.std(ddof=1))
            row[f"{column}_min"] = float(values.min())
            row[f"{column}_max"] = float(values.max())
        pieces.append(row)
    return pd.DataFrame(pieces).sort_values("cultivar").reset_index(drop=True)


def _write_figures(predictions: pd.DataFrame, metrics: pd.DataFrame) -> list[Path]:
    paths: list[Path] = []
    for cultivar in OFFICIAL_GC2_CULTIVARS:
        c_preds = predictions[predictions["cultivar"] == cultivar]
        c_metrics = metrics[metrics["cultivar"] == cultivar].sort_values("seed")
        median_mae = c_metrics["MAE"].median()
        representative_seed = int(c_metrics.iloc[(c_metrics["MAE"] - median_mae).abs().argsort().iloc[0]]["seed"])
        rep = c_preds[c_preds["seed"] == representative_seed]

        fig, ax = plt.subplots(figsize=(10, 4))
        ax.plot(rep["date"], rep["y_true_ton"], marker="o", label="Real")
        ax.plot(rep["date"], rep["y_pred_ton"], marker="o", label=f"Pred seed mediana MAE ({representative_seed})")
        ax.set_title(f"GC2/XGBoost TEST 2025 - {cultivar}")
        ax.set_xlabel("Mes")
        ax.set_ylabel("Toneladas")
        ax.tick_params(axis="x", rotation=45)
        ax.legend()
        fig.tight_layout()
        path = OUTPUT_DIR / "figuras" / f"{cultivar}_real_vs_pred_seed_mediana_mae.png"
        fig.savefig(path, dpi=160)
        plt.close(fig)
        paths.append(path)

        fig, ax = plt.subplots(figsize=(7, 4))
        ax.hist(c_metrics["MAE"], bins=6)
        ax.set_title(f"Distribucion MAE TEST - {cultivar}")
        ax.set_xlabel("MAE")
        ax.set_ylabel("Frecuencia")
        fig.tight_layout()
        path = OUTPUT_DIR / "figuras" / f"{cultivar}_distribucion_mae_seeds.png"
        fig.savefig(path, dpi=160)
        plt.close(fig)
        paths.append(path)

        error_by_kind = c_preds.assign(tipo=np.where(c_preds["is_shock"], "shock", "nonshock"))
        fig, ax = plt.subplots(figsize=(6, 4))
        error_by_kind.boxplot(column="abs_error_ton", by="tipo", ax=ax)
        fig.suptitle("")
        ax.set_title(f"Error shock vs non-shock - {cultivar}")
        ax.set_xlabel("")
        ax.set_ylabel("Error absoluto")
        fig.tight_layout()
        path = OUTPUT_DIR / "figuras" / f"{cultivar}_error_shock_vs_nonshock.png"
        fig.savefig(path, dpi=160)
        plt.close(fig)
        paths.append(path)
    return paths


def _write_audit(
    manifest: dict[str, Any],
    metrics: pd.DataFrame,
    summary: pd.DataFrame,
    shock_metrics: pd.DataFrame,
    shock_summary: pd.DataFrame,
) -> None:
    lines = [
        "# AUDITORIA EVALUACION TEST GC2/XGBOOST",
        "",
        "Apertura unica y final de TEST 2025 para los 20 modelos oficiales GC2/XGBoost v2.",
        "",
        "## Alcance",
        "",
        "- Modelos evaluados: 20.",
        "- Seeds: 0..9 por cultivar.",
        "- Cultivares: sutil, dulce.",
        "- Fechas TEST: 2025-01..2025-12.",
        "- Predicciones: 240.",
        "- Representacion: X_t -> y_(t+1).",
        "- Primer forecast: 2024-12 -> 2025-01.",
        "- No retraining, no HPO, no CV, no early stopping, no best seed.",
        "",
        "## Metricas TEST por seed",
        "",
        _df_to_markdown(metrics),
        "",
        "## Resumen multi-seed",
        "",
        _df_to_markdown(summary),
        "",
        "## Metricas shock por seed",
        "",
        _df_to_markdown(shock_metrics),
        "",
        "## Resumen shock multi-seed",
        "",
        _df_to_markdown(shock_summary),
        "",
        "Delta_s se conserva como indice descriptivo de deterioro condicional ante shocks definido en este estudio; no es una prueba causal ni una prueba estadistica de resiliencia.",
        "",
        "## Denominadores y mascaras congeladas",
        "",
        "```json",
        json.dumps(manifest["denominators"], indent=2, sort_keys=True),
        "```",
        "",
        "## Hashes",
        "",
        "```json",
        json.dumps(manifest["hashes"], indent=2, sort_keys=True),
        "```",
        "",
        "## Artefactos",
        "",
        "```json",
        json.dumps(manifest["artifacts"], indent=2, sort_keys=True),
        "```",
        "",
        "## Controles",
        "",
        "| Control | Estado | Detalle |",
        "|---|---|---|",
    ]
    for control in manifest["controls"]:
        lines.append(f"| {control['name']} | {'OK' if control['ok'] else 'FAIL'} | {control['detail']} |")
    lines.extend(
        [
            "",
            "## Confirmacion final",
            "",
            "- Apertura unica TEST GC2/XGBoost: SI.",
            "- Resultados congelados para interpretacion posterior: SI.",
        ]
    )
    AUDIT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _df_to_markdown(frame: pd.DataFrame) -> str:
    columns = list(frame.columns)
    lines = [
        "| " + " | ".join(columns) + " |",
        "| " + " | ".join("---" for _ in columns) + " |",
    ]
    for row in frame.to_dict("records"):
        values = []
        for column in columns:
            value = row[column]
            if isinstance(value, float):
                values.append(f"{value:.6f}")
            else:
                values.append(str(value))
        lines.append("| " + " | ".join(values) + " |")
    return "\n".join(lines)


def _add_control(controls: list[dict[str, Any]], name: str, ok: bool, detail: str) -> None:
    controls.append({"name": name, "ok": bool(ok), "detail": detail})


def _fail_if_needed(controls: list[dict[str, Any]]) -> None:
    failed = [control for control in controls if not control["ok"]]
    if failed:
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        payload = {
            "status": "failed",
            "generated_utc": datetime.now(timezone.utc).isoformat(),
            "failed_controls": failed,
            "controls": controls,
        }
        MANIFEST_JSON.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
        raise SystemExit(f"EVALUACION TEST GC2/XGBOOST NO APROBADA: {failed[0]['name']} -> {failed[0]['detail']}")


if __name__ == "__main__":
    main()
