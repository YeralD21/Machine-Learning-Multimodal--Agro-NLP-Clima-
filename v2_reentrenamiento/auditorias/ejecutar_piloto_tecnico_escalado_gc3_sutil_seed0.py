"""Scaled technical pilot run for GC3/Sutil/seed0.

This is not an official experimental run. It trains only one pilot model to
verify the corrected scaled-data runtime path and writes artifacts under
technical_pilot_scaled. No test targets are loaded into model.fit.
"""

from __future__ import annotations

import csv
import json
import platform
import sys
import time
from dataclasses import asdict, replace
from datetime import datetime, timezone
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

YEAR_COL = "a\u00f1o"
MONTH_COL = "mes"


def read_until_validation(path: Path):
    import pandas as pd

    rows: list[dict[str, str]] = []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            year = int(row[YEAR_COL])
            if year <= 2024:
                rows.append(row)

    df = pd.DataFrame(rows)
    for column in df.columns:
        try:
            df[column] = pd.to_numeric(df[column])
        except (TypeError, ValueError):
            pass
    df["fecha"] = pd.to_datetime(
        {
            "year": df[YEAR_COL].astype(int),
            "month": df[MONTH_COL].astype(int),
            "day": 1,
        }
    )
    return df.sort_values("fecha").reset_index(drop=True)


def build_train_validation_only(df, spec, lookback: int):
    import numpy as np
    import pandas as pd

    required = set(spec.all_inputs + (spec.target,))
    missing = sorted(required.difference(df.columns))
    if missing:
        raise ValueError(f"Missing required columns: {missing}")
    if (df["fecha"].dt.year >= 2025).any():
        raise ValueError("TEST rows are not allowed in the technical pilot sequence frame.")
    if df["fecha"].min() != pd.Timestamp("2016-07-01"):
        raise ValueError(f"Expected first feature row 2016-07, got {df['fecha'].min()}.")
    if df["fecha"].max() != pd.Timestamp("2024-12-01"):
        raise ValueError(f"Expected last validation row 2024-12, got {df['fecha'].max()}.")
    if not df["fecha"].is_monotonic_increasing:
        raise ValueError("Feature rows must be sorted.")
    if df["fecha"].duplicated().any():
        raise ValueError("Duplicate monthly rows are not allowed.")

    values_a = df.loc[:, spec.rama_a].to_numpy(dtype=float)
    values_b = df.loc[:, spec.rama_b].to_numpy(dtype=float)
    values_y = df.loc[:, spec.target].to_numpy(dtype=float)
    dates = df["fecha"].reset_index(drop=True)

    X_a, X_b, y, target_dates = [], [], [], []
    for target_idx in range(lookback, len(df)):
        start = target_idx - lookback
        stop = target_idx
        context_dates = dates.iloc[start:stop]
        target_date = dates.iloc[target_idx]
        if not (context_dates < target_date).all():
            raise ValueError("Future leakage detected in context dates.")
        X_a.append(values_a[start:stop])
        X_b.append(values_b[start:stop])
        y.append(float(values_y[target_idx]))
        target_dates.append(target_date)

    X_a_array = np.stack(X_a)
    X_b_array = np.stack(X_b)
    y_array = np.asarray(y, dtype=float)
    target_dates_series = pd.Series(target_dates, name="fecha")

    train_mask = (target_dates_series.dt.year <= 2023).to_numpy()
    val_mask = (target_dates_series.dt.year == 2024).to_numpy()

    return {
        "train": {
            "X_a": X_a_array[train_mask],
            "X_b": X_b_array[train_mask],
            "y": y_array[train_mask],
            "dates": target_dates_series[train_mask].reset_index(drop=True),
        },
        "validation": {
            "X_a": X_a_array[val_mask],
            "X_b": X_b_array[val_mask],
            "y": y_array[val_mask],
            "dates": target_dates_series[val_mask].reset_index(drop=True),
        },
        "feature_rows_loaded": int(len(df)),
    }


class LearningRateLogger:
    def __init__(self):
        self.values: list[float] = []
        self.reductions: list[int] = []

    def as_callback(self):
        import tensorflow as tf

        outer = self

        class _LearningRateLogger(tf.keras.callbacks.Callback):
            def on_epoch_end(self, epoch, logs=None):
                lr = float(tf.keras.backend.get_value(self.model.optimizer.learning_rate))
                outer.values.append(lr)
                if len(outer.values) > 1 and lr < outer.values[-2]:
                    outer.reductions.append(epoch + 1)

        return _LearningRateLogger()


def save_json(payload: dict, path: Path) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def prepare_pilot_directories(root: Path, figures_dir: Path, logs_dir: Path) -> None:
    if root.exists():
        existing_files = [path for path in root.rglob("*") if path.is_file()]
        if existing_files:
            raise FileExistsError(
                "Scaled technical pilot directory already contains files and will not be overwritten: "
                + ", ".join(str(path) for path in existing_files)
            )
    root.mkdir(parents=True, exist_ok=True)
    figures_dir.mkdir(parents=True, exist_ok=True)
    logs_dir.mkdir(parents=True, exist_ok=True)


def plot_history(history_frame, best_epoch: int, stopped_epoch: int, output_dir: Path) -> dict[str, str]:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    plots = {}
    specs = [
        ("mse", "loss", "val_loss", "TRAIN MSE vs VALIDATION MSE", "pilot_loss_mse.png"),
        ("mae", "mae", "val_mae", "TRAIN MAE vs VALIDATION MAE", "pilot_metric_mae.png"),
    ]
    for key, train_col, val_col, title, filename in specs:
        fig, ax = plt.subplots(figsize=(9, 5))
        ax.plot(history_frame["epoch"], history_frame[train_col], label=f"train {train_col}")
        ax.plot(history_frame["epoch"], history_frame[val_col], label=f"validation {val_col}")
        ax.axvline(best_epoch, color="tab:green", linestyle="--", label=f"best_epoch={best_epoch}")
        ax.axvline(stopped_epoch, color="tab:red", linestyle=":", label=f"stopped_epoch={stopped_epoch}")
        ax.set_title(f"TECHNICAL PILOT - NOT OFFICIAL RESULT\n{title}")
        ax.set_xlabel("Epoch")
        ax.set_ylabel(train_col.upper())
        ax.legend()
        ax.grid(alpha=0.25)
        fig.tight_layout()
        output_path = output_dir / filename
        fig.savefig(output_path, dpi=150)
        plt.close(fig)
        plots[key] = str(output_path)
    return plots


def stats(values) -> dict[str, float]:
    import numpy as np

    array = np.asarray(values, dtype=float)
    return {
        "min": float(np.min(array)),
        "max": float(np.max(array)),
        "mean": float(np.mean(array)),
        "std_pop": float(np.std(array, ddof=0)),
        "std_sample": float(np.std(array, ddof=1)) if len(array) > 1 else 0.0,
    }


def load_scaler_params(path: Path) -> dict[str, dict[str, float]]:
    import pandas as pd

    frame = pd.read_csv(path)
    return {
        str(row["feature"]): {
            "mean_train": float(row["mean_train"]),
            "scale_train": float(row["scale_train"]),
            "var_train": float(row["var_train"]),
        }
        for _, row in frame.iterrows()
    }


def verify_scaled_source(scaled_df, raw_df, spec, scaler_params: dict[str, dict[str, float]]) -> dict:
    import numpy as np

    if not scaled_df["fecha"].equals(raw_df["fecha"]):
        raise ValueError("Raw and scaled dataframes are not aligned by date.")
    for column in (YEAR_COL, MONTH_COL, "mes_sin", "mes_cos"):
        if not np.allclose(scaled_df[column].to_numpy(float), raw_df[column].to_numpy(float), atol=0.0):
            raise ValueError(f"Column {column} should remain raw and unchanged.")

    checked_columns = [
        column
        for column in spec.all_inputs + (spec.target,)
        if column not in ("mes_sin", "mes_cos")
    ]
    max_abs_diff = 0.0
    for column in checked_columns:
        params = scaler_params[column]
        expected = (raw_df[column].to_numpy(float) - params["mean_train"]) / params["scale_train"]
        actual = scaled_df[column].to_numpy(float)
        max_abs_diff = max(max_abs_diff, float(np.max(np.abs(actual - expected))))
    if max_abs_diff > 1e-10:
        raise ValueError(f"Scaled CSV does not match frozen scaler parameters: {max_abs_diff}")

    train_scaled = scaled_df.loc[scaled_df["fecha"].dt.year <= 2023, checked_columns]
    return {
        "unchanged_raw_columns": [YEAR_COL, MONTH_COL, "mes_sin", "mes_cos"],
        "scaled_columns_checked": checked_columns,
        "max_abs_diff_scaled_csv_vs_formula": max_abs_diff,
        "train_scaled_abs_mean_max": float(train_scaled.mean().abs().max()),
        "train_scaled_std_pop_min": float(train_scaled.std(ddof=0).min()),
        "train_scaled_std_pop_max": float(train_scaled.std(ddof=0).max()),
    }


def inverse_target(values, target_params: dict[str, float]):
    import numpy as np

    array = np.asarray(values, dtype=float)
    return array * target_params["scale_train"] + target_params["mean_train"]


def alignment_examples(sequences, raw_df, scaled_df, spec, target_params):
    examples = []
    target_dates = sequences["train"]["dates"]
    indices = [0, len(target_dates) // 2, len(target_dates) - 1]
    for sequence_idx in indices:
        target_date = target_dates.iloc[sequence_idx]
        target_row_idx = int(scaled_df.index[scaled_df["fecha"] == target_date][0])
        context_end_idx = target_row_idx - 1
        context_end_date = scaled_df.loc[context_end_idx, "fecha"]
        input_scaled = float(scaled_df.loc[context_end_idx, spec.rama_a[0]])
        input_raw = float(raw_df.loc[context_end_idx, spec.rama_a[0]])
        target_scaled = float(scaled_df.loc[target_row_idx, spec.target])
        target_raw = float(raw_df.loc[target_row_idx, spec.target])
        target_inverse = float(inverse_target([target_scaled], target_params)[0])
        if abs(target_inverse - target_raw) > 1e-8:
            raise ValueError("Target inverse transform failed for audit example.")
        examples.append(
            {
                "sequence_index": int(sequence_idx),
                "fecha_final_secuencia": str(context_end_date.date()),
                "produccion_t_input_raw": input_raw,
                "produccion_t_input_scaled": input_scaled,
                "fecha_target": str(target_date.date()),
                "target_raw_toneladas": target_raw,
                "target_scaled_model": target_scaled,
                "target_inverse_toneladas": target_inverse,
            }
        )
    return examples


def read_raw_summary(path: Path) -> dict | None:
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def write_report(path: Path, summary: dict) -> None:
    pre = summary["preflight"]
    raw = summary["technical_comparison_with_raw_pilot"]
    lines = [
        "# AUDITORIA PILOTO TECNICO ESCALADO GC3 SUTIL SEED0",
        "",
        "## 1. Alcance",
        "",
        "Auditoria del segundo piloto tecnico GC3/Sutil/seed0, ejecutado como",
        "`TECHNICAL_PILOT_SCALED` y separado de resultados oficiales. La unica",
        "correccion aplicada fue leer `master_dataset_sutil_v2_escalado.csv` como",
        "fuente de entrenamiento. No se modificaron arquitectura, features, split,",
        "loss, callbacks, seeds, scalers ni decisiones metodologicas.",
        "",
        "## 2. Fuente de datos",
        "",
        f"- Dataset usado por model.fit: `{summary['dataset_path']}`",
        f"- Scaler usado solo para metadata/inverse transform: `{summary['scaler_path']}`",
        f"- Se aplico scaler nuevamente sobre `_escalado.csv`: {summary['scaler_applied_again']}",
        f"- TEST cargado en el objeto de secuencias: {summary['test_loaded_into_sequence_object']}",
        f"- TEST usado: {summary['test_used']}",
        "",
        "## 3. Pre-flight",
        "",
        f"- Parametros GC3 esperados/obtenidos: 6273 / {pre['params']}",
        f"- Loss: {pre['loss']}",
        f"- Optimizer: {pre['optimizer']}",
        f"- Learning rate: {pre['learning_rate']}",
        f"- Shuffle: {pre['shuffle']}",
        "- Callbacks: EarlyStopping, ReduceLROnPlateau, ModelCheckpoint y logger tecnico de learning rate.",
        "",
        "## 4. Shapes",
        "",
        "| Tensor | Shape |",
        "|---|---:|",
    ]
    for key, value in summary["shapes"].items():
        lines.append(f"| {key} | {tuple(value)} |")
    lines.extend(
        [
            "",
            "## 5. Escala del target",
            "",
            "| Split | min | max | mean | std pop |",
            "|---|---:|---:|---:|---:|",
            f"| TRAIN y escalado | {summary['target_stats']['train']['min']:.12f} | {summary['target_stats']['train']['max']:.12f} | {summary['target_stats']['train']['mean']:.12f} | {summary['target_stats']['train']['std_pop']:.12f} |",
            f"| VAL y escalado | {summary['target_stats']['validation']['min']:.12f} | {summary['target_stats']['validation']['max']:.12f} | {summary['target_stats']['validation']['mean']:.12f} | {summary['target_stats']['validation']['std_pop']:.12f} |",
            "",
            "El target entregado a `model.fit()` esta en escala StandardScaler y se deriva",
            "de `produccion_t_sutil` en la fila t+1.",
            "",
            "## 6. Verificaciones de escalado",
            "",
            f"- `produccion_t_sutil` de Rama A escalada: {summary['scale_checks']['rama_a_produccion_scaled']}",
            f"- `mes_sin`/`mes_cos` permanecen raw: {summary['scale_checks']['mes_sin_cos_raw']}",
            f"- Resto de Rama B escalada: {summary['scale_checks']['rama_b_non_calendar_scaled']}",
            f"- max abs diff `_escalado.csv` vs formula del scaler congelado: {summary['scale_checks']['max_abs_diff_scaled_csv_vs_formula']:.3e}",
            "",
            "## 7. Ejemplos t -> t+1",
            "",
            "| fecha_final_secuencia | produccion_t raw | produccion_t scaled | fecha_target | target raw | target scaled | inverse(target) |",
            "|---|---:|---:|---|---:|---:|---:|",
        ]
    )
    for item in summary["alignment_examples"]:
        lines.append(
            "| {fecha_final_secuencia} | {produccion_t_input_raw:.6f} | {produccion_t_input_scaled:.12f} | "
            "{fecha_target} | {target_raw_toneladas:.6f} | {target_scaled_model:.12f} | {target_inverse_toneladas:.6f} |".format(
                **item
            )
        )
    lines.extend(
        [
            "",
            "## 8. Ejecucion model.fit",
            "",
            f"- Epochs ejecutadas: {summary['epochs_executed']}",
            f"- Best epoch: {summary['best_epoch']}",
            f"- Stopped epoch: {summary['stopped_epoch']}",
            f"- Best val_loss: {summary['best_val_loss']}",
            f"- Final val_loss tras restore_best_weights: {summary['final_val_loss_after_restore']}",
            f"- Restore best weights verificado: {summary['restore_best_weights_verified']}",
            f"- ReduceLROnPlateau epochs: {summary['reduce_lr_epochs']}",
            "",
            "## 9. History",
            "",
            f"- Ruta: `{summary['history_path']}`",
            f"- Filas: {summary['history_rows']}",
            f"- NaN en columnas numericas: {summary['history_nan_count']}",
            f"- Inf en columnas numericas: {summary['history_inf_count']}",
            f"- Columnas: {', '.join(summary['history_columns'])}",
            "",
            "## 10. Graficos",
            "",
            f"- TRAIN vs VAL MSE: `{summary['figures']['mse']}`",
            f"- TRAIN vs VAL MAE: `{summary['figures']['mae']}`",
            "",
            "Las figuras usan solo el history del piloto escalado e incluyen marcas de",
            "`best_epoch` y `stopped_epoch`.",
            "",
            "## 11. Checkpoint y metadata",
            "",
            f"- Checkpoint: `{summary['checkpoint_path']}`",
            f"- Metadata: `{summary['metadata_path']}`",
            f"- Config: `{summary['config_path']}`",
            f"- Log: `{summary['log_path']}`",
            "",
            "Estos artefactos pertenecen al piloto tecnico y no deben reutilizarse en las",
            "corridas oficiales.",
            "",
            "## 12. Tiempo",
            "",
            f"- Tiempo total fit: {summary['fit_seconds']:.6f} segundos",
            f"- Segundos/epoch: {summary['seconds_per_epoch']:.6f}",
            f"- Estimacion operativa 40 corridas: {summary['estimated_minutes_40_runs']:.3f} minutos",
            "",
            "La estimacion es solo una extrapolacion operativa del piloto. EarlyStopping",
            "puede producir duraciones distintas entre modelos/seeds.",
            "",
            "## 13. Comparacion tecnica con piloto raw",
            "",
            "| Aspecto | Piloto raw | Piloto escalado |",
            "|---|---:|---:|",
            f"| y_train min/max | {raw['raw_y_train_range']} | {raw['scaled_y_train_range']} |",
            f"| y_val min/max | {raw['raw_y_val_range']} | {raw['scaled_y_val_range']} |",
            f"| best val_loss | {raw['raw_best_val_loss']} | {raw['scaled_best_val_loss']} |",
            f"| tiempo fit segundos | {raw['raw_fit_seconds']} | {raw['scaled_fit_seconds']} |",
            f"| epochs | {raw['raw_epochs']} | {raw['scaled_epochs']} |",
            f"| ReduceLR epochs | {raw['raw_reduce_lr_epochs']} | {raw['scaled_reduce_lr_epochs']} |",
            "",
            "Comparacion estrictamente tecnica de escala, magnitud de loss, tiempo y",
            "callbacks. No constituye comparacion de rendimiento cientifico.",
            "",
            "## 14. Estado final",
            "",
            "El piloto escalado completo datos -> secuencias -> factory -> compile -> fit",
            "-> validation -> callbacks -> history -> checkpoint -> metadata usando",
            "`master_dataset_sutil_v2_escalado.csv`. TEST 2025 no fue usado.",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    start_total = time.perf_counter()

    from v2_reentrenamiento.src.training.determinism import configure_determinism

    determinism_report = configure_determinism(0)

    import numpy as np
    import pandas as pd
    import tensorflow as tf
    from tensorflow import keras

    from v2_reentrenamiento.src.models.dual_lstm_attention import build_gc3_ge_model
    from v2_reentrenamiento.src.models.features_gc3_ge import build_feature_spec
    from v2_reentrenamiento.src.training.artifacts_gc3_ge import build_run_paths
    from v2_reentrenamiento.src.training.callbacks_gc3_ge import build_callbacks
    from v2_reentrenamiento.src.training.config_gc3_ge import OFFICIAL_TRAINING_CONFIG
    from v2_reentrenamiento.src.training.history_gc3_ge import history_to_frame, save_history_frame
    from v2_reentrenamiento.src.training.metadata_gc3_ge import (
        base_metadata,
        sha256_file,
        write_metadata,
    )

    cultivar = "sutil"
    model_name = "GC3"
    seed = 0
    pilot_label = "TECHNICAL_PILOT_SCALED"
    dataset_path = REPO_ROOT / "v2_reentrenamiento/data/processed/master_dataset_sutil_v2_escalado.csv"
    raw_dataset_path = REPO_ROOT / "v2_reentrenamiento/data/processed/master_dataset_sutil_v2_features.csv"
    scaler_path = REPO_ROOT / "v2_reentrenamiento/resultados_v2_final/scalers/scaler_sutil_v2c.joblib"
    scaler_params_path = REPO_ROOT / "v2_reentrenamiento/resultados_v2_final/scalers/scaler_sutil_v2c_parametros.csv"
    raw_pilot_summary_path = (
        REPO_ROOT
        / "v2_reentrenamiento/resultados_v2_final/technical_pilot/sutil/GC3/seed_00/logs/pilot_run_summary.json"
    )
    report_path = (
        REPO_ROOT
        / "v2_reentrenamiento/auditorias/AUDITORIA_PILOTO_TECNICO_ESCALADO_GC3_SUTIL_SEED0.md"
    )
    base_dir = REPO_ROOT / "v2_reentrenamiento/resultados_v2_final/technical_pilot_scaled"
    paths = build_run_paths(base_dir, cultivar, model_name, seed)
    figures_dir = paths.root / "figures"
    prepare_pilot_directories(paths.root, figures_dir, paths.logs)

    spec = build_feature_spec(cultivar, model_name)
    scaled_df = read_until_validation(dataset_path)
    raw_df = read_until_validation(raw_dataset_path)
    scaler_params = load_scaler_params(scaler_params_path)
    source_checks = verify_scaled_source(scaled_df, raw_df, spec, scaler_params)
    sequences = build_train_validation_only(scaled_df, spec, OFFICIAL_TRAINING_CONFIG.lookback)

    Xa_train = sequences["train"]["X_a"]
    Xb_train = sequences["train"]["X_b"]
    y_train = sequences["train"]["y"]
    Xa_val = sequences["validation"]["X_a"]
    Xb_val = sequences["validation"]["X_b"]
    y_val = sequences["validation"]["y"]

    expected_shapes = {
        "Xa_train": (84, 6, 4),
        "Xb_train": (84, 6, 33),
        "y_train": (84,),
        "Xa_val": (12, 6, 4),
        "Xb_val": (12, 6, 33),
        "y_val": (12,),
    }
    obtained_shapes = {
        "Xa_train": tuple(Xa_train.shape),
        "Xb_train": tuple(Xb_train.shape),
        "y_train": tuple(y_train.shape),
        "Xa_val": tuple(Xa_val.shape),
        "Xb_val": tuple(Xb_val.shape),
        "y_val": tuple(y_val.shape),
    }
    if obtained_shapes != expected_shapes:
        raise ValueError(f"Shape mismatch: expected {expected_shapes}, got {obtained_shapes}")

    if sequences["validation"]["dates"].dt.year.tolist() != [2024] * 12:
        raise ValueError("Validation targets must be exactly 2024.")

    target_params = scaler_params[spec.target]
    raw_train_targets = raw_df.loc[raw_df["fecha"].isin(sequences["train"]["dates"]), spec.target].to_numpy(float)
    expected_y_train = (raw_train_targets - target_params["mean_train"]) / target_params["scale_train"]
    raw_val_targets = raw_df.loc[raw_df["fecha"].isin(sequences["validation"]["dates"]), spec.target].to_numpy(float)
    expected_y_val = (raw_val_targets - target_params["mean_train"]) / target_params["scale_train"]
    if not np.allclose(y_train, expected_y_train, atol=1e-12):
        raise ValueError("y_train is not the StandardScaler target transformation.")
    if not np.allclose(y_val, expected_y_val, atol=1e-12):
        raise ValueError("y_val is not the StandardScaler target transformation.")

    rama_a_context_flat = Xa_train[:, :, 0].reshape(-1)
    if abs(float(np.median(rama_a_context_flat))) > 10 or float(np.max(np.abs(rama_a_context_flat))) > 10:
        raise ValueError("Rama A produccion_t_sutil does not look scaled.")
    mes_sin_idx = spec.rama_b.index("mes_sin")
    mes_cos_idx = spec.rama_b.index("mes_cos")
    if not (
        np.max(np.abs(Xb_train[:, :, mes_sin_idx])) <= 1.0 + 1e-12
        and np.max(np.abs(Xb_train[:, :, mes_cos_idx])) <= 1.0 + 1e-12
    ):
        raise ValueError("mes_sin/mes_cos should remain raw calendar values.")
    non_calendar_b = [feature for feature in spec.rama_b if feature not in ("mes_sin", "mes_cos")]
    if not all(feature in scaler_params for feature in non_calendar_b):
        raise ValueError("All non-calendar Rama B features must have scaler parameters.")

    model = build_gc3_ge_model(model_name, n_features_a=4, n_features_b=33)
    if model.count_params() != 6273:
        raise ValueError(f"GC3 count_params mismatch: {model.count_params()}")
    if model.loss != "mse":
        raise ValueError(f"Expected loss mse, got {model.loss}")
    if model.optimizer.__class__.__name__ != "Adam":
        raise ValueError(f"Expected Adam optimizer, got {model.optimizer.__class__.__name__}")
    learning_rate = float(tf.keras.backend.get_value(model.optimizer.learning_rate))
    if not np.isclose(learning_rate, OFFICIAL_TRAINING_CONFIG.learning_rate):
        raise ValueError(f"Learning rate mismatch: {learning_rate}")
    if OFFICIAL_TRAINING_CONFIG.shuffle is not False:
        raise ValueError("Official shuffle must be False.")

    callbacks = build_callbacks(paths.root)
    lr_logger = LearningRateLogger()
    callbacks.append(lr_logger.as_callback())
    callback_checks = [
        {
            "class": cb.__class__.__name__,
            "monitor": getattr(cb, "monitor", None),
            "patience": getattr(cb, "patience", None),
            "restore_best_weights": getattr(cb, "restore_best_weights", None),
            "factor": getattr(cb, "factor", None),
            "min_lr": getattr(cb, "min_lr", None),
        }
        for cb in callbacks
    ]
    if callback_checks[0]["monitor"] != "val_loss" or callback_checks[1]["monitor"] != "val_loss":
        raise ValueError("Official callbacks must monitor val_loss.")

    target_stats = {
        "train": stats(y_train),
        "validation": stats(y_val),
    }
    examples = alignment_examples(sequences, raw_df, scaled_df, spec, target_params)
    scale_checks = {
        "rama_a_produccion_scaled": True,
        "mes_sin_cos_raw": True,
        "rama_b_non_calendar_scaled": True,
        **source_checks,
    }

    config_payload = {
        "run_label": pilot_label,
        "cultivar": cultivar,
        "model_name": model_name,
        "seed": seed,
        "official_result": False,
        "must_retrain_official_seed0": True,
        "raw_pilot_discarded": True,
        "scaled_training_source": True,
        "no_runtime_scaler_transform": True,
        "lookback": OFFICIAL_TRAINING_CONFIG.lookback,
        "batch_size": OFFICIAL_TRAINING_CONFIG.batch_size,
        "max_epochs": OFFICIAL_TRAINING_CONFIG.max_epochs,
        "shuffle": OFFICIAL_TRAINING_CONFIG.shuffle,
        "loss": OFFICIAL_TRAINING_CONFIG.loss,
        "metric": OFFICIAL_TRAINING_CONFIG.metric,
        "learning_rate": OFFICIAL_TRAINING_CONFIG.learning_rate,
        "callbacks": callback_checks,
        "features_a": spec.rama_a,
        "features_b": spec.rama_b,
        "target": spec.target,
        "target_scaler_params": target_params,
    }
    save_json(config_payload, paths.config)

    model_config_for_hash = {
        "architecture": "DualLSTM16_BahdanauAttention16_Dense16_Dropout020_Dense8_Dense1",
        "model": model_name,
        "features_a": spec.rama_a,
        "features_b": spec.rama_b,
        "config": config_payload,
    }

    fit_start = time.perf_counter()
    history = model.fit(
        [Xa_train, Xb_train],
        y_train,
        validation_data=([Xa_val, Xb_val], y_val),
        epochs=OFFICIAL_TRAINING_CONFIG.max_epochs,
        batch_size=OFFICIAL_TRAINING_CONFIG.batch_size,
        shuffle=OFFICIAL_TRAINING_CONFIG.shuffle,
        callbacks=callbacks,
        verbose=2,
    )
    fit_seconds = time.perf_counter() - fit_start

    history_frame = history_to_frame(history, lr_logger.values)
    save_history_frame(history_frame, paths.history)
    history_numeric = history_frame.loc[:, ["loss", "val_loss", "mae", "val_mae", "learning_rate"]]

    best_epoch = int(history_frame.loc[history_frame["val_loss"].idxmin(), "epoch"])
    stopped_epoch = int(len(history_frame))
    best_val_loss = float(history_frame["val_loss"].min())
    final_val_loss_after_restore = float(
        model.evaluate([Xa_val, Xb_val], y_val, verbose=0, return_dict=True)["loss"]
    )
    restore_best_weights_verified = bool(np.isclose(final_val_loss_after_restore, best_val_loss, rtol=1e-6, atol=1e-8))

    plots = plot_history(history_frame, best_epoch, stopped_epoch, figures_dir)

    metadata = base_metadata(
        cultivar=cultivar,
        model_name=model_name,
        seed=seed,
        model_config=model_config_for_hash,
        determinism_settings=asdict(determinism_report),
        dataset_path=dataset_path,
        scaler_path=scaler_path,
        lookback=OFFICIAL_TRAINING_CONFIG.lookback,
        batch_size=OFFICIAL_TRAINING_CONFIG.batch_size,
        learning_rate=OFFICIAL_TRAINING_CONFIG.learning_rate,
        max_epochs=OFFICIAL_TRAINING_CONFIG.max_epochs,
    )
    metadata = replace(
        metadata,
        tensorflow_version=tf.__version__,
        keras_version=keras.__version__,
        numpy_version=np.__version__,
        gpu=str(tf.config.list_physical_devices("GPU")),
        best_epoch=best_epoch,
        stopped_epoch=stopped_epoch,
    )
    write_metadata(metadata, paths.metadata)

    raw_summary = read_raw_summary(raw_pilot_summary_path)
    technical_comparison = {
        "raw_y_train_range": "raw pilot summary did not store target range",
        "raw_y_val_range": "raw pilot summary did not store target range",
        "raw_best_val_loss": raw_summary.get("best_val_loss") if raw_summary else None,
        "raw_fit_seconds": raw_summary.get("fit_seconds") if raw_summary else None,
        "raw_epochs": raw_summary.get("epochs_executed") if raw_summary else None,
        "raw_reduce_lr_epochs": raw_summary.get("reduce_lr_epochs") if raw_summary else None,
        "scaled_y_train_range": f"{target_stats['train']['min']:.6f}..{target_stats['train']['max']:.6f}",
        "scaled_y_val_range": f"{target_stats['validation']['min']:.6f}..{target_stats['validation']['max']:.6f}",
        "scaled_best_val_loss": best_val_loss,
        "scaled_fit_seconds": fit_seconds,
        "scaled_epochs": stopped_epoch,
        "scaled_reduce_lr_epochs": lr_logger.reductions,
    }

    fit_log = {
        "run_label": pilot_label,
        "official_result": False,
        "raw_pilot_discarded": True,
        "test_loaded_into_sequence_object": False,
        "test_used": False,
        "scaler_applied_again": False,
        "model_fit_executed": True,
        "epochs_executed": stopped_epoch,
        "best_epoch": best_epoch,
        "stopped_epoch": stopped_epoch,
        "fit_seconds": fit_seconds,
        "seconds_per_epoch": fit_seconds / stopped_epoch,
        "estimated_seconds_40_runs": fit_seconds * 40,
        "estimated_minutes_40_runs": (fit_seconds * 40) / 60,
        "reduce_lr_epochs": lr_logger.reductions,
        "restore_best_weights_verified": restore_best_weights_verified,
        "best_val_loss": best_val_loss,
        "final_val_loss_after_restore": final_val_loss_after_restore,
        "history_rows": int(len(history_frame)),
        "history_columns": list(history_frame.columns),
        "history_nan_count": int(history_numeric.isna().sum().sum()),
        "history_inf_count": int(np.isinf(history_numeric.to_numpy(dtype=float)).sum()),
        "history_path": str(paths.history),
        "checkpoint_path": str(paths.checkpoint),
        "config_path": str(paths.config),
        "metadata_path": str(paths.metadata),
        "log_path": str(paths.logs / "pilot_run_summary.json"),
        "figures": plots,
        "dataset_path": str(dataset_path),
        "raw_dataset_path": str(raw_dataset_path),
        "dataset_hash": sha256_file(dataset_path),
        "raw_dataset_hash": sha256_file(raw_dataset_path),
        "scaler_path": str(scaler_path),
        "scaler_hash": sha256_file(scaler_path),
        "scaler_params_path": str(scaler_params_path),
        "scaler_params_hash": sha256_file(scaler_params_path),
        "target_scaler_params": target_params,
        "target_stats": target_stats,
        "scale_checks": scale_checks,
        "alignment_examples": examples,
        "technical_comparison_with_raw_pilot": technical_comparison,
        "shapes": {key: list(value) for key, value in obtained_shapes.items()},
        "train_target_start": str(sequences["train"]["dates"].iloc[0].date()),
        "train_target_end": str(sequences["train"]["dates"].iloc[-1].date()),
        "validation_target_start": str(sequences["validation"]["dates"].iloc[0].date()),
        "validation_target_end": str(sequences["validation"]["dates"].iloc[-1].date()),
        "python": sys.version,
        "platform": platform.platform(),
        "tensorflow": tf.__version__,
        "keras": keras.__version__,
        "numpy": np.__version__,
        "pandas": pd.__version__,
        "tf_devices": {
            "cpu": [str(device) for device in tf.config.list_physical_devices("CPU")],
            "gpu": [str(device) for device in tf.config.list_physical_devices("GPU")],
            "logical": [str(device) for device in tf.config.list_logical_devices()],
        },
        "determinism": asdict(determinism_report),
        "preflight": {
            "params": int(model.count_params()),
            "loss": model.loss,
            "optimizer": model.optimizer.__class__.__name__,
            "learning_rate": learning_rate,
            "shuffle": OFFICIAL_TRAINING_CONFIG.shuffle,
            "callbacks": callback_checks,
        },
        "total_script_seconds": time.perf_counter() - start_total,
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
    }
    save_json(fit_log, paths.logs / "pilot_run_summary.json")
    write_report(report_path, fit_log)

    required_artifacts = [
        paths.history,
        paths.checkpoint,
        paths.config,
        paths.metadata,
        paths.logs / "pilot_run_summary.json",
        Path(plots["mse"]),
        Path(plots["mae"]),
        report_path,
    ]
    missing_artifacts = [str(path) for path in required_artifacts if not path.exists()]
    if missing_artifacts:
        raise FileNotFoundError(f"Missing artifacts after scaled pilot: {missing_artifacts}")

    print(json.dumps(fit_log, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
