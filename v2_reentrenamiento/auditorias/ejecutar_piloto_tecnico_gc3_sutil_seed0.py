"""Technical pilot run for GC3/Sutil/seed0.

This is not an official experimental run. It trains only one pilot model to
verify the end-to-end runtime path and writes artifacts under technical_pilot.
No test targets are loaded into the training/validation sequence object.
"""

from __future__ import annotations

import csv
import io
import json
import os
import platform
import sys
import time
from dataclasses import asdict, replace
from datetime import datetime, timezone
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def read_features_until_validation(path: Path):
    import pandas as pd

    rows: list[dict[str, str]] = []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            year = int(row["año"])
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
            "year": df["año"].astype(int),
            "month": df["mes"].astype(int),
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
                "Technical pilot directory already contains files and will not be overwritten: "
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
    pilot_label = "TECHNICAL_PILOT"
    dataset_path = REPO_ROOT / "v2_reentrenamiento/data/processed/master_dataset_sutil_v2_features.csv"
    scaler_path = REPO_ROOT / "v2_reentrenamiento/resultados_v2_final/scalers/scaler_sutil_v2c.joblib"
    base_dir = REPO_ROOT / "v2_reentrenamiento/resultados_v2_final/technical_pilot"
    paths = build_run_paths(base_dir, cultivar, model_name, seed)
    figures_dir = paths.root / "figures"
    prepare_pilot_directories(paths.root, figures_dir, paths.logs)

    log_buffer = io.StringIO()
    spec = build_feature_spec(cultivar, model_name)
    df = read_features_until_validation(dataset_path)
    sequences = build_train_validation_only(df, spec, OFFICIAL_TRAINING_CONFIG.lookback)

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

    config_payload = {
        "run_label": pilot_label,
        "cultivar": cultivar,
        "model_name": model_name,
        "seed": seed,
        "official_result": False,
        "must_retrain_official_seed0": True,
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
        gpu="[]",
        best_epoch=best_epoch,
        stopped_epoch=stopped_epoch,
    )
    write_metadata(metadata, paths.metadata)

    fit_log = {
        "run_label": pilot_label,
        "official_result": False,
        "test_loaded_into_sequence_object": False,
        "test_used": False,
        "model_fit_executed": True,
        "epochs_executed": stopped_epoch,
        "best_epoch": best_epoch,
        "stopped_epoch": stopped_epoch,
        "fit_seconds": fit_seconds,
        "seconds_per_epoch": fit_seconds / stopped_epoch,
        "estimated_seconds_40_runs": (fit_seconds / 1) * 40,
        "estimated_minutes_40_runs": (fit_seconds * 40) / 60,
        "reduce_lr_epochs": lr_logger.reductions,
        "restore_best_weights_verified": restore_best_weights_verified,
        "best_val_loss": best_val_loss,
        "final_val_loss_after_restore": final_val_loss_after_restore,
        "history_path": str(paths.history),
        "checkpoint_path": str(paths.checkpoint),
        "config_path": str(paths.config),
        "metadata_path": str(paths.metadata),
        "figures": plots,
        "dataset_path": str(dataset_path),
        "dataset_hash": sha256_file(dataset_path),
        "scaler_path": str(scaler_path),
        "scaler_hash": sha256_file(scaler_path),
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

    print(json.dumps(fit_log, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
