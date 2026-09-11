"""Runtime verification for GC3/GE v2 infrastructure.

This script builds the official models, checks parameter counts, compiles them,
constructs callbacks, and runs a synthetic forward pass. It does not train,
call model.fit, evaluate test data, or load official datasets.
"""

from __future__ import annotations

import io
import json
import os
import platform
import sys
import tempfile
import time
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def model_summary_text(model) -> str:
    buffer = io.StringIO()
    model.summary(print_fn=lambda line: buffer.write(line + "\n"))
    return buffer.getvalue()


def layer_params(model) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for layer in model.layers:
        rows.append(
            {
                "name": layer.name,
                "class": layer.__class__.__name__,
                "output_shape": str(getattr(layer, "output_shape", "n/a")),
                "params": int(layer.count_params()),
            }
        )
    return rows


def callback_report(callbacks) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for cb in callbacks:
        rows.append(
            {
                "class": cb.__class__.__name__,
                "monitor": getattr(cb, "monitor", None),
                "patience": getattr(cb, "patience", None),
                "restore_best_weights": getattr(cb, "restore_best_weights", None),
                "factor": getattr(cb, "factor", None),
                "min_lr": getattr(cb, "min_lr", None),
                "filepath": str(getattr(cb, "filepath", "")),
                "save_best_only": getattr(cb, "save_best_only", None),
            }
        )
    return rows


def verify_model(model_name: str, n_features_b: int) -> dict[str, object]:
    import tensorflow as tf

    from v2_reentrenamiento.src.models.dual_lstm_attention import (
        EXPECTED_PARAMS,
        build_gc3_ge_model,
    )

    build_start = time.perf_counter()
    model = build_gc3_ge_model(model_name, n_features_a=4, n_features_b=n_features_b)
    build_seconds = time.perf_counter() - build_start

    batch = 2
    xa = tf.zeros((batch, 6, 4), dtype=tf.float32)
    xb = tf.zeros((batch, 6, n_features_b), dtype=tf.float32)
    forward_start = time.perf_counter()
    output = model([xa, xb], training=False)
    forward_seconds = time.perf_counter() - forward_start

    return {
        "expected_params": EXPECTED_PARAMS[model_name],
        "actual_params": int(model.count_params()),
        "summary": model_summary_text(model),
        "layers": layer_params(model),
        "loss": model.loss,
        "optimizer": model.optimizer.__class__.__name__,
        "learning_rate": float(tf.keras.backend.get_value(model.optimizer.learning_rate)),
        "metrics_names": list(model.metrics_names),
        "input_shapes": [str(shape) for shape in model.input_shape],
        "output_shape": str(tuple(output.shape)),
        "build_seconds": round(build_seconds, 6),
        "forward_seconds": round(forward_seconds, 6),
    }


def main() -> None:
    import_start = time.perf_counter()

    from v2_reentrenamiento.src.training.determinism import configure_determinism

    determinism = configure_determinism(0)

    import tensorflow as tf
    from tensorflow import keras
    import numpy as np
    import pandas as pd
    import sklearn
    import joblib

    from v2_reentrenamiento.src.training.callbacks_gc3_ge import build_callbacks

    import_seconds = time.perf_counter() - import_start

    with tempfile.TemporaryDirectory(prefix="gc3_ge_callbacks_") as tmpdir:
        callbacks = build_callbacks(Path(tmpdir))
        callbacks_info = callback_report(callbacks)

    report = {
        "python": sys.version,
        "platform": platform.platform(),
        "tensorflow": tf.__version__,
        "keras": keras.__version__,
        "numpy": np.__version__,
        "pandas": pd.__version__,
        "sklearn": sklearn.__version__,
        "joblib": joblib.__version__,
        "import_seconds": round(import_seconds, 6),
        "env": {
            "PYTHONHASHSEED": os.environ.get("PYTHONHASHSEED"),
            "TF_ENABLE_ONEDNN_OPTS": os.environ.get("TF_ENABLE_ONEDNN_OPTS"),
            "TF_DETERMINISTIC_OPS": os.environ.get("TF_DETERMINISTIC_OPS"),
        },
        "determinism": determinism.__dict__,
        "tf": {
            "cuda_build": bool(tf.test.is_built_with_cuda()),
            "physical_cpu": [str(d) for d in tf.config.list_physical_devices("CPU")],
            "physical_gpu": [str(d) for d in tf.config.list_physical_devices("GPU")],
            "logical_devices": [str(d) for d in tf.config.list_logical_devices()],
            "build_info": dict(tf.sysconfig.get_build_info()),
        },
        "callbacks": callbacks_info,
        "models": {
            "GC3": verify_model("GC3", 33),
            "GE": verify_model("GE", 39),
        },
    }

    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
