"""Technical SHAP benchmark on frozen seed-0 models and two VAL samples only."""

from __future__ import annotations

import csv
import gc
import hashlib
import inspect
import io
import json
import os
import platform
import random
import subprocess
import sys
import threading
import time
import warnings
from collections import Counter
from contextlib import redirect_stderr
from datetime import datetime, timezone
from itertools import islice
from pathlib import Path

os.environ["TF_ENABLE_ONEDNN_OPTS"] = "0"
os.environ["TF_DETERMINISTIC_OPS"] = "1"
os.environ["TF_CPP_MIN_LOG_LEVEL"] = "2"

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

import joblib
import numpy as np
import pandas as pd
import psutil
import scipy
from scipy.stats import spearmanr
import shap
import tensorflow as tf
import xgboost
from tensorflow import keras

from v2_reentrenamiento.auditorias.auditar_shap_v2_preflight import (
    _build_gc3_ge_train_val_sequences,
)
from v2_reentrenamiento.src.models.dual_lstm_attention import BahdanauAttention
from v2_reentrenamiento.src.models.features_gc3_ge import build_feature_spec
from v2_reentrenamiento.src.models.features_gc2_xgboost import build_gc2_feature_spec
from v2_reentrenamiento.src.training.tabular_gc2_xgboost import build_gc2_tabular_bundle

AUDIT = ROOT / "v2_reentrenamiento/auditorias"
OUTPUT = AUDIT / "shap_v2_explainer_benchmark.json"
ARRAYS = AUDIT / "shap_v2_benchmark_arrays"
RESULTS = ROOT / "v2_reentrenamiento/resultados_v2_final"
VAL_INDICES = [0, 6]
RNG_REPEATS = [(1729, "reference"), (1729, "same_seed"), (1730, "different_seed")]
BUDGETS = {
    "KernelExplainer": {"base": 2048, "high": 4096},
    "SamplingExplainer": {"base": 16384, "high": 65536},
    "PermutationExplainer": {"base": 4, "high": 16},
}


def sha256(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def array_hash(array):
    array = np.ascontiguousarray(array)
    header = json.dumps({"shape": array.shape, "dtype": array.dtype.str}).encode()
    return hashlib.sha256(header + array.tobytes()).hexdigest()


def persist(evidence):
    OUTPUT.write_text(json.dumps(evidence, indent=2, allow_nan=False), encoding="utf-8")


def bounded_frame(cultivar):
    path = ROOT / f"v2_reentrenamiento/data/processed/master_dataset_{cultivar}_v2_escalado.csv"
    # islice never requests row 103: no 2025 record is parsed or materialized.
    with path.open(encoding="utf-8-sig", newline="") as handle:
        rows = list(islice(csv.DictReader(handle), 102))
    frame = pd.DataFrame(rows).apply(pd.to_numeric, errors="raise")
    frame["fecha"] = pd.to_datetime(dict(year=frame["a\u00f1o"], month=frame["mes"], day=1))
    expected = pd.date_range("2016-07-01", "2024-12-01", freq="MS")
    assert frame["fecha"].tolist() == expected.tolist()
    assert len(frame) == 102 and frame["fecha"].dt.year.max() == 2024
    return frame, path


def evenly_spaced(n, size):
    indices = np.rint(np.linspace(0, n - 1, size)).astype(int)
    assert len(np.unique(indices)) == size and indices[0] == 0 and indices[-1] == n - 1
    return indices


def flatten(split):
    return np.hstack([split["X_a"].reshape(len(split["y"]), -1),
                      split["X_b"].reshape(len(split["y"]), -1)]).astype(np.float32)


class PredictWrapper:
    def __init__(self, model, n_b):
        self.rows = 0
        self.calls = 0

        @tf.function(input_signature=[tf.TensorSpec([None, 24 + 6 * n_b], tf.float32)])
        def forward(x):
            return tf.reshape(model([tf.reshape(x[:, :24], [-1, 6, 4]),
                                     tf.reshape(x[:, 24:], [-1, 6, n_b])], training=False), [-1])

        self.forward = forward

    def __call__(self, x):
        x = np.asarray(x, dtype=np.float32)
        self.calls += 1
        self.rows += len(x)
        return np.concatenate([self.forward(x[i:i + 1024]).numpy()
                               for i in range(0, len(x), 1024)]).astype(np.float64)


class MemoryProbe:
    """Sample process RSS, including TensorFlow, at 20 ms intervals."""

    def __enter__(self):
        self.process = psutil.Process()
        self.start = self.peak = self.process.memory_info().rss
        self.stop = threading.Event()
        def monitor():
            while not self.stop.wait(0.02):
                self.peak = max(self.peak, self.process.memory_info().rss)
        self.thread = threading.Thread(target=monitor, daemon=True)
        self.thread.start()
        return self

    def __exit__(self, *args):
        self.stop.set()
        self.thread.join()
        self.end = self.process.memory_info().rss
        self.peak = max(self.peak, self.end)

    def result(self):
        return {"rss_start_mib": self.start / 2**20, "rss_peak_mib": self.peak / 2**20,
                "rss_peak_increase_mib": (self.peak - self.start) / 2**20,
                "scope": "process RSS; allocator retention and warm caches; not isolated explainer memory"}


def run_probe(method, predictor, background, samples, n_b, level, rng, repeat, run_id):
    budget = BUDGETS[method][level]
    params = {"link": "identity", "rng_seed": rng}
    if method == "PermutationExplainer":
        params.update(max_evals=budget * (2 * samples.shape[1] + 1),
                      nominal_permutations=budget, masker="Independent", max_samples=len(background),
                      batch_size=1024)
    else:
        params.update(nsamples=budget)
        if method == "KernelExplainer":
            params["l1_reg"] = 0.0
        else:
            params["min_samples_per_feature"] = 100
    row = {"id": run_id, "method": method, "background_n": len(background), "budget_level": level,
           "repeat": repeat, "params": params, "sample_hash": array_hash(samples),
           "background_hash": array_hash(background), "output_units": "scaled target",
           "test_loaded": False}
    random.seed(rng)
    np.random.seed(rng)
    before_calls, before_rows = predictor.calls, predictor.rows
    started = time.perf_counter()
    err_buffer = io.StringIO()
    with MemoryProbe() as memory, warnings.catch_warnings(record=True) as caught, redirect_stderr(err_buffer):
        warnings.simplefilter("always")
        try:
            init_start = time.perf_counter()
            if method == "PermutationExplainer":
                explainer = shap.PermutationExplainer(
                    predictor, shap.maskers.Independent(background, max_samples=len(background)), seed=rng)
            else:
                explainer = getattr(shap, method)(predictor, background, link="identity")
            row["setup_seconds"] = time.perf_counter() - init_start
            values, bases, durations = [], [], []
            for sample in samples:
                sample_start = time.perf_counter()
                if method == "PermutationExplainer":
                    result = explainer(sample[None, :], max_evals=params["max_evals"],
                                       batch_size=1024, silent=True)
                    value = np.asarray(result.values).reshape(-1)
                    base = float(np.asarray(result.base_values).reshape(-1)[0])
                else:
                    kwargs = {"nsamples": budget, "silent": True}
                    if method == "KernelExplainer":
                        kwargs["l1_reg"] = 0.0
                    else:
                        kwargs["min_samples_per_feature"] = 100
                    value = np.asarray(explainer.shap_values(sample[None, :], **kwargs)).reshape(-1)
                    base = float(np.asarray(explainer.expected_value).reshape(-1)[0])
                durations.append(time.perf_counter() - sample_start)
                values.append(value)
                bases.append(base)
            values = np.asarray(values)
            bases = np.asarray(bases)
            prediction = predictor(samples)
            elemental = np.concatenate([values[:, :24].reshape(-1, 6, 4),
                                        values[:, 24:].reshape(-1, 6, n_b)], axis=2)
            assert values.shape == samples.shape and np.isfinite(values).all()
            assert np.isfinite(bases).all() and np.isfinite(prediction).all()
            np.testing.assert_allclose(elemental.sum((1, 2)), values.sum(1), atol=1e-12)
            residual = np.abs(prediction - bases - values.sum(1))
            expected_background = float(predictor(background).mean())
            assert np.max(np.abs(bases - expected_background)) < 1e-5
            path = ARRAYS / f"{run_id}.npz"
            np.savez_compressed(path, flat_shap=values, elemental_shap=elemental,
                                base_values=bases, prediction=prediction, reconstruction_abs_error=residual)
            row.update(ok=True, shape_flat=list(values.shape), shape_elemental=list(elemental.shape),
                       base_values=bases.tolist(), prediction=prediction.tolist(),
                       expected_background_prediction=expected_background,
                       reconstruction_abs_error=residual.tolist(), reconstruction_max=float(residual.max()),
                       per_sample_seconds=durations, explanation_seconds=sum(durations),
                       seconds_per_sample=float(np.mean(durations)),
                       mean_abs_elemental=np.abs(values).mean(0).tolist(),
                       mean_abs_feature=np.abs(elemental).sum(1).mean(0).tolist(),
                       array_path=str(path.relative_to(ROOT)), array_sha256=sha256(path))
        except Exception as exc:
            row.update(ok=False, exception_type=type(exc).__name__, exception=str(exc))
        row["warnings"] = [{"category": category, "message": message, "count": count}
                           for (category, message), count in Counter(
                               (w.category.__name__, str(w.message)) for w in caught).items()]
    row.update(total_seconds=time.perf_counter() - started, memory=memory.result(),
               model_function_calls=predictor.calls - before_calls,
               model_rows_evaluated=predictor.rows - before_rows, stderr=err_buffer.getvalue())
    return row


def compare_rows(left, right):
    a = np.asarray(left["mean_abs_elemental"])
    b = np.asarray(right["mean_abs_elemental"])
    fa = np.asarray(left["mean_abs_feature"])
    fb = np.asarray(right["mean_abs_feature"])
    with np.load(ROOT / left["array_path"]) as la, np.load(ROOT / right["array_path"]) as rb:
        maximum = float(np.max(np.abs(la["flat_shap"] - rb["flat_shap"])))
    def rho(x, y):
        if np.array_equal(x, y):
            return 1.0
        result = float(spearmanr(x, y).statistic)
        return result if np.isfinite(result) else None
    top_a = set(np.argsort(-fa, kind="stable")[:10].tolist())
    top_b = set(np.argsort(-fb, kind="stable")[:10].tolist())
    return {"left": left["id"], "right": right["id"],
            "elemental_spearman": rho(a, b), "feature_spearman": rho(fa, fb),
            "feature_top10_overlap_fraction": len(top_a & top_b) / 10,
            "importance_relative_l1": float(np.abs(a - b).sum() / max(np.abs(a).sum(), 1e-12)),
            "max_abs_signed_shap_difference": maximum,
            "max_base_difference": float(np.max(np.abs(np.array(left["base_values"]) - right["base_values"])))}


def comparisons(evidence):
    rows = [r for r in evidence["runs"] if r["ok"]]
    pairs = []
    for left in rows:
        if left["repeat"] != "reference":
            continue
        for right in rows:
            if (left["cultivar"], left["model"], left["method"]) != (right["cultivar"], right["model"], right["method"]):
                continue
            same_bg = left["background_n"] == right["background_n"]
            same_budget = left["budget_level"] == right["budget_level"]
            kind = None
            if same_bg and same_budget and right["repeat"] in ("same_seed", "different_seed"):
                kind = right["repeat"]
            elif same_budget and right["repeat"] == "reference" and left["background_n"] < right["background_n"]:
                kind = "background_sensitivity"
            elif same_bg and right["repeat"] == "reference" and left["budget_level"] == "base" and right["budget_level"] == "high":
                kind = "budget_sensitivity"
            if kind:
                pairs.append(dict(kind=kind, cultivar=left["cultivar"], model=left["model"],
                                  method=left["method"], **compare_rows(left, right)))
    evidence["comparisons"] = pairs


def main():
    if OUTPUT.exists():
        raise FileExistsError(f"Preserve existing benchmark evidence: {OUTPUT}")
    ARRAYS.mkdir(exist_ok=True)
    tf.config.threading.set_intra_op_parallelism_threads(1)
    tf.config.threading.set_inter_op_parallelism_threads(1)
    keras.utils.set_random_seed(0)
    tf.config.experimental.enable_op_determinism()
    started = time.perf_counter()
    evidence = {
        "status": "running", "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "scope": "technical TRAIN backgrounds and VAL samples only; no scientific rankings interpreted",
        "test_loaded": False, "training_performed": False, "official_shap_produced": False,
        "technical_model_seed": 0, "val_indices": VAL_INDICES,
        "val_target_dates": ["2024-01-01", "2024-07-01"],
        "background_sizes": [8, 16, 32], "sampling_full_train_n": 84,
        "background_rule": "rint(linspace(0, n_train_sequences-1, N)); chronological zero-based indices",
        "rng_repeats": RNG_REPEATS, "budgets": BUDGETS,
        "planned_checks": {"reconstruction_atol_scaled": 1e-5, "same_seed_shap_atol": 1e-8,
                           "feature_rank_stability_reference": 0.9,
                           "stability_note": "0.9 is a technical descriptive reference, not statistical significance"},
        "environment": {"python": platform.python_version(), "shap": shap.__version__,
                        "tensorflow": tf.__version__, "keras": keras.__version__,
                        "numpy": np.__version__, "scipy": scipy.__version__, "xgboost": xgboost.__version__,
                        "platform": platform.platform(), "processor": platform.processor(),
                        "logical_cpus": psutil.cpu_count(), "ram_gib": psutil.virtual_memory().total / 2**30,
                        "tf_intra_threads": 1, "tf_inter_threads": 1, "predict_chunk_size": 1024,
                        "devices": [str(d) for d in tf.config.list_physical_devices()],
                        "TF_ENABLE_ONEDNN_OPTS": "0", "TF_DETERMINISTIC_OPS": "1"},
        "git_hash": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
        "code_hashes": {}, "frozen_hashes_before": {}, "datasets": {}, "models": [], "runs": [], "tree_runs": [],
        "methodological_notes": [
            "All three neural explainers use marginal masking; overlapping temporal lags can generate off-manifold coalitions.",
            "Additivity is enforced by Kernel and Sampling solvers and telescopes in Permutation; it is not evidence of Monte Carlo convergence.",
            "Sampling implementation supports all TRAIN rows without evaluating each background row per coalition.",
            "Full dataset byte hashes are integrity-only; only the first 102 CSV records are parsed.",
            "Earlier preflight neural loader read the full CSV before filtering; its test_loaded=False claim was too strong. Not reused.",
        ],
    }
    source_paths = [Path(__file__), AUDIT / "auditar_shap_v2_preflight.py",
                    ROOT / "v2_reentrenamiento/src/models/features_gc3_ge.py",
                    ROOT / "v2_reentrenamiento/src/models/dual_lstm_attention.py",
                    ROOT / "v2_reentrenamiento/src/models/features_gc2_xgboost.py",
                    ROOT / "v2_reentrenamiento/src/training/sequences_gc3_ge.py",
                    ROOT / "v2_reentrenamiento/src/training/tabular_gc2_xgboost.py"]
    source_paths += [Path(inspect.getfile(getattr(shap, method))) for method in (*BUDGETS, "TreeExplainer")]
    evidence["code_hashes"] = {str(p): sha256(p) for p in source_paths}
    for cultivar in ("sutil", "dulce"):
        frame, dataset_path = bounded_frame(cultivar)
        dataset_hash = sha256(dataset_path)
        scaler_path = RESULTS / f"scalers/scaler_{cultivar}_v2c.joblib"
        scaler_hash = sha256(scaler_path)
        evidence["datasets"][cultivar] = {"path": str(dataset_path), "sha256": dataset_hash,
            "rows_parsed": 102, "first": "2016-07-01", "last": "2024-12-01", "test_rows_parsed": 0}
        evidence["frozen_hashes_before"].update({str(dataset_path): dataset_hash, str(scaler_path): scaler_hash})
        for name in ("GC3", "GE"):
            spec = build_feature_spec(cultivar, name)
            bundle = _build_gc3_ge_train_val_sequences(frame, spec)
            train = flatten(bundle["train"])
            samples = flatten(bundle["validation"])[VAL_INDICES]
            assert train.shape == (84, 6 * len(spec.all_inputs))
            assert [bundle["validation"]["dates"][i] for i in VAL_INDICES] == evidence["val_target_dates"]
            configs = []
            for seed in range(10):
                base = RESULTS / "official_gc3_ge" / cultivar / name / f"seed_{seed:02d}"
                config_path, meta_path = base / "config.json", base / "metadata.json"
                config = json.loads(config_path.read_text(encoding="utf-8"))
                meta = json.loads(meta_path.read_text(encoding="utf-8"))
                assert config["seed"] == seed and config["official_result"] is True
                assert config["features_a"] == list(spec.rama_a) and config["features_b"] == list(spec.rama_b)
                assert config["training_config"]["lookback"] == 6
                assert meta["dataset_hash"] == dataset_hash and meta["scaler_hash"] == scaler_hash
                for path in (config_path, meta_path, base / "checkpoint_best.keras"):
                    evidence["frozen_hashes_before"][str(path)] = sha256(path)
                configs.append(config)
            model_path = RESULTS / "official_gc3_ge" / cultivar / name / "seed_00/checkpoint_best.keras"
            load_start = time.perf_counter()
            with warnings.catch_warnings(record=True) as load_warnings:
                warnings.simplefilter("always")
                model = keras.models.load_model(model_path, custom_objects={"BahdanauAttention": BahdanauAttention},
                                                compile=False, safe_mode=False)
            weights_hash = [array_hash(w) for w in model.get_weights()]
            predictor = PredictWrapper(model, len(spec.rama_b))
            warm_start = time.perf_counter()
            wrapped = predictor(samples)
            direct = model([samples[:, :24].reshape(-1, 6, 4),
                            samples[:, 24:].reshape(-1, 6, len(spec.rama_b))], training=False).numpy().reshape(-1)
            np.testing.assert_allclose(wrapped, direct, rtol=1e-6, atol=1e-7)
            model_info = {"cultivar": cultivar, "model": name, "loaded_seed": 0,
                "official_seeds_verified": list(range(10)), "checkpoint": str(model_path),
                "features_a": list(spec.rama_a), "features_b": list(spec.rama_b),
                "train_n": 84, "train_target_first": bundle["train"]["dates"][0],
                "train_target_last": bundle["train"]["dates"][-1],
                "warmup_seconds": time.perf_counter() - warm_start,
                "load_and_warmup_seconds": time.perf_counter() - load_start,
                "load_warnings": [str(w.message) for w in load_warnings],
                "wrapper_vs_direct_max_error": float(np.max(np.abs(wrapped - direct))), "backgrounds": []}
            evidence["models"].append(model_info)
            for size in (8, 16, 32, 84):
                indices = evenly_spaced(len(train), size)
                background = train[indices]
                path = ARRAYS / f"{cultivar}_{name}_background_{size}.npz"
                np.savez_compressed(path, background=background, indices=indices, validation=samples)
                model_info["backgrounds"].append({"n": size, "indices": indices.tolist(),
                    "target_dates": [bundle["train"]["dates"][i] for i in indices],
                    "sha256_array": array_hash(background), "path": str(path.relative_to(ROOT)),
                    "file_sha256": sha256(path), "selection_seed": None})
                methods = ["SamplingExplainer"] if size == 84 else list(BUDGETS)
                for method in methods:
                    levels = ["base", "high"] if size == 32 else ["base"]
                    for level in levels:
                        for rng, repeat in RNG_REPEATS:
                            run_id = f"{cultivar}_{name}_{method}_bg{size}_{level}_{repeat}"
                            print(f"START {run_id}", flush=True)
                            row = run_probe(method, predictor, background, samples, len(spec.rama_b),
                                            level, rng, repeat, run_id)
                            row.update(cultivar=cultivar, model=name, model_seed=0)
                            evidence["runs"].append(row)
                            persist(evidence)
                            print(f"DONE {run_id} ok={row['ok']} sec={row['total_seconds']:.2f} "
                                  f"reconstruction={row.get('reconstruction_max')} warnings={len(row['warnings'])}", flush=True)
            assert weights_hash == [array_hash(w) for w in model.get_weights()]
            model_info["weights_unchanged_in_memory"] = True
            del model, predictor
            keras.backend.clear_session()
            gc.collect()
        tree_benchmark(cultivar, frame, evidence)
    comparisons(evidence)
    evidence["frozen_hashes_after"] = {p: sha256(p) for p in evidence["frozen_hashes_before"]}
    assert evidence["frozen_hashes_before"] == evidence["frozen_hashes_after"]
    evidence["frozen_files_unchanged"] = True
    evidence["elapsed_seconds"] = time.perf_counter() - started
    evidence["completed_utc"] = datetime.now(timezone.utc).isoformat()
    evidence["status"] = "completed_technical_benchmark_pending_D52_D55"
    persist(evidence)
    print(json.dumps({"status": evidence["status"], "runs": len(evidence["runs"]),
                      "elapsed_seconds": evidence["elapsed_seconds"]}), flush=True)


def tree_benchmark(cultivar, frame, evidence):
    spec = build_gc2_feature_spec(cultivar)
    bundle = build_gc2_tabular_bundle(frame, spec)
    base = RESULTS / "official_gc2_xgboost" / cultivar / "GC2_XGBoost"
    for seed in range(10):
        path = base / f"seed_{seed:02d}/model.joblib"
        evidence["frozen_hashes_before"][str(path)] = sha256(path)
    model = joblib.load(base / "seed_00/model.joblib")
    assert model.n_features_in_ == 37
    samples = bundle.validation.X[VAL_INDICES]
    for size in (8, 16, 32):
        indices = evenly_spaced(len(bundle.train.X), size)
        background = bundle.train.X[indices]
        for repeat in ("reference", "same_seed"):
            started = time.perf_counter()
            with warnings.catch_warnings(record=True) as caught:
                explainer = shap.TreeExplainer(model, background, feature_perturbation="interventional", model_output="raw")
                setup = time.perf_counter() - started
                before = time.perf_counter()
                values = np.asarray(explainer.shap_values(samples))
                seconds = time.perf_counter() - before
            pred = model.predict(samples)
            base_value = float(explainer.expected_value)
            residual = np.abs(pred - base_value - values.sum(1))
            path = ARRAYS / f"{cultivar}_TreeExplainer_bg{size}_{repeat}.npz"
            np.savez_compressed(path, shap_values=values, base_value=base_value, prediction=pred,
                                background=background, background_indices=indices)
            evidence["tree_runs"].append({"cultivar": cultivar, "model_seed": 0, "background_n": size,
                "background_indices": indices.tolist(), "background_hash": array_hash(background),
                "repeat": repeat, "shape": list(values.shape), "base_value": base_value,
                "prediction": pred.tolist(), "reconstruction_max": float(residual.max()),
                "setup_seconds": setup, "explanation_seconds": seconds, "seconds_per_sample": seconds / len(samples),
                "total_seconds": time.perf_counter() - started, "warnings": [str(w.message) for w in caught],
                "array_path": str(path.relative_to(ROOT)), "array_sha256": sha256(path), "test_loaded": False})
    persist(evidence)


if __name__ == "__main__":
    main()
