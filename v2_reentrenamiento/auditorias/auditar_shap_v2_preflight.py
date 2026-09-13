"""Methodological audit and small technical pre-flight for SHAP v2.

This script does not train models, does not modify checkpoints and does not
compute official TEST SHAP results. It only checks frozen artifacts and runs tiny
TRAIN/VAL explainer compatibility probes.
"""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

os.environ.setdefault("TF_ENABLE_ONEDNN_OPTS", "0")
os.environ.setdefault("TF_DETERMINISTIC_OPS", "1")
os.environ.setdefault("PYTHONHASHSEED", "0")

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import joblib
import numpy as np
import pandas as pd
import shap
import tensorflow as tf
import xgboost as xgb
from tensorflow import keras

from v2_reentrenamiento.src.models.dual_lstm_attention import BahdanauAttention
from v2_reentrenamiento.src.models.features_gc2_xgboost import build_gc2_feature_spec
from v2_reentrenamiento.src.models.features_gc3_ge import (
    INDECI_BASE,
    NASA_BASE,
    NLP_LAGGED,
    TEMPORAL_FEATURES,
    build_feature_spec,
)
from v2_reentrenamiento.src.training.tabular_gc2_xgboost import (
    build_gc2_tabular_bundle,
    load_train_val_dataframe,
)


AUDIT_MD = REPO_ROOT / "v2_reentrenamiento/auditorias/AUDITORIA_METODOLOGICA_SHAP_V2.md"
PREFLIGHT_MD = REPO_ROOT / "v2_reentrenamiento/auditorias/AUDITORIA_PREFLIGHT_SHAP_V2.md"
PREFLIGHT_JSON = REPO_ROOT / "v2_reentrenamiento/auditorias/shap_v2_preflight.json"

GC3_GE_DIR = REPO_ROOT / "v2_reentrenamiento/resultados_v2_final/official_gc3_ge"
XGB_DIR = REPO_ROOT / "v2_reentrenamiento/resultados_v2_final/official_gc2_xgboost"
DATA_DIR = REPO_ROOT / "v2_reentrenamiento/data/processed"

CULTIVARS = ("sutil", "dulce")
NEURAL_MODELS = ("GC3", "GE")
SEEDS = tuple(range(10))
TEST_YEARS_FORBIDDEN = {2025}
BACKGROUND_N = 8
EXPLAIN_N = 2
TECHNICAL_SENTINEL_SEED = 0


def main() -> None:
    np.random.seed(0)
    tf.keras.utils.set_random_seed(0)

    started = datetime.now(timezone.utc)
    evidence: dict[str, Any] = {
        "generated_utc": started.isoformat(),
        "environment": _environment(),
        "git_status_sb": _git_status(),
        "git_commit_current": _git_commit(),
        "official_artifacts": {},
        "feature_audit": {},
        "historical_shap_v1": _historical_shap_evidence(),
        "preflight": {
            "scope": "tiny TRAIN/VAL compatibility probes only",
            "technical_sentinel_seed": TECHNICAL_SENTINEL_SEED,
            "background_n": BACKGROUND_N,
            "explain_n": EXPLAIN_N,
            "test_loaded": False,
            "neural": [],
            "xgboost": [],
        },
    }

    _audit_official_artifacts(evidence)
    _audit_features(evidence)
    _run_neural_preflight(evidence)
    _run_xgb_preflight(evidence)
    _write_documents(evidence)
    PREFLIGHT_JSON.write_text(json.dumps(evidence, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps({"status": "completed", "preflight_json": str(PREFLIGHT_JSON)}, indent=2))


def _environment() -> dict[str, str]:
    return {
        "python": os.sys.version.split()[0],
        "shap": shap.__version__,
        "tensorflow": tf.__version__,
        "keras": keras.__version__,
        "xgboost": xgb.__version__,
        "tf_enable_onednn_opts": os.environ.get("TF_ENABLE_ONEDNN_OPTS", ""),
        "tf_deterministic_ops": os.environ.get("TF_DETERMINISTIC_OPS", ""),
    }


def _audit_official_artifacts(evidence: dict[str, Any]) -> None:
    neural: dict[str, Any] = {}
    for cultivar in CULTIVARS:
        for model_name in NEURAL_MODELS:
            key = f"{cultivar}_{model_name}"
            paths = []
            metadata = []
            for seed in SEEDS:
                base = GC3_GE_DIR / cultivar / model_name / f"seed_{seed:02d}"
                checkpoint = base / "checkpoint_best.keras"
                meta_path = base / "metadata.json"
                config_path = base / "config.json"
                if not checkpoint.exists() or not meta_path.exists() or not config_path.exists():
                    raise FileNotFoundError(f"Missing official neural artifact: {base}")
                meta = json.loads(meta_path.read_text(encoding="utf-8"))
                metadata.append(meta)
                paths.append(
                    {
                        "seed": seed,
                        "checkpoint": str(checkpoint),
                        "metadata": str(meta_path),
                        "config": str(config_path),
                        "checkpoint_hash": _sha256(checkpoint),
                        "metadata_hash": _sha256(meta_path),
                        "config_hash": _sha256(config_path),
                    }
                )
            neural[key] = {
                "seeds": [m["seed"] for m in metadata],
                "all_10_seeds_present": [m["seed"] for m in metadata] == list(SEEDS),
                "dataset_hashes": sorted({m["dataset_hash"] for m in metadata}),
                "scaler_hashes": sorted({m["scaler_hash"] for m in metadata}),
                "tensorflow_versions": sorted({m["tensorflow_version"] for m in metadata}),
                "keras_versions": sorted({m["keras_version"] for m in metadata}),
                "paths": paths,
            }

    xgboost_artifacts: dict[str, Any] = {}
    for cultivar in CULTIVARS:
        paths = []
        configs = []
        for seed in SEEDS:
            base = XGB_DIR / cultivar / "GC2_XGBoost" / f"seed_{seed:02d}"
            model_path = base / "model.joblib"
            booster_path = base / "model.xgboost.json"
            meta_path = base / "metadata.json"
            config_path = base / "config.json"
            if not model_path.exists() or not booster_path.exists() or not meta_path.exists() or not config_path.exists():
                raise FileNotFoundError(f"Missing official XGBoost artifact: {base}")
            config = json.loads(config_path.read_text(encoding="utf-8"))
            configs.append(config)
            paths.append(
                {
                    "seed": seed,
                    "model_joblib": str(model_path),
                    "model_xgboost_json": str(booster_path),
                    "metadata": str(meta_path),
                    "config": str(config_path),
                    "model_hash": _sha256(model_path),
                    "booster_hash": _sha256(booster_path),
                    "metadata_hash": _sha256(meta_path),
                    "config_hash": _sha256(config_path),
                }
            )
        xgboost_artifacts[cultivar] = {
            "seeds": [c["seed"] for c in configs],
            "all_10_seeds_present": [c["seed"] for c in configs] == list(SEEDS),
            "feature_counts": sorted({len(c["features"]) for c in configs}),
            "params": configs[0]["xgb_params"],
            "paths": paths,
        }

    evidence["official_artifacts"] = {"GC3_GE": neural, "XGBoost": xgboost_artifacts}


def _audit_features(evidence: dict[str, Any]) -> None:
    groups = {
        "produccion": "Rama A completa: produccion contemporanea al origen y lags 1/3/6 dentro de la ventana",
        "temporalidad": list(TEMPORAL_FEATURES),
        "NASA": [f"{name}_lag{lag}" for name in NASA_BASE for lag in (1, 3, 6)],
        "INDECI": [f"{name}_lag{lag}" for name in INDECI_BASE for lag in (1, 3, 6)],
        "NLP": list(NLP_LAGGED),
    }
    for cultivar in CULTIVARS:
        for model_name in NEURAL_MODELS:
            spec = build_feature_spec(cultivar, model_name)
            evidence["feature_audit"][f"{cultivar}_{model_name}"] = {
                "rama_a_n": len(spec.rama_a),
                "rama_b_n": len(spec.rama_b),
                "total_inputs": len(spec.all_inputs),
                "rama_a": list(spec.rama_a),
                "rama_b": list(spec.rama_b),
                "target": spec.target,
                "lookback": 6,
            }
        xgb_spec = build_gc2_feature_spec(cultivar)
        evidence["feature_audit"][f"{cultivar}_XGBoost"] = {
            "predictors_n": len(xgb_spec.predictors),
            "predictors": list(xgb_spec.predictors),
            "target": xgb_spec.target,
        }
    evidence["feature_groups_recommended"] = groups


def _run_neural_preflight(evidence: dict[str, Any]) -> None:
    for cultivar in CULTIVARS:
        frame = _load_gc3_ge_train_val_frame(cultivar)
        for model_name in NEURAL_MODELS:
            spec = build_feature_spec(cultivar, model_name)
            bundle = _build_gc3_ge_train_val_sequences(frame, spec)
            base = GC3_GE_DIR / cultivar / model_name / f"seed_{TECHNICAL_SENTINEL_SEED:02d}"
            model = keras.models.load_model(
                base / "checkpoint_best.keras",
                custom_objects={"BahdanauAttention": BahdanauAttention},
                compile=False,
                safe_mode=False,
            )
            bg = [bundle["train"]["X_a"][:BACKGROUND_N].astype(np.float32), bundle["train"]["X_b"][:BACKGROUND_N].astype(np.float32)]
            samples = [bundle["validation"]["X_a"][:EXPLAIN_N].astype(np.float32), bundle["validation"]["X_b"][:EXPLAIN_N].astype(np.float32)]
            row = {
                "cultivar": cultivar,
                "model": model_name,
                "seed": TECHNICAL_SENTINEL_SEED,
                "train_shapes": {
                    "X_a": list(bundle["train"]["X_a"].shape),
                    "X_b": list(bundle["train"]["X_b"].shape),
                    "y": list(bundle["train"]["y"].shape),
                },
                "validation_shapes": {
                    "X_a": list(bundle["validation"]["X_a"].shape),
                    "X_b": list(bundle["validation"]["X_b"].shape),
                    "y": list(bundle["validation"]["y"].shape),
                },
                "train_target_first": bundle["train"]["dates"][0],
                "train_target_last": bundle["train"]["dates"][-1],
                "validation_target_first": bundle["validation"]["dates"][0],
                "validation_target_last": bundle["validation"]["dates"][-1],
                "test_loaded": False,
                "model_input_shapes": [list(shape) for shape in model.input_shape],
                "model_output_shape": list(model.output_shape),
                "deep_explainer": _try_neural_explainer("DeepExplainer", model, bg, samples),
                "gradient_explainer": _try_neural_explainer("GradientExplainer", model, bg, samples),
                "kernel_explainer": _try_kernel_explainer(
                    model,
                    bg,
                    samples,
                    n_features_a=len(spec.rama_a),
                    n_features_b=len(spec.rama_b),
                ),
            }
            evidence["preflight"]["neural"].append(row)


def _try_neural_explainer(name: str, model: keras.Model, background: list[np.ndarray], samples: list[np.ndarray]) -> dict[str, Any]:
    started = time.perf_counter()
    out: dict[str, Any] = {"name": name}
    try:
        explainer_cls = getattr(shap, name)
        explainer = explainer_cls(model, background)
        values = explainer.shap_values(samples)
        normalized = _normalize_shap_values(values)
        pred = model.predict(samples, verbose=0).reshape(-1)
        out.update(
            {
                "ok": True,
                "elapsed_seconds": round(time.perf_counter() - started, 3),
                "raw_type": type(values).__name__,
                "normalized_shapes": [[list(arr.shape) for arr in per_output] for per_output in normalized],
                "prediction_shape": list(pred.shape),
                "expected_value_available": hasattr(explainer, "expected_value"),
                "notes": "compatibility probe only; no official SHAP artifact generated",
            }
        )
    except Exception as exc:
        out.update(
            {
                "ok": False,
                "elapsed_seconds": round(time.perf_counter() - started, 3),
                "error_type": type(exc).__name__,
                "error": str(exc)[:1000],
            }
        )
    return out


def _try_kernel_explainer(
    model: keras.Model,
    background: list[np.ndarray],
    samples: list[np.ndarray],
    *,
    n_features_a: int,
    n_features_b: int,
) -> dict[str, Any]:
    started = time.perf_counter()
    lookback = 6
    n_a_flat = lookback * n_features_a
    bg_flat = np.hstack(
        [
            background[0].reshape(background[0].shape[0], -1),
            background[1].reshape(background[1].shape[0], -1),
        ]
    )
    sample_flat = np.hstack(
        [
            samples[0].reshape(samples[0].shape[0], -1),
            samples[1].reshape(samples[1].shape[0], -1),
        ]
    )

    def predict_fn(x_flat: np.ndarray) -> np.ndarray:
        x_flat = np.asarray(x_flat, dtype=np.float32)
        n = x_flat.shape[0]
        xa = x_flat[:, :n_a_flat].reshape(n, lookback, n_features_a)
        xb = x_flat[:, n_a_flat:].reshape(n, lookback, n_features_b)
        return model.predict([xa, xb], verbose=0).reshape(-1)

    try:
        explainer = shap.KernelExplainer(predict_fn, bg_flat)
        values = explainer.shap_values(sample_flat, nsamples=20, silent=True)
        values_arr = np.asarray(values)
        pred = predict_fn(sample_flat)
        expected = np.asarray(explainer.expected_value).reshape(-1)[0]
        reconstructed = values_arr.reshape(values_arr.shape[0], -1).sum(axis=1) + float(expected)
        return {
            "ok": True,
            "elapsed_seconds": round(time.perf_counter() - started, 3),
            "background_flat_shape": list(bg_flat.shape),
            "sample_flat_shape": list(sample_flat.shape),
            "shap_shape": list(values_arr.shape),
            "expected_value": float(expected),
            "max_additivity_abs_error": float(np.max(np.abs(reconstructed - pred))),
            "notes": "model-agnostic flatten/unflatten compatibility probe; nsamples=20 only",
        }
    except Exception as exc:
        return {
            "ok": False,
            "elapsed_seconds": round(time.perf_counter() - started, 3),
            "error_type": type(exc).__name__,
            "error": str(exc)[:1000],
        }


def _run_xgb_preflight(evidence: dict[str, Any]) -> None:
    for cultivar in CULTIVARS:
        spec = build_gc2_feature_spec(cultivar)
        frame = load_train_val_dataframe(DATA_DIR / f"master_dataset_{cultivar}_v2_escalado.csv")
        bundle = build_gc2_tabular_bundle(frame, spec)
        model_path = XGB_DIR / cultivar / "GC2_XGBoost" / f"seed_{TECHNICAL_SENTINEL_SEED:02d}" / "model.joblib"
        model = joblib.load(model_path)
        background = bundle.train.X[:BACKGROUND_N]
        samples = bundle.validation.X[:EXPLAIN_N]
        started = time.perf_counter()
        row: dict[str, Any] = {
            "cultivar": cultivar,
            "model": "XGBoost",
            "seed": TECHNICAL_SENTINEL_SEED,
            "train_shape": list(bundle.train.X.shape),
            "validation_shape": list(bundle.validation.X.shape),
            "validation_target_first": str(bundle.validation.target_dates.iloc[0].date()),
            "validation_target_last": str(bundle.validation.target_dates.iloc[-1].date()),
            "test_loaded": bool(bundle.test_loaded),
            "features_n": len(spec.predictors),
        }
        try:
            explainer = shap.TreeExplainer(model, data=background, feature_names=list(spec.predictors))
            values = explainer.shap_values(samples)
            expected = explainer.expected_value
            pred = np.asarray(model.predict(samples)).reshape(-1)
            reconstructed = np.asarray(values).sum(axis=1) + float(np.asarray(expected).reshape(-1)[0])
            row.update(
                {
                    "tree_explainer": {
                        "ok": True,
                        "elapsed_seconds": round(time.perf_counter() - started, 3),
                        "shap_shape": list(np.asarray(values).shape),
                        "expected_value": float(np.asarray(expected).reshape(-1)[0]),
                        "max_additivity_abs_error": float(np.max(np.abs(reconstructed - pred))),
                        "output_units": "scaled target units, because the frozen XGBoost model was trained on scaled v2 data",
                    }
                }
            )
        except Exception as exc:
            row.update(
                {
                    "tree_explainer": {
                        "ok": False,
                        "elapsed_seconds": round(time.perf_counter() - started, 3),
                        "error_type": type(exc).__name__,
                        "error": str(exc)[:1000],
                    }
                }
            )
        evidence["preflight"]["xgboost"].append(row)


def _normalize_shap_values(values: Any) -> list[list[np.ndarray]]:
    """Return list per output, each containing one array per model input."""

    if isinstance(values, list):
        if values and isinstance(values[0], list):
            return [[np.asarray(item) for item in output] for output in values]
        if len(values) == 2 and all(hasattr(item, "shape") for item in values):
            return [[np.asarray(item) for item in values]]
        return [[np.asarray(item)] for item in values]
    arr = np.asarray(values)
    return [[arr]]


def _load_gc3_ge_train_val_frame(cultivar: str) -> pd.DataFrame:
    path = DATA_DIR / f"master_dataset_{cultivar}_v2_escalado.csv"
    df = pd.read_csv(path)
    df["fecha"] = pd.to_datetime({"year": df["año"].astype(int), "month": df["mes"].astype(int), "day": 1})
    df = df[df["fecha"].dt.year <= 2024].copy().sort_values("fecha").reset_index(drop=True)
    if df["fecha"].min() != pd.Timestamp("2016-07-01") or df["fecha"].max() != pd.Timestamp("2024-12-01"):
        raise ValueError(f"Unexpected TRAIN+VAL frame range for {cultivar}: {df['fecha'].min()} .. {df['fecha'].max()}")
    if (df["fecha"].dt.year >= 2025).any():
        raise ValueError("TEST rows reached SHAP neural pre-flight.")
    return df


def _build_gc3_ge_train_val_sequences(df: pd.DataFrame, spec: Any) -> dict[str, Any]:
    X_a, X_b, y, dates = [], [], [], []
    values_a = df.loc[:, spec.rama_a].to_numpy(dtype=float)
    values_b = df.loc[:, spec.rama_b].to_numpy(dtype=float)
    values_y = df.loc[:, spec.target].to_numpy(dtype=float)
    all_dates = df["fecha"].reset_index(drop=True)
    for target_idx in range(6, len(df)):
        X_a.append(values_a[target_idx - 6 : target_idx])
        X_b.append(values_b[target_idx - 6 : target_idx])
        y.append(float(values_y[target_idx]))
        dates.append(all_dates.iloc[target_idx])
    X_a_arr = np.stack(X_a)
    X_b_arr = np.stack(X_b)
    y_arr = np.asarray(y)
    date_series = pd.Series(dates)
    train_mask = date_series.dt.year <= 2023
    val_mask = date_series.dt.year == 2024
    if int(train_mask.sum()) != 84 or int(val_mask.sum()) != 12:
        raise ValueError("Unexpected neural TRAIN/VAL sequence counts.")
    return {
        "train": {
            "X_a": X_a_arr[train_mask.to_numpy()],
            "X_b": X_b_arr[train_mask.to_numpy()],
            "y": y_arr[train_mask.to_numpy()],
            "dates": [d.strftime("%Y-%m-%d") for d in date_series[train_mask]],
        },
        "validation": {
            "X_a": X_a_arr[val_mask.to_numpy()],
            "X_b": X_b_arr[val_mask.to_numpy()],
            "y": y_arr[val_mask.to_numpy()],
            "dates": [d.strftime("%Y-%m-%d") for d in date_series[val_mask]],
        },
    }


def _historical_shap_evidence() -> dict[str, Any]:
    evidence: dict[str, Any] = {
        "verified_code": [],
        "historical_v1": [],
    }
    candidates = [
        REPO_ROOT / "gen_nb_actividad16.py",
        REPO_ROOT / "notebooks/fase4/actividad_16_shap.ipynb",
        REPO_ROOT / "notebooks/fase4/actividad_16_ejecutado.ipynb",
        REPO_ROOT / "generar_shap_shocks.py",
        REPO_ROOT / "visualizar_shocks_comparativo.py",
        REPO_ROOT / "docs/shap.html",
        REPO_ROOT / "dashboard/shap.html",
    ]
    for path in candidates:
        if not path.exists():
            continue
        text = path.read_text(encoding="utf-8", errors="ignore")
        hits = []
        for token in ("KernelExplainer", "DeepExplainer", "GradientExplainer", "TreeExplainer", "PermutationExplainer", "shap.kmeans", "shap_values"):
            if token.lower() in text.lower():
                hits.append(token)
        if hits:
            evidence["verified_code"].append({"path": str(path), "hits": sorted(set(hits)), "sha256": _sha256(path)})
    evidence["historical_v1"].append(
        "actividad_16_shap.ipynb/gen_nb_actividad16.py documented KernelExplainer with k-means background for a v1 GE-style dual-input wrapper; this is antecedent only and is not automatically valid for v2."
    )
    evidence["historical_v1"].append(
        "generar_shap_shocks.py and visualizar_shocks_comparativo.py consume resultados/shap/shap_resultados.json for legacy dashboard figures; they do not implement v2 SHAP."
    )
    return evidence


def _write_documents(evidence: dict[str, Any]) -> None:
    decisions = [
        ("D51", "modelos incluidos en SHAP", "GC3/GE/XGBoost vs solo GC3/GE", "Incluir GC3, GE y XGBoost; excluir Naive/SARIMA de SHAP", "GC3/GE son el foco arquitectonico y XGBoost aporta TreeSHAP complementario; Naive/SARIMA no requieren SHAP forzado."),
        ("D52", "explainer GC3/GE", "DeepExplainer, GradientExplainer, KernelExplainer, PermutationExplainer", "Usar KernelExplainer con wrapper flatten/unflatten si el pre-flight lo valida; mantener PermutationExplainer como fallback", "DeepExplainer y GradientExplainer no deben usarse si fallan con Keras 3/TF 2.21/capa custom; no modificar modelos para explicar."),
        ("D53", "explainer XGBoost", "TreeExplainer, PermutationExplainer", "TreeExplainer", "Es el metodo nativo para arboles; explica el modelo escalado tal como fue entrenado."),
        ("D54", "estrategia multi-seed", "10 seeds, seed mediana, ensemble/media", "Explicar las 10 seeds y resumir mean/median/SD/min/max de atribuciones", "Evita best seed y preserva sensibilidad a estocasticidad."),
        ("D55", "background", "TRAIN completo, muestra TRAIN, k-means TRAIN", "Background exclusivamente TRAIN; muestra deterministica o resumen TRAIN si el coste lo exige", "No usa TEST ni VAL para definir baseline; reproducible y compatible con coste."),
        ("D56", "agregacion temporal", "elemental, feature, lag, grupo", "Guardar elemental lag x feature y derivar feature/lag/grupo con mean absolute SHAP", "No colapsa dimensiones sin trazabilidad."),
        ("D57", "agrupacion de variables", "manual vs constantes oficiales", "Usar constantes oficiales de feature builders", "Evita nombres hardcodeados divergentes."),
        ("D58", "analisis shock", "global, shock, nonshock, locales", "mean absolute SHAP shock/nonshock y explicaciones locales de los 3 shocks", "Descriptivo con n_shock=3; sin causalidad ni significancia fuerte."),
        ("D59", "tratamiento NLP", "GE total, ranking NLP, participacion relativa", "Calcular aporte absoluto y relativo de las 6 NLP lagged en GE", "Aisla el bloque agregado en GE sin NLP contemporaneo."),
        ("D60", "attention complementaria", "ignorar, guardar pesos, sustituir SHAP", "Guardar attention solo como diagnostico separado si es tecnicamente estable", "Attention no equivale a explicacion causal ni reemplaza SHAP."),
        ("D61", "reproducibilidad/artefactos", "npy/csv/json/parquet", "Guardar arrays, expected values, nombres, posiciones, background metadata, hashes y entorno", "Permite auditoria y repeticion sin reentrenar."),
    ]

    audit_lines = [
        "# AUDITORIA METODOLOGICA SHAP V2",
        "",
        "Estado: PENDIENTE DE APROBACION METODOLOGICA.",
        "",
        "## Alcance",
        "",
        "Esta auditoria disena explicabilidad post-hoc para modelos v2 cerrados. No entrena, no reentrena, no modifica pesos/checkpoints, no cambia features, no cambia scalers, no hace HPO/CV y no selecciona seeds por desempeno.",
        "",
        "SHAP se interpretara como atribucion del modelo a sus predicciones, no como causalidad de las variables sobre la produccion.",
        "",
        "## Hechos Verificados En Codigo",
        "",
        f"- Entorno: shap {evidence['environment']['shap']}; TensorFlow {evidence['environment']['tensorflow']}; Keras {evidence['environment']['keras']}; XGBoost {evidence['environment']['xgboost']}.",
        "- GC3/GE: arquitectura Dual-LSTM + Bahdanau Attention en `v2_reentrenamiento/src/models/dual_lstm_attention.py`; dos inputs `rama_a` y `rama_b`; lookback=6; capa custom `BahdanauAttention`.",
        "- Features GC3/GE: `features_gc3_ge.py` define rama A con 4 variables de produccion; rama B GC3=33; rama B GE=39.",
        "- NLP GE: exactamente `avg_sentiment_lag1`, `avg_sentiment_lag3`, `avg_sentiment_lag6`, `n_noticias_lag1`, `n_noticias_lag3`, `n_noticias_lag6`.",
        "- XGBoost: `features_gc2_xgboost.py` define 37 predictores no NLP y `tabular_gc2_xgboost.py` construye X_t -> y_(t+1).",
        "- Artefactos oficiales: 10 seeds completos para GC3, GE y XGBoost en Sutil y Dulce.",
        "",
        "## Antecedente Historico v1",
        "",
        "- `notebooks/fase4/actividad_16_shap.ipynb` y `gen_nb_actividad16.py` documentan `KernelExplainer` con `shap.kmeans(X_train, 10)` sobre un wrapper dual-input v1. Es antecedente, no protocolo v2 aprobado.",
        "- `generar_shap_shocks.py` y `visualizar_shocks_comparativo.py` consumen resultados SHAP legacy para figuras/dashboard; no constituyen implementacion SHAP v2.",
        "",
        "## Comparacion De Explainers",
        "",
        "- GC3/GE: `DeepExplainer` debe considerarse solo si pasa compatibilidad real con TF 2.21/Keras 3.14/capa custom/dos inputs; si falla o emite limitaciones, no se modifica el modelo.",
        "- GC3/GE: `DeepExplainer` y `GradientExplainer` deben descartarse para protocolo oficial si el pre-flight falla con `StagingError` en TF 2.21/Keras 3.14/capa custom.",
        "- GC3/GE: `KernelExplainer` con wrapper flatten/unflatten es la recomendacion si el pre-flight pequeno valida shapes y aditividad; `PermutationExplainer` queda como fallback model-agnostico.",
        "- XGBoost: `TreeExplainer` es el explainer recomendado; las atribuciones quedan en unidades del target escalado porque el modelo fue entrenado con datos v2 escalados.",
        "",
        "## Background",
        "",
        "Background recomendado: exclusivamente TRAIN. Si el coste lo permite, usar TRAIN completo; si no, usar muestra deterministica de TRAIN o resumen k-means de TRAIN. No usar TEST para baseline. VAL tampoco es necesaria para definir baseline.",
        "",
        "## Agregacion Temporal",
        "",
        "Guardar importancia elemental `lag x feature`. Derivar importancia por feature mediante suma o media sobre los 6 lags con operacion documentada; ranking global por mean absolute SHAP. Derivar importancia por lag sumando/media sobre features. Derivar grupos: produccion, temporalidad, NASA, INDECI, NLP.",
        "",
        "## Shocks",
        "",
        "Analisis futuro recomendado: mean absolute SHAP shock, mean absolute SHAP nonshock, diferencia descriptiva, ranking por grupo y explicaciones locales de los 3 meses shock por cultivar. Con n_shock=3 no usar pruebas fuertes de significancia, causalidad ni lenguaje de variable responsable.",
        "",
        "## Attention",
        "",
        "Los pesos de atencion pueden guardarse como diagnostico interno separado. No sustituyen SHAP y no deben interpretarse como explicacion causal.",
        "",
        "## Tabla De Decisiones",
        "",
        "| ID | Decision | Alternativas | Recomendacion | Justificacion | Estado |",
        "|---|---|---|---|---|---|",
    ]
    for row in decisions:
        audit_lines.append(f"| {row[0]} | {row[1]} | {row[2]} | {row[3]} | {row[4]} | PENDIENTE DE APROBACION METODOLOGICA |")
    audit_lines.extend(
        [
            "",
            "## Recomendacion Concreta",
            "",
            "Protocolo recomendado: explicar GC3, GE y XGBoost en las 10 seeds oficiales; usar background TRAIN exclusivamente; para GC3/GE usar `KernelExplainer` con wrapper flatten/unflatten si queda validado por pre-flight y mantener `PermutationExplainer` como fallback; descartar `DeepExplainer`/`GradientExplainer` si fallan con la pila actual; para XGBoost usar `TreeExplainer`; guardar SHAP elemental, agregados por feature/lag/grupo, analisis descriptivo shock/nonshock y metadatos completos. Las decisiones D51-D61 permanecen pendientes de aprobacion metodologica.",
            "",
        ]
    )
    AUDIT_MD.write_text("\n".join(audit_lines), encoding="utf-8")

    neural_rows = []
    for row in evidence["preflight"]["neural"]:
        neural_rows.append(
            f"| {row['cultivar']} | {row['model']} | {row['seed']} | {row['deep_explainer']['ok']} | {row['gradient_explainer']['ok']} | {row['kernel_explainer']['ok']} | {row['train_shapes']['X_a']} / {row['train_shapes']['X_b']} | {row['validation_shapes']['X_a']} / {row['validation_shapes']['X_b']} |"
        )
    xgb_rows = []
    for row in evidence["preflight"]["xgboost"]:
        tree = row["tree_explainer"]
        xgb_rows.append(
            f"| {row['cultivar']} | {row['seed']} | {tree['ok']} | {row['train_shape']} | {row['validation_shape']} | {tree.get('shap_shape')} | {tree.get('max_additivity_abs_error')} |"
        )
    preflight_lines = [
        "# AUDITORIA PREFLIGHT SHAP V2",
        "",
        "Estado: COMPLETADA COMO PRUEBA TECNICA TRAIN/VAL. No genera resultados SHAP oficiales.",
        "",
        "## Restricciones Verificadas",
        "",
        f"- TEST cargado: {evidence['preflight']['test_loaded']}.",
        f"- Seed tecnico usado: {TECHNICAL_SENTINEL_SEED}; no es seleccion por desempeno ni seed oficial de interpretacion.",
        f"- Background usado en prueba: primeros {BACKGROUND_N} ejemplos TRAIN.",
        f"- Muestras explicadas en prueba: primeros {EXPLAIN_N} ejemplos VAL.",
        "- No entrenamiento, no reentrenamiento, no HPO, no CV, no modificacion de checkpoints.",
        "",
        "## GC3/GE",
        "",
        "| cultivar | modelo | seed tecnico | DeepExplainer ok | GradientExplainer ok | KernelExplainer ok | train X_a/X_b | VAL X_a/X_b |",
        "|---|---|---:|---|---|---|---|---|",
        *neural_rows,
        "",
        "## XGBoost",
        "",
        "| cultivar | seed tecnico | TreeExplainer ok | train shape | VAL shape | SHAP shape | max additivity abs error |",
        "|---|---:|---|---|---|---|---:|",
        *xgb_rows,
        "",
        "## Archivo JSON",
        "",
        f"- `{PREFLIGHT_JSON}`",
        "",
    ]
    PREFLIGHT_MD.write_text("\n".join(preflight_lines), encoding="utf-8")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _git_status() -> str:
    try:
        return subprocess.check_output(["git", "status", "-sb"], cwd=REPO_ROOT, text=True, stderr=subprocess.STDOUT).strip()
    except Exception as exc:
        return f"ERROR: {exc}"


def _git_commit() -> str:
    try:
        return subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=REPO_ROOT, text=True, stderr=subprocess.STDOUT).strip()
    except Exception as exc:
        return f"ERROR: {exc}"


if __name__ == "__main__":
    main()
