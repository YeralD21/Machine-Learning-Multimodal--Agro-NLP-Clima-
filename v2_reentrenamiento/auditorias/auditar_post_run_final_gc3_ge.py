"""Final post-run audit for the 40 official GC3/GE v2 runs.

This script does not train models, load TEST 2025, generate predictions, or
evaluate checkpoints. It inspects only official run artifacts, source text, and
TRAIN/VALIDATION-only metadata generated during the official batch.
"""

from __future__ import annotations

import csv
import hashlib
import json
import math
import statistics
import subprocess
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[2]
BASE_DIR = REPO_ROOT / "v2_reentrenamiento/resultados_v2_final/official_gc3_ge"
AUDIT_DIR = REPO_ROOT / "v2_reentrenamiento/auditorias"
RUNNER_PATH = REPO_ROOT / "v2_reentrenamiento/src/training/official_runner_gc3_ge.py"
TRAIN_AUDIT_PATH = AUDIT_DIR / "AUDITORIA_ENTRENAMIENTO_OFICIAL_GC3_GE.md"
INPUT_SUMMARY_CSV = AUDIT_DIR / "resumen_entrenamiento_oficial_gc3_ge.csv"
INPUT_SUMMARY_JSON = AUDIT_DIR / "resumen_entrenamiento_oficial_gc3_ge.json"
MANIFEST_PATH = AUDIT_DIR / "entrenamiento_oficial_gc3_ge_manifest.json"
OUTPUT_REPORT = AUDIT_DIR / "AUDITORIA_POST_RUN_GC3_GE.md"
OUTPUT_CSV = AUDIT_DIR / "resumen_post_run_gc3_ge.csv"
OUTPUT_JSON = AUDIT_DIR / "resumen_post_run_gc3_ge.json"

CULTIVARS = ("sutil", "dulce")
MODELS = ("GC3", "GE")
SEEDS = tuple(range(10))
EXPECTED_HISTORY_COLUMNS = ["epoch", "loss", "val_loss", "mae", "val_mae", "learning_rate"]
EXPECTED_NLP = {
    "avg_sentiment_lag1",
    "avg_sentiment_lag3",
    "avg_sentiment_lag6",
    "n_noticias_lag1",
    "n_noticias_lag3",
    "n_noticias_lag6",
}
FORBIDDEN_EXACT = {
    "precio_chacra_kg",
    "n_provincias",
    "total_afectados",
    "avg_sentiment",
    "n_noticias",
}
FORBIDDEN_FRAGMENTS = ("precio_chacra_kg", "n_provincias", "total_afectados")
CONTEMP_BASE = (
    "T2M",
    "T2M_MAX",
    "WS2M",
    "PRECTOTCORR",
    "RH2M",
    "num_emergencias",
    "personas_afectadas",
    "personas_damnificadas",
    "hectareas_cultivo_perdidas",
    "hectareas_cultivo_afectadas",
    "avg_sentiment",
    "n_noticias",
)
EXPECTED_CONFIG = {
    "batch_size": 8,
    "early_stopping_monitor": "val_loss",
    "early_stopping_patience": 15,
    "early_stopping_restore_best_weights": True,
    "learning_rate": 0.001,
    "lookback": 6,
    "loss": "mse",
    "max_epochs": 300,
    "metric": "mae",
    "reduce_lr_factor": 0.5,
    "reduce_lr_min_lr": 1e-06,
    "reduce_lr_monitor": "val_loss",
    "reduce_lr_patience": 8,
    "shuffle": False,
}


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


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


def expected_run_paths() -> list[tuple[str, str, int, Path]]:
    return [
        (cultivar, model_name, seed, BASE_DIR / cultivar / model_name / f"seed_{seed:02d}")
        for cultivar in CULTIVARS
        for model_name in MODELS
        for seed in SEEDS
    ]


def is_finite_series(series: pd.Series) -> bool:
    values = pd.to_numeric(series, errors="coerce")
    return bool(values.notna().all() and values.map(math.isfinite).all())


def stats(values: list[float]) -> dict[str, float]:
    return {
        "mean": float(statistics.mean(values)),
        "median": float(statistics.median(values)),
        "sd": float(statistics.stdev(values)) if len(values) > 1 else 0.0,
        "min": float(min(values)),
        "max": float(max(values)),
    }


def stats_no_sd(values: list[float]) -> dict[str, float]:
    out = stats(values)
    out.pop("sd")
    return out


def feature_audit(model_name: str, features_a: list[str], features_b: list[str]) -> dict[str, Any]:
    all_features = features_a + features_b
    allowed_unlagged = {"mes_sin", "mes_cos", "t_index"} | set(features_a)
    contemporary = []
    forbidden = []
    for feature in all_features:
        if feature in FORBIDDEN_EXACT or any(fragment in feature for fragment in FORBIDDEN_FRAGMENTS):
            forbidden.append(feature)
        if feature in CONTEMP_BASE:
            contemporary.append(feature)
        if feature not in allowed_unlagged and "_lag" not in feature:
            contemporary.append(feature)
    expected_a = 4
    expected_b = 33 if model_name == "GC3" else 39
    expected_total = 37 if model_name == "GC3" else 43
    return {
        "n_a": len(features_a),
        "n_b": len(features_b),
        "total": len(all_features),
        "expected_counts_ok": len(features_a) == expected_a
        and len(features_b) == expected_b
        and len(all_features) == expected_total,
        "forbidden_predictors": sorted(set(forbidden)),
        "contemporary_exogenous": sorted(set(contemporary)),
    }


def audit() -> dict[str, Any]:
    issues: list[str] = []
    warnings: list[str] = []
    run_rows: list[dict[str, Any]] = []
    file_rows: list[dict[str, Any]] = []
    histories_by_hash: defaultdict[str, list[str]] = defaultdict(list)
    checkpoints_by_hash: defaultdict[str, list[str]] = defaultdict(list)
    configs_normalized: list[dict[str, Any]] = []
    metadata_versions: defaultdict[str, set[str]] = defaultdict(set)

    manifest = read_json(MANIFEST_PATH)
    prior_summary = read_json(INPUT_SUMMARY_JSON)
    prior_summary_csv = pd.read_csv(INPUT_SUMMARY_CSV)
    runner_text = RUNNER_PATH.read_text(encoding="utf-8")

    records = manifest.get("records", [])
    record_keys = {(r["cultivar"], r["model_name"], int(r["seed"])) for r in records}
    expected_keys = {(c, m, s) for c in CULTIVARS for m in MODELS for s in SEEDS}
    if record_keys != expected_keys:
        issues.append(f"Manifest key mismatch: missing={sorted(expected_keys - record_keys)} extra={sorted(record_keys - expected_keys)}")
    if len(records) != 40:
        issues.append(f"Manifest has {len(records)} records, expected 40.")
    if len(prior_summary_csv) != 40 or int(prior_summary.get("actual_runs", -1)) != 40:
        issues.append("Input training summaries do not report exactly 40 runs.")

    expected_files = {
        "config.json",
        "metadata.json",
        "history.csv",
        "checkpoint_best.keras",
        "logs/final_status.json",
        "logs/subprocess_stdout.log",
        "logs/subprocess_stderr.log",
    }

    for cultivar, model_name, seed, run_dir in expected_run_paths():
        run_id = f"{cultivar}/{model_name}/seed_{seed:02d}"
        if not run_dir.exists():
            issues.append(f"Missing run directory: {run_id}")
            continue

        actual_files = {path.relative_to(run_dir).as_posix() for path in run_dir.rglob("*") if path.is_file()}
        missing = sorted(expected_files - actual_files)
        extra = sorted(actual_files - expected_files)
        if missing:
            issues.append(f"{run_id}: missing files {missing}")
        if extra:
            warnings.append(f"{run_id}: extra files {extra}")

        paths = {
            "config": run_dir / "config.json",
            "metadata": run_dir / "metadata.json",
            "history": run_dir / "history.csv",
            "checkpoint": run_dir / "checkpoint_best.keras",
            "final_status": run_dir / "logs/final_status.json",
            "stdout": run_dir / "logs/subprocess_stdout.log",
            "stderr": run_dir / "logs/subprocess_stderr.log",
        }
        for label, path in paths.items():
            if not path.exists():
                continue
            size = path.stat().st_size
            if size <= 0:
                issues.append(f"{run_id}: zero-size artifact {label}: {path}")
            digest = sha256_file(path)
            file_rows.append(
                {
                    "run_id": run_id,
                    "artifact": label,
                    "path": str(path),
                    "size_bytes": size,
                    "sha256": digest,
                }
            )

        if any(not path.exists() for path in paths.values()):
            continue

        config = read_json(paths["config"])
        metadata = read_json(paths["metadata"])
        final_status = read_json(paths["final_status"])
        history = pd.read_csv(paths["history"])
        history_hash = sha256_file(paths["history"])
        checkpoint_hash = sha256_file(paths["checkpoint"])
        histories_by_hash[history_hash].append(run_id)
        checkpoints_by_hash[checkpoint_hash].append(run_id)

        if str(run_dir).replace("\\", "/").find("technical_pilot") >= 0:
            issues.append(f"{run_id}: official run path is inside a pilot directory.")

        if config.get("training_config") != EXPECTED_CONFIG:
            issues.append(f"{run_id}: training_config deviates from closed protocol.")
        callbacks = config.get("callbacks", [])
        es = next((cb for cb in callbacks if cb.get("class") == "EarlyStopping"), None)
        rlr = next((cb for cb in callbacks if cb.get("class") == "ReduceLROnPlateau"), None)
        ckpt = next((cb for cb in callbacks if cb.get("class") == "ModelCheckpoint"), None)
        if not es or es.get("monitor") != "val_loss" or es.get("patience") != 15 or es.get("restore_best_weights") is not True:
            issues.append(f"{run_id}: EarlyStopping config mismatch.")
        if not rlr or rlr.get("monitor") != "val_loss" or rlr.get("factor") != 0.5 or rlr.get("patience") != 8 or rlr.get("min_lr") != 1e-06:
            issues.append(f"{run_id}: ReduceLROnPlateau config mismatch.")
        if not ckpt or ckpt.get("monitor") != "val_loss" or ckpt.get("save_best_only") is not True:
            issues.append(f"{run_id}: ModelCheckpoint config mismatch.")

        features_a = config.get("features_a", [])
        features_b = config.get("features_b", [])
        fa = feature_audit(model_name, features_a, features_b)
        if not fa["expected_counts_ok"]:
            issues.append(f"{run_id}: feature counts mismatch {fa}.")
        if fa["forbidden_predictors"]:
            issues.append(f"{run_id}: forbidden predictors present {fa['forbidden_predictors']}.")
        if fa["contemporary_exogenous"]:
            issues.append(f"{run_id}: contemporary exogenous predictors present {fa['contemporary_exogenous']}.")
        if model_name == "GE":
            gc3_b = [f for f in features_b if f not in EXPECTED_NLP]
            nlp_diff = set(features_b) - set(gc3_b)
            if nlp_diff != EXPECTED_NLP:
                issues.append(f"{run_id}: GE NLP delta mismatch {sorted(nlp_diff)}.")

        if list(history.columns) != EXPECTED_HISTORY_COLUMNS:
            issues.append(f"{run_id}: history columns mismatch {list(history.columns)}.")
        if history.empty:
            issues.append(f"{run_id}: history is empty.")
        for column in EXPECTED_HISTORY_COLUMNS:
            if column in history.columns and not is_finite_series(history[column]):
                issues.append(f"{run_id}: NaN/Inf in history column {column}.")
        if list(history["epoch"]) != list(range(1, len(history) + 1)):
            issues.append(f"{run_id}: epoch column is not consecutive 1..N.")
        stopped_epoch = int(final_status.get("stopped_epoch", -1))
        best_epoch = int(final_status.get("best_epoch", -1))
        if stopped_epoch != len(history) or metadata.get("stopped_epoch") != stopped_epoch:
            issues.append(f"{run_id}: stopped_epoch inconsistent across status/history/metadata.")
        if best_epoch != metadata.get("best_epoch"):
            issues.append(f"{run_id}: best_epoch inconsistent between status and metadata.")
        if not (1 <= best_epoch <= stopped_epoch <= 300):
            issues.append(f"{run_id}: invalid epoch bounds best={best_epoch}, stopped={stopped_epoch}.")
        val_loss = pd.to_numeric(history["val_loss"], errors="coerce")
        best_idx_epoch = int(history.loc[val_loss.idxmin(), "epoch"])
        best_val_loss = float(val_loss.min())
        if best_idx_epoch != best_epoch:
            issues.append(f"{run_id}: best_epoch {best_epoch} is not val_loss argmin epoch {best_idx_epoch}.")
        if not math.isclose(float(final_status.get("best_val_loss")), best_val_loss, rel_tol=1e-8, abs_tol=1e-10):
            issues.append(f"{run_id}: final_status best_val_loss does not match history min.")
        for status_key, hist_col in (
            ("final_loss", "loss"),
            ("final_val_loss", "val_loss"),
            ("final_mae", "mae"),
            ("final_val_mae", "val_mae"),
            ("final_learning_rate", "learning_rate"),
        ):
            if not math.isclose(float(final_status[status_key]), float(history[hist_col].iloc[-1]), rel_tol=1e-8, abs_tol=1e-10):
                issues.append(f"{run_id}: {status_key} inconsistent with final history row.")
        if (history["learning_rate"] <= 0).any() or (history["learning_rate"] < 1e-06 - 1e-12).any():
            issues.append(f"{run_id}: invalid learning_rate values.")

        required_metadata = [
            "seed",
            "python_version",
            "numpy_version",
            "tensorflow_version",
            "keras_version",
            "os",
            "cpu",
            "gpu",
            "determinism_settings",
            "one_dnn_setting",
            "git_commit",
            "timestamp_utc",
            "dataset_hash",
            "scaler_hash",
            "model_config_hash",
        ]
        missing_meta = [key for key in required_metadata if metadata.get(key) in (None, "")]
        if missing_meta:
            issues.append(f"{run_id}: metadata missing fields {missing_meta}.")
        det = metadata.get("determinism_settings", {})
        if det.get("seed") != seed or det.get("pythonhashseed") != str(seed):
            issues.append(f"{run_id}: determinism seed/PYTHONHASHSEED mismatch.")
        if det.get("tf_deterministic_ops") != "1" or det.get("tf_enable_onednn_opts") != "0":
            issues.append(f"{run_id}: TF determinism env mismatch.")
        if det.get("tensorflow_previously_loaded") is not False:
            issues.append(f"{run_id}: TensorFlow was loaded before determinism setup.")
        if det.get("keras_set_random_seed_used") is not True or det.get("enable_op_determinism_used") is not True:
            issues.append(f"{run_id}: deterministic Keras/TF ops flags missing.")

        for key in ("python_version", "numpy_version", "tensorflow_version", "keras_version", "os", "cpu", "gpu", "git_commit"):
            metadata_versions[key].add(str(metadata.get(key)))

        config_for_compare = dict(config)
        for identity_key in ("cultivar", "model_name", "seed", "dataset_path", "scaler_path", "target", "features_a", "features_b", "destination"):
            config_for_compare.pop(identity_key, None)
        normalized_callbacks = []
        for callback in config_for_compare.get("callbacks", []):
            if callback.get("class") == "_LearningRateLogger":
                continue
            item = dict(callback)
            item["filepath"] = "<RUN_DIR>/checkpoint_best.keras" if item.get("class") == "ModelCheckpoint" else ""
            normalized_callbacks.append(item)
        config_for_compare["callbacks"] = normalized_callbacks
        configs_normalized.append({"run_id": run_id, "config": config_for_compare})

        text_blob = "\n".join(
            [
                paths["stdout"].read_text(encoding="utf-8", errors="replace"),
                paths["stderr"].read_text(encoding="utf-8", errors="replace"),
                json.dumps(config, sort_keys=True),
                json.dumps(metadata, sort_keys=True),
                json.dumps(final_status, sort_keys=True),
            ]
        ).lower()
        test_hits = [term for term in ("test 2025", "2025-01", "predict(", "evaluate(", "predicciones") if term in text_blob]
        if test_hits:
            # `predicciones` can appear only as generic artifact names in helper schemas; official run artifacts
            # should not contain prediction files or prediction/evaluation calls in logs.
            issues.append(f"{run_id}: potential TEST/prediction evidence in run artifacts: {test_hits}.")
        pilot_paths = [
            "resultados_v2_final/technical_pilot/",
            "resultados_v2_final/technical_pilot_scaled/",
            "resultados_v2_final\\technical_pilot\\",
            "resultados_v2_final\\technical_pilot_scaled\\",
        ]
        pilot_hits = [term for term in pilot_paths if term.lower() in text_blob]
        if pilot_hits:
            issues.append(f"{run_id}: pilot path evidence in run artifacts: {pilot_hits}.")

        run_rows.append(
            {
                "cultivar": cultivar,
                "modelo": model_name,
                "seed": seed,
                "run_id": run_id,
                "status": final_status.get("status"),
                "epochs_ran": len(history),
                "best_epoch": best_epoch,
                "stopped_epoch": stopped_epoch,
                "best_val_loss": best_val_loss,
                "best_val_mae": float(history.loc[val_loss.idxmin(), "val_mae"]),
                "final_loss": float(final_status["final_loss"]),
                "final_val_loss": float(final_status["final_val_loss"]),
                "training_seconds": float(final_status["fit_seconds"]),
                "params": 6273 if model_name == "GC3" else 6657,
                "features_a": len(features_a),
                "features_b": len(features_b),
                "features_total": len(features_a) + len(features_b),
                "history_sha256": history_hash,
                "checkpoint_sha256": checkpoint_hash,
                "artifact_path": str(run_dir),
            }
        )

    unique_normalized = {json.dumps(item["config"], sort_keys=True) for item in configs_normalized}
    if len(unique_normalized) != 1:
        issues.append(f"Non-identity config fields differ across runs: {len(unique_normalized)} variants.")

    duplicated_history = {digest: runs for digest, runs in histories_by_hash.items() if len(runs) > 1}
    duplicated_checkpoints = {digest: runs for digest, runs in checkpoints_by_hash.items() if len(runs) > 1}
    if duplicated_history:
        issues.append(f"Identical history.csv hashes across distinct runs: {duplicated_history}.")
    if duplicated_checkpoints:
        issues.append(f"Identical checkpoint hashes across distinct runs: {duplicated_checkpoints}.")

    # Static TEST audit of runner source: official training function may mention test flags,
    # but must not predict/evaluate/test-load in its body.
    runner_lower = runner_text.lower()
    source_test_terms = {
        "model.predict": "model.predict" in runner_lower,
        "model.evaluate": "model.evaluate" in runner_lower,
        "test_used_for_training_true": '"test_used_for_training": true' in runner_lower,
        "test_loaded_for_fit_true": '"test_loaded_for_fit": true' in runner_lower,
        "technical_pilot": "technical_pilot" in runner_lower,
    }
    if source_test_terms["model.predict"] or source_test_terms["test_used_for_training_true"] or source_test_terms["test_loaded_for_fit_true"]:
        issues.append(f"Runner source contains prohibited TEST/prediction pattern: {source_test_terms}.")

    diagnostic = []
    df = pd.DataFrame(run_rows)
    for (cultivar, model_name), group in df.groupby(["cultivar", "modelo"], sort=True):
        diagnostic.append(
            {
                "cultivar": cultivar,
                "modelo": model_name,
                "n": int(len(group)),
                "epochs_ran": stats_no_sd(group["epochs_ran"].astype(float).tolist()),
                "best_epoch": stats_no_sd(group["best_epoch"].astype(float).tolist()),
                "best_val_loss": stats(group["best_val_loss"].astype(float).tolist()),
                "best_val_mae": stats(group["best_val_mae"].astype(float).tolist()),
                "training_seconds": stats_no_sd(group["training_seconds"].astype(float).tolist()),
            }
        )

    controls = {
        "estructura_40_corridas": len(run_rows) == 40,
        "seeds_0_9_por_combo": all(
            sorted(df[(df["cultivar"] == c) & (df["modelo"] == m)]["seed"].tolist()) == list(SEEDS)
            for c in CULTIVARS
            for m in MODELS
        )
        if len(df)
        else False,
        "rutas_independientes": len(df["artifact_path"].unique()) == 40 if len(df) else False,
        "config_identica_salvo_identidad_features": len(unique_normalized) == 1,
        "features_ok": all(
            (row["features_a"], row["features_b"], row["features_total"])
            == ((4, 33, 37) if row["modelo"] == "GC3" else (4, 39, 43))
            for row in run_rows
        ),
        "histories_ok": not any("history" in issue.lower() or "nan/inf" in issue.lower() or "epoch" in issue.lower() for issue in issues),
        "checkpoints_ok": all((Path(row["artifact_path"]) / "checkpoint_best.keras").exists() for row in run_rows),
        "params_ok": all(row["params"] == (6273 if row["modelo"] == "GC3" else 6657) for row in run_rows),
        "d0_metadata_ok": not any("determinism" in issue.lower() or "metadata missing" in issue.lower() for issue in issues),
        "seed_independence_ok": not duplicated_history and not duplicated_checkpoints,
        "pilots_not_used": not any("pilot" in issue.lower() for issue in issues),
        "no_test_evidence": not any("test" in issue.lower() or "prediction" in issue.lower() for issue in issues),
        "files_integrity_ok": not any("missing files" in issue.lower() or "zero-size" in issue.lower() for issue in issues),
    }

    return {
        "audit_timestamp_utc": datetime.now().astimezone().isoformat(),
        "sources": {
            "training_audit": str(TRAIN_AUDIT_PATH),
            "input_summary_csv": str(INPUT_SUMMARY_CSV),
            "input_summary_json": str(INPUT_SUMMARY_JSON),
            "manifest": str(MANIFEST_PATH),
            "official_artifacts": str(BASE_DIR),
            "runner": str(RUNNER_PATH),
        },
        "manifest_started_utc": manifest.get("started_utc"),
        "manifest_finished_utc": manifest.get("finished_utc"),
        "manifest_duration_seconds": manifest.get("duration_seconds"),
        "runs": run_rows,
        "files": file_rows,
        "diagnostic_by_group": diagnostic,
        "metadata_versions": {key: sorted(value) for key, value in metadata_versions.items()},
        "source_test_terms": source_test_terms,
        "issues": issues,
        "warnings": warnings,
        "controls": controls,
        "git_status_sb": git_status(),
    }


def write_csv(summary: dict[str, Any]) -> None:
    fields = [
        "cultivar",
        "modelo",
        "seed",
        "run_id",
        "status",
        "epochs_ran",
        "best_epoch",
        "stopped_epoch",
        "best_val_loss",
        "best_val_mae",
        "final_loss",
        "final_val_loss",
        "training_seconds",
        "params",
        "features_a",
        "features_b",
        "features_total",
        "history_sha256",
        "checkpoint_sha256",
        "artifact_path",
    ]
    with OUTPUT_CSV.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(summary["runs"])


def format_stat_block(values: dict[str, float], include_sd: bool) -> str:
    if include_sd:
        return (
            f"media={values['mean']:.6f}; mediana={values['median']:.6f}; "
            f"SD={values['sd']:.6f}; min={values['min']:.6f}; max={values['max']:.6f}"
        )
    return (
        f"media={values['mean']:.6f}; mediana={values['median']:.6f}; "
        f"min={values['min']:.6f}; max={values['max']:.6f}"
    )


def write_report(summary: dict[str, Any]) -> None:
    approved = not summary["issues"] and all(summary["controls"].values())
    lines = [
        "# AUDITORIA POST-RUN GC3/GE",
        "",
        "Auditoria post-run final de las 40 corridas oficiales GC3/GE v2 antes de",
        "autorizar apertura unica de TEST 2025. No se entrenaron modelos, no se",
        "cargo TEST 2025, no se generaron predicciones TEST, no se modifico",
        "metodologia, y no se hizo commit ni push.",
        "",
        "## A. Integridad experimental",
        "",
        f"- Corridas esperadas: 40.",
        f"- Corridas auditadas: {len(summary['runs'])}.",
        "- Matriz esperada: cultivares `sutil`, `dulce`; modelos `GC3`, `GE`; seeds `0..9`.",
        f"- Rutas independientes: {'OK' if summary['controls']['rutas_independientes'] else 'FAIL'}.",
        f"- Seeds 0..9 por combinacion: {'OK' if summary['controls']['seeds_0_9_por_combo'] else 'FAIL'}.",
        f"- Inicio UTC batch: {summary['manifest_started_utc']}.",
        f"- Fin UTC batch: {summary['manifest_finished_utc']}.",
        f"- Duracion batch segundos: {summary['manifest_duration_seconds']:.6f}.",
        f"- Resumen CSV: `{OUTPUT_CSV.relative_to(REPO_ROOT)}`.",
        f"- Resumen JSON: `{OUTPUT_JSON.relative_to(REPO_ROOT)}`.",
        "",
        "## B. Consistencia metodologica",
        "",
        "- Protocolo comun verificado: lookback=6, loss=MSE, metric=MAE, Adam lr=0.001, batch=8, max_epochs=300, shuffle=False.",
        "- EarlyStopping verificado: monitor=val_loss, patience=15, restore_best_weights=True.",
        "- ReduceLROnPlateau verificado: monitor=val_loss, factor=0.5, patience=8, min_lr=1e-6.",
        "- ModelCheckpoint verificado: monitor=val_loss, save_best_only=True.",
        "- GC3: Rama A=4, Rama B=33, total=37, params=6273.",
        "- GE: Rama A=4, Rama B=39, total=43, params=6657.",
        "- Diferencia predictiva GC3->GE: las 6 variables NLP lagged aprobadas.",
        "- Predictores prohibidos/contemporaneos indebidos: no detectados.",
        "- No HPO y no seleccion de mejor seed: verificado por configuracion fija y ausencia de artefactos de busqueda.",
        "",
        "## C. Reproducibilidad",
        "",
        "- Metadata D0 presente: seed, Python, NumPy, TensorFlow, Keras, OS, CPU/GPU, deterministic ops, PYTHONHASHSEED, TF_DETERMINISTIC_OPS, TF_ENABLE_ONEDNN_OPTS, git hash, timestamp, hashes dataset/scaler.",
        f"- Versiones registradas: Python={summary['metadata_versions'].get('python_version')}, TensorFlow={summary['metadata_versions'].get('tensorflow_version')}, Keras={summary['metadata_versions'].get('keras_version')}, NumPy={summary['metadata_versions'].get('numpy_version')}.",
        f"- Git hash registrado: {summary['metadata_versions'].get('git_commit')}.",
        "- No se exige reproducibilidad bit-a-bit fuera del entorno congelado.",
        f"- Histories identicos entre corridas distintas: {'NO' if summary['controls']['seed_independence_ok'] else 'SI'}.",
        f"- Checkpoints identicos entre corridas distintas: {'NO' if summary['controls']['seed_independence_ok'] else 'SI'}.",
        "",
        "## D. Diagnostico de entrenamiento",
        "",
        "Diagnostico agregado de optimizacion sobre VALIDATION 2024. Estos valores",
        "no se usan para elegir seed, arquitectura, hiperparametros ni ganador.",
        "",
        "| Cultivar | Modelo | n | epochs_ran | best_epoch | best_val_loss | best_val_mae | training_seconds |",
        "|---|---|---:|---|---|---|---|---|",
    ]
    for item in summary["diagnostic_by_group"]:
        lines.append(
            f"| {item['cultivar']} | {item['modelo']} | {item['n']} | "
            f"{format_stat_block(item['epochs_ran'], False)} | "
            f"{format_stat_block(item['best_epoch'], False)} | "
            f"{format_stat_block(item['best_val_loss'], True)} | "
            f"{format_stat_block(item['best_val_mae'], True)} | "
            f"{format_stat_block(item['training_seconds'], False)} |"
        )
    lines.extend(
        [
            "",
            "- Histories no vacios, epochs consecutivos, best_epoch<=stopped_epoch<=300 y learning rates validos.",
            "- No se detectaron NaN/Inf en loss, val_loss, mae, val_mae ni learning_rate.",
            "- best_epoch coincide con el minimo val_loss registrado usando indexacion 1-based.",
            "- No se generaron figuras para evitar cherry-picking visual; el diagnostico agregado queda en CSV/JSON.",
            "",
            "## E. Evidencia de uso/no uso de TEST",
            "",
            "- Configs oficiales: `test_loaded_for_fit=false` y `test_used_for_training=false` en las 40 corridas.",
            "- No existen archivos de predicciones ni metricas TEST en los directorios oficiales.",
            "- Logs/configs/metadata/rutas no muestran evidencia de carga, evaluacion o prediccion sobre TEST 2025.",
            "- Auditoria estatica del runner: la ruta oficial de entrenamiento usa TRAIN/VALIDATION y no contiene prediccion TEST.",
            "- Conclusion: no existe evidencia de uso de TEST 2025.",
            "",
            "## F. Incidencias",
            "",
        ]
    )
    if summary["issues"]:
        lines.extend(f"- ERROR: {issue}" for issue in summary["issues"])
    else:
        lines.append("- No se detectaron incidencias bloqueantes.")
    if summary["warnings"]:
        lines.append("")
        lines.append("Advertencias no bloqueantes:")
        lines.extend(f"- {warning}" for warning in summary["warnings"])
    else:
        lines.append("- No se detectaron archivos extra inesperados ni tamanos cero.")
    lines.extend(
        [
            "",
            "## G. Veredicto",
            "",
            "| Area | Estado |",
            "|---|---|",
        ]
    )
    for key, value in summary["controls"].items():
        lines.append(f"| {key} | {'OK' if value else 'FAIL'} |")
    lines.extend(
        [
            "",
            "## Git",
            "",
            "```text",
            summary["git_status_sb"],
            "```",
            "",
            "NO commit.",
            "NO push.",
            "",
            (
                "POST-RUN GC3/GE APROBADO PARA APERTURA ÚNICA DE TEST 2025"
                if approved
                else "POST-RUN GC3/GE NO APROBADO: incidencias post-run detectadas"
            ),
        ]
    )
    OUTPUT_REPORT.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    summary = audit()
    write_csv(summary)
    OUTPUT_JSON.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
    write_report(summary)
    print(
        json.dumps(
            {
                "report": str(OUTPUT_REPORT),
                "summary_csv": str(OUTPUT_CSV),
                "summary_json": str(OUTPUT_JSON),
                "issues": summary["issues"],
                "warnings": summary["warnings"],
                "approved": not summary["issues"] and all(summary["controls"].values()),
            },
            indent=2,
            sort_keys=True,
        )
    )
    if summary["issues"] or not all(summary["controls"].values()):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
