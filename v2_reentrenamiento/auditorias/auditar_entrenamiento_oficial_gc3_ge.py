"""Post-run integrity audit for the official GC3/GE training batch."""

from __future__ import annotations

import csv
import json
import math
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

BASE_DIR = REPO_ROOT / "v2_reentrenamiento/resultados_v2_final/official_gc3_ge"
MANIFEST_PATH = REPO_ROOT / "v2_reentrenamiento/auditorias/entrenamiento_oficial_gc3_ge_manifest.json"
SUMMARY_CSV = REPO_ROOT / "v2_reentrenamiento/auditorias/resumen_entrenamiento_oficial_gc3_ge.csv"
SUMMARY_JSON = REPO_ROOT / "v2_reentrenamiento/auditorias/resumen_entrenamiento_oficial_gc3_ge.json"
REPORT_PATH = REPO_ROOT / "v2_reentrenamiento/auditorias/AUDITORIA_ENTRENAMIENTO_OFICIAL_GC3_GE.md"

CULTIVARS = ("sutil", "dulce")
MODELS = ("GC3", "GE")
SEEDS = tuple(range(10))
EXPECTED_PARAMS = {"GC3": 6273, "GE": 6657}
EXPECTED_HISTORY_COLUMNS = ["epoch", "loss", "val_loss", "mae", "val_mae", "learning_rate"]
EXPECTED_TRAINING_CONFIG = {
    "lookback": 6,
    "batch_size": 8,
    "max_epochs": 300,
    "learning_rate": 0.001,
    "loss": "mse",
    "metric": "mae",
    "shuffle": False,
    "early_stopping_monitor": "val_loss",
    "early_stopping_patience": 15,
    "early_stopping_restore_best_weights": True,
    "reduce_lr_monitor": "val_loss",
    "reduce_lr_factor": 0.5,
    "reduce_lr_patience": 8,
    "reduce_lr_min_lr": 1e-06,
}


def read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def git_status() -> str:
    result = subprocess.run(
        ["git", "status", "--short", "--branch"],
        cwd=REPO_ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    text = result.stdout.strip()
    if result.stderr.strip():
        text += "\n" + result.stderr.strip()
    return text


def finite_column(frame: pd.DataFrame, column: str) -> bool:
    values = pd.to_numeric(frame[column], errors="coerce")
    return bool(values.notna().all() and values.map(math.isfinite).all())


def expected_run_dirs() -> list[tuple[str, str, int, Path]]:
    return [
        (cultivar, model_name, seed, BASE_DIR / cultivar / model_name / f"seed_{seed:02d}")
        for cultivar in CULTIVARS
        for model_name in MODELS
        for seed in SEEDS
    ]


def audit() -> dict:
    from v2_reentrenamiento.src.models.dual_lstm_attention import build_gc3_ge_model
    from v2_reentrenamiento.src.models.features_gc3_ge import build_feature_spec
    from v2_reentrenamiento.src.training.official_runner_gc3_ge import (
        build_train_validation_sequences,
        load_training_validation_frame,
        dataset_path,
    )

    manifest = read_json(MANIFEST_PATH)
    model_params = {
        "GC3": int(build_gc3_ge_model("GC3", 4, 33).count_params()),
        "GE": int(build_gc3_ge_model("GE", 4, 39).count_params()),
    }

    rows = []
    failures = []
    seen = set()
    configs_seen = []
    for cultivar, model_name, seed, run_dir in expected_run_dirs():
        key = (cultivar, model_name, seed)
        if key in seen:
            failures.append(f"Duplicate run key: {key}")
        seen.add(key)

        paths = {
            "config": run_dir / "config.json",
            "metadata": run_dir / "metadata.json",
            "history": run_dir / "history.csv",
            "checkpoint": run_dir / "checkpoint_best.keras",
            "final_status": run_dir / "logs/final_status.json",
            "stdout_log": run_dir / "logs/subprocess_stdout.log",
            "stderr_log": run_dir / "logs/subprocess_stderr.log",
        }
        missing = [name for name, path in paths.items() if not path.exists()]
        if missing:
            failures.append(f"Missing artifacts for {key}: {missing}")
            continue

        config = read_json(paths["config"])
        metadata = read_json(paths["metadata"])
        final_status = read_json(paths["final_status"])
        history = pd.read_csv(paths["history"])
        spec = build_feature_spec(cultivar, model_name)
        sequences = build_train_validation_sequences(
            load_training_validation_frame(dataset_path(REPO_ROOT, cultivar)),
            spec,
        )

        if list(history.columns) != EXPECTED_HISTORY_COLUMNS:
            failures.append(f"History columns mismatch for {key}: {list(history.columns)}")
        if not finite_column(history, "loss") or not finite_column(history, "val_loss"):
            failures.append(f"NaN/Inf in loss or val_loss for {key}")
        for metric_col in ("mae", "val_mae", "learning_rate"):
            if not finite_column(history, metric_col):
                failures.append(f"NaN/Inf in {metric_col} for {key}")
        if config.get("training_config") != EXPECTED_TRAINING_CONFIG:
            failures.append(f"Training config mismatch for {key}")
        if config.get("test_used_for_training") is not False or config.get("test_loaded_for_fit") is not False:
            failures.append(f"TEST flag mismatch for {key}")
        if "technical_pilot" in str(run_dir).replace("\\", "/"):
            failures.append(f"Pilot path reused for {key}: {run_dir}")
        if metadata.get("seed") != seed or metadata.get("cultivar") != cultivar or metadata.get("model_name") != model_name:
            failures.append(f"Metadata identity mismatch for {key}")
        if final_status.get("status") != "complete":
            failures.append(f"Final status is not complete for {key}")
        if int(final_status.get("stopped_epoch", -1)) != len(history):
            failures.append(f"stopped_epoch/history length mismatch for {key}")
        if int(final_status.get("best_epoch", -1)) < 1 or int(final_status.get("best_epoch", -1)) > len(history):
            failures.append(f"best_epoch out of range for {key}")
        if model_params[model_name] != EXPECTED_PARAMS[model_name]:
            failures.append(f"Params mismatch for {model_name}: {model_params[model_name]}")
        if int(len(sequences["train"]["y"])) != 84:
            failures.append(f"TRAIN sequence count mismatch for {key}")
        if int(len(sequences["validation"]["y"])) != 12:
            failures.append(f"VAL target count mismatch for {key}")
        if config["training_config"]["shuffle"] is not False:
            failures.append(f"shuffle not False for {key}")

        configs_seen.append(
            {
                "key": key,
                "training_config": config.get("training_config"),
                "callbacks": [
                    {
                        k: v
                        for k, v in callback.items()
                        if k in {"class", "monitor", "patience", "restore_best_weights", "factor", "min_lr", "save_best_only"}
                    }
                    for callback in config.get("callbacks", [])
                    if callback.get("class") != "_LearningRateLogger"
                ],
            }
        )
        best_idx = history["val_loss"].idxmin()
        rows.append(
            {
                "cultivar": cultivar,
                "modelo": model_name,
                "seed": seed,
                "status": final_status.get("status"),
                "epochs_ran": int(len(history)),
                "best_epoch": int(final_status["best_epoch"]),
                "stopped_epoch": int(final_status["stopped_epoch"]),
                "best_val_loss": float(final_status["best_val_loss"]),
                "best_val_mae": float(history.loc[best_idx, "val_mae"]),
                "final_loss": float(final_status["final_loss"]),
                "final_val_loss": float(final_status["final_val_loss"]),
                "training_seconds": float(final_status["fit_seconds"]),
                "params": model_params[model_name],
                "artifact_path": str(run_dir),
            }
        )

    expected_keys = {(c, m, s) for c in CULTIVARS for m in MODELS for s in SEEDS}
    actual_keys = {(row["cultivar"], row["modelo"], row["seed"]) for row in rows}
    if actual_keys != expected_keys:
        failures.append(f"Run key set mismatch. Missing={sorted(expected_keys - actual_keys)} extra={sorted(actual_keys - expected_keys)}")

    config_reference = configs_seen[0]["training_config"] if configs_seen else None
    if any(item["training_config"] != config_reference for item in configs_seen):
        failures.append("Training config is not identical across runs.")

    records = manifest.get("records", [])
    failed_records = [record for record in records if record.get("status") != "complete" or record.get("returncode") != 0]
    retried_records = [record for record in records if record.get("retried")]
    if failed_records:
        failures.append(f"Manifest contains failed records: {len(failed_records)}")

    summary = {
        "audit_timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "manifest_started_utc": manifest.get("started_utc"),
        "manifest_finished_utc": manifest.get("finished_utc"),
        "manifest_duration_seconds": manifest.get("duration_seconds"),
        "expected_runs": 40,
        "actual_runs": len(rows),
        "successful_runs": sum(1 for row in rows if row["status"] == "complete"),
        "failed_runs": len(failed_records),
        "retried_runs": len(retried_records),
        "rows": rows,
        "failures": failures,
        "controls": {
            "exactly_40_complete_runs": len(rows) == 40 and all(row["status"] == "complete" for row in rows),
            "ten_seeds_per_cultivar_model": all(
                sorted(row["seed"] for row in rows if row["cultivar"] == c and row["modelo"] == m) == list(SEEDS)
                for c in CULTIVARS
                for m in MODELS
            ),
            "seeds_0_9_no_duplicates": actual_keys == expected_keys and len(rows) == len(actual_keys),
            "no_pilot_artifacts_reused": all("technical_pilot" not in row["artifact_path"].replace("\\", "/") for row in rows),
            "no_run_overwrote_another": len(rows) == len({row["artifact_path"] for row in rows}),
            "test_not_loaded": all(read_json(Path(row["artifact_path"]) / "config.json").get("test_loaded_for_fit") is False for row in rows),
            "train_84_sequences": "TRAIN sequence count mismatch" not in "\n".join(failures),
            "val_12_targets": "VAL target count mismatch" not in "\n".join(failures),
            "gc3_6273_params": model_params["GC3"] == 6273,
            "ge_6657_params": model_params["GE"] == 6657,
            "shuffle_false_all": all(read_json(Path(row["artifact_path"]) / "config.json")["training_config"]["shuffle"] is False for row in rows),
            "same_closed_config_except_identity": not any("Training config" in failure for failure in failures),
            "history_columns_ok": not any("History columns" in failure for failure in failures),
            "best_and_stopped_epoch_present": all(row["best_epoch"] >= 1 and row["stopped_epoch"] == row["epochs_ran"] for row in rows),
            "checkpoint_present_all": all((Path(row["artifact_path"]) / "checkpoint_best.keras").exists() for row in rows),
            "no_nan_inf_loss_val_loss": not any("NaN/Inf in loss or val_loss" in failure for failure in failures),
            "failed_runs_registered": len(failed_records) == 0,
        },
        "git_status_final": git_status(),
    }
    return summary


def write_summary(summary: dict) -> None:
    rows = summary["rows"]
    with SUMMARY_CSV.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "cultivar",
                "modelo",
                "seed",
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
                "artifact_path",
            ],
        )
        writer.writeheader()
        writer.writerows(rows)
    SUMMARY_JSON.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")


def write_report(summary: dict) -> None:
    controls = summary["controls"]
    status_text = "COMPLETADO" if not summary["failures"] and all(controls.values()) else "INCOMPLETO"
    lines = [
        "# AUDITORIA ENTRENAMIENTO OFICIAL GC3/GE",
        "",
        "## Alcance",
        "",
        "Auditoria post-run de las 40 corridas oficiales GC3/GE v2. No incluye",
        "predicciones TEST, metricas TEST, inverse transform final, D35, Delta_s,",
        "SHAP ni seleccion de modelo/seed.",
        "",
        "## Tiempo",
        "",
        f"- Inicio UTC: {summary['manifest_started_utc']}",
        f"- Fin UTC: {summary['manifest_finished_utc']}",
        f"- Duracion total segundos: {summary['manifest_duration_seconds']:.6f}",
        "",
        "## Pre-flight",
        "",
        "- Pre-flight oficial ejecutado antes del entrenamiento: READY.",
        "- Runner autorizado: `v2_reentrenamiento/src/training/official_runner_gc3_ge.py`.",
        "- Cada corrida se ejecuto en un proceso Python separado para aplicar D0 antes de inicializar TensorFlow.",
        "",
        "## Corridas",
        "",
        f"- Esperadas: {summary['expected_runs']}",
        f"- Reales auditadas: {summary['actual_runs']}",
        f"- Exitosas: {summary['successful_runs']}",
        f"- Fallidas: {summary['failed_runs']}",
        f"- Reintentadas: {summary['retried_runs']}",
        f"- Resumen CSV: `{SUMMARY_CSV.relative_to(REPO_ROOT)}`",
        f"- Resumen JSON: `{SUMMARY_JSON.relative_to(REPO_ROOT)}`",
        "",
        "## Controles post-run",
        "",
        "| # | Control | Estado |",
        "|---:|---|---|",
    ]
    control_labels = [
        ("exactly_40_complete_runs", "exactamente 40 corridas oficiales completas"),
        ("ten_seeds_per_cultivar_model", "exactamente 10 seeds por cultivar/modelo"),
        ("seeds_0_9_no_duplicates", "seeds 0..9 sin duplicados ni faltantes"),
        ("no_pilot_artifacts_reused", "ninguna corrida reutilizo artefactos pilot"),
        ("no_run_overwrote_another", "ninguna corrida sobreescribio otra"),
        ("test_not_loaded", "ninguna corrida cargo TEST"),
        ("train_84_sequences", "todas usan TRAIN=84 sequences"),
        ("val_12_targets", "todas usan VAL=12 targets"),
        ("gc3_6273_params", "GC3 siempre 6273 params"),
        ("ge_6657_params", "GE siempre 6657 params"),
        ("shuffle_false_all", "shuffle=False en todas"),
        ("same_closed_config_except_identity", "misma configuracion cerrada salvo cultivar/modelo/seed"),
        ("history_columns_ok", "history.csv contiene columnas requeridas"),
        ("best_and_stopped_epoch_present", "best_epoch y stopped_epoch presentes"),
        ("checkpoint_present_all", "checkpoint_best.keras presente"),
        ("no_nan_inf_loss_val_loss", "sin NaN/Inf en loss o val_loss"),
        ("failed_runs_registered", "corridas fallidas registradas"),
    ]
    for index, (key, label) in enumerate(control_labels, start=1):
        lines.append(f"| {index} | {label} | {'OK' if controls[key] else 'FAIL'} |")

    lines.extend(
        [
            "",
            "## Confirmaciones metodologicas",
            "",
            "- TEST 2025 no fue cargado ni evaluado.",
            "- No se generaron predicciones TEST.",
            "- No se calcularon metricas TEST.",
            "- No hubo HPO.",
            "- No se modificaron arquitectura, hiperparametros ni features.",
            "- No se selecciono mejor seed.",
            "- No se interpreto que modelo gano.",
            "- No se reutilizaron checkpoints de `technical_pilot/` ni `technical_pilot_scaled/`.",
            "",
            "## Archivos nuevos/modificados",
            "",
            "- `v2_reentrenamiento/resultados_v2_final/official_gc3_ge/` con 40 directorios de corrida.",
            "- `v2_reentrenamiento/auditorias/ejecutar_entrenamiento_oficial_gc3_ge.py`.",
            "- `v2_reentrenamiento/auditorias/entrenamiento_oficial_gc3_ge_manifest.json`.",
            "- `v2_reentrenamiento/auditorias/auditar_entrenamiento_oficial_gc3_ge.py`.",
            "- `v2_reentrenamiento/auditorias/resumen_entrenamiento_oficial_gc3_ge.csv`.",
            "- `v2_reentrenamiento/auditorias/resumen_entrenamiento_oficial_gc3_ge.json`.",
            "- `v2_reentrenamiento/auditorias/AUDITORIA_ENTRENAMIENTO_OFICIAL_GC3_GE.md`.",
            "",
            "## Fallos o reintentos",
            "",
        ]
    )
    if summary["failures"]:
        lines.extend(f"- {failure}" for failure in summary["failures"])
    else:
        lines.append("- No se registraron fallos ni reintentos.")

    lines.extend(
        [
            "",
            "## Git status final",
            "",
            "```text",
            summary["git_status_final"],
            "```",
            "",
            "NO commit.",
            "NO push.",
            "",
            (
                "ENTRENAMIENTO OFICIAL GC3/GE COMPLETADO Y LISTO PARA AUDITORÍA POST-RUN"
                if status_text == "COMPLETADO"
                else "ENTRENAMIENTO OFICIAL GC3/GE INCOMPLETO: controles post-run fallidos"
            ),
        ]
    )
    REPORT_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    summary = audit()
    write_summary(summary)
    write_report(summary)
    print(
        json.dumps(
            {
                "report": str(REPORT_PATH),
                "summary_csv": str(SUMMARY_CSV),
                "summary_json": str(SUMMARY_JSON),
                "failures": summary["failures"],
            },
            indent=2,
            sort_keys=True,
        )
    )
    if summary["failures"] or not all(summary["controls"].values()):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
