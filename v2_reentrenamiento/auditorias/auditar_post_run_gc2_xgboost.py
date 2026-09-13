"""Post-run audit for the 20 official GC2/XGBoost runs."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[2]
MANIFEST_PATH = REPO_ROOT / "v2_reentrenamiento/auditorias/entrenamiento_oficial_gc2_xgboost_manifest.json"
RESULTS_DIR = REPO_ROOT / "v2_reentrenamiento/resultados_v2_final/official_gc2_xgboost"
SUMMARY_CSV = REPO_ROOT / "v2_reentrenamiento/auditorias/resumen_post_run_gc2_xgboost.csv"
SUMMARY_JSON = REPO_ROOT / "v2_reentrenamiento/auditorias/resumen_post_run_gc2_xgboost.json"
AUDIT_MD = REPO_ROOT / "v2_reentrenamiento/auditorias/AUDITORIA_POST_RUN_GC2_XGBOOST.md"

CULTIVARS = ("sutil", "dulce")
SEEDS = tuple(range(10))
REQUIRED_FILES = (
    "model.xgboost.json",
    "model.joblib",
    "metricas.json",
    "predicciones_train_val.csv",
    "feature_importance.csv",
    "config.json",
    "metadata.json",
    "logs/subprocess_stdout.log",
    "logs/subprocess_stderr.log",
)


def audit() -> dict:
    manifest = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
    records = manifest.get("records", [])
    rows = []
    controls = []

    expected_pairs = {(cultivar, seed) for cultivar in CULTIVARS for seed in SEEDS}
    actual_pairs = {(record.get("cultivar"), record.get("seed")) for record in records}
    controls.append(_control("20 manifest records", len(records) == 20, str(len(records))))
    controls.append(_control("expected cultivar/seed pairs", actual_pairs == expected_pairs, str(sorted(actual_pairs))))
    controls.append(_control("all subprocesses complete", all(r.get("status") == "complete" and r.get("returncode") == 0 for r in records), "manifest status/returncode"))

    for cultivar in CULTIVARS:
        for seed in SEEDS:
            run_dir = RESULTS_DIR / cultivar / "GC2_XGBoost" / f"seed_{seed:02d}"
            missing = [name for name in REQUIRED_FILES if not (run_dir / name).exists()]
            metadata = json.loads((run_dir / "metadata.json").read_text(encoding="utf-8")) if not missing else {}
            metrics = json.loads((run_dir / "metricas.json").read_text(encoding="utf-8")) if not missing else {}
            preds = pd.read_csv(run_dir / "predicciones_train_val.csv") if not missing else pd.DataFrame()
            rows.append(
                {
                    "cultivar": cultivar,
                    "model_name": "GC2_XGBoost",
                    "seed": seed,
                    "run_dir": str(run_dir),
                    "missing_files": "|".join(missing),
                    "train_mae": metrics.get("train", {}).get("mae"),
                    "train_rmse": metrics.get("train", {}).get("rmse"),
                    "train_r2": metrics.get("train", {}).get("r2"),
                    "val_mae": metrics.get("validation", {}).get("mae"),
                    "val_rmse": metrics.get("validation", {}).get("rmse"),
                    "val_r2": metrics.get("validation", {}).get("r2"),
                    "train_rows": int((preds["particion"] == "train").sum()) if not preds.empty else None,
                    "val_rows": int((preds["particion"] == "validation").sum()) if not preds.empty else None,
                    "test_loaded": metadata.get("test_loaded", False),
                    "config_hash": metadata.get("config_hash"),
                }
            )
            controls.append(_control(f"{cultivar} seed_{seed:02d} required artifacts", not missing, "|".join(missing) or "ok"))
            if metadata:
                controls.append(_control(f"{cultivar} seed_{seed:02d} train shape", metadata.get("train_shape") == {"X": [89, 37], "y": [89]}, str(metadata.get("train_shape"))))
                controls.append(_control(f"{cultivar} seed_{seed:02d} validation shape", metadata.get("validation_shape") == {"X": [12, 37], "y": [12]}, str(metadata.get("validation_shape"))))
                controls.append(_control(f"{cultivar} seed_{seed:02d} no TEST", metadata.get("test_loaded") is False, str(metadata.get("test_loaded"))))

    summary = pd.DataFrame(rows)
    summary.to_csv(SUMMARY_CSV, index=False, encoding="utf-8")
    payload = {
        "status": "approved" if all(item["ok"] for item in controls) else "failed",
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "manifest": str(MANIFEST_PATH),
        "summary_csv": str(SUMMARY_CSV),
        "controls": controls,
        "rows": rows,
    }
    SUMMARY_JSON.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    _write_md(payload, summary)
    return payload


def _control(name: str, ok: bool, detail: str) -> dict:
    return {"name": name, "ok": bool(ok), "detail": detail}


def _write_md(payload: dict, summary: pd.DataFrame) -> None:
    lines = [
        "# AUDITORIA POST-RUN GC2/XGBOOST",
        "",
        f"Generado UTC: `{payload['generated_utc']}`",
        "",
        "## Resultado",
        "",
        f"- Estado: `{payload['status']}`",
        f"- Corridas esperadas: 20",
        f"- Corridas auditadas: {len(summary)}",
        "- TEST 2025 abierto: NO.",
        "- HPO/CV/early stopping/best seed: NO.",
        "",
        "## Resumen de validacion",
        "",
        "| Cultivar | Seed | MAE val | RMSE val | R2 val |",
        "|---|---:|---:|---:|---:|",
    ]
    for row in summary.sort_values(["cultivar", "seed"]).to_dict("records"):
        lines.append(
            "| {cultivar} | {seed} | {mae:.6f} | {rmse:.6f} | {r2:.6f} |".format(
                cultivar=row["cultivar"],
                seed=row["seed"],
                mae=row["val_mae"],
                rmse=row["val_rmse"],
                r2=row["val_r2"],
            )
        )
    lines.extend(
        [
            "",
            "## Controles",
            "",
            "| Control | Estado | Detalle |",
            "|---|---|---|",
        ]
    )
    for item in payload["controls"]:
        lines.append(f"| {item['name']} | {'OK' if item['ok'] else 'FAIL'} | {item['detail']} |")
    AUDIT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")


if __name__ == "__main__":
    result = audit()
    print(json.dumps({"status": result["status"], "controls": len(result["controls"])}, indent=2))
